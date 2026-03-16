from __future__ import annotations

import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyzer import append_artifacts_to_index
from .evolution import issue, normalize_response_text
from .java_bff import (
    bundle_phase_root_for,
    bundle_root_for,
    bundle_slug,
    estimate_tokens,
    iter_bundle_payloads,
    load_bundle_payload,
    load_phase_pack_payload,
    resolve_java_bff_root,
    safe_name,
)
from .java_bff_context import compile_java_bff_context_pack, context_pack_paths, write_java_bff_context_pack
from .llm_provider import (
    build_request_artifact,
    extract_response_text,
    post_chat_completion,
    render_run_summary_markdown,
    resolve_provider_config,
)
from .models import ArtifactDescriptor


JAVA_BFF_ALLOWED_PHASES = {
    "phase-1-plan",
    "phase-2-repository-chunk",
    "phase-2-repository-merge",
    "phase-3-bff-assembly",
    "phase-4-verify",
}
JAVA_IDENTIFIER_RE = r"^[a-z][A-Za-z0-9]*$"
JAVA_LAYER_LEAK_TERMS = {
    "phase-2-repository-chunk": ["controller", "service", "restcontroller", "requestmapping"],
    "phase-2-repository-merge": ["controller", "service", "restcontroller", "requestmapping"],
    "phase-3-bff-assembly": ["mapsqlparametersource", "namedparameterjdbctemplate", "rowmapper", "queryforlist", "jdbc"],
}
JAVA_FORBIDDEN_TERMS = [
    "hibernate",
    "entitymanager",
    "jpa",
]


def ensure_java_bff_context_artifacts(
    analysis_root: Path,
    phase_pack_path: Path,
    prompt_profile: str | None = None,
) -> tuple[dict[str, Any], list[Path], list[ArtifactDescriptor]]:
    pack = compile_java_bff_context_pack(
        analysis_root=analysis_root,
        phase_pack_path=phase_pack_path,
        prompt_profile=prompt_profile,
    )
    paths = write_java_bff_context_pack(analysis_root, pack)
    artifacts = [
        artifact_descriptor_for_path(paths[0], "json", f"Java BFF context pack: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(paths[1], "text", f"Java BFF context prompt: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(paths[2], "markdown", f"Java BFF context summary: {phase_pack_path.stem}", "java_bff"),
    ]
    append_artifacts_to_index(resolve_java_bff_root(analysis_root).parent.parent, artifacts)
    return pack, paths, artifacts


def build_java_bff_validation_context(
    analysis_root: Path,
    phase_payload: dict[str, Any],
    phase_pack_path: Path,
) -> dict[str, Any]:
    java_root = resolve_java_bff_root(analysis_root)
    bundle_id = str(phase_payload["bundle_id"])
    bundle_payload = load_bundle_payload(analysis_root, bundle_id)
    bundle_queries = {
        str(item.get("query_id") or ""): item
        for item in bundle_payload.get("queries", [])
        if isinstance(item, dict)
    }
    query_id = phase_query_id_from_payload(phase_payload)
    query_payload = bundle_queries.get(query_id or "")
    card_payload = None
    if query_payload:
        card_path = Path(str(query_payload.get("card_json_path") or ""))
        if card_path.exists():
            loaded = json.loads(card_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                card_payload = loaded

    chunk_payload = None
    chunk_id = phase_chunk_id_from_payload(phase_payload)
    if chunk_id:
        chunk_path = java_root / "sql_chunks" / f"{safe_name(chunk_id)}.json"
        if chunk_path.exists():
            loaded = json.loads(chunk_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                chunk_payload = loaded

    plan_review = load_latest_review(java_root, bundle_id, "phase-1-plan")
    assembly_review = load_latest_review(java_root, bundle_id, "phase-3-bff-assembly")
    merge_reviews = {
        query_key: load_latest_review(java_root, bundle_id, "phase-2-repository-merge", query_id=query_key)
        for query_key in bundle_queries
    }
    accepted_chunk_reviews = load_reviews_for_phase(java_root, bundle_id, "phase-2-repository-chunk", query_id=query_id)
    return {
        "bundle_payload": bundle_payload,
        "bundle_query_ids": [item for item in bundle_queries if item],
        "phase_pack_path": str(phase_pack_path.resolve()),
        "query_id": query_id,
        "chunk_id": chunk_id,
        "query_payload": query_payload,
        "card_payload": card_payload,
        "chunk_payload": chunk_payload,
        "accepted_plan_review": plan_review,
        "accepted_chunk_reviews": accepted_chunk_reviews,
        "accepted_merge_reviews": merge_reviews,
        "accepted_assembly_review": assembly_review,
    }


def invoke_java_bff_phase_pack(
    analysis_root: Path,
    phase_pack_path: Path,
    provider_config_path: Path | None = None,
    provider_base_url: str | None = None,
    provider_api_key: str | None = None,
    provider_api_key_env: str = "OPENAI_API_KEY",
    provider_model: str | None = None,
    provider_name: str | None = None,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout_seconds: float | None = None,
    review: bool = False,
) -> dict[str, Any]:
    java_root = resolve_java_bff_root(analysis_root)
    phase_payload = load_phase_pack_payload(phase_pack_path)
    context_pack, context_paths, context_artifacts = ensure_java_bff_context_artifacts(
        analysis_root=analysis_root,
        phase_pack_path=phase_pack_path,
        prompt_profile=str(phase_payload.get("prompt_profile") or "qwen3-128k-java-bff"),
    )
    config = resolve_provider_config(
        provider_config_path=provider_config_path,
        provider_base_url=provider_base_url,
        provider_api_key=provider_api_key,
        provider_api_key_env=provider_api_key_env,
        provider_model=provider_model,
        provider_name=provider_name,
        token_limit=token_limit,
        temperature=temperature,
        timeout_seconds=timeout_seconds,
    )
    prompt_text = str(context_pack["prompt_text"])
    response_payload = post_chat_completion(config=config, prompt_text=prompt_text)
    response_text = extract_response_text(response_payload)

    bundle_id = str(phase_payload["bundle_id"])
    run_root = java_root / "llm_runs" / bundle_slug(bundle_id)
    run_root.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_dir = run_root / f"{timestamp}-{safe_name(phase_pack_path.stem)}"
    run_dir.mkdir(parents=True, exist_ok=True)

    request_path = run_dir / "request.json"
    response_json_path = run_dir / "response.json"
    response_text_path = run_dir / "response.txt"
    summary_path = run_dir / "run_summary.json"
    summary_md_path = run_dir / "run_summary.md"

    run_summary = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_id": bundle_id,
        "cluster_id": bundle_id,
        "stage": phase_payload["phase"],
        "budget": f"{phase_payload['budget']['usable_input_limit']}-tokens",
        "prompt_model": phase_payload["prompt_profile"],
        "phase": phase_payload["phase"],
        "phase_pack_path": str(phase_pack_path.resolve()),
        "prompt_path": str(phase_pack_path.resolve()),
        "prompt_profile": phase_payload["prompt_profile"],
        "context_pack_path": str(context_paths[0].resolve()),
        "context_pack_text_path": str(context_paths[1].resolve()),
        "context_missing_inputs": context_pack["missing_inputs"],
        "provider_name": config.provider_name or config.model,
        "provider_model": config.model,
        "provider_base_url": config.base_url,
        "token_limit": config.token_limit,
        "temperature": config.temperature,
        "prompt_estimated_tokens": int(context_pack["estimated_prompt_tokens"]),
        "prompt_sha256": hashlib.sha256(prompt_text.encode("utf-8")).hexdigest(),
        "response_usage": response_payload.get("usage", {}),
        "review_enabled": review,
    }

    request_path.write_text(json.dumps(build_request_artifact(config, prompt_text), indent=2, ensure_ascii=False), encoding="utf-8")
    response_json_path.write_text(json.dumps(response_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    response_text_path.write_text(response_text, encoding="utf-8")
    summary_path.write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")
    summary_md_path.write_text(render_run_summary_markdown(run_summary), encoding="utf-8")

    artifacts = [
        *context_artifacts,
        artifact_descriptor_for_path(request_path, "json", f"Java BFF LLM request: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(response_json_path, "json", f"Java BFF LLM response JSON: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(response_text_path, "text", f"Java BFF LLM response text: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(summary_path, "json", f"Java BFF LLM run summary: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(summary_md_path, "markdown", f"Java BFF LLM run summary (Markdown): {phase_pack_path.stem}", "java_bff"),
    ]

    review_result = None
    if review:
        review_result = review_java_bff_response_from_analysis(
            analysis_root=analysis_root,
            phase_pack_path=phase_pack_path,
            response_path=response_text_path,
        )
        run_summary["review_status"] = review_result["review"]["status"]
        run_summary["review_path"] = review_result["review"].get("review_json_path")
        summary_path.write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")
        summary_md_path.write_text(render_run_summary_markdown(run_summary), encoding="utf-8")
        artifacts.extend(review_result["artifacts"])

    append_java_bff_run_index(java_root / "llm_runs", summary_path, run_summary)
    append_artifacts_to_index(java_root.parent.parent, artifacts)
    return {
        "run_summary": run_summary,
        "response_text": response_text,
        "response_text_path": str(response_text_path.resolve()),
        "summary_path": str(summary_path.resolve()),
        "response_payload": response_payload,
        "artifacts": artifacts,
        "review": review_result,
    }


def review_java_bff_response_from_analysis(
    analysis_root: Path,
    phase_pack_path: Path,
    response_path: Path,
) -> dict[str, Any]:
    java_root = resolve_java_bff_root(analysis_root)
    phase_payload = load_phase_pack_payload(phase_pack_path)
    bundle_id = str(phase_payload["bundle_id"])
    raw_text = response_path.read_text(encoding="utf-8")
    validation_context = build_java_bff_validation_context(analysis_root, phase_payload, phase_pack_path)
    review = review_java_bff_response(
        phase_payload,
        raw_text,
        bundle_id=bundle_id,
        phase_pack_path=phase_pack_path,
        validation_context=validation_context,
    )

    review_root = java_root / "reviews" / bundle_slug(bundle_id)
    review_root.mkdir(parents=True, exist_ok=True)
    review_base = review_root / f"{safe_name(phase_pack_path.stem)}-review"
    review_json_path = Path(f"{review_base}.json")
    review_md_path = Path(f"{review_base}.md")
    review["response_path"] = str(response_path.resolve())
    review["review_json_path"] = str(review_json_path.resolve())
    review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
    review_md_path.write_text(render_java_bff_review_markdown(review), encoding="utf-8")

    artifacts = [
        artifact_descriptor_for_path(review_json_path, "json", f"Java BFF review: {phase_pack_path.stem}", "java_bff"),
        artifact_descriptor_for_path(review_md_path, "markdown", f"Java BFF review summary: {phase_pack_path.stem}", "java_bff"),
    ]
    repair_prompt = review.get("repair_prompt_text")
    if isinstance(repair_prompt, str) and repair_prompt.strip():
        repair_path = review_root / f"{safe_name(phase_pack_path.stem)}-repair.txt"
        repair_path.write_text(repair_prompt, encoding="utf-8")
        review["repair_prompt_path"] = str(repair_path.resolve())
        review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts.append(artifact_descriptor_for_path(repair_path, "text", f"Java BFF repair prompt: {phase_pack_path.stem}", "java_bff"))

    append_artifacts_to_index(java_root.parent.parent, artifacts)
    return {
        "review": review,
        "artifacts": artifacts,
    }


def review_java_bff_response(
    phase_payload: dict[str, Any],
    raw_text: str,
    bundle_id: str,
    phase_pack_path: Path,
    validation_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_text, normalization_notes = normalize_response_text(raw_text)
    issues: list[dict[str, Any]] = []
    parsed_response: dict[str, Any] | None = None
    schema = phase_payload.get("answer_schema", {})
    phase = str(phase_payload.get("phase") or "")
    if phase not in JAVA_BFF_ALLOWED_PHASES:
        raise ValueError(f"Unsupported Java BFF phase: {phase}")

    try:
        loaded = json.loads(normalized_text)
    except json.JSONDecodeError as exc:
        issues.append(issue("INVALID_JSON", "error", f"Response is not valid JSON: {exc.msg}."))
    else:
        if not isinstance(loaded, dict):
            issues.append(issue("RESPONSE_NOT_OBJECT", "error", "Response JSON must be a top-level object."))
        else:
            parsed_response = loaded
            issues.extend(validate_against_schema(schema, parsed_response))
            issues.extend(validate_java_bff_response(phase_payload, parsed_response, validation_context=validation_context))

    has_errors = any(item["severity"] == "error" for item in issues)
    status = "needs_revision" if has_errors else "accepted"
    if phase == "phase-4-verify" and parsed_response and str(parsed_response.get("verdict")) == "needs_more_context":
        status = "insufficient_evidence"

    next_phase_pack_path = None
    if status == "accepted":
        next_phase_pack_path = find_next_phase_pack_path(phase_pack_path, bundle_id)

    repair_prompt_text = None
    if status == "needs_revision":
        repair_prompt_text = render_java_bff_repair_prompt(phase_payload, raw_text, issues)

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_id": bundle_id,
        "phase": phase,
        "phase_pack_path": str(phase_pack_path.resolve()),
        "status": status,
        "normalization_notes": normalization_notes,
        "issues": issues,
        "parsed_response": parsed_response,
        "next_phase_pack_path": next_phase_pack_path,
        "safe_to_merge": status in {"accepted", "insufficient_evidence"},
        "accepted_for_merge": status in {"accepted", "insufficient_evidence"},
        "repair_prompt_text": repair_prompt_text,
    }


def merge_java_bff_phases(
    analysis_root: Path,
    bundle_id: str,
) -> dict[str, Any]:
    java_root = resolve_java_bff_root(analysis_root)
    bundle_payload = load_bundle_payload(analysis_root, bundle_id)
    review_rows = load_java_bff_reviews(analysis_root, bundle_id)
    accepted_by_prompt: dict[str, dict[str, Any]] = {}
    for row in review_rows:
        phase_pack = str(row.get("phase_pack_path") or "")
        if not phase_pack or row.get("status") not in {"accepted", "insufficient_evidence"}:
            continue
        accepted_by_prompt[phase_pack] = row

    missing_prompts = [path for path in bundle_payload.get("recommended_sequence", []) if f"{Path(path).with_suffix('.json')}" not in accepted_by_prompt]

    plan_output = None
    assembly_output = None
    verify_output = None
    repository_chunks: dict[str, list[dict[str, Any]]] = {}
    repository_merges: dict[str, dict[str, Any]] = {}
    for phase_pack_json, review in accepted_by_prompt.items():
        phase = str(review.get("phase") or "")
        parsed = review.get("parsed_response")
        if not isinstance(parsed, dict):
            continue
        if phase == "phase-1-plan":
            plan_output = parsed
        elif phase == "phase-3-bff-assembly":
            assembly_output = parsed
        elif phase == "phase-4-verify":
            verify_output = parsed
        elif phase == "phase-2-repository-chunk":
            query_id = str(parsed.get("query_id") or "unknown")
            repository_chunks.setdefault(query_id, []).append(parsed)
        elif phase == "phase-2-repository-merge":
            query_id = str(parsed.get("query_id") or "unknown")
            repository_merges[query_id] = parsed

    completion = {
        "accepted_prompt_count": len(accepted_by_prompt),
        "total_prompt_count": len(bundle_payload.get("recommended_sequence", [])),
        "missing_prompts": missing_prompts,
        "ready_for_skeletons": bool(plan_output) and bool(repository_merges) and bool(assembly_output),
    }
    repository_plan = {
        "methods": list(plan_output.get("repository_methods", [])) if isinstance(plan_output, dict) else [],
        "queries": [],
    }
    for query in bundle_payload.get("queries", []):
        if not isinstance(query, dict):
            continue
        query_id = str(query.get("query_id") or "unknown")
        repository_plan["queries"].append(
            {
                "query_id": query_id,
                "chunk_count": len(repository_chunks.get(query_id, [])),
                "chunk_outputs": repository_chunks.get(query_id, []),
                "merge_output": repository_merges.get(query_id),
            }
        )
    merged_payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_id": bundle_id,
        "bundle_slug": bundle_slug(bundle_id),
        "entry_query_id": bundle_payload.get("entry_query_id", bundle_id),
        "status": "ready" if completion["ready_for_skeletons"] else "incomplete",
        "plan_output": plan_output,
        "repository_chunk_outputs": {key: value for key, value in sorted(repository_chunks.items())},
        "repository_merge_outputs": {key: value for key, value in sorted(repository_merges.items())},
        "assembly_output": assembly_output,
        "verify_output": verify_output,
        "repository_plan": repository_plan,
        "bff_plan": {
            "service_logic": list(assembly_output.get("service_logic", [])) if isinstance(assembly_output, dict) else [],
            "controller_logic": list(assembly_output.get("controller_logic", [])) if isinstance(assembly_output, dict) else [],
            "dto_contract_hints": list(assembly_output.get("dto_contract_hints", [])) if isinstance(assembly_output, dict) else [],
            "error_handling": list(assembly_output.get("error_handling", [])) if isinstance(assembly_output, dict) else [],
        },
        "verification": verify_output or {},
        "completion": completion,
    }

    merged_root = java_root / "merged" / bundle_slug(bundle_id)
    merged_root.mkdir(parents=True, exist_ok=True)
    merged_json_path = merged_root / "implementation_plan.json"
    merged_md_path = merged_root / "implementation_plan.md"
    phase_outputs_path = merged_root / "accepted_phase_outputs.json"
    merged_json_path.write_text(json.dumps(merged_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    merged_md_path.write_text(render_java_bff_merged_markdown(merged_payload), encoding="utf-8")
    phase_outputs_path.write_text(json.dumps(accepted_by_prompt, indent=2, ensure_ascii=False), encoding="utf-8")
    artifacts = [
        artifact_descriptor_for_path(merged_json_path, "json", f"Java BFF implementation plan: {bundle_id}", "java_bff"),
        artifact_descriptor_for_path(merged_md_path, "markdown", f"Java BFF implementation plan summary: {bundle_id}", "java_bff"),
        artifact_descriptor_for_path(phase_outputs_path, "json", f"Java BFF accepted phase outputs: {bundle_id}", "java_bff"),
    ]
    append_artifacts_to_index(java_root.parent.parent, artifacts)
    return {
        "merged_payload": merged_payload,
        "implementation_plan": merged_payload,
        "artifacts": artifacts,
        "merged_path": merged_json_path,
    }


def load_java_bff_reviews(analysis_root: Path, bundle_id: str) -> list[dict[str, Any]]:
    review_root = resolve_java_bff_root(analysis_root) / "reviews" / bundle_slug(bundle_id)
    if not review_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(review_root.glob("*-review.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            rows.append(payload)
    rows.sort(key=lambda item: str(item.get("generated_at") or ""))
    return rows


def find_next_phase_pack_path(current_phase_pack_path: Path, bundle_id: str) -> str | None:
    bundle = load_bundle_payload(current_phase_pack_path.parents[3], bundle_id)
    current_json = str(current_phase_pack_path.with_suffix(".json").resolve())
    sequence = [str(Path(item).with_suffix(".json").resolve()) for item in bundle.get("recommended_sequence", [])]
    try:
        index = sequence.index(current_json)
    except ValueError:
        return None
    if index + 1 >= len(sequence):
        return None
    return sequence[index + 1]


def validate_java_bff_response(
    phase_payload: dict[str, Any],
    response: dict[str, Any],
    validation_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    bundle_id = str(phase_payload.get("bundle_id") or "")
    phase = str(phase_payload.get("phase") or "")
    if "entry_query_id" in response and response["entry_query_id"] != bundle_id:
        issues.append(issue("ENTRY_QUERY_ID_MISMATCH", "error", "entry_query_id does not match the bundle id.", field="entry_query_id"))
    if "bundle_id" in response and response["bundle_id"] != bundle_id:
        issues.append(issue("BUNDLE_ID_MISMATCH", "error", "bundle_id does not match the phase pack bundle.", field="bundle_id"))
    issues.extend(validate_forbidden_terms(response))
    issues.extend(validate_phase_specific_java_output(phase, response, validation_context or {}))
    return issues


def validate_phase_specific_java_output(
    phase: str,
    response: dict[str, Any],
    validation_context: dict[str, Any],
) -> list[dict[str, Any]]:
    if phase == "phase-1-plan":
        return validate_plan_output(response, validation_context)
    if phase == "phase-2-repository-chunk":
        return validate_repository_chunk_output(response, validation_context)
    if phase == "phase-2-repository-merge":
        return validate_repository_merge_output(response, validation_context)
    if phase == "phase-3-bff-assembly":
        return validate_assembly_output(response, validation_context)
    if phase == "phase-4-verify":
        return validate_verify_output(response, validation_context)
    return []


def validate_plan_output(response: dict[str, Any], validation_context: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    bundle_query_ids = set(validation_context.get("bundle_query_ids", []))
    seen_methods: set[str] = set()
    methods = response.get("repository_methods", [])
    if not isinstance(methods, list) or not methods:
        issues.append(issue("JAVA_PLAN_NO_METHODS", "error", "repository_methods must contain at least one method.", field="repository_methods"))
        return issues
    for index, item in enumerate(methods):
        if not isinstance(item, dict):
            continue
        query_id = str(item.get("query_id") or "")
        method_name = str(item.get("method_name") or "")
        if query_id not in bundle_query_ids:
            issues.append(issue("JAVA_UNKNOWN_QUERY_ID", "error", f"repository_methods[{index}].query_id is not part of the bundle.", field=f"repository_methods[{index}].query_id"))
        if not re.match(JAVA_IDENTIFIER_RE, method_name):
            issues.append(issue("JAVA_METHOD_NAME_INVALID", "error", f"repository_methods[{index}].method_name is not a Java-style identifier.", field=f"repository_methods[{index}].method_name"))
        if method_name in seen_methods:
            issues.append(issue("JAVA_DUPLICATE_METHOD_NAME", "error", f"repository_methods[{index}].method_name is duplicated.", field=f"repository_methods[{index}].method_name"))
        seen_methods.add(method_name)
    return issues


def validate_repository_chunk_output(response: dict[str, Any], validation_context: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    expected_query_id = validation_context.get("query_id")
    expected_chunk_id = validation_context.get("chunk_id")
    if expected_query_id and response.get("query_id") != expected_query_id:
        issues.append(issue("JAVA_QUERY_ID_MISMATCH", "error", "query_id does not match the expected repository chunk query.", field="query_id"))
    if expected_chunk_id and response.get("chunk_id") != expected_chunk_id:
        issues.append(issue("JAVA_CHUNK_ID_MISMATCH", "error", "chunk_id does not match the expected SQL chunk.", field="chunk_id"))
    expected_method_name = (
        validation_context.get("card_payload", {})
        .get("java_bff_logic", {})
        .get("method_name")
    )
    if expected_method_name and response.get("method_name") != expected_method_name:
        issues.append(issue("JAVA_METHOD_NAME_MISMATCH", "error", "method_name does not match the recommended repository method.", field="method_name"))
    parameter_names = {
        str(item.get("parameter_name") or "")
        for item in validation_context.get("card_payload", {}).get("parameters", [])
        if isinstance(item, dict)
    }
    bindings = response.get("parameter_binding", [])
    if parameter_names and isinstance(bindings, list):
        seen_bindings = set()
        for index, item in enumerate(bindings):
            if not isinstance(item, dict):
                continue
            parameter_name = str(item.get("parameter_name") or "")
            java_name = str(item.get("java_argument_name") or "")
            seen_bindings.add(parameter_name)
            if parameter_name not in parameter_names:
                issues.append(issue("JAVA_UNKNOWN_PARAMETER_BINDING", "error", f"parameter_binding[{index}] references an unknown SQL parameter.", field=f"parameter_binding[{index}].parameter_name"))
            if java_name and not re.match(JAVA_IDENTIFIER_RE, java_name):
                issues.append(issue("JAVA_ARGUMENT_NAME_INVALID", "error", f"parameter_binding[{index}].java_argument_name is not a Java-style identifier.", field=f"parameter_binding[{index}].java_argument_name"))
        if parameter_names and not seen_bindings:
            issues.append(issue("JAVA_PARAMETER_BINDING_EMPTY", "error", "parameter_binding is empty even though the SQL chunk exposes parameters.", field="parameter_binding"))
    issues.extend(validate_layer_terms(response, "phase-2-repository-chunk"))
    issues.extend(validate_sql_rewrite_terms(response, ["sql_logic_steps", "carry_forward_context"]))
    return issues


def validate_repository_merge_output(response: dict[str, Any], validation_context: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    expected_query_id = validation_context.get("query_id")
    expected_method_name = (
        validation_context.get("card_payload", {})
        .get("java_bff_logic", {})
        .get("method_name")
    )
    if expected_query_id and response.get("query_id") != expected_query_id:
        issues.append(issue("JAVA_QUERY_ID_MISMATCH", "error", "query_id does not match the expected repository merge query.", field="query_id"))
    if expected_method_name and response.get("method_name") != expected_method_name:
        issues.append(issue("JAVA_METHOD_NAME_MISMATCH", "error", "method_name does not match the recommended repository method.", field="method_name"))
    expected_chunk_ids = [
        str(item.get("parsed_response", {}).get("chunk_id") or "")
        for item in validation_context.get("accepted_chunk_reviews", [])
        if isinstance(item, dict)
    ]
    returned_chunk_ids = [str(item) for item in response.get("sql_chunk_order", []) if isinstance(item, str)]
    if expected_chunk_ids and returned_chunk_ids != expected_chunk_ids:
        issues.append(issue("JAVA_SQL_CHUNK_ORDER_MISMATCH", "error", "sql_chunk_order does not match the accepted repository chunk outputs.", field="sql_chunk_order"))
    if not validation_context.get("accepted_chunk_reviews"):
        issues.append(issue("JAVA_MERGE_INPUTS_MISSING", "error", "Accepted repository chunk outputs are missing for this query.", field="sql_chunk_order"))
    issues.extend(validate_layer_terms(response, "phase-2-repository-merge"))
    issues.extend(validate_sql_rewrite_terms(response, ["repository_logic", "parameter_contract"]))
    return issues


def validate_assembly_output(response: dict[str, Any], validation_context: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    if not validation_context.get("accepted_plan_review"):
        issues.append(issue("JAVA_ASSEMBLY_PLAN_MISSING", "error", "Accepted phase-1 plan output is missing.", field="service_logic"))
    missing_merges = [
        query_id
        for query_id, review in validation_context.get("accepted_merge_reviews", {}).items()
        if not review
    ]
    if missing_merges:
        issues.append(issue("JAVA_ASSEMBLY_REPOSITORY_MERGES_MISSING", "error", f"Accepted repository merge outputs are missing for: {', '.join(missing_merges)}.", field="service_logic"))
    dto_hints = response.get("dto_contract_hints", [])
    if not isinstance(dto_hints, list) or not dto_hints:
        issues.append(issue("JAVA_DTO_HINTS_EMPTY", "warning", "dto_contract_hints is empty; the BFF DTO contract is likely underspecified.", field="dto_contract_hints"))
    issues.extend(validate_layer_terms(response, "phase-3-bff-assembly"))
    issues.extend(validate_sql_rewrite_terms(response, ["service_logic", "controller_logic"]))
    return issues


def validate_verify_output(response: dict[str, Any], validation_context: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    verdict = str(response.get("verdict") or "")
    token_budget = response.get("token_budget_check", {})
    missing_artifacts = response.get("missing_artifacts", [])
    guess_risks = response.get("guess_risks", [])
    if verdict == "ready":
        if isinstance(token_budget, dict) and not bool(token_budget.get("within_limit")):
            issues.append(issue("JAVA_VERIFY_READY_OVER_BUDGET", "error", "verdict is ready even though token_budget_check.within_limit is false.", field="token_budget_check.within_limit"))
        if isinstance(missing_artifacts, list) and missing_artifacts:
            issues.append(issue("JAVA_VERIFY_READY_WITH_MISSING_ARTIFACTS", "error", "verdict is ready even though missing_artifacts is not empty.", field="missing_artifacts"))
        if isinstance(guess_risks, list) and guess_risks:
            issues.append(issue("JAVA_VERIFY_READY_WITH_GUESS_RISKS", "error", "verdict is ready even though guess_risks is not empty.", field="guess_risks"))
    if not validation_context.get("accepted_assembly_review"):
        issues.append(issue("JAVA_VERIFY_ASSEMBLY_MISSING", "error", "Accepted phase-3 assembly output is missing.", field="missing_artifacts"))
    return issues


def validate_layer_terms(response: dict[str, Any], phase: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    lowered = json.dumps(response, ensure_ascii=False).lower()
    for term in JAVA_LAYER_LEAK_TERMS.get(phase, []):
        if term in lowered:
            issues.append(issue("JAVA_LAYER_LEAK", "error", f"The response includes `{term}`, which belongs to the wrong implementation layer for {phase}."))
    return issues


def validate_forbidden_terms(response: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    lowered = json.dumps(response, ensure_ascii=False).lower()
    for term in JAVA_FORBIDDEN_TERMS:
        if term in lowered:
            issues.append(issue("JAVA_FORBIDDEN_TERM", "error", f"The response includes forbidden implementation drift term `{term}`."))
    return issues


def validate_sql_rewrite_terms(response: dict[str, Any], fields: list[str]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for field in fields:
        values = response.get(field)
        if not isinstance(values, list):
            continue
        for index, item in enumerate(values):
            text = str(item).lower()
            if "rewrite" in text and "sql" in text:
                issues.append(issue("JAVA_SQL_REWRITE_RISK", "error", f"{field}[{index}] suggests rewriting SQL instead of preserving the analyzed Oracle 19c query.", field=f"{field}[{index}]"))
            if "add join" in text or "new table" in text:
                issues.append(issue("JAVA_SQL_SHAPE_DRIFT", "error", f"{field}[{index}] suggests changing the SQL shape beyond the analyzed artifacts.", field=f"{field}[{index}]"))
    return issues


def phase_query_id_from_payload(phase_payload: dict[str, Any]) -> str | None:
    schema = phase_payload.get("answer_schema", {})
    if not isinstance(schema, dict):
        return None
    value = schema.get("query_id")
    if isinstance(value, str) and value.strip() and value.strip().lower() != "string":
        return value
    return None


def phase_chunk_id_from_payload(phase_payload: dict[str, Any]) -> str | None:
    schema = phase_payload.get("answer_schema", {})
    if not isinstance(schema, dict):
        return None
    value = schema.get("chunk_id")
    if isinstance(value, str) and value.strip() and value.strip().lower() != "string":
        return value
    return None


def load_latest_review(java_root: Path, bundle_id: str, phase: str, query_id: str | None = None) -> dict[str, Any] | None:
    reviews = load_reviews_for_phase(java_root, bundle_id, phase, query_id=query_id)
    return reviews[-1] if reviews else None


def load_reviews_for_phase(java_root: Path, bundle_id: str, phase: str, query_id: str | None = None) -> list[dict[str, Any]]:
    review_root = java_root / "reviews" / bundle_slug(bundle_id)
    if not review_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(review_root.glob("*-review.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue
        if payload.get("status") not in {"accepted", "insufficient_evidence"}:
            continue
        if payload.get("phase") != phase:
            continue
        parsed = payload.get("parsed_response")
        if query_id and isinstance(parsed, dict) and str(parsed.get("query_id") or "") != query_id:
            continue
        payload["__path"] = str(path.resolve())
        rows.append(payload)
    return rows


def validate_against_schema(schema: Any, payload: Any, path: str = "") -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    field_path = path or "<root>"
    if isinstance(schema, dict):
        if not isinstance(payload, dict):
            return [issue("TYPE_MISMATCH", "error", f"{field_path} must be an object.", field=path or None)]
        for key, value in schema.items():
            next_path = f"{path}.{key}" if path else key
            if key not in payload:
                issues.append(issue("MISSING_FIELD", "error", f"Missing required field '{next_path}'.", field=next_path))
                continue
            issues.extend(validate_against_schema(value, payload[key], next_path))
        return issues
    if isinstance(schema, list):
        if not isinstance(payload, list):
            return [issue("TYPE_MISMATCH", "error", f"{field_path} must be a list.", field=path or None)]
        if schema:
            for index, item in enumerate(payload[:5]):
                issues.extend(validate_against_schema(schema[0], item, f"{path}[{index}]"))
        return issues
    if isinstance(schema, bool):
        if not isinstance(payload, bool):
            issues.append(issue("TYPE_MISMATCH", "error", f"{field_path} must be a boolean.", field=path or None))
        return issues
    if isinstance(schema, str):
        issues.extend(validate_string_schema(schema, payload, path))
        return issues
    return issues


def validate_string_schema(schema_value: str, payload: Any, path: str) -> list[dict[str, Any]]:
    field = path or None
    if not isinstance(payload, str):
        return [issue("TYPE_MISMATCH", "error", f"{path or '<root>'} must be a string.", field=field)]
    stripped = schema_value.strip()
    placeholder_values = {"string"}
    if stripped in placeholder_values:
        return []
    if "|" in stripped:
        allowed = {part.strip() for part in stripped.split("|")}
        if payload not in allowed:
            return [issue("ENUM_MISMATCH", "error", f"{path or '<root>'} must be one of {', '.join(sorted(allowed))}.", field=field)]
        return []
    if payload != schema_value:
        return [issue("VALUE_MISMATCH", "error", f"{path or '<root>'} must equal '{schema_value}'.", field=field)]
    return []


def render_java_bff_repair_prompt(phase_payload: dict[str, Any], raw_text: str, issues: list[dict[str, Any]]) -> str:
    lines = [
        f"Phase: {phase_payload['phase']}",
        f"Bundle: {phase_payload['bundle_id']}",
        "Your previous Java BFF phase output was invalid.",
        "Return JSON only and fix these issues:",
    ]
    for item in issues:
        lines.append(f"- {item['code']}: {item['message']}")
    lines.extend(
        [
            "",
            "Expected schema:",
            json.dumps(phase_payload["answer_schema"], indent=2, ensure_ascii=False),
            "",
            "Previous response:",
            raw_text,
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_java_bff_review_markdown(review: dict[str, Any]) -> str:
    lines = [
        "# Java BFF Review",
        "",
        f"- Bundle: `{review['bundle_id']}`",
        f"- Phase: `{review['phase']}`",
        f"- Status: `{review['status']}`",
        f"- Phase pack: `{review['phase_pack_path']}`",
        "",
        "## Issues",
    ]
    if review["issues"]:
        for item in review["issues"]:
            lines.append(f"- `{item['code']}` {item['severity']}: {item['message']}")
    else:
        lines.append("- None")
    if review.get("next_phase_pack_path"):
        lines.extend(["", "## Next Phase", f"- `{review['next_phase_pack_path']}`"])
    return "\n".join(lines).rstrip() + "\n"


def render_java_bff_merged_markdown(payload: dict[str, Any]) -> str:
    completion = payload["completion"]
    lines = [
        "# Java BFF Implementation Plan",
        "",
        f"- Bundle: `{payload['bundle_id']}`",
        f"- Accepted prompts: {completion['accepted_prompt_count']} / {completion['total_prompt_count']}",
        f"- Ready for skeletons: `{completion['ready_for_skeletons']}`",
        "",
        "## Missing Prompts",
    ]
    if completion["missing_prompts"]:
        for item in completion["missing_prompts"]:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    lines.extend(["", "## Repository Merge Outputs"])
    if payload["repository_merge_outputs"]:
        for query_id in sorted(payload["repository_merge_outputs"]):
            lines.append(f"- `{query_id}`")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def append_java_bff_run_index(run_root: Path, summary_path: Path, run_summary: dict[str, Any]) -> None:
    run_root.mkdir(parents=True, exist_ok=True)
    index_path = run_root / "index.json"
    payload = {"runs": []}
    if index_path.exists():
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"runs": []}
    payload.setdefault("runs", [])
    payload["runs"].append(run_summary | {"summary_path": str(summary_path.resolve())})
    index_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def append_java_bff_task_index(tasks_root: Path, task_payload: dict[str, Any]) -> None:
    tasks_root.mkdir(parents=True, exist_ok=True)
    index_path = tasks_root / "index.json"
    payload = {"tasks": []}
    if index_path.exists():
        try:
            payload = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"tasks": []}
    payload.setdefault("tasks", [])
    payload["tasks"].append(task_payload)
    index_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def artifact_descriptor_for_path(path: Path, kind: str, title: str, scope: str) -> ArtifactDescriptor:
    content = path.read_text(encoding="utf-8")
    estimated = estimate_tokens(content)
    return ArtifactDescriptor(
        kind=kind,
        path=str(path.resolve()),
        title=title,
        estimated_tokens=estimated,
        safe_for_128k_single_pass=estimated <= 100_000,
        needs_selective_prompting=estimated > 40_000,
        scope=scope,
    )


class JavaBffProviderRunner:
    def __init__(self, provider_kwargs: dict[str, Any]) -> None:
        self.provider_kwargs = provider_kwargs

    def run_phase_pack(self, analysis_root: Path, phase_pack_path: Path) -> dict[str, Any]:
        return invoke_java_bff_phase_pack(
            analysis_root=analysis_root,
            phase_pack_path=phase_pack_path,
            review=False,
            **self.provider_kwargs,
        )


class JavaBffClineBridgeRunner:
    def __init__(self, cline_bridge_command: str | None = None) -> None:
        self.cline_bridge_command = cline_bridge_command

    def run_phase_pack(self, analysis_root: Path, phase_pack_path: Path) -> dict[str, Any]:
        phase_payload = load_phase_pack_payload(phase_pack_path)
        context_pack, context_paths, context_artifacts = ensure_java_bff_context_artifacts(
            analysis_root=analysis_root,
            phase_pack_path=phase_pack_path,
            prompt_profile=str(phase_payload.get("prompt_profile") or "qwen3-128k-java-bff"),
        )
        bundle_id = str(phase_payload["bundle_id"])
        java_root = resolve_java_bff_root(analysis_root)
        tasks_root = java_root / "tasks" / bundle_slug(bundle_id)
        tasks_root.mkdir(parents=True, exist_ok=True)
        task_path = tasks_root / f"{safe_name(phase_pack_path.stem)}.json"
        result_path = java_root / "agent_runs" / bundle_slug(bundle_id) / f"{safe_name(phase_pack_path.stem)}.result.json"
        task_payload = {
            "task_contract_version": "java-bff-task-v1",
            "task_id": f"{bundle_slug(bundle_id)}:{phase_payload['phase']}:{safe_name(phase_pack_path.stem)}",
            "runner_mode": "cline_bridge",
            "bundle_id": bundle_id,
            "phase": phase_payload["phase"],
            "phase_pack_path": str(phase_pack_path.resolve()),
            "context_pack_path": str(context_paths[0].resolve()),
            "context_prompt_path": str(context_paths[1].resolve()),
            "expected_schema": phase_payload.get("answer_schema", {}),
            "token_budget": context_pack["budget"],
            "missing_inputs": context_pack["missing_inputs"],
            "recommended_result_path": str(result_path.resolve()),
        }
        task_path.write_text(json.dumps(task_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        append_java_bff_task_index(java_root / "tasks", task_payload)
        if self.cline_bridge_command:
            subprocess.run(self.cline_bridge_command, shell=True, check=True, cwd=str(analysis_root.parent))
        if not result_path.exists():
            raise FileNotFoundError(f"Cline bridge did not write Java BFF result file {result_path}.")
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        response_text_path = Path(str(payload["response_text_path"]))
        task_artifacts = [
            *context_artifacts,
            artifact_descriptor_for_path(task_path, "json", f"Java BFF bridge task: {phase_pack_path.stem}", "java_bff"),
            artifact_descriptor_for_path(result_path, "json", f"Java BFF bridge result: {phase_pack_path.stem}", "java_bff"),
        ]
        append_artifacts_to_index(java_root.parent.parent, task_artifacts)
        return {
            "run_summary": {
                "bundle_id": bundle_id,
                "phase": phase_payload["phase"],
                "phase_pack_path": str(phase_pack_path.resolve()),
                "task_path": str(task_path.resolve()),
                "context_pack_path": str(context_paths[0].resolve()),
            },
            "response_text": response_text_path.read_text(encoding="utf-8"),
            "response_text_path": str(response_text_path.resolve()),
            "response_payload": payload,
            "result_path": str(result_path.resolve()),
            "task_path": str(task_path.resolve()),
            "context_pack_path": str(context_paths[0].resolve()),
            "artifacts": task_artifacts,
        }


class JavaBffFakeRunner:
    def __init__(self, responses: dict[str, Any], output_dir: Path | None = None) -> None:
        self.responses = responses
        self.output_dir = output_dir

    def run_phase_pack(self, analysis_root: Path, phase_pack_path: Path) -> dict[str, Any]:
        key = str(phase_pack_path.resolve())
        phase_payload = load_phase_pack_payload(phase_pack_path)
        context_pack, context_paths, context_artifacts = ensure_java_bff_context_artifacts(
            analysis_root=analysis_root,
            phase_pack_path=phase_pack_path,
            prompt_profile=str(phase_payload.get("prompt_profile") or "qwen3-128k-java-bff"),
        )
        response = self.responses.get(key)
        if response is None:
            response = self.responses.get(str(phase_pack_path))
        if response is None:
            response = self.responses.get(phase_pack_path.name)
        if response is None:
            response = self.responses.get(phase_pack_path.stem)
        if response is None:
            response = self.responses.get(str(phase_payload.get("phase") or ""))
        if response is None:
            raise KeyError(f"No fake Java BFF response configured for {key}")
        if isinstance(response, dict):
            text = json.dumps(response, ensure_ascii=False)
        else:
            text = str(response)
        java_root = resolve_java_bff_root(analysis_root)
        bundle_id = str(phase_payload["bundle_id"])
        run_root = java_root / "agent_runs" / bundle_slug(bundle_id)
        run_root.mkdir(parents=True, exist_ok=True)
        response_text_path = run_root / f"{safe_name(phase_pack_path.stem)}.response.txt"
        response_text_path.write_text(text, encoding="utf-8")
        result_payload = {
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "bundle_id": bundle_id,
            "phase": phase_payload["phase"],
            "phase_pack_path": str(phase_pack_path.resolve()),
            "context_pack_path": str(context_paths[0].resolve()),
            "response_text_path": str(response_text_path.resolve()),
        }
        result_path = run_root / f"{safe_name(phase_pack_path.stem)}.result.json"
        result_path.write_text(json.dumps(result_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts = [
            *context_artifacts,
            artifact_descriptor_for_path(result_path, "json", f"Java BFF fake result: {phase_pack_path.stem}", "java_bff"),
        ]
        append_artifacts_to_index(java_root.parent.parent, artifacts)
        return {
            "run_summary": result_payload,
            "response_text": text,
            "response_text_path": str(response_text_path.resolve()),
            "response_payload": result_payload,
            "result_path": str(result_path.resolve()),
            "context_pack_path": str(context_paths[0].resolve()),
            "artifacts": artifacts,
        }
