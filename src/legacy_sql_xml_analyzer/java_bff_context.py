from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .java_bff import (
    bundle_slug,
    estimate_tokens,
    load_bundle_payload,
    load_phase_pack_payload,
    resolve_java_bff_root,
    safe_name,
)
from .prompt_profiles import phase_budget_for
from .prompting import resolve_analysis_root


CONTEXT_VERSION = "java-bff-context-v1"
PHASE_MISSING_FIELD = {
    "phase-1-plan": "open_questions",
    "phase-2-repository-chunk": "manual_review_flags",
    "phase-2-repository-merge": "manual_review_flags",
    "phase-3-bff-assembly": "follow_up_questions",
    "phase-4-verify": "missing_artifacts",
}


def compile_java_bff_context_pack(
    analysis_root: Path,
    phase_pack_path: Path,
    prompt_profile: str | None = None,
    max_input_tokens: int | None = None,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    java_root = resolve_java_bff_root(analysis_root)
    phase_pack_path = phase_pack_path.resolve()
    phase_payload = load_phase_pack_payload(phase_pack_path)
    phase = str(phase_payload["phase"])
    bundle_id = str(phase_payload["bundle_id"])
    bundle = load_bundle_payload(analysis_root, bundle_id)
    profile = prompt_profile or str(phase_payload.get("prompt_profile") or "qwen3-128k-java-bff")
    budget = phase_budget_for(profile, phase)
    usable_input_limit = min(int(max_input_tokens), budget["usable_input_limit"]) if max_input_tokens else budget["usable_input_limit"]

    sections, missing_inputs = build_context_sections(
        analysis_root=analysis_root,
        java_root=java_root,
        phase_payload=phase_payload,
        bundle=bundle,
        phase_pack_path=phase_pack_path,
    )
    selected_sections = trim_sections_for_budget(sections, usable_input_limit, reserve_tokens=2200)
    prompt_text = render_context_prompt(
        phase_payload=phase_payload,
        bundle=bundle,
        prompt_profile=profile,
        sections=selected_sections,
        missing_inputs=missing_inputs,
    )
    estimated_tokens = estimate_tokens(prompt_text)

    if estimated_tokens > usable_input_limit:
        selected_sections = trim_sections_for_budget(sections, usable_input_limit, reserve_tokens=3200)
        prompt_text = render_context_prompt(
            phase_payload=phase_payload,
            bundle=bundle,
            prompt_profile=profile,
            sections=selected_sections,
            missing_inputs=missing_inputs,
        )
        estimated_tokens = estimate_tokens(prompt_text)

    return {
        "context_contract_version": CONTEXT_VERSION,
        "generated_at": phase_payload.get("generated_at"),
        "phase": phase,
        "bundle_id": bundle_id,
        "bundle_slug": bundle_slug(bundle_id),
        "phase_pack_path": str(phase_pack_path),
        "prompt_profile": profile,
        "budget": {
            **budget,
            "usable_input_limit": usable_input_limit,
        },
        "estimated_prompt_tokens": estimated_tokens,
        "safe_for_qwen3": estimated_tokens <= usable_input_limit,
        "included_artifacts": collect_included_artifacts(selected_sections),
        "missing_inputs": missing_inputs,
        "phase_focus": phase_focus_for_context(phase_payload),
        "sections": selected_sections,
        "answer_schema": phase_payload.get("answer_schema", {}),
        "prompt_text": prompt_text,
    }


def write_java_bff_context_pack(analysis_root: Path, pack: dict[str, Any]) -> list[Path]:
    analysis_root = resolve_analysis_root(analysis_root)
    root = resolve_java_bff_root(analysis_root) / "context_packs" / str(pack["bundle_slug"])
    root.mkdir(parents=True, exist_ok=True)
    stem = safe_name(Path(str(pack["phase_pack_path"])).stem)
    json_path = root / f"{stem}.json"
    txt_path = root / f"{stem}.txt"
    md_path = root / f"{stem}.md"
    json_path.write_text(json.dumps(pack, indent=2, ensure_ascii=False), encoding="utf-8")
    txt_path.write_text(str(pack["prompt_text"]), encoding="utf-8")
    md_path.write_text(render_context_pack_markdown(pack), encoding="utf-8")
    return [json_path, txt_path, md_path]


def context_pack_paths(analysis_root: Path, phase_pack_path: Path) -> list[Path]:
    analysis_root = resolve_analysis_root(analysis_root)
    phase_payload = load_phase_pack_payload(phase_pack_path)
    root = resolve_java_bff_root(analysis_root) / "context_packs" / bundle_slug(str(phase_payload["bundle_id"]))
    stem = safe_name(Path(str(phase_pack_path)).stem)
    return [root / f"{stem}.json", root / f"{stem}.txt", root / f"{stem}.md"]


def build_context_sections(
    analysis_root: Path,
    java_root: Path,
    phase_payload: dict[str, Any],
    bundle: dict[str, Any],
    phase_pack_path: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    phase = str(phase_payload["phase"])
    bundle_id = str(bundle["bundle_id"])
    sections: list[dict[str, Any]] = [
        {
            "kind": "bundle_summary",
            "title": "Bundle Summary",
            "required": True,
            "text": render_bundle_summary(bundle),
            "artifacts": [str((java_root / "bundles" / bundle_slug(bundle_id) / "bundle.json").resolve())],
        },
        {
            "kind": "phase_contract",
            "title": "Phase Contract",
            "required": True,
            "text": render_phase_contract(phase_payload),
            "artifacts": [str(phase_pack_path.resolve())],
        },
    ]
    missing_inputs: list[str] = []
    query_id = phase_query_id(phase_payload)
    query_card = load_query_card(java_root, query_id) if query_id else None
    if query_card is not None:
        sections.append(
            {
                "kind": "query_card",
                "title": "Query Card",
                "required": phase in {"phase-2-repository-chunk", "phase-2-repository-merge"},
                "text": render_query_card_summary(query_card),
                "artifacts": [str(query_card["__path"])],
            }
        )

    if phase == "phase-1-plan":
        sections.extend(build_plan_sections(bundle, java_root))
    elif phase == "phase-2-repository-chunk":
        sections.extend(build_repository_chunk_sections(phase_payload, query_card, java_root, missing_inputs))
    elif phase == "phase-2-repository-merge":
        phase_sections, phase_missing = build_repository_merge_sections(analysis_root, bundle, phase_payload, java_root)
        sections.extend(phase_sections)
        missing_inputs.extend(phase_missing)
    elif phase == "phase-3-bff-assembly":
        phase_sections, phase_missing = build_assembly_sections(analysis_root, bundle, java_root)
        sections.extend(phase_sections)
        missing_inputs.extend(phase_missing)
    elif phase == "phase-4-verify":
        phase_sections, phase_missing = build_verify_sections(analysis_root, bundle, java_root)
        sections.extend(phase_sections)
        missing_inputs.extend(phase_missing)
    return sections, missing_inputs


def build_plan_sections(bundle: dict[str, Any], java_root: Path) -> list[dict[str, Any]]:
    lines = []
    artifacts: list[str] = []
    for query in bundle.get("queries", [])[:8]:
        if not isinstance(query, dict):
            continue
        lines.append(
            f"- {query['query_id']} status={query['status']} chunks={query['chunk_count']} "
            f"statement={query['statement_type']} summary={query['summary']}"
        )
        card_path = Path(str(query.get("card_json_path") or ""))
        if card_path.exists():
            artifacts.append(str(card_path.resolve()))
    return [
        {
            "kind": "query_summaries",
            "title": "Bundle Query Summaries",
            "required": True,
            "text": "\n".join(lines) if lines else "- None",
            "artifacts": artifacts,
        }
    ]


def build_repository_chunk_sections(
    phase_payload: dict[str, Any],
    query_card: dict[str, Any] | None,
    java_root: Path,
    missing_inputs: list[str],
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = []
    chunk_id = phase_chunk_id(phase_payload)
    if not chunk_id:
        missing_inputs.append("Missing chunk_id in phase payload.")
        return sections
    chunk_path = java_root / "sql_chunks" / f"{safe_name(chunk_id)}.json"
    if not chunk_path.exists():
        missing_inputs.append(f"Missing SQL chunk artifact for {chunk_id}.")
        return sections
    chunk_payload = json.loads(chunk_path.read_text(encoding="utf-8"))
    sections.append(
        {
            "kind": "sql_chunk",
            "title": "SQL Chunk",
            "required": True,
            "text": render_sql_chunk_summary(chunk_payload),
            "artifacts": [str(chunk_path.resolve())],
        }
    )
    plan_review = load_latest_review(java_root, str(phase_payload["bundle_id"]), "phase-1-plan")
    if plan_review is not None:
        sections.append(
            {
                "kind": "accepted_plan",
                "title": "Accepted Phase-1 Plan",
                "required": False,
                "text": render_plan_output_summary(plan_review.get("parsed_response")),
                "artifacts": [str(plan_review["__path"])],
            }
        )
    if query_card is not None and query_card.get("java_bff_logic", {}).get("manual_review_flags"):
        sections.append(
            {
                "kind": "manual_flags",
                "title": "Manual Review Flags",
                "required": False,
                "text": "\n".join(
                    f"- {item}" for item in query_card.get("java_bff_logic", {}).get("manual_review_flags", [])
                ),
                "artifacts": [str(query_card["__path"])],
            }
        )
    return sections


def build_repository_merge_sections(
    analysis_root: Path,
    bundle: dict[str, Any],
    phase_payload: dict[str, Any],
    java_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    query_id = phase_query_id(phase_payload)
    if not query_id:
        return [], ["Missing query_id for repository merge phase."]
    sections: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    accepted_chunks = load_reviews_for_phase(java_root, str(bundle["bundle_id"]), "phase-2-repository-chunk", query_id=query_id)
    if accepted_chunks:
        sections.append(
            {
                "kind": "accepted_chunk_outputs",
                "title": "Accepted Repository Chunk Outputs",
                "required": True,
                "text": render_chunk_review_summaries(accepted_chunks),
                "artifacts": [str(item["__path"]) for item in accepted_chunks],
            }
        )
    else:
        missing_inputs.append(f"Accepted phase-2 chunk outputs are missing for {query_id}.")
    plan_review = load_latest_review(java_root, str(bundle["bundle_id"]), "phase-1-plan")
    if plan_review is not None:
        sections.append(
            {
                "kind": "accepted_plan",
                "title": "Accepted Phase-1 Plan",
                "required": False,
                "text": render_plan_output_summary(plan_review.get("parsed_response")),
                "artifacts": [str(plan_review["__path"])],
            }
        )
    return sections, missing_inputs


def build_assembly_sections(
    analysis_root: Path,
    bundle: dict[str, Any],
    java_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    sections: list[dict[str, Any]] = []
    missing_inputs: list[str] = []
    plan_review = load_latest_review(java_root, str(bundle["bundle_id"]), "phase-1-plan")
    if plan_review is not None:
        sections.append(
            {
                "kind": "accepted_plan",
                "title": "Accepted Phase-1 Plan",
                "required": True,
                "text": render_plan_output_summary(plan_review.get("parsed_response")),
                "artifacts": [str(plan_review["__path"])],
            }
        )
    else:
        missing_inputs.append("Accepted phase-1 plan output is missing.")

    merge_reviews: list[dict[str, Any]] = []
    for query in bundle.get("queries", []):
        if not isinstance(query, dict):
            continue
        query_id = str(query.get("query_id") or "")
        review = load_latest_review(java_root, str(bundle["bundle_id"]), "phase-2-repository-merge", query_id=query_id)
        if review is None:
            missing_inputs.append(f"Accepted repository merge output is missing for {query_id}.")
            continue
        merge_reviews.append(review)
    if merge_reviews:
        sections.append(
            {
                "kind": "accepted_repository_merges",
                "title": "Accepted Repository Merge Outputs",
                "required": True,
                "text": render_merge_review_summaries(merge_reviews),
                "artifacts": [str(item["__path"]) for item in merge_reviews],
            }
        )
    return sections, missing_inputs


def build_verify_sections(
    analysis_root: Path,
    bundle: dict[str, Any],
    java_root: Path,
) -> tuple[list[dict[str, Any]], list[str]]:
    sections, missing_inputs = build_assembly_sections(analysis_root, bundle, java_root)
    assembly_review = load_latest_review(java_root, str(bundle["bundle_id"]), "phase-3-bff-assembly")
    if assembly_review is not None:
        sections.append(
            {
                "kind": "accepted_assembly",
                "title": "Accepted BFF Assembly Output",
                "required": True,
                "text": render_assembly_output_summary(assembly_review.get("parsed_response")),
                "artifacts": [str(assembly_review["__path"])],
            }
        )
    else:
        missing_inputs.append("Accepted phase-3 assembly output is missing.")
    return sections, missing_inputs


def trim_sections_for_budget(sections: list[dict[str, Any]], max_tokens: int, reserve_tokens: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    used = 0
    for section in [item for item in sections if item.get("required")]:
        tokens = estimate_tokens(str(section.get("text") or ""))
        selected.append(section)
        used += tokens
    for section in [item for item in sections if not item.get("required")]:
        tokens = estimate_tokens(str(section.get("text") or ""))
        if used + tokens + reserve_tokens > max_tokens:
            continue
        selected.append(section)
        used += tokens
    return selected


def collect_included_artifacts(sections: list[dict[str, Any]]) -> list[str]:
    included: list[str] = []
    for section in sections:
        for artifact in section.get("artifacts", []):
            if artifact not in included:
                included.append(artifact)
    return included


def render_context_prompt(
    phase_payload: dict[str, Any],
    bundle: dict[str, Any],
    prompt_profile: str,
    sections: list[dict[str, Any]],
    missing_inputs: list[str],
) -> str:
    phase = str(phase_payload["phase"])
    missing_field = PHASE_MISSING_FIELD.get(phase, "manual_review_flags")
    lines = [
        "You are generating implementation logic for a Java Spring Boot BFF API backed by Oracle 19c SQL.",
        f"Prompt profile: {prompt_profile}",
        f"Phase: {phase}",
        f"Bundle id: {bundle['bundle_id']}",
        "",
        "Allowed actions:",
        "- Summarize only the evidence shown below.",
        "- Preserve Oracle 19c SQL semantics and placeholder names.",
        "- Keep repository, service, controller, and DTO responsibilities separated.",
        "",
        "Forbidden actions:",
        "- Do not invent missing SQL clauses, tables, joins, or parameters.",
        "- Do not switch to JPA, Hibernate, EntityManager, or ORM entities.",
        "- Do not rewrite the Oracle SQL into a different query shape.",
        "- Do not return prose outside the required JSON object.",
    ]
    if missing_inputs:
        lines.extend(
            [
                "",
                "Missing inputs detected:",
                *[f"- {item}" for item in missing_inputs],
                "",
                f"If those missing inputs block the phase, do not guess. Reflect the blockers in `{missing_field}`.",
            ]
        )
        if phase == "phase-4-verify":
            lines.append("If blockers remain, use `needs_more_context` instead of `ready`.")
    lines.extend(["", "Evidence:"])
    for section in sections:
        lines.append(f"## {section['title']}")
        lines.append(str(section["text"]).rstrip())
        lines.append("")
    lines.extend(
        [
            "Return JSON only with this schema:",
            json.dumps(phase_payload.get("answer_schema", {}), indent=2, ensure_ascii=False),
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_context_pack_markdown(pack: dict[str, Any]) -> str:
    lines = [
        "# Java BFF Context Pack",
        "",
        f"- Contract version: `{pack['context_contract_version']}`",
        f"- Bundle: `{pack['bundle_id']}`",
        f"- Phase: `{pack['phase']}`",
        f"- Prompt profile: `{pack['prompt_profile']}`",
        f"- Estimated prompt tokens: {pack['estimated_prompt_tokens']}",
        f"- Budget limit: {pack['budget']['usable_input_limit']}",
        f"- Safe for qwen3: `{pack['safe_for_qwen3']}`",
        "",
        "## Included Artifacts",
    ]
    if pack["included_artifacts"]:
        for item in pack["included_artifacts"]:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    if pack["missing_inputs"]:
        lines.extend(["", "## Missing Inputs"])
        for item in pack["missing_inputs"]:
            lines.append(f"- {item}")
    lines.extend(["", "## Sections"])
    for section in pack["sections"]:
        lines.append(f"- `{section['kind']}`")
    return "\n".join(lines).rstrip() + "\n"


def render_bundle_summary(bundle: dict[str, Any]) -> str:
    query_lines = []
    for item in bundle.get("queries", [])[:10]:
        if not isinstance(item, dict):
            continue
        query_lines.append(
            f"- {item['query_id']} status={item['status']} chunks={item['chunk_count']} statement={item['statement_type']}"
        )
    return "\n".join(
        [
            f"entry_query_id: {bundle.get('entry_query_id')}",
            f"entry_query_name: {bundle.get('entry_query_name')}",
            f"query_count: {bundle.get('query_count')}",
            "queries:",
            *(query_lines or ["- None"]),
        ]
    )


def render_phase_contract(phase_payload: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"phase: {phase_payload['phase']}",
            f"bundle_id: {phase_payload['bundle_id']}",
            f"prompt_profile: {phase_payload.get('prompt_profile')}",
            f"estimated_prompt_tokens: {phase_payload.get('estimated_prompt_tokens')}",
            "recommended_input_artifacts:",
            *[f"- {item}" for item in phase_payload.get("recommended_input_artifacts", [])],
        ]
    )


def render_query_card_summary(card: dict[str, Any]) -> str:
    parameters = [item.get("parameter_name") for item in card.get("parameters", []) if isinstance(item, dict)]
    flags = card.get("java_bff_logic", {}).get("manual_review_flags", [])
    return "\n".join(
        [
            f"query_id: {card.get('query_id')}",
            f"status: {card.get('status')}",
            f"statement_type: {card.get('sql_logic', {}).get('statement_type')}",
            f"recommended_method_name: {card.get('java_bff_logic', {}).get('method_name')}",
            f"parameters: {', '.join(item for item in parameters if item) or 'none'}",
            f"oracle_features: {', '.join(card.get('sql_logic', {}).get('oracle_features', [])) or 'none'}",
            "manual_review_flags:",
            *([f"- {item}" for item in flags] if flags else ["- None"]),
        ]
    )


def render_sql_chunk_summary(chunk: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"chunk_id: {chunk.get('chunk_id')}",
            f"query_id: {chunk.get('query_id')}",
            f"sequence: {chunk.get('sequence')}",
            f"estimated_tokens: {chunk.get('estimated_tokens')}",
            f"line_range: {chunk.get('start_line')}-{chunk.get('end_line')}",
            f"clause_hints: {', '.join(chunk.get('clause_hints', [])) or 'none'}",
            "sql_excerpt:",
            "```sql",
            str(chunk.get("sql_excerpt") or "").strip(),
            "```",
        ]
    )


def render_plan_output_summary(parsed: Any) -> str:
    if not isinstance(parsed, dict):
        return "- Missing accepted phase-1 plan."
    methods = parsed.get("repository_methods", [])
    lines = ["repository_methods:"]
    if isinstance(methods, list) and methods:
        for item in methods[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- {item.get('query_id')} -> {item.get('method_name')} purpose={item.get('purpose')}"
            )
    else:
        lines.append("- None")
    lines.append("service_flow:")
    for item in parsed.get("service_flow", [])[:6] if isinstance(parsed.get("service_flow"), list) else []:
        lines.append(f"- {item}")
    lines.append("controller_contract_hints:")
    for item in parsed.get("controller_contract_hints", [])[:4] if isinstance(parsed.get("controller_contract_hints"), list) else []:
        lines.append(f"- {item}")
    return "\n".join(lines)


def render_chunk_review_summaries(reviews: list[dict[str, Any]]) -> str:
    lines = []
    for review in reviews:
        parsed = review.get("parsed_response")
        if not isinstance(parsed, dict):
            continue
        lines.append(f"- chunk_id={parsed.get('chunk_id')} method={parsed.get('method_name')}")
        binding = parsed.get("parameter_binding", [])
        if isinstance(binding, list) and binding:
            for item in binding[:6]:
                if isinstance(item, dict):
                    lines.append(
                        f"  bind {item.get('parameter_name')} -> {item.get('java_argument_name')}: {item.get('binding_note')}"
                    )
        for item in parsed.get("sql_logic_steps", [])[:4] if isinstance(parsed.get("sql_logic_steps"), list) else []:
            lines.append(f"  logic: {item}")
    return "\n".join(lines) if lines else "- None"


def render_merge_review_summaries(reviews: list[dict[str, Any]]) -> str:
    lines = []
    for review in reviews:
        parsed = review.get("parsed_response")
        if not isinstance(parsed, dict):
            continue
        lines.append(f"- query_id={parsed.get('query_id')} method={parsed.get('method_name')}")
        for item in parsed.get("repository_logic", [])[:4] if isinstance(parsed.get("repository_logic"), list) else []:
            lines.append(f"  repository_logic: {item}")
        for item in parsed.get("parameter_contract", [])[:4] if isinstance(parsed.get("parameter_contract"), list) else []:
            lines.append(f"  parameter_contract: {item}")
    return "\n".join(lines) if lines else "- None"


def render_assembly_output_summary(parsed: Any) -> str:
    if not isinstance(parsed, dict):
        return "- Missing accepted assembly output."
    lines = ["service_logic:"]
    for item in parsed.get("service_logic", [])[:6] if isinstance(parsed.get("service_logic"), list) else []:
        lines.append(f"- {item}")
    lines.append("controller_logic:")
    for item in parsed.get("controller_logic", [])[:4] if isinstance(parsed.get("controller_logic"), list) else []:
        lines.append(f"- {item}")
    lines.append("dto_contract_hints:")
    for item in parsed.get("dto_contract_hints", [])[:4] if isinstance(parsed.get("dto_contract_hints"), list) else []:
        lines.append(f"- {item}")
    return "\n".join(lines)


def phase_focus_for_context(phase_payload: dict[str, Any]) -> str:
    phase = str(phase_payload["phase"])
    if phase == "phase-1-plan":
        return "Query-to-repository/service/controller planning"
    if phase == "phase-2-repository-chunk":
        return "Single SQL chunk repository binding and Oracle 19c preservation"
    if phase == "phase-2-repository-merge":
        return "Merge chunk-level repository logic into one query-level method plan"
    if phase == "phase-3-bff-assembly":
        return "Assemble service/controller/DTO logic from repository outputs"
    if phase == "phase-4-verify":
        return "Validate completeness, token safety, and guess risk before handoff"
    return phase


def phase_query_id(phase_payload: dict[str, Any]) -> str | None:
    schema = phase_payload.get("answer_schema", {})
    if isinstance(schema, dict):
        value = schema.get("query_id")
        if isinstance(value, str) and value.strip() and value.strip().lower() != "string":
            return value
    return None


def phase_chunk_id(phase_payload: dict[str, Any]) -> str | None:
    schema = phase_payload.get("answer_schema", {})
    if isinstance(schema, dict):
        value = schema.get("chunk_id")
        if isinstance(value, str) and value.strip() and value.strip().lower() != "string":
            return value
    return None


def load_query_card(java_root: Path, query_id: str | None) -> dict[str, Any] | None:
    if not query_id:
        return None
    path = java_root / "implementation_cards" / f"{safe_name(query_id)}.json"
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    payload["__path"] = str(path.resolve())
    return payload


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
        if query_id and isinstance(parsed, dict) and parsed.get("query_id") != query_id:
            continue
        payload["__path"] = str(path.resolve())
        rows.append(payload)
    return rows
