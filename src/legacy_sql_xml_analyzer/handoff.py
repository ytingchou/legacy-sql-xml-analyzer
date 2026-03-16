from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .company_prompt_profiles import build_response_template, render_company_prompt
from .context_compiler import compile_context_pack_from_analysis, write_context_pack
from .java_bff import load_phase_pack_payload, resolve_java_bff_root, safe_name
from .java_bff_context import compile_java_bff_context_pack, write_java_bff_context_pack
from .prompting import answer_schema_for_cluster, load_failure_clusters, resolve_analysis_root


def export_vscode_cline_pack(
    analysis_root: Path,
    *,
    cluster_id: str | None = None,
    stage: str | None = None,
    prompt_json: Path | None = None,
    review_path: Path | None = None,
    output_dir: Path | None = None,
    profile_name: str = "company-qwen3-java-phase",
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    if review_path is not None:
        payload = build_pack_from_review(analysis_root, review_path=review_path, profile_name=profile_name)
    elif prompt_json is not None:
        payload = build_pack_from_java_phase(analysis_root, prompt_json=prompt_json, profile_name=profile_name)
    elif cluster_id and stage:
        payload = build_pack_from_generic_context(analysis_root, cluster_id=cluster_id, stage=stage, profile_name=profile_name)
    else:
        raise ValueError("Provide either --review, --prompt-json, or --cluster together with --stage.")
    written = write_handoff_pack(output_dir or analysis_root.parent, payload)
    payload["written_paths"] = [str(path.resolve()) for path in written]
    return payload


def build_pack_from_generic_context(
    analysis_root: Path,
    *,
    cluster_id: str,
    stage: str,
    profile_name: str,
) -> dict[str, Any]:
    pack = compile_context_pack_from_analysis(
        analysis_root=analysis_root,
        cluster_id=cluster_id,
        phase=stage,
        prompt_profile="qwen3-128k-autonomous",
    )
    write_context_pack(analysis_root.parent, pack)
    clusters = load_failure_clusters(analysis_root)
    cluster = next(item for item in clusters["clusters"] if item["cluster_id"] == cluster_id)
    schema = answer_schema_for_cluster(cluster, stage=stage)
    evidence_sections = [(section["title"], str(section["text"])) for section in pack.sections]
    prompt_text = render_company_prompt(
        profile_name=profile_name,
        title=f"Company weak-LLM task: cluster {cluster_id} / stage {stage}",
        objective_lines=[
            f"Handle only cluster {cluster_id}.",
            f"Complete only stage {stage}.",
            "Prefer the smallest safe answer and return insufficient_evidence when proof is missing.",
        ],
        evidence_sections=evidence_sections,
        schema=schema,
    )
    return {
        "generated_at": timestamp_now(),
        "kind": "generic_cluster",
        "profile_name": profile_name,
        "cluster_id": cluster_id,
        "stage": stage,
        "title": f"{cluster_id} / {stage}",
        "prompt_text": prompt_text,
        "schema": schema,
        "response_template": build_response_template(schema),
        "operator_notes": [
            "Paste prompt.txt into Cline CLI or VS Code Cline.",
            "Return JSON only.",
            "Do not attach the full analysis directory.",
        ],
        "source_artifacts": pack.included_artifacts,
    }


def build_pack_from_java_phase(
    analysis_root: Path,
    *,
    prompt_json: Path,
    profile_name: str,
) -> dict[str, Any]:
    pack = compile_java_bff_context_pack(
        analysis_root=analysis_root,
        phase_pack_path=prompt_json.resolve(),
        prompt_profile="qwen3-128k-java-bff",
    )
    write_java_bff_context_pack(analysis_root, pack)
    phase_payload = load_phase_pack_payload(prompt_json.resolve())
    schema = phase_payload.get("answer_schema", {})
    evidence_sections = [(section["title"], str(section["text"])) for section in pack["sections"]]
    prompt_text = render_company_prompt(
        profile_name=profile_name,
        title=f"Company weak-LLM Java phase: {phase_payload['phase']}",
        objective_lines=[
            f"Handle only bundle {phase_payload['bundle_id']}.",
            f"Complete only phase {phase_payload['phase']}.",
            "Do not merge other phases or guess missing SQL semantics.",
        ],
        evidence_sections=evidence_sections,
        schema=schema,
    )
    return {
        "generated_at": timestamp_now(),
        "kind": "java_phase",
        "profile_name": profile_name,
        "bundle_id": phase_payload["bundle_id"],
        "phase": phase_payload["phase"],
        "title": f"{phase_payload['bundle_id']} / {phase_payload['phase']}",
        "prompt_text": prompt_text,
        "schema": schema,
        "response_template": build_response_template(schema),
        "operator_notes": [
            "Paste prompt.txt into Cline CLI or VS Code Cline.",
            "Return JSON only.",
            "Do not merge multiple Java BFF phases in one response.",
        ],
        "source_artifacts": pack["included_artifacts"],
    }


def build_pack_from_review(
    analysis_root: Path,
    *,
    review_path: Path,
    profile_name: str,
) -> dict[str, Any]:
    payload = json.loads(review_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected review JSON object at {review_path}")
    prompt_text = str(payload.get("repair_prompt_text") or payload.get("next_prompt_text") or "").strip()
    if not prompt_text:
        raise ValueError(f"Review at {review_path} does not contain a repair or next prompt.")
    schema = payload.get("parsed_response")
    if not isinstance(schema, dict):
        schema = {"result": "string"}
    title = f"Repair prompt from {review_path.name}"
    return {
        "generated_at": timestamp_now(),
        "kind": "review_repair",
        "profile_name": profile_name,
        "title": title,
        "prompt_text": prompt_text,
        "schema": schema,
        "response_template": build_response_template(schema),
        "operator_notes": [
            "Use this pack to repair a rejected response.",
            "Do not add commentary outside JSON.",
        ],
        "source_artifacts": [str(review_path.resolve())],
    }


def write_handoff_pack(output_root: Path, payload: dict[str, Any]) -> list[Path]:
    analysis_root = resolve_analysis_root(output_root)
    handoff_root = analysis_root / "handoff" / safe_name(str(payload.get("title") or "pack"))
    handoff_root.mkdir(parents=True, exist_ok=True)
    prompt_path = handoff_root / "prompt.txt"
    schema_path = handoff_root / "schema.json"
    template_path = handoff_root / "response_template.json"
    notes_path = handoff_root / "operator_notes.md"
    readme_path = handoff_root / "README.md"

    prompt_path.write_text(str(payload["prompt_text"]), encoding="utf-8")
    schema_path.write_text(json.dumps(payload["schema"], indent=2, ensure_ascii=False), encoding="utf-8")
    template_path.write_text(json.dumps(payload["response_template"], indent=2, ensure_ascii=False), encoding="utf-8")
    notes_path.write_text(render_operator_notes(payload), encoding="utf-8")
    readme_path.write_text(build_handoff_readme(payload), encoding="utf-8")
    meta_path = handoff_root / "pack.json"
    meta_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return [prompt_path, schema_path, template_path, notes_path, readme_path, meta_path]


def render_operator_notes(payload: dict[str, Any]) -> str:
    lines = ["# Operator Notes", ""]
    for note in payload.get("operator_notes", []):
        lines.append(f"- {note}")
    return "\n".join(lines).rstrip() + "\n"


def build_handoff_readme(payload: dict[str, Any]) -> str:
    lines = [
        "# Cline Handoff Pack",
        "",
        f"- Title: `{payload['title']}`",
        f"- Kind: `{payload['kind']}`",
        f"- Prompt profile: `{payload['profile_name']}`",
        "",
        "## Files",
        "- `prompt.txt`: paste into Cline CLI or VS Code Cline",
        "- `schema.json`: expected response schema",
        "- `response_template.json`: minimal JSON shape",
        "- `operator_notes.md`: how to use the pack",
        "",
        "## Source Artifacts",
    ]
    for item in payload.get("source_artifacts", []):
        lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
