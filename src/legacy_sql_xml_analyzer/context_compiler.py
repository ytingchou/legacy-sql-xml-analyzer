from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from .prompt_profiles import phase_budget_for, phase_example_limit_for
from .prompting import answer_schema_for_cluster, load_failure_clusters, resolve_analysis_root
from .schemas import ContextPack


def estimate_tokens(text: str) -> int:
    return max(1, round(len(text) / 4)) if text else 0


def compile_context_pack_from_analysis(
    analysis_root: Path,
    cluster_id: str,
    phase: str,
    prompt_profile: str,
    prior_response: dict[str, Any] | None = None,
) -> ContextPack:
    analysis_root = resolve_analysis_root(analysis_root)
    payload = load_failure_clusters(analysis_root)
    cluster = next((item for item in payload["clusters"] if item["cluster_id"] == cluster_id), None)
    if cluster is None:
        raise ValueError(f"Unknown cluster_id: {cluster_id}")

    budget = phase_budget_for(prompt_profile, phase)
    example_limit = phase_example_limit_for(prompt_profile, phase)
    sections = build_sections(analysis_root, cluster, phase, example_limit, prior_response)
    selected_sections = trim_sections_for_budget(sections, budget["usable_input_limit"])
    prompt_text = render_context_prompt(cluster, phase, prompt_profile, selected_sections, prior_response)
    estimated = estimate_tokens(prompt_text)
    return ContextPack(
        cluster_id=cluster_id,
        phase=phase,
        prompt_profile=prompt_profile,
        estimated_tokens=estimated,
        max_input_tokens=budget["usable_input_limit"],
        included_artifacts=collect_included_artifacts(selected_sections),
        sections=selected_sections,
        prompt_text=prompt_text,
    )


def write_context_pack(output_dir: Path, pack: ContextPack) -> list[Path]:
    root = output_dir / "analysis" / "context_packs"
    root.mkdir(parents=True, exist_ok=True)
    base_name = f"{sanitize_for_filename(pack.cluster_id)}-{sanitize_for_filename(pack.phase)}"
    json_path = root / f"{base_name}.json"
    txt_path = root / f"{base_name}.txt"
    md_path = root / f"{base_name}.md"
    json_path.write_text(json.dumps(pack.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    txt_path.write_text(pack.prompt_text, encoding="utf-8")
    md_path.write_text(render_context_pack_markdown(pack), encoding="utf-8")
    return [json_path, txt_path, md_path]


def build_sections(
    analysis_root: Path,
    cluster: dict[str, Any],
    phase: str,
    example_limit: int,
    prior_response: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    sections: list[dict[str, Any]] = [
        {
            "kind": "problem_summary",
            "title": "Problem Summary",
            "text": "\n".join(
                [
                    f"cluster_id: {cluster['cluster_id']}",
                    f"task_type: {cluster['task_type']}",
                    f"code: {cluster['code']}",
                    f"severity: {cluster['severity']}",
                    f"representative_message: {cluster['representative_message']}",
                    f"suggested_fix: {cluster.get('suggested_fix') or 'n/a'}",
                ]
            ),
            "artifacts": [str(analysis_root / "failure_clusters.json")],
        }
    ]

    sample_diagnostics = cluster.get("sample_diagnostics", [])[:example_limit]
    if sample_diagnostics:
        sections.append(
            {
                "kind": "sample_diagnostics",
                "title": "Sample Diagnostics",
                "text": json.dumps(sample_diagnostics, indent=2, ensure_ascii=False),
                "artifacts": [str(analysis_root / "failure_clusters.json")],
            }
        )

    query_section = build_query_card_section(analysis_root, sample_diagnostics)
    if query_section is not None:
        sections.append(query_section)

    if phase == "verify" and prior_response is not None:
        sections.append(
            {
                "kind": "prior_proposal",
                "title": "Prior Proposal",
                "text": json.dumps(prior_response, indent=2, ensure_ascii=False),
                "artifacts": [],
            }
        )
    return sections


def build_query_card_section(analysis_root: Path, sample_diagnostics: list[dict[str, Any]]) -> dict[str, Any] | None:
    queries_root = analysis_root / "markdown" / "queries"
    if not queries_root.exists():
        return None

    for diagnostic in sample_diagnostics:
        query_id = str(diagnostic.get("query_id") or "").strip()
        if not query_id:
            continue
        path = queries_root / f"{safe_name(query_id)}.md"
        if path.exists():
            return {
                "kind": "query_card",
                "title": "Relevant Query Card",
                "text": path.read_text(encoding="utf-8"),
                "artifacts": [str(path)],
            }
    return None


def trim_sections_for_budget(sections: list[dict[str, Any]], max_tokens: int) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    used = 0
    for section in sections:
        text = section.get("text", "")
        tokens = estimate_tokens(text)
        if selected and used + tokens > max_tokens:
            break
        selected.append(section)
        used += tokens
    return selected


def collect_included_artifacts(sections: list[dict[str, Any]]) -> list[str]:
    seen: list[str] = []
    for section in sections:
        for artifact in section.get("artifacts", []):
            if artifact not in seen:
                seen.append(artifact)
    return seen


def render_context_prompt(
    cluster: dict[str, Any],
    phase: str,
    prompt_profile: str,
    sections: list[dict[str, Any]],
    prior_response: dict[str, Any] | None,
) -> str:
    schema = json.dumps(answer_schema_for_cluster(cluster, stage=phase), indent=2, ensure_ascii=False)
    lines = [
        "You are assisting a legacy SQL XML analyzer in company autonomous mode.",
        f"Prompt profile: {prompt_profile}",
        f"Phase: {phase}",
        f"Cluster: {cluster['cluster_id']}",
        "",
        "Constraints:",
        "- Return valid JSON only.",
        "- Do not invent business SQL semantics.",
        "- If evidence is insufficient, say so explicitly.",
    ]
    if phase == "verify":
        lines.append("- Do not introduce a brand new rule in verify phase.")
    lines.append("")
    lines.append("Context:")
    for section in sections:
        lines.append(f"## {section['title']}")
        lines.append(str(section["text"]).rstrip())
        lines.append("")
    if phase == "verify" and prior_response is not None:
        lines.append("Prior proposal is authoritative for this verify task.")
        lines.append("")
    lines.extend(
        [
            "Return JSON only with this schema:",
            schema,
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_context_pack_markdown(pack: ContextPack) -> str:
    lines = [
        "# Context Pack",
        "",
        f"- Cluster: `{pack.cluster_id}`",
        f"- Phase: `{pack.phase}`",
        f"- Prompt profile: `{pack.prompt_profile}`",
        f"- Estimated tokens: {pack.estimated_tokens}",
        f"- Max input tokens: {pack.max_input_tokens}",
        "",
        "## Included Artifacts",
    ]
    if pack.included_artifacts:
        for item in pack.included_artifacts:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    lines.extend(["", "## Sections"])
    for section in pack.sections:
        lines.append(f"- `{section['kind']}`")
    return "\n".join(lines).rstrip() + "\n"


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)


def sanitize_for_filename(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-._")
