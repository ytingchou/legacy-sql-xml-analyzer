from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .prompting import resolve_analysis_root


def explain_failure_from_output_dir(output_dir: Path, *, scope: str = "all") -> dict[str, Any]:
    analysis_root = resolve_analysis_root(output_dir)
    if analysis_root == output_dir.resolve() and (analysis_root / "analysis").exists():
        analysis_root = analysis_root / "analysis"
    explanations: list[dict[str, Any]] = []

    if scope in {"all", "generic"}:
        generic_completion = analysis_root / "agent_loop" / "completion_report.json"
        if generic_completion.exists():
            explanations.extend(explain_completion_report(generic_completion, flavor="generic"))

        for review_path in sorted((analysis_root / "llm_reviews").glob("*-review.json")):
            explanations.extend(explain_review_payload(review_path))

    if scope in {"all", "java-bff"}:
        java_completion = analysis_root / "java_bff" / "loop" / "completion_report.json"
        if java_completion.exists():
            explanations.extend(explain_completion_report(java_completion, flavor="java-bff"))

        reviews_root = analysis_root / "java_bff" / "reviews"
        if reviews_root.exists():
            for review_path in sorted(reviews_root.glob("*/*-review.json")):
                explanations.extend(explain_review_payload(review_path))

    provider_root = analysis_root / "provider_validation"
    if scope == "all" and provider_root.exists():
        for debug_path in sorted(provider_root.glob("*/debug.json")):
            explanations.extend(explain_provider_debug(debug_path))

    root = analysis_root / "failure_explanations"
    root.mkdir(parents=True, exist_ok=True)
    index = {
        "generated_at": timestamp_now(),
        "scope": scope,
        "count": len(explanations),
        "explanations": explanations,
    }
    index_path = root / "index.json"
    md_path = root / "index.md"
    index_path.write_text(json.dumps(index, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_explanation_index_markdown(index), encoding="utf-8")

    for explanation in explanations:
        base = root / safe_failure_name(f"{explanation['failure_code']}-{explanation['slug']}")
        (base.with_suffix(".json")).write_text(json.dumps(explanation, indent=2, ensure_ascii=False), encoding="utf-8")
        (base.with_suffix(".md")).write_text(render_explanation_markdown(explanation), encoding="utf-8")

    return {
        "index": index,
        "index_json_path": str(index_path.resolve()),
        "index_md_path": str(md_path.resolve()),
    }


def explain_completion_report(path: Path, *, flavor: str) -> list[dict[str, Any]]:
    payload = load_json(path)
    stop_reason = str(payload.get("stop_reason") or "")
    if not stop_reason:
        return []
    code = {
        "java_bff_phase_failed": "JAVA_BFF_PHASE_FAILED",
        "java_bff_human_review_required": "JAVA_BFF_HUMAN_REVIEW_REQUIRED",
        "java_bff_artifacts_incomplete": "JAVA_BFF_ARTIFACTS_INCOMPLETE",
        "max_iterations_reached": "MAX_ITERATIONS_REACHED",
        "all_required_artifacts_completed": "LOOP_COMPLETED",
        "all_artifacts_completed": "JAVA_BFF_COMPLETED",
    }.get(stop_reason, stop_reason.upper())
    return [
        build_explanation(
            failure_code=code,
            slug=f"{flavor}-completion",
            summary=f"{flavor} loop stop reason: {stop_reason}",
            what_happened=f"The {flavor} loop stopped with stop_reason={stop_reason}.",
            likely_causes=completion_causes(payload),
            recommended_next_step=completion_next_step(stop_reason),
            recommended_command=completion_command(stop_reason, flavor),
            relevant_artifacts=[str(path.resolve())],
        )
    ]


def explain_provider_debug(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    category = str(payload.get("failure_category") or payload.get("classification") or "provider_validation_failed")
    summary = str(payload.get("summary") or payload.get("message") or category)
    code = {
        "authentication": "PROVIDER_AUTHENTICATION_FAILED",
        "endpoint": "PROVIDER_ENDPOINT_INVALID",
        "network": "PROVIDER_NETWORK_FAILED",
        "response_shape": "PROVIDER_RESPONSE_SHAPE_INVALID",
        "response_format": "PROVIDER_NON_JSON_RESPONSE",
        "sse": "PROVIDER_SSE_RESPONSE",
    }.get(category, category.upper())
    hints = payload.get("troubleshooting_hints")
    causes = hints if isinstance(hints, list) else ["Inspect provider base URL, auth, and response shape."]
    return [
        build_explanation(
            failure_code=code,
            slug=path.parent.name,
            summary=summary,
            what_happened=f"Provider validation reported {category}.",
            likely_causes=[str(item) for item in causes],
            recommended_next_step="Validate the provider before rerunning loops.",
            recommended_command="PYTHONPATH=src python3 -m legacy_sql_xml_analyzer validate-provider --output <out_dir> --provider-config <provider.json>",
            relevant_artifacts=[str(path.resolve())],
        )
    ]


def explain_review_payload(path: Path) -> list[dict[str, Any]]:
    payload = load_json(path)
    status = str(payload.get("status") or "")
    if status in {"accepted", "insufficient_evidence"}:
        return []
    issues = payload.get("issues")
    if not isinstance(issues, list):
        issues = []
    issue_codes = [str(item.get("code") or "") for item in issues if isinstance(item, dict)]
    primary_code = issue_codes[0] if issue_codes else "PHASE_REVIEW_REJECTED"
    phase = str(payload.get("phase") or payload.get("stage") or "unknown")
    repair_path = str(payload.get("repair_prompt_path") or payload.get("next_prompt_path") or "")
    recommended_command = None
    if repair_path:
        recommended_command = f"Use repair prompt at {repair_path} with Cline, then rerun the same review command."
    return [
        build_explanation(
            failure_code=primary_code,
            slug=Path(path).stem,
            summary=f"Review rejected phase={phase} status={status}",
            what_happened=f"The review at {path.name} returned status={status}.",
            likely_causes=[str(item.get("message") or item.get("code")) for item in issues[:5] if isinstance(item, dict)] or ["Schema or semantics validation failed."],
            recommended_next_step="Use the repair prompt or inspect the normalized response and issues.",
            recommended_command=recommended_command,
            relevant_artifacts=[str(path.resolve()), repair_path] if repair_path else [str(path.resolve())],
        )
    ]


def build_explanation(
    *,
    failure_code: str,
    slug: str,
    summary: str,
    what_happened: str,
    likely_causes: list[str],
    recommended_next_step: str,
    recommended_command: str | None,
    relevant_artifacts: list[str],
) -> dict[str, Any]:
    return {
        "generated_at": timestamp_now(),
        "failure_code": failure_code,
        "slug": slug,
        "summary": summary,
        "what_happened": what_happened,
        "likely_causes": likely_causes,
        "recommended_next_step": recommended_next_step,
        "recommended_command": recommended_command,
        "relevant_artifacts": [item for item in relevant_artifacts if item],
        "company_llm_prompt": render_company_llm_prompt(
            failure_code=failure_code,
            summary=summary,
            likely_causes=likely_causes,
            relevant_artifacts=relevant_artifacts,
        ),
        "human_review_needed": failure_code.endswith("HUMAN_REVIEW_REQUIRED"),
    }


def render_company_llm_prompt(
    *,
    failure_code: str,
    summary: str,
    likely_causes: list[str],
    relevant_artifacts: list[str],
) -> str:
    lines = [
        f"Failure code: {failure_code}",
        f"Summary: {summary}",
        "",
        "Task:",
        "- Explain the minimum safe next step.",
        "- Return JSON only.",
        "- Do not guess missing SQL business semantics.",
        "",
        "Likely causes:",
    ]
    lines.extend(f"- {item}" for item in likely_causes)
    lines.extend(["", "Relevant artifacts:"])
    lines.extend(f"- {item}" for item in relevant_artifacts if item)
    lines.extend(
        [
            "",
            'Return JSON only with schema: {"root_cause":"string","next_action":"string","prompt_to_run":"string","notes":["string"]}',
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def completion_causes(payload: dict[str, Any]) -> list[str]:
    last_error = payload.get("last_error")
    if isinstance(last_error, dict):
        return [f"{key}: {value}" for key, value in last_error.items()]
    missing = payload.get("missing_artifacts")
    if isinstance(missing, list) and missing:
        return [f"missing_artifact: {item}" for item in missing[:5]]
    return ["No detailed last_error recorded."]


def completion_next_step(stop_reason: str) -> str:
    if stop_reason in {"java_bff_human_review_required", "java_bff_phase_failed"}:
        return "Inspect the failed review or task result before resuming."
    if stop_reason == "max_iterations_reached":
        return "Resume the loop or narrow the scope with entry-file and entry-main-query."
    if stop_reason == "java_bff_artifacts_incomplete":
        return "Inspect missing artifacts and rerun the loop."
    return "Inspect the completion report and related artifacts."


def completion_command(stop_reason: str, flavor: str) -> str | None:
    if flavor == "java-bff":
        if stop_reason in {"java_bff_phase_failed", "java_bff_human_review_required", "java_bff_artifacts_incomplete", "max_iterations_reached"}:
            return "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-java-bff-loop --output <out_dir>"
    else:
        if stop_reason in {"max_iterations_reached"}:
            return "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-agent-loop --output <out_dir>"
    return None


def render_explanation_index_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Failure Explanations",
        "",
        f"- Count: {payload['count']}",
        f"- Scope: `{payload['scope']}`",
        "",
        "## Entries",
    ]
    for item in payload["explanations"]:
        lines.append(f"- `{item['failure_code']}` {item['summary']}")
    return "\n".join(lines).rstrip() + "\n"


def render_explanation_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Failure Explanation",
        "",
        f"- Code: `{payload['failure_code']}`",
        f"- Summary: {payload['summary']}",
        f"- Human Review Needed: `{payload['human_review_needed']}`",
        "",
        "## What Happened",
        payload["what_happened"],
        "",
        "## Likely Causes",
    ]
    for item in payload["likely_causes"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Recommended Next Step", payload["recommended_next_step"]])
    if payload.get("recommended_command"):
        lines.extend(["", "## Recommended Command", f"`{payload['recommended_command']}`"])
    lines.extend(["", "## Relevant Artifacts"])
    for item in payload["relevant_artifacts"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Company LLM Prompt", "```text", payload["company_llm_prompt"].rstrip(), "```"])
    return "\n".join(lines).rstrip() + "\n"


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object at {path}")
    return payload


def safe_failure_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "-" for char in value).strip("-")


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
