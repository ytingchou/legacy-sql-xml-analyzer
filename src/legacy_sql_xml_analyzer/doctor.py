from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .failure_explainer import explain_failure_from_output_dir
from .prompting import resolve_analysis_root


def doctor_run(output_dir: Path) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(output_dir)
    if analysis_root == output_dir.resolve() and (analysis_root / "analysis").exists():
        analysis_root = analysis_root / "analysis"

    failure_payload = explain_failure_from_output_dir(analysis_root.parent if analysis_root.name == "analysis" else analysis_root)
    generic_completion = load_json(analysis_root / "agent_loop" / "completion_report.json")
    java_completion = load_json(analysis_root / "java_bff" / "loop" / "completion_report.json")
    provider_rows = load_provider_rows(analysis_root / "provider_validation")
    doctor_payload = {
        "generated_at": timestamp_now(),
        "analysis_root": str(analysis_root.resolve()),
        "status": classify_status(generic_completion, java_completion, provider_rows, failure_payload["index"]),
        "generic_loop": summarize_loop(generic_completion),
        "java_bff_loop": summarize_loop(java_completion),
        "provider_validations": provider_rows,
        "failure_summary": summarize_failures(failure_payload["index"]),
        "recommended_actions": build_recommended_actions(generic_completion, java_completion, provider_rows, failure_payload["index"]),
        "artifacts": build_artifact_list(analysis_root, failure_payload),
    }
    root = analysis_root / "doctor"
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "doctor_report.json"
    md_path = root / "doctor_report.md"
    json_path.write_text(json.dumps(doctor_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_doctor_markdown(doctor_payload), encoding="utf-8")
    doctor_payload["json_path"] = str(json_path.resolve())
    doctor_payload["md_path"] = str(md_path.resolve())
    return doctor_payload


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def load_provider_rows(root: Path) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for summary_path in sorted(root.glob("*/summary.json")):
        payload = load_json(summary_path)
        if payload:
            payload["summary_path"] = str(summary_path.resolve())
            rows.append(payload)
    return rows[-5:]


def summarize_loop(payload: dict[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {"status": "missing", "stop_reason": "missing_completion_report", "iterations": 0, "missing_artifact_count": 0}
    missing_artifacts = payload.get("missing_artifacts")
    return {
        "status": payload.get("status"),
        "stop_reason": payload.get("stop_reason"),
        "iterations": int(payload.get("iterations", payload.get("iteration_count", 0)) or 0),
        "missing_artifact_count": len(missing_artifacts) if isinstance(missing_artifacts, list) else 0,
        "missing_artifacts": missing_artifacts if isinstance(missing_artifacts, list) else [],
    }


def summarize_failures(index_payload: dict[str, Any]) -> dict[str, Any]:
    explanations = index_payload.get("explanations", []) if isinstance(index_payload, dict) else []
    codes: dict[str, int] = {}
    for item in explanations:
        if not isinstance(item, dict):
            continue
        code = str(item.get("failure_code") or "unknown")
        codes[code] = codes.get(code, 0) + 1
    top_codes = sorted(codes.items(), key=lambda row: (-row[1], row[0]))[:8]
    return {
        "count": len(explanations),
        "top_codes": [{"failure_code": code, "count": count} for code, count in top_codes],
    }


def classify_status(
    generic_completion: dict[str, Any] | None,
    java_completion: dict[str, Any] | None,
    provider_rows: list[dict[str, Any]],
    failure_index: dict[str, Any],
) -> str:
    if any(str(row.get("status")) == "failed" for row in provider_rows):
        return "provider_attention_required"
    if java_completion and str(java_completion.get("status")) == "failed":
        return "java_bff_failed"
    if generic_completion and str(generic_completion.get("status")) == "failed":
        return "generic_failed"
    if any(str(item.get("failure_code") or "").endswith("HUMAN_REVIEW_REQUIRED") for item in failure_index.get("explanations", [])):
        return "human_review_required"
    if java_completion and str(java_completion.get("status")) != "completed":
        return "java_bff_incomplete"
    if generic_completion and str(generic_completion.get("status")) != "completed":
        return "generic_incomplete"
    return "healthy"


def build_recommended_actions(
    generic_completion: dict[str, Any] | None,
    java_completion: dict[str, Any] | None,
    provider_rows: list[dict[str, Any]],
    failure_index: dict[str, Any],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if any(str(row.get("status")) == "failed" for row in provider_rows):
        actions.append(
            {
                "category": "provider",
                "summary": "Provider validation is failing or stale.",
                "command": "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer validate-provider --output <out_dir> --provider-config <provider.json>",
            }
        )
    if java_completion and str(java_completion.get("status")) not in {"completed", "missing"}:
        actions.append(
            {
                "category": "java-bff-loop",
                "summary": "Resume the Java BFF loop or inspect the latest failure explanation.",
                "command": "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-java-bff-loop --output <out_dir>",
            }
        )
    if generic_completion and str(generic_completion.get("status")) not in {"completed", "missing"}:
        actions.append(
            {
                "category": "generic-loop",
                "summary": "Resume the generic loop if XML/profile repair is still incomplete.",
                "command": "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-agent-loop --output <out_dir>",
            }
        )
    explanations = failure_index.get("explanations", []) if isinstance(failure_index, dict) else []
    for item in explanations[:3]:
        if not isinstance(item, dict):
            continue
        command = item.get("recommended_command")
        if command:
            actions.append(
                {
                    "category": str(item.get("failure_code") or "failure"),
                    "summary": str(item.get("summary") or ""),
                    "command": str(command),
                }
            )
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in actions:
        key = (str(item["category"]), str(item["command"]))
        if key not in seen:
            seen.add(key)
            deduped.append(item)
    return deduped[:8]


def build_artifact_list(analysis_root: Path, failure_payload: dict[str, Any]) -> list[str]:
    artifacts = [
        str(analysis_root / "agent_loop" / "completion_report.json"),
        str(analysis_root / "java_bff" / "loop" / "completion_report.json"),
        failure_payload.get("index_json_path"),
    ]
    return [str(Path(item).resolve()) for item in artifacts if item and Path(str(item)).exists()]


def render_doctor_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Doctor Report",
        "",
        f"- Status: `{payload['status']}`",
        f"- Analysis root: `{payload['analysis_root']}`",
        "",
        "## Generic Loop",
        f"- Status: `{payload['generic_loop']['status']}`",
        f"- Stop reason: `{payload['generic_loop']['stop_reason']}`",
        f"- Missing artifacts: {payload['generic_loop']['missing_artifact_count']}",
        "",
        "## Java BFF Loop",
        f"- Status: `{payload['java_bff_loop']['status']}`",
        f"- Stop reason: `{payload['java_bff_loop']['stop_reason']}`",
        f"- Missing artifacts: {payload['java_bff_loop']['missing_artifact_count']}",
        "",
        "## Recommended Actions",
    ]
    if payload["recommended_actions"]:
        for item in payload["recommended_actions"]:
            lines.append(f"- `{item['category']}` {item['summary']}")
            lines.append(f"  command: `{item['command']}`")
    else:
        lines.append("- None")
    lines.extend(["", "## Top Failure Codes"])
    if payload["failure_summary"]["top_codes"]:
        for item in payload["failure_summary"]["top_codes"]:
            lines.append(f"- `{item['failure_code']}` x{item['count']}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
