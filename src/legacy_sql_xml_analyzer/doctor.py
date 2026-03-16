from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .failure_explainer import explain_failure_from_output_dir
from .context_compiler import estimate_tokens
from .prompting import resolve_analysis_root


def doctor_run(output_dir: Path) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(output_dir)
    if analysis_root == output_dir.resolve() and (analysis_root / "analysis").exists():
        analysis_root = analysis_root / "analysis"

    failure_payload = explain_failure_from_output_dir(analysis_root.parent if analysis_root.name == "analysis" else analysis_root)
    generic_completion = load_json(analysis_root / "agent_loop" / "completion_report.json")
    java_completion = load_json(analysis_root / "java_bff" / "loop" / "completion_report.json")
    provider_rows = load_provider_rows(analysis_root / "provider_validation")
    response_scoreboard = build_response_scoreboard(analysis_root)
    retry_scoreboard = build_retry_scoreboard(analysis_root)
    history_trend = build_history_trend(analysis_root)
    handoff_lifecycle = summarize_handoff_lifecycle(analysis_root)
    phase_queue = build_phase_queue_summary(analysis_root)
    latest_review_candidate = find_latest_review_candidate(analysis_root)
    doctor_payload = {
        "generated_at": timestamp_now(),
        "analysis_root": str(analysis_root.resolve()),
        "status": classify_status(generic_completion, java_completion, provider_rows, failure_payload["index"]),
        "generic_loop": summarize_loop(generic_completion),
        "java_bff_loop": summarize_loop(java_completion),
        "provider_validations": provider_rows,
        "failure_summary": summarize_failures(failure_payload["index"]),
        "recommended_actions": build_recommended_actions(
            generic_completion,
            java_completion,
            provider_rows,
            failure_payload["index"],
            retry_scoreboard,
        ),
        "response_scoreboard": response_scoreboard,
        "retry_scoreboard": retry_scoreboard,
        "history_trend": history_trend,
        "handoff_lifecycle": handoff_lifecycle,
        "phase_queue": phase_queue,
        "latest_review_candidate": latest_review_candidate,
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


def retry_from_doctor(output_dir: Path) -> dict[str, Any]:
    from .adaptive_prompt import (
        compile_adaptive_generic_context,
        compile_adaptive_java_context,
        plan_prompt_downgrade,
        write_adaptive_payload,
    )
    from .handoff import export_vscode_cline_pack

    doctor_payload = doctor_run(output_dir)
    analysis_root = Path(str(doctor_payload["analysis_root"])).resolve()
    latest_review = doctor_payload.get("latest_review_candidate")
    plan = {
        "generated_at": timestamp_now(),
        "analysis_root": str(analysis_root),
        "doctor_status": doctor_payload.get("status"),
        "recommended_actions": doctor_payload.get("recommended_actions", []),
        "latest_review_candidate": latest_review,
        "generated_artifacts": [],
        "commands": [],
        "notes": [],
    }
    for item in doctor_payload.get("recommended_actions", [])[:3]:
        if isinstance(item, dict) and item.get("command"):
            plan["commands"].append(str(item["command"]))

    if isinstance(latest_review, dict) and latest_review.get("review_path"):
        review_path = Path(str(latest_review["review_path"]))
        handoff = export_vscode_cline_pack(
            analysis_root,
            review_path=review_path,
            profile_name="company-qwen3-verify" if latest_review.get("kind") == "generic" else "company-qwen3-java-phase",
            initial_state="repaired",
        )
        plan["generated_artifacts"].extend(handoff.get("written_paths", []))
        plan["notes"].append(f"Generated repair handoff pack for {review_path.name}.")

        current_tokens = estimate_retry_prompt_tokens(review_path)
        targets = plan_prompt_downgrade(current_tokens, max_candidates=3)
        adaptive_paths: list[str] = []
        if latest_review.get("kind") == "generic" and latest_review.get("cluster_id") and latest_review.get("stage"):
            payload = compile_adaptive_generic_context(
                analysis_root=analysis_root,
                cluster_id=str(latest_review["cluster_id"]),
                phase=str(latest_review["stage"]),
                prompt_profile="qwen3-128k-autonomous",
                targets=targets["candidate_targets"] or None,
            )
            adaptive_paths = [str(path.resolve()) for path in write_adaptive_payload(analysis_root, payload)]
        elif latest_review.get("kind") == "java-bff" and latest_review.get("phase_pack_path"):
            payload = compile_adaptive_java_context(
                analysis_root=analysis_root,
                prompt_json=Path(str(latest_review["phase_pack_path"])),
                prompt_profile="qwen3-128k-java-bff",
                targets=targets["candidate_targets"] or None,
            )
            adaptive_paths = [str(path.resolve()) for path in write_adaptive_payload(analysis_root, payload)]
        if adaptive_paths:
            plan["generated_artifacts"].extend(adaptive_paths)
            plan["notes"].append(
                f"Generated adaptive retry variants for targets={','.join(str(item) for item in targets['candidate_targets'])}."
            )

    root = analysis_root / "doctor"
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "retry_plan.json"
    md_path = root / "retry_plan.md"
    json_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_retry_plan_markdown(plan), encoding="utf-8")
    plan["json_path"] = str(json_path.resolve())
    plan["md_path"] = str(md_path.resolve())
    return plan


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


def load_review_rows(root: Path, *, kind: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if kind == "generic":
        paths = sorted(root.glob("llm_reviews/*-review.json"))
    else:
        paths = sorted((root / "java_bff" / "reviews").glob("*/*-review.json"))
    for path in paths:
        payload = load_json(path)
        if payload:
            payload["review_path"] = str(path.resolve())
            payload["kind"] = kind
            payload["mtime"] = path.stat().st_mtime
            rows.append(payload)
    return rows


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


def build_response_scoreboard(analysis_root: Path) -> dict[str, Any]:
    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for payload in load_review_rows(analysis_root, kind="generic") + load_review_rows(analysis_root, kind="java-bff"):
        stage = str(payload.get("stage") or payload.get("phase") or "unknown")
        kind = str(payload.get("kind") or "unknown")
        key = (kind, stage)
        bucket = buckets.setdefault(
            key,
            {
                "kind": kind,
                "stage": stage,
                "review_count": 0,
                "accepted": 0,
                "needs_revision": 0,
                "insufficient_evidence": 0,
            },
        )
        bucket["review_count"] += 1
        status = str(payload.get("status") or "unknown")
        if status in bucket:
            bucket[status] += 1
    rows = []
    for bucket in sorted(buckets.values(), key=lambda item: (item["kind"], item["stage"])):
        count = max(int(bucket["review_count"]), 1)
        rows.append(
            {
                **bucket,
                "acceptance_rate": round((int(bucket["accepted"]) / count) * 100, 1),
                "revision_rate": round((int(bucket["needs_revision"]) / count) * 100, 1),
            }
        )
    return {"rows": rows, "total_reviews": sum(int(item["review_count"]) for item in rows)}


def build_retry_scoreboard(analysis_root: Path) -> dict[str, Any]:
    from .handoff import list_handoff_sessions

    sessions = list_handoff_sessions(analysis_root)
    state_counts: dict[str, int] = {}
    attempts_total = 0
    retry_ready = 0
    human_review_required = 0
    resolved = 0
    rows: list[dict[str, Any]] = []
    for item in sessions:
        state = str(item.get("state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        attempts = int(item.get("attempt_count", 0) or 0)
        attempts_total += attempts
        status = str(item.get("status") or "pending_response")
        if status == "retry_ready":
            retry_ready += 1
        elif status == "human_review_required":
            human_review_required += 1
        elif status == "resolved":
            resolved += 1
        rows.append(
            {
                "title": item.get("title"),
                "kind": item.get("kind"),
                "status": status,
                "state": state,
                "attempt_count": attempts,
                "max_attempts": int(item.get("max_attempts", 0) or 0),
                "session_path": item.get("session_path"),
            }
        )
    session_count = len(sessions)
    return {
        "session_count": session_count,
        "retry_ready_count": retry_ready,
        "human_review_required_count": human_review_required,
        "resolved_count": resolved,
        "average_attempts": round(attempts_total / session_count, 2) if session_count else 0.0,
        "state_counts": state_counts,
        "rows": rows[:20],
    }


def build_history_trend(analysis_root: Path) -> dict[str, Any]:
    generic_history = load_json_list(analysis_root / "agent_loop" / "phase_history.json")
    java_history = load_json_list(analysis_root / "java_bff" / "loop" / "phase_history.json")
    handoff_lifecycle = summarize_handoff_lifecycle(analysis_root)
    return {
        "generic_event_count": len(generic_history),
        "java_event_count": len(java_history),
        "latest_generic_event": generic_history[-1] if generic_history else None,
        "latest_java_event": java_history[-1] if java_history else None,
        "handoff_state_counts": handoff_lifecycle.get("state_counts", {}),
    }


def summarize_handoff_lifecycle(analysis_root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    state_counts: dict[str, int] = {}
    for path in sorted((analysis_root / "handoff").glob("*/lifecycle.json")):
        payload = load_json(path)
        if not payload:
            continue
        state = str(payload.get("state") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        rows.append(
            {
                "title": payload.get("title") or path.parent.name,
                "state": state,
                "lifecycle_path": str(path.resolve()),
                "history_length": len(payload.get("history", [])) if isinstance(payload.get("history"), list) else 0,
            }
        )
    return {
        "count": len(rows),
        "state_counts": state_counts,
        "rows": rows[-20:],
    }


def build_phase_queue_summary(analysis_root: Path) -> dict[str, Any]:
    from .java_bff import safe_name

    java_root = analysis_root / "java_bff"
    rows: list[dict[str, Any]] = []
    bundles_root = java_root / "bundles"
    if not bundles_root.exists():
        return {"bundle_count": 0, "pending_bundle_count": 0, "rows": rows}
    for bundle_path in sorted(bundles_root.glob("*/bundle.json")):
        payload = load_json(bundle_path)
        if not payload:
            continue
        bundle_id = str(payload.get("bundle_id") or bundle_path.parent.name)
        slug = safe_name(bundle_id)
        sequence = payload.get("recommended_sequence", [])
        completed_phases = 0
        pending_phases = 0
        latest_status = "pending"
        next_prompt = None
        for prompt in sequence if isinstance(sequence, list) else []:
            prompt_json = Path(str(prompt)).with_suffix(".json")
            review_json = java_root / "reviews" / slug / f"{safe_name(prompt_json.stem)}-review.json"
            review = load_json(review_json)
            if review and str(review.get("status")) in {"accepted", "insufficient_evidence"}:
                completed_phases += 1
                latest_status = str(review.get("status"))
                continue
            pending_phases += 1
            if next_prompt is None:
                next_prompt = str(prompt_json.resolve())
                latest_status = str(review.get("status") or "pending") if review else "pending"
        rows.append(
            {
                "bundle_id": bundle_id,
                "completed_phases": completed_phases,
                "pending_phases": pending_phases,
                "latest_status": latest_status,
                "next_prompt": next_prompt,
            }
        )
    return {
        "bundle_count": len(rows),
        "pending_bundle_count": sum(1 for item in rows if int(item["pending_phases"]) > 0),
        "rows": rows,
    }


def find_latest_review_candidate(analysis_root: Path) -> dict[str, Any] | None:
    rows = [
        item
        for item in load_review_rows(analysis_root, kind="generic") + load_review_rows(analysis_root, kind="java-bff")
        if str(item.get("status") or "") == "needs_revision"
    ]
    if not rows:
        return None
    latest = max(rows, key=lambda item: float(item.get("mtime", 0)))
    return {
        "kind": latest.get("kind"),
        "review_path": latest.get("review_path"),
        "cluster_id": latest.get("cluster_id"),
        "stage": latest.get("stage"),
        "phase": latest.get("phase"),
        "phase_pack_path": latest.get("phase_pack_path"),
        "status": latest.get("status"),
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
    retry_scoreboard: dict[str, Any] | None = None,
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
    retry_scoreboard = retry_scoreboard or {}
    if int(retry_scoreboard.get("retry_ready_count", 0) or 0) > 0:
        actions.append(
            {
                "category": "handoff-retry",
                "summary": "Review retry-ready handoff sessions and process saved response files automatically.",
                "command": "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer watch-cline-directory --analysis-root <out_dir>",
            }
        )
    if int(retry_scoreboard.get("human_review_required_count", 0) or 0) > 0:
        actions.append(
            {
                "category": "handoff-human-review",
                "summary": "Some handoff sessions exhausted retries and now need manual operator review.",
                "command": "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer resume-from-handoff --pack <analysis/handoff/.../session.json>",
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


def estimate_retry_prompt_tokens(review_path: Path) -> int | None:
    payload = load_json(review_path)
    if not payload:
        return None
    for key in ("repair_prompt_text", "next_prompt_text"):
        text = payload.get(key)
        if isinstance(text, str) and text.strip():
            return estimate_tokens(text)
    for key in ("repair_prompt_path", "next_prompt_path"):
        prompt_path = payload.get(key)
        if prompt_path and Path(str(prompt_path)).exists():
            return estimate_tokens(Path(str(prompt_path)).read_text(encoding="utf-8"))
    return None


def load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    return [item for item in payload if isinstance(item, dict)]


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
    lines.extend(["", "## Review Scoreboard"])
    for item in payload.get("response_scoreboard", {}).get("rows", [])[:10]:
        lines.append(
            f"- `{item['kind']}:{item['stage']}` reviews={item['review_count']} "
            f"accepted={item['accepted']} needs_revision={item['needs_revision']}"
        )
    retry = payload.get("retry_scoreboard", {})
    lines.extend(
        [
            "",
            "## Retry Scoreboard",
            f"- Sessions: {retry.get('session_count', 0)}",
            f"- Resolved: {retry.get('resolved_count', 0)}",
            f"- Retry ready: {retry.get('retry_ready_count', 0)}",
            f"- Human review required: {retry.get('human_review_required_count', 0)}",
            f"- Average attempts: {retry.get('average_attempts', 0.0)}",
        ]
    )
    if payload.get("latest_review_candidate"):
        latest = payload["latest_review_candidate"]
        lines.extend(
            [
                "",
                "## Latest Retry Candidate",
                f"- Kind: `{latest.get('kind') or 'unknown'}`",
                f"- Review: `{latest.get('review_path') or 'n/a'}`",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def render_retry_plan_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Doctor Retry Plan",
        "",
        f"- Doctor status: `{payload.get('doctor_status')}`",
        f"- Analysis root: `{payload.get('analysis_root')}`",
        "",
        "## Commands",
    ]
    commands = payload.get("commands", [])
    if commands:
        for item in commands:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    lines.extend(["", "## Generated Artifacts"])
    artifacts = payload.get("generated_artifacts", [])
    if artifacts:
        for item in artifacts:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    if payload.get("notes"):
        lines.extend(["", "## Notes"])
        for item in payload["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
