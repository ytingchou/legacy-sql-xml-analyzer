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
    initial_state: str = "new",
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
    written = write_handoff_pack(output_dir or analysis_root.parent, payload, initial_state=initial_state)
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
        "phase_pack_path": str(prompt_json.resolve()),
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


def write_handoff_pack(output_root: Path, payload: dict[str, Any], *, initial_state: str = "new") -> list[Path]:
    analysis_root = resolve_analysis_root(output_root)
    handoff_root = analysis_root / "handoff" / safe_name(str(payload.get("title") or "pack"))
    handoff_root.mkdir(parents=True, exist_ok=True)
    prompt_path = handoff_root / "prompt.txt"
    schema_path = handoff_root / "schema.json"
    template_path = handoff_root / "response_template.json"
    notes_path = handoff_root / "operator_notes.md"
    readme_path = handoff_root / "README.md"
    session_path = handoff_root / "session.json"

    prompt_path.write_text(str(payload["prompt_text"]), encoding="utf-8")
    schema_path.write_text(json.dumps(payload["schema"], indent=2, ensure_ascii=False), encoding="utf-8")
    template_path.write_text(json.dumps(payload["response_template"], indent=2, ensure_ascii=False), encoding="utf-8")
    notes_path.write_text(render_operator_notes(payload), encoding="utf-8")
    readme_path.write_text(build_handoff_readme(payload), encoding="utf-8")
    meta_path = handoff_root / "pack.json"
    lifecycle_path = handoff_root / "lifecycle.json"
    lifecycle_payload = {
        "generated_at": timestamp_now(),
        "title": payload.get("title"),
        "kind": payload.get("kind"),
        "state": initial_state,
        "history": [
            {
                "generated_at": timestamp_now(),
                "event": "created",
                "state": initial_state,
                "notes": [f"Created from kind={payload.get('kind') or 'unknown'}."],
            }
        ],
    }
    payload = dict(payload)
    payload["pack_json_path"] = str(meta_path.resolve())
    payload["pack_root"] = str(handoff_root.resolve())
    payload["lifecycle_path"] = str(lifecycle_path.resolve())
    payload["session_path"] = str(session_path.resolve())
    meta_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    lifecycle_path.write_text(json.dumps(lifecycle_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    session_payload = build_handoff_session_payload(
        analysis_root=analysis_root,
        handoff_root=handoff_root,
        payload=payload,
        initial_state=initial_state,
    )
    session_path.write_text(json.dumps(session_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return [prompt_path, schema_path, template_path, notes_path, readme_path, meta_path, lifecycle_path, session_path]


def load_handoff_pack(pack_path: Path) -> dict[str, Any]:
    payload = json.loads(pack_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected handoff pack JSON object at {pack_path}")
    return payload


def update_handoff_lifecycle(
    pack_path: Path,
    *,
    state: str,
    event: str,
    notes: list[str] | None = None,
    related_artifacts: list[str] | None = None,
) -> dict[str, Any]:
    payload = load_handoff_pack(pack_path)
    lifecycle_path = Path(str(payload.get("lifecycle_path") or pack_path.parent / "lifecycle.json"))
    if lifecycle_path.exists():
        lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8"))
        if not isinstance(lifecycle, dict):
            lifecycle = {}
    else:
        lifecycle = {}
    history = lifecycle.get("history")
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "generated_at": timestamp_now(),
            "event": event,
            "state": state,
            "notes": notes or [],
            "related_artifacts": related_artifacts or [],
        }
    )
    lifecycle.update(
        {
            "generated_at": lifecycle.get("generated_at") or timestamp_now(),
            "title": payload.get("title"),
            "kind": payload.get("kind"),
            "state": state,
            "history": history,
        }
    )
    lifecycle_path.write_text(json.dumps(lifecycle, indent=2, ensure_ascii=False), encoding="utf-8")
    payload["lifecycle_path"] = str(lifecycle_path.resolve())
    Path(pack_path).write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return lifecycle


def build_handoff_session_payload(
    *,
    analysis_root: Path,
    handoff_root: Path,
    payload: dict[str, Any],
    initial_state: str,
) -> dict[str, Any]:
    response_path = handoff_root / "response.json"
    retry_targets = [8000, 16000, 24000]
    session_kind = "manual_repair" if payload.get("kind") == "review_repair" else "prompt_execution"
    return {
        "generated_at": timestamp_now(),
        "title": payload.get("title"),
        "kind": payload.get("kind"),
        "profile_name": payload.get("profile_name"),
        "session_kind": session_kind,
        "state": initial_state,
        "status": "pending_response",
        "attempt_count": 0,
        "max_attempts": 3,
        "retry_targets": retry_targets,
        "analysis_root": str(analysis_root.resolve()),
        "pack_root": str(handoff_root.resolve()),
        "pack_json_path": str(Path(str(payload["pack_json_path"])).resolve()),
        "lifecycle_path": str(Path(str(payload["lifecycle_path"])).resolve()),
        "prompt_path": str((handoff_root / "prompt.txt").resolve()),
        "schema_path": str((handoff_root / "schema.json").resolve()),
        "response_template_path": str((handoff_root / "response_template.json").resolve()),
        "response_path": str(response_path.resolve()),
        "cluster_id": payload.get("cluster_id"),
        "stage": payload.get("stage"),
        "bundle_id": payload.get("bundle_id"),
        "phase": payload.get("phase"),
        "phase_pack_path": payload.get("phase_pack_path"),
        "source_artifacts": payload.get("source_artifacts", []),
        "suggested_commands": build_session_commands(analysis_root=analysis_root, payload=payload, response_path=response_path),
        "history": [
            {
                "generated_at": timestamp_now(),
                "event": "session_created",
                "status": "pending_response",
                "notes": ["Awaiting a response file from Cline CLI or VS Code Cline."],
            }
        ],
        "last_review_path": None,
        "last_watch_report_path": None,
        "last_adaptive_retry": [],
        "last_repair_pack": [],
    }


def build_session_commands(
    *,
    analysis_root: Path,
    payload: dict[str, Any],
    response_path: Path,
) -> dict[str, str]:
    if payload.get("kind") == "generic_cluster":
        return {
            "watch_and_review": (
                "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer watch-and-review "
                f"--analysis-root {analysis_root} --cluster {payload['cluster_id']} --stage {payload['stage']} "
                f"--response {response_path} --source-pack {Path(str(payload['pack_json_path'])).resolve()}"
            ),
            "emit_repair": (
                "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer repair-company-prompt "
                f"--analysis-root {analysis_root} --review <review.json>"
            ),
        }
    if payload.get("kind") == "java_phase":
        prompt_json = str(payload.get("phase_pack_path") or "<phase-pack.json>")
        return {
            "watch_and_review": (
                "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer watch-and-review "
                f"--analysis-root {analysis_root} --prompt-json {prompt_json} --response {response_path} "
                f"--source-pack {Path(str(payload['pack_json_path'])).resolve()}"
            ),
            "merge_java_phase": (
                "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer merge-java-bff-phases "
                f"--analysis-root {analysis_root} --bundle-id {payload.get('bundle_id', '<bundle-id>')}"
            ),
        }
    return {
        "watch_and_review": (
            "PYTHONPATH=src python3 -m legacy_sql_xml_analyzer watch-and-review "
            f"--analysis-root {analysis_root} --response {response_path} --source-pack {Path(str(payload['pack_json_path'])).resolve()}"
        )
    }


def load_handoff_session(session_path: Path) -> dict[str, Any]:
    payload = json.loads(session_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected handoff session JSON object at {session_path}")
    return payload


def update_handoff_session(
    session_path: Path,
    *,
    status: str,
    state: str | None = None,
    attempt_increment: int = 0,
    notes: list[str] | None = None,
    review_path: str | None = None,
    watch_report_path: str | None = None,
    adaptive_retry: list[str] | None = None,
    repair_pack: list[str] | None = None,
) -> dict[str, Any]:
    payload = load_handoff_session(session_path)
    payload["status"] = status
    if state:
        payload["state"] = state
    payload["attempt_count"] = int(payload.get("attempt_count", 0)) + int(attempt_increment)
    if review_path:
        payload["last_review_path"] = review_path
    if watch_report_path:
        payload["last_watch_report_path"] = watch_report_path
    if adaptive_retry is not None:
        payload["last_adaptive_retry"] = adaptive_retry
    if repair_pack is not None:
        payload["last_repair_pack"] = repair_pack
    history = payload.get("history")
    if not isinstance(history, list):
        history = []
    history.append(
        {
            "generated_at": timestamp_now(),
            "event": "session_update",
            "status": status,
            "state": payload.get("state"),
            "notes": notes or [],
        }
    )
    payload["history"] = history
    session_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def list_handoff_sessions(analysis_root: Path) -> list[dict[str, Any]]:
    analysis_root = resolve_analysis_root(analysis_root)
    rows: list[dict[str, Any]] = []
    for path in sorted((analysis_root / "handoff").glob("*/session.json")):
        try:
            payload = load_handoff_session(path)
        except Exception:  # noqa: BLE001
            continue
        payload["session_path"] = str(path.resolve())
        rows.append(payload)
    return rows


def resume_from_handoff(pack_or_session_path: Path) -> dict[str, Any]:
    path = pack_or_session_path.resolve()
    if path.name == "pack.json":
        pack = load_handoff_pack(path)
        session_path = Path(str(pack.get("session_path") or path.parent / "session.json"))
    elif path.name == "session.json":
        session_path = path
        pack = load_handoff_pack(path.parent / "pack.json")
    else:
        raise ValueError("Expected a handoff pack.json or session.json path.")
    session = load_handoff_session(session_path)
    lifecycle_path = Path(str(pack.get("lifecycle_path") or path.parent / "lifecycle.json"))
    lifecycle = json.loads(lifecycle_path.read_text(encoding="utf-8")) if lifecycle_path.exists() else {}
    response_path = Path(str(session.get("response_path") or ""))
    next_action = "await_response"
    next_command = session.get("suggested_commands", {}).get("watch_and_review")
    notes: list[str] = []
    status = str(session.get("status") or "pending_response")
    if response_path.exists() and status in {"pending_response", "retry_ready", "reviewing"}:
        next_action = "review_response"
        next_command = session.get("suggested_commands", {}).get("watch_and_review")
        notes.append("A response file already exists and can be reviewed now.")
    elif status == "retry_ready":
        next_action = "retry_with_smaller_prompt"
        next_command = session.get("suggested_commands", {}).get("watch_and_review")
        notes.append("Use the last repair pack or adaptive prompt before retrying.")
    elif status == "human_review_required":
        next_action = "human_review"
        next_command = None
        notes.append("Max retry attempts reached; inspect repair pack and review artifacts manually.")
    elif status == "resolved":
        next_action = "resolved"
        next_command = None
        notes.append("This handoff session is already resolved.")

    payload = {
        "generated_at": timestamp_now(),
        "pack_path": str((path.parent / "pack.json").resolve() if path.name == "session.json" else path.resolve()),
        "session_path": str(session_path.resolve()),
        "title": pack.get("title"),
        "status": status,
        "state": session.get("state"),
        "attempt_count": session.get("attempt_count"),
        "max_attempts": session.get("max_attempts"),
        "response_exists": response_path.exists(),
        "next_action": next_action,
        "next_command": next_command,
        "notes": notes,
        "lifecycle_state": lifecycle.get("state"),
    }
    report_json = session_path.parent / "resume_report.json"
    report_md = session_path.parent / "resume_report.md"
    report_json.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    report_md.write_text(render_resume_report_markdown(payload), encoding="utf-8")
    payload["json_path"] = str(report_json.resolve())
    payload["md_path"] = str(report_md.resolve())
    return payload


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
        "- `lifecycle.json`: pack lifecycle state for operators and watch-and-review",
        "- `session.json`: response target, retry policy, and ready-to-run commands",
        "",
        "## Source Artifacts",
    ]
    for item in payload.get("source_artifacts", []):
        lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def render_resume_report_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Handoff Resume Report",
        "",
        f"- Title: `{payload['title']}`",
        f"- Status: `{payload['status']}`",
        f"- State: `{payload['state']}`",
        f"- Attempts: `{payload['attempt_count']}` / `{payload['max_attempts']}`",
        f"- Response exists: `{payload['response_exists']}`",
        f"- Next action: `{payload['next_action']}`",
    ]
    if payload.get("next_command"):
        lines.extend(["", "## Next Command", f"`{payload['next_command']}`"])
    if payload.get("notes"):
        lines.extend(["", "## Notes"])
        for item in payload["notes"]:
            lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
