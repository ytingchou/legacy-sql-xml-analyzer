from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .adaptive_prompt import (
    compile_adaptive_generic_context,
    compile_adaptive_java_context,
    plan_prompt_downgrade,
    write_adaptive_payload,
)
from .evolution import review_llm_response_from_analysis
from .handoff import (
    export_vscode_cline_pack,
    list_handoff_sessions,
    load_handoff_session,
    update_handoff_lifecycle,
    update_handoff_session,
)
from .java_bff_runtime import review_java_bff_response_from_analysis
from .prompting import resolve_analysis_root
from .context_compiler import estimate_tokens


def watch_and_review(
    analysis_root: Path,
    *,
    response_path: Path,
    cluster_id: str | None = None,
    stage: str | None = None,
    prompt_json: Path | None = None,
    source_pack_path: Path | None = None,
    timeout_seconds: float = 300.0,
    poll_seconds: float = 2.0,
    emit_repair_pack: bool = True,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    started = time.time()
    response_path = response_path.resolve()
    session_path = source_pack_path.resolve().parent / "session.json" if source_pack_path is not None else None
    while not response_path.exists():
        if time.time() - started > timeout_seconds:
            raise TimeoutError(f"Timed out waiting for response file: {response_path}")
        time.sleep(poll_seconds)

    if prompt_json is not None:
        if source_pack_path is not None:
            update_handoff_lifecycle(
                source_pack_path.resolve(),
                state="used",
                event="response_received",
                notes=["A response file was detected for this handoff pack."],
                related_artifacts=[str(response_path)],
            )
            if session_path is not None and session_path.exists():
                update_handoff_session(
                    session_path,
                    status="reviewing",
                    state="used",
                    attempt_increment=1,
                    notes=["Response received; starting Java BFF review."],
                )
        result = review_java_bff_response_from_analysis(analysis_root, prompt_json.resolve(), response_path)
        review = result["review"]
        payload = {
            "generated_at": timestamp_now(),
            "kind": "java-bff",
            "status": review["status"],
            "review_path": review.get("review_json_path"),
            "response_path": str(response_path),
            "repair_pack": None,
            "adaptive_retry": None,
            "source_pack_path": str(source_pack_path.resolve()) if source_pack_path else None,
        }
        if emit_repair_pack and review["status"] == "needs_revision":
            repair = export_vscode_cline_pack(
                analysis_root,
                review_path=Path(str(review["review_json_path"])),
                initial_state="repaired",
            )
            payload["repair_pack"] = repair["written_paths"]
            adaptive_paths = build_java_adaptive_retry(
                analysis_root=analysis_root,
                prompt_json=prompt_json.resolve(),
                review_payload=review,
            )
            payload["adaptive_retry"] = adaptive_paths
            if source_pack_path is not None:
                update_handoff_lifecycle(
                    source_pack_path.resolve(),
                    state="repaired",
                    event="review_needs_revision",
                    notes=["The reviewed response needs revision; use the generated repair pack or adaptive prompt."],
                    related_artifacts=[str(review.get("review_json_path") or ""), *repair["written_paths"], *adaptive_paths],
                )
                if session_path is not None and session_path.exists():
                    session_payload = load_handoff_session(session_path)
                    max_attempts = int(session_payload.get("max_attempts", 3))
                    next_status = "human_review_required" if int(session_payload.get("attempt_count", 0)) >= max_attempts else "retry_ready"
                    next_state = "repaired" if next_status == "retry_ready" else "resolved"
                    update_handoff_session(
                        session_path,
                        status=next_status,
                        state=next_state,
                        review_path=str(review.get("review_json_path") or ""),
                        watch_report_path=None,
                        adaptive_retry=adaptive_paths,
                        repair_pack=repair["written_paths"],
                        notes=[f"Java review returned {review['status']}."],
                    )
        elif source_pack_path is not None and review["status"] in {"accepted", "insufficient_evidence"}:
            update_handoff_lifecycle(
                source_pack_path.resolve(),
                state="resolved",
                event="review_accepted",
                notes=[f"Review status={review['status']}"],
                related_artifacts=[str(review.get("review_json_path") or ""), str(response_path)],
            )
            if session_path is not None and session_path.exists():
                update_handoff_session(
                    session_path,
                    status="resolved",
                    state="resolved",
                    review_path=str(review.get("review_json_path") or ""),
                    watch_report_path=None,
                    notes=[f"Java review returned {review['status']}."],
                )
        written = write_watch_payload(analysis_root, payload)
        if session_path is not None and session_path.exists():
            update_handoff_session(
                session_path,
                status=load_handoff_session(session_path).get("status", "reviewed"),
                state=load_handoff_session(session_path).get("state"),
                watch_report_path=str(written["json_path"]),
                notes=["Watch-and-review report written."],
            )
        return written

    if not cluster_id or not stage:
        raise ValueError("Generic watch-and-review requires --cluster and --stage.")
    if source_pack_path is not None:
        update_handoff_lifecycle(
            source_pack_path.resolve(),
            state="used",
            event="response_received",
            notes=["A response file was detected for this handoff pack."],
            related_artifacts=[str(response_path)],
        )
        if session_path is not None and session_path.exists():
            update_handoff_session(
                session_path,
                status="reviewing",
                state="used",
                attempt_increment=1,
                notes=["Response received; starting generic review."],
            )
    result = review_llm_response_from_analysis(
        analysis_root=analysis_root,
        cluster_id=cluster_id,
        response_path=response_path,
        stage=stage,
        budget="128k",
        model="company-watch-review",
    )
    review = result["review"]
    payload = {
        "generated_at": timestamp_now(),
        "kind": "generic",
        "cluster_id": cluster_id,
        "stage": stage,
        "status": review["status"],
        "review_path": review.get("review_json_path"),
        "response_path": str(response_path),
        "repair_pack": None,
        "adaptive_retry": None,
        "source_pack_path": str(source_pack_path.resolve()) if source_pack_path else None,
    }
    if emit_repair_pack and review["status"] == "needs_revision":
        repair = export_vscode_cline_pack(
            analysis_root,
            review_path=Path(str(review["review_json_path"])),
            initial_state="repaired",
        )
        payload["repair_pack"] = repair["written_paths"]
        adaptive_paths = build_generic_adaptive_retry(
            analysis_root=analysis_root,
            cluster_id=cluster_id,
            stage=stage,
            review_payload=review,
        )
        payload["adaptive_retry"] = adaptive_paths
        if source_pack_path is not None:
            update_handoff_lifecycle(
                source_pack_path.resolve(),
                state="repaired",
                event="review_needs_revision",
                notes=["The reviewed response needs revision; use the generated repair pack or adaptive prompt."],
                related_artifacts=[str(review.get("review_json_path") or ""), *repair["written_paths"], *adaptive_paths],
            )
            if session_path is not None and session_path.exists():
                session_payload = load_handoff_session(session_path)
                max_attempts = int(session_payload.get("max_attempts", 3))
                next_status = "human_review_required" if int(session_payload.get("attempt_count", 0)) >= max_attempts else "retry_ready"
                next_state = "repaired" if next_status == "retry_ready" else "resolved"
                update_handoff_session(
                    session_path,
                    status=next_status,
                    state=next_state,
                    review_path=str(review.get("review_json_path") or ""),
                    adaptive_retry=adaptive_paths,
                    repair_pack=repair["written_paths"],
                    notes=[f"Generic review returned {review['status']}."],
                )
    elif source_pack_path is not None and review["status"] in {"accepted", "insufficient_evidence"}:
        update_handoff_lifecycle(
            source_pack_path.resolve(),
            state="resolved",
            event="review_accepted",
            notes=[f"Review status={review['status']}"],
            related_artifacts=[str(review.get("review_json_path") or ""), str(response_path)],
        )
        if session_path is not None and session_path.exists():
            update_handoff_session(
                session_path,
                status="resolved",
                state="resolved",
                review_path=str(review.get("review_json_path") or ""),
                notes=[f"Generic review returned {review['status']}."],
            )
    written = write_watch_payload(analysis_root, payload)
    if session_path is not None and session_path.exists():
        update_handoff_session(
            session_path,
            status=load_handoff_session(session_path).get("status", "reviewed"),
            state=load_handoff_session(session_path).get("state"),
            watch_report_path=str(written["json_path"]),
            notes=["Watch-and-review report written."],
        )
    return written


def watch_cline_directory(
    analysis_root: Path,
    *,
    timeout_seconds: float = 300.0,
    poll_seconds: float = 2.0,
    emit_repair_pack: bool = True,
    process_once: bool = False,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    started = time.time()
    processed: list[dict[str, Any]] = []
    while True:
        sessions = list_handoff_sessions(analysis_root)
        pending = [
            session
            for session in sessions
            if str(session.get("status") or "pending_response") in {"pending_response", "retry_ready", "reviewing"}
        ]
        for session in pending:
            response_path = Path(str(session.get("response_path") or ""))
            if not response_path.exists():
                continue
            pack_path = Path(str(session.get("pack_json_path") or ""))
            if session.get("kind") == "generic_cluster":
                result = watch_and_review(
                    analysis_root=analysis_root,
                    response_path=response_path,
                    cluster_id=str(session.get("cluster_id") or ""),
                    stage=str(session.get("stage") or ""),
                    source_pack_path=pack_path,
                    timeout_seconds=0.0,
                    poll_seconds=poll_seconds,
                    emit_repair_pack=emit_repair_pack,
                )
            else:
                source_pack = load_handoff_session(Path(str(session.get("session_path") or pack_path.parent / "session.json")))
                prompt_json = Path(str(source_pack.get("phase_pack_path") or ""))
                result = watch_and_review(
                    analysis_root=analysis_root,
                    response_path=response_path,
                    prompt_json=prompt_json,
                    source_pack_path=pack_path,
                    timeout_seconds=0.0,
                    poll_seconds=poll_seconds,
                    emit_repair_pack=emit_repair_pack,
                )
            processed.append(
                {
                    "title": session.get("title"),
                    "session_path": session.get("session_path"),
                    "response_path": str(response_path),
                    "result_path": result.get("json_path"),
                    "status": result.get("status"),
                }
            )
        if processed and process_once:
            break
        if process_once:
            break
        if processed:
            break
        if time.time() - started > timeout_seconds:
            break
        time.sleep(poll_seconds)

    payload = {
        "generated_at": timestamp_now(),
        "analysis_root": str(analysis_root.resolve()),
        "processed_count": len(processed),
        "pending_session_count": len(
            [
                session
                for session in list_handoff_sessions(analysis_root)
                if str(session.get("status") or "pending_response") in {"pending_response", "retry_ready", "reviewing"}
            ]
        ),
        "processed": processed,
    }
    root = analysis_root / "watch_review"
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "session_watch.json"
    md_path = root / "session_watch.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_session_watch_markdown(payload), encoding="utf-8")
    payload["json_path"] = str(json_path.resolve())
    payload["md_path"] = str(md_path.resolve())
    return payload


def write_watch_payload(analysis_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    root = analysis_root / "watch_review"
    root.mkdir(parents=True, exist_ok=True)
    slug_parts = [payload["kind"], payload.get("cluster_id") or payload.get("stage") or "review"]
    slug = "-".join(str(part) for part in slug_parts if part)
    json_path = root / f"{slug}.json"
    md_path = root / f"{slug}.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_watch_markdown(payload), encoding="utf-8")
    payload["json_path"] = str(json_path.resolve())
    payload["md_path"] = str(md_path.resolve())
    return payload


def render_watch_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Watch And Review",
        "",
        f"- Kind: `{payload['kind']}`",
        f"- Status: `{payload['status']}`",
        f"- Response: `{payload['response_path']}`",
        f"- Review: `{payload.get('review_path') or 'n/a'}`",
    ]
    repair_pack = payload.get("repair_pack")
    if isinstance(repair_pack, list) and repair_pack:
        lines.extend(["", "## Repair Pack"])
        for item in repair_pack:
            lines.append(f"- `{item}`")
    adaptive_retry = payload.get("adaptive_retry")
    if isinstance(adaptive_retry, list) and adaptive_retry:
        lines.extend(["", "## Adaptive Retry Artifacts"])
        for item in adaptive_retry:
            lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def render_session_watch_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Session Watch",
        "",
        f"- Processed sessions: `{payload['processed_count']}`",
        f"- Pending sessions: `{payload['pending_session_count']}`",
        "",
        "## Processed",
    ]
    for item in payload.get("processed", []):
        lines.append(
            f"- `{item['title']}` status=`{item['status']}` response=`{item['response_path']}` result=`{item['result_path']}`"
        )
    if not payload.get("processed"):
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def build_generic_adaptive_retry(
    *,
    analysis_root: Path,
    cluster_id: str,
    stage: str,
    review_payload: dict[str, Any],
) -> list[str]:
    targets = plan_prompt_downgrade(estimate_review_prompt_tokens(review_payload))
    payload = compile_adaptive_generic_context(
        analysis_root=analysis_root,
        cluster_id=cluster_id,
        phase=stage,
        prompt_profile="qwen3-128k-autonomous",
        targets=targets["candidate_targets"] or None,
        prior_response=review_payload.get("parsed_response") if isinstance(review_payload.get("parsed_response"), dict) else None,
    )
    return [str(path.resolve()) for path in write_adaptive_payload(analysis_root, payload)]


def build_java_adaptive_retry(
    *,
    analysis_root: Path,
    prompt_json: Path,
    review_payload: dict[str, Any],
) -> list[str]:
    targets = plan_prompt_downgrade(estimate_review_prompt_tokens(review_payload))
    payload = compile_adaptive_java_context(
        analysis_root=analysis_root,
        prompt_json=prompt_json,
        prompt_profile="qwen3-128k-java-bff",
        targets=targets["candidate_targets"] or None,
    )
    return [str(path.resolve()) for path in write_adaptive_payload(analysis_root, payload)]


def estimate_review_prompt_tokens(review_payload: dict[str, Any]) -> int | None:
    for key in ("repair_prompt_text", "next_prompt_text"):
        text = review_payload.get(key)
        if isinstance(text, str) and text.strip():
            return estimate_tokens(text)
    for key in ("repair_prompt_path", "next_prompt_path"):
        path_value = review_payload.get(key)
        if path_value and Path(str(path_value)).exists():
            return estimate_tokens(Path(str(path_value)).read_text(encoding="utf-8"))
    return None


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
