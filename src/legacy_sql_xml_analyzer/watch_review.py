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
from .handoff import export_vscode_cline_pack, update_handoff_lifecycle
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
        elif source_pack_path is not None and review["status"] in {"accepted", "insufficient_evidence"}:
            update_handoff_lifecycle(
                source_pack_path.resolve(),
                state="resolved",
                event="review_accepted",
                notes=[f"Review status={review['status']}"],
                related_artifacts=[str(review.get("review_json_path") or ""), str(response_path)],
            )
        return write_watch_payload(analysis_root, payload)

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
    elif source_pack_path is not None and review["status"] in {"accepted", "insufficient_evidence"}:
        update_handoff_lifecycle(
            source_pack_path.resolve(),
            state="resolved",
            event="review_accepted",
            notes=[f"Review status={review['status']}"],
            related_artifacts=[str(review.get("review_json_path") or ""), str(response_path)],
        )
    return write_watch_payload(analysis_root, payload)


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
