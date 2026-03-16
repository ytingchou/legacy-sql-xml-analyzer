from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .evolution import review_llm_response_from_analysis
from .handoff import export_vscode_cline_pack
from .java_bff_runtime import review_java_bff_response_from_analysis
from .prompting import resolve_analysis_root


def watch_and_review(
    analysis_root: Path,
    *,
    response_path: Path,
    cluster_id: str | None = None,
    stage: str | None = None,
    prompt_json: Path | None = None,
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
        result = review_java_bff_response_from_analysis(analysis_root, prompt_json.resolve(), response_path)
        review = result["review"]
        payload = {
            "generated_at": timestamp_now(),
            "kind": "java-bff",
            "status": review["status"],
            "review_path": review.get("review_json_path"),
            "response_path": str(response_path),
            "repair_pack": None,
        }
        if emit_repair_pack and review["status"] == "needs_revision":
            repair = export_vscode_cline_pack(analysis_root, review_path=Path(str(review["review_json_path"])))
            payload["repair_pack"] = repair["written_paths"]
        return write_watch_payload(analysis_root, payload)

    if not cluster_id or not stage:
        raise ValueError("Generic watch-and-review requires --cluster and --stage.")
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
    }
    if emit_repair_pack and review["status"] == "needs_revision":
        repair = export_vscode_cline_pack(analysis_root, review_path=Path(str(review["review_json_path"])))
        payload["repair_pack"] = repair["written_paths"]
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
    return "\n".join(lines).rstrip() + "\n"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
