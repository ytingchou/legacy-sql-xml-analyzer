from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def make_analysis_root(root: Path) -> Path:
    analysis_root = root / "analysis"
    (analysis_root / "markdown" / "queries").mkdir(parents=True, exist_ok=True)
    return analysis_root


def write_failure_clusters(analysis_root: Path, clusters: list[dict[str, Any]]) -> Path:
    path = analysis_root / "failure_clusters.json"
    payload = {
        "generated_at": "2026-03-16T00:00:00+00:00",
        "summary": {
            "cluster_count": len(clusters),
            "diagnostic_count": sum(int(item.get("occurrence_count", 0)) for item in clusters),
        },
        "clusters": clusters,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def write_query_card(analysis_root: Path, query_id: str, text: str) -> Path:
    queries_root = analysis_root / "markdown" / "queries"
    queries_root.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(char if char.isalnum() or char in "._-" else "_" for char in query_id)
    path = queries_root / f"{safe_name}.md"
    path.write_text(text, encoding="utf-8")
    return path


def write_profile(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))
