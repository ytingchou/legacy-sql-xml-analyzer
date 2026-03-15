from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import AnalysisResult, ArtifactDescriptor, DiagnosticModel


SEVERITY_PRIORITY = {"fatal": 4, "error": 3, "warning": 2, "info": 1}
BUDGET_TO_EXAMPLES = {"8k": 1, "32k": 3, "128k": 5}

RULE_DIGEST = [
    "References can be local or external and may point to main-query or sub-query targets.",
    "Profiles should prefer safe, evidence-backed mappings and avoid guessing business SQL semantics.",
    "A weak LLM should propose only minimal, testable rule/profile changes or minimal SQL/XML fixes.",
    "If evidence is insufficient, the model must say so instead of inventing hidden XML structure.",
]


def write_failure_clusters(output_dir: Path, result: AnalysisResult) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    analysis_root.mkdir(parents=True, exist_ok=True)
    prompt_root = analysis_root / "prompt_packs"
    prompt_root.mkdir(parents=True, exist_ok=True)

    clusters = build_failure_clusters(result)
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "summary": {
            "cluster_count": len(clusters),
            "diagnostic_count": len(result.diagnostics),
        },
        "clusters": clusters,
    }

    clusters_json_path = analysis_root / "failure_clusters.json"
    clusters_md_path = analysis_root / "failure_clusters.md"
    clusters_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    clusters_md_path.write_text(render_failure_clusters_markdown(payload), encoding="utf-8")

    artifacts = [
        artifact_descriptor_for_path(clusters_json_path, "json", "Failure clusters", "prompting"),
        artifact_descriptor_for_path(clusters_md_path, "markdown", "Failure clusters (Markdown)", "prompting"),
    ]

    for cluster in clusters[:5]:
        prompt_artifacts = prepare_prompt_pack(
            analysis_root=analysis_root,
            cluster=cluster,
            budget="128k",
            model="weak-128k",
            write_to_disk=True,
        )
        artifacts.extend(prompt_artifacts)
    return artifacts


def prepare_prompt_pack(
    analysis_root: Path,
    cluster: dict[str, Any],
    budget: str = "128k",
    model: str = "weak-128k",
    write_to_disk: bool = True,
) -> list[ArtifactDescriptor]:
    prompt_root = analysis_root / "prompt_packs"
    prompt_root.mkdir(parents=True, exist_ok=True)
    cluster_id = cluster["cluster_id"]
    sample_limit = BUDGET_TO_EXAMPLES.get(budget, 3)
    samples = cluster["sample_diagnostics"][:sample_limit]
    prompt_text = render_prompt_pack_text(cluster=cluster, samples=samples, budget=budget, model=model)
    prompt_payload = {
        "cluster_id": cluster_id,
        "budget": budget,
        "model": model,
        "task_type": cluster["task_type"],
        "answer_schema": answer_schema_for_cluster(cluster),
        "samples": samples,
        "prompt_text": prompt_text,
    }

    text_path = prompt_root / f"{cluster_id}-{sanitize_token(budget)}-{sanitize_token(model)}.txt"
    json_path = prompt_root / f"{cluster_id}-{sanitize_token(budget)}-{sanitize_token(model)}.json"
    if write_to_disk:
        text_path.write_text(prompt_text, encoding="utf-8")
        json_path.write_text(json.dumps(prompt_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    return [
        artifact_descriptor_for_path(text_path, "text", f"Prompt pack: {cluster_id}", "prompting"),
        artifact_descriptor_for_path(json_path, "json", f"Prompt pack metadata: {cluster_id}", "prompting"),
    ]


def load_failure_clusters(analysis_root: Path) -> dict[str, Any]:
    clusters_path = resolve_analysis_root(analysis_root) / "failure_clusters.json"
    return json.loads(clusters_path.read_text(encoding="utf-8"))


def prepare_prompt_pack_from_analysis(
    analysis_root: Path,
    cluster_id: str,
    budget: str = "128k",
    model: str = "weak-128k",
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    payload = load_failure_clusters(analysis_root)
    cluster = next((item for item in payload["clusters"] if item["cluster_id"] == cluster_id), None)
    if cluster is None:
        raise ValueError(f"Unknown cluster_id: {cluster_id}")
    artifacts = prepare_prompt_pack(
        analysis_root=analysis_root,
        cluster=cluster,
        budget=budget,
        model=model,
        write_to_disk=True,
    )
    return {"cluster": cluster, "artifacts": artifacts}


def build_failure_clusters(result: AnalysisResult) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[DiagnosticModel]] = defaultdict(list)
    for diagnostic in result.diagnostics:
        key = (
            diagnostic.code,
            diagnostic.severity,
            diagnostic.tag or "",
        )
        grouped[key].append(diagnostic)

    clusters: list[dict[str, Any]] = []
    for (code, severity, tag), diagnostics in grouped.items():
        sample_context_keys = Counter(
            key
            for diagnostic in diagnostics
            for key in diagnostic.context.keys()
        )
        cluster_id = sanitize_token(code.lower())
        task_type = infer_task_type(code)
        suggested_fix = most_common_text([item.suggested_fix for item in diagnostics if item.suggested_fix])
        representative_message = most_common_text([item.message for item in diagnostics if item.message]) or code
        sample_diagnostics = [
            {
                "source_path": str(item.source_path),
                "query_id": item.query_id,
                "tag": item.tag,
                "message": item.message,
                "context": item.context,
                "suggested_fix": item.suggested_fix,
                "prompt_hint": item.prompt_hint,
            }
            for item in diagnostics[:5]
        ]
        clusters.append(
            {
                "cluster_id": cluster_id,
                "code": code,
                "severity": severity,
                "tag": tag or None,
                "task_type": task_type,
                "occurrence_count": len(diagnostics),
                "files_affected": len({str(item.source_path) for item in diagnostics}),
                "queries_affected": len({item.query_id for item in diagnostics if item.query_id}),
                "representative_message": representative_message,
                "suggested_fix": suggested_fix,
                "common_context_keys": [item[0] for item in sample_context_keys.most_common(8)],
                "sample_diagnostics": sample_diagnostics,
            }
        )

    clusters.sort(
        key=lambda item: (
            -SEVERITY_PRIORITY.get(item["severity"], 0),
            -int(item["occurrence_count"]),
            item["code"],
        )
    )
    return clusters


def render_failure_clusters_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Failure Clusters",
        "",
        "## Summary",
        f"- Clusters: {payload['summary']['cluster_count']}",
        f"- Diagnostics: {payload['summary']['diagnostic_count']}",
        "",
        "## Clusters",
    ]
    for cluster in payload["clusters"]:
        lines.append(
            f"- `{cluster['cluster_id']}` code={cluster['code']} severity={cluster['severity']} "
            f"occurrences={cluster['occurrence_count']} task={cluster['task_type']}"
        )
        lines.append(f"  message: {cluster['representative_message']}")
    return "\n".join(lines).rstrip() + "\n"


def render_prompt_pack_text(
    cluster: dict[str, Any],
    samples: list[dict[str, Any]],
    budget: str,
    model: str,
) -> str:
    schema = json.dumps(answer_schema_for_cluster(cluster), indent=2, ensure_ascii=False)
    sample_blocks = []
    for index, sample in enumerate(samples, start=1):
        sample_blocks.append(
            "\n".join(
                [
                    f"Example {index}:",
                    f"- source_path: {sample['source_path']}",
                    f"- query_id: {sample['query_id'] or 'n/a'}",
                    f"- tag: {sample['tag'] or 'n/a'}",
                    f"- message: {sample['message']}",
                    f"- context: {json.dumps(sample['context'], ensure_ascii=False)}",
                    f"- suggested_fix: {sample['suggested_fix'] or 'n/a'}",
                ]
            )
        )

    lines = [
        f"Task type: {cluster['task_type']}",
        f"Target model profile: {model}",
        f"Token budget: {budget}",
        "",
        "You are helping improve a legacy SQL XML analyzer.",
        "Solve only the single problem described below. Do not rewrite unrelated SQL or invent hidden XML structures.",
        "",
        "Problem summary:",
        f"- Diagnostic code: {cluster['code']}",
        f"- Severity: {cluster['severity']}",
        f"- Representative message: {cluster['representative_message']}",
        f"- Occurrences: {cluster['occurrence_count']}",
        f"- Files affected: {cluster['files_affected']}",
        f"- Queries affected: {cluster['queries_affected']}",
        f"- Common context keys: {', '.join(cluster['common_context_keys']) or 'none'}",
        f"- Suggested fix from analyzer: {cluster['suggested_fix'] or 'n/a'}",
        "",
        "Hard constraints:",
    ]
    for rule in RULE_DIGEST:
        lines.append(f"- {rule}")

    lines.extend(["", "Evidence samples:"])
    if sample_blocks:
        for block in sample_blocks:
            lines.append(block)
            lines.append("")
    else:
        lines.append("- No sample diagnostics available.")

    lines.extend(
        [
            "Return JSON only with this schema:",
            schema,
            "",
            "If evidence is insufficient, set \"insufficient_evidence\" to true and explain exactly what extra evidence is needed.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def answer_schema_for_cluster(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        "problem_type": cluster["task_type"],
        "root_cause": "string",
        "proposed_change_type": "profile_rule | xml_fix | sql_fix | insufficient_evidence",
        "proposed_rule_or_fix": {
            "rule_type": "string",
            "scope": "global | source_scoped | local",
            "payload": {},
        },
        "confidence": "low | medium | high",
        "why": ["string"],
        "verification_steps": ["string"],
        "risks": ["string"],
        "insufficient_evidence": False,
    }


def infer_task_type(code: str) -> str:
    if code in {"REFERENCE_TARGET_MISSING", "EXT_XML_MISSING", "REFERENCE_TOKEN_NOT_FOUND"}:
        return "mapping_inference"
    if code.startswith("PARAMETER_") or code == "SQL_PARAMETER_UNDEFINED":
        return "parameter_modeling"
    if code in {"DATASET_CAST_MISSING", "DML_SEMICOLON_MISSING", "COMMENT_FORBIDDEN_CHAR"}:
        return "sql_hygiene"
    if code in {"REF_BOTH_TARGETS", "COPY_SUBQUERY_UNSUPPORTED", "TARGET_MISSING"}:
        return "rule_conflict"
    return "generic_diagnostic"


def artifact_descriptor_for_path(path: Path, kind: str, title: str, scope: str) -> ArtifactDescriptor:
    content = path.read_text(encoding="utf-8")
    estimated_tokens = max(1, round(len(content) / 4)) if content else 0
    return ArtifactDescriptor(
        kind=kind,
        path=str(path),
        title=title,
        estimated_tokens=estimated_tokens,
        safe_for_128k_single_pass=estimated_tokens <= 100_000,
        needs_selective_prompting=estimated_tokens > 40_000,
        scope=scope,
    )


def sanitize_token(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-._")


def most_common_text(values: list[str]) -> str | None:
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def resolve_analysis_root(path: Path) -> Path:
    path = path.resolve()
    if (path / "failure_clusters.json").exists():
        return path
    if (path / "analysis" / "failure_clusters.json").exists():
        return path / "analysis"
    return path
