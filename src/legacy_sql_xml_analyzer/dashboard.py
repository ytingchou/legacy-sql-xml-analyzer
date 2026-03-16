from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .doctor import doctor_run
from .failure_explainer import explain_failure_from_output_dir
from .learning import load_profile
from .models import AnalysisResult, ArtifactDescriptor, ResolvedQueryModel


JOIN_PATTERN = re.compile(r"\bjoin\b", re.IGNORECASE)
SUBQUERY_PATTERN = re.compile(r"\(\s*select\b", re.IGNORECASE)
UNION_PATTERN = re.compile(r"\bunion(?:\s+all)?\b", re.IGNORECASE)
CASE_PATTERN = re.compile(r"\bcase\b", re.IGNORECASE)
PREDICATE_PATTERN = re.compile(r"\b(?:and|or)\b", re.IGNORECASE)


def safe_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in "._-" else "_" for char in value)


def write_executive_report(
    output_dir: Path,
    result: AnalysisResult,
    profile_path: Path | None,
) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    analysis_root.mkdir(parents=True, exist_ok=True)
    evolution_summary = build_evolution_summary(output_dir)
    summary = build_executive_summary(
        output_dir=output_dir,
        result=result,
        profile_path=profile_path,
        evolution_summary=evolution_summary,
    )

    json_path = analysis_root / "executive_summary.json"
    md_path = analysis_root / "executive_summary.md"
    html_path = analysis_root / "dashboard.html"
    complexity_csv_path = analysis_root / "executive_complexity.csv"
    value_csv_path = analysis_root / "executive_value.csv"
    diagnostics_csv_path = analysis_root / "executive_diagnostics.csv"
    trend_csv_path = analysis_root / "executive_trend.csv"
    llm_effectiveness_csv_path = analysis_root / "llm_effectiveness.csv"
    profile_lifecycle_csv_path = analysis_root / "profile_lifecycle.csv"
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_executive_summary_markdown(summary), encoding="utf-8")
    html_path.write_text(render_dashboard_html(summary), encoding="utf-8")
    write_csv(
        complexity_csv_path,
        summary["complexity_summary"]["top_complex_queries"],
        [
            "query_id",
            "file",
            "query_type",
            "status",
            "complexity_score",
            "complexity_risk",
            "value_score",
            "dependencies",
            "inbound_references",
            "joins",
            "subqueries",
            "unions",
            "parameters",
            "tables",
            "line_count",
            "statement_type",
        ],
    )
    write_csv(
        value_csv_path,
        summary["value_summary"]["top_value_queries"],
        [
            "query_id",
            "file",
            "query_type",
            "status",
            "value_score",
            "complexity_score",
            "complexity_risk",
            "inbound_references",
            "dependencies",
            "parameters",
            "statement_type",
        ],
    )
    write_csv(
        diagnostics_csv_path,
        summary["diagnostics_summary"]["top_codes"],
        ["code", "count"],
    )
    write_csv(
        trend_csv_path,
        summary["trend_summary"]["history"],
        ["snapshot_id", "generated_at", "label", "resolved_queries", "partial_queries", "failed_queries", "error_count", "warning_count"],
    )
    write_csv(
        llm_effectiveness_csv_path,
        evolution_summary["provider_scoreboard"],
        [
            "provider_name",
            "provider_model",
            "stage",
            "run_count",
            "reviewed_runs",
            "accepted_reviews",
            "needs_revision_reviews",
            "rejected_reviews",
            "accepted_review_rate",
            "avg_prompt_tokens",
            "avg_completion_tokens",
            "avg_requested_tokens",
            "avg_total_tokens",
        ],
    )
    write_csv(
        profile_lifecycle_csv_path,
        summary["profile_lifecycle"]["history_rows"],
        [
            "generated_at",
            "event_type",
            "from_status",
            "to_status",
            "source_profile_path",
            "target_profile_path",
            "assessment_classification",
            "promotion_readiness",
            "reason",
        ],
    )
    evolution_artifacts = write_evolution_report(output_dir, evolution_summary=evolution_summary)

    return [
        artifact_descriptor_for_path(json_path, "json", "Executive summary", "executive"),
        artifact_descriptor_for_path(md_path, "markdown", "Executive summary (Markdown)", "executive"),
        artifact_descriptor_for_path(html_path, "html", "Executive dashboard", "executive"),
        artifact_descriptor_for_path(complexity_csv_path, "csv", "Executive complexity export", "executive"),
        artifact_descriptor_for_path(value_csv_path, "csv", "Executive value export", "executive"),
        artifact_descriptor_for_path(diagnostics_csv_path, "csv", "Executive diagnostics export", "executive"),
        artifact_descriptor_for_path(trend_csv_path, "csv", "Executive trend export", "executive"),
        artifact_descriptor_for_path(llm_effectiveness_csv_path, "csv", "LLM effectiveness export", "executive"),
        artifact_descriptor_for_path(profile_lifecycle_csv_path, "csv", "Profile lifecycle export", "executive"),
    ] + evolution_artifacts


def build_executive_summary(
    output_dir: Path,
    result: AnalysisResult,
    profile_path: Path | None,
    evolution_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inbound_references = Counter(
        dependency
        for resolved in result.resolved_queries
        for dependency in set(resolved.dependencies)
    )
    file_diagnostics = Counter(str(diagnostic.source_path) for diagnostic in result.diagnostics)
    trend_summary = build_trend_summary(output_dir)
    evolution_summary = evolution_summary or build_evolution_summary(output_dir)
    profile_lifecycle = build_profile_lifecycle_summary(profile_path)
    agent_loop_summary = build_agent_loop_summary(output_dir)
    java_bff_summary = build_java_bff_summary(output_dir)
    snapshot_count = trend_summary["snapshot_count"]

    complexity_rows = [
        complexity_row(resolved, inbound_references[resolved.query.id])
        for resolved in result.resolved_queries
    ]
    complexity_rows.sort(key=lambda item: (-int(item["complexity_score"]), item["query_id"]))
    value_rows = sorted(
        complexity_rows,
        key=lambda item: (-int(item["value_score"]), -int(item["complexity_score"]), item["query_id"]),
    )

    complexity_bands = Counter(item["complexity_risk"] for item in complexity_rows)
    severity_counts = Counter(diagnostic.severity for diagnostic in result.diagnostics)
    top_diagnostics = Counter(diagnostic.code for diagnostic in result.diagnostics).most_common(8)

    high_risk_queries = [item for item in complexity_rows if item["complexity_risk"] in {"high", "critical"}]
    management_summary = build_management_summary(
        result=result,
        snapshot_count=snapshot_count,
        top_diagnostics=top_diagnostics,
        high_risk_queries=high_risk_queries,
        complexity_rows=complexity_rows,
        file_diagnostics=file_diagnostics,
        evolution_summary=evolution_summary,
        agent_loop_summary=agent_loop_summary,
        java_bff_summary=java_bff_summary,
    )
    next_actions = build_next_actions(
        profile_path=profile_path,
        top_diagnostics=top_diagnostics,
        high_risk_queries=high_risk_queries,
        file_diagnostics=file_diagnostics,
        result=result,
        evolution_summary=evolution_summary,
        agent_loop_summary=agent_loop_summary,
        java_bff_summary=java_bff_summary,
    )

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "profile_path": str(profile_path) if profile_path else None,
        "headline": {
            "files_scanned": len(result.files),
            "queries_discovered": len(result.queries),
            "resolved_queries": sum(1 for item in result.resolved_queries if item.status == "resolved"),
            "partial_queries": sum(1 for item in result.resolved_queries if item.status == "partial"),
            "failed_queries": sum(1 for item in result.resolved_queries if item.status == "failed"),
            "error_count": severity_counts.get("error", 0) + severity_counts.get("fatal", 0),
            "warning_count": severity_counts.get("warning", 0),
            "snapshot_count": snapshot_count,
        },
        "management_summary": management_summary,
        "trend_summary": trend_summary,
        "complexity_summary": {
            "average_score": round(
                sum(item["complexity_score"] for item in complexity_rows) / max(len(complexity_rows), 1),
                1,
            ),
            "max_score": max((item["complexity_score"] for item in complexity_rows), default=0),
            "bands": dict(sorted(complexity_bands.items())),
            "top_complex_queries": complexity_rows[:10],
        },
        "value_summary": {
            "top_value_queries": value_rows[:10],
            "top_files_by_diagnostics": [
                {"file": file_path, "diagnostic_count": count}
                for file_path, count in file_diagnostics.most_common(10)
            ],
        },
        "diagnostics_summary": {
            "by_severity": dict(sorted(severity_counts.items())),
            "top_codes": [
                {"code": code, "count": count}
                for code, count in top_diagnostics
            ],
        },
        "evolution_summary": evolution_summary,
        "agent_loop_summary": agent_loop_summary,
        "java_bff_summary": java_bff_summary,
        "profile_lifecycle": profile_lifecycle,
        "next_actions": next_actions,
    }


def complexity_row(resolved: ResolvedQueryModel, inbound_reference_count: int) -> dict[str, Any]:
    sql = resolved.resolved_sql or resolved.query.raw_sql
    joins = len(JOIN_PATTERN.findall(sql))
    subqueries = len(SUBQUERY_PATTERN.findall(sql))
    unions = len(UNION_PATTERN.findall(sql))
    cases = len(CASE_PATTERN.findall(sql))
    predicates = len(PREDICATE_PATTERN.findall(sql))
    parameter_count = len(resolved.sql_stats.get("parameters", []))
    table_count = len(resolved.sql_stats.get("tables", []))
    dependency_count = len(set(resolved.dependencies))
    line_count = int(resolved.sql_stats.get("line_count", 0))

    complexity_score = (
        joins * 8
        + subqueries * 12
        + unions * 6
        + cases * 4
        + dependency_count * 5
        + parameter_count * 3
        + table_count * 4
        + min(line_count // 4, 18)
        + min(predicates // 3, 8)
        + (10 if resolved.status != "resolved" else 0)
    )
    if complexity_score >= 70:
        complexity_risk = "critical"
    elif complexity_score >= 45:
        complexity_risk = "high"
    elif complexity_score >= 20:
        complexity_risk = "medium"
    else:
        complexity_risk = "low"

    value_score = (
        (10 if resolved.query.query_type == "main" else 3)
        + inbound_reference_count * 8
        + dependency_count * 3
        + parameter_count * 2
        + (6 if resolved.sql_stats.get("statement_type") != "select" else 0)
    )

    return {
        "query_id": resolved.query.id,
        "file": str(resolved.query.source_path),
        "query_type": resolved.query.query_type,
        "status": resolved.status,
        "complexity_score": complexity_score,
        "complexity_risk": complexity_risk,
        "value_score": value_score,
        "joins": joins,
        "subqueries": subqueries,
        "unions": unions,
        "parameters": parameter_count,
        "dependencies": dependency_count,
        "inbound_references": inbound_reference_count,
        "tables": table_count,
        "line_count": line_count,
        "statement_type": resolved.sql_stats.get("statement_type", "unknown"),
    }


def build_management_summary(
    result: AnalysisResult,
    snapshot_count: int,
    top_diagnostics: list[tuple[str, int]],
    high_risk_queries: list[dict[str, Any]],
    complexity_rows: list[dict[str, Any]],
    file_diagnostics: Counter[str],
    evolution_summary: dict[str, Any],
    agent_loop_summary: dict[str, Any],
    java_bff_summary: dict[str, Any],
) -> list[str]:
    resolved_queries = sum(1 for item in result.resolved_queries if item.status == "resolved")
    total_queries = len(result.resolved_queries)
    summary = [
        f"{resolved_queries} of {total_queries} discovered queries are fully analyzable in the current run.",
        f"{len(high_risk_queries)} queries are high or critical complexity and should be treated as refactor hotspots.",
    ]
    if top_diagnostics:
        summary.append(
            "The most common diagnostic classes are "
            + ", ".join(f"{code} ({count})" for code, count in top_diagnostics[:3])
            + "."
        )
    if complexity_rows:
        summary.append(
            f"The most complex query is {complexity_rows[0]['query_id']} with a score of "
            f"{complexity_rows[0]['complexity_score']}."
        )
    if file_diagnostics:
        file_path, count = file_diagnostics.most_common(1)[0]
        summary.append(f"The file with the highest diagnostic concentration is {Path(file_path).name} ({count} findings).")
    if snapshot_count:
        summary.append(f"This output directory now contains {snapshot_count} historical run snapshot(s).")
    evolution_headline = evolution_summary.get("headline", {})
    if evolution_headline.get("llm_run_count"):
        summary.append(
            f"Weak-LLM evolution has {evolution_headline['llm_run_count']} saved run(s), "
            f"{evolution_headline['accepted_reviews']} accepted review(s), and "
            f"{evolution_headline['accepted_patch_count']} accepted patch proposal(s)."
        )
    if agent_loop_summary.get("available"):
        summary.append(
            f"Autonomous agent loop is {agent_loop_summary['status']} at phase "
            f"{agent_loop_summary['current_phase'] or 'n/a'} after {agent_loop_summary['iteration_count']} iteration(s)."
        )
    if java_bff_summary.get("available"):
        summary.append(
            f"Java BFF pack has {java_bff_summary['bundle_count']} bundle(s), "
            f"{java_bff_summary['accepted_review_count']} accepted review(s), "
            f"{java_bff_summary['context_pack_count']} context pack(s), and "
            f"{java_bff_summary['skeleton_bundle_count']} skeleton bundle(s)."
        )
    return summary


def build_next_actions(
    profile_path: Path | None,
    top_diagnostics: list[tuple[str, int]],
    high_risk_queries: list[dict[str, Any]],
    file_diagnostics: Counter[str],
    result: AnalysisResult,
    evolution_summary: dict[str, Any],
    agent_loop_summary: dict[str, Any],
    java_bff_summary: dict[str, Any],
) -> list[str]:
    actions: list[str] = []
    if result.diagnostics:
        actions.append("Triage fatal and error diagnostics first; they block reliable SQL expansion.")
    if high_risk_queries:
        actions.append(
            f"Review the top complex query {high_risk_queries[0]['query_id']} and consider extracting reusable sub-queries."
        )
    if top_diagnostics:
        actions.append(
            f"Target the most common rule failures first, starting with {top_diagnostics[0][0]}."
        )
    if file_diagnostics:
        actions.append(
            f"Prioritize cleanup in {Path(file_diagnostics.most_common(1)[0][0]).name}, which currently has the densest findings."
        )
    evolution_headline = evolution_summary.get("headline", {})
    if evolution_headline.get("needs_revision_reviews", 0) > 0:
        actions.append("Review weak-LLM repair prompts and retry the clusters that still return invalid or low-confidence JSON.")
    elif evolution_headline.get("accepted_patch_count", 0) > 0:
        actions.append("Simulate and grade the accepted weak-LLM patch candidates before promoting them into a trusted profile.")
    if agent_loop_summary.get("available") and agent_loop_summary.get("status") != "completed":
        actions.append(
            f"Inspect the autonomous agent loop stop reason `{agent_loop_summary.get('stop_reason') or 'n/a'}` "
            "before re-running or resuming the loop."
        )
    if agent_loop_summary.get("missing_artifact_count", 0) > 0:
        actions.append(
            f"Close the remaining {agent_loop_summary['missing_artifact_count']} required autonomous-loop artifact(s) "
            "before treating the run as fully packaged."
        )
    if java_bff_summary.get("available") and java_bff_summary.get("missing_merge_count", 0) > 0:
        actions.append(
            f"Complete the remaining {java_bff_summary['missing_merge_count']} Java BFF merged plan(s) before code skeleton generation."
        )
    if java_bff_summary.get("available") and java_bff_summary.get("missing_skeleton_count", 0) > 0:
        actions.append(
            f"Generate the remaining {java_bff_summary['missing_skeleton_count']} Java BFF skeleton bundle(s) so implementation handoff is complete."
        )
    if profile_path is None:
        actions.append("Run the learn -> infer-rules -> freeze-profile workflow to reduce repetitive resolution failures.")
    else:
        actions.append("Use validate-profile on each updated frozen profile before adopting it in regular analysis runs.")
    return actions[:5]


def write_evolution_report(
    output_dir: Path,
    evolution_summary: dict[str, Any] | None = None,
) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    analysis_root.mkdir(parents=True, exist_ok=True)
    summary = evolution_summary or build_evolution_summary(output_dir)
    json_path = analysis_root / "evolution_summary.json"
    md_path = analysis_root / "evolution_summary.md"
    scoreboard_json_path = analysis_root / "prompt_scoreboard.json"
    scoreboard_csv_path = analysis_root / "prompt_scoreboard.csv"
    html_path = analysis_root / "evolution_console.html"
    prompt_lab_path = analysis_root / "prompt_lab.html"
    failure_console_path = analysis_root / "failure_console.html"
    operator_console_path = analysis_root / "operator_console.html"
    bundle_explorer_path = analysis_root / "bundle_explorer.html"
    handoff_explorer_path = analysis_root / "handoff_explorer.html"
    json_path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_evolution_summary_markdown(summary), encoding="utf-8")
    scoreboard_json_path.write_text(
        json.dumps(summary["provider_scoreboard"], indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    write_csv(
        scoreboard_csv_path,
        summary["provider_scoreboard"],
        [
            "provider_name",
            "provider_model",
            "stage",
            "run_count",
            "reviewed_runs",
            "accepted_reviews",
            "needs_revision_reviews",
            "rejected_reviews",
            "accepted_review_rate",
            "avg_prompt_tokens",
            "avg_requested_tokens",
            "avg_completion_tokens",
            "avg_total_tokens",
        ],
    )
    html_path.write_text(render_evolution_console_html(summary), encoding="utf-8")
    prompt_lab_summary = build_prompt_lab_summary(output_dir)
    prompt_lab_path.write_text(render_prompt_lab_html(prompt_lab_summary), encoding="utf-8")
    failure_payload = explain_failure_from_output_dir(output_dir)
    failure_console_path.write_text(render_failure_console_html(failure_payload["index"]), encoding="utf-8")
    doctor_payload = doctor_run(output_dir)
    operator_console_path.write_text(
        render_operator_console_html(
            build_operator_console_summary(
                output_dir=output_dir,
                prompt_lab_summary=prompt_lab_summary,
                failure_payload=failure_payload["index"],
                doctor_payload=doctor_payload,
            )
        ),
        encoding="utf-8",
    )
    bundle_explorer_path.write_text(
        render_bundle_explorer_html(build_bundle_explorer_summary(output_dir)),
        encoding="utf-8",
    )
    handoff_explorer_path.write_text(
        render_handoff_explorer_html(build_handoff_explorer_summary(output_dir)),
        encoding="utf-8",
    )
    return [
        artifact_descriptor_for_path(json_path, "json", "Evolution summary", "prompting"),
        artifact_descriptor_for_path(md_path, "markdown", "Evolution summary (Markdown)", "prompting"),
        artifact_descriptor_for_path(scoreboard_json_path, "json", "Prompt scoreboard", "prompting"),
        artifact_descriptor_for_path(scoreboard_csv_path, "csv", "Prompt scoreboard export", "prompting"),
        artifact_descriptor_for_path(html_path, "html", "Evolution console", "prompting"),
        artifact_descriptor_for_path(prompt_lab_path, "html", "Prompt lab", "prompting"),
        artifact_descriptor_for_path(failure_console_path, "html", "Failure console", "prompting"),
        artifact_descriptor_for_path(operator_console_path, "html", "Operator console", "prompting"),
        artifact_descriptor_for_path(bundle_explorer_path, "html", "Java bundle explorer", "prompting"),
        artifact_descriptor_for_path(handoff_explorer_path, "html", "Handoff explorer", "prompting"),
    ]


def build_evolution_summary(output_dir: Path) -> dict[str, Any]:
    analysis_root = output_dir / "analysis"
    failure_clusters = load_json_list(analysis_root / "failure_clusters.json", "clusters")
    llm_run_summaries = load_run_summaries(analysis_root / "llm_runs")
    review_payloads = load_review_payloads(analysis_root / "llm_reviews")
    proposal_payload = load_json_payload(analysis_root / "proposals" / "rule_proposals.json")
    candidate_profile_payload = load_json_payload(analysis_root / "proposals" / "candidate_profile.json")

    headline = {
        "failure_cluster_count": len(failure_clusters),
        "llm_run_count": len(llm_run_summaries),
        "reviewed_count": len(review_payloads),
        "accepted_reviews": sum(1 for item in review_payloads if item.get("status") == "accepted"),
        "needs_revision_reviews": sum(1 for item in review_payloads if item.get("status") == "needs_revision"),
        "insufficient_evidence_reviews": sum(1 for item in review_payloads if item.get("status") == "insufficient_evidence"),
        "safe_patch_candidates": sum(1 for item in review_payloads if item.get("safe_to_apply_candidate")),
        "accepted_patch_count": int(proposal_payload.get("summary", {}).get("accepted_patch_count", 0)),
        "candidate_rule_count": int(proposal_payload.get("summary", {}).get("candidate_rule_count", 0)),
    }
    provider_rows = build_provider_scoreboard(llm_run_summaries)
    cluster_rows = build_cluster_scoreboard(failure_clusters, llm_run_summaries, review_payloads)
    top_repairs = sorted(
        [item for item in cluster_rows if item["needs_revision_reviews"] > 0],
        key=lambda item: (-int(item["needs_revision_reviews"]), item["cluster_id"]),
    )[:8]
    management_summary = [
        f"{headline['llm_run_count']} weak-LLM run(s) and {headline['reviewed_count']} review(s) have been captured.",
        f"{headline['accepted_reviews']} review(s) were accepted, and {headline['needs_revision_reviews']} still need repair prompts.",
        f"{headline['accepted_patch_count']} accepted patch candidate(s) are currently available for simulation.",
    ]
    if provider_rows:
        best_provider = provider_rows[0]
        management_summary.append(
            f"Most productive provider/stage so far: {best_provider['provider_name']} / {best_provider['provider_model']} / "
            f"{best_provider['stage']} with accepted rate {best_provider['accepted_review_rate']}%."
        )

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "headline": headline,
        "management_summary": management_summary,
        "provider_scoreboard": provider_rows,
        "cluster_scoreboard": cluster_rows[:10],
        "repair_hotspots": top_repairs,
        "proposal_summary": {
            "accepted_patch_count": headline["accepted_patch_count"],
            "candidate_rule_count": headline["candidate_rule_count"],
            "skipped_review_count": int(proposal_payload.get("summary", {}).get("skipped_review_count", 0)),
            "base_rule_count": int(proposal_payload.get("summary", {}).get("base_rule_count", 0)),
            "candidate_profile_status": candidate_profile_payload.get("profile_status"),
            "candidate_profile_version": candidate_profile_payload.get("profile_version"),
        },
    }


def build_prompt_lab_summary(output_dir: Path) -> dict[str, Any]:
    analysis_root = output_dir / "analysis"
    generic_packs = []
    for path in sorted((analysis_root / "context_packs").glob("*.json")):
        payload = load_json_payload(path)
        if not payload:
            continue
        generic_packs.append(
            {
                "label": f"{payload.get('cluster_id')} / {payload.get('phase')}",
                "estimated_tokens": int(payload.get("estimated_tokens", 0) or 0),
                "profile": payload.get("prompt_profile"),
                "path": str(path.resolve()),
            }
        )
    java_packs = []
    for path in sorted((analysis_root / "java_bff" / "context_packs").glob("*/*.json")):
        payload = load_json_payload(path)
        if not payload:
            continue
        java_packs.append(
            {
                "label": f"{payload.get('bundle_id')} / {payload.get('phase')}",
                "estimated_tokens": int(payload.get("estimated_prompt_tokens", 0) or 0),
                "profile": payload.get("prompt_profile"),
                "path": str(path.resolve()),
            }
        )
    handoff_packs = []
    for path in sorted((analysis_root / "handoff").glob("*/pack.json")):
        payload = load_json_payload(path)
        if not payload:
            continue
        handoff_packs.append(
            {
                "label": payload.get("title"),
                "estimated_tokens": estimate_text_tokens(str(payload.get("prompt_text") or "")),
                "profile": payload.get("profile_name"),
                "path": str(path.resolve()),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "generic_pack_count": len(generic_packs),
        "java_pack_count": len(java_packs),
        "handoff_pack_count": len(handoff_packs),
        "generic_packs": generic_packs[:20],
        "java_packs": java_packs[:20],
        "handoff_packs": handoff_packs[:20],
    }


def render_prompt_lab_html(summary: dict[str, Any]) -> str:
    def render_rows(rows: list[dict[str, Any]]) -> str:
        return "".join(
            f"<tr><td>{escape_html(str(item['label']))}</td><td>{item['estimated_tokens']}</td>"
            f"<td>{escape_html(str(item.get('profile') or 'n/a'))}</td><td>{escape_html(item['path'])}</td></tr>"
            for item in rows
        )

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Prompt Lab</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #12212e; background: #f6f7f8; }}
    h1, h2 {{ margin-bottom: 8px; }}
    .grid {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 16px; margin: 16px 0 24px; }}
    .card {{ background: #fff; border: 1px solid #d9dee3; border-radius: 12px; padding: 16px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border-bottom: 1px solid #e6eaee; text-align: left; padding: 10px; vertical-align: top; }}
    th {{ background: #eef3f6; }}
    code {{ font-family: ui-monospace, SFMono-Regular, monospace; }}
  </style>
</head>
<body>
  <h1>Prompt Lab</h1>
  <p>Operator view for copy-ready prompt/context artifacts, handoff packs, and weak-model token budgets.</p>
  <div class="grid">
    <div class="card"><strong>{summary['generic_pack_count']}</strong><div>Generic Context Packs</div></div>
    <div class="card"><strong>{summary['java_pack_count']}</strong><div>Java BFF Context Packs</div></div>
    <div class="card"><strong>{summary['handoff_pack_count']}</strong><div>Cline Handoff Packs</div></div>
  </div>
  <h2>Handoff Packs</h2>
  <table><thead><tr><th>Label</th><th>Est. Tokens</th><th>Profile</th><th>Path</th></tr></thead><tbody>{render_rows(summary['handoff_packs']) or '<tr><td colspan="4">No handoff packs yet</td></tr>'}</tbody></table>
  <h2>Java BFF Context Packs</h2>
  <table><thead><tr><th>Label</th><th>Est. Tokens</th><th>Profile</th><th>Path</th></tr></thead><tbody>{render_rows(summary['java_packs']) or '<tr><td colspan="4">No Java BFF context packs yet</td></tr>'}</tbody></table>
  <h2>Generic Context Packs</h2>
  <table><thead><tr><th>Label</th><th>Est. Tokens</th><th>Profile</th><th>Path</th></tr></thead><tbody>{render_rows(summary['generic_packs']) or '<tr><td colspan="4">No generic context packs yet</td></tr>'}</tbody></table>
</body>
</html>
"""


def render_failure_console_html(payload: dict[str, Any]) -> str:
    rows = "".join(
        f"<tr><td>{escape_html(item['failure_code'])}</td><td>{escape_html(item['summary'])}</td>"
        f"<td>{escape_html(item['recommended_next_step'])}</td>"
        f"<td>{escape_html(str(item.get('recommended_command') or 'n/a'))}</td></tr>"
        for item in payload.get("explanations", [])
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Failure Console</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #12212e; background: #f6f7f8; }}
    .card {{ background: #fff; border: 1px solid #d9dee3; border-radius: 12px; padding: 16px; margin-bottom: 20px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border-bottom: 1px solid #e6eaee; text-align: left; padding: 10px; vertical-align: top; }}
    th {{ background: #eef3f6; }}
    code {{ font-family: ui-monospace, SFMono-Regular, monospace; }}
  </style>
</head>
<body>
  <h1>Failure Console</h1>
  <div class="card">
    <strong>{payload.get('count', 0)}</strong> explanation(s) available.
  </div>
  <table>
    <thead><tr><th>Code</th><th>Summary</th><th>Next Step</th><th>Recommended Command</th></tr></thead>
    <tbody>{rows or '<tr><td colspan="4">No failure explanations yet</td></tr>'}</tbody>
  </table>
</body>
</html>
"""


def build_operator_console_summary(
    *,
    output_dir: Path,
    prompt_lab_summary: dict[str, Any],
    failure_payload: dict[str, Any],
    doctor_payload: dict[str, Any],
) -> dict[str, Any]:
    analysis_root = output_dir / "analysis"
    generic_loop = load_json_payload(analysis_root / "agent_loop" / "completion_report.json")
    java_loop = load_json_payload(analysis_root / "java_bff" / "loop" / "completion_report.json")
    latest_handoff = sorted((analysis_root / "handoff").glob("*/pack.json"))
    handoff_rows = []
    for path in latest_handoff[-10:]:
        payload = load_json_payload(path)
        if payload:
            handoff_rows.append(
                {
                    "title": payload.get("title"),
                    "profile": payload.get("profile_name"),
                    "path": str(path.resolve()),
                }
            )
    recent_sessions = build_handoff_explorer_summary(output_dir).get("rows", [])[:10]
    session_watch = load_json_payload(analysis_root / "watch_review" / "session_watch.json") or {}
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "doctor_status": doctor_payload.get("status"),
        "generic_loop": generic_loop or {},
        "java_loop": java_loop or {},
        "recommended_actions": doctor_payload.get("recommended_actions", []),
        "failure_count": failure_payload.get("count", 0),
        "generic_pack_count": prompt_lab_summary.get("generic_pack_count", 0),
        "java_pack_count": prompt_lab_summary.get("java_pack_count", 0),
        "handoff_pack_count": prompt_lab_summary.get("handoff_pack_count", 0),
        "handoff_rows": handoff_rows,
        "handoff_state_counts": doctor_payload.get("handoff_lifecycle", {}).get("state_counts", {}),
        "retry_scoreboard": doctor_payload.get("retry_scoreboard", {}),
        "phase_queue": doctor_payload.get("phase_queue", {}),
        "response_scoreboard": doctor_payload.get("response_scoreboard", {}).get("rows", []),
        "history_trend": doctor_payload.get("history_trend", {}),
        "latest_review_candidate": doctor_payload.get("latest_review_candidate"),
        "recent_sessions": recent_sessions,
        "session_watch": session_watch,
    }


def render_operator_console_html(summary: dict[str, Any]) -> str:
    action_rows = "".join(
        f"<tr><td>{escape_html(str(item['category']))}</td><td>{escape_html(str(item['summary']))}</td>"
        f"<td><code>{escape_html(str(item['command']))}</code></td></tr>"
        for item in summary.get("recommended_actions", [])
    )
    handoff_rows = "".join(
        f"<tr><td>{escape_html(str(item['title']))}</td><td>{escape_html(str(item['profile']))}</td>"
        f"<td>{escape_html(str(item['path']))}</td></tr>"
        for item in summary.get("handoff_rows", [])
    )
    queue_rows = "".join(
        f"<tr><td>{escape_html(str(item['bundle_id']))}</td><td>{item['completed_phases']}</td>"
        f"<td>{item['pending_phases']}</td><td>{escape_html(str(item['latest_status']))}</td>"
        f"<td>{escape_html(str(item.get('next_prompt') or 'n/a'))}</td></tr>"
        for item in summary.get("phase_queue", {}).get("rows", [])[:12]
    )
    response_rows = "".join(
        f"<tr><td>{escape_html(str(item['kind']))}</td><td>{escape_html(str(item['stage']))}</td>"
        f"<td>{item['review_count']}</td><td>{item['accepted']}</td>"
        f"<td>{item['needs_revision']}</td><td>{item['acceptance_rate']}%</td></tr>"
        for item in summary.get("response_scoreboard", [])[:12]
    )
    generic_loop = summary.get("generic_loop", {})
    java_loop = summary.get("java_loop", {})
    latest_review = summary.get("latest_review_candidate") or {}
    handoff_states = summary.get("handoff_state_counts", {})
    history_trend = summary.get("history_trend", {})
    retry_scoreboard = summary.get("retry_scoreboard", {})
    session_watch = summary.get("session_watch", {})
    session_rows = "".join(
        f"<tr><td>{escape_html(str(item.get('title') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('status') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('attempt_count') or 0))} / {escape_html(str(item.get('max_attempts') or 0))}</td>"
        f"<td>{escape_html(str(item.get('response_exists')))}</td>"
        f"<td>{escape_html(str(item.get('session_path') or 'n/a'))}</td></tr>"
        for item in summary.get("recent_sessions", [])
    )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Operator Console</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #12212e; background: #f6f7f8; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 16px 0 24px; }}
    .card {{ background: #fff; border: 1px solid #d9dee3; border-radius: 12px; padding: 16px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; margin-bottom: 20px; }}
    th, td {{ border-bottom: 1px solid #e6eaee; text-align: left; padding: 10px; vertical-align: top; }}
    th {{ background: #eef3f6; }}
    code {{ font-family: ui-monospace, SFMono-Regular, monospace; }}
  </style>
</head>
<body>
  <h1>Operator Console</h1>
  <p>Single page status view for loops, doctor-run guidance, handoff packs, and next commands.</p>
  <div class="grid">
    <div class="card"><strong>{escape_html(str(summary['doctor_status']))}</strong><div>Doctor Status</div></div>
    <div class="card"><strong>{escape_html(str(generic_loop.get('status') or 'missing'))}</strong><div>Generic Loop</div></div>
    <div class="card"><strong>{escape_html(str(java_loop.get('status') or 'missing'))}</strong><div>Java BFF Loop</div></div>
    <div class="card"><strong>{summary['failure_count']}</strong><div>Failure Explanations</div></div>
  </div>
  <div class="grid">
    <div class="card"><strong>{summary['generic_pack_count']}</strong><div>Generic Context Packs</div></div>
    <div class="card"><strong>{summary['java_pack_count']}</strong><div>Java Context Packs</div></div>
    <div class="card"><strong>{summary['handoff_pack_count']}</strong><div>Handoff Packs</div></div>
    <div class="card"><strong>{len(summary.get('recommended_actions', []))}</strong><div>Recommended Actions</div></div>
  </div>
  <div class="grid">
    <div class="card"><strong>{handoff_states.get('new', 0)}</strong><div>New Handoffs</div></div>
    <div class="card"><strong>{handoff_states.get('used', 0)}</strong><div>Used Handoffs</div></div>
    <div class="card"><strong>{handoff_states.get('repaired', 0)}</strong><div>Repair Handoffs</div></div>
    <div class="card"><strong>{handoff_states.get('resolved', 0)}</strong><div>Resolved Handoffs</div></div>
  </div>
  <div class="grid">
    <div class="card"><strong>{history_trend.get('generic_event_count', 0)}</strong><div>Generic History Events</div></div>
    <div class="card"><strong>{history_trend.get('java_event_count', 0)}</strong><div>Java History Events</div></div>
    <div class="card"><strong>{summary.get('phase_queue', {}).get('bundle_count', 0)}</strong><div>Java Bundles</div></div>
    <div class="card"><strong>{summary.get('phase_queue', {}).get('pending_bundle_count', 0)}</strong><div>Pending Bundles</div></div>
  </div>
  <div class="grid">
    <div class="card"><strong>{retry_scoreboard.get('session_count', 0)}</strong><div>Session Packs</div></div>
    <div class="card"><strong>{retry_scoreboard.get('retry_ready_count', 0)}</strong><div>Retry Ready</div></div>
    <div class="card"><strong>{retry_scoreboard.get('human_review_required_count', 0)}</strong><div>Human Review Needed</div></div>
    <div class="card"><strong>{session_watch.get('processed_count', 0)}</strong><div>Recent Watched Sessions</div></div>
  </div>
  <h2>Recommended Actions</h2>
  <table><thead><tr><th>Category</th><th>Summary</th><th>Command</th></tr></thead><tbody>{action_rows or '<tr><td colspan="3">No actions</td></tr>'}</tbody></table>
  <h2>Latest Retry Candidate</h2>
  <div class="card">
    <div><strong>Kind:</strong> {escape_html(str(latest_review.get('kind') or 'n/a'))}</div>
    <div><strong>Review:</strong> {escape_html(str(latest_review.get('review_path') or 'n/a'))}</div>
    <div><strong>Stage/Phase:</strong> {escape_html(str(latest_review.get('stage') or latest_review.get('phase') or 'n/a'))}</div>
  </div>
  <h2>Phase Queue</h2>
  <table><thead><tr><th>Bundle</th><th>Completed</th><th>Pending</th><th>Status</th><th>Next Prompt</th></tr></thead><tbody>{queue_rows or '<tr><td colspan="5">No Java bundle queue yet</td></tr>'}</tbody></table>
  <h2>Response Scoreboard</h2>
  <table><thead><tr><th>Kind</th><th>Stage/Phase</th><th>Reviews</th><th>Accepted</th><th>Needs Revision</th><th>Acceptance</th></tr></thead><tbody>{response_rows or '<tr><td colspan="6">No review data yet</td></tr>'}</tbody></table>
  <h2>Recent Session Packs</h2>
  <table><thead><tr><th>Title</th><th>Status</th><th>Attempts</th><th>Response Exists</th><th>Session Path</th></tr></thead><tbody>{session_rows or '<tr><td colspan="5">No session packs</td></tr>'}</tbody></table>
  <h2>Recent Handoff Packs</h2>
  <table><thead><tr><th>Title</th><th>Profile</th><th>Path</th></tr></thead><tbody>{handoff_rows or '<tr><td colspan="3">No handoff packs</td></tr>'}</tbody></table>
  <div><a href="dashboard.html">Dashboard</a> · <a href="prompt_lab.html">Prompt Lab</a> · <a href="failure_console.html">Failure Console</a> · <a href="bundle_explorer.html">Bundle Explorer</a> · <a href="handoff_explorer.html">Handoff Explorer</a> · <a href="evolution_console.html">Evolution Console</a></div>
</body>
</html>
"""


def build_bundle_explorer_summary(output_dir: Path) -> dict[str, Any]:
    analysis_root = output_dir / "analysis"
    java_root = analysis_root / "java_bff"
    rows: list[dict[str, Any]] = []
    for bundle_path in sorted((java_root / "bundles").glob("*/bundle.json")):
        payload = load_json_payload(bundle_path)
        if not payload:
            continue
        bundle_id = str(payload.get("bundle_id") or bundle_path.parent.name)
        slug = bundle_path.parent.name
        merged_path = java_root / "merged" / slug / "implementation_plan.json"
        starter_root = java_root / "skeletons" / slug / "starter_project"
        quality_gate = load_json_payload(starter_root / "quality_gate.json")
        delivery_summary = load_json_payload(starter_root / "delivery_summary.json")
        phase_rows = []
        for prompt in payload.get("recommended_sequence", []) if isinstance(payload.get("recommended_sequence"), list) else []:
            prompt_json = Path(str(prompt)).with_suffix(".json")
            review_json = java_root / "reviews" / slug / f"{safe_name(prompt_json.stem)}-review.json"
            review = load_json_payload(review_json)
            phase_rows.append(
                {
                    "phase": prompt_json.stem,
                    "status": review.get("status") if isinstance(review, dict) else "pending",
                    "review_path": str(review_json.resolve()) if review_json.exists() else "",
                }
            )
        rows.append(
            {
                "bundle_id": bundle_id,
                "merged_ready": bool((load_json_payload(merged_path) or {}).get("completion", {}).get("ready_for_skeletons")),
                "delivery_ready": bool((quality_gate or {}).get("ready_for_delivery")),
                "quality_blockers": len((quality_gate or {}).get("blocking_issues", [])) if isinstance(quality_gate, dict) else 0,
                "next_steps": (delivery_summary or {}).get("next_steps", [])[:3] if isinstance(delivery_summary, dict) else [],
                "phase_rows": phase_rows,
                "starter_root": str(starter_root.resolve()),
                "quality_gate_path": str((starter_root / "quality_gate.json").resolve()) if (starter_root / "quality_gate.json").exists() else "",
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_count": len(rows),
        "rows": rows,
    }


def render_bundle_explorer_html(summary: dict[str, Any]) -> str:
    rows = []
    for item in summary.get("rows", []):
        phase_summary = "<br>".join(
            f"{escape_html(str(phase['phase']))}: {escape_html(str(phase['status']))}" for phase in item.get("phase_rows", [])
        )
        next_steps = "<br>".join(escape_html(str(step)) for step in item.get("next_steps", []))
        rows.append(
            f"<tr><td>{escape_html(str(item['bundle_id']))}</td>"
            f"<td>{escape_html(str(item['merged_ready']))}</td>"
            f"<td>{escape_html(str(item['delivery_ready']))}</td>"
            f"<td>{item['quality_blockers']}</td>"
            f"<td>{phase_summary or 'n/a'}</td>"
            f"<td>{next_steps or 'n/a'}</td>"
            f"<td>{escape_html(str(item.get('quality_gate_path') or 'n/a'))}</td></tr>"
        )
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Bundle Explorer</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #12212e; background: #f6f7f8; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border-bottom: 1px solid #e6eaee; text-align: left; padding: 10px; vertical-align: top; }}
    th {{ background: #eef3f6; }}
  </style>
</head>
<body>
  <h1>Bundle Explorer</h1>
  <p>Java BFF bundle progress, quality gate status, and next delivery steps.</p>
  <table>
    <thead><tr><th>Bundle</th><th>Merged Ready</th><th>Delivery Ready</th><th>Blockers</th><th>Phase Status</th><th>Next Steps</th><th>Quality Gate</th></tr></thead>
    <tbody>{''.join(rows) or '<tr><td colspan=\"7\">No Java bundles available</td></tr>'}</tbody>
  </table>
  <div style="margin-top:20px;"><a href="operator_console.html">Operator Console</a> · <a href="dashboard.html">Dashboard</a> · <a href="failure_console.html">Failure Console</a> · <a href="handoff_explorer.html">Handoff Explorer</a></div>
</body>
</html>
"""


def build_handoff_explorer_summary(output_dir: Path) -> dict[str, Any]:
    analysis_root = output_dir / "analysis"
    handoff_root = analysis_root / "handoff"
    session_watch = load_json_payload(analysis_root / "watch_review" / "session_watch.json") or {}
    rows: list[dict[str, Any]] = []
    for session_path in sorted(handoff_root.glob("*/session.json")):
        payload = load_json_payload(session_path)
        if not payload:
            continue
        response_path = Path(str(payload.get("response_path") or ""))
        rows.append(
            {
                "title": payload.get("title") or session_path.parent.name,
                "kind": payload.get("kind"),
                "profile_name": payload.get("profile_name"),
                "status": payload.get("status"),
                "state": payload.get("state"),
                "attempt_count": int(payload.get("attempt_count", 0) or 0),
                "max_attempts": int(payload.get("max_attempts", 0) or 0),
                "response_exists": response_path.exists(),
                "response_path": str(response_path.resolve()) if response_path.exists() else str(response_path),
                "session_path": str(session_path.resolve()),
                "last_review_path": payload.get("last_review_path"),
                "last_watch_report_path": payload.get("last_watch_report_path"),
                "retry_count": len(payload.get("last_adaptive_retry", []) or []),
                "repair_count": len(payload.get("last_repair_pack", []) or []),
                "next_command": (payload.get("suggested_commands") or {}).get("watch_and_review"),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "session_count": len(rows),
        "rows": rows[-50:],
        "session_watch": session_watch,
    }


def render_handoff_explorer_html(summary: dict[str, Any]) -> str:
    rows = "".join(
        f"<tr><td>{escape_html(str(item['title']))}</td>"
        f"<td>{escape_html(str(item.get('kind') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('status') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('state') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('attempt_count') or 0))} / {escape_html(str(item.get('max_attempts') or 0))}</td>"
        f"<td>{escape_html(str(item.get('response_exists')))}</td>"
        f"<td>{escape_html(str(item.get('retry_count') or 0))}</td>"
        f"<td>{escape_html(str(item.get('repair_count') or 0))}</td>"
        f"<td>{escape_html(str(item.get('next_command') or 'n/a'))}</td>"
        f"<td>{escape_html(str(item.get('session_path') or 'n/a'))}</td></tr>"
        for item in summary.get("rows", [])
    )
    session_watch = summary.get("session_watch", {})
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Handoff Explorer</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 24px; color: #12212e; background: #f6f7f8; }}
    .grid {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 16px; margin: 16px 0 24px; }}
    .card {{ background: #fff; border: 1px solid #d9dee3; border-radius: 12px; padding: 16px; }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; }}
    th, td {{ border-bottom: 1px solid #e6eaee; text-align: left; padding: 10px; vertical-align: top; }}
    th {{ background: #eef3f6; }}
    code {{ font-family: ui-monospace, SFMono-Regular, monospace; }}
  </style>
</head>
<body>
  <h1>Handoff Explorer</h1>
  <p>Session-oriented view for Cline/VS Code handoff packs, retry readiness, and watch-and-review follow-up.</p>
  <div class="grid">
    <div class="card"><strong>{summary.get('session_count', 0)}</strong><div>Session Packs</div></div>
    <div class="card"><strong>{session_watch.get('processed_count', 0)}</strong><div>Recently Processed</div></div>
    <div class="card"><strong>{session_watch.get('pending_session_count', 0)}</strong><div>Pending Sessions</div></div>
    <div class="card"><strong>{len([item for item in summary.get('rows', []) if item.get('status') == 'human_review_required'])}</strong><div>Human Review Needed</div></div>
  </div>
  <table>
    <thead><tr><th>Title</th><th>Kind</th><th>Status</th><th>State</th><th>Attempts</th><th>Response Exists</th><th>Adaptive Retry</th><th>Repair Packs</th><th>Next Command</th><th>Session Path</th></tr></thead>
    <tbody>{rows or '<tr><td colspan="10">No handoff sessions available</td></tr>'}</tbody>
  </table>
  <div style="margin-top:20px;"><a href="operator_console.html">Operator Console</a> · <a href="prompt_lab.html">Prompt Lab</a> · <a href="failure_console.html">Failure Console</a> · <a href="dashboard.html">Dashboard</a></div>
</body>
</html>
"""


def build_provider_scoreboard(llm_run_summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[tuple[str, str, str], dict[str, Any]] = {}
    for item in llm_run_summaries:
        key = (
            str(item.get("provider_name") or item.get("provider_model") or "unknown"),
            str(item.get("provider_model") or "unknown"),
            str(item.get("stage") or "unknown"),
        )
        bucket = buckets.setdefault(
            key,
            {
                "provider_name": key[0],
                "provider_model": key[1],
                "stage": key[2],
                "run_count": 0,
                "reviewed_runs": 0,
                "accepted_reviews": 0,
                "needs_revision_reviews": 0,
                "rejected_reviews": 0,
                "prompt_tokens_total": 0,
                "requested_tokens_total": 0,
                "completion_tokens_total": 0,
                "total_tokens_total": 0,
            },
        )
        bucket["run_count"] += 1
        bucket["prompt_tokens_total"] += int(item.get("prompt_estimated_tokens", 0))
        bucket["requested_tokens_total"] += int(item.get("token_limit", 0) or 0)
        usage = item.get("response_usage", {})
        if isinstance(usage, dict):
            bucket["completion_tokens_total"] += int(usage.get("completion_tokens", 0) or 0)
            bucket["total_tokens_total"] += int(usage.get("total_tokens", 0) or 0)
        review_status = str(item.get("review_status") or "").strip()
        if review_status:
            bucket["reviewed_runs"] += 1
            if review_status == "accepted":
                bucket["accepted_reviews"] += 1
            elif review_status == "needs_revision":
                bucket["needs_revision_reviews"] += 1
            else:
                bucket["rejected_reviews"] += 1

    rows = []
    for bucket in buckets.values():
        run_count = max(bucket["run_count"], 1)
        reviewed_runs = max(bucket["reviewed_runs"], 1) if bucket["reviewed_runs"] else 0
        accepted_rate = round((bucket["accepted_reviews"] / reviewed_runs) * 100, 1) if reviewed_runs else 0.0
        rows.append(
            {
                "provider_name": bucket["provider_name"],
                "provider_model": bucket["provider_model"],
                "stage": bucket["stage"],
                "run_count": bucket["run_count"],
                "reviewed_runs": bucket["reviewed_runs"],
                "reviewed_run_count": bucket["reviewed_runs"],
                "accepted_reviews": bucket["accepted_reviews"],
                "needs_revision_reviews": bucket["needs_revision_reviews"],
                "rejected_reviews": bucket["rejected_reviews"],
                "accepted_review_rate": accepted_rate,
                "acceptance_rate": accepted_rate,
                "avg_prompt_tokens": round(bucket["prompt_tokens_total"] / run_count, 1),
                "avg_requested_tokens": round(bucket["requested_tokens_total"] / run_count, 1),
                "avg_completion_tokens": round(bucket["completion_tokens_total"] / run_count, 1),
                "avg_total_tokens": round(bucket["total_tokens_total"] / run_count, 1),
            }
        )
    rows.sort(
        key=lambda item: (
            -float(item["accepted_review_rate"]),
            -int(item["accepted_reviews"]),
            -int(item["run_count"]),
            item["provider_name"],
            item["stage"],
        )
    )
    return rows


def build_profile_lifecycle_summary(profile_path: Path | None) -> dict[str, Any]:
    if profile_path is None or not profile_path.exists():
        return {
            "profile_name": None,
            "profile_status": None,
            "profile_version": None,
            "validation_count": 0,
            "lifecycle_event_count": 0,
            "history_rows": [],
        }
    profile = load_profile(profile_path)
    if profile is None:
        return {
            "profile_name": None,
            "profile_status": None,
            "profile_version": None,
            "validation_count": 0,
            "lifecycle_event_count": 0,
            "history_rows": [],
        }
    return {
        "profile_name": profile.profile_name,
        "profile_status": profile.profile_status,
        "profile_version": profile.profile_version,
        "validation_count": len(profile.validation_history),
        "lifecycle_event_count": len(profile.lifecycle_history),
        "history_rows": [
            {
                "generated_at": item.get("generated_at"),
                "event_type": item.get("event_type"),
                "from_status": item.get("from_status"),
                "to_status": item.get("to_status"),
                "source_profile_path": item.get("source_profile_path"),
                "target_profile_path": item.get("target_profile_path"),
                "assessment_classification": item.get("assessment_classification"),
                "promotion_readiness": item.get("promotion_readiness"),
                "reason": item.get("reason"),
            }
            for item in profile.lifecycle_history
            if isinstance(item, dict)
        ],
    }


def build_cluster_scoreboard(
    failure_clusters: list[dict[str, Any]],
    llm_run_summaries: list[dict[str, Any]],
    review_payloads: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for cluster in failure_clusters:
        cluster_id = str(cluster.get("cluster_id") or "unknown")
        buckets[cluster_id] = {
            "cluster_id": cluster_id,
            "task_type": str(cluster.get("task_type") or "unknown"),
            "occurrence_count": int(cluster.get("occurrence_count", 0) or 0),
            "files_affected": int(cluster.get("files_affected", 0) or 0),
            "llm_runs": 0,
            "accepted_reviews": 0,
            "needs_revision_reviews": 0,
            "rejected_reviews": 0,
            "insufficient_evidence_reviews": 0,
            "safe_patch_candidates": 0,
        }
    for item in llm_run_summaries:
        cluster_id = str(item.get("cluster_id") or "unknown")
        bucket = buckets.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "task_type": "unknown",
                "occurrence_count": 0,
                "files_affected": 0,
                "llm_runs": 0,
                "accepted_reviews": 0,
                "needs_revision_reviews": 0,
                "rejected_reviews": 0,
                "insufficient_evidence_reviews": 0,
                "safe_patch_candidates": 0,
            },
        )
        bucket["llm_runs"] += 1
    for item in review_payloads:
        cluster_id = str(item.get("cluster_id") or "unknown")
        bucket = buckets.setdefault(
            cluster_id,
            {
                "cluster_id": cluster_id,
                "task_type": "unknown",
                "occurrence_count": 0,
                "files_affected": 0,
                "llm_runs": 0,
                "accepted_reviews": 0,
                "needs_revision_reviews": 0,
                "rejected_reviews": 0,
                "insufficient_evidence_reviews": 0,
                "safe_patch_candidates": 0,
            },
        )
        status = str(item.get("status") or "").strip()
        if status == "accepted":
            bucket["accepted_reviews"] += 1
        elif status == "needs_revision":
            bucket["needs_revision_reviews"] += 1
        elif status == "insufficient_evidence":
            bucket["insufficient_evidence_reviews"] += 1
        elif status:
            bucket["rejected_reviews"] += 1
        if item.get("safe_to_apply_candidate"):
            bucket["safe_patch_candidates"] += 1
    rows = list(buckets.values())
    for item in rows:
        reviewed_count = item["accepted_reviews"] + item["needs_revision_reviews"] + item["rejected_reviews"]
        item["acceptance_rate"] = round((item["accepted_reviews"] / reviewed_count) * 100, 1) if reviewed_count else 0.0
    rows.sort(
        key=lambda item: (
            -int(item["safe_patch_candidates"]),
            -int(item["accepted_reviews"]),
            -int(item["llm_runs"]),
            -int(item["occurrence_count"]),
            item["cluster_id"],
        )
    )
    return rows


def load_json_payload(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def estimate_text_tokens(text: str) -> int:
    return max(1, round(len(text) / 4)) if text else 0


def load_json_list(path: Path, key: str) -> list[dict[str, Any]]:
    payload = load_json_payload(path)
    items = payload.get(key, [])
    return [item for item in items if isinstance(item, dict)]


def load_run_summaries(run_root: Path) -> list[dict[str, Any]]:
    if not run_root.exists():
        return []
    index_payload = load_json_payload(run_root / "index.json")
    indexed_runs = index_payload.get("runs", [])
    if isinstance(indexed_runs, list) and indexed_runs:
        return [item for item in indexed_runs if isinstance(item, dict)]
    rows: list[dict[str, Any]] = []
    for summary_path in sorted(run_root.glob("*/run_summary.json")):
        payload = load_json_payload(summary_path)
        if payload:
            rows.append(payload)
    return rows


def load_review_payloads(review_root: Path) -> list[dict[str, Any]]:
    if not review_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for review_path in sorted(review_root.glob("*-review.json")):
        payload = load_json_payload(review_path)
        if payload:
            rows.append(payload)
    return rows


def build_agent_loop_summary(output_dir: Path) -> dict[str, Any]:
    loop_root = output_dir / "analysis" / "agent_loop"
    state_payload = load_json_payload(loop_root / "loop_state.json")
    completion_payload = load_json_payload(loop_root / "completion_report.json")
    history_payload = load_json_payload(loop_root / "inspection.json")

    if not state_payload:
        return {
            "available": False,
            "status": None,
            "current_phase": None,
            "iteration_count": 0,
            "stop_reason": None,
            "missing_artifact_count": 0,
            "missing_artifacts": [],
            "latest_events": [],
        }

    latest_events = history_payload.get("latest_history", [])
    if not isinstance(latest_events, list):
        latest_events = []

    missing_artifacts = completion_payload.get("missing_artifacts", [])
    if not isinstance(missing_artifacts, list):
        missing_artifacts = []

    return {
        "available": True,
        "status": state_payload.get("status"),
        "current_phase": state_payload.get("current_phase"),
        "iteration_count": int(state_payload.get("iteration_count", 0) or 0),
        "stop_reason": completion_payload.get("stop_reason") or state_payload.get("stop_reason"),
        "missing_artifact_count": len(missing_artifacts),
        "missing_artifacts": [str(item) for item in missing_artifacts],
        "latest_events": [item for item in latest_events[-5:] if isinstance(item, dict)],
    }


def build_java_bff_summary(output_dir: Path) -> dict[str, Any]:
    java_root = output_dir / "analysis" / "java_bff"
    overview = load_json_payload(java_root / "overview.json")
    if not overview:
        return {
            "available": False,
            "bundle_count": 0,
            "accepted_review_count": 0,
            "needs_revision_review_count": 0,
            "context_pack_count": 0,
            "task_count": 0,
            "task_result_count": 0,
            "merged_bundle_count": 0,
            "skeleton_bundle_count": 0,
            "missing_merge_count": 0,
            "missing_skeleton_count": 0,
            "loop_status": None,
            "bundles": [],
        }

    reviews_root = java_root / "reviews"
    review_rows: list[dict[str, Any]] = []
    if reviews_root.exists():
        for review_path in sorted(reviews_root.glob("*/*-review.json")):
            payload = load_json_payload(review_path)
            if payload:
                review_rows.append(payload)

    bundles = overview.get("bundles", [])
    if not isinstance(bundles, list):
        bundles = []
    merged_bundle_count = 0
    skeleton_bundle_count = 0
    bundle_rows: list[dict[str, Any]] = []
    for item in bundles:
        if not isinstance(item, dict):
            continue
        bundle_id = str(item.get("bundle_id") or item.get("entry_query_id") or "unknown")
        bundle_slug = re.sub(r"[^A-Za-z0-9._-]+", "_", bundle_id)
        merged_exists = (java_root / "merged" / bundle_slug / "implementation_plan.json").exists()
        skeleton_exists = (java_root / "skeletons" / bundle_slug / "README.md").exists()
        merged_bundle_count += 1 if merged_exists else 0
        skeleton_bundle_count += 1 if skeleton_exists else 0
        bundle_rows.append(
            {
                "bundle_id": bundle_id,
                "query_count": int(item.get("query_count", 0) or 0),
                "prompt_count": int(item.get("prompt_count", 0) or 0),
                "merged": merged_exists,
                "skeleton": skeleton_exists,
            }
        )

    loop_payload = load_json_payload(java_root / "loop" / "completion_report.json")
    context_pack_count = sum(1 for _ in (java_root / "context_packs").glob("*/*.json")) if (java_root / "context_packs").exists() else 0
    task_count = sum(1 for _ in (java_root / "tasks").glob("*/*.json")) if (java_root / "tasks").exists() else 0
    task_result_count = sum(1 for _ in (java_root / "agent_runs").glob("*/*.result.json")) if (java_root / "agent_runs").exists() else 0
    return {
        "available": True,
        "bundle_count": len(bundle_rows),
        "accepted_review_count": sum(1 for item in review_rows if item.get("status") == "accepted"),
        "needs_revision_review_count": sum(1 for item in review_rows if item.get("status") == "needs_revision"),
        "context_pack_count": context_pack_count,
        "task_count": task_count,
        "task_result_count": task_result_count,
        "merged_bundle_count": merged_bundle_count,
        "skeleton_bundle_count": skeleton_bundle_count,
        "missing_merge_count": max(len(bundle_rows) - merged_bundle_count, 0),
        "missing_skeleton_count": max(len(bundle_rows) - skeleton_bundle_count, 0),
        "loop_status": loop_payload.get("status"),
        "bundles": bundle_rows[:10],
    }


def build_profile_lifecycle_summary(profile_path: Path | None) -> dict[str, Any]:
    if profile_path is None or not profile_path.exists():
        return {
            "profile_name": None,
            "profile_status": None,
            "profile_version": None,
            "validation_count": 0,
            "lifecycle_event_count": 0,
            "assessment_counts": {},
            "latest_validation": None,
            "latest_event": None,
            "history_rows": [],
        }

    profile = load_profile(profile_path)
    if profile is None:
        return {
            "profile_name": profile_path.stem,
            "profile_status": None,
            "profile_version": None,
            "validation_count": 0,
            "lifecycle_event_count": 0,
            "assessment_counts": {},
            "latest_validation": None,
            "latest_event": None,
            "history_rows": [],
        }

    assessment_counts = Counter(
        str(item.get("assessment_classification", "")).strip().lower()
        for item in profile.validation_history
        if isinstance(item, dict)
    )
    history_rows = [
        {
            "generated_at": event.get("generated_at"),
            "event_type": event.get("event_type"),
            "from_status": event.get("from_status"),
            "to_status": event.get("to_status"),
            "source_profile_path": event.get("source_profile_path"),
            "target_profile_path": event.get("target_profile_path"),
            "assessment_classification": event.get("assessment_classification"),
            "promotion_readiness": event.get("promotion_readiness"),
            "reason": event.get("reason"),
        }
        for event in profile.lifecycle_history
        if isinstance(event, dict)
    ]
    if not history_rows:
        history_rows = [
            {
                "generated_at": profile.generated_at,
                "event_type": "current",
                "from_status": None,
                "to_status": profile.profile_status,
                "source_profile_path": profile.parent_profile,
                "target_profile_path": str(profile_path.resolve()),
                "assessment_classification": None,
                "promotion_readiness": None,
                "reason": None,
            }
        ]

    return {
        "profile_name": profile.profile_name or profile_path.stem,
        "profile_status": profile.profile_status,
        "profile_version": profile.profile_version,
        "validation_count": len(profile.validation_history),
        "lifecycle_event_count": len(profile.lifecycle_history),
        "assessment_counts": dict(sorted((key, value) for key, value in assessment_counts.items() if key)),
        "latest_validation": profile.validation_history[-1] if profile.validation_history else None,
        "latest_event": profile.lifecycle_history[-1] if profile.lifecycle_history else None,
        "history_rows": history_rows,
    }


def render_executive_summary_markdown(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
    evolution = summary["evolution_summary"]
    agent_loop = summary["agent_loop_summary"]
    java_bff = summary["java_bff_summary"]
    lifecycle = summary["profile_lifecycle"]
    lines = [
        "# Executive Summary",
        "",
        "## Headline",
        f"- Files scanned: {headline['files_scanned']}",
        f"- Queries discovered: {headline['queries_discovered']}",
        f"- Resolved queries: {headline['resolved_queries']}",
        f"- Partial queries: {headline['partial_queries']}",
        f"- Failed queries: {headline['failed_queries']}",
        f"- Errors: {headline['error_count']}",
        f"- Warnings: {headline['warning_count']}",
        "",
        "## Management Summary",
    ]
    for item in summary["management_summary"]:
        lines.append(f"- {item}")

    lines.extend(["", "## Top Complex Queries"])
    for item in summary["complexity_summary"]["top_complex_queries"][:5]:
        lines.append(
            f"- `{item['query_id']}`: score={item['complexity_score']}, risk={item['complexity_risk']}, "
            f"deps={item['dependencies']}, joins={item['joins']}, params={item['parameters']}"
        )

    lines.extend(["", "## Top Value Queries"])
    for item in summary["value_summary"]["top_value_queries"][:5]:
        lines.append(
            f"- `{item['query_id']}`: value={item['value_score']}, inbound_refs={item['inbound_references']}, "
            f"query_type={item['query_type']}"
        )

    lines.extend(["", "## Trend"])
    trend = summary["trend_summary"]
    lines.append(
        f"- Snapshot count: {trend['snapshot_count']}, resolved delta vs previous: {trend['resolved_queries_delta_vs_previous']:+d}, "
        f"error delta vs previous: {trend['error_count_delta_vs_previous']:+d}, warning delta vs previous: {trend['warning_count_delta_vs_previous']:+d}"
    )
    if trend["status_line"]:
        lines.append(f"- {trend['status_line']}")

    lines.extend(["", "## Evolution"])
    lines.append(
        f"- LLM runs: {evolution['headline']['llm_run_count']}, reviews: {evolution['headline']['reviewed_count']}, "
        f"accepted patches: {evolution['headline']['accepted_patch_count']}"
    )
    for item in evolution["management_summary"][:3]:
        lines.append(f"- {item}")

    lines.extend(["", "## Agent Loop"])
    if agent_loop["available"]:
        lines.append(
            f"- Status: `{agent_loop['status']}`, current phase: `{agent_loop['current_phase'] or 'n/a'}`, "
            f"iterations: {agent_loop['iteration_count']}, missing artifacts: {agent_loop['missing_artifact_count']}."
        )
        if agent_loop["stop_reason"]:
            lines.append(f"- Stop reason: `{agent_loop['stop_reason']}`")
    else:
        lines.append("- No autonomous agent loop state is available for this run.")

    lines.extend(["", "## Java BFF"])
    if java_bff["available"]:
        lines.append(
            f"- Bundles: {java_bff['bundle_count']}, accepted reviews: {java_bff['accepted_review_count']}, "
            f"context packs: {java_bff['context_pack_count']}, tasks: {java_bff['task_count']}, "
            f"merged bundles: {java_bff['merged_bundle_count']}, skeleton bundles: {java_bff['skeleton_bundle_count']}."
        )
        if java_bff.get("loop_status"):
            lines.append(f"- Java BFF loop status: `{java_bff['loop_status']}`")
    else:
        lines.append("- No Java Spring Boot BFF artifact pack is available for this run.")

    lines.extend(["", "## Profile Lifecycle"])
    if lifecycle["profile_status"]:
        lines.append(
            f"- Profile `{lifecycle['profile_name']}` is currently `{lifecycle['profile_status']}` at version {lifecycle['profile_version']}."
        )
        lines.append(
            f"- Validation records: {lifecycle['validation_count']}, lifecycle events: {lifecycle['lifecycle_event_count']}."
        )
    else:
        lines.append("- No active profile metadata was supplied for this run.")

    lines.extend(["", "## Next Actions"])
    for item in summary["next_actions"]:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def render_dashboard_html(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
    evolution = summary["evolution_summary"]
    agent_loop = summary["agent_loop_summary"]
    java_bff = summary["java_bff_summary"]
    lifecycle = summary["profile_lifecycle"]
    management_cards = "".join(f"<li>{escape_html(item)}</li>" for item in summary["management_summary"])
    next_actions = "".join(f"<li>{escape_html(item)}</li>" for item in summary["next_actions"])
    complex_rows = "".join(render_query_row(item) for item in summary["complexity_summary"]["top_complex_queries"][:10])
    value_rows = "".join(render_value_row(item) for item in summary["value_summary"]["top_value_queries"][:10])
    diagnostic_rows = "".join(
        f"<tr><td>{escape_html(item['code'])}</td><td>{item['count']}</td></tr>"
        for item in summary["diagnostics_summary"]["top_codes"]
    )
    file_rows = "".join(
        f"<tr><td>{escape_html(Path(item['file']).name)}</td><td>{item['diagnostic_count']}</td></tr>"
        for item in summary["value_summary"]["top_files_by_diagnostics"]
    )
    trend = summary["trend_summary"]
    trend_history = trend["history"]
    resolved_sparkline = render_sparkline_svg([item["resolved_queries"] for item in trend_history], "#174b63")
    error_sparkline = render_sparkline_svg([item["error_count"] for item in trend_history], "#8f1d22")
    warning_sparkline = render_sparkline_svg([item["warning_count"] for item in trend_history], "#b45f29")
    evolution_rows = "".join(
        f"<tr><td>{escape_html(item['provider_name'])}</td><td>{escape_html(item['provider_model'])}</td><td>{escape_html(item['stage'])}</td>"
        f"<td>{item['run_count']}</td><td>{item['accepted_review_rate']}%</td></tr>"
        for item in evolution["provider_scoreboard"][:8]
    )
    lifecycle_rows = "".join(
        f"<tr><td>{escape_html(item.get('event_type') or 'current')}</td><td>{escape_html(item.get('from_status') or 'n/a')}</td>"
        f"<td>{escape_html(item.get('to_status') or 'n/a')}</td><td>{escape_html(item.get('assessment_classification') or item.get('reason') or 'n/a')}</td></tr>"
        for item in lifecycle["history_rows"][-6:]
    )
    lifecycle_summary = (
        f"{lifecycle['profile_name']} is {lifecycle['profile_status']} at version {lifecycle['profile_version']}."
        if lifecycle["profile_status"]
        else "No active profile metadata for this run."
    )
    trend_rows = "".join(
        f"<tr><td>{escape_html(item['label'] or item['snapshot_id'])}</td><td>{item['resolved_queries']}</td>"
        f"<td>{item['error_count']}</td><td>{item['warning_count']}</td></tr>"
        for item in trend_history[-8:]
    )
    agent_loop_rows = "".join(
        f"<tr><td>{escape_html(item.get('phase') or 'n/a')}</td><td>{escape_html(item.get('status') or 'n/a')}</td>"
        f"<td>{escape_html(item.get('cluster_id') or 'n/a')}</td><td>{escape_html(str(item.get('attempt') or 'n/a'))}</td></tr>"
        for item in agent_loop["latest_events"]
    )
    agent_loop_summary = (
        f"Loop is {agent_loop['status']} at phase {agent_loop['current_phase'] or 'n/a'} after "
        f"{agent_loop['iteration_count']} iteration(s)."
        if agent_loop["available"]
        else "No autonomous agent loop state is available yet."
    )
    java_bff_rows = "".join(
        f"<tr><td>{escape_html(item['bundle_id'])}</td><td>{item['query_count']}</td><td>{item['prompt_count']}</td>"
        f"<td>{'yes' if item['merged'] else 'no'}</td><td>{'yes' if item['skeleton'] else 'no'}</td></tr>"
        for item in java_bff["bundles"]
    )
    java_bff_summary = (
        f"Java BFF packs: bundles={java_bff['bundle_count']}, accepted reviews={java_bff['accepted_review_count']}, "
        f"contexts={java_bff['context_pack_count']}, tasks={java_bff['task_count']}, "
        f"merged={java_bff['merged_bundle_count']}, skeletons={java_bff['skeleton_bundle_count']}."
        if java_bff["available"]
        else "No Java BFF artifact pack is available yet."
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Legacy SQL XML Analyzer Dashboard</title>
  <style>
    :root {{
      --bg: #f4efe8;
      --panel: #fffaf2;
      --ink: #18202a;
      --muted: #6b6f74;
      --accent: #174b63;
      --accent-2: #b45f29;
      --border: #d7c9b8;
      --critical: #8f1d22;
      --high: #b45f29;
      --medium: #876f2a;
      --low: #2f6f47;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(180,95,41,0.12), transparent 28%),
        linear-gradient(180deg, #f9f4ec 0%, var(--bg) 100%);
    }}
    .wrap {{
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }}
    h1, h2 {{ margin: 0 0 14px; }}
    p {{ color: var(--muted); }}
    .hero {{
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
      margin-bottom: 24px;
    }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 18px 40px rgba(24, 32, 42, 0.08);
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .metric {{
      background: rgba(23, 75, 99, 0.06);
      border-radius: 14px;
      padding: 12px;
    }}
    .metric strong {{
      display: block;
      font-size: 1.6rem;
      color: var(--accent);
    }}
    .two-col {{
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 16px;
      margin-bottom: 16px;
    }}
    .three-col {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
      margin-bottom: 16px;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.95rem;
    }}
    th, td {{
      padding: 10px 8px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.08);
      text-align: left;
      vertical-align: top;
    }}
    th {{
      color: var(--muted);
      font-weight: 600;
    }}
    .badge {{
      display: inline-block;
      border-radius: 999px;
      padding: 4px 10px;
      font-size: 0.78rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .risk-low {{ background: rgba(47,111,71,0.12); color: var(--low); }}
    .risk-medium {{ background: rgba(135,111,42,0.14); color: var(--medium); }}
    .risk-high {{ background: rgba(180,95,41,0.14); color: var(--high); }}
    .risk-critical {{ background: rgba(143,29,34,0.14); color: var(--critical); }}
    ul {{
      margin: 0;
      padding-left: 20px;
      line-height: 1.5;
    }}
    .footer {{
      margin-top: 18px;
      color: var(--muted);
      font-size: 0.9rem;
    }}
    @media (max-width: 960px) {{
      .hero, .two-col, .three-col {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="hero">
      <div class="panel">
        <h1>Executive SQL XML Dashboard</h1>
        <p>Management-focused view of analyzer coverage, complexity hotspots, diagnostic concentration, and suggested next actions.</p>
        <div class="metric-grid">
          <div class="metric"><strong>{headline['files_scanned']}</strong><span>Files</span></div>
          <div class="metric"><strong>{headline['queries_discovered']}</strong><span>Queries</span></div>
          <div class="metric"><strong>{headline['resolved_queries']}</strong><span>Resolved</span></div>
          <div class="metric"><strong>{headline['failed_queries']}</strong><span>Failed</span></div>
          <div class="metric"><strong>{headline['error_count']}</strong><span>Errors</span></div>
          <div class="metric"><strong>{headline['snapshot_count']}</strong><span>Snapshots</span></div>
        </div>
      </div>
      <div class="panel">
        <h2>Management Summary</h2>
        <ul>{management_cards}</ul>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>Complexity Hotspots</h2>
        <table>
          <thead>
            <tr><th>Query</th><th>Risk</th><th>Score</th><th>Deps</th><th>Joins</th><th>Params</th></tr>
          </thead>
          <tbody>{complex_rows}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Next Actions</h2>
        <ul>{next_actions}</ul>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>Progress Trend</h2>
        <p>{escape_html(trend['status_line'] or 'Trend data will appear after repeated runs in the same output directory.')}</p>
        <div style="display:grid; gap:10px;">
          <div><strong>Resolved Queries</strong>{resolved_sparkline}</div>
          <div><strong>Error Count</strong>{error_sparkline}</div>
          <div><strong>Warning Count</strong>{warning_sparkline}</div>
        </div>
      </div>
      <div class="panel">
        <h2>Recent Snapshots</h2>
        <table>
          <thead>
            <tr><th>Snapshot</th><th>Resolved</th><th>Errors</th><th>Warnings</th></tr>
          </thead>
          <tbody>{trend_rows or '<tr><td colspan=\"4\">No history yet</td></tr>'}</tbody>
        </table>
      </div>
    </section>

    <section class="three-col">
      <div class="panel">
        <h2>LLM Evolution</h2>
        <p>{escape_html(evolution['management_summary'][0] if evolution['management_summary'] else 'No LLM evolution data yet.')}</p>
        <div class="metric-grid">
          <div class="metric"><strong>{evolution['headline']['llm_run_count']}</strong><span>LLM Runs</span></div>
          <div class="metric"><strong>{evolution['headline']['reviewed_count']}</strong><span>Reviews</span></div>
          <div class="metric"><strong>{evolution['headline']['accepted_reviews']}</strong><span>Accepted</span></div>
          <div class="metric"><strong>{evolution['headline']['accepted_patch_count']}</strong><span>Patches</span></div>
        </div>
        <div class="footer"><a href="evolution_console.html">Open evolution console</a> · <a href="prompt_lab.html">Open prompt lab</a> · <a href="failure_console.html">Open failure console</a> · <a href="operator_console.html">Open operator console</a></div>
      </div>
      <div class="panel">
        <h2>Provider Scoreboard</h2>
        <table>
          <thead>
            <tr><th>Provider</th><th>Model</th><th>Stage</th><th>Runs</th><th>Accepted</th></tr>
          </thead>
          <tbody>{evolution_rows or '<tr><td colspan=\"5\">No LLM runs yet</td></tr>'}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Profile Lifecycle</h2>
        <p>{escape_html(lifecycle_summary)}</p>
        <table>
          <thead>
            <tr><th>Event</th><th>From</th><th>To</th><th>Detail</th></tr>
          </thead>
          <tbody>{lifecycle_rows or '<tr><td colspan=\"4\">No lifecycle history yet</td></tr>'}</tbody>
        </table>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>Agent Loop</h2>
        <p>{escape_html(agent_loop_summary)}</p>
        <div class="metric-grid">
          <div class="metric"><strong>{agent_loop['iteration_count']}</strong><span>Iterations</span></div>
          <div class="metric"><strong>{agent_loop['missing_artifact_count']}</strong><span>Missing Artifacts</span></div>
          <div class="metric"><strong>{escape_html(agent_loop['current_phase'] or 'n/a')}</strong><span>Current Phase</span></div>
        </div>
        <div style="height: 14px"></div>
        <table>
          <thead>
            <tr><th>Phase</th><th>Status</th><th>Cluster</th><th>Attempt</th></tr>
          </thead>
          <tbody>{agent_loop_rows or '<tr><td colspan=\"4\">No loop history yet</td></tr>'}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Remaining Loop Gaps</h2>
        <p>{escape_html(agent_loop['stop_reason'] or 'Loop is still active or has no recorded stop reason.')}</p>
        <ul>{"".join(f"<li>{escape_html(item)}</li>" for item in agent_loop['missing_artifacts'][:8]) or "<li>No missing required artifacts</li>"}</ul>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>Java BFF</h2>
        <p>{escape_html(java_bff_summary)}</p>
        <div class="metric-grid">
          <div class="metric"><strong>{java_bff['bundle_count']}</strong><span>Bundles</span></div>
          <div class="metric"><strong>{java_bff['accepted_review_count']}</strong><span>Accepted Reviews</span></div>
          <div class="metric"><strong>{java_bff['context_pack_count']}</strong><span>Context Packs</span></div>
          <div class="metric"><strong>{java_bff['task_count']}</strong><span>Tasks</span></div>
          <div class="metric"><strong>{java_bff['merged_bundle_count']}</strong><span>Merged</span></div>
          <div class="metric"><strong>{java_bff['skeleton_bundle_count']}</strong><span>Skeletons</span></div>
        </div>
      </div>
      <div class="panel">
        <h2>Java BFF Bundle Status</h2>
        <table>
          <thead>
            <tr><th>Bundle</th><th>Queries</th><th>Prompts</th><th>Merged</th><th>Skeleton</th></tr>
          </thead>
          <tbody>{java_bff_rows or '<tr><td colspan=\"5\">No Java BFF bundles yet</td></tr>'}</tbody>
        </table>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>High-Value Queries</h2>
        <table>
          <thead>
            <tr><th>Query</th><th>Value</th><th>Inbound</th><th>Type</th><th>Status</th></tr>
          </thead>
          <tbody>{value_rows}</tbody>
        </table>
      </div>
      <div class="panel">
        <h2>Diagnostics & File Hotspots</h2>
        <table>
          <thead>
            <tr><th>Diagnostic</th><th>Count</th></tr>
          </thead>
          <tbody>{diagnostic_rows or '<tr><td colspan=\"2\">No diagnostics</td></tr>'}</tbody>
        </table>
        <div style="height: 14px"></div>
        <table>
          <thead>
            <tr><th>File</th><th>Diagnostics</th></tr>
          </thead>
          <tbody>{file_rows or '<tr><td colspan=\"2\">No hotspots</td></tr>'}</tbody>
        </table>
      </div>
    </section>

    <div class="footer">Generated at {escape_html(summary['generated_at'])}</div>
  </div>
</body>
</html>
"""


def render_query_row(item: dict[str, Any]) -> str:
    return (
        "<tr>"
        f"<td>{escape_html(item['query_id'])}</td>"
        f"<td><span class=\"badge risk-{item['complexity_risk']}\">{escape_html(item['complexity_risk'])}</span></td>"
        f"<td>{item['complexity_score']}</td>"
        f"<td>{item['dependencies']}</td>"
        f"<td>{item['joins']}</td>"
        f"<td>{item['parameters']}</td>"
        "</tr>"
    )


def render_value_row(item: dict[str, Any]) -> str:
    return (
        "<tr>"
        f"<td>{escape_html(item['query_id'])}</td>"
        f"<td>{item['value_score']}</td>"
        f"<td>{item['inbound_references']}</td>"
        f"<td>{escape_html(item['query_type'])}</td>"
        f"<td>{escape_html(item['status'])}</td>"
        "</tr>"
    )


def render_evolution_summary_markdown(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
    lines = [
        "# Evolution Summary",
        "",
        "## Headline",
        f"- Failure clusters: {headline['failure_cluster_count']}",
        f"- LLM runs: {headline['llm_run_count']}",
        f"- Reviews: {headline['reviewed_count']}",
        f"- Accepted reviews: {headline['accepted_reviews']}",
        f"- Needs revision: {headline['needs_revision_reviews']}",
        f"- Accepted patches: {headline['accepted_patch_count']}",
        "",
        "## Management Summary",
    ]
    for item in summary["management_summary"]:
        lines.append(f"- {item}")

    lines.extend(["", "## Provider Scoreboard"])
    if summary["provider_scoreboard"]:
        for item in summary["provider_scoreboard"][:8]:
            lines.append(
                f"- `{item['provider_name']}` / `{item['provider_model']}` / `{item['stage']}`: "
                f"runs={item['run_count']}, accepted={item['accepted_review_rate']}%, "
                f"avg_prompt_tokens={item['avg_prompt_tokens']}"
            )
    else:
        lines.append("- None")

    lines.extend(["", "## Cluster Scoreboard"])
    if summary["cluster_scoreboard"]:
        for item in summary["cluster_scoreboard"][:8]:
            lines.append(
                f"- `{item['cluster_id']}`: occurrences={item['occurrence_count']}, llm_runs={item['llm_runs']}, "
                f"accepted={item['accepted_reviews']}, repairs={item['needs_revision_reviews']}, "
                f"safe_patches={item['safe_patch_candidates']}"
            )
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def render_evolution_console_html(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
    management_cards = "".join(f"<li>{escape_html(item)}</li>" for item in summary["management_summary"])
    provider_rows = "".join(
        f"<tr><td>{escape_html(item['provider_name'])}</td><td>{escape_html(item['provider_model'])}</td><td>{escape_html(item['stage'])}</td>"
        f"<td>{item['run_count']}</td><td>{item['reviewed_runs']}</td><td>{item['accepted_review_rate']}%</td>"
        f"<td>{item['avg_prompt_tokens']}</td><td>{item['avg_completion_tokens']}</td></tr>"
        for item in summary["provider_scoreboard"][:12]
    )
    cluster_rows = "".join(
        f"<tr><td>{escape_html(item['cluster_id'])}</td><td>{escape_html(item['task_type'])}</td><td>{item['occurrence_count']}</td>"
        f"<td>{item['llm_runs']}</td><td>{item['accepted_reviews']}</td><td>{item['needs_revision_reviews']}</td>"
        f"<td>{item['safe_patch_candidates']}</td></tr>"
        for item in summary["cluster_scoreboard"][:12]
    )
    repair_rows = "".join(
        f"<tr><td>{escape_html(item['cluster_id'])}</td><td>{item['needs_revision_reviews']}</td><td>{item['llm_runs']}</td></tr>"
        for item in summary["repair_hotspots"][:8]
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Legacy SQL XML Analyzer Evolution Console</title>
  <style>
    :root {{
      --bg: #f5f1e7;
      --panel: #fffaf1;
      --ink: #18202a;
      --muted: #6b6f74;
      --accent: #174b63;
      --accent-2: #8f1d22;
      --border: #d7c9b8;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", Georgia, serif;
      color: var(--ink);
      background: linear-gradient(180deg, #faf5ed 0%, var(--bg) 100%);
    }}
    .wrap {{ max-width: 1280px; margin: 0 auto; padding: 32px 20px 56px; }}
    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 18px 40px rgba(24, 32, 42, 0.08);
      margin-bottom: 16px;
    }}
    .metric-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 12px;
      margin-top: 14px;
    }}
    .metric {{
      background: rgba(23, 75, 99, 0.06);
      border-radius: 14px;
      padding: 12px;
    }}
    .metric strong {{ display: block; font-size: 1.6rem; color: var(--accent); }}
    .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 0.95rem; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid rgba(0, 0, 0, 0.08); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 600; }}
    ul {{ margin: 0; padding-left: 20px; line-height: 1.5; }}
    .footer {{ margin-top: 18px; color: var(--muted); font-size: 0.9rem; }}
    @media (max-width: 960px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <h1>Evolution Console</h1>
      <p>Operator view for weak-LLM runs, review outcomes, repair loops, and candidate patch throughput.</p>
      <div class="metric-grid">
        <div class="metric"><strong>{headline['failure_cluster_count']}</strong><span>Failure Clusters</span></div>
        <div class="metric"><strong>{headline['llm_run_count']}</strong><span>LLM Runs</span></div>
        <div class="metric"><strong>{headline['reviewed_count']}</strong><span>Reviews</span></div>
        <div class="metric"><strong>{headline['accepted_reviews']}</strong><span>Accepted</span></div>
        <div class="metric"><strong>{headline['needs_revision_reviews']}</strong><span>Needs Revision</span></div>
        <div class="metric"><strong>{headline['accepted_patch_count']}</strong><span>Accepted Patches</span></div>
      </div>
    </section>

    <section class="two-col">
      <div class="panel">
        <h2>Management Summary</h2>
        <ul>{management_cards}</ul>
      </div>
      <div class="panel">
        <h2>Repair Hotspots</h2>
        <table>
          <thead>
            <tr><th>Cluster</th><th>Needs Revision</th><th>LLM Runs</th></tr>
          </thead>
          <tbody>{repair_rows or '<tr><td colspan=\"3\">No repair hotspots yet</td></tr>'}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <h2>Provider Scoreboard</h2>
      <table>
        <thead>
          <tr><th>Provider</th><th>Model</th><th>Stage</th><th>Runs</th><th>Reviewed</th><th>Accepted</th><th>Avg Prompt Tokens</th><th>Avg Completion Tokens</th></tr>
        </thead>
        <tbody>{provider_rows or '<tr><td colspan=\"8\">No provider runs yet</td></tr>'}</tbody>
      </table>
    </section>

    <section class="panel">
      <h2>Cluster Scoreboard</h2>
      <table>
        <thead>
          <tr><th>Cluster</th><th>Task Type</th><th>Occurrences</th><th>LLM Runs</th><th>Accepted</th><th>Needs Revision</th><th>Safe Patches</th></tr>
        </thead>
        <tbody>{cluster_rows or '<tr><td colspan=\"7\">No cluster activity yet</td></tr>'}</tbody>
      </table>
      <div class="footer">Generated at {escape_html(summary['generated_at'])} · <a href="prompt_lab.html">Prompt Lab</a> · <a href="failure_console.html">Failure Console</a> · <a href="operator_console.html">Operator Console</a></div>
    </section>
  </div>
</body>
</html>
"""


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


def escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_trend_summary(output_dir: Path) -> dict[str, Any]:
    history_index_path = output_dir / "analysis" / "history" / "index.json"
    snapshots = []
    if history_index_path.exists():
        try:
            history_payload = json.loads(history_index_path.read_text(encoding="utf-8"))
            snapshots = history_payload.get("snapshots", [])
        except json.JSONDecodeError:
            snapshots = []

    normalized = []
    for item in snapshots:
        summary = item.get("summary", {})
        normalized.append(
            {
                "snapshot_id": item.get("snapshot_id"),
                "generated_at": item.get("generated_at"),
                "label": item.get("label"),
                "resolved_queries": int(summary.get("resolved_queries", 0)),
                "partial_queries": int(summary.get("partial_queries", 0)),
                "failed_queries": int(summary.get("failed_queries", 0)),
                "error_count": int(summary.get("diagnostics_by_severity", {}).get("error", 0))
                + int(summary.get("diagnostics_by_severity", {}).get("fatal", 0)),
                "warning_count": int(summary.get("diagnostics_by_severity", {}).get("warning", 0)),
            }
        )

    normalized.sort(key=lambda item: item.get("generated_at") or "")
    previous = normalized[-2] if len(normalized) >= 2 else None
    current = normalized[-1] if normalized else None
    resolved_delta = (current["resolved_queries"] - previous["resolved_queries"]) if current and previous else 0
    error_delta = (current["error_count"] - previous["error_count"]) if current and previous else 0
    warning_delta = (current["warning_count"] - previous["warning_count"]) if current and previous else 0
    status_line = ""
    if current and previous:
        if resolved_delta > 0 or error_delta < 0 or warning_delta < 0:
            status_line = "Current run is trending in a positive direction versus the previous snapshot."
        elif resolved_delta < 0 or error_delta > 0:
            status_line = "Current run regressed versus the previous snapshot and should be reviewed."
        else:
            status_line = "Current run is broadly stable versus the previous snapshot."
    elif current:
        status_line = "Trend analysis will become more meaningful after at least two runs."

    return {
        "snapshot_count": len(normalized),
        "resolved_queries_delta_vs_previous": resolved_delta,
        "error_count_delta_vs_previous": error_delta,
        "warning_count_delta_vs_previous": warning_delta,
        "status_line": status_line,
        "history": normalized,
    }


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def render_sparkline_svg(values: list[int], stroke: str) -> str:
    if not values:
        return "<svg width=\"100%\" height=\"54\" viewBox=\"0 0 240 54\"></svg>"
    if len(values) == 1:
        values = values + values
    width = 240
    height = 54
    min_value = min(values)
    max_value = max(values)
    spread = max(max_value - min_value, 1)
    points = []
    for index, value in enumerate(values):
        x = 8 + (index * (width - 16) / max(len(values) - 1, 1))
        y = height - 8 - ((value - min_value) / spread) * (height - 16)
        points.append(f"{x:.1f},{y:.1f}")
    polyline = " ".join(points)
    return (
        f"<svg width=\"100%\" height=\"54\" viewBox=\"0 0 {width} {height}\" preserveAspectRatio=\"none\">"
        f"<polyline fill=\"none\" stroke=\"{stroke}\" stroke-width=\"3\" points=\"{polyline}\" />"
        "</svg>"
    )
