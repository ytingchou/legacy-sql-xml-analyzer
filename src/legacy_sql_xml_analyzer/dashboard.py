from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import AnalysisResult, ArtifactDescriptor, ResolvedQueryModel


JOIN_PATTERN = re.compile(r"\bjoin\b", re.IGNORECASE)
SUBQUERY_PATTERN = re.compile(r"\(\s*select\b", re.IGNORECASE)
UNION_PATTERN = re.compile(r"\bunion(?:\s+all)?\b", re.IGNORECASE)
CASE_PATTERN = re.compile(r"\bcase\b", re.IGNORECASE)
PREDICATE_PATTERN = re.compile(r"\b(?:and|or)\b", re.IGNORECASE)


def write_executive_report(
    output_dir: Path,
    result: AnalysisResult,
    profile_path: Path | None,
) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    analysis_root.mkdir(parents=True, exist_ok=True)
    summary = build_executive_summary(output_dir=output_dir, result=result, profile_path=profile_path)

    json_path = analysis_root / "executive_summary.json"
    md_path = analysis_root / "executive_summary.md"
    html_path = analysis_root / "dashboard.html"
    complexity_csv_path = analysis_root / "executive_complexity.csv"
    value_csv_path = analysis_root / "executive_value.csv"
    diagnostics_csv_path = analysis_root / "executive_diagnostics.csv"
    trend_csv_path = analysis_root / "executive_trend.csv"
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

    return [
        artifact_descriptor_for_path(json_path, "json", "Executive summary", "executive"),
        artifact_descriptor_for_path(md_path, "markdown", "Executive summary (Markdown)", "executive"),
        artifact_descriptor_for_path(html_path, "html", "Executive dashboard", "executive"),
        artifact_descriptor_for_path(complexity_csv_path, "csv", "Executive complexity export", "executive"),
        artifact_descriptor_for_path(value_csv_path, "csv", "Executive value export", "executive"),
        artifact_descriptor_for_path(diagnostics_csv_path, "csv", "Executive diagnostics export", "executive"),
        artifact_descriptor_for_path(trend_csv_path, "csv", "Executive trend export", "executive"),
    ]


def build_executive_summary(
    output_dir: Path,
    result: AnalysisResult,
    profile_path: Path | None,
) -> dict[str, Any]:
    inbound_references = Counter(
        dependency
        for resolved in result.resolved_queries
        for dependency in set(resolved.dependencies)
    )
    file_diagnostics = Counter(str(diagnostic.source_path) for diagnostic in result.diagnostics)
    trend_summary = build_trend_summary(output_dir)
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
    )
    next_actions = build_next_actions(
        profile_path=profile_path,
        top_diagnostics=top_diagnostics,
        high_risk_queries=high_risk_queries,
        file_diagnostics=file_diagnostics,
        result=result,
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
    return summary


def build_next_actions(
    profile_path: Path | None,
    top_diagnostics: list[tuple[str, int]],
    high_risk_queries: list[dict[str, Any]],
    file_diagnostics: Counter[str],
    result: AnalysisResult,
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
    if profile_path is None:
        actions.append("Run the learn -> infer-rules -> freeze-profile workflow to reduce repetitive resolution failures.")
    else:
        actions.append("Use validate-profile on each updated frozen profile before adopting it in regular analysis runs.")
    return actions[:5]


def render_executive_summary_markdown(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
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

    lines.extend(["", "## Next Actions"])
    for item in summary["next_actions"]:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def render_dashboard_html(summary: dict[str, Any]) -> str:
    headline = summary["headline"]
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
    trend_rows = "".join(
        f"<tr><td>{escape_html(item['label'] or item['snapshot_id'])}</td><td>{item['resolved_queries']}</td>"
        f"<td>{item['error_count']}</td><td>{item['warning_count']}</td></tr>"
        for item in trend_history[-8:]
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
