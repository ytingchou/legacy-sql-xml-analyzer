from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyzer import analyze_directory
from .models import AnalysisResult, ArtifactDescriptor, DiagnosticModel, ParameterModel, ResolvedQueryModel
from .prompt_profiles import phase_budget_for


ORACLE_FEATURE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("nvl", re.compile(r"\bnvl\s*\(", re.IGNORECASE)),
    ("decode", re.compile(r"\bdecode\s*\(", re.IGNORECASE)),
    ("connect_by", re.compile(r"\bconnect\s+by\b", re.IGNORECASE)),
    ("start_with", re.compile(r"\bstart\s+with\b", re.IGNORECASE)),
    ("rownum", re.compile(r"\brownum\b", re.IGNORECASE)),
    ("sysdate", re.compile(r"\bsysdate\b", re.IGNORECASE)),
    ("systimestamp", re.compile(r"\bsystimestamp\b", re.IGNORECASE)),
    ("listagg", re.compile(r"\blistagg\s*\(", re.IGNORECASE)),
    ("regexp_like", re.compile(r"\bregexp_like\s*\(", re.IGNORECASE)),
    ("trunc", re.compile(r"\btrunc\s*\(", re.IGNORECASE)),
    ("to_char", re.compile(r"\bto_char\s*\(", re.IGNORECASE)),
    ("to_date", re.compile(r"\bto_date\s*\(", re.IGNORECASE)),
    ("merge", re.compile(r"^\s*merge\b", re.IGNORECASE)),
]

CLAUSE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("select", re.compile(r"\bselect\b", re.IGNORECASE)),
    ("from", re.compile(r"\bfrom\b", re.IGNORECASE)),
    ("join", re.compile(r"\bjoin\b", re.IGNORECASE)),
    ("where", re.compile(r"\bwhere\b", re.IGNORECASE)),
    ("group_by", re.compile(r"\bgroup\s+by\b", re.IGNORECASE)),
    ("having", re.compile(r"\bhaving\b", re.IGNORECASE)),
    ("order_by", re.compile(r"\border\s+by\b", re.IGNORECASE)),
    ("union", re.compile(r"\bunion(?:\s+all)?\b", re.IGNORECASE)),
]

BFF_PHASES = (
    "phase-1-plan",
    "phase-2-repository-chunk",
    "phase-2-repository-merge",
    "phase-3-bff-assembly",
    "phase-4-verify",
)


@dataclass(slots=True)
class SqlChunk:
    chunk_id: str
    query_id: str
    sequence: int
    estimated_tokens: int
    start_line: int
    end_line: int
    sql_excerpt: str
    clause_hints: list[str]
    implementation_focus: list[str]
    output_path_json: str | None = None
    output_path_md: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "chunk_id": self.chunk_id,
            "query_id": self.query_id,
            "sequence": self.sequence,
            "estimated_tokens": self.estimated_tokens,
            "start_line": self.start_line,
            "end_line": self.end_line,
            "sql_excerpt": self.sql_excerpt,
            "clause_hints": self.clause_hints,
            "implementation_focus": self.implementation_focus,
            "output_path_json": self.output_path_json,
            "output_path_md": self.output_path_md,
        }


def prepare_java_bff_from_input(
    input_dir: Path,
    output_dir: Path,
    profile_path: Path | None = None,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
    prompt_profile: str = "qwen3-128k-java-bff",
    max_sql_chunk_tokens: int | None = None,
) -> dict[str, Any]:
    result = analyze_directory(
        input_dir=input_dir,
        output_dir=output_dir,
        profile_path=profile_path,
        entry_file=entry_file,
        entry_main_query=entry_main_query,
    )
    package = write_java_bff_artifacts(
        output_dir=output_dir,
        result=result,
        entry_file=entry_file,
        entry_main_query=entry_main_query,
        prompt_profile=prompt_profile,
        max_sql_chunk_tokens=max_sql_chunk_tokens,
    )
    return {
        "analysis": result,
        "summary": package["summary"],
        "artifacts": package["artifacts"],
        "overview_path": package["overview_path"],
        "chunk_manifest_path": package["chunk_manifest_path"],
    }


def write_java_bff_artifacts(
    output_dir: Path,
    result: AnalysisResult,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
    prompt_profile: str = "qwen3-128k-java-bff",
    max_sql_chunk_tokens: int | None = None,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    analysis_root = output_dir / "analysis"
    java_root = analysis_root / "java_bff"
    cards_root = java_root / "implementation_cards"
    chunks_root = java_root / "sql_chunks"
    bundles_root = java_root / "bundles"
    phase_root = java_root / "phase_packs"
    for directory in (java_root, cards_root, chunks_root, bundles_root, phase_root):
        directory.mkdir(parents=True, exist_ok=True)

    resolved_map = {item.query.id: item for item in result.resolved_queries}
    diagnostics_by_query = group_diagnostics_by_query(result.diagnostics)
    selected_entries = select_entry_queries(result, entry_file=entry_file, entry_main_query=entry_main_query)
    if not selected_entries:
        raise ValueError("No matching main-query found for Java BFF artifact packaging.")

    prompt_budget = phase_budget_for(prompt_profile, "phase-2-repository-chunk")
    chunk_token_limit = max_sql_chunk_tokens or min(6_000, max(1_200, prompt_budget["usable_input_limit"] // 3))

    artifacts: list[ArtifactDescriptor] = []
    global_chunk_rows: list[dict[str, Any]] = []
    bundle_rows: list[dict[str, Any]] = []
    processed_queries: set[str] = set()

    for entry in selected_entries:
        bundle_queries = bundle_query_sequence(entry, resolved_map)
        bundle_slug = safe_name(entry.query.id)
        bundle_root = bundles_root / bundle_slug
        bundle_phase_root = phase_root / bundle_slug
        bundle_root.mkdir(parents=True, exist_ok=True)
        bundle_phase_root.mkdir(parents=True, exist_ok=True)

        query_rows: list[dict[str, Any]] = []
        phase_rows: list[dict[str, Any]] = []
        bundle_chunk_ids: list[str] = []
        for resolved in bundle_queries:
            card_payload = build_implementation_card(
                resolved=resolved,
                diagnostics=diagnostics_by_query.get(resolved.query.id, []),
                prompt_profile=prompt_profile,
            )
            card_slug = safe_name(resolved.query.id)
            card_json_path = cards_root / f"{card_slug}.json"
            card_md_path = cards_root / f"{card_slug}.md"
            if resolved.query.id not in processed_queries:
                card_json_path.write_text(json.dumps(card_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                card_md_path.write_text(render_implementation_card_markdown(card_payload), encoding="utf-8")
                artifacts.extend(
                    [
                        artifact_descriptor_for_path(card_json_path, "json", f"Java BFF implementation card: {resolved.query.id}", "java_bff"),
                        artifact_descriptor_for_path(card_md_path, "markdown", f"Java BFF implementation card (Markdown): {resolved.query.id}", "java_bff"),
                    ]
                )

            sql_chunks = chunk_sql_for_query(resolved, max_chunk_tokens=chunk_token_limit)
            for chunk in sql_chunks:
                chunk_slug = safe_name(chunk.chunk_id)
                chunk_json_path = chunks_root / f"{chunk_slug}.json"
                chunk_md_path = chunks_root / f"{chunk_slug}.md"
                chunk.output_path_json = str(chunk_json_path)
                chunk.output_path_md = str(chunk_md_path)
                if resolved.query.id not in processed_queries or not chunk_json_path.exists():
                    chunk_json_path.write_text(json.dumps(chunk.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
                    chunk_md_path.write_text(render_sql_chunk_markdown(chunk), encoding="utf-8")
                    artifacts.extend(
                        [
                            artifact_descriptor_for_path(chunk_json_path, "json", f"Java BFF SQL chunk: {chunk.chunk_id}", "java_bff"),
                            artifact_descriptor_for_path(chunk_md_path, "markdown", f"Java BFF SQL chunk (Markdown): {chunk.chunk_id}", "java_bff"),
                        ]
                    )
                global_chunk_rows.append(
                    {
                        "bundle_id": entry.query.id,
                        "query_id": resolved.query.id,
                        "chunk_id": chunk.chunk_id,
                        "sequence": chunk.sequence,
                        "estimated_tokens": chunk.estimated_tokens,
                        "prompt_phase": "phase-2-repository-chunk",
                        "safe_for_qwen3": chunk.estimated_tokens <= prompt_budget["usable_input_limit"],
                    }
                )
                bundle_chunk_ids.append(chunk.chunk_id)

            compact_summary = compact_query_summary(card_payload)
            query_rows.append(
                {
                    "query_id": resolved.query.id,
                    "status": resolved.status,
                    "chunk_count": len(sql_chunks),
                    "statement_type": card_payload["sql_logic"]["statement_type"],
                    "estimated_sql_tokens": estimate_tokens(resolved.resolved_sql or resolved.query.raw_sql),
                    "card_json_path": str(card_json_path),
                    "card_md_path": str(card_md_path),
                    "summary": compact_summary,
                }
            )
            phase_rows.extend(
                write_repository_phase_prompts(
                    bundle_phase_root=bundle_phase_root,
                    entry_query=entry,
                    card_payload=card_payload,
                    sql_chunks=sql_chunks,
                    prompt_profile=prompt_profile,
                )
            )
            processed_queries.add(resolved.query.id)

        phase_rows.append(
            write_plan_phase_prompt(
                bundle_phase_root=bundle_phase_root,
                entry_query=entry,
                bundle_queries=query_rows,
                prompt_profile=prompt_profile,
            )
        )
        phase_rows.append(
            write_assembly_phase_prompt(
                bundle_phase_root=bundle_phase_root,
                entry_query=entry,
                bundle_queries=query_rows,
                prompt_profile=prompt_profile,
            )
        )
        phase_rows.append(
            write_verify_phase_prompt(
                bundle_phase_root=bundle_phase_root,
                entry_query=entry,
                bundle_queries=query_rows,
                prompt_profile=prompt_profile,
            )
        )
        for phase_row in phase_rows:
            artifacts.extend(
                [
                    artifact_descriptor_for_path(Path(phase_row["json_path"]), "json", f"Java BFF phase prompt: {phase_row['phase']}", "java_bff"),
                    artifact_descriptor_for_path(Path(phase_row["txt_path"]), "text", f"Java BFF phase prompt text: {phase_row['phase']}", "java_bff"),
                ]
            )

        bundle_payload = build_bundle_payload(
            entry_query=entry,
            query_rows=query_rows,
            phase_rows=phase_rows,
            prompt_profile=prompt_profile,
            chunk_token_limit=chunk_token_limit,
        )
        bundle_json_path = bundle_root / "bundle.json"
        bundle_md_path = bundle_root / "bundle.md"
        bundle_json_path.write_text(json.dumps(bundle_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        bundle_md_path.write_text(render_bundle_markdown(bundle_payload), encoding="utf-8")
        artifacts.extend(
            [
                artifact_descriptor_for_path(bundle_json_path, "json", f"Java BFF bundle: {entry.query.id}", "java_bff"),
                artifact_descriptor_for_path(bundle_md_path, "markdown", f"Java BFF bundle (Markdown): {entry.query.id}", "java_bff"),
            ]
        )
        bundle_rows.append(bundle_payload)

    overview_payload = build_bff_overview(
        entry_rows=bundle_rows,
        prompt_profile=prompt_profile,
        selected_entries=selected_entries,
        chunk_token_limit=chunk_token_limit,
    )
    overview_json_path = java_root / "overview.json"
    overview_md_path = java_root / "overview.md"
    overview_json_path.write_text(json.dumps(overview_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    overview_md_path.write_text(render_overview_markdown(overview_payload), encoding="utf-8")
    artifacts.extend(
        [
            artifact_descriptor_for_path(overview_json_path, "json", "Java BFF overview", "java_bff"),
            artifact_descriptor_for_path(overview_md_path, "markdown", "Java BFF overview (Markdown)", "java_bff"),
        ]
    )

    chunk_manifest_payload = build_chunk_manifest_payload(
        overview_payload=overview_payload,
        bundle_rows=bundle_rows,
        chunk_rows=global_chunk_rows,
    )
    chunk_manifest_json_path = java_root / "chunk_manifest.json"
    chunk_manifest_md_path = java_root / "chunk_manifest.md"
    chunk_manifest_json_path.write_text(json.dumps(chunk_manifest_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    chunk_manifest_md_path.write_text(render_chunk_manifest_markdown(chunk_manifest_payload), encoding="utf-8")
    artifacts.extend(
        [
            artifact_descriptor_for_path(chunk_manifest_json_path, "json", "Java BFF chunk manifest", "java_bff"),
            artifact_descriptor_for_path(chunk_manifest_md_path, "markdown", "Java BFF chunk manifest (Markdown)", "java_bff"),
        ]
    )

    append_artifacts_to_index(output_dir, artifacts)
    return {
        "summary": {
            "bundle_count": len(bundle_rows),
            "entry_query_count": len(selected_entries),
            "chunk_count": len(global_chunk_rows),
            "prompt_count": sum(len(bundle["phase_prompts"]) for bundle in bundle_rows),
            "chunk_token_limit": chunk_token_limit,
            "prompt_profile": prompt_profile,
        },
        "artifacts": artifacts,
        "overview_path": overview_json_path,
        "chunk_manifest_path": chunk_manifest_json_path,
    }


def select_entry_queries(
    result: AnalysisResult,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
) -> list[ResolvedQueryModel]:
    entries = [item for item in result.resolved_queries if item.query.query_type == "main"]
    if entry_file:
        entries = [item for item in entries if item.query.source_path.name == entry_file]
    if entry_main_query:
        entries = [item for item in entries if item.query.name == entry_main_query]
    entries.sort(key=lambda item: (item.query.source_path.name, item.query.name))
    return entries


def bundle_query_sequence(entry: ResolvedQueryModel, resolved_map: dict[str, ResolvedQueryModel]) -> list[ResolvedQueryModel]:
    seen: set[str] = set()
    ordered: list[ResolvedQueryModel] = []

    def add(query_id: str) -> None:
        if query_id in seen:
            return
        seen.add(query_id)
        resolved = resolved_map.get(query_id)
        if resolved is None:
            return
        ordered.append(resolved)

    add(entry.query.id)
    for dependency in entry.dependencies:
        add(dependency)
    if len(ordered) <= 1:
        return ordered
    return [ordered[0]] + sorted(ordered[1:], key=lambda value: (value.query.query_type != "sub", value.query.name))


def build_implementation_card(
    resolved: ResolvedQueryModel,
    diagnostics: list[DiagnosticModel],
    prompt_profile: str,
) -> dict[str, Any]:
    sql = resolved.resolved_sql or resolved.query.raw_sql
    sql_logic = build_sql_logic_summary(resolved, sql)
    binding_hints = [parameter_binding_hint(parameter) for parameter in resolved.query.parameters]
    manual_review_flags = build_manual_review_flags(resolved)
    repository_style = "NamedParameterJdbcTemplate"
    method_name = recommended_repository_method_name(resolved)
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "query_id": resolved.query.id,
        "file": str(resolved.query.source_path),
        "query_type": resolved.query.query_type,
        "query_name": resolved.query.name,
        "status": resolved.status,
        "dependencies": resolved.dependencies,
        "prompt_profile": prompt_profile,
        "parameters": binding_hints,
        "diagnostics": [
            {
                "code": item.code,
                "severity": item.severity,
                "message": item.message,
                "suggested_fix": item.suggested_fix,
            }
            for item in diagnostics[:5]
        ],
        "sql_logic": sql_logic,
        "java_bff_logic": {
            "recommended_repository_style": repository_style,
            "method_name": method_name,
            "repository_steps": build_repository_steps(resolved, repository_style, method_name),
            "service_steps": build_service_steps(resolved),
            "controller_notes": build_controller_notes(resolved),
            "result_shape_guidance": result_shape_guidance(resolved),
            "manual_review_flags": manual_review_flags,
        },
    }


def build_sql_logic_summary(resolved: ResolvedQueryModel, sql: str) -> dict[str, Any]:
    oracle_features = [name for name, pattern in ORACLE_FEATURE_PATTERNS if pattern.search(sql)]
    return {
        "statement_type": str(resolved.sql_stats.get("statement_type", "unknown")),
        "tables": list(resolved.sql_stats.get("tables", [])),
        "parameters_used": list(resolved.sql_stats.get("parameters", [])),
        "sql_skeleton": resolved.sql_skeleton or "",
        "character_count": int(resolved.sql_stats.get("character_count", len(sql))),
        "line_count": int(resolved.sql_stats.get("line_count", len(sql.splitlines()))),
        "oracle_features": oracle_features,
        "select_preview": clause_preview(sql, "select", "from"),
        "from_preview": clause_preview(sql, "from", "where"),
        "where_preview": clause_preview(sql, "where", "group by"),
        "group_by_preview": clause_preview(sql, "group by", "having"),
        "order_by_preview": clause_preview(sql, "order by", None),
    }


def parameter_binding_hint(parameter: ParameterModel) -> dict[str, Any]:
    java_name = to_java_identifier(parameter.name)
    notes: list[str] = []
    if parameter.data_type and parameter.data_type.lower() == "sql":
        notes.append("This parameter looks like a raw SQL fragment and needs manual whitelisting.")
    if parameter.data_type and parameter.data_type.lower().endswith("array"):
        notes.append("This parameter may need explicit collection expansion before executing Oracle SQL.")
    return {
        "parameter_name": parameter.name,
        "java_argument_name": java_name,
        "data_type": parameter.data_type,
        "sample": parameter.sample,
        "default": parameter.default,
        "binding_strategy": "Bind with MapSqlParameterSource using the original SQL placeholder name.",
        "notes": notes,
    }


def build_manual_review_flags(resolved: ResolvedQueryModel) -> list[str]:
    flags: list[str] = []
    if resolved.status != "resolved":
        flags.append("The query did not fully resolve. Do not invent missing SQL fragments.")
    if any((parameter.data_type or "").lower() == "sql" for parameter in resolved.query.parameters):
        flags.append("At least one parameter is marked as SQL and must be manually validated before execution.")
    if any((parameter.data_type or "").lower().endswith("array") for parameter in resolved.query.parameters):
        flags.append("Array-style parameters need explicit Java-side expansion or Oracle-compatible collection handling.")
    return flags


def build_repository_steps(resolved: ResolvedQueryModel, repository_style: str, method_name: str) -> list[str]:
    statement_type = str(resolved.sql_stats.get("statement_type", "unknown"))
    steps = [
        f"Create a repository method `{method_name}` backed by `{repository_style}` so the Oracle 19c SQL can remain close to the analyzed query.",
        "Build a dedicated MapSqlParameterSource or immutable parameter map from the incoming API/service inputs.",
        "Keep the SQL as an external constant or multiline text block so Oracle-specific syntax is not re-written by the model.",
    ]
    if statement_type == "select":
        steps.append("Use query(...) or queryForObject(...) only after the API contract confirms whether the expected cardinality is list or single-row.")
        steps.append("Add a RowMapper or manual column-to-DTO projection after the SQL contract is confirmed.")
    else:
        steps.append("Execute the SQL as a DML statement and return affected row count or a service-level success contract.")
        steps.append("Do not send trailing semicolons to JDBC execution even if the original XML carried them for DML hygiene.")
    return steps


def build_service_steps(resolved: ResolvedQueryModel) -> list[str]:
    steps = [
        "Validate and normalize incoming API fields before invoking the repository.",
        "Translate API naming into the original SQL parameter names instead of re-shaping the SQL itself.",
        "Keep orchestration and fallback logic in the service layer, not inside the repository SQL text.",
    ]
    if resolved.dependencies:
        steps.append("If dependent sub-queries exist, keep their composition order stable and document which repository call owns the final assembled SQL.")
    return steps


def build_controller_notes(resolved: ResolvedQueryModel) -> list[str]:
    statement_type = str(resolved.sql_stats.get("statement_type", "unknown"))
    notes = [
        "Keep the controller thin: accept request DTOs, call the service, and return BFF-oriented response DTOs.",
        "Avoid leaking Oracle column aliases directly into the external API contract until the service DTO is defined.",
    ]
    if statement_type == "select":
        notes.append("Support pagination or sorting only if the analyzed SQL already contains a stable ordering strategy.")
    else:
        notes.append("For DML-oriented flows, expose business confirmation instead of raw JDBC row counts unless the API contract explicitly needs them.")
    return notes


def result_shape_guidance(resolved: ResolvedQueryModel) -> str:
    statement_type = str(resolved.sql_stats.get("statement_type", "unknown"))
    if statement_type != "select":
        return "Treat this as a write path. Return affected-row semantics or a service-defined business response."
    if re.search(r"\bcount\s*\(", resolved.resolved_sql or resolved.query.raw_sql, re.IGNORECASE):
        return "The SQL appears to aggregate counts. Single-row or scalar return is likely, but confirm with the API contract."
    return "Cardinality is not guaranteed by the analyzer. Default to list-style repository logic unless the API contract confirms a single-row response."


def chunk_sql_for_query(resolved: ResolvedQueryModel, max_chunk_tokens: int) -> list[SqlChunk]:
    sql = (resolved.resolved_sql or resolved.query.raw_sql).strip()
    if not sql:
        return [
            SqlChunk(
                chunk_id=f"{resolved.query.id}:chunk:01",
                query_id=resolved.query.id,
                sequence=1,
                estimated_tokens=0,
                start_line=1,
                end_line=1,
                sql_excerpt="",
                clause_hints=[],
                implementation_focus=["No SQL body was available. Treat this as a manual reconstruction task."],
            )
        ]
    lines = sql.splitlines() or [sql]
    chunks: list[SqlChunk] = []
    buffer: list[str] = []
    start_line = 1
    current_tokens = 0
    sequence = 1
    for index, line in enumerate(lines, start=1):
        token_cost = estimate_tokens(line + "\n")
        if buffer and current_tokens + token_cost > max_chunk_tokens:
            chunks.append(
                build_sql_chunk(
                    query_id=resolved.query.id,
                    sequence=sequence,
                    start_line=start_line,
                    end_line=index - 1,
                    text="\n".join(buffer).strip(),
                )
            )
            sequence += 1
            buffer = [line]
            start_line = index
            current_tokens = token_cost
        else:
            buffer.append(line)
            current_tokens += token_cost
    if buffer:
        chunks.append(
            build_sql_chunk(
                query_id=resolved.query.id,
                sequence=sequence,
                start_line=start_line,
                end_line=len(lines),
                text="\n".join(buffer).strip(),
            )
        )
    return chunks


def build_sql_chunk(query_id: str, sequence: int, start_line: int, end_line: int, text: str) -> SqlChunk:
    clause_hints = [name for name, pattern in CLAUSE_PATTERNS if pattern.search(text)]
    implementation_focus = [
        "Preserve Oracle 19c syntax and placeholder names exactly as shown in this chunk.",
        "Describe Java-side binding and orchestration logic for this chunk only.",
    ]
    if "join" in clause_hints:
        implementation_focus.append("Keep alias usage and join order stable when describing repository logic.")
    if "where" in clause_hints:
        implementation_focus.append("Call out which request fields must be normalized before they are bound into WHERE predicates.")
    if "group_by" in clause_hints or "order_by" in clause_hints:
        implementation_focus.append("Explain aggregation or ordering behavior instead of rewriting the SQL clause.")
    return SqlChunk(
        chunk_id=f"{query_id}:chunk:{sequence:02d}",
        query_id=query_id,
        sequence=sequence,
        estimated_tokens=estimate_tokens(text),
        start_line=start_line,
        end_line=end_line,
        sql_excerpt=text,
        clause_hints=clause_hints,
        implementation_focus=implementation_focus,
    )


def write_repository_phase_prompts(
    bundle_phase_root: Path,
    entry_query: ResolvedQueryModel,
    card_payload: dict[str, Any],
    sql_chunks: list[SqlChunk],
    prompt_profile: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chunk in sql_chunks:
        payload = build_repository_chunk_prompt(card_payload, chunk, prompt_profile)
        path_base = bundle_phase_root / f"{safe_name(card_payload['query_id'])}-phase-2-repository-{chunk.sequence:02d}"
        rows.append(write_prompt_payload(path_base, payload))
    merge_payload = build_repository_merge_prompt(card_payload, sql_chunks, prompt_profile)
    merge_base = bundle_phase_root / f"{safe_name(card_payload['query_id'])}-phase-2-repository-merge"
    rows.append(write_prompt_payload(merge_base, merge_payload))
    return rows


def write_plan_phase_prompt(
    bundle_phase_root: Path,
    entry_query: ResolvedQueryModel,
    bundle_queries: list[dict[str, Any]],
    prompt_profile: str,
) -> dict[str, Any]:
    payload = build_plan_prompt(entry_query, bundle_queries, prompt_profile)
    return write_prompt_payload(bundle_phase_root / "phase-1-plan", payload)


def write_assembly_phase_prompt(
    bundle_phase_root: Path,
    entry_query: ResolvedQueryModel,
    bundle_queries: list[dict[str, Any]],
    prompt_profile: str,
) -> dict[str, Any]:
    payload = build_assembly_prompt(entry_query, bundle_queries, prompt_profile)
    return write_prompt_payload(bundle_phase_root / "phase-3-bff-assembly", payload)


def write_verify_phase_prompt(
    bundle_phase_root: Path,
    entry_query: ResolvedQueryModel,
    bundle_queries: list[dict[str, Any]],
    prompt_profile: str,
) -> dict[str, Any]:
    payload = build_verify_prompt(entry_query, bundle_queries, prompt_profile)
    return write_prompt_payload(bundle_phase_root / "phase-4-verify", payload)


def build_plan_prompt(entry_query: ResolvedQueryModel, bundle_queries: list[dict[str, Any]], prompt_profile: str) -> dict[str, Any]:
    phase = "phase-1-plan"
    schema = {
        "entry_query_id": entry_query.query.id,
        "repository_methods": [
            {
                "query_id": "string",
                "method_name": "string",
                "purpose": "string",
                "input_params": ["string"],
                "result_contract": "string",
            }
        ],
        "service_flow": ["string"],
        "controller_contract_hints": ["string"],
        "risks": ["string"],
        "open_questions": ["string"],
    }
    summary_lines = []
    for item in bundle_queries:
        summary_lines.append(
            f"- {item['query_id']} status={item['status']} chunks={item['chunk_count']} "
            f"statement={item['statement_type']} summary={item['summary']}"
        )
    prefix_lines = [
        "You are preparing implementation logic for a Java Spring Boot BFF API.",
        "Target database: Oracle 19c.",
        "Target Java access pattern: NamedParameterJdbcTemplate unless a strong reason says otherwise.",
        "Goal: produce repository/service/controller implementation logic only. Do not emit Java code yet.",
        "Hard constraints:",
        "- Preserve SQL semantics from the analyzer artifacts.",
        "- Do not invent business fields or hidden joins.",
        "- Keep one repository method per analyzed query.",
        "- Return JSON only.",
        "",
        f"Entry query: {entry_query.query.id}",
        "Bundle query summaries:",
    ]
    suffix_lines = [
        "",
        "Return JSON only with this schema:",
        json.dumps(schema, indent=2, ensure_ascii=False),
    ]
    prompt_text = build_budgeted_prompt_text(prefix_lines, summary_lines, suffix_lines, prompt_profile, phase)
    return build_phase_prompt_payload(
        phase=phase,
        bundle_id=entry_query.query.id,
        prompt_profile=prompt_profile,
        prompt_text=prompt_text,
        schema=schema,
        recommended_input_artifacts=["overview.json", "bundle.json", "implementation_cards/*.json"],
    )


def build_repository_chunk_prompt(card_payload: dict[str, Any], chunk: SqlChunk, prompt_profile: str) -> dict[str, Any]:
    phase = "phase-2-repository-chunk"
    schema = {
        "query_id": card_payload["query_id"],
        "chunk_id": chunk.chunk_id,
        "method_name": card_payload["java_bff_logic"]["method_name"],
        "parameter_binding": [
            {
                "parameter_name": "string",
                "java_argument_name": "string",
                "binding_note": "string",
            }
        ],
        "sql_logic_steps": ["string"],
        "oracle_19c_notes": ["string"],
        "row_mapping_notes": ["string"],
        "manual_review_flags": ["string"],
        "carry_forward_context": ["string"],
    }
    prompt_text = "\n".join(
        [
            "You are explaining repository implementation logic for one SQL chunk only.",
            "Target stack: Java Spring Boot BFF + NamedParameterJdbcTemplate + Oracle 19c.",
            "Do not emit Java code. Explain logic and parameter binding only for this chunk.",
            "Do not guess missing SQL from other chunks.",
            "Return JSON only.",
            "",
            f"Query: {card_payload['query_id']}",
            f"Recommended method name: {card_payload['java_bff_logic']['method_name']}",
            f"Status: {card_payload['status']}",
            f"Parameters: {json.dumps(card_payload['parameters'], ensure_ascii=False)}",
            f"Manual review flags: {json.dumps(card_payload['java_bff_logic']['manual_review_flags'], ensure_ascii=False)}",
            "",
            f"Chunk id: {chunk.chunk_id}",
            f"Chunk line range: {chunk.start_line}-{chunk.end_line}",
            f"Chunk clause hints: {', '.join(chunk.clause_hints) or 'none'}",
            "Chunk SQL:",
            "```sql",
            chunk.sql_excerpt,
            "```",
            "",
            "Return JSON only with this schema:",
            json.dumps(schema, indent=2, ensure_ascii=False),
        ]
    ).rstrip() + "\n"
    return build_phase_prompt_payload(
        phase=phase,
        bundle_id=card_payload["query_id"],
        prompt_profile=prompt_profile,
        prompt_text=prompt_text,
        schema=schema,
        recommended_input_artifacts=[card_payload["query_id"], chunk.output_path_json or chunk.chunk_id],
    )


def build_repository_merge_prompt(card_payload: dict[str, Any], chunks: list[SqlChunk], prompt_profile: str) -> dict[str, Any]:
    phase = "phase-2-repository-merge"
    schema = {
        "query_id": card_payload["query_id"],
        "method_name": card_payload["java_bff_logic"]["method_name"],
        "repository_logic": ["string"],
        "parameter_contract": ["string"],
        "sql_chunk_order": [chunk.chunk_id for chunk in chunks],
        "oracle_19c_risks": ["string"],
        "manual_review_flags": ["string"],
    }
    chunk_list = ", ".join(chunk.chunk_id for chunk in chunks)
    prompt_text = "\n".join(
        [
            "You are merging per-chunk repository logic into one query-level implementation plan.",
            "Do not invent SQL that was not already covered by prior chunk outputs.",
            "Use the original analyzed query id and method name.",
            "Return JSON only.",
            "",
            f"Query: {card_payload['query_id']}",
            f"Method name: {card_payload['java_bff_logic']['method_name']}",
            f"Chunk order: {chunk_list}",
            "Prior chunk outputs to merge:",
            "PASTE_PHASE_2_CHUNK_OUTPUTS_HERE",
            "",
            "Return JSON only with this schema:",
            json.dumps(schema, indent=2, ensure_ascii=False),
        ]
    ).rstrip() + "\n"
    return build_phase_prompt_payload(
        phase=phase,
        bundle_id=card_payload["query_id"],
        prompt_profile=prompt_profile,
        prompt_text=prompt_text,
        schema=schema,
        recommended_input_artifacts=[card_payload["query_id"], "phase-2-repository chunk outputs"],
    )


def build_assembly_prompt(entry_query: ResolvedQueryModel, bundle_queries: list[dict[str, Any]], prompt_profile: str) -> dict[str, Any]:
    phase = "phase-3-bff-assembly"
    schema = {
        "entry_query_id": entry_query.query.id,
        "service_logic": ["string"],
        "controller_logic": ["string"],
        "dto_contract_hints": ["string"],
        "error_handling": ["string"],
        "follow_up_questions": ["string"],
    }
    query_lines = [
        f"- {item['query_id']} status={item['status']} chunk_count={item['chunk_count']} summary={item['summary']}"
        for item in bundle_queries
    ]
    prefix_lines = [
        "You are assembling Java Spring Boot BFF implementation logic from repository-level artifacts.",
        "Do not generate Java code. Produce service/controller orchestration logic only.",
        "If repository chunk outputs are missing, say so instead of guessing.",
        "Return JSON only.",
        "",
        f"Entry query: {entry_query.query.id}",
        "Included queries:",
    ]
    suffix_lines = [
        "",
        "Inputs available:",
        "- phase-1-plan output",
        "- phase-2-repository-merge outputs for each query",
        "- implementation cards",
        "",
        "Return JSON only with this schema:",
        json.dumps(schema, indent=2, ensure_ascii=False),
    ]
    prompt_text = build_budgeted_prompt_text(prefix_lines, query_lines, suffix_lines, prompt_profile, phase)
    return build_phase_prompt_payload(
        phase=phase,
        bundle_id=entry_query.query.id,
        prompt_profile=prompt_profile,
        prompt_text=prompt_text,
        schema=schema,
        recommended_input_artifacts=["phase-1-plan output", "phase-2-repository-merge outputs"],
    )


def build_verify_prompt(entry_query: ResolvedQueryModel, bundle_queries: list[dict[str, Any]], prompt_profile: str) -> dict[str, Any]:
    phase = "phase-4-verify"
    schema = {
        "bundle_id": entry_query.query.id,
        "verdict": "ready | needs_more_context | unsafe_guess_detected",
        "token_budget_check": {
            "within_limit": True,
            "recommended_next_prompt": "string",
        },
        "oracle_19c_risks": ["string"],
        "guess_risks": ["string"],
        "missing_artifacts": ["string"],
        "final_recommendations": ["string"],
    }
    query_lines = [
        f"- {item['query_id']} status={item['status']} chunks={item['chunk_count']}"
        for item in bundle_queries
    ]
    prefix_lines = [
        "You are validating a weak-LLM implementation logic package for Java Spring Boot BFF + Oracle 19c.",
        "Check only for completeness, Oracle compatibility, and weak-model token safety.",
        "Do not invent missing business logic.",
        "Return JSON only.",
        "",
        f"Bundle id: {entry_query.query.id}",
        "Bundle queries:",
    ]
    suffix_lines = [
        "",
        "Available prior outputs:",
        "- phase-1-plan output",
        "- phase-2-repository chunk outputs",
        "- phase-2-repository-merge outputs",
        "- phase-3-bff-assembly output",
        "",
        "Return JSON only with this schema:",
        json.dumps(schema, indent=2, ensure_ascii=False),
    ]
    prompt_text = build_budgeted_prompt_text(prefix_lines, query_lines, suffix_lines, prompt_profile, phase)
    return build_phase_prompt_payload(
        phase=phase,
        bundle_id=entry_query.query.id,
        prompt_profile=prompt_profile,
        prompt_text=prompt_text,
        schema=schema,
        recommended_input_artifacts=["phase-1 to phase-3 outputs", "bundle.json"],
    )


def build_phase_prompt_payload(
    phase: str,
    bundle_id: str,
    prompt_profile: str,
    prompt_text: str,
    schema: dict[str, Any],
    recommended_input_artifacts: list[str],
) -> dict[str, Any]:
    budget = phase_budget_for(prompt_profile, phase)
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "phase": phase,
        "bundle_id": bundle_id,
        "prompt_profile": prompt_profile,
        "estimated_prompt_tokens": estimate_tokens(prompt_text),
        "budget": budget,
        "recommended_input_artifacts": recommended_input_artifacts,
        "answer_schema": schema,
        "prompt_text": prompt_text,
        "safe_for_qwen3": estimate_tokens(prompt_text) <= budget["usable_input_limit"],
    }


def build_budgeted_prompt_text(
    prefix_lines: list[str],
    summary_lines: list[str],
    suffix_lines: list[str],
    prompt_profile: str,
    phase: str,
) -> str:
    budget = phase_budget_for(prompt_profile, phase)["usable_input_limit"]
    kept_lines = list(prefix_lines)
    overflow = False
    for line in summary_lines:
        candidate = "\n".join(kept_lines + [line] + suffix_lines).rstrip() + "\n"
        if estimate_tokens(candidate) > budget:
            overflow = True
            break
        kept_lines.append(line)
    if overflow:
        kept_lines.append("- [Additional query summaries omitted to stay within the weak-model token budget.]")
    return "\n".join(kept_lines + suffix_lines).rstrip() + "\n"


def write_prompt_payload(path_base: Path, payload: dict[str, Any]) -> dict[str, Any]:
    path_base.parent.mkdir(parents=True, exist_ok=True)
    json_path = Path(f"{path_base}.json")
    txt_path = Path(f"{path_base}.txt")
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    txt_path.write_text(payload["prompt_text"], encoding="utf-8")
    return {
        "phase": payload["phase"],
        "json_path": str(json_path),
        "txt_path": str(txt_path),
        "estimated_prompt_tokens": payload["estimated_prompt_tokens"],
        "safe_for_qwen3": payload["safe_for_qwen3"],
    }


def build_bundle_payload(
    entry_query: ResolvedQueryModel,
    query_rows: list[dict[str, Any]],
    phase_rows: list[dict[str, Any]],
    prompt_profile: str,
    chunk_token_limit: int,
) -> dict[str, Any]:
    recommended_sequence = [item["txt_path"] for item in sorted(phase_rows, key=phase_sort_key)]
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_id": entry_query.query.id,
        "entry_query_id": entry_query.query.id,
        "entry_query_name": entry_query.query.name,
        "entry_file": str(entry_query.query.source_path),
        "status": entry_query.status,
        "prompt_profile": prompt_profile,
        "chunk_token_limit": chunk_token_limit,
        "query_count": len(query_rows),
        "queries": query_rows,
        "phase_prompts": sorted(phase_rows, key=phase_sort_key),
        "recommended_sequence": recommended_sequence,
        "usage_rules": [
            "Feed only one phase prompt at a time to Qwen3.",
            "Finish every phase-2 repository chunk prompt before running the phase-2 merge prompt.",
            "Do not paste the whole java_bff folder into one prompt.",
            "If a query status is partial or failed, treat the diagnostics as a blocker instead of guessing missing SQL.",
        ],
    }


def build_bff_overview(
    entry_rows: list[dict[str, Any]],
    prompt_profile: str,
    selected_entries: list[ResolvedQueryModel],
    chunk_token_limit: int,
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "target_runtime": {
            "framework": "java spring boot bff",
            "database": "oracle 19c",
            "recommended_data_access": "NamedParameterJdbcTemplate",
        },
        "weak_model_strategy": {
            "model_profile": prompt_profile,
            "hard_token_limit": 128000,
            "recommended_single_prompt_limit": 24000,
            "sql_chunk_token_limit": chunk_token_limit,
            "rules": [
                "Feed one bundle at a time.",
                "Feed one phase-2 repository chunk prompt at a time.",
                "Use JSON-only prompts because Qwen3 is weak and can drift without schema guidance.",
                "Treat partial or failed queries as blockers instead of asking the model to reconstruct hidden SQL.",
            ],
        },
        "entry_query_count": len(selected_entries),
        "bundle_count": len(entry_rows),
        "bundles": [
            {
                "bundle_id": item["bundle_id"],
                "entry_query_id": item["entry_query_id"],
                "query_count": item["query_count"],
                "prompt_count": len(item["phase_prompts"]),
                "recommended_sequence": item["recommended_sequence"],
            }
            for item in entry_rows
        ],
    }


def build_chunk_manifest_payload(
    overview_payload: dict[str, Any],
    bundle_rows: list[dict[str, Any]],
    chunk_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "model_profile": overview_payload["weak_model_strategy"]["model_profile"],
        "recommended_single_prompt_limit": overview_payload["weak_model_strategy"]["recommended_single_prompt_limit"],
        "bundle_count": len(bundle_rows),
        "chunk_count": len(chunk_rows),
        "bundles": [
            {
                "bundle_id": item["bundle_id"],
                "entry_query_id": item["entry_query_id"],
                "recommended_sequence": item["recommended_sequence"],
            }
            for item in bundle_rows
        ],
        "chunks": chunk_rows,
    }


def render_implementation_card_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Java BFF Implementation Card: {payload['query_id']}",
        "",
        "## Identity",
        f"- File: `{payload['file']}`",
        f"- Query type: `{payload['query_type']}`",
        f"- Status: `{payload['status']}`",
        f"- Recommended method name: `{payload['java_bff_logic']['method_name']}`",
        "",
        "## SQL Logic",
        f"- Statement type: `{payload['sql_logic']['statement_type']}`",
        f"- Tables: `{', '.join(payload['sql_logic']['tables']) or 'n/a'}`",
        f"- Oracle features: `{', '.join(payload['sql_logic']['oracle_features']) or 'none'}`",
        f"- Parameters used: `{', '.join(payload['sql_logic']['parameters_used']) or 'none'}`",
        "",
        "## Java BFF Logic",
    ]
    for item in payload["java_bff_logic"]["repository_steps"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Manual Review Flags"])
    if payload["java_bff_logic"]["manual_review_flags"]:
        for item in payload["java_bff_logic"]["manual_review_flags"]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")
    lines.extend(["", "## Diagnostics"])
    if payload["diagnostics"]:
        for item in payload["diagnostics"]:
            lines.append(f"- `{item['code']}` {item['severity']}: {item['message']}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def render_sql_chunk_markdown(chunk: SqlChunk) -> str:
    lines = [
        f"# Java BFF SQL Chunk: {chunk.chunk_id}",
        "",
        f"- Query: `{chunk.query_id}`",
        f"- Sequence: {chunk.sequence}",
        f"- Estimated tokens: {chunk.estimated_tokens}",
        f"- Line range: {chunk.start_line}-{chunk.end_line}",
        f"- Clause hints: `{', '.join(chunk.clause_hints) or 'none'}`",
        "",
        "## Implementation Focus",
    ]
    for item in chunk.implementation_focus:
        lines.append(f"- {item}")
    lines.extend(["", "## SQL Excerpt", "```sql", chunk.sql_excerpt, "```"])
    return "\n".join(lines).rstrip() + "\n"


def render_bundle_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Java BFF Bundle: {payload['bundle_id']}",
        "",
        f"- Entry query: `{payload['entry_query_id']}`",
        f"- Query count: {payload['query_count']}",
        f"- Prompt profile: `{payload['prompt_profile']}`",
        f"- Chunk token limit: {payload['chunk_token_limit']}",
        "",
        "## Usage Rules",
    ]
    for item in payload["usage_rules"]:
        lines.append(f"- {item}")
    lines.extend(["", "## Recommended Sequence"])
    for item in payload["recommended_sequence"]:
        lines.append(f"- `{item}`")
    lines.extend(["", "## Queries"])
    for item in payload["queries"]:
        lines.append(
            f"- `{item['query_id']}` status={item['status']} chunks={item['chunk_count']} "
            f"statement={item['statement_type']}"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_overview_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Java Spring Boot BFF Artifact Overview",
        "",
        "## Runtime Target",
        f"- Framework: `{payload['target_runtime']['framework']}`",
        f"- Database: `{payload['target_runtime']['database']}`",
        f"- Recommended data access: `{payload['target_runtime']['recommended_data_access']}`",
        "",
        "## Weak Model Strategy",
        f"- Model profile: `{payload['weak_model_strategy']['model_profile']}`",
        f"- Hard token limit: {payload['weak_model_strategy']['hard_token_limit']}",
        f"- Recommended single prompt limit: {payload['weak_model_strategy']['recommended_single_prompt_limit']}",
        f"- SQL chunk token limit: {payload['weak_model_strategy']['sql_chunk_token_limit']}",
    ]
    for rule in payload["weak_model_strategy"]["rules"]:
        lines.append(f"- {rule}")
    lines.extend(["", "## Bundles"])
    for item in payload["bundles"]:
        lines.append(
            f"- `{item['bundle_id']}` queries={item['query_count']} prompts={item['prompt_count']}"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_chunk_manifest_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Java BFF Chunk Manifest",
        "",
        f"- Model profile: `{payload['model_profile']}`",
        f"- Recommended single prompt limit: {payload['recommended_single_prompt_limit']}",
        f"- Bundles: {payload['bundle_count']}",
        f"- Chunks: {payload['chunk_count']}",
        "",
        "## Bundles",
    ]
    for item in payload["bundles"]:
        lines.append(f"- `{item['bundle_id']}`")
        for sequence_path in item["recommended_sequence"][:8]:
            lines.append(f"  sequence: `{sequence_path}`")
    lines.extend(["", "## Chunks"])
    for item in payload["chunks"][:50]:
        lines.append(
            f"- `{item['chunk_id']}` query={item['query_id']} tokens={item['estimated_tokens']} "
            f"safe_for_qwen3={item['safe_for_qwen3']}"
        )
    return "\n".join(lines).rstrip() + "\n"


def compact_query_summary(card_payload: dict[str, Any]) -> str:
    sql_logic = card_payload["sql_logic"]
    return (
        f"statement={sql_logic['statement_type']}, tables={','.join(sql_logic['tables']) or 'n/a'}, "
        f"params={','.join(item['parameter_name'] for item in card_payload['parameters']) or 'none'}, "
        f"oracle={','.join(sql_logic['oracle_features']) or 'none'}"
    )


def group_diagnostics_by_query(diagnostics: list[DiagnosticModel]) -> dict[str, list[DiagnosticModel]]:
    grouped: dict[str, list[DiagnosticModel]] = defaultdict(list)
    for diagnostic in diagnostics:
        if diagnostic.query_id:
            grouped[diagnostic.query_id].append(diagnostic)
    return grouped


def clause_preview(sql: str, start_clause: str, end_clause: str | None) -> str:
    collapsed = re.sub(r"\s+", " ", sql).strip()
    start_pattern = re.compile(re.escape(start_clause), re.IGNORECASE)
    start_match = start_pattern.search(collapsed)
    if not start_match:
        return ""
    start_index = start_match.start()
    if end_clause:
        end_pattern = re.compile(re.escape(end_clause), re.IGNORECASE)
        end_match = end_pattern.search(collapsed, start_match.end())
        end_index = end_match.start() if end_match else len(collapsed)
    else:
        end_index = len(collapsed)
    return truncate_text(collapsed[start_index:end_index], 280)


def recommended_repository_method_name(resolved: ResolvedQueryModel) -> str:
    base = to_pascal_case(resolved.query.name)
    statement_type = str(resolved.sql_stats.get("statement_type", "select"))
    prefix = {
        "select": "fetch",
        "insert": "insert",
        "update": "update",
        "delete": "delete",
        "merge": "merge",
    }.get(statement_type, "run")
    return prefix + base


def to_java_identifier(name: str) -> str:
    clean = re.sub(r"^:+", "", name).strip()
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", clean) if part]
    if not parts:
        return "param"
    head = parts[0].lower()
    tail = "".join(part.capitalize() for part in parts[1:])
    return head + tail


def to_pascal_case(name: str) -> str:
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", name) if part]
    if not parts:
        return "Query"
    return "".join(part[:1].upper() + part[1:] for part in parts)


def phase_sort_key(item: dict[str, Any]) -> tuple[int, str]:
    phase_order = {
        "phase-1-plan": 1,
        "phase-2-repository-chunk": 2,
        "phase-2-repository-merge": 3,
        "phase-3-bff-assembly": 4,
        "phase-4-verify": 5,
    }
    return (phase_order.get(item["phase"], 99), item["txt_path"])


def truncate_text(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[:limit].rstrip() + "..."


def artifact_descriptor_for_path(path: Path, kind: str, title: str, scope: str) -> ArtifactDescriptor:
    content = path.read_text(encoding="utf-8")
    estimated_tokens = estimate_tokens(content)
    return ArtifactDescriptor(
        kind=kind,
        path=str(path),
        title=title,
        estimated_tokens=estimated_tokens,
        safe_for_128k_single_pass=estimated_tokens <= 100_000,
        needs_selective_prompting=estimated_tokens > 40_000,
        scope=scope,
    )


def append_artifacts_to_index(output_dir: Path, extra_artifacts: list[ArtifactDescriptor]) -> None:
    index_path = output_dir / "analysis" / "index.json"
    if not index_path.exists():
        return
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    payload.setdefault("artifacts", [])
    payload["artifacts"].extend(item.to_dict() for item in extra_artifacts)
    index_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def estimate_tokens(text: str) -> int:
    if not text:
        return 0
    return max(1, round(len(text) / 4))


def safe_name(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value)
