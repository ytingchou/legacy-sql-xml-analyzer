from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from xml.etree import ElementTree as ET

from .dashboard import write_executive_report
from .learning import AnalysisProfile, load_profile
from .models import (
    ALLOWED_DATA_TYPES,
    AnalysisResult,
    ArtifactDescriptor,
    DiagnosticModel,
    FileSummary,
    ParameterModel,
    QueryModel,
    ReferenceModel,
    ResolvedQueryModel,
)
from .profile import resolve_external_xml_path
from .prompting import write_failure_clusters


PARAMETER_PATTERN = re.compile(r":[A-Za-z_][A-Za-z0-9_]*")
WORD_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_$#]*")
TABLE_PATTERN = re.compile(r"\b(?:from|join|update|into)\s+([A-Za-z0-9_.$#]+)", re.IGNORECASE)
SQL_COMMENT_PATTERN = re.compile(r"(--[^\n]*|/\*.*?\*/)", re.DOTALL)
DATASET_PATTERN = re.compile(
    r"select\s+:(?P<param>[A-Za-z_][A-Za-z0-9_]*)\b(?P<body>.*?)\bfrom\s+dual\b",
    re.IGNORECASE | re.DOTALL,
)
DML_PATTERN = re.compile(r"^\s*(insert|update|delete|merge)\b", re.IGNORECASE)


@dataclass(slots=True)
class AnalyzeOptions:
    input_dir: Path
    output_dir: Path
    strict: bool = False
    entry_file: str | None = None
    entry_main_query: str | None = None
    profile: AnalysisProfile | None = None
    snapshot_label: str | None = None


class Analyzer:
    def __init__(self, options: AnalyzeOptions):
        self.options = options
        self.diagnostics: list[DiagnosticModel] = []
        self.queries: list[QueryModel] = []
        self.registry: dict[tuple[Path, str, str], QueryModel] = {}
        self.file_paths: set[Path] = set()
        self.rule_usage: Counter[str] = Counter()

    def analyze(self, write_artifacts: bool = True) -> AnalysisResult:
        xml_files = sorted(self.options.input_dir.rglob("*.xml"))
        if not xml_files:
            self._diagnostic(
                code="NO_XML_FOUND",
                severity="fatal",
                message=f"No XML files found under {self.options.input_dir}",
                source_path=self.options.input_dir,
                suggested_fix="Point --input to a directory that contains SQL mapping XML files.",
            )
            return AnalysisResult(files=[], queries=[], resolved_queries=[], diagnostics=self.diagnostics, artifacts=[])

        file_summaries: list[FileSummary] = []

        for xml_path in xml_files:
            self.file_paths.add(xml_path.resolve())
            before_count = len(self.diagnostics)
            parsed = self._parse_file(xml_path)
            self.queries.extend(parsed)
            file_summaries.append(
                FileSummary(
                    path=xml_path.resolve(),
                    parse_status="ok",
                    query_count=len(parsed),
                    diagnostic_count=0,
                )
            )

        self._build_registry()
        resolved_queries = [self._resolve_query(query) for query in self.queries]
        self._lint_queries(resolved_queries)
        for summary in file_summaries:
            file_diagnostics = [diag for diag in self.diagnostics if diag.source_path == summary.path]
            severities = {diag.severity for diag in file_diagnostics}
            if "fatal" in severities:
                summary.parse_status = "failed"
            elif "error" in severities or "warning" in severities:
                summary.parse_status = "partial"
            else:
                summary.parse_status = "ok"
            summary.diagnostic_count = len(file_diagnostics)
        artifacts: list[ArtifactDescriptor] = []
        if write_artifacts:
            artifacts = self._write_artifacts(file_summaries, resolved_queries)
        return AnalysisResult(
            files=file_summaries,
            queries=self.queries,
            resolved_queries=resolved_queries,
            diagnostics=self.diagnostics,
            artifacts=artifacts,
        )

    def _parse_file(self, xml_path: Path) -> list[QueryModel]:
        try:
            root = ET.parse(xml_path).getroot()
        except ET.ParseError as exc:
            self._diagnostic(
                code="XML_PARSE_ERROR",
                severity="fatal",
                message=f"Failed to parse XML: {exc}",
                source_path=xml_path.resolve(),
                suggested_fix="Fix malformed XML before re-running the analyzer.",
            )
            return []

        if root.tag != "sql-mapping":
            self._diagnostic(
                code="ROOT_TAG_INVALID",
                severity="error",
                message=f"Expected <sql-mapping> root, got <{root.tag}>.",
                source_path=xml_path.resolve(),
                suggested_fix="Wrap mapping files with a <sql-mapping> root element.",
            )

        queries: list[QueryModel] = []
        for child in root:
            if child.tag not in {"main-query", "sub-query"}:
                if self._is_ignored_tag(child.tag):
                    self._record_rule_usage("ignore_tag", child.tag)
                    continue
                self._diagnostic(
                    code="UNSUPPORTED_TAG",
                    severity="warning",
                    message=f"Unsupported tag <{child.tag}> under <sql-mapping>.",
                    source_path=xml_path.resolve(),
                    tag=child.tag,
                    suggested_fix="Keep only <main-query> and <sub-query> directly under <sql-mapping>.",
                )
                continue

            query_type = "main" if child.tag == "main-query" else "sub"
            query_name = (child.attrib.get("name") or "").strip()
            if not query_name:
                self._diagnostic(
                    code="QUERY_NAME_MISSING",
                    severity="error",
                    message=f"<{child.tag}> is missing the required name attribute.",
                    source_path=xml_path.resolve(),
                    tag=child.tag,
                    suggested_fix=f'Add name="..." to <{child.tag}>.',
                )
                continue

            sql_body_node = child.find("sql-body")
            raw_sql = (sql_body_node.text or "").strip() if sql_body_node is not None and sql_body_node.text else ""
            query = QueryModel(
                name=query_name,
                query_type=query_type,
                source_path=xml_path.resolve(),
                raw_sql=raw_sql,
            )
            for nested in child:
                if nested.tag == "parameter":
                    query.parameters.append(self._parse_parameter(nested, query))
                elif nested.tag in {"sql-refer-to", "ext-sql-refer-to", "sql-copy", "ext-sql-copy"}:
                    reference = self._parse_reference(nested, query)
                    if reference:
                        query.references.append(reference)
            if sql_body_node is None:
                self._diagnostic(
                    code="SQL_BODY_MISSING",
                    severity="error",
                    message=f"Query {query.id} is missing <sql-body>.",
                    source_path=xml_path.resolve(),
                    query_id=query.id,
                    tag=child.tag,
                    suggested_fix="Add a <sql-body><![CDATA[...]]></sql-body> element.",
                )
            queries.append(query)
        return queries

    def _parse_parameter(self, node: ET.Element, query: QueryModel) -> ParameterModel:
        parameter = ParameterModel(
            name=(node.attrib.get("name") or "").strip(),
            data_type=(node.attrib.get("data_type") or "").strip() or None,
            sample=(node.attrib.get("sample") or "").strip() or None,
            default=(node.attrib.get("default") or "").strip() or None,
            source_path=query.source_path,
            owner_query_id=query.id,
        )
        if not parameter.name:
            self._diagnostic(
                code="PARAMETER_NAME_MISSING",
                severity="error",
                message=f"Parameter in {query.id} is missing a name attribute.",
                source_path=query.source_path,
                query_id=query.id,
                tag="parameter",
                suggested_fix='Add name=":YourParameter" to <parameter>.',
            )
        return parameter

    def _parse_reference(self, node: ET.Element, query: QueryModel) -> ReferenceModel | None:
        tag = node.tag
        mode = "copy" if "copy" in tag else "refer"
        scope = "external" if tag.startswith("ext-") else "local"
        target_type = None
        target_name = None
        source_path = query.source_path
        name = (node.attrib.get("name") or "").strip()
        main_query = (node.attrib.get("main-query") or "").strip()
        sub_query = (node.attrib.get("sub-query") or "").strip()
        xml_name = (node.attrib.get("xml") or "").strip() or None

        if mode == "copy" and sub_query:
            self._diagnostic(
                code="COPY_SUBQUERY_UNSUPPORTED",
                severity="error",
                message=f"{tag} in {query.id} does not support sub-query targets.",
                source_path=source_path,
                query_id=query.id,
                tag=tag,
                context={"name": name, "sub-query": sub_query},
                suggested_fix=f"Use main-query=\"...\" on <{tag}>.",
            )

        if main_query and sub_query:
            self._diagnostic(
                code="REF_BOTH_TARGETS",
                severity="error",
                message=f"{tag} in {query.id} specifies both main-query and sub-query.",
                source_path=source_path,
                query_id=query.id,
                tag=tag,
                context={"main-query": main_query, "sub-query": sub_query},
                suggested_fix="Specify exactly one target type.",
            )

        if main_query:
            target_type = "main"
            target_name = main_query
        elif sub_query:
            target_type = "sub"
            target_name = sub_query
        elif tag == "sql-refer-to":
            target_name = name or None
            target_type = None
        else:
            self._diagnostic(
                code="TARGET_MISSING",
                severity="error",
                message=f"{tag} in {query.id} does not specify a target.",
                source_path=source_path,
                query_id=query.id,
                tag=tag,
                suggested_fix="Provide main-query=\"...\" or sub-query=\"...\" as required by the rule.",
            )

        if not name:
            self._diagnostic(
                code="REFERENCE_NAME_MISSING",
                severity="error",
                message=f"{tag} in {query.id} is missing the name attribute used for replacement.",
                source_path=source_path,
                query_id=query.id,
                tag=tag,
                suggested_fix='Add name="TOKEN_TO_REPLACE" to the reference node.',
            )

        if scope == "external" and not xml_name:
            self._diagnostic(
                code="EXT_XML_MISSING",
                severity="error",
                message=f"{tag} in {query.id} is missing the xml attribute.",
                source_path=source_path,
                query_id=query.id,
                tag=tag,
                suggested_fix='Add xml="other-file.xml" to external references.',
            )

        return ReferenceModel(
            name=name,
            tag=tag,
            mode=mode,
            scope=scope,
            target_type=target_type,
            target_name=target_name,
            source_path=source_path,
            owner_query_id=query.id,
            xml_name=xml_name,
        )

    def _build_registry(self) -> None:
        for query in self.queries:
            key = (query.source_path.resolve(), query.query_type, query.name)
            if key in self.registry:
                self._diagnostic(
                    code="DUPLICATE_QUERY",
                    severity="error",
                    message=f"Duplicate definition for {query.id}.",
                    source_path=query.source_path,
                    query_id=query.id,
                    tag=f"{query.query_type}-query",
                    suggested_fix="Keep query names unique within the same file and type.",
                )
                continue
            self.registry[key] = query

    def _resolve_query(self, query: QueryModel) -> ResolvedQueryModel:
        stack: list[str] = []
        dependencies: list[str] = []
        resolved_sql, status = self._resolve_sql(query, stack, dependencies)
        sql_skeleton = self._sql_skeleton(resolved_sql or query.raw_sql)
        sql_stats = self._sql_stats(resolved_sql or query.raw_sql)
        return ResolvedQueryModel(
            query=query,
            resolved_sql=resolved_sql,
            sql_skeleton=sql_skeleton,
            status=status,
            dependencies=dependencies,
            sql_stats=sql_stats,
        )

    def _resolve_sql(
        self,
        query: QueryModel,
        stack: list[str],
        dependencies: list[str],
    ) -> tuple[str | None, str]:
        if query.id in stack:
            self._diagnostic(
                code="CYCLE_DETECTED",
                severity="fatal",
                message=f"Cycle detected while resolving {query.id}: {' -> '.join(stack + [query.id])}",
                source_path=query.source_path,
                query_id=query.id,
                suggested_fix="Break the circular reference between main-query/sub-query nodes.",
            )
            return None, "failed"

        stack.append(query.id)
        current_sql = query.raw_sql
        status = "resolved"
        for reference in query.references:
            replacement, replacement_status = self._resolve_reference(reference, stack, dependencies, query)
            if replacement is None:
                status = "failed" if replacement_status == "failed" else "partial"
                continue
            if replacement_status == "failed":
                status = "failed"
            elif replacement_status == "partial" and status == "resolved":
                status = "partial"
            replacement_token = self._find_replacement_token(reference, current_sql)
            if replacement_token:
                current_sql = current_sql.replace(replacement_token, replacement)
            else:
                self._diagnostic(
                    code="REFERENCE_TOKEN_NOT_FOUND",
                    severity="warning",
                    message=f"Reference token '{reference.name}' was not found in SQL body for {query.id}.",
                    source_path=query.source_path,
                    query_id=query.id,
                    tag=reference.tag,
                    context={"reference_name": reference.name, "target_name": reference.target_name},
                    suggested_fix="Ensure the raw SQL contains the literal token named in the reference tag.",
                )
                status = "partial" if status == "resolved" else status
        stack.pop()
        return current_sql, status

    def _resolve_reference(
        self,
        reference: ReferenceModel,
        stack: list[str],
        dependencies: list[str],
        owner_query: QueryModel,
    ) -> tuple[str | None, str]:
        target_candidates = self._reference_candidates(reference, owner_query.source_path)
        if not target_candidates:
            return None, "failed"

        target_query = None
        for candidate in target_candidates:
            if candidate in self.registry:
                target_query = self.registry[candidate]
                break

        if target_query is None:
            self._diagnostic(
                code="REFERENCE_TARGET_MISSING",
                severity="error",
                message=(
                    f"Could not resolve {reference.tag} target "
                    f"{reference.target_name or reference.name} for {owner_query.id}."
                ),
                source_path=owner_query.source_path,
                query_id=owner_query.id,
                tag=reference.tag,
                context={
                    "candidates": [[str(item[0]), item[1], item[2]] for item in target_candidates],
                },
                suggested_fix="Confirm the target query exists in the same XML or the referenced external XML.",
            )
            return None, "failed"

        dependencies.append(target_query.id)

        if reference.mode == "copy":
            return target_query.raw_sql, "resolved"
        if reference.target_type == "sub":
            return target_query.raw_sql, "resolved"

        resolved_sql, status = self._resolve_sql(target_query, stack, dependencies)
        return resolved_sql, status

    def _reference_candidates(
        self,
        reference: ReferenceModel,
        source_path: Path,
    ) -> list[tuple[Path, str, str]]:
        resolved_path, mapped_name, mapping_rule_type = resolve_external_xml_path(
            reference.xml_name,
            source_path,
            self.options.input_dir,
            self.options.profile,
        )
        if mapped_name and mapping_rule_type:
            self._record_rule_usage(mapping_rule_type, f"{reference.xml_name}->{mapped_name}")
        target_name = reference.target_name or reference.name
        if not target_name:
            return []
        if reference.target_type:
            return [(resolved_path, reference.target_type, target_name)]
        target_order = ["sub", "main"]
        if self.options.profile:
            target_order = self.options.profile.reference_target_default_order
            self._record_rule_usage("reference_target_default_order", " -> ".join(target_order))
        return [(resolved_path, target_type, target_name) for target_type in target_order]

    def _find_replacement_token(self, reference: ReferenceModel, current_sql: str) -> str | None:
        for rendered_token, pattern in self._render_reference_tokens(reference.name):
            if rendered_token in current_sql:
                if pattern != "{name}":
                    self._record_rule_usage("reference_token_pattern", pattern)
                return rendered_token
        return None

    def _render_reference_tokens(self, reference_name: str) -> list[tuple[str, str]]:
        if not reference_name:
            return []
        patterns = ["{name}"]
        if self.options.profile:
            patterns = self.options.profile.reference_token_patterns + patterns
        rendered: list[tuple[str, str]] = []
        seen_tokens: set[str] = set()
        for pattern in patterns:
            if "{name}" not in pattern:
                continue
            token = pattern.replace("{name}", reference_name)
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            rendered.append((token, pattern))
        rendered.sort(key=lambda item: len(item[0]), reverse=True)
        return rendered

    def _is_ignored_tag(self, tag: str) -> bool:
        return bool(self.options.profile and tag in self.options.profile.ignore_tags)

    def _record_rule_usage(self, rule_type: str, detail: str) -> None:
        self.rule_usage[f"{rule_type}:{detail}"] += 1

    def _lint_queries(self, resolved_queries: Iterable[ResolvedQueryModel]) -> None:
        for resolved in resolved_queries:
            query = resolved.query
            sql = resolved.resolved_sql or query.raw_sql
            parameter_names = {parameter.name for parameter in query.parameters if parameter.name}
            referenced_parameters = set(PARAMETER_PATTERN.findall(sql))

            for parameter in query.parameters:
                if parameter.name and not parameter.name.startswith(":"):
                    self._diagnostic(
                        code="PARAMETER_PREFIX_INVALID",
                        severity="error",
                        message=f"Parameter {parameter.name} in {query.id} does not start with ':'.",
                        source_path=query.source_path,
                        query_id=query.id,
                        tag="parameter",
                        suggested_fix="Parameter names must start with ':', for example ':fPriceCheckRule'.",
                    )
                if parameter.data_type and parameter.data_type not in ALLOWED_DATA_TYPES:
                    self._diagnostic(
                        code="PARAMETER_DATATYPE_INVALID",
                        severity="error",
                        message=(
                            f"Parameter {parameter.name or '<unnamed>'} in {query.id} uses unsupported "
                            f"data_type {parameter.data_type}."
                        ),
                        source_path=query.source_path,
                        query_id=query.id,
                        tag="parameter",
                        suggested_fix="Use one of Int, Double, String, DateTime, IntArray, StringArray, SQL.",
                    )

            for placeholder in referenced_parameters:
                if placeholder not in parameter_names:
                    self._diagnostic(
                        code="SQL_PARAMETER_UNDEFINED",
                        severity="warning",
                        message=f"SQL in {query.id} uses {placeholder} but no matching <parameter> is defined.",
                        source_path=query.source_path,
                        query_id=query.id,
                        suggested_fix="Add a matching <parameter> node or remove the placeholder.",
                    )

            for comment in SQL_COMMENT_PATTERN.findall(sql):
                if ":" in comment or "'" in comment:
                    self._diagnostic(
                        code="COMMENT_FORBIDDEN_CHAR",
                        severity="warning",
                        message=f"SQL comments in {query.id} contain ':' or single quote.",
                        source_path=query.source_path,
                        query_id=query.id,
                        suggested_fix="Remove ':' and single quotes from SQL comments to follow the legacy rule.",
                    )
                    break

            if DML_PATTERN.search(sql) and not sql.rstrip().endswith(";"):
                self._diagnostic(
                    code="DML_SEMICOLON_MISSING",
                    severity="warning",
                    message=f"DML SQL in {query.id} does not end with ';'.",
                    source_path=query.source_path,
                    query_id=query.id,
                    suggested_fix="Append ';' to the end of INSERT/UPDATE/DELETE/MERGE statements.",
                )

            for match in DATASET_PATTERN.finditer(sql):
                param_name = f":{match.group('param')}"
                body = match.group('body')
                if "cast(" not in body.lower():
                    self._diagnostic(
                        code="DATASET_CAST_MISSING",
                        severity="warning",
                        message=(
                            f"Query {query.id} selects {param_name} from dual without CAST, "
                            "which may break mORMotServer typing."
                        ),
                        source_path=query.source_path,
                        query_id=query.id,
                        suggested_fix=(
                            f"Wrap {param_name} with CAST(... AS VARCHAR2(n)) or CAST(... AS NUMBER(...))."
                        ),
                    )

    def _write_artifacts(
        self,
        file_summaries: list[FileSummary],
        resolved_queries: list[ResolvedQueryModel],
    ) -> list[ArtifactDescriptor]:
        output_root = self.options.output_dir / "analysis"
        markdown_root = output_root / "markdown"
        queries_root = markdown_root / "queries"
        diagnostics_root = markdown_root / "diagnostics"
        for directory in (output_root, markdown_root, queries_root, diagnostics_root):
            directory.mkdir(parents=True, exist_ok=True)

        overview_path = markdown_root / "overview.md"
        overview_content = self._render_overview(file_summaries, resolved_queries)
        overview_path.write_text(overview_content, encoding="utf-8")

        artifacts: list[ArtifactDescriptor] = [
            self._artifact_descriptor(overview_path, "markdown", "Project overview", "project"),
        ]

        for resolved in resolved_queries:
            query_path = queries_root / f"{self._safe_name(resolved.query.id)}.md"
            content = self._render_query_markdown(resolved)
            query_path.write_text(content, encoding="utf-8")
            artifacts.append(
                self._artifact_descriptor(
                    query_path,
                    "markdown",
                    f"Query card: {resolved.query.id}",
                    "query",
                )
            )

        for index, diagnostic in enumerate(self.diagnostics, start=1):
            diagnostic_path = diagnostics_root / f"{index:04d}-{diagnostic.code.lower()}.md"
            content = self._render_diagnostic_markdown(diagnostic)
            diagnostic_path.write_text(content, encoding="utf-8")
            artifacts.append(
                self._artifact_descriptor(
                    diagnostic_path,
                    "markdown",
                    f"Diagnostic: {diagnostic.code}",
                    "diagnostic",
                )
            )

        index_path = output_root / "index.json"
        artifacts.insert(0, ArtifactDescriptor(
            kind="json",
            path=str(index_path),
            title="Machine-readable index",
            estimated_tokens=0,
            safe_for_128k_single_pass=True,
            needs_selective_prompting=False,
            scope="project",
        ))
        index_payload = {
            "files": [item.to_dict() for item in file_summaries],
            "queries": [item.to_dict() for item in self.queries],
            "resolved_queries": [item.to_dict() for item in resolved_queries],
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "artifacts": [item.to_dict() for item in artifacts],
            "entrypoint": {
                "entry_file": self.options.entry_file,
                "entry_main_query": self.options.entry_main_query,
            },
        }
        index_path.write_text(json.dumps(index_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts[0] = self._artifact_descriptor(index_path, "json", "Machine-readable index", "project")
        index_payload["artifacts"] = [item.to_dict() for item in artifacts]
        index_path.write_text(json.dumps(index_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        return artifacts

    def _render_overview(
        self,
        file_summaries: list[FileSummary],
        resolved_queries: list[ResolvedQueryModel],
    ) -> str:
        diagnostics_by_severity = Counter(diag.severity for diag in self.diagnostics)
        lines = [
            "# Legacy SQL XML Analyzer Overview",
            "",
            "## Summary",
            f"- Files scanned: {len(file_summaries)}",
            f"- Queries discovered: {len(self.queries)}",
            f"- Resolved queries: {sum(1 for item in resolved_queries if item.status == 'resolved')}",
            f"- Partial queries: {sum(1 for item in resolved_queries if item.status == 'partial')}",
            f"- Failed queries: {sum(1 for item in resolved_queries if item.status == 'failed')}",
            f"- Diagnostics: fatal={diagnostics_by_severity.get('fatal', 0)}, "
            f"error={diagnostics_by_severity.get('error', 0)}, "
            f"warning={diagnostics_by_severity.get('warning', 0)}, "
            f"info={diagnostics_by_severity.get('info', 0)}",
            "",
            "## How To Feed An LLM",
            "- Start with this overview and the query card for the target main-query.",
            "- Add matching diagnostic cards when the query status is partial or failed.",
            "- Prefer query cards with estimated tokens below 40k for a single prompt.",
            "",
            "## Files",
        ]
        for summary in file_summaries:
            lines.append(
                f"- `{summary.path.name}`: status={summary.parse_status}, "
                f"queries={summary.query_count}, diagnostics={summary.diagnostic_count}"
            )
        hot_queries = sorted(resolved_queries, key=lambda item: len(item.dependencies), reverse=True)[:10]
        lines.extend(["", "## Dependency Hotspots"])
        for item in hot_queries:
            lines.append(
                f"- `{item.query.id}`: deps={len(item.dependencies)}, "
                f"status={item.status}, tokens≈{self._estimate_tokens(item.resolved_sql or item.query.raw_sql)}"
            )
        return "\n".join(lines).rstrip() + "\n"

    def _render_query_markdown(self, resolved: ResolvedQueryModel) -> str:
        query = resolved.query
        lines = [
            f"# Query Card: {query.id}",
            "",
            "## Identity",
            f"- File: `{query.source_path}`",
            f"- Type: `{query.query_type}`",
            f"- Name: `{query.name}`",
            f"- Status: `{resolved.status}`",
            "",
            "## Parameters",
        ]
        if query.parameters:
            for parameter in query.parameters:
                lines.append(
                    f"- `{parameter.name}`: data_type={parameter.data_type or 'n/a'}, "
                    f"sample={parameter.sample or 'n/a'}, default={parameter.default or 'n/a'}"
                )
        else:
            lines.append("- None")

        lines.extend(["", "## References"])
        if query.references:
            for reference in query.references:
                lines.append(
                    f"- `{reference.tag}` name=`{reference.name}` target="
                    f"`{reference.target_type or 'unknown'}:{reference.target_name or 'n/a'}` "
                    f"scope={reference.scope}"
                )
        else:
            lines.append("- None")

        lines.extend(
            [
                "",
                "## SQL Stats",
                f"- Statement type: `{resolved.sql_stats.get('statement_type', 'unknown')}`",
                f"- Tables: `{', '.join(resolved.sql_stats.get('tables', [])) or 'n/a'}`",
                f"- Parameters used: `{', '.join(resolved.sql_stats.get('parameters', [])) or 'n/a'}`",
                f"- Estimated tokens: `{self._estimate_tokens(resolved.resolved_sql or query.raw_sql)}`",
                "",
                "## SQL Skeleton",
                "```sql",
                resolved.sql_skeleton or "",
                "```",
                "",
                "## Resolved SQL Preview",
                "```sql",
                self._truncate_sql(resolved.resolved_sql or query.raw_sql),
                "```",
                "",
                "## Dependency Chain",
            ]
        )
        if resolved.dependencies:
            for dependency in resolved.dependencies:
                lines.append(f"- `{dependency}`")
        else:
            lines.append("- None")
        return "\n".join(lines).rstrip() + "\n"

    def _render_diagnostic_markdown(self, diagnostic: DiagnosticModel) -> str:
        lines = [
            f"# Diagnostic: {diagnostic.code}",
            "",
            "## Summary",
            f"- Severity: `{diagnostic.severity}`",
            f"- Source: `{diagnostic.source_path}`",
            f"- Query: `{diagnostic.query_id or 'n/a'}`",
            f"- Tag: `{diagnostic.tag or 'n/a'}`",
            "",
            "## Message",
            diagnostic.message,
            "",
            "## Context",
            "```json",
            json.dumps(diagnostic.context, indent=2, ensure_ascii=False),
            "```",
            "",
            "## Suggested Fix",
            diagnostic.suggested_fix or "No suggested fix available.",
            "",
            "## Prompt Hint",
            diagnostic.prompt_hint or self._build_prompt_hint(diagnostic),
        ]
        return "\n".join(lines).rstrip() + "\n"

    def _artifact_descriptor(self, path: Path, kind: str, title: str, scope: str) -> ArtifactDescriptor:
        content = path.read_text(encoding="utf-8")
        estimated_tokens = self._estimate_tokens(content)
        return ArtifactDescriptor(
            kind=kind,
            path=str(path),
            title=title,
            estimated_tokens=estimated_tokens,
            safe_for_128k_single_pass=estimated_tokens <= 100_000,
            needs_selective_prompting=estimated_tokens > 40_000,
            scope=scope,
        )

    def _sql_stats(self, sql: str) -> dict[str, object]:
        statement_type = "unknown"
        match = re.search(r"\b(select|insert|update|delete|merge)\b", sql, re.IGNORECASE)
        if match:
            statement_type = match.group(1).lower()
        tables = sorted(set(TABLE_PATTERN.findall(sql)))
        parameters = sorted(set(PARAMETER_PATTERN.findall(sql)))
        return {
            "statement_type": statement_type,
            "tables": tables,
            "parameters": parameters,
            "line_count": len(sql.splitlines()),
            "character_count": len(sql),
        }

    def _sql_skeleton(self, sql: str) -> str:
        collapsed = re.sub(r"\s+", " ", sql).strip()
        collapsed = re.sub(r"'[^']*'", "'...'", collapsed)
        return self._truncate_sql(collapsed, limit=1200)

    def _truncate_sql(self, sql: str, limit: int = 4000) -> str:
        sql = sql.strip()
        if len(sql) <= limit:
            return sql
        return f"{sql[:limit]}\n-- [truncated {len(sql) - limit} chars]"

    def _estimate_tokens(self, text: str) -> int:
        if not text:
            return 0
        return max(1, round(len(text) / 4))

    def _safe_name(self, value: str) -> str:
        return re.sub(r"[^A-Za-z0-9._-]+", "_", value)

    def _build_prompt_hint(self, diagnostic: DiagnosticModel) -> str:
        artifact_hint = "analysis/index.json and the matching query card under analysis/markdown/queries/"
        return (
            "請協助診斷這個 legacy SQL XML 規則錯誤。\n\n"
            f"問題摘要: {diagnostic.message}\n"
            f"錯誤代碼: {diagnostic.code}\n"
            f"來源檔案: {diagnostic.source_path}\n"
            f"查詢: {diagnostic.query_id or 'n/a'}\n\n"
            "我需要你做的事:\n"
            "1. 根據現有規則判斷根因。\n"
            "2. 提出最小修正方式。\n"
            "3. 不要臆測未提供的 XML 結構。\n\n"
            f"建議一併提供的 artifact: {artifact_hint}"
        )

    def _diagnostic(
        self,
        code: str,
        severity: str,
        message: str,
        source_path: Path,
        query_id: str | None = None,
        tag: str | None = None,
        context: dict[str, object] | None = None,
        suggested_fix: str | None = None,
    ) -> None:
        diagnostic = DiagnosticModel(
            code=code,
            severity=severity,
            message=message,
            source_path=source_path,
            query_id=query_id,
            tag=tag,
            context=context or {},
            suggested_fix=suggested_fix,
        )
        diagnostic.prompt_hint = self._build_prompt_hint(diagnostic)
        self.diagnostics.append(diagnostic)


def analyze_directory(
    input_dir: Path,
    output_dir: Path,
    strict: bool = False,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
    profile_path: Path | None = None,
    snapshot_label: str | None = None,
) -> AnalysisResult:
    profile = load_profile(profile_path)
    baseline_result: AnalysisResult | None = None
    if profile is not None:
        baseline_analyzer = Analyzer(
            AnalyzeOptions(
                input_dir=input_dir,
                output_dir=output_dir,
                strict=strict,
                entry_file=entry_file,
                entry_main_query=entry_main_query,
                profile=None,
                snapshot_label=snapshot_label,
            )
        )
        baseline_result = baseline_analyzer.analyze(write_artifacts=False)

    analyzer = Analyzer(
        AnalyzeOptions(
            input_dir=input_dir,
            output_dir=output_dir,
            strict=strict,
            entry_file=entry_file,
            entry_main_query=entry_main_query,
            profile=profile,
            snapshot_label=snapshot_label,
        )
    )
    result = analyzer.analyze(write_artifacts=True)
    extra_artifacts: list[ArtifactDescriptor] = []
    if profile is not None and baseline_result is not None:
        extra_artifacts.extend(write_profile_analysis_artifacts(
            output_dir=output_dir,
            profile=profile,
            profile_path=profile_path,
            baseline=baseline_result,
            profiled=result,
            rule_usage=analyzer.rule_usage,
        ))
        result.artifacts.extend(extra_artifacts)
        append_artifacts_to_index(output_dir, extra_artifacts)
    history_artifacts = write_run_history_artifacts(
        output_dir=output_dir,
        summary=summarize_analysis_result(result),
        profile_path=profile_path,
        snapshot_label=snapshot_label,
    )
    result.artifacts.extend(history_artifacts)
    append_artifacts_to_index(output_dir, history_artifacts)
    executive_artifacts = write_executive_report(
        output_dir=output_dir,
        result=result,
        profile_path=profile_path,
    )
    result.artifacts.extend(executive_artifacts)
    append_artifacts_to_index(output_dir, executive_artifacts)
    prompting_artifacts = write_failure_clusters(
        output_dir=output_dir,
        result=result,
    )
    result.artifacts.extend(prompting_artifacts)
    append_artifacts_to_index(output_dir, prompting_artifacts)
    return result


def write_profile_analysis_artifacts(
    output_dir: Path,
    profile: AnalysisProfile,
    profile_path: Path | None,
    baseline: AnalysisResult,
    profiled: AnalysisResult,
    rule_usage: Counter[str],
) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    applied_rules_path = analysis_root / "applied_rules.json"
    fix_delta_path = analysis_root / "fix_delta.json"
    fix_delta_markdown_path = analysis_root / "fix_delta.md"

    applied_payload = {
        "profile_source": str(profile_path) if profile_path else None,
        "profile_version": profile.profile_version,
        "source_observation_digest": profile.source_observation_digest,
        "active_defaults": {
            "reference_target_default_order": profile.reference_target_default_order,
            "reference_token_patterns": profile.reference_token_patterns,
            "external_xml_name_map": profile.external_xml_name_map,
            "external_xml_scoped_map": profile.external_xml_scoped_map,
            "ignore_tags": profile.ignore_tags,
        },
        "rules": [rule.to_dict() for rule in profile.rules],
        "usage": dict(sorted(rule_usage.items())),
    }
    applied_rules_path.write_text(json.dumps(applied_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    baseline_summary = summarize_analysis_result(baseline)
    profiled_summary = summarize_analysis_result(profiled)
    delta_payload = {
        "baseline": baseline_summary,
        "profiled": profiled_summary,
        "delta": build_summary_delta(baseline_summary, profiled_summary),
    }
    fix_delta_path.write_text(json.dumps(delta_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    fix_delta_markdown_path.write_text(render_fix_delta_markdown(delta_payload), encoding="utf-8")

    return [
        artifact_descriptor_for_path(applied_rules_path, "json", "Applied profile rules", "profile"),
        artifact_descriptor_for_path(fix_delta_path, "json", "Profile delta report", "profile"),
        artifact_descriptor_for_path(fix_delta_markdown_path, "markdown", "Profile delta summary", "profile"),
    ]


def summarize_analysis_result(result: AnalysisResult) -> dict[str, Any]:
    severity_counts = Counter(item.severity for item in result.diagnostics)
    code_counts = Counter(item.code for item in result.diagnostics)
    status_counts = Counter(item.status for item in result.resolved_queries)
    return {
        "files": len(result.files),
        "queries": len(result.queries),
        "resolved_queries": status_counts.get("resolved", 0),
        "partial_queries": status_counts.get("partial", 0),
        "failed_queries": status_counts.get("failed", 0),
        "diagnostics_by_severity": dict(sorted(severity_counts.items())),
        "diagnostics_by_code": dict(sorted(code_counts.items())),
    }


def build_summary_delta(baseline: dict[str, Any], profiled: dict[str, Any]) -> dict[str, Any]:
    diagnostics_by_code = {}
    baseline_codes = baseline.get("diagnostics_by_code", {})
    profiled_codes = profiled.get("diagnostics_by_code", {})
    for code in sorted(set(baseline_codes) | set(profiled_codes)):
        diagnostics_by_code[code] = profiled_codes.get(code, 0) - baseline_codes.get(code, 0)
    return {
        "resolved_queries_delta": profiled.get("resolved_queries", 0) - baseline.get("resolved_queries", 0),
        "partial_queries_delta": profiled.get("partial_queries", 0) - baseline.get("partial_queries", 0),
        "failed_queries_delta": profiled.get("failed_queries", 0) - baseline.get("failed_queries", 0),
        "diagnostics_by_code_delta": diagnostics_by_code,
        "error_delta": profiled.get("diagnostics_by_severity", {}).get("error", 0)
        - baseline.get("diagnostics_by_severity", {}).get("error", 0),
        "fatal_delta": profiled.get("diagnostics_by_severity", {}).get("fatal", 0)
        - baseline.get("diagnostics_by_severity", {}).get("fatal", 0),
        "warning_delta": profiled.get("diagnostics_by_severity", {}).get("warning", 0)
        - baseline.get("diagnostics_by_severity", {}).get("warning", 0),
    }


def render_fix_delta_markdown(delta_payload: dict[str, Any]) -> str:
    baseline = delta_payload["baseline"]
    profiled = delta_payload["profiled"]
    delta = delta_payload["delta"]
    lines = [
        "# Profile Delta",
        "",
        "## Summary",
        f"- Resolved queries: {baseline['resolved_queries']} -> {profiled['resolved_queries']} "
        f"(delta {delta['resolved_queries_delta']:+d})",
        f"- Partial queries: {baseline['partial_queries']} -> {profiled['partial_queries']} "
        f"(delta {delta['partial_queries_delta']:+d})",
        f"- Failed queries: {baseline['failed_queries']} -> {profiled['failed_queries']} "
        f"(delta {delta['failed_queries_delta']:+d})",
        f"- Errors: {baseline['diagnostics_by_severity'].get('error', 0)} -> "
        f"{profiled['diagnostics_by_severity'].get('error', 0)} (delta {delta['error_delta']:+d})",
        f"- Fatals: {baseline['diagnostics_by_severity'].get('fatal', 0)} -> "
        f"{profiled['diagnostics_by_severity'].get('fatal', 0)} (delta {delta['fatal_delta']:+d})",
        f"- Warnings: {baseline['diagnostics_by_severity'].get('warning', 0)} -> "
        f"{profiled['diagnostics_by_severity'].get('warning', 0)} (delta {delta['warning_delta']:+d})",
        "",
        "## Diagnostic Code Delta",
    ]
    code_delta = delta["diagnostics_by_code_delta"]
    if code_delta:
        for code, change in code_delta.items():
            if change != 0:
                lines.append(f"- `{code}`: {change:+d}")
    else:
        lines.append("- None")
    if lines[-1] == "## Diagnostic Code Delta":
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


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


def append_artifacts_to_index(output_dir: Path, extra_artifacts: list[ArtifactDescriptor]) -> None:
    index_path = output_dir / "analysis" / "index.json"
    if not index_path.exists():
        return
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    payload.setdefault("artifacts", [])
    payload["artifacts"].extend(item.to_dict() for item in extra_artifacts)
    index_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def write_run_history_artifacts(
    output_dir: Path,
    summary: dict[str, Any],
    profile_path: Path | None,
    snapshot_label: str | None,
) -> list[ArtifactDescriptor]:
    analysis_root = output_dir / "analysis"
    history_root = analysis_root / "history"
    history_root.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc)
    timestamp_text = timestamp.strftime("%Y%m%dT%H%M%S%fZ")
    label = sanitize_snapshot_label(snapshot_label)
    snapshot_name = f"{timestamp_text}-{label}.json" if label else f"{timestamp_text}.json"
    snapshot_path = history_root / snapshot_name
    latest_path = history_root / "latest.json"
    index_path = history_root / "index.json"
    run_snapshot_path = analysis_root / "run_snapshot.json"

    snapshot_payload = {
        "generated_at": timestamp.replace(microsecond=0).isoformat(),
        "snapshot_id": snapshot_name.removesuffix(".json"),
        "label": snapshot_label,
        "profile_path": str(profile_path) if profile_path else None,
        "summary": summary,
    }
    snapshot_path.write_text(json.dumps(snapshot_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    latest_path.write_text(json.dumps(snapshot_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    run_snapshot_path.write_text(json.dumps(snapshot_payload, indent=2, ensure_ascii=False), encoding="utf-8")

    history_index = {"snapshots": []}
    if index_path.exists():
        history_index = json.loads(index_path.read_text(encoding="utf-8"))
        history_index.setdefault("snapshots", [])
    history_index["snapshots"].append(
        {
            "snapshot_id": snapshot_payload["snapshot_id"],
            "generated_at": snapshot_payload["generated_at"],
            "label": snapshot_label,
            "profile_path": snapshot_payload["profile_path"],
            "path": str(snapshot_path),
            "summary": summary,
        }
    )
    index_path.write_text(json.dumps(history_index, indent=2, ensure_ascii=False), encoding="utf-8")

    return [
        artifact_descriptor_for_path(run_snapshot_path, "json", "Current run snapshot", "history"),
        artifact_descriptor_for_path(latest_path, "json", "Latest run snapshot", "history"),
        artifact_descriptor_for_path(index_path, "json", "Run history index", "history"),
    ]


def sanitize_snapshot_label(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-._")
    return cleaned or None
