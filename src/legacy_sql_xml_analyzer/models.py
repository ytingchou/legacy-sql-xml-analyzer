from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


ALLOWED_DATA_TYPES = {
    "Int",
    "Double",
    "String",
    "DateTime",
    "IntArray",
    "StringArray",
    "SQL",
}


Severity = Literal["info", "warning", "error", "fatal"]
QueryType = Literal["main", "sub"]
ReferenceMode = Literal["refer", "copy"]
ReferenceScope = Literal["local", "external"]
TargetType = Literal["main", "sub"]


@dataclass(slots=True)
class ParameterModel:
    name: str
    data_type: str | None
    sample: str | None
    default: str | None
    source_path: Path
    owner_query_id: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "data_type": self.data_type,
            "sample": self.sample,
            "default": self.default,
        }


@dataclass(slots=True)
class ReferenceModel:
    name: str
    tag: str
    mode: ReferenceMode
    scope: ReferenceScope
    target_type: TargetType | None
    target_name: str | None
    source_path: Path
    owner_query_id: str
    xml_name: str | None = None

    def target_key(self, source_path: Path) -> tuple[Path, TargetType, str] | None:
        if self.target_type is None or not self.target_name:
            return None
        resolved_path = source_path.parent / self.xml_name if self.xml_name else source_path
        return (resolved_path.resolve(), self.target_type, self.target_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "tag": self.tag,
            "mode": self.mode,
            "scope": self.scope,
            "target_type": self.target_type,
            "target_name": self.target_name,
            "xml_name": self.xml_name,
        }


@dataclass(slots=True)
class QueryModel:
    name: str
    query_type: QueryType
    source_path: Path
    raw_sql: str
    parameters: list[ParameterModel] = field(default_factory=list)
    references: list[ReferenceModel] = field(default_factory=list)

    @property
    def id(self) -> str:
        return f"{self.source_path.name}:{self.query_type}:{self.name}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "query_type": self.query_type,
            "source_path": str(self.source_path),
            "raw_sql_length": len(self.raw_sql),
            "parameters": [parameter.to_dict() for parameter in self.parameters],
            "references": [reference.to_dict() for reference in self.references],
        }


@dataclass(slots=True)
class DiagnosticModel:
    code: str
    severity: Severity
    message: str
    source_path: Path
    query_id: str | None = None
    tag: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    suggested_fix: str | None = None
    prompt_hint: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "source_path": str(self.source_path),
            "query_id": self.query_id,
            "tag": self.tag,
            "context": self.context,
            "suggested_fix": self.suggested_fix,
            "prompt_hint": self.prompt_hint,
        }


@dataclass(slots=True)
class ResolvedQueryModel:
    query: QueryModel
    resolved_sql: str | None
    sql_skeleton: str | None
    status: Literal["unresolved", "partial", "resolved", "failed"]
    dependencies: list[str]
    sql_stats: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.query.id,
            "status": self.status,
            "dependencies": self.dependencies,
            "resolved_sql_length": len(self.resolved_sql or ""),
            "sql_stats": self.sql_stats,
            "sql_skeleton": self.sql_skeleton,
        }


@dataclass(slots=True)
class FileSummary:
    path: Path
    parse_status: Literal["ok", "partial", "failed"]
    query_count: int
    diagnostic_count: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": str(self.path),
            "parse_status": self.parse_status,
            "query_count": self.query_count,
            "diagnostic_count": self.diagnostic_count,
        }


@dataclass(slots=True)
class ArtifactDescriptor:
    kind: str
    path: str
    title: str
    estimated_tokens: int
    safe_for_128k_single_pass: bool
    needs_selective_prompting: bool
    scope: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "path": self.path,
            "title": self.title,
            "estimated_tokens": self.estimated_tokens,
            "safe_for_128k_single_pass": self.safe_for_128k_single_pass,
            "needs_selective_prompting": self.needs_selective_prompting,
            "scope": self.scope,
        }


@dataclass(slots=True)
class AnalysisResult:
    files: list[FileSummary]
    queries: list[QueryModel]
    resolved_queries: list[ResolvedQueryModel]
    diagnostics: list[DiagnosticModel]
    artifacts: list[ArtifactDescriptor]

    def to_dict(self) -> dict[str, Any]:
        return {
            "files": [item.to_dict() for item in self.files],
            "queries": [item.to_dict() for item in self.queries],
            "resolved_queries": [item.to_dict() for item in self.resolved_queries],
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "artifacts": [item.to_dict() for item in self.artifacts],
        }
