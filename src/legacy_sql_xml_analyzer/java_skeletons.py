from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyzer import append_artifacts_to_index
from .java_bff import bundle_root_for, iter_bundle_payloads, resolve_java_bff_root, safe_name, to_pascal_case
from .models import ArtifactDescriptor


def generate_java_skeletons(
    analysis_root: Path,
    bundle_id: str | None = None,
    base_package: str = "com.example.bff",
) -> dict[str, Any]:
    java_root = resolve_java_bff_root(analysis_root)
    bundles = []
    if bundle_id:
        bundles.append(load_bundle_payload(analysis_root, bundle_id))
    else:
        bundles.extend(iter_bundle_payloads(analysis_root))

    artifacts: list[ArtifactDescriptor] = []
    manifests: list[dict[str, Any]] = []
    for bundle in bundles:
        merged_path = java_root / "merged" / safe_name(bundle["bundle_id"]) / "implementation_plan.json"
        if not merged_path.exists():
            continue
        merged = json.loads(merged_path.read_text(encoding="utf-8"))
        manifest = write_bundle_skeletons(
            java_root=java_root,
            bundle=bundle,
            merged=merged,
            base_package=base_package,
        )
        manifests.append(manifest["manifest"])
        artifacts.extend(manifest["artifacts"])

    if artifacts:
        append_artifacts_to_index(java_root.parent.parent, artifacts)
    return {
        "bundle_count": len(manifests),
        "manifests": manifests,
        "artifacts": artifacts,
    }


def load_bundle_payload(analysis_root: Path, bundle_id: str) -> dict[str, Any]:
    path = bundle_root_for(analysis_root, bundle_id) / "bundle.json"
    return json.loads(path.read_text(encoding="utf-8"))


def write_bundle_skeletons(
    java_root: Path,
    bundle: dict[str, Any],
    merged: dict[str, Any],
    base_package: str,
) -> dict[str, Any]:
    bundle_slug = safe_name(bundle["bundle_id"])
    skeleton_root = java_root / "skeletons" / bundle_slug
    package_path = skeleton_root / "src" / "main" / "java" / Path(*base_package.split("."))
    repository_root = package_path / "repository"
    service_root = package_path / "service"
    controller_root = package_path / "controller"
    dto_root = package_path / "dto"
    for directory in (repository_root, service_root, controller_root, dto_root):
        directory.mkdir(parents=True, exist_ok=True)

    class_base = to_pascal_case(bundle["entry_query_name"])
    repository_interface = repository_root / f"{class_base}Repository.java"
    repository_impl = repository_root / f"{class_base}RepositoryImpl.java"
    service_file = service_root / f"{class_base}Service.java"
    controller_file = controller_root / f"{class_base}Controller.java"
    request_file = dto_root / f"{class_base}Request.java"
    response_file = dto_root / f"{class_base}Response.java"
    manifest_json = skeleton_root / "manifest.json"
    manifest_md = skeleton_root / "manifest.md"
    readme_md = skeleton_root / "README.md"

    repository_methods = merged.get("plan_output", {}).get("repository_methods", [])
    if not isinstance(repository_methods, list) or not repository_methods:
        repository_methods = fallback_repository_methods(bundle)

    repository_interface.write_text(
        render_repository_interface(base_package, class_base, repository_methods),
        encoding="utf-8",
    )
    repository_impl.write_text(
        render_repository_impl(base_package, class_base, repository_methods, bundle, merged),
        encoding="utf-8",
    )
    service_file.write_text(
        render_service(base_package, class_base, repository_methods, merged),
        encoding="utf-8",
    )
    controller_file.write_text(
        render_controller(base_package, class_base, merged),
        encoding="utf-8",
    )
    request_file.write_text(
        render_request_dto(base_package, class_base, bundle),
        encoding="utf-8",
    )
    response_file.write_text(
        render_response_dto(base_package, class_base, merged),
        encoding="utf-8",
    )

    manifest_payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "bundle_id": bundle["bundle_id"],
        "base_package": base_package,
        "ready_for_skeletons": bool(merged.get("completion", {}).get("ready_for_skeletons")),
        "source_artifacts": {
            "bundle_json": str((java_root / "bundles" / bundle_slug / "bundle.json").resolve()),
            "merged_plan_json": str((java_root / "merged" / bundle_slug / "implementation_plan.json").resolve()),
        },
        "files": [
            str(repository_interface.resolve()),
            str(repository_impl.resolve()),
            str(service_file.resolve()),
            str(controller_file.resolve()),
            str(request_file.resolve()),
            str(response_file.resolve()),
        ],
    }
    manifest_json.write_text(json.dumps(manifest_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    manifest_md.write_text(render_manifest_markdown(manifest_payload), encoding="utf-8")
    readme_md.write_text(render_bundle_readme(manifest_payload, merged), encoding="utf-8")
    artifacts = [
        artifact_descriptor_for_path(repository_interface, "code", f"Java skeleton repository interface: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(repository_impl, "code", f"Java skeleton repository impl: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(service_file, "code", f"Java skeleton service: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(controller_file, "code", f"Java skeleton controller: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(request_file, "code", f"Java skeleton request DTO: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(response_file, "code", f"Java skeleton response DTO: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(manifest_json, "json", f"Java skeleton manifest: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(manifest_md, "markdown", f"Java skeleton manifest summary: {bundle['bundle_id']}", "java_bff"),
        artifact_descriptor_for_path(readme_md, "markdown", f"Java skeleton handoff readme: {bundle['bundle_id']}", "java_bff"),
    ]
    return {
        "manifest": manifest_payload,
        "artifacts": artifacts,
    }


def fallback_repository_methods(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    methods = []
    for query in bundle.get("queries", []):
        methods.append(
            {
                "query_id": query["query_id"],
                "method_name": to_pascal_case(Path(query["query_id"]).stem) if ":" not in query["query_id"] else to_pascal_case(query["query_id"].split(":")[-1]),
                "purpose": f"Execute logic for {query['query_id']}.",
                "input_params": ["request"],
                "result_contract": "Map<String, Object> or DTO projection",
            }
        )
    return methods


def render_repository_interface(base_package: str, class_base: str, repository_methods: list[dict[str, Any]]) -> str:
    lines = [
        f"package {base_package}.repository;",
        "",
        "import java.util.List;",
        "import java.util.Map;",
        "",
        f"public interface {class_base}Repository {{",
    ]
    for method in repository_methods:
        method_name = str(method.get("method_name") or f"run{class_base}")
        lines.append(f"    List<Map<String, Object>> {method_name}(Map<String, Object> request);")
    lines.append("}")
    return "\n".join(lines).rstrip() + "\n"


def render_repository_impl(
    base_package: str,
    class_base: str,
    repository_methods: list[dict[str, Any]],
    bundle: dict[str, Any],
    merged: dict[str, Any],
) -> str:
    lines = [
        f"package {base_package}.repository;",
        "",
        "import java.util.List;",
        "import java.util.Map;",
        "import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;",
        "import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;",
        "import org.springframework.stereotype.Repository;",
        "",
        "@Repository",
        f"public class {class_base}RepositoryImpl implements {class_base}Repository {{",
        "    private final NamedParameterJdbcTemplate jdbcTemplate;",
        "",
        f"    public {class_base}RepositoryImpl(NamedParameterJdbcTemplate jdbcTemplate) {{",
        "        this.jdbcTemplate = jdbcTemplate;",
        "    }",
        "",
    ]
    repository_merge_outputs = merged.get("repository_merge_outputs", {})
    for method in repository_methods:
        method_name = str(method.get("method_name") or f"run{class_base}")
        query_id = str(method.get("query_id") or bundle["entry_query_id"])
        merge_notes = repository_merge_outputs.get(query_id, {})
        lines.extend(
            [
                "    @Override",
                f"    public List<Map<String, Object>> {method_name}(Map<String, Object> request) {{",
                f"        // Source query: {query_id}",
                f"        // Purpose: {method.get('purpose') or 'TODO'}",
                "        MapSqlParameterSource params = new MapSqlParameterSource();",
                "        // TODO: map request fields into SQL parameters using the analyzed implementation cards.",
                f"        // TODO: load Oracle 19c SQL for {query_id} from the generated artifact bundle or an external resource file.",
            ]
        )
        for item in merge_notes.get("repository_logic", []) if isinstance(merge_notes, dict) else []:
            lines.append(f"        // MERGED LOGIC: {item}")
        lines.extend(
            [
                "        String sql = \"\";",
                "        // TODO: replace the empty SQL string with the preserved analyzed SQL text.",
                "        return jdbcTemplate.queryForList(sql, params);",
                "    }",
                "",
            ]
        )
    lines.append("}")
    return "\n".join(lines).rstrip() + "\n"


def render_service(base_package: str, class_base: str, repository_methods: list[dict[str, Any]], merged: dict[str, Any]) -> str:
    service_flow = merged.get("plan_output", {}).get("service_flow", [])
    lines = [
        f"package {base_package}.service;",
        "",
        f"import {base_package}.repository.{class_base}Repository;",
        f"import {base_package}.dto.{class_base}Request;",
        f"import {base_package}.dto.{class_base}Response;",
        "import org.springframework.stereotype.Service;",
        "",
        "@Service",
        f"public class {class_base}Service {{",
        f"    private final {class_base}Repository repository;",
        "",
        f"    public {class_base}Service({class_base}Repository repository) {{",
        "        this.repository = repository;",
        "    }",
        "",
        f"    public {class_base}Response handle({class_base}Request request) {{",
        "        // TODO: validate request and normalize API input before invoking the repository.",
    ]
    for item in service_flow[:6] if isinstance(service_flow, list) else []:
        lines.append(f"        // SERVICE FLOW: {item}")
    first_method = str(repository_methods[0].get("method_name") or f"run{class_base}") if repository_methods else f"run{class_base}"
    lines.extend(
        [
            f"        repository.{first_method}(request.toParameterMap());",
            f"        return new {class_base}Response();",
            "    }",
            "}",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_controller(base_package: str, class_base: str, merged: dict[str, Any]) -> str:
    controller_notes = merged.get("assembly_output", {}).get("controller_logic", [])
    endpoint = "/" + "".join(("-" + char.lower() if char.isupper() else char) for char in class_base).lstrip("-")
    lines = [
        f"package {base_package}.controller;",
        "",
        f"import {base_package}.dto.{class_base}Request;",
        f"import {base_package}.dto.{class_base}Response;",
        f"import {base_package}.service.{class_base}Service;",
        "import org.springframework.web.bind.annotation.PostMapping;",
        "import org.springframework.web.bind.annotation.RequestBody;",
        "import org.springframework.web.bind.annotation.RequestMapping;",
        "import org.springframework.web.bind.annotation.RestController;",
        "",
        "@RestController",
        f"@RequestMapping(\"{endpoint}\")",
        f"public class {class_base}Controller {{",
        f"    private final {class_base}Service service;",
        "",
        f"    public {class_base}Controller({class_base}Service service) {{",
        "        this.service = service;",
        "    }",
        "",
        "    @PostMapping",
        f"    public {class_base}Response handle(@RequestBody {class_base}Request request) {{",
    ]
    for item in controller_notes[:4] if isinstance(controller_notes, list) else []:
        lines.append(f"        // CONTROLLER NOTE: {item}")
    lines.extend(
        [
            "        return service.handle(request);",
            "    }",
            "}",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_request_dto(base_package: str, class_base: str, bundle: dict[str, Any]) -> str:
    entry_query = next((item for item in bundle.get("queries", []) if item["query_id"] == bundle["entry_query_id"]), None)
    lines = [
        f"package {base_package}.dto;",
        "",
        "import java.util.HashMap;",
        "import java.util.Map;",
        "",
        f"public class {class_base}Request {{",
        "    // TODO: replace generic fields with the real request contract derived from the accepted Java BFF plan.",
        "    private String keyword;",
        "",
        "    public String getKeyword() {",
        "        return keyword;",
        "    }",
        "",
        "    public void setKeyword(String keyword) {",
        "        this.keyword = keyword;",
        "    }",
        "",
        "    public Map<String, Object> toParameterMap() {",
        "        Map<String, Object> params = new HashMap<>();",
        "        params.put(\"keyword\", keyword);",
    ]
    if entry_query:
        lines.append(f"        // Entry query source: {entry_query['query_id']}")
    lines.extend(
        [
            "        return params;",
            "    }",
            "}",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_response_dto(base_package: str, class_base: str, merged: dict[str, Any]) -> str:
    lines = [
        f"package {base_package}.dto;",
        "",
        "import java.util.ArrayList;",
        "import java.util.List;",
        "import java.util.Map;",
        "",
        f"public class {class_base}Response {{",
        "    // TODO: replace this generic payload with a typed DTO contract.",
        "    private List<Map<String, Object>> items = new ArrayList<>();",
        "",
        "    public List<Map<String, Object>> getItems() {",
        "        return items;",
        "    }",
        "",
        "    public void setItems(List<Map<String, Object>> items) {",
        "        this.items = items;",
        "    }",
        "}",
    ]
    return "\n".join(lines).rstrip() + "\n"


def render_manifest_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Java Skeleton Manifest",
        "",
        f"- Bundle: `{payload['bundle_id']}`",
        f"- Base package: `{payload['base_package']}`",
        f"- Ready for skeletons: `{payload['ready_for_skeletons']}`",
        "",
        "## Files",
    ]
    for item in payload["files"]:
        lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def render_bundle_readme(payload: dict[str, Any], merged: dict[str, Any]) -> str:
    repository_queries = merged.get("repository_plan", {}).get("queries", [])
    service_logic = merged.get("bff_plan", {}).get("service_logic", [])
    controller_logic = merged.get("bff_plan", {}).get("controller_logic", [])
    lines = [
        "# Java BFF Skeleton Bundle",
        "",
        f"- Bundle: `{payload['bundle_id']}`",
        f"- Base package: `{payload['base_package']}`",
        f"- Ready for skeletons: `{payload['ready_for_skeletons']}`",
        "",
        "## What This Bundle Contains",
        "- Repository interface and JDBC-backed repository implementation skeletons.",
        "- Service and controller skeletons aligned to the accepted Java BFF phase outputs.",
        "- Request/response DTO placeholders that still need business-contract hardening.",
        "",
        "## Repository Queries",
    ]
    if isinstance(repository_queries, list) and repository_queries:
        for item in repository_queries:
            if not isinstance(item, dict):
                continue
            lines.append(
                f"- `{item.get('query_id', 'unknown')}` chunks={item.get('chunk_count', 0)} "
                f"merged={'yes' if item.get('merge_output') else 'no'}"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Service Logic Hints"])
    if isinstance(service_logic, list) and service_logic:
        for item in service_logic[:6]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")
    lines.extend(["", "## Controller Logic Hints"])
    if isinstance(controller_logic, list) and controller_logic:
        for item in controller_logic[:4]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")
    lines.extend(["", "## Files"])
    for item in payload["files"]:
        lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def artifact_descriptor_for_path(path: Path, kind: str, title: str, scope: str) -> ArtifactDescriptor:
    content = path.read_text(encoding="utf-8")
    estimated_tokens = max(1, round(len(content) / 4)) if content else 0
    return ArtifactDescriptor(
        kind=kind,
        path=str(path.resolve()),
        title=title,
        estimated_tokens=estimated_tokens,
        safe_for_128k_single_pass=estimated_tokens <= 100_000,
        needs_selective_prompting=estimated_tokens > 40_000,
        scope=scope,
    )
