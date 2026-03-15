from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .prompting import artifact_descriptor_for_path


ARTIFACT_SPECS: list[dict[str, Any]] = [
    {
        "path_pattern": "analysis/index.json",
        "kind": "json",
        "produced_by": ["analyze"],
        "description": "Machine-readable project index for queries, diagnostics, resolved SQL, and generated artifacts.",
        "top_level_fields": ["files", "queries", "resolved_queries", "diagnostics", "artifacts", "entrypoint"],
    },
    {
        "path_pattern": "analysis/executive_summary.json",
        "kind": "json",
        "produced_by": ["analyze"],
        "description": "Management summary for analyzer coverage, complexity hotspots, value hotspots, trend, and evolution.",
        "top_level_fields": [
            "generated_at",
            "profile_path",
            "headline",
            "management_summary",
            "trend_summary",
            "complexity_summary",
            "value_summary",
            "diagnostics_summary",
            "evolution_summary",
            "profile_lifecycle",
            "next_actions",
        ],
    },
    {
        "path_pattern": "analysis/failure_clusters.json",
        "kind": "json",
        "produced_by": ["analyze", "prepare-prompt"],
        "description": "Grouped recurring diagnostics with minimal evidence and task types for weak-LLM prompting.",
        "top_level_fields": ["generated_at", "clusters"],
    },
    {
        "path_pattern": "analysis/prompt_packs/*-bundle.json",
        "kind": "json",
        "produced_by": ["analyze", "prepare-prompt"],
        "description": "Prompt bundle metadata for classify/propose/verify stages and expected JSON schemas.",
        "top_level_fields": ["generated_at", "cluster_id", "budget", "model", "stages"],
    },
    {
        "path_pattern": "analysis/llm_runs/*/run_summary.json",
        "kind": "json",
        "produced_by": ["invoke-llm"],
        "description": "Single provider invocation summary with prompt metadata, token limits, usage, and optional review status.",
        "top_level_fields": [
            "generated_at",
            "cluster_id",
            "stage",
            "budget",
            "prompt_model",
            "provider_name",
            "provider_model",
            "token_limit",
            "prompt_estimated_tokens",
            "response_usage",
            "review_enabled",
        ],
    },
    {
        "path_pattern": "analysis/llm_reviews/*-review.json",
        "kind": "json",
        "produced_by": ["review-llm-response", "invoke-llm --review"],
        "description": "Weak-LLM review result with schema validation, accepted patch candidate, and next prompt guidance.",
        "top_level_fields": [
            "cluster_id",
            "stage",
            "status",
            "issues",
            "safe_to_apply_candidate",
            "profile_patch_candidate",
            "next_prompt_kind",
        ],
    },
    {
        "path_pattern": "analysis/proposals/rule_proposals.json",
        "kind": "json",
        "produced_by": ["propose-rules"],
        "description": "Accepted weak-LLM patch candidates collected into a proposal bundle for simulation.",
        "top_level_fields": [
            "generated_at",
            "analysis_root",
            "profile_source",
            "summary",
            "accepted_patches",
            "skipped_reviews",
        ],
    },
    {
        "path_pattern": "analysis/proposals/candidate_profile.json",
        "kind": "json",
        "produced_by": ["propose-rules", "apply-profile-patch"],
        "description": "Merged candidate profile ready for simulation, grading, and promotion.",
        "top_level_fields": [
            "profile_version",
            "profile_name",
            "profile_status",
            "parent_profile",
            "reference_target_default_order",
            "reference_token_patterns",
            "external_xml_name_map",
            "external_xml_scoped_map",
            "ignore_tags",
            "rules",
            "validation_history",
            "lifecycle_history",
        ],
    },
    {
        "path_pattern": "validation/profile_validation.json",
        "kind": "json",
        "produced_by": ["validate-profile", "simulate-profile"],
        "description": "Baseline-versus-profile validation payload with delta and recommendation.",
        "top_level_fields": ["profile_path", "assessment", "baseline", "profiled", "delta", "rule_usage"],
    },
    {
        "path_pattern": "simulation/profile_simulation.json",
        "kind": "json",
        "produced_by": ["simulate-profile"],
        "description": "Candidate profile simulation wrapper around the validation payload.",
        "top_level_fields": ["generated_at", "candidate_profile_path", "assessment", "validation_payload"],
    },
    {
        "path_pattern": "grade/profile_grade.json",
        "kind": "json",
        "produced_by": ["grade-profile"],
        "description": "Lifecycle grade with suggested next status, rollback recommendation, and reasoning.",
        "top_level_fields": [
            "generated_at",
            "profile_path",
            "validation_report_path",
            "current_status",
            "suggested_status",
            "promotion_readiness",
            "assessment",
            "history_summary",
            "latest_delta",
            "rollback_recommendation",
            "reasoning",
        ],
    },
    {
        "path_pattern": "profiles/*.history.json",
        "kind": "json",
        "produced_by": ["promote-profile", "rollback-profile"],
        "description": "Lifecycle sidecar summarizing profile validation history and lifecycle events.",
        "top_level_fields": [
            "generated_at",
            "profile_path",
            "profile_name",
            "profile_status",
            "profile_version",
            "parent_profile",
            "validation_history",
            "lifecycle_history",
        ],
    },
    {
        "path_pattern": "analysis/schema/artifact_catalog.json",
        "kind": "json",
        "produced_by": ["analyze"],
        "description": "Catalog of stable artifact contracts for humans, automation, and weaker LLMs.",
        "top_level_fields": ["generated_at", "artifacts"],
    },
]


def write_artifact_catalog(output_dir: Path) -> list:
    analysis_root = output_dir / "analysis" / "schema"
    analysis_root.mkdir(parents=True, exist_ok=True)
    json_path = analysis_root / "artifact_catalog.json"
    md_path = analysis_root / "artifact_catalog.md"
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "artifacts": ARTIFACT_SPECS,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_artifact_catalog_markdown(payload), encoding="utf-8")
    return [
        artifact_descriptor_for_path(json_path, "json", "Artifact catalog", "project"),
        artifact_descriptor_for_path(md_path, "markdown", "Artifact catalog (Markdown)", "project"),
    ]


def render_artifact_catalog_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Artifact Catalog",
        "",
        "Stable artifact contracts for the analyzer, self-healing workflow, and weak-LLM integration.",
        "",
    ]
    for item in payload["artifacts"]:
        lines.extend(
            [
                f"## {item['path_pattern']}",
                f"- Kind: `{item['kind']}`",
                f"- Produced by: {', '.join(f'`{command}`' for command in item['produced_by'])}",
                f"- Description: {item['description']}",
                f"- Top-level fields: {', '.join(f'`{field}`' for field in item['top_level_fields'])}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"
