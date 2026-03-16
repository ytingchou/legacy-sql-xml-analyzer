from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyzer import append_artifacts_to_index
from .dashboard import write_evolution_report
from .learning import AnalysisProfile, ProfileRule, load_profile
from .prompting import (
    PROMPT_STAGES,
    answer_schema_for_cluster,
    artifact_descriptor_for_path,
    load_failure_clusters,
    render_prompt_pack_text,
    resolve_analysis_root,
    sanitize_token,
)
from .response_normalizer import normalize_response as normalize_llm_response


ALLOWED_CONFIDENCE = {"low": 0.45, "medium": 0.7, "high": 0.9}
ALLOWED_SCOPE = {"global", "source_scoped", "local"}
ALLOWED_NEXT_STAGES = {"propose", "insufficient_evidence"}
ALLOWED_VERDICTS = {"accept", "needs_review", "reject"}
PROFILE_RULE_TYPES = {
    "external_xml_name_mapping",
    "external_xml_scoped_mapping",
    "reference_token_pattern",
    "reference_target_default_order",
    "ignore_tag",
}


def review_llm_response_from_analysis(
    analysis_root: Path,
    cluster_id: str,
    response_path: Path,
    stage: str = "propose",
    budget: str = "128k",
    model: str = "weak-128k",
    profile_path: Path | None = None,
) -> dict[str, Any]:
    if stage not in PROMPT_STAGES:
        raise ValueError(f"Unsupported stage: {stage}")

    analysis_root = resolve_analysis_root(analysis_root)
    payload = load_failure_clusters(analysis_root)
    cluster = next((item for item in payload["clusters"] if item["cluster_id"] == cluster_id), None)
    if cluster is None:
        raise ValueError(f"Unknown cluster_id: {cluster_id}")

    profile = load_profile(profile_path) if profile_path else None
    raw_text = response_path.read_text(encoding="utf-8")
    review = review_llm_response(
        cluster=cluster,
        raw_text=raw_text,
        stage=stage,
        budget=budget,
        model=model,
        profile=profile,
    )
    review["response_path"] = str(response_path.resolve())
    review["profile_path"] = str(profile_path.resolve()) if profile_path else None

    review_root = analysis_root / "llm_reviews"
    review_root.mkdir(parents=True, exist_ok=True)
    base_name = f"{cluster_id}-{sanitize_token(stage)}"
    review_json_path = review_root / f"{base_name}-review.json"
    review_md_path = review_root / f"{base_name}-review.md"
    review["review_json_path"] = str(review_json_path.resolve())
    review["review_md_path"] = str(review_md_path.resolve())
    review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
    review_md_path.write_text(render_review_markdown(review), encoding="utf-8")

    artifacts = [
        artifact_descriptor_for_path(review_json_path, "json", f"LLM review ({stage}): {cluster_id}", "prompting"),
        artifact_descriptor_for_path(review_md_path, "markdown", f"LLM review summary ({stage}): {cluster_id}", "prompting"),
    ]
    normalization_report = review.get("normalization_report")
    if isinstance(normalization_report, dict):
        normalization_path = review_root / f"{base_name}-normalization.json"
        normalization_path.write_text(json.dumps(normalization_report, indent=2, ensure_ascii=False), encoding="utf-8")
        review["normalization_report_path"] = str(normalization_path.resolve())
        review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts.append(
            artifact_descriptor_for_path(normalization_path, "json", f"LLM normalization ({stage}): {cluster_id}", "prompting")
        )

    next_prompt_text = review.get("next_prompt_text")
    if isinstance(next_prompt_text, str) and next_prompt_text.strip():
        next_prompt_path = review_root / f"{base_name}-{review['next_prompt_kind']}.txt"
        next_prompt_path.write_text(next_prompt_text, encoding="utf-8")
        review["next_prompt_path"] = str(next_prompt_path)
        review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts.append(
            artifact_descriptor_for_path(next_prompt_path, "text", f"LLM follow-up prompt ({stage}): {cluster_id}", "prompting")
        )

    patch_candidate = review.get("profile_patch_candidate")
    if isinstance(patch_candidate, dict):
        patch_path = review_root / f"{base_name}-profile-patch.json"
        patch_path.write_text(json.dumps(patch_candidate, indent=2, ensure_ascii=False), encoding="utf-8")
        review["profile_patch_path"] = str(patch_path)
        review_json_path.write_text(json.dumps(review, indent=2, ensure_ascii=False), encoding="utf-8")
        artifacts.append(
            artifact_descriptor_for_path(patch_path, "json", f"Profile patch candidate ({stage}): {cluster_id}", "prompting")
        )

    evolution_artifacts = write_evolution_report(analysis_root.parent)
    append_artifacts_to_index(analysis_root.parent, artifacts)
    append_artifacts_to_index(analysis_root.parent, evolution_artifacts)
    return {
        "cluster": cluster,
        "review": review,
        "artifacts": artifacts + evolution_artifacts,
    }


def review_llm_response(
    cluster: dict[str, Any],
    raw_text: str,
    stage: str,
    budget: str,
    model: str,
    profile: AnalysisProfile | None = None,
) -> dict[str, Any]:
    normalization = normalize_llm_response(raw_text, source="generic-review")
    normalized_text = normalization.normalized_text
    normalization_notes = normalization.applied_steps
    issues: list[dict[str, Any]] = []
    parsed_response: dict[str, Any] | None = None
    profile_patch_candidate: dict[str, Any] | None = None

    loaded = normalization.normalized_object
    if loaded is None:
        try:
            loaded = json.loads(normalized_text)
        except json.JSONDecodeError as exc:
            issues.append(issue("INVALID_JSON", "error", f"Response is not valid JSON: {exc.msg}."))
        else:
            if not isinstance(loaded, dict):
                issues.append(issue("RESPONSE_NOT_OBJECT", "error", "Response JSON must be a top-level object."))
                loaded = None
    if loaded is not None:
        if not isinstance(loaded, dict):
            issues.append(issue("RESPONSE_NOT_OBJECT", "error", "Response JSON must be a top-level object."))
        else:
            parsed_response = loaded
            issues.extend(validate_common_fields(cluster, parsed_response, stage))
            if stage == "classify":
                issues.extend(validate_classify_response(cluster, parsed_response))
            elif stage == "propose":
                proposal_issues, profile_patch_candidate = validate_propose_response(cluster, parsed_response, profile)
                issues.extend(proposal_issues)
            elif stage == "verify":
                issues.extend(validate_verify_response(cluster, parsed_response))

    has_errors = any(item["severity"] == "error" for item in issues)
    insufficient_evidence = bool(parsed_response and parsed_response.get("insufficient_evidence"))
    if has_errors:
        status = "needs_revision"
    elif insufficient_evidence:
        status = "insufficient_evidence"
    else:
        status = "accepted"

    next_prompt_kind = None
    next_prompt_text = None
    if status == "needs_revision":
        next_prompt_kind = "repair"
        next_prompt_text = render_repair_prompt(cluster, stage, raw_text, issues)
    elif stage == "classify" and parsed_response is not None:
        if parsed_response.get("recommended_next_stage") == "propose":
            next_prompt_kind = "next-propose"
            next_prompt_text = render_prompt_pack_text(
                cluster=cluster,
                samples=cluster["sample_diagnostics"][:3],
                budget=budget,
                model=model,
                stage="propose",
                prior_response=parsed_response,
            )
    elif stage == "propose" and parsed_response is not None:
        if status == "accepted":
            next_prompt_kind = "next-verify"
            next_prompt_text = render_prompt_pack_text(
                cluster=cluster,
                samples=cluster["sample_diagnostics"][:3],
                budget=budget,
                model=model,
                stage="verify",
                prior_response=parsed_response,
            )
        elif status == "insufficient_evidence":
            next_prompt_kind = "evidence-request"
            next_prompt_text = render_evidence_request_prompt(cluster, parsed_response)

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "cluster_id": cluster["cluster_id"],
        "stage": stage,
        "status": status,
        "normalization_notes": normalization_notes,
        "normalization_report": normalization.to_dict(),
        "issues": issues,
        "parsed_response": parsed_response,
        "profile_patch_candidate": profile_patch_candidate,
        "next_prompt_kind": next_prompt_kind,
        "next_prompt_text": next_prompt_text,
        "safe_to_apply_candidate": bool(profile_patch_candidate) and not has_errors and not any(
            item["severity"] == "warning" for item in issues
        ),
    }


def validate_common_fields(cluster: dict[str, Any], response: dict[str, Any], stage: str) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    required_fields = required_fields_for_stage(stage)
    for field_name in required_fields:
        if field_name not in response:
            issues.append(issue("MISSING_FIELD", "error", f"Missing required field '{field_name}'.", field=field_name))

    cluster_id = response.get("cluster_id")
    if cluster_id is not None and cluster_id != cluster["cluster_id"]:
        issues.append(
            issue(
                "CLUSTER_ID_MISMATCH",
                "error",
                f"Response cluster_id '{cluster_id}' does not match expected '{cluster['cluster_id']}'.",
                field="cluster_id",
            )
        )

    problem_type = response.get("problem_type")
    if problem_type is not None and problem_type != cluster["task_type"]:
        issues.append(
            issue(
                "PROBLEM_TYPE_MISMATCH",
                "warning",
                f"problem_type '{problem_type}' does not match expected '{cluster['task_type']}'.",
                field="problem_type",
            )
        )

    confidence = response.get("confidence")
    if confidence is not None and confidence not in ALLOWED_CONFIDENCE:
        issues.append(
            issue(
                "CONFIDENCE_INVALID",
                "error",
                f"confidence must be one of {', '.join(sorted(ALLOWED_CONFIDENCE))}.",
                field="confidence",
            )
        )

    insufficient_evidence = response.get("insufficient_evidence")
    if insufficient_evidence is not None and not isinstance(insufficient_evidence, bool):
        issues.append(
            issue(
                "INSUFFICIENT_EVIDENCE_INVALID",
                "error",
                "insufficient_evidence must be a boolean.",
                field="insufficient_evidence",
            )
        )
    return issues


def validate_classify_response(cluster: dict[str, Any], response: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    next_stage = response.get("recommended_next_stage")
    if next_stage is not None and next_stage not in ALLOWED_NEXT_STAGES:
        issues.append(
            issue(
                "NEXT_STAGE_INVALID",
                "error",
                f"recommended_next_stage must be one of {', '.join(sorted(ALLOWED_NEXT_STAGES))}.",
                field="recommended_next_stage",
            )
        )
    for key in ("evidence_summary", "missing_evidence"):
        if key in response and not isinstance(response.get(key), list):
            issues.append(issue("FIELD_NOT_LIST", "error", f"Field '{key}' must be a list of strings.", field=key))
    return issues


def validate_propose_response(
    cluster: dict[str, Any],
    response: dict[str, Any],
    profile: AnalysisProfile | None,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    issues: list[dict[str, Any]] = []
    patch_candidate = None
    change_type = response.get("proposed_change_type")
    if change_type not in {"profile_rule", "xml_fix", "sql_fix", "insufficient_evidence"}:
        issues.append(
            issue(
                "CHANGE_TYPE_INVALID",
                "error",
                "proposed_change_type must be one of profile_rule, xml_fix, sql_fix, insufficient_evidence.",
                field="proposed_change_type",
            )
        )
        return issues, None

    for key in ("why", "verification_steps", "risks"):
        if key in response and not isinstance(response.get(key), list):
            issues.append(issue("FIELD_NOT_LIST", "error", f"Field '{key}' must be a list of strings.", field=key))

    if change_type != "profile_rule":
        return issues, None

    proposal = response.get("proposed_rule_or_fix")
    if not isinstance(proposal, dict):
        issues.append(
            issue(
                "PROPOSAL_INVALID",
                "error",
                "proposed_rule_or_fix must be an object when proposed_change_type is profile_rule.",
                field="proposed_rule_or_fix",
            )
        )
        return issues, None

    patch_candidate, patch_issues = normalize_profile_rule(cluster, proposal, response.get("confidence", "medium"), profile)
    issues.extend(patch_issues)
    return issues, patch_candidate


def validate_verify_response(cluster: dict[str, Any], response: dict[str, Any]) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    verdict = response.get("verdict")
    if verdict is not None and verdict not in ALLOWED_VERDICTS:
        issues.append(
            issue(
                "VERDICT_INVALID",
                "error",
                f"verdict must be one of {', '.join(sorted(ALLOWED_VERDICTS))}.",
                field="verdict",
            )
        )
    if "safe_to_apply" in response and not isinstance(response.get("safe_to_apply"), bool):
        issues.append(issue("SAFE_TO_APPLY_INVALID", "error", "safe_to_apply must be boolean.", field="safe_to_apply"))
    for key in ("checked_constraints", "violations", "follow_up_actions"):
        if key in response and not isinstance(response.get(key), list):
            issues.append(issue("FIELD_NOT_LIST", "error", f"Field '{key}' must be a list of strings.", field=key))
    normalized = response.get("normalized_rule_or_fix")
    if normalized is not None and not isinstance(normalized, dict):
        issues.append(
            issue(
                "NORMALIZED_RULE_INVALID",
                "error",
                "normalized_rule_or_fix must be an object when provided.",
                field="normalized_rule_or_fix",
            )
        )
    return issues


def normalize_profile_rule(
    cluster: dict[str, Any],
    proposal: dict[str, Any],
    confidence_label: str,
    profile: AnalysisProfile | None,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    rule_type = str(proposal.get("rule_type", "")).strip()
    scope = str(proposal.get("scope", "")).strip()
    payload = proposal.get("payload")
    if rule_type not in PROFILE_RULE_TYPES:
        issues.append(
            issue(
                "RULE_TYPE_UNSUPPORTED",
                "error",
                f"Unsupported profile rule type '{rule_type}'.",
                field="proposed_rule_or_fix.rule_type",
            )
        )
        return None, issues
    if scope not in ALLOWED_SCOPE:
        issues.append(
            issue(
                "RULE_SCOPE_INVALID",
                "error",
                f"Rule scope must be one of {', '.join(sorted(ALLOWED_SCOPE))}.",
                field="proposed_rule_or_fix.scope",
            )
        )
        return None, issues
    if not isinstance(payload, dict):
        issues.append(
            issue(
                "RULE_PAYLOAD_INVALID",
                "error",
                "Rule payload must be an object.",
                field="proposed_rule_or_fix.payload",
            )
        )
        return None, issues

    normalized_action: dict[str, Any] | None = None
    if rule_type == "external_xml_name_mapping":
        if scope != "global":
            issues.append(issue("RULE_SCOPE_MISMATCH", "error", "external_xml_name_mapping must use global scope."))
        xml_name = compact_string(payload.get("xml_name"))
        mapped_to = compact_string(payload.get("mapped_to"))
        if not xml_name or not mapped_to:
            issues.append(issue("RULE_PAYLOAD_MISSING", "error", "xml_name and mapped_to are required for external_xml_name_mapping."))
        else:
            normalized_action = {"xml_name": xml_name, "mapped_to": mapped_to}
            issues.extend(check_profile_conflicts(profile, rule_type, normalized_action))
    elif rule_type == "external_xml_scoped_mapping":
        if scope != "source_scoped":
            issues.append(issue("RULE_SCOPE_MISMATCH", "error", "external_xml_scoped_mapping must use source_scoped scope."))
        source_dir = compact_string(payload.get("source_dir"))
        xml_name = compact_string(payload.get("xml_name"))
        mapped_to = compact_string(payload.get("mapped_to"))
        if not source_dir or not xml_name or not mapped_to:
            issues.append(
                issue(
                    "RULE_PAYLOAD_MISSING",
                    "error",
                    "source_dir, xml_name, and mapped_to are required for external_xml_scoped_mapping.",
                )
            )
        else:
            normalized_action = {"source_dir": source_dir, "xml_name": xml_name, "mapped_to": mapped_to}
            issues.extend(check_profile_conflicts(profile, rule_type, normalized_action))
    elif rule_type == "reference_token_pattern":
        if scope != "global":
            issues.append(issue("RULE_SCOPE_MISMATCH", "error", "reference_token_pattern must use global scope."))
        pattern = compact_string(payload.get("pattern"))
        if not pattern or "{name}" not in pattern:
            issues.append(
                issue(
                    "RULE_PATTERN_INVALID",
                    "error",
                    "reference_token_pattern payload.pattern must contain '{name}'.",
                )
            )
        else:
            normalized_action = {"pattern": pattern}
            issues.extend(check_profile_conflicts(profile, rule_type, normalized_action))
    elif rule_type == "reference_target_default_order":
        if scope != "global":
            issues.append(issue("RULE_SCOPE_MISMATCH", "error", "reference_target_default_order must use global scope."))
        order = payload.get("reference_target_default_order", payload.get("order"))
        if not isinstance(order, list):
            issues.append(
                issue(
                    "RULE_PAYLOAD_INVALID",
                    "error",
                    "reference_target_default_order payload must include a list under reference_target_default_order or order.",
                )
            )
        else:
            normalized_order = [str(item) for item in order if str(item) in {"sub", "main"}]
            if len(normalized_order) != 2 or len(set(normalized_order)) != 2:
                issues.append(
                    issue(
                        "RULE_ORDER_INVALID",
                        "error",
                        "reference_target_default_order must contain both 'sub' and 'main' exactly once.",
                    )
                )
            else:
                normalized_action = {"reference_target_default_order": normalized_order}
                issues.extend(check_profile_conflicts(profile, rule_type, normalized_action))
    elif rule_type == "ignore_tag":
        if scope != "global":
            issues.append(issue("RULE_SCOPE_MISMATCH", "error", "ignore_tag must use global scope."))
        tag = compact_string(payload.get("tag"))
        if not tag:
            issues.append(issue("RULE_PAYLOAD_MISSING", "error", "ignore_tag payload.tag is required."))
        else:
            normalized_action = {"tag": tag}
            issues.extend(check_profile_conflicts(profile, rule_type, normalized_action))

    if normalized_action is None or any(item["severity"] == "error" for item in issues):
        return None, issues

    rule_id = f"llm-{cluster['cluster_id']}-{sanitize_token(rule_type)}"
    patch_candidate = {
        "rule_id": rule_id,
        "rule_type": rule_type,
        "scope": scope,
        "confidence_label": confidence_label,
        "confidence_score": ALLOWED_CONFIDENCE.get(confidence_label, 0.7),
        "description": f"LLM-proposed {rule_type} derived from failure cluster {cluster['cluster_id']}.",
        "evidence": {
            "cluster_id": cluster["cluster_id"],
            "cluster_code": cluster["code"],
            "occurrence_count": cluster["occurrence_count"],
            "files_affected": cluster["files_affected"],
        },
        "proposed_action": normalized_action,
        "merge_preview": build_merge_preview(rule_type, normalized_action),
    }
    return patch_candidate, issues


def check_profile_conflicts(
    profile: AnalysisProfile | None,
    rule_type: str,
    action: dict[str, Any],
) -> list[dict[str, Any]]:
    if profile is None:
        return []
    issues: list[dict[str, Any]] = []
    if rule_type == "external_xml_name_mapping":
        existing = profile.external_xml_name_map.get(action["xml_name"])
        if existing == action["mapped_to"]:
            issues.append(issue("RULE_ALREADY_PRESENT", "info", "This external_xml_name_mapping already exists in the profile."))
        elif existing:
            issues.append(
                issue(
                    "PROFILE_CONFLICT",
                    "error",
                    f"Profile already maps xml_name '{action['xml_name']}' to '{existing}', not '{action['mapped_to']}'.",
                )
            )
    elif rule_type == "external_xml_scoped_mapping":
        key = f"{action['source_dir']}::{action['xml_name']}"
        existing = profile.external_xml_scoped_map.get(key)
        if existing == action["mapped_to"]:
            issues.append(issue("RULE_ALREADY_PRESENT", "info", "This external_xml_scoped_mapping already exists in the profile."))
        elif existing:
            issues.append(
                issue(
                    "PROFILE_CONFLICT",
                    "error",
                    f"Profile already maps '{key}' to '{existing}', not '{action['mapped_to']}'.",
                )
            )
    elif rule_type == "reference_token_pattern":
        if action["pattern"] in profile.reference_token_patterns:
            issues.append(issue("RULE_ALREADY_PRESENT", "info", "This reference_token_pattern already exists in the profile."))
    elif rule_type == "reference_target_default_order":
        if action["reference_target_default_order"] == profile.reference_target_default_order:
            issues.append(issue("RULE_ALREADY_PRESENT", "info", "This reference_target_default_order already matches the active profile."))
    elif rule_type == "ignore_tag":
        if action["tag"] in profile.ignore_tags:
            issues.append(issue("RULE_ALREADY_PRESENT", "info", "This ignore_tag already exists in the profile."))
    return issues


def build_merge_preview(rule_type: str, action: dict[str, Any]) -> dict[str, Any]:
    if rule_type == "external_xml_name_mapping":
        return {"external_xml_name_map_add": {action["xml_name"]: action["mapped_to"]}}
    if rule_type == "external_xml_scoped_mapping":
        key = f"{action['source_dir']}::{action['xml_name']}"
        return {"external_xml_scoped_map_add": {key: action["mapped_to"]}}
    if rule_type == "reference_token_pattern":
        return {"reference_token_patterns_add": [action["pattern"]]}
    if rule_type == "reference_target_default_order":
        return {"reference_target_default_order": action["reference_target_default_order"]}
    if rule_type == "ignore_tag":
        return {"ignore_tags_add": [action["tag"]]}
    return {}


def render_review_markdown(review: dict[str, Any]) -> str:
    lines = [
        "# LLM Response Review",
        "",
        "## Summary",
        f"- Cluster: `{review['cluster_id']}`",
        f"- Stage: `{review['stage']}`",
        f"- Status: `{review['status']}`",
        f"- Safe to apply candidate: `{review['safe_to_apply_candidate']}`",
        "",
        "## Normalization Notes",
    ]
    notes = review.get("normalization_notes", [])
    if notes:
        for note in notes:
            lines.append(f"- {note}")
    else:
        lines.append("- None")

    lines.extend(["", "## Issues"])
    issues = review.get("issues", [])
    if issues:
        for item in issues:
            field_text = f" field={item['field']}" if item.get("field") else ""
            lines.append(f"- `{item['severity']}` `{item['code']}`{field_text}: {item['message']}")
    else:
        lines.append("- None")

    lines.extend(["", "## Parsed Response", "```json", json.dumps(review.get("parsed_response"), indent=2, ensure_ascii=False), "```"])
    lines.extend(["", "## Profile Patch Candidate", "```json", json.dumps(review.get("profile_patch_candidate"), indent=2, ensure_ascii=False), "```"])
    if review.get("next_prompt_kind"):
        lines.extend(["", "## Next Prompt", f"- Kind: `{review['next_prompt_kind']}`"])
    return "\n".join(lines).rstrip() + "\n"


def render_repair_prompt(
    cluster: dict[str, Any],
    stage: str,
    raw_text: str,
    issues: list[dict[str, Any]],
) -> str:
    schema = json.dumps(answer_schema_for_cluster(cluster, stage=stage), indent=2, ensure_ascii=False)
    lines = [
        f"Your previous response for cluster {cluster['cluster_id']} at stage {stage} could not be accepted.",
        "",
        "Problems to fix:",
    ]
    for item in issues:
        lines.append(f"- {item['code']}: {item['message']}")
    lines.extend(
        [
            "",
            "Return corrected JSON only with this schema:",
            schema,
            "",
            "Do not include markdown fences or extra explanation.",
            "Do not invent XML structure that is not supported by the evidence.",
            "",
            "Previous response:",
            raw_text.strip() or "<empty>",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def render_evidence_request_prompt(cluster: dict[str, Any], response: dict[str, Any]) -> str:
    missing_evidence = response.get("missing_evidence")
    if not isinstance(missing_evidence, list):
        missing_evidence = ["Explain exactly what evidence is still required."]
    lines = [
        f"Stage: collect evidence for cluster {cluster['cluster_id']}",
        "",
        "The previous model response marked this issue as insufficient_evidence.",
        "Identify the minimum additional analyzer artifacts or XML snippets needed before proposing a rule.",
        "",
        "Missing evidence to collect:",
    ]
    for item in missing_evidence:
        lines.append(f"- {item}")
    lines.extend(
        [
            "",
            "Do not propose a rule yet. Only request the smallest additional evidence set.",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def normalize_response_text(raw_text: str) -> tuple[str, list[str]]:
    normalization = normalize_llm_response(raw_text, source="generic-review")
    return normalization.normalized_text, normalization.applied_steps


def required_fields_for_stage(stage: str) -> list[str]:
    if stage == "classify":
        return [
            "cluster_id",
            "problem_type",
            "suspected_root_cause",
            "evidence_summary",
            "missing_evidence",
            "recommended_next_stage",
            "confidence",
            "insufficient_evidence",
        ]
    if stage == "verify":
        return [
            "cluster_id",
            "problem_type",
            "verdict",
            "safe_to_apply",
            "checked_constraints",
            "violations",
            "follow_up_actions",
        ]
    return [
        "cluster_id",
        "problem_type",
        "root_cause",
        "proposed_change_type",
        "proposed_rule_or_fix",
        "confidence",
        "why",
        "verification_steps",
        "risks",
        "insufficient_evidence",
    ]


def compact_string(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def issue(code: str, severity: str, message: str, field: str | None = None) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "field": field,
    }


def propose_rules_from_analysis(
    analysis_root: Path,
    profile_path: Path | None = None,
    min_confidence: float = 0.7,
    include_needs_review: bool = False,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    base_profile = load_profile(profile_path) if profile_path else None
    proposals_root = analysis_root / "proposals"
    proposals_root.mkdir(parents=True, exist_ok=True)

    accepted_candidates, skipped_reviews = collect_patch_candidates(
        analysis_root=analysis_root,
        min_confidence=min_confidence,
        include_needs_review=include_needs_review,
    )
    merged_profile = merge_patch_candidates(base_profile, accepted_candidates)
    merged_profile.profile_status = "candidate"
    merged_profile.parent_profile = str(profile_path.resolve()) if profile_path else merged_profile.parent_profile

    proposal_payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "analysis_root": str(analysis_root),
        "profile_source": str(profile_path.resolve()) if profile_path else None,
        "min_confidence": min_confidence,
        "include_needs_review": include_needs_review,
        "summary": {
            "accepted_patch_count": len(accepted_candidates),
            "skipped_review_count": len(skipped_reviews),
            "base_rule_count": len(base_profile.rules) if base_profile else 0,
            "candidate_rule_count": len(merged_profile.rules),
        },
        "accepted_patches": accepted_candidates,
        "skipped_reviews": skipped_reviews,
    }

    proposals_json_path = proposals_root / "rule_proposals.json"
    proposals_md_path = proposals_root / "rule_proposals.md"
    candidate_profile_path = proposals_root / "candidate_profile.json"
    candidate_profile_md_path = proposals_root / "candidate_profile.md"

    proposals_json_path.write_text(json.dumps(proposal_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    proposals_md_path.write_text(render_rule_proposals_markdown(proposal_payload), encoding="utf-8")
    candidate_profile_path.write_text(json.dumps(merged_profile.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    candidate_profile_md_path.write_text(render_candidate_profile_markdown(merged_profile, proposal_payload), encoding="utf-8")

    artifacts = [
        artifact_descriptor_for_path(proposals_json_path, "json", "Rule proposals", "proposal"),
        artifact_descriptor_for_path(proposals_md_path, "markdown", "Rule proposals summary", "proposal"),
        artifact_descriptor_for_path(candidate_profile_path, "json", "Candidate profile", "profile"),
        artifact_descriptor_for_path(candidate_profile_md_path, "markdown", "Candidate profile summary", "profile"),
    ]
    evolution_artifacts = write_evolution_report(analysis_root.parent)
    append_artifacts_to_index(analysis_root.parent, artifacts)
    append_artifacts_to_index(analysis_root.parent, evolution_artifacts)
    return {
        "proposal_payload": proposal_payload,
        "candidate_profile": merged_profile,
        "candidate_profile_path": candidate_profile_path,
        "artifacts": artifacts + evolution_artifacts,
    }


def apply_profile_patch_bundle(
    patch_bundle_path: Path,
    output_path: Path,
    profile_path: Path | None = None,
) -> AnalysisProfile:
    payload = json.loads(patch_bundle_path.read_text(encoding="utf-8"))
    accepted_candidates = [item for item in payload.get("accepted_patches", []) if isinstance(item, dict)]
    base_profile = load_profile(profile_path) if profile_path else None
    merged_profile = merge_patch_candidates(base_profile, accepted_candidates)
    merged_profile.profile_status = "candidate"
    merged_profile.parent_profile = str(profile_path.resolve()) if profile_path else merged_profile.parent_profile
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(merged_profile.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return merged_profile


def simulate_candidate_profile(
    input_dir: Path,
    output_dir: Path,
    analysis_root: Path | None = None,
    candidate_profile_path: Path | None = None,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
) -> dict[str, Any]:
    if candidate_profile_path is None:
        if analysis_root is None:
            raise ValueError("Either analysis_root or candidate_profile_path must be provided.")
        analysis_root = resolve_analysis_root(analysis_root)
        candidate_profile_path = analysis_root / "proposals" / "candidate_profile.json"
    if not candidate_profile_path.exists():
        raise ValueError(f"Candidate profile does not exist: {candidate_profile_path}")

    from .validation import validate_profile

    validation_result = validate_profile(
        input_dir=input_dir,
        output_dir=output_dir,
        profile_path=candidate_profile_path,
        entry_file=entry_file,
        entry_main_query=entry_main_query,
    )

    simulation_root = output_dir / "simulation"
    simulation_root.mkdir(parents=True, exist_ok=True)
    simulation_json_path = simulation_root / "profile_simulation.json"
    simulation_md_path = simulation_root / "profile_simulation.md"
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "candidate_profile_path": str(candidate_profile_path.resolve()),
        "assessment": validation_result["assessment"],
        "validation_payload": validation_result["payload"],
    }
    simulation_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    simulation_md_path.write_text(render_profile_simulation_markdown(payload), encoding="utf-8")
    artifacts = [
        artifact_descriptor_for_path(simulation_json_path, "json", "Profile simulation report", "validation"),
        artifact_descriptor_for_path(simulation_md_path, "markdown", "Profile simulation summary", "validation"),
    ]
    return {
        "assessment": validation_result["assessment"],
        "payload": payload,
        "artifacts": validation_result["artifacts"] + artifacts,
    }


def collect_patch_candidates(
    analysis_root: Path,
    min_confidence: float = 0.7,
    include_needs_review: bool = False,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    review_root = resolve_analysis_root(analysis_root) / "llm_reviews"
    if not review_root.exists():
        return [], []

    accepted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for review_path in sorted(review_root.glob("*-review.json")):
        review = json.loads(review_path.read_text(encoding="utf-8"))
        review_stage = review.get("stage")
        review_status = review.get("status")
        safe_candidate = bool(review.get("safe_to_apply_candidate"))
        patch_candidate = review.get("profile_patch_candidate")

        if review_stage != "propose":
            skipped.append(build_skipped_review(review_path, review, "stage_not_propose"))
            continue
        if review_status != "accepted":
            if include_needs_review and review_status == "needs_revision":
                skipped.append(build_skipped_review(review_path, review, "needs_revision_included_for_manual_review"))
            else:
                skipped.append(build_skipped_review(review_path, review, f"status_{review_status}"))
            continue
        if not safe_candidate or not isinstance(patch_candidate, dict):
            skipped.append(build_skipped_review(review_path, review, "not_safe_or_missing_patch_candidate"))
            continue

        confidence_score = float(patch_candidate.get("confidence_score", 0.0))
        if confidence_score < min_confidence:
            skipped.append(build_skipped_review(review_path, review, "below_min_confidence"))
            continue

        dedupe_key = json.dumps(
            {
                "rule_type": patch_candidate.get("rule_type"),
                "scope": patch_candidate.get("scope"),
                "proposed_action": patch_candidate.get("proposed_action"),
            },
            sort_keys=True,
            ensure_ascii=False,
        )
        if dedupe_key in seen_keys:
            skipped.append(build_skipped_review(review_path, review, "duplicate_patch_candidate"))
            continue
        seen_keys.add(dedupe_key)

        accepted.append(
            {
                "review_path": str(review_path),
                "cluster_id": review.get("cluster_id"),
                "confidence_score": confidence_score,
                "patch_candidate": patch_candidate,
            }
        )
    accepted.sort(key=lambda item: (-float(item["confidence_score"]), item["cluster_id"] or ""))
    return accepted, skipped


def merge_patch_candidates(
    base_profile: AnalysisProfile | None,
    accepted_candidates: list[dict[str, Any]],
) -> AnalysisProfile:
    merged = clone_profile(base_profile)
    merged.generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    if base_profile is not None:
        merged.profile_version = base_profile.profile_version + 1

    existing_rule_keys = {
        canonical_rule_key(rule.rule_type, rule.proposed_action)
        for rule in merged.rules
    }
    for item in accepted_candidates:
        patch_candidate = item.get("patch_candidate", {})
        rule_type = str(patch_candidate.get("rule_type", "")).strip()
        proposed_action = patch_candidate.get("proposed_action", {})
        if not rule_type or not isinstance(proposed_action, dict):
            continue

        apply_patch_action(merged, rule_type, proposed_action)

        rule_key = canonical_rule_key(rule_type, proposed_action)
        if rule_key in existing_rule_keys:
            continue
        existing_rule_keys.add(rule_key)
        merged.rules.append(
            ProfileRule(
                rule_id=str(patch_candidate.get("rule_id", f"candidate-{sanitize_token(rule_type)}")),
                rule_type=rule_type,
                description=str(patch_candidate.get("description", f"Candidate {rule_type} rule")),
                confidence=float(patch_candidate.get("confidence_score", 0.7)),
                evidence=dict(patch_candidate.get("evidence", {})),
                proposed_action=proposed_action,
            )
        )

    merged.reference_token_patterns = dedupe_strings(merged.reference_token_patterns, seed=["{name}"])
    merged.ignore_tags = dedupe_strings(merged.ignore_tags)
    return merged


def build_skipped_review(review_path: Path, review: dict[str, Any], reason: str) -> dict[str, Any]:
    return {
        "review_path": str(review_path),
        "cluster_id": review.get("cluster_id"),
        "stage": review.get("stage"),
        "status": review.get("status"),
        "reason": reason,
    }


def clone_profile(profile: AnalysisProfile | None) -> AnalysisProfile:
    if profile is None:
        return AnalysisProfile(generated_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat())
    return AnalysisProfile.from_dict(profile.to_dict())


def canonical_rule_key(rule_type: str, proposed_action: dict[str, Any]) -> str:
    return json.dumps(
        {
            "rule_type": rule_type,
            "proposed_action": proposed_action,
        },
        sort_keys=True,
        ensure_ascii=False,
    )


def apply_patch_action(profile: AnalysisProfile, rule_type: str, action: dict[str, Any]) -> None:
    if rule_type == "external_xml_name_mapping":
        profile.external_xml_name_map[str(action["xml_name"])] = str(action["mapped_to"])
    elif rule_type == "external_xml_scoped_mapping":
        scoped_key = f"{action['source_dir']}::{action['xml_name']}"
        profile.external_xml_scoped_map[scoped_key] = str(action["mapped_to"])
    elif rule_type == "reference_token_pattern":
        pattern = str(action["pattern"])
        if "{name}" in pattern:
            profile.reference_token_patterns.append(pattern)
    elif rule_type == "reference_target_default_order":
        profile.reference_target_default_order = [
            item for item in action.get("reference_target_default_order", []) if item in {"sub", "main"}
        ] or ["sub", "main"]
    elif rule_type == "ignore_tag":
        profile.ignore_tags.append(str(action["tag"]))


def dedupe_strings(values: list[str], seed: list[str] | None = None) -> list[str]:
    ordered = list(seed or [])
    seen = set(ordered)
    for value in values:
        if value not in seen:
            seen.add(value)
            ordered.append(value)
    return ordered


def render_rule_proposals_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Rule Proposals",
        "",
        "## Summary",
        f"- Accepted patches: {payload['summary']['accepted_patch_count']}",
        f"- Skipped reviews: {payload['summary']['skipped_review_count']}",
        f"- Base rules: {payload['summary']['base_rule_count']}",
        f"- Candidate rules: {payload['summary']['candidate_rule_count']}",
        "",
        "## Accepted Patches",
    ]
    accepted = payload.get("accepted_patches", [])
    if accepted:
        for item in accepted:
            patch = item["patch_candidate"]
            lines.append(
                f"- `{patch['rule_type']}` cluster={item['cluster_id']} confidence={item['confidence_score']:.2f}"
            )
            lines.append(f"  action: {json.dumps(patch['proposed_action'], ensure_ascii=False)}")
    else:
        lines.append("- None")

    lines.extend(["", "## Skipped Reviews"])
    skipped = payload.get("skipped_reviews", [])
    if skipped:
        for item in skipped[:20]:
            lines.append(
                f"- cluster={item.get('cluster_id') or 'n/a'} stage={item.get('stage') or 'n/a'} "
                f"status={item.get('status') or 'n/a'} reason={item['reason']}"
            )
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def render_candidate_profile_markdown(profile: AnalysisProfile, payload: dict[str, Any]) -> str:
    lines = [
        "# Candidate Profile",
        "",
        "## Summary",
        f"- Profile version: {profile.profile_version}",
        f"- Rules: {len(profile.rules)}",
        f"- Reference target order: {', '.join(profile.reference_target_default_order)}",
        f"- Token patterns: {', '.join(profile.reference_token_patterns)}",
        f"- External XML aliases: {len(profile.external_xml_name_map)}",
        f"- Scoped XML aliases: {len(profile.external_xml_scoped_map)}",
        f"- Ignore tags: {', '.join(profile.ignore_tags) or 'none'}",
        "",
        "## Accepted Patches Applied",
    ]
    accepted = payload.get("accepted_patches", [])
    if accepted:
        for item in accepted:
            patch = item["patch_candidate"]
            lines.append(f"- `{patch['rule_type']}`: {json.dumps(patch['proposed_action'], ensure_ascii=False)}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"


def render_profile_simulation_markdown(payload: dict[str, Any]) -> str:
    assessment = payload["assessment"]
    validation_payload = payload["validation_payload"]
    lines = [
        "# Profile Simulation",
        "",
        "## Summary",
        f"- Candidate profile: `{payload['candidate_profile_path']}`",
        f"- Classification: `{assessment['classification']}`",
        f"- Recommendation: {assessment['recommendation']}",
        "",
        "## Improvements",
    ]
    improvements = assessment.get("improvements", [])
    if improvements:
        for item in improvements:
            lines.append(f"- {item}")
    else:
        lines.append("- None")

    lines.extend(["", "## Delta Snapshot"])
    delta = validation_payload.get("delta", {})
    lines.append(f"- Resolved query delta: {delta.get('resolved_queries_delta', 0):+d}")
    lines.append(f"- Failed query delta: {delta.get('failed_queries_delta', 0):+d}")
    lines.append(f"- Error delta: {delta.get('error_delta', 0):+d}")
    lines.append(f"- Warning delta: {delta.get('warning_delta', 0):+d}")
    return "\n".join(lines).rstrip() + "\n"
