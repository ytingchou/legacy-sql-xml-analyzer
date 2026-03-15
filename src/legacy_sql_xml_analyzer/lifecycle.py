from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .learning import AnalysisProfile, load_profile
from .prompting import artifact_descriptor_for_path


def grade_profile(
    profile_path: Path,
    validation_report_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    profile = load_profile(profile_path)
    if profile is None:
        raise ValueError(f"Could not load profile from {profile_path}")

    validation_payload = load_validation_payload(validation_report_path)
    grade_payload = build_grade_payload(profile, profile_path, validation_report_path, validation_payload)

    grade_root = output_dir / "grade"
    grade_root.mkdir(parents=True, exist_ok=True)
    grade_json_path = grade_root / "profile_grade.json"
    grade_md_path = grade_root / "profile_grade.md"
    grade_json_path.write_text(json.dumps(grade_payload, indent=2, ensure_ascii=False), encoding="utf-8")
    grade_md_path.write_text(render_profile_grade_markdown(grade_payload), encoding="utf-8")
    return {
        "grade_payload": grade_payload,
        "artifacts": [
            artifact_descriptor_for_path(grade_json_path, "json", "Profile grade", "validation"),
            artifact_descriptor_for_path(grade_md_path, "markdown", "Profile grade summary", "validation"),
        ],
    }


def promote_profile(
    profile_path: Path,
    grade_report_path: Path,
    output_path: Path,
    profile_name: str | None = None,
) -> AnalysisProfile:
    profile = load_profile(profile_path)
    if profile is None:
        raise ValueError(f"Could not load profile from {profile_path}")
    grade_payload = load_grade_payload(grade_report_path)

    promoted = AnalysisProfile.from_dict(profile.to_dict())
    promoted.profile_version += 1
    promoted.generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    promoted.profile_status = str(grade_payload.get("suggested_status", promoted.profile_status))
    promoted.profile_name = profile_name or promoted.profile_name or output_path.stem
    promoted.parent_profile = str(profile_path.resolve())
    promoted.validation_history.append(
        {
            "generated_at": grade_payload["generated_at"],
            "assessment_classification": grade_payload["assessment"]["classification"],
            "suggested_status": grade_payload["suggested_status"],
            "promotion_readiness": grade_payload["promotion_readiness"],
            "validation_report_path": grade_payload["validation_report_path"],
            "resolved_queries_delta": grade_payload["latest_delta"].get("resolved_queries_delta", 0),
            "failed_queries_delta": grade_payload["latest_delta"].get("failed_queries_delta", 0),
            "error_delta": grade_payload["latest_delta"].get("error_delta", 0),
            "warning_delta": grade_payload["latest_delta"].get("warning_delta", 0),
        }
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(promoted.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return promoted


def build_grade_payload(
    profile: AnalysisProfile,
    profile_path: Path,
    validation_report_path: Path,
    validation_payload: dict[str, Any],
) -> dict[str, Any]:
    assessment = validation_payload["assessment"]
    current_status = profile.profile_status
    history_counts = Counter(
        str(item.get("assessment_classification", "")).strip().lower()
        for item in profile.validation_history
    )
    latest_classification = str(assessment.get("classification", "stable")).strip().lower()
    suggested_status, promotion_readiness, reasoning = classify_profile_lifecycle(
        current_status=current_status,
        latest_classification=latest_classification,
        history_counts=history_counts,
    )
    latest_delta = validation_payload.get("delta", {})
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "profile_path": str(profile_path.resolve()),
        "validation_report_path": str(validation_report_path.resolve()),
        "profile_name": profile.profile_name,
        "current_status": current_status,
        "suggested_status": suggested_status,
        "promotion_readiness": promotion_readiness,
        "assessment": assessment,
        "history_summary": {
            "total": len(profile.validation_history),
            "improved": history_counts.get("improved", 0),
            "stable": history_counts.get("stable", 0),
            "review": history_counts.get("review", 0),
            "regressed": history_counts.get("regressed", 0),
        },
        "latest_delta": {
            "resolved_queries_delta": latest_delta.get("resolved_queries_delta", 0),
            "failed_queries_delta": latest_delta.get("failed_queries_delta", 0),
            "error_delta": latest_delta.get("error_delta", 0),
            "warning_delta": latest_delta.get("warning_delta", 0),
        },
        "reasoning": reasoning,
    }


def classify_profile_lifecycle(
    current_status: str,
    latest_classification: str,
    history_counts: Counter[str],
) -> tuple[str, str, list[str]]:
    reasoning: list[str] = []
    improved_runs = history_counts.get("improved", 0) + (1 if latest_classification == "improved" else 0)
    regressed_runs = history_counts.get("regressed", 0)

    if latest_classification == "regressed":
        reasoning.append("Latest validation regressed, so this profile should not continue in active use.")
        return "deprecated", "reject", reasoning

    if latest_classification == "review":
        reasoning.append("Latest validation increased warning risk or needs manual review.")
        if current_status == "trusted":
            reasoning.append("Trusted profiles fall back to trial when new review risk appears.")
            return "trial", "review", reasoning
        return current_status if current_status != "deprecated" else "candidate", "review", reasoning

    if latest_classification == "stable":
        reasoning.append("Latest validation is stable but does not yet justify a stronger promotion.")
        if current_status == "deprecated":
            return "candidate", "review", reasoning
        return current_status, "hold", reasoning

    if latest_classification == "improved":
        reasoning.append("Latest validation improved resolved coverage or reduced failures.")
        if current_status == "candidate":
            reasoning.append("A candidate with a fresh improvement is ready to become trial.")
            return "trial", "promote", reasoning
        if current_status == "trial":
            if improved_runs >= 2 and regressed_runs == 0:
                reasoning.append("The profile has multiple improvements without regressions, so it can be trusted.")
                return "trusted", "promote", reasoning
            reasoning.append("Trial status is retained until it accumulates more successful validations.")
            return "trial", "hold", reasoning
        if current_status == "trusted":
            reasoning.append("Trusted status is retained because the latest run is still improving.")
            return "trusted", "hold", reasoning
        reasoning.append("A deprecated profile needs manual review before it can return to active use.")
        return "trial", "review", reasoning

    reasoning.append("Unknown assessment classification; keep the profile in candidate status for manual review.")
    return "candidate", "review", reasoning


def load_validation_payload(path: Path) -> dict[str, Any]:
    resolved_path = resolve_json_report_path(path)
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if "validation_payload" in payload and isinstance(payload["validation_payload"], dict):
        return payload["validation_payload"]
    return payload


def load_grade_payload(path: Path) -> dict[str, Any]:
    resolved_path = resolve_json_report_path(path)
    return json.loads(resolved_path.read_text(encoding="utf-8"))


def resolve_json_report_path(path: Path) -> Path:
    path = path.resolve()
    if path.is_file():
        return path
    candidates = [
        path / "grade" / "profile_grade.json",
        path / "validation" / "profile_validation.json",
        path / "simulation" / "profile_simulation.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not resolve a JSON report under {path}")


def render_profile_grade_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Profile Grade",
        "",
        "## Summary",
        f"- Current status: `{payload['current_status']}`",
        f"- Suggested status: `{payload['suggested_status']}`",
        f"- Promotion readiness: `{payload['promotion_readiness']}`",
        f"- Assessment: `{payload['assessment']['classification']}`",
        "",
        "## History Summary",
        f"- Improved: {payload['history_summary']['improved']}",
        f"- Stable: {payload['history_summary']['stable']}",
        f"- Review: {payload['history_summary']['review']}",
        f"- Regressed: {payload['history_summary']['regressed']}",
        "",
        "## Latest Delta",
        f"- Resolved query delta: {payload['latest_delta']['resolved_queries_delta']:+d}",
        f"- Failed query delta: {payload['latest_delta']['failed_queries_delta']:+d}",
        f"- Error delta: {payload['latest_delta']['error_delta']:+d}",
        f"- Warning delta: {payload['latest_delta']['warning_delta']:+d}",
        "",
        "## Reasoning",
    ]
    for item in payload["reasoning"]:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"
