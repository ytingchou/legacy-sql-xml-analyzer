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
    promoted.lifecycle_history.append(
        build_lifecycle_event(
            event_type="promote",
            from_status=profile.profile_status,
            to_status=promoted.profile_status,
            source_profile_path=profile_path,
            target_profile_path=output_path,
            grade_payload=grade_payload,
        )
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(promoted.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    write_profile_history_sidecars(promoted, output_path)
    return promoted


def rollback_profile(
    profile_path: Path,
    output_path: Path,
    target_profile_path: Path | None = None,
    reason: str | None = None,
    profile_name: str | None = None,
) -> AnalysisProfile:
    current_profile = load_profile(profile_path)
    if current_profile is None:
        raise ValueError(f"Could not load profile from {profile_path}")

    resolved_target_path = target_profile_path
    if resolved_target_path is None:
        parent = str(current_profile.parent_profile or "").strip()
        if not parent:
            raise ValueError("Current profile does not declare parent_profile, so rollback needs --target-profile.")
        resolved_target_path = Path(parent)
    if not resolved_target_path.exists():
        raise FileNotFoundError(f"Rollback target profile does not exist: {resolved_target_path}")

    target_profile = load_profile(resolved_target_path)
    if target_profile is None:
        raise ValueError(f"Could not load rollback target profile from {resolved_target_path}")

    rolled_back = AnalysisProfile.from_dict(target_profile.to_dict())
    rolled_back.profile_version = max(current_profile.profile_version, target_profile.profile_version) + 1
    rolled_back.generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rolled_back.profile_name = profile_name or current_profile.profile_name or target_profile.profile_name or output_path.stem
    rolled_back.parent_profile = str(resolved_target_path.resolve())
    rolled_back.lifecycle_history.append(
        build_lifecycle_event(
            event_type="rollback",
            from_status=current_profile.profile_status,
            to_status=rolled_back.profile_status,
            source_profile_path=profile_path,
            target_profile_path=resolved_target_path,
            reason=reason,
        )
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rolled_back.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    write_profile_history_sidecars(rolled_back, output_path)
    return rolled_back


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
    rollback_recommendation = build_rollback_recommendation(profile, latest_classification)
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
        "rollback_recommendation": rollback_recommendation,
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


def build_rollback_recommendation(profile: AnalysisProfile, latest_classification: str) -> dict[str, Any]:
    parent_profile = str(profile.parent_profile or "").strip()
    parent_exists = bool(parent_profile) and Path(parent_profile).exists()
    should_rollback = latest_classification == "regressed" and parent_exists
    reason = None
    if latest_classification == "regressed":
        if parent_exists:
            reason = "Latest validation regressed and the profile has a recoverable parent_profile."
        else:
            reason = "Latest validation regressed, but no readable parent_profile is available for automatic rollback."
    return {
        "should_rollback": should_rollback,
        "target_profile_path": parent_profile or None,
        "target_profile_exists": parent_exists,
        "reason": reason,
    }


def build_lifecycle_event(
    event_type: str,
    from_status: str,
    to_status: str,
    source_profile_path: Path,
    target_profile_path: Path,
    grade_payload: dict[str, Any] | None = None,
    reason: str | None = None,
) -> dict[str, Any]:
    event = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "event_type": event_type,
        "from_status": from_status,
        "to_status": to_status,
        "source_profile_path": str(source_profile_path.resolve()),
        "target_profile_path": str(target_profile_path.resolve()),
    }
    if grade_payload is not None:
        event["grade_report_path"] = grade_payload.get("validation_report_path")
        event["assessment_classification"] = grade_payload.get("assessment", {}).get("classification")
        event["promotion_readiness"] = grade_payload.get("promotion_readiness")
    if reason:
        event["reason"] = reason
    return event


def write_profile_history_sidecars(profile: AnalysisProfile, output_path: Path) -> None:
    history_json_path = output_path.with_name(f"{output_path.stem}.history.json")
    history_md_path = output_path.with_name(f"{output_path.stem}.history.md")
    payload = build_profile_history_payload(profile, output_path)
    history_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    history_md_path.write_text(render_profile_history_markdown(payload), encoding="utf-8")


def build_profile_history_payload(profile: AnalysisProfile, output_path: Path) -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "profile_path": str(output_path.resolve()),
        "profile_name": profile.profile_name,
        "profile_status": profile.profile_status,
        "profile_version": profile.profile_version,
        "parent_profile": profile.parent_profile,
        "validation_history": profile.validation_history,
        "lifecycle_history": profile.lifecycle_history,
    }


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
        "## Rollback Recommendation",
        f"- Should rollback: {payload['rollback_recommendation']['should_rollback']}",
        f"- Target profile: {payload['rollback_recommendation']['target_profile_path'] or 'none'}",
        f"- Reason: {payload['rollback_recommendation']['reason'] or 'none'}",
        "",
        "## Reasoning",
    ]
    for item in payload["reasoning"]:
        lines.append(f"- {item}")
    return "\n".join(lines).rstrip() + "\n"


def render_profile_history_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Profile History",
        "",
        "## Summary",
        f"- Profile: `{payload['profile_name'] or Path(payload['profile_path']).stem}`",
        f"- Status: `{payload['profile_status']}`",
        f"- Version: {payload['profile_version']}",
        f"- Parent profile: {payload['parent_profile'] or 'none'}",
        f"- Validation records: {len(payload['validation_history'])}",
        f"- Lifecycle events: {len(payload['lifecycle_history'])}",
        "",
        "## Lifecycle Events",
    ]
    if payload["lifecycle_history"]:
        for item in payload["lifecycle_history"]:
            lines.append(
                f"- `{item.get('event_type', 'unknown')}` {item.get('from_status', 'n/a')} -> {item.get('to_status', 'n/a')} "
                f"at {item.get('generated_at', 'n/a')}"
            )
            if item.get("reason"):
                lines.append(f"  reason: {item['reason']}")
    else:
        lines.append("- None")

    lines.extend(["", "## Validation History"])
    if payload["validation_history"]:
        for item in payload["validation_history"]:
            lines.append(
                f"- `{item.get('assessment_classification', 'n/a')}` suggested={item.get('suggested_status', 'n/a')} "
                f"readiness={item.get('promotion_readiness', 'n/a')}"
            )
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"
