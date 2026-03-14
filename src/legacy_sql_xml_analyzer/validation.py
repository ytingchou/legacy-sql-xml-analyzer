from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .analyzer import (
    AnalyzeOptions,
    Analyzer,
    artifact_descriptor_for_path,
    build_summary_delta,
    render_fix_delta_markdown,
    summarize_analysis_result,
)
from .learning import load_profile


def validate_profile(
    input_dir: Path,
    output_dir: Path,
    profile_path: Path,
    entry_file: str | None = None,
    entry_main_query: str | None = None,
) -> dict[str, Any]:
    profile = load_profile(profile_path)
    if profile is None:
        raise ValueError(f"Could not load profile from {profile_path}")

    baseline = Analyzer(
        AnalyzeOptions(
            input_dir=input_dir,
            output_dir=output_dir,
            entry_file=entry_file,
            entry_main_query=entry_main_query,
            profile=None,
        )
    ).analyze(write_artifacts=False)

    profiled_analyzer = Analyzer(
        AnalyzeOptions(
            input_dir=input_dir,
            output_dir=output_dir,
            entry_file=entry_file,
            entry_main_query=entry_main_query,
            profile=profile,
        )
    )
    profiled = profiled_analyzer.analyze(write_artifacts=False)

    baseline_summary = summarize_analysis_result(baseline)
    profiled_summary = summarize_analysis_result(profiled)
    delta = build_summary_delta(baseline_summary, profiled_summary)
    assessment = classify_profile_delta(delta)

    validation_root = output_dir / "validation"
    validation_root.mkdir(parents=True, exist_ok=True)
    validation_json_path = validation_root / "profile_validation.json"
    validation_md_path = validation_root / "profile_validation.md"

    payload = {
        "profile_path": str(profile_path),
        "profile_source_observation_digest": profile.source_observation_digest,
        "assessment": assessment,
        "baseline": baseline_summary,
        "profiled": profiled_summary,
        "delta": delta,
        "rule_usage": dict(sorted(profiled_analyzer.rule_usage.items())),
    }
    validation_json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    validation_md_path.write_text(render_profile_validation_markdown(payload), encoding="utf-8")

    return {
        "assessment": assessment,
        "artifacts": [
            artifact_descriptor_for_path(validation_json_path, "json", "Profile validation report", "validation"),
            artifact_descriptor_for_path(validation_md_path, "markdown", "Profile validation summary", "validation"),
        ],
        "payload": payload,
    }


def classify_profile_delta(delta: dict[str, Any]) -> dict[str, Any]:
    hard_regressions: list[str] = []
    soft_regressions: list[str] = []
    improvements: list[str] = []

    if delta.get("resolved_queries_delta", 0) < 0:
        hard_regressions.append("Resolved query count decreased.")
    elif delta.get("resolved_queries_delta", 0) > 0:
        improvements.append("Resolved query count increased.")

    if delta.get("failed_queries_delta", 0) > 0:
        hard_regressions.append("Failed query count increased.")
    elif delta.get("failed_queries_delta", 0) < 0:
        improvements.append("Failed query count decreased.")

    if delta.get("error_delta", 0) > 0:
        hard_regressions.append("Error diagnostics increased.")
    elif delta.get("error_delta", 0) < 0:
        improvements.append("Error diagnostics decreased.")

    if delta.get("fatal_delta", 0) > 0:
        hard_regressions.append("Fatal diagnostics increased.")
    elif delta.get("fatal_delta", 0) < 0:
        improvements.append("Fatal diagnostics decreased.")

    if delta.get("warning_delta", 0) > 0:
        soft_regressions.append("Warning diagnostics increased.")
    elif delta.get("warning_delta", 0) < 0:
        improvements.append("Warning diagnostics decreased.")

    if hard_regressions:
        classification = "regressed"
        recommendation = "Do not freeze or adopt this profile yet."
    elif improvements:
        classification = "improved"
        recommendation = "This profile is improving results and is a good candidate for adoption."
    elif soft_regressions:
        classification = "review"
        recommendation = "Review the profile before adoption because warning volume increased."
    else:
        classification = "stable"
        recommendation = "This profile is neutral; keep it only if it improves readability or maintenance."

    return {
        "classification": classification,
        "recommendation": recommendation,
        "hard_regressions": hard_regressions,
        "soft_regressions": soft_regressions,
        "improvements": improvements,
    }


def render_profile_validation_markdown(payload: dict[str, Any]) -> str:
    assessment = payload["assessment"]
    delta = payload["delta"]
    lines = [
        "# Profile Validation",
        "",
        "## Assessment",
        f"- Classification: `{assessment['classification']}`",
        f"- Recommendation: {assessment['recommendation']}",
        "",
        "## Improvements",
    ]
    if assessment["improvements"]:
        for item in assessment["improvements"]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")

    lines.extend(["", "## Hard Regressions"])
    if assessment["hard_regressions"]:
        for item in assessment["hard_regressions"]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")

    lines.extend(["", "## Soft Regressions"])
    if assessment["soft_regressions"]:
        for item in assessment["soft_regressions"]:
            lines.append(f"- {item}")
    else:
        lines.append("- None")

    lines.extend(["", "## Delta Summary"])
    lines.extend(render_fix_delta_markdown({"baseline": payload["baseline"], "profiled": payload["profiled"], "delta": delta}).splitlines()[3:])
    lines.extend(["", "## Rule Usage"])
    rule_usage = payload.get("rule_usage", {})
    if rule_usage:
        for key, count in rule_usage.items():
            lines.append(f"- `{key}`: {count}")
    else:
        lines.append("- None")
    return "\n".join(lines).rstrip() + "\n"
