from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .context_compiler import (
    build_sections,
    collect_included_artifacts,
    estimate_tokens,
    render_context_prompt,
    trim_sections_for_budget,
)
from .java_bff import safe_name
from .java_bff_context import compile_java_bff_context_pack
from .prompt_profiles import phase_example_limit_for
from .prompting import load_failure_clusters, resolve_analysis_root


DEFAULT_TARGETS = [8000, 16000, 24000, 48000]


def compile_adaptive_generic_context(
    analysis_root: Path,
    *,
    cluster_id: str,
    phase: str,
    prompt_profile: str,
    targets: list[int] | None = None,
    prior_response: dict[str, Any] | None = None,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    payload = load_failure_clusters(analysis_root)
    cluster = next(item for item in payload["clusters"] if item["cluster_id"] == cluster_id)
    example_limit = phase_example_limit_for(prompt_profile, phase)
    sections = build_sections(analysis_root, cluster, phase, example_limit, prior_response)
    variants = []
    for target in targets or DEFAULT_TARGETS:
        selected_sections = trim_sections_for_budget(sections, target)
        prompt_text = render_context_prompt(cluster, phase, prompt_profile, selected_sections, prior_response)
        variants.append(
            {
                "target_tokens": target,
                "estimated_tokens": estimate_tokens(prompt_text),
                "included_artifacts": collect_included_artifacts(selected_sections),
                "sections": selected_sections,
                "prompt_text": prompt_text,
                "safe": estimate_tokens(prompt_text) <= target,
            }
        )
    return {
        "generated_at": timestamp_now(),
        "kind": "generic_adaptive_context",
        "analysis_root": str(analysis_root.resolve()),
        "cluster_id": cluster_id,
        "phase": phase,
        "prompt_profile": prompt_profile,
        "variants": variants,
    }


def compile_adaptive_java_context(
    analysis_root: Path,
    *,
    prompt_json: Path,
    prompt_profile: str | None = None,
    targets: list[int] | None = None,
) -> dict[str, Any]:
    analysis_root = resolve_analysis_root(analysis_root)
    variants = []
    for target in targets or DEFAULT_TARGETS:
        pack = compile_java_bff_context_pack(
            analysis_root=analysis_root,
            phase_pack_path=prompt_json.resolve(),
            prompt_profile=prompt_profile,
            max_input_tokens=target,
        )
        variants.append(pack)
    return {
        "generated_at": timestamp_now(),
        "kind": "java_adaptive_context",
        "analysis_root": str(analysis_root.resolve()),
        "prompt_json": str(prompt_json.resolve()),
        "prompt_profile": prompt_profile,
        "variants": variants,
    }


def shrink_prompt_text(prompt_text: str, target_tokens: int) -> dict[str, Any]:
    lines = [line for line in prompt_text.splitlines()]
    original_tokens = estimate_tokens(prompt_text)
    if original_tokens <= target_tokens:
        return {
            "target_tokens": target_tokens,
            "estimated_tokens": original_tokens,
            "prompt_text": prompt_text,
            "strategy": ["already_within_budget"],
        }

    strategy: list[str] = []
    kept: list[str] = []
    reserved_tail = extract_tail(lines)
    body = lines[: len(lines) - len(reserved_tail)] if reserved_tail else list(lines)
    current = ""
    for line in body:
        trial = "\n".join(kept + [line] + [""] + reserved_tail).strip() + "\n"
        if estimate_tokens(trial) > target_tokens:
            strategy.append("trimmed_body_lines")
            break
        kept.append(line)
        current = trial
    if not current:
        current = "\n".join((kept + [""] + reserved_tail) if reserved_tail else kept).strip() + "\n"
    return {
        "target_tokens": target_tokens,
        "estimated_tokens": estimate_tokens(current),
        "prompt_text": current,
        "strategy": strategy or ["line_trim"],
    }


def plan_prompt_downgrade(
    current_tokens: int | None,
    *,
    targets: list[int] | None = None,
    max_candidates: int = 3,
) -> dict[str, Any]:
    ordered_targets = sorted({int(item) for item in (targets or DEFAULT_TARGETS) if int(item) > 0}, reverse=True)
    if not ordered_targets:
        ordered_targets = sorted(DEFAULT_TARGETS, reverse=True)
    baseline = int(current_tokens or 0)
    recommended: list[int] = []
    if baseline > 0:
        recommended = [target for target in ordered_targets if target < baseline]
    if not recommended:
        recommended = list(reversed(sorted(ordered_targets)))[:max_candidates]
    recommended = recommended[:max_candidates]
    return {
        "current_tokens": baseline,
        "candidate_targets": recommended,
        "recommended_target": recommended[0] if recommended else None,
    }


def write_adaptive_payload(output_root: Path, payload: dict[str, Any]) -> list[Path]:
    analysis_root = resolve_output_analysis_root(output_root)
    root = analysis_root / "adaptive_prompts"
    root.mkdir(parents=True, exist_ok=True)
    if payload["kind"] == "generic_adaptive_context":
        base = f"{safe_name(payload['cluster_id'])}-{safe_name(payload['phase'])}"
    elif payload["kind"] == "shrunk_prompt":
        source_pack = Path(str(payload.get("source_pack") or "shrunk-prompt"))
        base = safe_name(source_pack.stem)
    else:
        base = safe_name(Path(str(payload["prompt_json"])).stem)
    json_path = root / f"{base}.adaptive.json"
    md_path = root / f"{base}.adaptive.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_adaptive_markdown(payload), encoding="utf-8")
    variant_paths: list[Path] = [json_path, md_path]
    for variant in payload["variants"]:
        target = int(variant.get("target_tokens") or variant.get("budget", {}).get("usable_input_limit", 0) or 0)
        if target <= 0:
            continue
        txt_path = root / f"{base}-{target}.txt"
        txt_path.write_text(str(variant["prompt_text"]), encoding="utf-8")
        variant_paths.append(txt_path)
    return variant_paths


def extract_tail(lines: list[str]) -> list[str]:
    for index, line in enumerate(lines):
        if line.strip().startswith("Return JSON only"):
            return lines[index:]
    return lines[-12:] if len(lines) > 12 else list(lines)


def render_adaptive_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Adaptive Prompt Variants",
        "",
        f"- Kind: `{payload['kind']}`",
        f"- Generated at: `{payload['generated_at']}`",
        "",
        "## Variants",
    ]
    for variant in payload["variants"]:
        target = int(variant.get("target_tokens") or variant.get("budget", {}).get("usable_input_limit", 0) or 0)
        estimated = int(variant.get("estimated_tokens") or variant.get("estimated_prompt_tokens", 0) or 0)
        lines.append(f"- target={target} estimated={estimated} safe=`{variant.get('safe', variant.get('safe_for_qwen3'))}`")
    return "\n".join(lines).rstrip() + "\n"


def resolve_output_analysis_root(path: Path) -> Path:
    resolved = path.resolve()
    if resolved.name == "analysis":
        return resolved
    for candidate in [resolved, *resolved.parents]:
        if candidate.name == "analysis":
            return candidate
        if (candidate / "analysis").exists():
            return candidate / "analysis"
    return resolve_analysis_root(resolved)


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
