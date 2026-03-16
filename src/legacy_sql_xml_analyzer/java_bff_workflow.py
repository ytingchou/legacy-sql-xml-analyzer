from __future__ import annotations

from pathlib import Path
from typing import Any

from .java_bff_runtime import (
    invoke_java_bff_phase_pack,
    merge_java_bff_phases as merge_java_bff_phase_reviews,
    review_java_bff_response_from_analysis as review_java_bff_phase_response,
)
from .java_skeletons import generate_java_skeletons


def invoke_java_bff_prompt(
    analysis_root: Path,
    prompt_json_path: Path,
    provider_config_path: Path | None = None,
    provider_base_url: str | None = None,
    provider_api_key: str | None = None,
    provider_api_key_env: str = "OPENAI_API_KEY",
    provider_model: str | None = None,
    provider_name: str | None = None,
    token_limit: int | None = None,
    temperature: float | None = None,
    timeout_seconds: float | None = None,
    review: bool = False,
) -> dict[str, Any]:
    return invoke_java_bff_phase_pack(
        analysis_root=analysis_root,
        phase_pack_path=prompt_json_path,
        provider_config_path=provider_config_path,
        provider_base_url=provider_base_url,
        provider_api_key=provider_api_key,
        provider_api_key_env=provider_api_key_env,
        provider_model=provider_model,
        provider_name=provider_name,
        token_limit=token_limit,
        temperature=temperature,
        timeout_seconds=timeout_seconds,
        review=review,
    )


def review_java_bff_response_from_analysis(
    analysis_root: Path,
    prompt_json_path: Path,
    response_path: Path,
) -> dict[str, Any]:
    return review_java_bff_phase_response(
        analysis_root=analysis_root,
        phase_pack_path=prompt_json_path,
        response_path=response_path,
    )


def merge_java_bff_phases(
    analysis_root: Path,
    bundle_id: str,
) -> dict[str, Any]:
    result = merge_java_bff_phase_reviews(analysis_root=analysis_root, bundle_id=bundle_id)
    return {
        "implementation_plan": result["implementation_plan"],
        "merged_payload": result["merged_payload"],
        "artifacts": result["artifacts"],
        "merged_path": result["merged_path"],
    }


def generate_java_bff_skeleton(
    analysis_root: Path,
    bundle_id: str,
    package_name: str = "com.example.legacybff",
) -> dict[str, Any]:
    result = generate_java_skeletons(
        analysis_root=analysis_root,
        bundle_id=bundle_id,
        base_package=package_name,
    )
    return result


def generate_java_bff_starter(
    analysis_root: Path,
    bundle_id: str,
    package_name: str = "com.example.legacybff",
) -> dict[str, Any]:
    return generate_java_bff_skeleton(
        analysis_root=analysis_root,
        bundle_id=bundle_id,
        package_name=package_name,
    )
