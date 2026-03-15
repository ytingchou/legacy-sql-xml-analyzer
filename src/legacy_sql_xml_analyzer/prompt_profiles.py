from __future__ import annotations

from typing import Any


PROFILES: dict[str, dict[str, Any]] = {
    "qwen3-128k-classify": {
        "model_limit": 128000,
        "reserved_output": 12000,
        "reserved_system": 4000,
        "reserved_repair": 8000,
        "stage_examples": {"classify": 3},
        "phase_limits": {"classify": 16000},
    },
    "qwen3-128k-propose": {
        "model_limit": 128000,
        "reserved_output": 12000,
        "reserved_system": 4000,
        "reserved_repair": 8000,
        "stage_examples": {"propose": 2},
        "phase_limits": {"propose": 24000},
    },
    "qwen3-128k-verify": {
        "model_limit": 128000,
        "reserved_output": 12000,
        "reserved_system": 4000,
        "reserved_repair": 8000,
        "stage_examples": {"verify": 1},
        "phase_limits": {"verify": 16000},
    },
    "qwen3-128k-autonomous": {
        "model_limit": 128000,
        "reserved_output": 12000,
        "reserved_system": 4000,
        "reserved_repair": 8000,
        "stage_examples": {
            "classify": 3,
            "propose": 2,
            "verify": 1,
        },
        "phase_limits": {
            "classify": 16000,
            "propose": 24000,
            "verify": 16000,
            "package": 12000,
        },
    },
    "qwen3-128k-java-bff": {
        "model_limit": 128000,
        "reserved_output": 12000,
        "reserved_system": 4000,
        "reserved_repair": 8000,
        "stage_examples": {
            "phase-1-plan": 8,
            "phase-2-repository-chunk": 1,
            "phase-2-repository-merge": 1,
            "phase-3-bff-assembly": 8,
            "phase-4-verify": 8,
        },
        "phase_limits": {
            "phase-1-plan": 18000,
            "phase-2-repository-chunk": 22000,
            "phase-2-repository-merge": 12000,
            "phase-3-bff-assembly": 18000,
            "phase-4-verify": 14000,
        },
    },
}


def get_prompt_profile(name: str) -> dict[str, Any]:
    try:
        return PROFILES[name]
    except KeyError as exc:
        raise ValueError(f"Unknown prompt profile: {name}") from exc


def phase_budget_for(profile_name: str, phase: str) -> dict[str, int]:
    profile = get_prompt_profile(profile_name)
    return {
        "model_limit": int(profile["model_limit"]),
        "usable_input_limit": int(profile.get("phase_limits", {}).get(phase, 12000)),
        "reserved_output": int(profile["reserved_output"]),
        "reserved_system": int(profile["reserved_system"]),
        "reserved_repair": int(profile["reserved_repair"]),
    }


def phase_example_limit_for(profile_name: str, phase: str) -> int:
    profile = get_prompt_profile(profile_name)
    return int(profile.get("stage_examples", {}).get(phase, 1))
