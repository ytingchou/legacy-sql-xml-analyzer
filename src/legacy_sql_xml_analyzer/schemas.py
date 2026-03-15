from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal


PhaseName = Literal["scan", "classify", "propose", "verify", "simulate", "grade", "package"]
LoopStatus = Literal["running", "completed", "stopped", "failed"]
RunnerMode = Literal["provider", "cline_bridge", "fake"]


def required_artifacts_for_company_mode() -> list[str]:
    return [
        "analysis/index.json",
        "analysis/failure_clusters.json",
        "analysis/context_packs",
        "analysis/llm_reviews",
        "analysis/proposals/rule_proposals.json",
        "analysis/proposals/candidate_profile.json",
        "simulation/profile_simulation.json",
        "grade/profile_grade.json",
        "analysis/executive_summary.json",
    ]


@dataclass(slots=True)
class LoopConfig:
    input_dir: Path
    output_dir: Path
    profile_path: Path | None = None
    runner_mode: RunnerMode = "provider"
    prompt_profile: str = "qwen3-128k-autonomous"
    max_iterations: int = 20
    max_attempts_per_task: int = 3
    company_mode: bool = True
    autonomous: bool = True
    provider_config_path: Path | None = None
    provider_base_url: str | None = None
    provider_api_key: str | None = None
    provider_api_key_env: str = "OPENAI_API_KEY"
    provider_model: str | None = None
    provider_name: str | None = None
    token_limit: int | None = None
    temperature: float | None = None
    timeout_seconds: float | None = None
    cline_bridge_command: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["input_dir"] = str(self.input_dir)
        payload["output_dir"] = str(self.output_dir)
        payload["profile_path"] = str(self.profile_path) if self.profile_path else None
        payload["provider_config_path"] = str(self.provider_config_path) if self.provider_config_path else None
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LoopConfig":
        return cls(
            input_dir=Path(str(payload["input_dir"])),
            output_dir=Path(str(payload["output_dir"])),
            profile_path=Path(str(payload["profile_path"])) if payload.get("profile_path") else None,
            runner_mode=str(payload.get("runner_mode", "provider")),  # type: ignore[arg-type]
            prompt_profile=str(payload.get("prompt_profile", "qwen3-128k-autonomous")),
            max_iterations=int(payload.get("max_iterations", 20)),
            max_attempts_per_task=int(payload.get("max_attempts_per_task", 3)),
            company_mode=bool(payload.get("company_mode", True)),
            autonomous=bool(payload.get("autonomous", True)),
            provider_config_path=Path(str(payload["provider_config_path"])) if payload.get("provider_config_path") else None,
            provider_base_url=payload.get("provider_base_url"),
            provider_api_key=payload.get("provider_api_key"),
            provider_api_key_env=str(payload.get("provider_api_key_env", "OPENAI_API_KEY")),
            provider_model=payload.get("provider_model"),
            provider_name=payload.get("provider_name"),
            token_limit=int(payload["token_limit"]) if payload.get("token_limit") is not None else None,
            temperature=float(payload["temperature"]) if payload.get("temperature") is not None else None,
            timeout_seconds=float(payload["timeout_seconds"]) if payload.get("timeout_seconds") is not None else None,
            cline_bridge_command=payload.get("cline_bridge_command"),
        )


@dataclass(slots=True)
class LoopState:
    run_id: str
    status: LoopStatus
    current_phase: PhaseName | None
    completed_phases: list[PhaseName] = field(default_factory=list)
    pending_clusters: list[str] = field(default_factory=list)
    active_task_id: str | None = None
    iteration_count: int = 0
    required_artifacts: list[str] = field(default_factory=list)
    completed_artifacts: list[str] = field(default_factory=list)
    stop_reason: str | None = None
    last_error: dict[str, Any] | None = None
    token_budget: dict[str, int] = field(default_factory=dict)
    config: dict[str, Any] = field(default_factory=dict)
    completed_cluster_stages: dict[str, list[str]] = field(default_factory=dict)
    cluster_attempts: dict[str, dict[str, int]] = field(default_factory=dict)
    latest_outputs: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "LoopState":
        return cls(
            run_id=str(payload["run_id"]),
            status=str(payload.get("status", "running")),  # type: ignore[arg-type]
            current_phase=payload.get("current_phase"),
            completed_phases=[item for item in payload.get("completed_phases", []) if isinstance(item, str)],
            pending_clusters=[item for item in payload.get("pending_clusters", []) if isinstance(item, str)],
            active_task_id=payload.get("active_task_id"),
            iteration_count=int(payload.get("iteration_count", 0)),
            required_artifacts=[item for item in payload.get("required_artifacts", []) if isinstance(item, str)],
            completed_artifacts=[item for item in payload.get("completed_artifacts", []) if isinstance(item, str)],
            stop_reason=payload.get("stop_reason"),
            last_error=payload.get("last_error") if isinstance(payload.get("last_error"), dict) else None,
            token_budget={str(key): int(value) for key, value in payload.get("token_budget", {}).items()},
            config=payload.get("config", {}) if isinstance(payload.get("config"), dict) else {},
            completed_cluster_stages={
                str(key): [item for item in value if isinstance(item, str)]
                for key, value in payload.get("completed_cluster_stages", {}).items()
                if isinstance(value, list)
            },
            cluster_attempts={
                str(key): {str(k): int(v) for k, v in value.items()}
                for key, value in payload.get("cluster_attempts", {}).items()
                if isinstance(value, dict)
            },
            latest_outputs=payload.get("latest_outputs", {}) if isinstance(payload.get("latest_outputs"), dict) else {},
        )


@dataclass(slots=True)
class PhaseTask:
    task_id: str
    phase: PhaseName
    cluster_id: str | None
    query_id: str | None
    runner_mode: RunnerMode
    model_profile: str
    input_pack_path: Path
    expected_schema: str
    attempt: int
    max_attempts: int
    token_budget: dict[str, int]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["input_pack_path"] = str(self.input_pack_path)
        return payload


@dataclass(slots=True)
class PhaseResult:
    task_id: str
    phase: PhaseName
    status: str
    next_action: str
    structured_output: dict[str, Any]
    output_artifacts: list[str]
    diagnostics: list[dict[str, Any]]
    usage: dict[str, Any] = field(default_factory=dict)
    raw_output_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ContextPack:
    cluster_id: str
    phase: PhaseName
    prompt_profile: str
    estimated_tokens: int
    max_input_tokens: int
    included_artifacts: list[str]
    sections: list[dict[str, Any]]
    prompt_text: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
