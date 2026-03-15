from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .prompt_profiles import phase_budget_for
from .schemas import LoopConfig, LoopState, PhaseTask, required_artifacts_for_company_mode


PHASE_ORDER = ["scan", "classify", "propose", "verify", "simulate", "grade", "package"]
CLUSTER_PHASES = ["classify", "propose", "verify"]


def build_initial_state(config: LoopConfig) -> LoopState:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return LoopState(
        run_id=f"{timestamp}-{config.runner_mode}",
        status="running",
        current_phase="scan",
        required_artifacts=required_artifacts_for_company_mode() if config.company_mode else [],
        token_budget=phase_budget_for(config.prompt_profile, "propose"),
        config=config.to_dict(),
    )


def select_next_phase(state: LoopState) -> str | None:
    if "scan" not in state.completed_phases:
        return "scan"

    for cluster_id in state.pending_clusters:
        completed = state.completed_cluster_stages.get(cluster_id, [])
        for phase in CLUSTER_PHASES:
            if phase not in completed:
                return phase

    for phase in ["simulate", "grade", "package"]:
        if phase not in state.completed_phases:
            return phase
    return None


def next_cluster_for_phase(state: LoopState, phase: str) -> str | None:
    if phase not in CLUSTER_PHASES:
        return None
    for cluster_id in state.pending_clusters:
        if phase not in state.completed_cluster_stages.get(cluster_id, []):
            return cluster_id
    return None


def build_phase_task(state: LoopState, config: LoopConfig, analysis_root: Path) -> PhaseTask:
    phase = state.current_phase
    if phase is None:
        raise ValueError("Cannot build a phase task without current_phase.")
    cluster_id = next_cluster_for_phase(state, phase)
    if phase not in CLUSTER_PHASES or cluster_id is None:
        raise ValueError(f"Phase {phase} does not require an external runner task.")

    attempt = state.cluster_attempts.get(cluster_id, {}).get(phase, 0) + 1
    expected_schema = f"{phase}_response_v1"
    input_pack_path = analysis_root / "context_packs" / f"{cluster_id}-{phase}.json"
    budget = phase_budget_for(config.prompt_profile, phase)
    return PhaseTask(
        task_id=f"{phase}-{cluster_id}-{attempt:03d}",
        phase=phase,
        cluster_id=cluster_id,
        query_id=None,
        runner_mode=config.runner_mode,
        model_profile=config.prompt_profile,
        input_pack_path=input_pack_path,
        expected_schema=expected_schema,
        attempt=attempt,
        max_attempts=config.max_attempts_per_task,
        token_budget=budget,
    )


def apply_phase_status(
    state: LoopState,
    phase: str,
    cluster_id: str | None = None,
    artifacts: list[str] | None = None,
    latest_output: dict[str, Any] | None = None,
) -> LoopState:
    if phase not in state.completed_phases and phase in PHASE_ORDER and phase not in CLUSTER_PHASES:
        state.completed_phases.append(phase)
    if cluster_id is not None and phase in CLUSTER_PHASES:
        completed = state.completed_cluster_stages.setdefault(cluster_id, [])
        if phase not in completed:
            completed.append(phase)
    if artifacts:
        for artifact in artifacts:
            mark_artifact_completion(state, artifact)
    if latest_output is not None:
        if cluster_id is not None:
            state.latest_outputs[cluster_id] = latest_output
        else:
            state.latest_outputs[phase] = latest_output
    state.current_phase = select_next_phase(state)
    state.active_task_id = None
    return state


def register_task_attempt(state: LoopState, cluster_id: str, phase: str) -> None:
    attempts = state.cluster_attempts.setdefault(cluster_id, {})
    attempts[phase] = attempts.get(phase, 0) + 1


def should_stop(state: LoopState, output_dir: Path, max_iterations: int) -> tuple[bool, str | None]:
    refresh_completed_artifacts(state, output_dir)
    if state.required_artifacts and all(artifact in state.completed_artifacts for artifact in state.required_artifacts):
        return True, "all_required_artifacts_completed"
    if state.iteration_count >= max_iterations:
        return True, "max_iterations_reached"
    if state.stop_reason:
        return True, state.stop_reason
    if state.current_phase is None:
        return True, "phase_sequence_exhausted"
    return False, None


def refresh_completed_artifacts(state: LoopState, output_dir: Path) -> None:
    for artifact in state.required_artifacts:
        path = output_dir / artifact
        if path.exists():
            mark_artifact_completion(state, artifact)


def mark_artifact_completion(state: LoopState, artifact: str) -> None:
    if artifact not in state.completed_artifacts:
        state.completed_artifacts.append(artifact)
