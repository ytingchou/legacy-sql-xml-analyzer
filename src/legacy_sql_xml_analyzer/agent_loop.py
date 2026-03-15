from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .analyzer import analyze_directory
from .context_compiler import compile_context_pack_from_analysis, write_context_pack
from .dashboard import write_evolution_report
from .evolution import propose_rules_from_analysis, review_llm_response_from_analysis, simulate_candidate_profile
from .lifecycle import grade_profile
from .phase_engine import (
    CLUSTER_PHASES,
    apply_phase_status,
    build_initial_state,
    build_phase_task,
    mark_artifact_completion,
    refresh_completed_artifacts,
    register_task_attempt,
    select_next_phase,
    should_stop,
)
from .schemas import LoopConfig, LoopState


def run_agent_loop(config: LoopConfig, runner: Any | None = None) -> dict[str, Any]:
    state = build_initial_state(config)
    state_path = persist_loop_state(config.output_dir, state)
    append_phase_history(
        config.output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "bootstrap",
            "status": "started",
            "state_path": str(state_path),
        },
    )
    return _run_loop(config, state, runner=runner)


def resume_agent_loop(output_dir: Path, config: LoopConfig | None = None, runner: Any | None = None) -> dict[str, Any]:
    state = load_loop_state(output_dir)
    resolved_config = config or LoopConfig.from_dict(state.config)
    return _run_loop(resolved_config, state, runner=runner)


def inspect_agent_loop(output_dir: Path) -> dict[str, Any]:
    state = load_loop_state(output_dir)
    refresh_completed_artifacts(state, output_dir.resolve())
    history = load_phase_history(output_dir)
    payload = {
        "state": state.to_dict(),
        "history_count": len(history),
        "latest_history": history[-5:],
    }
    inspection_path = output_dir / "analysis" / "agent_loop" / "inspection.json"
    inspection_path.parent.mkdir(parents=True, exist_ok=True)
    inspection_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _run_loop(config: LoopConfig, state: LoopState, runner: Any | None = None) -> dict[str, Any]:
    output_dir = config.output_dir.resolve()
    analysis_root = output_dir / "analysis"
    active_runner = runner or build_runner(config)

    while True:
        state.current_phase = select_next_phase(state)
        stop, reason = should_stop(state, output_dir, config.max_iterations)
        if stop:
            state.status = "completed" if reason == "all_required_artifacts_completed" else "stopped"
            state.stop_reason = reason
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        try:
            if state.current_phase == "scan":
                handle_scan_phase(config, state)
            elif state.current_phase in CLUSTER_PHASES:
                handle_cluster_phase(config, state, active_runner, analysis_root)
            elif state.current_phase == "simulate":
                handle_simulate_phase(config, state)
            elif state.current_phase == "grade":
                handle_grade_phase(config, state)
            elif state.current_phase == "package":
                handle_package_phase(config, state)
            else:
                state.stop_reason = f"unsupported_phase:{state.current_phase}"
                state.status = "failed"
        except Exception as exc:  # noqa: BLE001
            state.status = "failed"
            state.stop_reason = "phase_execution_failed"
            state.last_error = {
                "phase": state.current_phase,
                "message": str(exc),
            }
            append_phase_history(
                output_dir,
                {
                    "generated_at": timestamp_now(),
                    "phase": state.current_phase,
                    "status": "failed",
                    "error": str(exc),
                },
            )
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        state.iteration_count += 1
        persist_loop_state(output_dir, state)


def handle_scan_phase(config: LoopConfig, state: LoopState) -> None:
    output_dir = config.output_dir.resolve()
    result = analyze_directory(
        input_dir=config.input_dir.resolve(),
        output_dir=output_dir,
        profile_path=config.profile_path.resolve() if config.profile_path else None,
        snapshot_label=f"agent-loop-{state.run_id}-scan",
    )
    clusters_path = output_dir / "analysis" / "failure_clusters.json"
    cluster_payload = json.loads(clusters_path.read_text(encoding="utf-8"))
    state.pending_clusters = [item["cluster_id"] for item in cluster_payload.get("clusters", []) if isinstance(item, dict)]
    apply_phase_status(
        state,
        phase="scan",
        artifacts=[
            "analysis/index.json",
            "analysis/failure_clusters.json",
            "analysis/executive_summary.json",
        ],
        latest_output={
            "queries": len(result.queries),
            "diagnostics": len(result.diagnostics),
            "clusters": len(state.pending_clusters),
        },
    )
    append_phase_history(
        output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "scan",
            "status": "completed",
            "cluster_count": len(state.pending_clusters),
        },
    )


def handle_cluster_phase(config: LoopConfig, state: LoopState, runner: Any, analysis_root: Path) -> None:
    output_dir = config.output_dir.resolve()
    task = build_phase_task(state, config, analysis_root)
    register_task_attempt(state, task.cluster_id or "global", task.phase)
    state.active_task_id = task.task_id
    prior_response = None
    if task.phase == "verify":
        prior_response = (
            state.latest_outputs.get(task.cluster_id or "", {})
            .get("propose", {})
            .get("parsed_response")
        )

    context_pack = compile_context_pack_from_analysis(
        analysis_root=analysis_root,
        cluster_id=task.cluster_id or "",
        phase=task.phase,
        prompt_profile=config.prompt_profile,
        prior_response=prior_response if isinstance(prior_response, dict) else None,
    )
    context_paths = write_context_pack(output_dir, context_pack)
    runner_result = runner.run_task(task, context_pack.prompt_text, analysis_root)
    result_payload = json.loads(Path(str(runner_result["result_path"])).read_text(encoding="utf-8"))
    response_text_path = Path(str(result_payload["response_text_path"]))
    review_result = review_llm_response_from_analysis(
        analysis_root=analysis_root,
        cluster_id=task.cluster_id or "",
        response_path=response_text_path,
        stage=task.phase,
        budget=budget_label_from_tokens(task.token_budget.get("usable_input_limit", 0)),
        model=config.prompt_profile,
        profile_path=config.profile_path.resolve() if config.profile_path else None,
    )
    review = review_result["review"]

    latest_cluster_output = state.latest_outputs.setdefault(task.cluster_id or "", {})
    latest_cluster_output[task.phase] = {
        "status": review["status"],
        "parsed_response": review.get("parsed_response"),
        "review_path": review.get("response_path"),
        "result_path": runner_result["result_path"],
    }

    artifacts = [
        "analysis/context_packs",
        "analysis/llm_reviews",
    ]
    if review["status"] in {"accepted", "insufficient_evidence"}:
        if review["status"] == "insufficient_evidence" and task.phase in {"classify", "propose"}:
            for phase in remaining_cluster_phases(task.phase):
                apply_phase_status(state, phase=phase, cluster_id=task.cluster_id)
        apply_phase_status(
            state,
            phase=task.phase,
            cluster_id=task.cluster_id,
            artifacts=artifacts,
            latest_output=latest_cluster_output,
        )
    else:
        if task.attempt >= task.max_attempts:
            if task.phase in {"classify", "propose"}:
                for phase in remaining_cluster_phases(task.phase):
                    apply_phase_status(state, phase=phase, cluster_id=task.cluster_id)
            else:
                apply_phase_status(state, phase=task.phase, cluster_id=task.cluster_id)
            mark_artifact_completion(state, "analysis/llm_reviews")
        else:
            mark_artifact_completion(state, "analysis/context_packs")
            mark_artifact_completion(state, "analysis/llm_reviews")
            state.current_phase = task.phase
            state.active_task_id = None

    append_phase_history(
        output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": task.phase,
            "cluster_id": task.cluster_id,
            "task_id": task.task_id,
            "attempt": task.attempt,
            "status": review["status"],
            "result_path": runner_result["result_path"],
            "context_paths": [str(path) for path in context_paths],
        },
    )


def handle_simulate_phase(config: LoopConfig, state: LoopState) -> None:
    output_dir = config.output_dir.resolve()
    proposal_result = propose_rules_from_analysis(
        analysis_root=output_dir,
        profile_path=config.profile_path.resolve() if config.profile_path else None,
        min_confidence=0.7,
    )
    simulation_result = simulate_candidate_profile(
        input_dir=config.input_dir.resolve(),
        output_dir=output_dir,
        analysis_root=output_dir,
    )
    apply_phase_status(
        state,
        phase="simulate",
        artifacts=[
            "analysis/proposals/rule_proposals.json",
            "analysis/proposals/candidate_profile.json",
            "simulation/profile_simulation.json",
        ],
        latest_output={
            "proposal_summary": proposal_result["proposal_payload"]["summary"],
            "assessment": simulation_result["assessment"],
        },
    )
    append_phase_history(
        output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "simulate",
            "status": simulation_result["assessment"]["classification"],
        },
    )


def handle_grade_phase(config: LoopConfig, state: LoopState) -> None:
    output_dir = config.output_dir.resolve()
    candidate_profile_path = output_dir / "analysis" / "proposals" / "candidate_profile.json"
    result = grade_profile(
        profile_path=candidate_profile_path,
        validation_report_path=output_dir / "simulation" / "profile_simulation.json",
        output_dir=output_dir,
    )
    apply_phase_status(
        state,
        phase="grade",
        artifacts=["grade/profile_grade.json"],
        latest_output=result["grade_payload"],
    )
    append_phase_history(
        output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "grade",
            "status": result["grade_payload"]["suggested_status"],
            "readiness": result["grade_payload"]["promotion_readiness"],
        },
    )


def handle_package_phase(config: LoopConfig, state: LoopState) -> None:
    output_dir = config.output_dir.resolve()
    write_evolution_report(output_dir)
    apply_phase_status(
        state,
        phase="package",
        artifacts=[
            "analysis/executive_summary.json",
            "analysis/proposals/rule_proposals.json",
            "analysis/proposals/candidate_profile.json",
            "simulation/profile_simulation.json",
            "grade/profile_grade.json",
        ],
        latest_output={"status": "packaged"},
    )
    append_phase_history(
        output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "package",
            "status": "completed",
        },
    )


def build_runner(config: LoopConfig) -> Any:
    if config.runner_mode == "provider":
        from .agent_runners import OpenAICompatibleRunner

        return OpenAICompatibleRunner(config)
    if config.runner_mode == "cline_bridge":
        from .agent_runners import ClineBridgeRunner

        return ClineBridgeRunner(config)
    raise ValueError(f"runner_mode {config.runner_mode} requires an explicit runner instance.")


def load_loop_state(output_dir: Path) -> LoopState:
    path = output_dir.resolve() / "analysis" / "agent_loop" / "loop_state.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    return LoopState.from_dict(payload)


def persist_loop_state(output_dir: Path, state: LoopState) -> Path:
    root = output_dir.resolve() / "analysis" / "agent_loop"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "loop_state.json"
    path.write_text(json.dumps(state.to_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
    return path


def append_phase_history(output_dir: Path, event: dict[str, Any]) -> None:
    root = output_dir.resolve() / "analysis" / "agent_loop"
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "phase_history.json"
    md_path = root / "phase_history.md"
    history = load_phase_history(output_dir)
    history.append(event)
    json_path.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_phase_history_markdown(history), encoding="utf-8")


def load_phase_history(output_dir: Path) -> list[dict[str, Any]]:
    path = output_dir.resolve() / "analysis" / "agent_loop" / "phase_history.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def write_completion_report(output_dir: Path, state: LoopState) -> dict[str, Any]:
    root = output_dir.resolve() / "analysis" / "agent_loop"
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "completion_report.json"
    md_path = root / "completion_report.md"
    refresh_completed_artifacts(state, output_dir.resolve())
    missing = [item for item in state.required_artifacts if item not in state.completed_artifacts]
    payload = {
        "generated_at": timestamp_now(),
        "run_id": state.run_id,
        "status": state.status,
        "iterations": state.iteration_count,
        "completed_artifacts": state.completed_artifacts,
        "missing_artifacts": missing,
        "stop_reason": state.stop_reason,
        "last_error": state.last_error,
    }
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_completion_markdown(payload), encoding="utf-8")
    return payload


def render_completion_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Agent Loop Completion",
        "",
        f"- Run id: `{payload['run_id']}`",
        f"- Status: `{payload['status']}`",
        f"- Iterations: {payload['iterations']}",
        f"- Stop reason: {payload['stop_reason'] or 'n/a'}",
        "",
        "## Missing Artifacts",
    ]
    if payload["missing_artifacts"]:
        for item in payload["missing_artifacts"]:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    if payload.get("last_error"):
        lines.extend(["", "## Last Error", f"- {payload['last_error']}"])
    return "\n".join(lines).rstrip() + "\n"


def render_phase_history_markdown(history: list[dict[str, Any]]) -> str:
    lines = ["# Agent Loop Phase History", ""]
    for item in history:
        lines.append(
            f"- `{item.get('phase', 'n/a')}` status={item.get('status', 'n/a')} "
            f"cluster={item.get('cluster_id', 'n/a')} attempt={item.get('attempt', 'n/a')}"
        )
    return "\n".join(lines).rstrip() + "\n"


def budget_label_from_tokens(tokens: int) -> str:
    if tokens <= 8_192:
        return "8k"
    if tokens <= 32_768:
        return "32k"
    return "128k"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def remaining_cluster_phases(start_phase: str) -> list[str]:
    if start_phase not in CLUSTER_PHASES:
        return []
    index = CLUSTER_PHASES.index(start_phase)
    return CLUSTER_PHASES[index:]
