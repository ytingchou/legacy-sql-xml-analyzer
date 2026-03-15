from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .java_bff import iter_bundle_payloads, prepare_java_bff_from_input, resolve_java_bff_root, safe_name
from .java_bff_runtime import (
    JavaBffClineBridgeRunner,
    JavaBffFakeRunner,
    JavaBffProviderRunner,
    merge_java_bff_phases,
    review_java_bff_response_from_analysis,
)
from .java_skeletons import generate_java_skeletons


@dataclass(slots=True)
class JavaBffLoopConfig:
    input_dir: Path
    output_dir: Path
    profile_path: Path | None = None
    bundle_id: str | None = None
    prompt_profile: str = "qwen3-128k-java-bff"
    max_iterations: int = 64
    max_attempts_per_prompt: int = 3
    runner_mode: str = "provider"
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
    package_name: str = "com.example.legacybff"
    entry_file: str | None = None
    entry_main_query: str | None = None
    max_sql_chunk_tokens: int | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["input_dir"] = str(self.input_dir)
        payload["output_dir"] = str(self.output_dir)
        payload["profile_path"] = str(self.profile_path) if self.profile_path else None
        payload["provider_config_path"] = str(self.provider_config_path) if self.provider_config_path else None
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "JavaBffLoopConfig":
        return cls(
            input_dir=Path(str(payload["input_dir"])),
            output_dir=Path(str(payload["output_dir"])),
            profile_path=Path(str(payload["profile_path"])) if payload.get("profile_path") else None,
            bundle_id=payload.get("bundle_id"),
            prompt_profile=str(payload.get("prompt_profile", "qwen3-128k-java-bff")),
            max_iterations=int(payload.get("max_iterations", 64)),
            max_attempts_per_prompt=int(payload.get("max_attempts_per_prompt", 3)),
            runner_mode=str(payload.get("runner_mode", "provider")),
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
            package_name=str(payload.get("package_name", "com.example.legacybff")),
            entry_file=payload.get("entry_file"),
            entry_main_query=payload.get("entry_main_query"),
            max_sql_chunk_tokens=int(payload["max_sql_chunk_tokens"]) if payload.get("max_sql_chunk_tokens") is not None else None,
        )


def run_java_bff_loop(config: JavaBffLoopConfig, runner: Any | None = None) -> dict[str, Any]:
    entry_file = config.entry_file
    entry_main_query = config.entry_main_query
    if config.bundle_id and not entry_file and not entry_main_query:
        parsed_file, parsed_query = parse_bundle_selector(config.bundle_id)
        entry_file = parsed_file or entry_file
        entry_main_query = parsed_query or entry_main_query

    prepare_java_bff_from_input(
        input_dir=config.input_dir.resolve(),
        output_dir=config.output_dir.resolve(),
        profile_path=config.profile_path.resolve() if config.profile_path else None,
        entry_file=entry_file,
        entry_main_query=entry_main_query,
        prompt_profile=config.prompt_profile,
        max_sql_chunk_tokens=config.max_sql_chunk_tokens,
    )
    state = build_initial_state(config)
    persist_loop_state(config.output_dir, state)
    append_loop_history(
        config.output_dir,
        {
            "generated_at": timestamp_now(),
            "phase": "bootstrap",
            "status": "started",
        },
    )
    return _run_loop(config=config, state=state, runner=runner or build_runner(config))


def resume_java_bff_loop(output_dir: Path, config: JavaBffLoopConfig | None = None, runner: Any | None = None) -> dict[str, Any]:
    state = load_loop_state(output_dir)
    resolved_config = config or JavaBffLoopConfig.from_dict(state["config"])
    return _run_loop(config=resolved_config, state=state, runner=runner or build_runner(resolved_config))


def inspect_java_bff_loop(output_dir: Path) -> dict[str, Any]:
    state = load_loop_state(output_dir)
    history = load_loop_history(output_dir)
    payload = {
        "state": state,
        "history_count": len(history),
        "latest_history": history[-10:],
    }
    inspection_path = loop_root(output_dir) / "inspection.json"
    inspection_path.parent.mkdir(parents=True, exist_ok=True)
    inspection_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return payload


def _run_loop(config: JavaBffLoopConfig, state: dict[str, Any], runner: Any) -> dict[str, Any]:
    output_dir = config.output_dir.resolve()
    analysis_root = output_dir / "analysis"
    while True:
        if int(state.get("iteration_count", 0)) >= int(config.max_iterations):
            refresh_completion_state(analysis_root, state)
            state["status"] = "stopped"
            state["stop_reason"] = "max_iterations_reached"
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        next_prompt_path = select_next_prompt(state, analysis_root)
        if next_prompt_path is None:
            finalize_merges_and_skeletons(analysis_root, state, package_name=config.package_name)
            refresh_completion_state(analysis_root, state)
            if state["missing_artifacts"]:
                state["status"] = "stopped"
                state["stop_reason"] = "java_bff_artifacts_incomplete"
            else:
                state["status"] = "completed"
                state["stop_reason"] = "all_artifacts_completed"
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        state["current_prompt_json_path"] = str(next_prompt_path.resolve())
        state["current_prompt_path"] = state["current_prompt_json_path"]
        attempts = state.setdefault("prompt_attempts", {})
        attempts[str(next_prompt_path.resolve())] = attempts.get(str(next_prompt_path.resolve()), 0) + 1
        persist_loop_state(output_dir, state)

        try:
            run_result = runner.run_phase_pack(analysis_root=analysis_root, phase_pack_path=next_prompt_path)
            review_result = review_java_bff_response_from_analysis(
                analysis_root=analysis_root,
                phase_pack_path=next_prompt_path,
                response_path=Path(str(run_result["response_text_path"])),
            )
        except Exception as exc:  # noqa: BLE001
            state["status"] = "failed"
            state["stop_reason"] = "java_bff_phase_failed"
            state["last_error"] = {"prompt_path": str(next_prompt_path), "message": str(exc)}
            append_loop_history(
                output_dir,
                {
                    "generated_at": timestamp_now(),
                    "phase": "prompt",
                    "status": "failed",
                    "prompt_path": str(next_prompt_path),
                    "error": str(exc),
                },
            )
            refresh_completion_state(analysis_root, state)
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        review = review_result["review"]
        append_loop_history(
            output_dir,
            {
                "generated_at": timestamp_now(),
                "phase": review["phase"],
                "status": review["status"],
                "prompt_path": str(next_prompt_path),
                "review_path": review.get("review_json_path"),
            },
        )

        prompt_key = str(next_prompt_path.resolve())
        if review["status"] in {"accepted", "insufficient_evidence"}:
            completed = state.setdefault("completed_prompts", [])
            if prompt_key not in completed:
                completed.append(prompt_key)
            accepted_reviews = state.setdefault("accepted_reviews", [])
            review_path = str(review.get("review_json_path") or "")
            if review_path and review_path not in accepted_reviews:
                accepted_reviews.append(review_path)
        elif attempts[prompt_key] >= int(config.max_attempts_per_prompt):
            state["status"] = "stopped"
            state["stop_reason"] = "java_bff_human_review_required"
            state["last_error"] = {
                "prompt_path": str(next_prompt_path),
                "message": "Max attempts exceeded for Java BFF prompt.",
            }
            refresh_completion_state(analysis_root, state)
            persist_loop_state(output_dir, state)
            return write_completion_report(output_dir, state)

        state["iteration_count"] = int(state.get("iteration_count", 0)) + 1
        refresh_completion_state(analysis_root, state)
        persist_loop_state(output_dir, state)


def finalize_merges_and_skeletons(analysis_root: Path, state: dict[str, Any], package_name: str) -> None:
    merged_bundles: list[str] = []
    ready_bundles: list[str] = []
    for bundle in selected_bundles(analysis_root, state):
        bundle_id = str(bundle["bundle_id"])
        result = merge_java_bff_phases(analysis_root, bundle_id=bundle_id)
        merged_bundles.append(bundle_id)
        if bool(result["implementation_plan"].get("completion", {}).get("ready_for_skeletons")):
            ready_bundles.append(bundle_id)
    state["merged_bundles"] = merged_bundles
    state["ready_bundles"] = ready_bundles
    if ready_bundles:
        generate_java_skeletons(
            analysis_root=analysis_root,
            bundle_id=None if len(ready_bundles) > 1 else ready_bundles[0],
            base_package=package_name,
        )
    state["generated_skeletons"] = ready_bundles
    state["skeleton_bundles"] = ready_bundles


def build_initial_state(config: JavaBffLoopConfig) -> dict[str, Any]:
    state = {
        "run_id": datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
        "status": "running",
        "bundle_id": config.bundle_id,
        "entry_file": config.entry_file,
        "entry_main_query": config.entry_main_query,
        "current_prompt_json_path": None,
        "current_prompt_path": None,
        "completed_prompts": [],
        "accepted_reviews": [],
        "prompt_attempts": {},
        "iteration_count": 0,
        "merged_bundles": [],
        "ready_bundles": [],
        "generated_skeletons": [],
        "skeleton_bundles": [],
        "stop_reason": None,
        "last_error": None,
        "required_artifacts": [],
        "completed_artifacts": [],
        "missing_artifacts": [],
        "config": config.to_dict(),
    }
    refresh_completion_state(config.output_dir.resolve() / "analysis", state)
    return state


def selected_bundles(analysis_root: Path, state: dict[str, Any]) -> list[dict[str, Any]]:
    bundles = iter_bundle_payloads(analysis_root)
    bundle_id = state.get("bundle_id")
    entry_file = state.get("entry_file")
    entry_main_query = state.get("entry_main_query")
    if bundle_id:
        bundles = [
            item
            for item in bundles
            if item.get("bundle_id") == bundle_id or item.get("entry_query_name") == bundle_id
        ]
    if entry_file:
        bundles = [item for item in bundles if Path(str(item.get("entry_file") or "")).name == entry_file]
    if entry_main_query:
        bundles = [item for item in bundles if item.get("entry_query_name") == entry_main_query]
    return bundles


def select_next_prompt(state: dict[str, Any], analysis_root: Path) -> Path | None:
    completed = set(state.get("completed_prompts", []))
    for bundle in selected_bundles(analysis_root, state):
        for prompt_path in bundle.get("recommended_sequence", []):
            json_path = Path(str(prompt_path)).with_suffix(".json").resolve()
            if str(json_path) not in completed:
                return json_path
    return None


def build_runner(config: JavaBffLoopConfig) -> Any:
    if config.runner_mode == "provider":
        return JavaBffProviderRunner(
            {
                "provider_config_path": config.provider_config_path.resolve() if config.provider_config_path else None,
                "provider_base_url": config.provider_base_url,
                "provider_api_key": config.provider_api_key,
                "provider_api_key_env": config.provider_api_key_env,
                "provider_model": config.provider_model,
                "provider_name": config.provider_name,
                "token_limit": config.token_limit,
                "temperature": config.temperature,
                "timeout_seconds": config.timeout_seconds,
            }
        )
    if config.runner_mode == "cline_bridge":
        return JavaBffClineBridgeRunner(cline_bridge_command=config.cline_bridge_command)
    if config.runner_mode == "fake":
        return JavaBffFakeRunner({})
    raise ValueError(f"Unsupported Java BFF runner mode: {config.runner_mode}")


def persist_loop_state(output_dir: Path, state: dict[str, Any]) -> None:
    root = loop_root(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    (root / "loop_state.json").write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def load_loop_state(output_dir: Path) -> dict[str, Any]:
    return json.loads((loop_root(output_dir) / "loop_state.json").read_text(encoding="utf-8"))


def append_loop_history(output_dir: Path, event: dict[str, Any]) -> None:
    root = loop_root(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    json_path = root / "history.json"
    md_path = root / "history.md"
    history = load_loop_history(output_dir)
    history.append(event)
    json_path.write_text(json.dumps(history, indent=2, ensure_ascii=False), encoding="utf-8")
    md_path.write_text(render_history_markdown(history), encoding="utf-8")


def load_loop_history(output_dir: Path) -> list[dict[str, Any]]:
    path = loop_root(output_dir) / "history.json"
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def write_completion_report(output_dir: Path, state: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "generated_at": timestamp_now(),
        "run_id": state["run_id"],
        "status": state["status"],
        "stop_reason": state.get("stop_reason"),
        "iterations": state.get("iteration_count", 0),
        "iteration_count": state.get("iteration_count", 0),
        "completed_prompt_count": len(state.get("completed_prompts", [])),
        "merged_bundles": state.get("merged_bundles", []),
        "skeleton_bundles": state.get("skeleton_bundles", []),
        "generated_skeletons": state.get("generated_skeletons", []),
        "completed_artifacts": state.get("completed_artifacts", []),
        "missing_artifacts": state.get("missing_artifacts", []),
        "last_error": state.get("last_error"),
    }
    root = loop_root(output_dir)
    root.mkdir(parents=True, exist_ok=True)
    (root / "completion_report.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    (root / "completion_report.md").write_text(render_completion_markdown(payload), encoding="utf-8")
    return payload


def refresh_completion_state(analysis_root: Path, state: dict[str, Any]) -> None:
    java_root = resolve_java_bff_root(analysis_root)
    required: list[str] = [
        str((java_root / "overview.json").resolve()),
        str((java_root / "chunk_manifest.json").resolve()),
    ]
    completed: list[str] = [path for path in required if Path(path).exists()]

    for bundle in selected_bundles(analysis_root, state):
        bundle_id = str(bundle["bundle_id"])
        slug = safe_name(bundle_id)
        bundle_json = java_root / "bundles" / slug / "bundle.json"
        required.append(str(bundle_json.resolve()))
        if bundle_json.exists():
            completed.append(str(bundle_json.resolve()))

        for prompt_path in bundle.get("recommended_sequence", []):
            prompt_json = Path(str(prompt_path)).with_suffix(".json")
            review_json = java_root / "reviews" / slug / f"{safe_name(prompt_json.stem)}-review.json"
            required.append(str(review_json.resolve()))
            if review_json.exists():
                completed.append(str(review_json.resolve()))

        merged_json = java_root / "merged" / slug / "implementation_plan.json"
        required.append(str(merged_json.resolve()))
        if merged_json.exists():
            completed.append(str(merged_json.resolve()))
            try:
                merged_payload = json.loads(merged_json.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                merged_payload = {}
            if isinstance(merged_payload, dict) and bool(merged_payload.get("completion", {}).get("ready_for_skeletons")):
                manifest_json = java_root / "skeletons" / slug / "manifest.json"
                readme_md = java_root / "skeletons" / slug / "README.md"
                required.extend([str(manifest_json.resolve()), str(readme_md.resolve())])
                if manifest_json.exists():
                    completed.append(str(manifest_json.resolve()))
                if readme_md.exists():
                    completed.append(str(readme_md.resolve()))

    unique_required = list(dict.fromkeys(required))
    unique_completed = list(dict.fromkeys(path for path in completed if path in set(unique_required)))
    state["required_artifacts"] = unique_required
    state["completed_artifacts"] = unique_completed
    state["missing_artifacts"] = [path for path in unique_required if path not in set(unique_completed)]


def render_history_markdown(history: list[dict[str, Any]]) -> str:
    lines = ["# Java BFF Loop History", ""]
    for item in history:
        lines.append(
            f"- `{item.get('phase')}` status={item.get('status')} prompt={item.get('prompt_path', 'n/a')}"
        )
    return "\n".join(lines).rstrip() + "\n"


def render_completion_markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Java BFF Loop Completion",
        "",
        f"- Run id: `{payload['run_id']}`",
        f"- Status: `{payload['status']}`",
        f"- Stop reason: `{payload['stop_reason'] or 'n/a'}`",
        f"- Iterations: {payload['iterations']}",
        f"- Completed prompts: {payload['completed_prompt_count']}",
        "",
        "## Missing Artifacts",
    ]
    if payload["missing_artifacts"]:
        for item in payload["missing_artifacts"]:
            lines.append(f"- `{item}`")
    else:
        lines.append("- None")
    lines.extend(["", "## Bundles"])
    if payload["merged_bundles"]:
        for item in payload["merged_bundles"]:
            lines.append(f"- merged `{item}`")
    else:
        lines.append("- None")
    if payload["skeleton_bundles"]:
        lines.extend(["", "## Skeletons"])
        for item in payload["skeleton_bundles"]:
            lines.append(f"- `{item}`")
    return "\n".join(lines).rstrip() + "\n"


def parse_bundle_selector(bundle_id: str) -> tuple[str | None, str | None]:
    parts = bundle_id.split(":")
    if len(parts) >= 3 and parts[1] == "main":
        return parts[0], parts[2]
    return None, None


def loop_root(output_dir: Path) -> Path:
    return resolve_java_bff_root(output_dir) / "loop"


def timestamp_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
