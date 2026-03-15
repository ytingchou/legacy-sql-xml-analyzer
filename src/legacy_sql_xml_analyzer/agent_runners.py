from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .llm_provider import (
    append_run_summary_to_index,
    build_request_artifact,
    extract_response_text,
    post_chat_completion,
    render_run_summary_markdown,
    resolve_provider_config,
)
from .prompting import sanitize_token
from .schemas import LoopConfig, PhaseTask


class OpenAICompatibleRunner:
    def __init__(self, config: LoopConfig) -> None:
        self.config = config

    def run_task(self, task: PhaseTask, prompt_text: str, analysis_root: Path) -> dict[str, Any]:
        provider = resolve_provider_config(
            provider_config_path=self.config.provider_config_path,
            provider_base_url=self.config.provider_base_url,
            provider_api_key=self.config.provider_api_key,
            provider_api_key_env=self.config.provider_api_key_env,
            provider_model=self.config.provider_model,
            provider_name=self.config.provider_name,
            token_limit=task.token_budget.get("reserved_output"),
            temperature=self.config.temperature,
            timeout_seconds=self.config.timeout_seconds,
        )

        response_payload = post_chat_completion(provider, prompt_text)
        response_text = extract_response_text(response_payload)
        run_root = analysis_root / "llm_runs"
        run_root.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        run_dir = run_root / f"{timestamp}-{task.task_id}-{sanitize_token(provider.provider_name or provider.model)}"
        run_dir.mkdir(parents=True, exist_ok=True)

        request_path = run_dir / "request.json"
        response_json_path = run_dir / "response.json"
        response_text_path = run_dir / "response.txt"
        summary_path = run_dir / "run_summary.json"
        summary_md_path = run_dir / "run_summary.md"

        run_summary = {
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "cluster_id": task.cluster_id,
            "stage": task.phase,
            "budget": f"{task.token_budget.get('usable_input_limit', 0)}-tokens",
            "prompt_model": task.model_profile,
            "provider_name": provider.provider_name or provider.model,
            "provider_model": provider.model,
            "provider_base_url": provider.base_url,
            "token_limit": provider.token_limit,
            "temperature": provider.temperature,
            "timeout_seconds": provider.timeout_seconds,
            "prompt_path": str(task.input_pack_path),
            "prompt_estimated_tokens": max(1, round(len(prompt_text) / 4)),
            "prompt_sha256": "",
            "response_usage": response_payload.get("usage", {}),
            "response_id": response_payload.get("id"),
            "review_enabled": False,
            "run_id": task.metadata.get("run_id"),
            "task_id": task.task_id,
        }

        request_path.write_text(json.dumps(build_request_artifact(provider, prompt_text), indent=2, ensure_ascii=False), encoding="utf-8")
        response_json_path.write_text(json.dumps(response_payload, indent=2, ensure_ascii=False), encoding="utf-8")
        response_text_path.write_text(response_text, encoding="utf-8")
        summary_path.write_text(json.dumps(run_summary, indent=2, ensure_ascii=False), encoding="utf-8")
        summary_md_path.write_text(render_run_summary_markdown(run_summary), encoding="utf-8")
        append_run_summary_to_index(run_root, summary_path, run_summary)

        result_path = write_agent_result(
            analysis_root=analysis_root,
            task=task,
            raw_text=response_text,
            structured_output=parse_json_if_possible(response_text),
            usage=response_payload.get("usage", {}),
            runner_name="provider",
        )
        return {
            "raw_text": response_text,
            "structured_output": parse_json_if_possible(response_text),
            "usage": response_payload.get("usage", {}),
            "result_path": str(result_path),
        }


class ClineBridgeRunner:
    def __init__(self, config: LoopConfig) -> None:
        self.config = config

    def run_task(self, task: PhaseTask, prompt_text: str, analysis_root: Path) -> dict[str, Any]:
        tasks_root = analysis_root / "agent_tasks"
        tasks_root.mkdir(parents=True, exist_ok=True)
        task_path = tasks_root / f"{task.task_id}.json"
        task_payload = task.to_dict()
        task_payload["prompt_text"] = prompt_text
        task_path.write_text(json.dumps(task_payload, indent=2, ensure_ascii=False), encoding="utf-8")

        if self.config.cline_bridge_command:
            subprocess.run(
                self.config.cline_bridge_command,
                shell=True,
                check=True,
                cwd=str(self.config.output_dir),
            )

        result_path = analysis_root / "agent_runs" / f"{task.task_id}.result.json"
        if not result_path.exists():
            raise FileNotFoundError(
                f"Cline bridge did not produce result file {result_path}. "
                "Expected the external bridge to read agent_tasks/*.json and write agent_runs/*.result.json."
            )
        payload = json.loads(result_path.read_text(encoding="utf-8"))
        return {
            "raw_text": str(payload.get("raw_text", "")),
            "structured_output": payload.get("structured_output", {}) if isinstance(payload.get("structured_output"), dict) else {},
            "usage": payload.get("usage", {}) if isinstance(payload.get("usage"), dict) else {},
            "result_path": str(result_path),
        }


class FakeRunner:
    def __init__(self, responses: dict[tuple[str, str | None], Any]) -> None:
        self.responses = responses

    def run_task(self, task: PhaseTask, prompt_text: str, analysis_root: Path) -> dict[str, Any]:
        key = (task.phase, task.cluster_id)
        response = self.responses.get(key)
        if response is None:
            raise KeyError(f"No fake response configured for {key}")
        if isinstance(response, dict):
            raw_text = json.dumps(response, ensure_ascii=False)
            structured = response
        else:
            raw_text = str(response)
            structured = parse_json_if_possible(raw_text)
        result_path = write_agent_result(
            analysis_root=analysis_root,
            task=task,
            raw_text=raw_text,
            structured_output=structured,
            usage={},
            runner_name="fake",
        )
        return {
            "raw_text": raw_text,
            "structured_output": structured,
            "usage": {},
            "result_path": str(result_path),
        }


def write_agent_result(
    analysis_root: Path,
    task: PhaseTask,
    raw_text: str,
    structured_output: dict[str, Any],
    usage: dict[str, Any],
    runner_name: str,
) -> Path:
    runs_root = analysis_root / "agent_runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    result_path = runs_root / f"{task.task_id}.result.json"
    text_path = runs_root / f"{task.task_id}.response.txt"
    payload = {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "task_id": task.task_id,
        "phase": task.phase,
        "cluster_id": task.cluster_id,
        "runner_name": runner_name,
        "response_text_path": str(text_path),
        "raw_text": raw_text,
        "structured_output": structured_output,
        "usage": usage,
    }
    text_path.write_text(raw_text, encoding="utf-8")
    result_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return result_path


def parse_json_if_possible(text: str) -> dict[str, Any]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}
