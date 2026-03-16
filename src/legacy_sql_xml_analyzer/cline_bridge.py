from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


COMMAND_PROFILES = {
    "cline-json": {"json_output": True, "yolo": False},
    "cline-json-yolo": {"json_output": True, "yolo": True},
    "cline-text": {"json_output": False, "yolo": False},
    "cline-text-yolo": {"json_output": False, "yolo": True},
}


@dataclass(slots=True)
class BridgeTask:
    mode: str
    task_id: str
    phase: str
    prompt_text: str
    prompt_file: Path
    task_path: Path
    result_path: Path
    response_text_path: Path
    root: Path
    payload: dict[str, Any]


@dataclass(slots=True)
class ExecutionSpec:
    argv: list[str] | None
    shell_command: str | None
    stdin_text: str | None
    cwd: Path
    response_parser: str
    display_command: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cline-bridge")
    parser.add_argument("mode", choices=["generic", "java-bff"], help="Task contract family to process.")
    parser.add_argument("root", type=Path, help="analysis root or java_bff root that contains task files.")
    parser.add_argument("--stdin-command", help="Shell command that accepts the prompt on stdin and returns the response on stdout.")
    parser.add_argument(
        "--command-template",
        help=(
            "Shell command template. Supported placeholders include "
            "{task_file}, {prompt_file}, {response_file}, {result_file}, {task_id}, {phase}, {cluster_id}, {bundle_id}, {root}."
        ),
    )
    parser.add_argument(
        "--command-profile",
        choices=sorted(COMMAND_PROFILES),
        help="Built-in execution profile. Use this instead of manually crafting --stdin-command or --command-template.",
    )
    parser.add_argument("--task-id", help="Optional single task_id filter.")
    parser.add_argument("--watch", action="store_true", help="Keep polling for new tasks until interrupted.")
    parser.add_argument("--poll-seconds", type=float, default=2.0, help="Polling interval used with --watch.")
    parser.add_argument("--dry-run", action="store_true", help="Print the resolved command and prompt source without executing it.")
    parser.add_argument("--verbose", action="store_true", help="Print more bridge detail while tasks are processed.")
    parser.add_argument("--cline-command", default="cline", help="Base command used by built-in cline command profiles.")
    parser.add_argument("--cline-cwd", type=Path, help="Optional working directory passed to Cline via --cwd.")
    parser.add_argument("--cline-model", help="Optional model name passed to Cline via --model.")
    parser.add_argument("--cline-config", type=Path, help="Optional configuration directory passed to Cline via --config.")
    parser.add_argument("--cline-extra-args", help="Extra arguments appended to the built-in cline command profile.")
    parser.add_argument("--cline-timeout", type=int, help="Optional --timeout value used by yolo profiles.")
    parser.add_argument("--cline-verbose-output", action="store_true", help="Pass --verbose to the built-in cline command profile.")
    parser.add_argument(
        "--cline-double-check-completion",
        action="store_true",
        help="Pass --double-check-completion to built-in cline command profiles.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configured = [bool(args.stdin_command), bool(args.command_template), bool(args.command_profile)]
    if sum(configured) != 1:
        raise SystemExit("Specify exactly one of --stdin-command, --command-template, or --command-profile.")
    processed = process_pending_tasks(
        mode=args.mode,
        root=args.root.resolve(),
        stdin_command=args.stdin_command,
        command_template=args.command_template,
        command_profile=args.command_profile,
        task_id=args.task_id,
        watch=args.watch,
        poll_seconds=args.poll_seconds,
        dry_run=args.dry_run,
        verbose=args.verbose,
        cline_command=args.cline_command,
        cline_cwd=args.cline_cwd.resolve() if args.cline_cwd else None,
        cline_model=args.cline_model,
        cline_config=args.cline_config.resolve() if args.cline_config else None,
        cline_extra_args=args.cline_extra_args,
        cline_timeout=args.cline_timeout,
        cline_verbose_output=args.cline_verbose_output,
        cline_double_check_completion=args.cline_double_check_completion,
    )
    print(f"Processed {processed} task(s).")
    return 0


def process_pending_tasks(
    *,
    mode: str,
    root: Path,
    stdin_command: str | None,
    command_template: str | None,
    command_profile: str | None,
    task_id: str | None = None,
    watch: bool = False,
    poll_seconds: float = 2.0,
    dry_run: bool = False,
    verbose: bool = False,
    cline_command: str = "cline",
    cline_cwd: Path | None = None,
    cline_model: str | None = None,
    cline_config: Path | None = None,
    cline_extra_args: str | None = None,
    cline_timeout: int | None = None,
    cline_verbose_output: bool = False,
    cline_double_check_completion: bool = False,
) -> int:
    total_processed = 0
    while True:
        pending = discover_pending_tasks(mode=mode, root=root, task_id=task_id)
        if not pending:
            if not watch:
                return total_processed
            if verbose:
                print(f"[bridge] no pending {mode} tasks under {root}, sleeping {poll_seconds:.1f}s")
            time.sleep(poll_seconds)
            continue

        for task in pending:
            process_single_task(
                task=task,
                stdin_command=stdin_command,
                command_template=command_template,
                command_profile=command_profile,
                dry_run=dry_run,
                verbose=verbose,
                cline_command=cline_command,
                cline_cwd=cline_cwd,
                cline_model=cline_model,
                cline_config=cline_config,
                cline_extra_args=cline_extra_args,
                cline_timeout=cline_timeout,
                cline_verbose_output=cline_verbose_output,
                cline_double_check_completion=cline_double_check_completion,
            )
            total_processed += 1

        if not watch:
            return total_processed


def discover_pending_tasks(mode: str, root: Path, task_id: str | None = None) -> list[BridgeTask]:
    if mode == "generic":
        analysis_root = resolve_generic_root(root)
        tasks_root = analysis_root / "agent_tasks"
        return [
            task
            for task in (
                build_generic_task(analysis_root, path)
                for path in sorted(tasks_root.glob("*.json"))
            )
            if task is not None and (task_id is None or task.task_id == task_id) and not task.result_path.exists()
        ]
    java_root = resolve_java_bff_root(root)
    tasks_root = java_root / "tasks"
    built: list[BridgeTask] = []
    for path in sorted(tasks_root.glob("*/*.json")):
        task = build_java_task(java_root, path)
        if task is None:
            continue
        if task_id is not None and task.task_id != task_id:
            continue
        if task.result_path.exists():
            continue
        built.append(task)
    return built


def resolve_generic_root(root: Path) -> Path:
    if (root / "agent_tasks").exists():
        return root
    if (root / "analysis" / "agent_tasks").exists():
        return root / "analysis"
    raise FileNotFoundError(f"Could not find generic agent_tasks under {root}.")


def resolve_java_bff_root(root: Path) -> Path:
    if (root / "tasks").exists():
        return root
    if (root / "analysis" / "java_bff" / "tasks").exists():
        return root / "analysis" / "java_bff"
    if (root / "java_bff" / "tasks").exists():
        return root / "java_bff"
    raise FileNotFoundError(f"Could not find Java BFF tasks under {root}.")


def build_generic_task(analysis_root: Path, task_path: Path) -> BridgeTask | None:
    payload = load_json(task_path)
    task_id = str(payload.get("task_id") or task_path.stem)
    prompt_text = str(payload.get("prompt_text") or "").strip()
    if not prompt_text:
        input_pack_path = Path(str(payload.get("input_pack_path") or ""))
        if input_pack_path.exists():
            prompt_candidate = input_pack_path.with_suffix(".txt")
            if prompt_candidate.exists():
                prompt_text = prompt_candidate.read_text(encoding="utf-8")
    if not prompt_text:
        return None
    runs_root = analysis_root / "agent_runs"
    runs_root.mkdir(parents=True, exist_ok=True)
    prompt_file = runs_root / f"{task_id}.prompt.txt"
    response_text_path = runs_root / f"{task_id}.response.txt"
    result_path = runs_root / f"{task_id}.result.json"
    return BridgeTask(
        mode="generic",
        task_id=task_id,
        phase=str(payload.get("phase") or "unknown"),
        prompt_text=prompt_text,
        prompt_file=prompt_file,
        task_path=task_path,
        result_path=result_path,
        response_text_path=response_text_path,
        root=analysis_root,
        payload=payload,
    )


def build_java_task(java_root: Path, task_path: Path) -> BridgeTask | None:
    payload = load_json(task_path)
    task_id = str(payload.get("task_id") or task_path.stem)
    prompt_file = Path(str(payload.get("context_prompt_path") or ""))
    if not prompt_file.exists():
        return None
    prompt_text = prompt_file.read_text(encoding="utf-8")
    result_value = str(payload.get("recommended_result_path") or "").strip()
    if not result_value:
        return None
    result_path = Path(result_value)
    response_text_path = result_path.with_suffix(".response.txt")
    return BridgeTask(
        mode="java-bff",
        task_id=task_id,
        phase=str(payload.get("phase") or "unknown"),
        prompt_text=prompt_text,
        prompt_file=prompt_file,
        task_path=task_path,
        result_path=result_path,
        response_text_path=response_text_path,
        root=java_root,
        payload=payload,
    )


def process_single_task(
    *,
    task: BridgeTask,
    stdin_command: str | None,
    command_template: str | None,
    command_profile: str | None,
    dry_run: bool,
    verbose: bool,
    cline_command: str,
    cline_cwd: Path | None,
    cline_model: str | None,
    cline_config: Path | None,
    cline_extra_args: str | None,
    cline_timeout: int | None,
    cline_verbose_output: bool,
    cline_double_check_completion: bool,
) -> None:
    task.prompt_file.parent.mkdir(parents=True, exist_ok=True)
    if not task.prompt_file.exists() or task.prompt_file.read_text(encoding="utf-8") != task.prompt_text:
        task.prompt_file.write_text(task.prompt_text, encoding="utf-8")

    execution = build_execution_spec(
        task,
        stdin_command=stdin_command,
        command_template=command_template,
        command_profile=command_profile,
        cline_command=cline_command,
        cline_cwd=cline_cwd,
        cline_model=cline_model,
        cline_config=cline_config,
        cline_extra_args=cline_extra_args,
        cline_timeout=cline_timeout,
        cline_verbose_output=cline_verbose_output,
        cline_double_check_completion=cline_double_check_completion,
    )
    if verbose or dry_run:
        print(f"[bridge] task={task.task_id} phase={task.phase} command={execution.display_command}")
    if dry_run:
        return

    result = subprocess.run(
        execution.argv if execution.argv is not None else execution.shell_command,
        shell=execution.shell_command is not None,
        text=True,
        input=execution.stdin_text,
        capture_output=True,
        cwd=str(execution.cwd),
    )
    if result.returncode != 0:
        stdout_preview = (result.stdout or "").strip()[:2000] or "no stdout"
        stderr_preview = (result.stderr or "").strip()[:2000] or "no stderr"
        raise RuntimeError(
            f"Bridge command failed for task {task.task_id} with exit code {result.returncode}. "
            f"stderr: {stderr_preview}; stdout: {stdout_preview}"
        )

    response_text = resolve_response_text(task, result.stdout, execution.response_parser)
    if response_text is None:
        raise RuntimeError(
            f"Bridge command completed for task {task.task_id} but produced no response text and did not write {task.response_text_path}."
        )
    task.response_text_path.parent.mkdir(parents=True, exist_ok=True)
    task.response_text_path.write_text(response_text, encoding="utf-8")

    structured_output = parse_json_if_possible(response_text)
    payload = build_result_payload(task, response_text, structured_output)
    task.result_path.parent.mkdir(parents=True, exist_ok=True)
    task.result_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[bridge] wrote result for task={task.task_id} result={task.result_path}")


def build_execution_spec(
    task: BridgeTask,
    *,
    stdin_command: str | None,
    command_template: str | None,
    command_profile: str | None,
    cline_command: str,
    cline_cwd: Path | None,
    cline_model: str | None,
    cline_config: Path | None,
    cline_extra_args: str | None,
    cline_timeout: int | None,
    cline_verbose_output: bool,
    cline_double_check_completion: bool,
) -> ExecutionSpec:
    if stdin_command:
        return ExecutionSpec(
            argv=None,
            shell_command=stdin_command,
            stdin_text=task.prompt_text,
            cwd=task.task_path.parent,
            response_parser="plain",
            display_command=stdin_command,
        )
    if command_template:
        shell_command = build_template_command(task, command_template)
        return ExecutionSpec(
            argv=None,
            shell_command=shell_command,
            stdin_text=None,
            cwd=task.task_path.parent,
            response_parser="plain",
            display_command=shell_command,
        )
    assert command_profile is not None
    return build_profile_execution_spec(
        task,
        command_profile=command_profile,
        cline_command=cline_command,
        cline_cwd=cline_cwd,
        cline_model=cline_model,
        cline_config=cline_config,
        cline_extra_args=cline_extra_args,
        cline_timeout=cline_timeout,
        cline_verbose_output=cline_verbose_output,
        cline_double_check_completion=cline_double_check_completion,
    )


def build_template_command(task: BridgeTask, command_template: str) -> str:
    placeholders = {
        "task_file": str(task.task_path.resolve()),
        "prompt_file": str(task.prompt_file.resolve()),
        "response_file": str(task.response_text_path.resolve()),
        "result_file": str(task.result_path.resolve()),
        "task_id": task.task_id,
        "phase": task.phase,
        "cluster_id": str(task.payload.get("cluster_id") or ""),
        "bundle_id": str(task.payload.get("bundle_id") or ""),
        "root": str(task.root.resolve()),
        "root_quoted": shlex.quote(str(task.root.resolve())),
        "prompt_file_quoted": shlex.quote(str(task.prompt_file.resolve())),
        "response_file_quoted": shlex.quote(str(task.response_text_path.resolve())),
        "task_file_quoted": shlex.quote(str(task.task_path.resolve())),
        "result_file_quoted": shlex.quote(str(task.result_path.resolve())),
    }
    return command_template.format(**placeholders)


def build_profile_execution_spec(
    task: BridgeTask,
    *,
    command_profile: str,
    cline_command: str,
    cline_cwd: Path | None,
    cline_model: str | None,
    cline_config: Path | None,
    cline_extra_args: str | None,
    cline_timeout: int | None,
    cline_verbose_output: bool,
    cline_double_check_completion: bool,
) -> ExecutionSpec:
    profile = COMMAND_PROFILES[command_profile]
    effective_cwd = (cline_cwd or task.root).resolve()
    argv = shlex.split(cline_command)
    argv.extend(["task"])
    if profile["json_output"]:
        argv.append("--json")
    if profile["yolo"]:
        argv.append("--yolo")
        if cline_timeout is not None:
            argv.extend(["--timeout", str(cline_timeout)])
    if cline_model:
        argv.extend(["--model", cline_model])
    if cline_config:
        argv.extend(["--config", str(cline_config.resolve())])
    if effective_cwd:
        argv.extend(["--cwd", str(effective_cwd)])
    if cline_verbose_output:
        argv.append("--verbose")
    if cline_double_check_completion:
        argv.append("--double-check-completion")
    if cline_extra_args:
        argv.extend(shlex.split(cline_extra_args))
    argv.append(task.prompt_text)
    return ExecutionSpec(
        argv=argv,
        shell_command=None,
        stdin_text=None,
        cwd=effective_cwd,
        response_parser="cline-json" if profile["json_output"] else "plain",
        display_command=" ".join(shlex.quote(part) for part in argv),
    )


def resolve_response_text(task: BridgeTask, stdout_text: str, response_parser: str) -> str | None:
    if task.response_text_path.exists():
        text = task.response_text_path.read_text(encoding="utf-8")
        if text.strip():
            return text
    if response_parser == "cline-json":
        extracted = extract_cline_response_text(stdout_text)
        if extracted:
            return extracted
    if stdout_text.strip():
        return stdout_text
    return None


def build_result_payload(task: BridgeTask, raw_text: str, structured_output: dict[str, Any]) -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    base = {
        "generated_at": generated_at,
        "response_text_path": str(task.response_text_path.resolve()),
        "raw_text": raw_text,
        "structured_output": structured_output,
        "usage": {},
        "runner_name": "cline_bridge",
        "task_id": task.task_id,
        "phase": task.phase,
    }
    if task.mode == "generic":
        base["cluster_id"] = task.payload.get("cluster_id")
        return base
    base["bundle_id"] = task.payload.get("bundle_id")
    base["phase_pack_path"] = task.payload.get("phase_pack_path")
    base["context_pack_path"] = task.payload.get("context_pack_path")
    return base


def parse_json_if_possible(text: str) -> dict[str, Any]:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3:
            stripped = "\n".join(lines[1:-1]).strip()
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def extract_cline_response_text(stdout_text: str) -> str | None:
    texts: list[str] = []
    for event in parse_json_events(stdout_text):
        text = extract_assistant_text_from_event(event)
        if text:
            texts.append(text)
    if texts:
        return texts[-1]
    return None


def parse_json_events(stdout_text: str) -> list[Any]:
    stripped = stdout_text.strip()
    if not stripped:
        return []
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        pass
    else:
        if isinstance(payload, list):
            return payload
        return [payload]

    events: list[Any] = []
    for line in stdout_text.splitlines():
        candidate = line.strip()
        if not candidate:
            continue
        try:
            events.append(json.loads(candidate))
        except json.JSONDecodeError:
            continue
    return events


def extract_assistant_text_from_event(event: Any) -> str | None:
    if not isinstance(event, dict):
        return None
    candidates: list[str] = []
    for key in ("message", "data", "result", "response", "payload", "delta"):
        value = event.get(key)
        text = extract_text_blob(value)
        if text:
            candidates.append(text)
    direct = extract_text_blob(event)
    if direct:
        candidates.append(direct)
    if not candidates:
        return None

    role_markers = " ".join(
        str(event.get(key) or "")
        for key in ("role", "type", "kind", "event", "name")
    ).lower()
    if role_markers and any(token in role_markers for token in ("tool", "system", "user", "status", "progress")):
        if not any(token in role_markers for token in ("assistant", "final", "result", "response", "completion")):
            return None

    return candidates[-1]


def extract_text_blob(node: Any) -> str | None:
    if isinstance(node, str):
        text = node.strip()
        return text or None
    if isinstance(node, list):
        parts = [part for item in node if (part := extract_text_blob(item))]
        return "\n".join(parts).strip() or None
    if isinstance(node, dict):
        role = str(node.get("role") or "").lower()
        if role in {"tool", "system", "user"}:
            return None
        if "text" in node and isinstance(node["text"], str):
            text = node["text"].strip()
            if text:
                return text
        if "content" in node:
            text = extract_text_blob(node["content"])
            if text:
                return text
    return None


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON at {path}.")
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
