from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

from .agent_loop import inspect_agent_loop, resume_agent_loop, run_agent_loop
from .adaptive_prompt import (
    compile_adaptive_generic_context,
    compile_adaptive_java_context,
    shrink_prompt_text,
    write_adaptive_payload,
)
from .analyzer import analyze_directory
from .cline_bridge import COMMAND_PROFILES
from .console import ConsoleReporter
from .context_compiler import compile_context_pack_from_analysis, write_context_pack
from .doctor import doctor_run
from .evolution import (
    apply_profile_patch_bundle,
    propose_rules_from_analysis,
    review_llm_response_from_analysis,
    simulate_candidate_profile,
)
from .failure_explainer import explain_failure_from_output_dir
from .handoff import export_vscode_cline_pack
from .java_bff import prepare_java_bff_from_input
from .java_bff_context import compile_java_bff_context_pack, write_java_bff_context_pack
from .java_bff_loop import (
    JavaBffLoopConfig,
    inspect_java_bff_loop,
    resume_java_bff_loop,
    run_java_bff_loop,
)
from .java_bff_workflow import (
    generate_java_bff_skeleton,
    generate_java_bff_starter,
    invoke_java_bff_prompt,
    merge_java_bff_phases,
    review_java_bff_response_from_analysis,
)
from .lifecycle import grade_profile, promote_profile, rollback_profile
from .llm_provider import LlmProviderError, invoke_llm_from_analysis, validate_provider_connection
from .learning import freeze_profile, infer_rules, learn_directory
from .prompt_profiles import phase_budget_for
from .prompting import prepare_prompt_pack_from_analysis, resolve_analysis_root
from .schemas import LoopConfig
from .validation import validate_profile
from .watch_review import watch_and_review
from .web import serve_report


def add_cline_bridge_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cline-bridge-command", help="Optional shell command to trigger the Cline bridge after writing each task file.")
    parser.add_argument(
        "--cline-bridge-profile",
        choices=sorted(COMMAND_PROFILES),
        help="Generate a local tools/cline_bridge.py command automatically from a built-in Cline command profile.",
    )
    parser.add_argument("--cline-command", default="cline", help="Base Cline command used by --cline-bridge-profile.")
    parser.add_argument("--cline-cwd", type=Path, help="Workspace path passed to generated Cline bridge commands via --cline-cwd.")
    parser.add_argument("--cline-model", help="Optional model name passed to generated Cline bridge commands.")
    parser.add_argument("--cline-config", type=Path, help="Optional config directory passed to generated Cline bridge commands.")
    parser.add_argument("--cline-extra-args", help="Extra arguments appended to the generated Cline bridge command.")
    parser.add_argument("--cline-timeout", type=int, help="Optional --timeout passed through to generated Cline bridge commands.")
    parser.add_argument("--cline-verbose-output", action="store_true", help="Pass --cline-verbose-output to generated Cline bridge commands.")
    parser.add_argument(
        "--cline-double-check-completion",
        action="store_true",
        help="Pass --cline-double-check-completion to generated Cline bridge commands.",
    )


def resolve_cline_bridge_command(args: argparse.Namespace, *, output_dir: Path, mode: str, runner_mode: str) -> str | None:
    profile_specific_values = {
        "cline_cwd": args.cline_cwd,
        "cline_model": args.cline_model,
        "cline_config": args.cline_config,
        "cline_extra_args": args.cline_extra_args,
        "cline_timeout": args.cline_timeout,
        "cline_verbose_output": args.cline_verbose_output,
        "cline_double_check_completion": args.cline_double_check_completion,
    }
    has_profile_specific = any(
        value not in (None, False, "")
        for value in profile_specific_values.values()
    )
    if args.cline_bridge_command and (args.cline_bridge_profile or has_profile_specific):
        raise ValueError("Use either --cline-bridge-command or --cline-bridge-profile/--cline-* options, not both.")
    if args.cline_bridge_command:
        return args.cline_bridge_command
    if args.cline_bridge_profile is None:
        if has_profile_specific:
            raise ValueError("--cline-* bridge options require --cline-bridge-profile.")
        return None
    if runner_mode != "cline_bridge":
        raise ValueError("--cline-bridge-profile can only be used when --runner-mode=cline_bridge.")

    repo_root = Path(__file__).resolve().parents[2]
    bridge_script = repo_root / "tools" / "cline_bridge.py"
    bridge_root = output_dir.resolve() / "analysis"
    if mode == "java-bff":
        bridge_root = bridge_root / "java_bff"

    command: list[str] = [
        sys.executable,
        str(bridge_script),
        mode,
        str(bridge_root),
        "--command-profile",
        args.cline_bridge_profile,
        "--cline-command",
        args.cline_command,
    ]
    if args.cline_cwd:
        command.extend(["--cline-cwd", str(args.cline_cwd.resolve())])
    if args.cline_model:
        command.extend(["--cline-model", args.cline_model])
    if args.cline_config:
        command.extend(["--cline-config", str(args.cline_config.resolve())])
    if args.cline_extra_args:
        command.extend(["--cline-extra-args", args.cline_extra_args])
    if args.cline_timeout is not None:
        command.extend(["--cline-timeout", str(args.cline_timeout)])
    if args.cline_verbose_output:
        command.append("--cline-verbose-output")
    if args.cline_double_check_completion:
        command.append("--cline-double-check-completion")
    return " ".join(shlex.quote(part) for part in command)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="legacy-sql-xml-analyzer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze SQL XML mappings.")
    analyze_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    analyze_parser.add_argument("--output", required=True, type=Path, help="Output directory for generated artifacts.")
    analyze_parser.add_argument("--entry-file", help="Optional XML filename to emphasize in artifacts.")
    analyze_parser.add_argument("--entry-main-query", help="Optional main-query name to emphasize in artifacts.")
    analyze_parser.add_argument("--profile", type=Path, help="Optional learned/frozen profile JSON.")
    analyze_parser.add_argument("--snapshot-label", help="Optional label for run history snapshots.")
    analyze_parser.add_argument("--strict", action="store_true", help="Exit non-zero when any error or fatal diagnostic exists.")

    learn_parser = subparsers.add_parser("learn", help="Observe XML structure and generate learning artifacts.")
    learn_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    learn_parser.add_argument("--output", required=True, type=Path, help="Output directory for learning artifacts.")

    infer_parser = subparsers.add_parser("infer-rules", help="Infer a rule profile from observations.")
    infer_parser.add_argument("--input", required=True, type=Path, help="observations.json or directory that contains it.")
    infer_parser.add_argument("--output", required=True, type=Path, help="Output directory for inferred rule artifacts.")

    freeze_parser = subparsers.add_parser("freeze-profile", help="Freeze a candidate rule profile into a reusable profile.")
    freeze_parser.add_argument("--input", required=True, type=Path, help="rule_candidates.json path.")
    freeze_parser.add_argument("--output", required=True, type=Path, help="Output path for the frozen profile JSON.")
    freeze_parser.add_argument(
        "--min-confidence",
        type=float,
        default=0.8,
        help="Minimum rule confidence required to keep a rule in the frozen profile.",
    )

    validate_parser = subparsers.add_parser("validate-profile", help="Validate a learned profile against baseline analysis.")
    validate_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    validate_parser.add_argument("--output", required=True, type=Path, help="Output directory for validation artifacts.")
    validate_parser.add_argument("--profile", required=True, type=Path, help="Frozen or learned profile JSON.")
    validate_parser.add_argument("--entry-file", help="Optional XML filename to emphasize in validation.")
    validate_parser.add_argument("--entry-main-query", help="Optional main-query name to emphasize in validation.")
    validate_parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit non-zero when validation classifies the profile as regressed.",
    )

    serve_parser = subparsers.add_parser("serve-report", help="Serve the generated HTML dashboard locally.")
    serve_parser.add_argument("--root", required=True, type=Path, help="Output directory that contains analysis/dashboard.html.")
    serve_parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    serve_parser.add_argument("--port", type=int, default=8000, help="Port to bind the local HTTP server.")

    prompt_parser = subparsers.add_parser("prepare-prompt", help="Generate a weak-LLM prompt pack for a failure cluster.")
    prompt_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory or analysis directory that contains failure_clusters.json.")
    prompt_parser.add_argument("--cluster", required=True, help="cluster_id from failure_clusters.json.")
    prompt_parser.add_argument("--budget", default="128k", choices=["8k", "32k", "128k"], help="Target prompt budget.")
    prompt_parser.add_argument("--model", default="weak-128k", help="Descriptive model profile label for the pack.")

    invoke_parser = subparsers.add_parser(
        "invoke-llm",
        help="Send a staged prompt pack to an OpenAI-compatible provider and optionally review the response.",
    )
    invoke_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory or analysis directory that contains failure_clusters.json.")
    invoke_parser.add_argument("--cluster", required=True, help="cluster_id from failure_clusters.json.")
    invoke_parser.add_argument("--stage", default="propose", choices=["classify", "propose", "verify"], help="Prompt stage to send.")
    invoke_parser.add_argument("--budget", default="128k", choices=["8k", "32k", "128k"], help="Prompt budget label used to resolve the prompt pack.")
    invoke_parser.add_argument("--prompt-model", default="weak-128k", help="Prompt-pack model label used to resolve the prompt files.")
    invoke_parser.add_argument("--provider-config", type=Path, help="Optional JSON config for the OpenAI-compatible provider.")
    invoke_parser.add_argument("--provider-base-url", help="Provider base URL or /v1 root for the OpenAI-compatible endpoint.")
    invoke_parser.add_argument("--provider-api-key", help="Provider API key. If omitted, --provider-api-key-env or provider-config is used.")
    invoke_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name that stores the provider API key.")
    invoke_parser.add_argument("--provider-model", help="Provider model id to send to the chat/completions endpoint.")
    invoke_parser.add_argument("--provider-name", help="Optional provider label for saved artifacts.")
    invoke_parser.add_argument("--token-limit", type=int, help="Maximum completion tokens to request from the provider.")
    invoke_parser.add_argument("--temperature", type=float, help="Sampling temperature sent to the provider.")
    invoke_parser.add_argument("--timeout-seconds", type=float, help="HTTP timeout for provider requests.")
    invoke_parser.add_argument("--review", action="store_true", help="Immediately run review-llm-response on the saved response text.")
    invoke_parser.add_argument("--profile", type=Path, help="Optional profile JSON used during review to detect redundant or conflicting rules.")

    validate_provider_parser = subparsers.add_parser(
        "validate-provider",
        help="Validate an OpenAI-compatible LLM provider connection and save debug artifacts.",
    )
    validate_provider_parser.add_argument("--output", required=True, type=Path, help="Output directory for provider validation artifacts.")
    validate_provider_parser.add_argument("--provider-config", type=Path, help="Optional JSON config for the OpenAI-compatible provider.")
    validate_provider_parser.add_argument("--provider-base-url", help="Provider base URL or /v1 root for the OpenAI-compatible endpoint.")
    validate_provider_parser.add_argument("--provider-api-key", help="Provider API key. If omitted, --provider-api-key-env or provider-config is used.")
    validate_provider_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name that stores the provider API key.")
    validate_provider_parser.add_argument("--provider-model", help="Provider model id to send to the chat/completions endpoint.")
    validate_provider_parser.add_argument("--provider-name", help="Optional provider label for saved artifacts.")
    validate_provider_parser.add_argument("--token-limit", type=int, help="Maximum completion tokens to request from the provider.")
    validate_provider_parser.add_argument("--temperature", type=float, help="Sampling temperature sent to the provider.")
    validate_provider_parser.add_argument("--timeout-seconds", type=float, help="HTTP timeout for provider requests.")
    validate_provider_parser.add_argument("--prompt-text", help="Optional probe prompt override.")
    validate_provider_parser.add_argument("--no-expect-json", action="store_true", help="Skip the probe JSON-shape check and only validate connectivity plus response text extraction.")

    review_parser = subparsers.add_parser(
        "review-llm-response",
        help="Review a weak-LLM JSON response and generate follow-up prompts or profile patch candidates.",
    )
    review_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory or analysis directory that contains failure_clusters.json.")
    review_parser.add_argument("--cluster", required=True, help="cluster_id from failure_clusters.json.")
    review_parser.add_argument("--response", required=True, type=Path, help="Path to the LLM response text or JSON file.")
    review_parser.add_argument("--stage", default="propose", choices=["classify", "propose", "verify"], help="Prompt stage that produced the response.")
    review_parser.add_argument("--budget", default="128k", choices=["8k", "32k", "128k"], help="Target prompt budget for generated follow-up prompts.")
    review_parser.add_argument("--model", default="weak-128k", help="Descriptive model profile label for the follow-up prompt.")
    review_parser.add_argument("--profile", type=Path, help="Optional profile JSON used to detect redundant or conflicting proposed rules.")

    propose_parser = subparsers.add_parser(
        "propose-rules",
        help="Collect accepted LLM review patch candidates and build a candidate profile.",
    )
    propose_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory or analysis directory that contains llm_reviews.")
    propose_parser.add_argument("--profile", type=Path, help="Optional base profile JSON to merge candidate patches into.")
    propose_parser.add_argument("--min-confidence", type=float, default=0.7, help="Minimum patch confidence score to include in the candidate profile.")
    propose_parser.add_argument("--include-needs-review", action="store_true", help="Keep needs_revision reviews in the proposal report as manual follow-up context.")

    apply_patch_parser = subparsers.add_parser(
        "apply-profile-patch",
        help="Apply a rule proposal bundle to a base profile and write a merged profile JSON.",
    )
    apply_patch_parser.add_argument("--patch-bundle", required=True, type=Path, help="rule_proposals.json generated by propose-rules.")
    apply_patch_parser.add_argument("--output", required=True, type=Path, help="Output path for the merged profile JSON.")
    apply_patch_parser.add_argument("--profile", type=Path, help="Optional base profile JSON to merge into.")

    simulate_parser = subparsers.add_parser(
        "simulate-profile",
        help="Validate a candidate profile against the XML corpus before promoting it.",
    )
    simulate_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    simulate_parser.add_argument("--output", required=True, type=Path, help="Output directory for simulation artifacts.")
    simulate_parser.add_argument("--analysis-root", type=Path, help="Output directory or analysis directory that contains proposals/candidate_profile.json.")
    simulate_parser.add_argument("--candidate-profile", type=Path, help="Optional explicit candidate_profile.json path.")
    simulate_parser.add_argument("--entry-file", help="Optional XML filename to emphasize in simulation.")
    simulate_parser.add_argument("--entry-main-query", help="Optional main-query name to emphasize in simulation.")

    grade_parser = subparsers.add_parser(
        "grade-profile",
        help="Grade a profile lifecycle state from a validation or simulation report.",
    )
    grade_parser.add_argument("--profile", required=True, type=Path, help="Profile JSON to grade.")
    grade_parser.add_argument("--report", required=True, type=Path, help="profile_validation.json, profile_simulation.json, or a directory that contains one.")
    grade_parser.add_argument("--output", required=True, type=Path, help="Output directory for grade artifacts.")

    promote_parser = subparsers.add_parser(
        "promote-profile",
        help="Promote a profile to its next lifecycle state using a grade report.",
    )
    promote_parser.add_argument("--profile", required=True, type=Path, help="Profile JSON to promote.")
    promote_parser.add_argument("--grade-report", required=True, type=Path, help="profile_grade.json or a directory that contains it.")
    promote_parser.add_argument("--output", required=True, type=Path, help="Output path for the promoted profile JSON.")
    promote_parser.add_argument("--profile-name", help="Optional profile display name to stamp into the promoted profile.")

    rollback_parser = subparsers.add_parser(
        "rollback-profile",
        help="Rollback a promoted profile to its parent profile or an explicit target profile.",
    )
    rollback_parser.add_argument("--profile", required=True, type=Path, help="Current promoted profile JSON to rollback from.")
    rollback_parser.add_argument("--output", required=True, type=Path, help="Output path for the rolled-back profile JSON.")
    rollback_parser.add_argument("--target-profile", type=Path, help="Optional explicit rollback target profile JSON. Defaults to parent_profile.")
    rollback_parser.add_argument("--reason", help="Optional rollback reason to stamp into lifecycle history.")
    rollback_parser.add_argument("--profile-name", help="Optional profile display name for the rolled-back profile.")

    explain_failure_parser = subparsers.add_parser(
        "explain-failure",
        help="Summarize loop, review, and provider failures into actionable explanations and company-LLM prompts.",
    )
    explain_failure_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis artifacts.")
    explain_failure_parser.add_argument("--scope", choices=["all", "generic", "java-bff"], default="all", help="Failure scope to summarize.")

    emit_company_prompt_parser = subparsers.add_parser(
        "emit-company-prompt",
        help="Generate a company weak-LLM prompt pack from a generic cluster or a Java phase pack.",
    )
    emit_company_prompt_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    emit_company_prompt_parser.add_argument("--cluster", help="Generic cluster_id.")
    emit_company_prompt_parser.add_argument("--stage", choices=["classify", "propose", "verify"], help="Generic stage when --cluster is used.")
    emit_company_prompt_parser.add_argument("--prompt-json", type=Path, help="Java BFF phase prompt JSON path.")
    emit_company_prompt_parser.add_argument("--profile-name", default="company-qwen3-java-phase", help="Company weak-model prompt profile.")
    emit_company_prompt_parser.add_argument("--output-dir", type=Path, help="Optional output directory for the generated handoff pack.")

    repair_company_prompt_parser = subparsers.add_parser(
        "repair-company-prompt",
        help="Generate a repair pack from a rejected generic or Java review JSON.",
    )
    repair_company_prompt_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    repair_company_prompt_parser.add_argument("--review", required=True, type=Path, help="Review JSON path.")
    repair_company_prompt_parser.add_argument("--profile-name", default="company-qwen3-verify", help="Company weak-model prompt profile.")
    repair_company_prompt_parser.add_argument("--output-dir", type=Path, help="Optional output directory for the generated handoff pack.")

    export_handoff_parser = subparsers.add_parser(
        "export-vscode-cline-pack",
        help="Export a handoff pack for Cline CLI or VS Code Cline from a cluster, Java phase pack, or review JSON.",
    )
    export_handoff_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    export_handoff_parser.add_argument("--cluster", help="Generic cluster_id.")
    export_handoff_parser.add_argument("--stage", choices=["classify", "propose", "verify"], help="Generic stage when --cluster is used.")
    export_handoff_parser.add_argument("--prompt-json", type=Path, help="Java BFF phase prompt JSON path.")
    export_handoff_parser.add_argument("--review", type=Path, help="Review JSON path used to create a repair handoff pack.")
    export_handoff_parser.add_argument("--profile-name", default="company-qwen3-java-phase", help="Company weak-model prompt profile.")
    export_handoff_parser.add_argument("--output-dir", type=Path, help="Optional output directory for the generated handoff pack.")

    doctor_parser = subparsers.add_parser(
        "doctor-run",
        help="Inspect an analysis output directory and generate actionable operator guidance.",
    )
    doctor_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis artifacts.")

    watch_review_parser = subparsers.add_parser(
        "watch-and-review",
        help="Watch for a response file, review it automatically, and optionally emit a repair handoff pack.",
    )
    watch_review_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    watch_review_parser.add_argument("--response", required=True, type=Path, help="Response file to wait for and review.")
    watch_review_parser.add_argument("--cluster", help="Generic cluster_id.")
    watch_review_parser.add_argument("--stage", choices=["classify", "propose", "verify"], help="Generic stage when --cluster is used.")
    watch_review_parser.add_argument("--prompt-json", type=Path, help="Java BFF phase prompt JSON path.")
    watch_review_parser.add_argument("--timeout-seconds", type=float, default=300.0, help="Maximum time to wait for the response file.")
    watch_review_parser.add_argument("--poll-seconds", type=float, default=2.0, help="Polling interval while waiting for the response file.")
    watch_review_parser.add_argument("--no-repair-pack", action="store_true", help="Do not emit a repair handoff pack when review fails.")

    adaptive_context_parser = subparsers.add_parser(
        "compile-adaptive-context",
        help="Generate multiple token-budgeted context variants for a generic cluster or Java BFF phase pack.",
    )
    adaptive_context_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    adaptive_context_parser.add_argument("--cluster", help="Generic cluster_id.")
    adaptive_context_parser.add_argument("--stage", choices=["classify", "propose", "verify"], help="Generic stage when --cluster is used.")
    adaptive_context_parser.add_argument("--prompt-json", type=Path, help="Java BFF phase prompt JSON path.")
    adaptive_context_parser.add_argument("--prompt-profile", default="qwen3-128k-autonomous", help="Prompt profile for generic adaptive variants.")
    adaptive_context_parser.add_argument("--targets", default="8000,16000,24000,48000", help="Comma-separated target token budgets.")

    shrink_prompt_parser = subparsers.add_parser(
        "shrink-prompt",
        help="Shrink a prompt-bearing JSON artifact to a target token budget.",
    )
    shrink_prompt_parser.add_argument("--pack-json", required=True, type=Path, help="JSON artifact that contains prompt_text.")
    shrink_prompt_parser.add_argument("--target-tokens", required=True, type=int, help="Target prompt token budget.")
    shrink_prompt_parser.add_argument("--output-dir", type=Path, help="Optional output directory for the shrunken prompt artifact.")

    context_parser = subparsers.add_parser(
        "compile-context",
        help="Compile a phase-specific context pack for weak 128k-token models.",
    )
    context_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory or analysis directory that contains failure_clusters.json.")
    context_parser.add_argument("--cluster", required=True, help="cluster_id from failure_clusters.json.")
    context_parser.add_argument("--phase", required=True, choices=["classify", "propose", "verify"], help="Phase-specific context pack to compile.")
    context_parser.add_argument("--prompt-profile", default="qwen3-128k-autonomous", help="Prompt profile tuned for the target weak LLM.")

    loop_parser = subparsers.add_parser(
        "run-agent-loop",
        help="Run the autonomous multi-phase agent loop until required artifacts are produced or a stop condition is reached.",
    )
    loop_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    loop_parser.add_argument("--output", required=True, type=Path, help="Output directory for analyzer and loop artifacts.")
    loop_parser.add_argument("--profile", type=Path, help="Optional frozen or promoted profile JSON.")
    loop_parser.add_argument("--runner-mode", choices=["provider", "cline_bridge"], default="provider", help="Execution mode for LLM tasks.")
    loop_parser.add_argument("--prompt-profile", default="qwen3-128k-autonomous", help="Prompt profile tuned for the target weak LLM.")
    loop_parser.add_argument("--max-iterations", type=int, default=20, help="Maximum loop iterations before stopping.")
    loop_parser.add_argument("--max-attempts-per-task", type=int, default=3, help="Maximum retries per cluster/phase task.")
    loop_parser.add_argument("--provider-config", type=Path, help="Optional provider config for provider mode.")
    loop_parser.add_argument("--provider-base-url", help="Provider base URL or /v1 root for OpenAI-compatible mode.")
    loop_parser.add_argument("--provider-api-key", help="Provider API key.")
    loop_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name for the provider API key.")
    loop_parser.add_argument("--provider-model", help="Provider model name.")
    loop_parser.add_argument("--provider-name", help="Optional provider label for artifacts.")
    loop_parser.add_argument("--token-limit", type=int, help="Optional completion token limit override for provider mode.")
    loop_parser.add_argument("--temperature", type=float, help="Optional provider temperature override.")
    loop_parser.add_argument("--timeout-seconds", type=float, help="Optional provider timeout override.")
    add_cline_bridge_arguments(loop_parser)

    resume_parser = subparsers.add_parser(
        "resume-agent-loop",
        help="Resume a previously started autonomous agent loop from loop_state.json.",
    )
    resume_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis/agent_loop/loop_state.json.")
    resume_parser.add_argument("--provider-config", type=Path, help="Optional provider config override for resumed provider-mode runs.")
    resume_parser.add_argument("--provider-base-url", help="Optional provider base URL override.")
    resume_parser.add_argument("--provider-api-key", help="Optional provider API key override.")
    resume_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name for the provider API key.")
    resume_parser.add_argument("--provider-model", help="Optional provider model override.")
    resume_parser.add_argument("--provider-name", help="Optional provider label override.")
    resume_parser.add_argument("--token-limit", type=int, help="Optional completion token limit override.")
    resume_parser.add_argument("--temperature", type=float, help="Optional provider temperature override.")
    resume_parser.add_argument("--timeout-seconds", type=float, help="Optional provider timeout override.")
    add_cline_bridge_arguments(resume_parser)

    inspect_parser = subparsers.add_parser(
        "inspect-agent-loop",
        help="Inspect loop_state and phase history for an autonomous agent loop run.",
    )
    inspect_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis/agent_loop/loop_state.json.")

    java_bff_parser = subparsers.add_parser(
        "prepare-java-bff",
        help="Analyze SQL XML and emit chunked Java Spring Boot BFF artifacts for weak 128k-token models.",
    )
    java_bff_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    java_bff_parser.add_argument("--output", required=True, type=Path, help="Output directory for analysis and Java BFF artifacts.")
    java_bff_parser.add_argument("--profile", type=Path, help="Optional frozen or promoted profile JSON.")
    java_bff_parser.add_argument("--entry-file", help="Optional XML filename to focus the Java BFF pack.")
    java_bff_parser.add_argument("--entry-main-query", help="Optional main-query name to focus the Java BFF pack.")
    java_bff_parser.add_argument("--prompt-profile", default="qwen3-128k-java-bff", help="Prompt profile tuned for the target weak LLM.")
    java_bff_parser.add_argument("--max-sql-chunk-tokens", type=int, help="Optional SQL chunk token cap for phase-2 repository prompts.")

    compile_java_context_parser = subparsers.add_parser(
        "compile-java-bff-context",
        help="Compile a phase-specific Java BFF context pack that stays within weak-model token budgets.",
    )
    compile_java_context_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    compile_java_context_parser.add_argument("--prompt-json", required=True, type=Path, help="Java BFF phase prompt JSON path.")
    compile_java_context_parser.add_argument("--prompt-profile", help="Optional prompt profile override.")
    compile_java_context_parser.add_argument("--max-input-tokens", type=int, help="Optional max input token override.")

    invoke_java_parser = subparsers.add_parser(
        "invoke-java-bff",
        help="Send a Java BFF phase prompt to an OpenAI-compatible provider and optionally review the response.",
    )
    invoke_java_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    invoke_java_parser.add_argument("--prompt-json", required=True, type=Path, help="Java BFF phase prompt JSON path.")
    invoke_java_parser.add_argument("--provider-config", type=Path, help="Optional provider config for the OpenAI-compatible provider.")
    invoke_java_parser.add_argument("--provider-base-url", help="Provider base URL or /v1 root.")
    invoke_java_parser.add_argument("--provider-api-key", help="Provider API key.")
    invoke_java_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name for the provider API key.")
    invoke_java_parser.add_argument("--provider-model", help="Provider model name.")
    invoke_java_parser.add_argument("--provider-name", help="Optional provider label.")
    invoke_java_parser.add_argument("--token-limit", type=int, help="Optional completion token limit override.")
    invoke_java_parser.add_argument("--temperature", type=float, help="Optional provider temperature override.")
    invoke_java_parser.add_argument("--timeout-seconds", type=float, help="Optional provider timeout override.")
    invoke_java_parser.add_argument("--review", action="store_true", help="Immediately run Java BFF review on the saved response text.")

    review_java_parser = subparsers.add_parser(
        "review-java-bff-response",
        help="Review a Java BFF phase response against its phase schema and merge-safety rules.",
    )
    review_java_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    review_java_parser.add_argument("--prompt-json", required=True, type=Path, help="Java BFF phase prompt JSON path.")
    review_java_parser.add_argument("--response", required=True, type=Path, help="Path to the LLM response text or JSON file.")

    merge_java_parser = subparsers.add_parser(
        "merge-java-bff-phases",
        help="Merge accepted Java BFF phase reviews into repository, BFF, and verification plans.",
    )
    merge_java_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    merge_java_parser.add_argument("--bundle-id", required=True, help="Java BFF bundle id from overview.json.")

    skeleton_java_parser = subparsers.add_parser(
        "generate-java-bff-skeleton",
        help="Generate Java Spring Boot skeleton files from a merged Java BFF implementation plan.",
    )
    skeleton_java_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    skeleton_java_parser.add_argument("--bundle-id", required=True, help="Java BFF bundle id from overview.json.")
    skeleton_java_parser.add_argument("--package-name", default="com.example.legacybff", help="Java package name for generated skeleton files.")

    starter_java_parser = subparsers.add_parser(
        "generate-java-bff-starter",
        help="Generate a starter Spring Boot project scaffold, SQL resource placeholders, and verification artifacts for a merged Java BFF bundle.",
    )
    starter_java_parser.add_argument("--analysis-root", required=True, type=Path, help="Output directory, analysis directory, or java_bff directory.")
    starter_java_parser.add_argument("--bundle-id", required=True, help="Java BFF bundle id from overview.json.")
    starter_java_parser.add_argument("--package-name", default="com.example.legacybff", help="Java package name for generated starter project files.")

    java_loop_parser = subparsers.add_parser(
        "run-java-bff-loop",
        help="Run the autonomous Java BFF phase loop until merged plans and skeletons are complete or a stop condition is reached.",
    )
    java_loop_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    java_loop_parser.add_argument("--output", required=True, type=Path, help="Output directory for analysis and Java BFF artifacts.")
    java_loop_parser.add_argument("--profile", type=Path, help="Optional frozen or promoted profile JSON.")
    java_loop_parser.add_argument("--prompt-profile", default="qwen3-128k-java-bff", help="Prompt profile tuned for the target weak LLM.")
    java_loop_parser.add_argument("--max-iterations", type=int, default=64, help="Maximum Java BFF loop iterations before stopping.")
    java_loop_parser.add_argument("--max-attempts-per-prompt", type=int, default=3, help="Maximum retries for a single Java BFF phase prompt.")
    java_loop_parser.add_argument("--runner-mode", choices=["provider", "cline_bridge"], default="provider", help="Execution mode for Java BFF prompts.")
    java_loop_parser.add_argument("--provider-config", type=Path, help="Optional provider config for provider mode.")
    java_loop_parser.add_argument("--provider-base-url", help="Provider base URL or /v1 root.")
    java_loop_parser.add_argument("--provider-api-key", help="Provider API key.")
    java_loop_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name for the provider API key.")
    java_loop_parser.add_argument("--provider-model", help="Provider model name.")
    java_loop_parser.add_argument("--provider-name", help="Optional provider label.")
    java_loop_parser.add_argument("--token-limit", type=int, help="Optional completion token limit override.")
    java_loop_parser.add_argument("--temperature", type=float, help="Optional provider temperature override.")
    java_loop_parser.add_argument("--timeout-seconds", type=float, help="Optional provider timeout override.")
    add_cline_bridge_arguments(java_loop_parser)
    java_loop_parser.add_argument("--package-name", default="com.example.legacybff", help="Java package name for generated skeleton files.")
    java_loop_parser.add_argument("--entry-file", help="Optional XML filename to focus the Java BFF pack.")
    java_loop_parser.add_argument("--entry-main-query", help="Optional main-query name to focus the Java BFF pack.")
    java_loop_parser.add_argument("--max-sql-chunk-tokens", type=int, help="Optional SQL chunk token cap for phase-2 repository prompts.")

    resume_java_loop_parser = subparsers.add_parser(
        "resume-java-bff-loop",
        help="Resume a previously started Java BFF autonomous loop from loop_state.json.",
    )
    resume_java_loop_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis/java_bff/loop/loop_state.json.")
    resume_java_loop_parser.add_argument("--provider-config", type=Path, help="Optional provider config override.")
    resume_java_loop_parser.add_argument("--provider-base-url", help="Optional provider base URL override.")
    resume_java_loop_parser.add_argument("--provider-api-key", help="Optional provider API key override.")
    resume_java_loop_parser.add_argument("--provider-api-key-env", default="OPENAI_API_KEY", help="Environment variable name for the provider API key.")
    resume_java_loop_parser.add_argument("--provider-model", help="Optional provider model override.")
    resume_java_loop_parser.add_argument("--provider-name", help="Optional provider label override.")
    resume_java_loop_parser.add_argument("--token-limit", type=int, help="Optional completion token limit override.")
    resume_java_loop_parser.add_argument("--temperature", type=float, help="Optional provider temperature override.")
    resume_java_loop_parser.add_argument("--timeout-seconds", type=float, help="Optional provider timeout override.")
    add_cline_bridge_arguments(resume_java_loop_parser)
    resume_java_loop_parser.add_argument("--package-name", help="Optional package name override.")

    inspect_java_loop_parser = subparsers.add_parser(
        "inspect-java-bff-loop",
        help="Inspect Java BFF loop state and history.",
    )
    inspect_java_loop_parser.add_argument("--output", required=True, type=Path, help="Output directory that contains analysis/java_bff/loop/loop_state.json.")
    apply_common_runtime_flags(
        [
            analyze_parser,
            learn_parser,
            infer_parser,
            freeze_parser,
            validate_parser,
            serve_parser,
            prompt_parser,
            invoke_parser,
            validate_provider_parser,
            review_parser,
            propose_parser,
            apply_patch_parser,
            simulate_parser,
            grade_parser,
            promote_parser,
            rollback_parser,
            explain_failure_parser,
            emit_company_prompt_parser,
            repair_company_prompt_parser,
            export_handoff_parser,
            doctor_parser,
            watch_review_parser,
            adaptive_context_parser,
            shrink_prompt_parser,
            context_parser,
            loop_parser,
            resume_parser,
            inspect_parser,
            java_bff_parser,
            compile_java_context_parser,
            invoke_java_parser,
            review_java_parser,
            merge_java_parser,
            skeleton_java_parser,
            starter_java_parser,
            java_loop_parser,
            resume_java_loop_parser,
            inspect_java_loop_parser,
        ]
    )
    return parser


def apply_common_runtime_flags(parsers: list[argparse.ArgumentParser]) -> None:
    for parser in parsers:
        parser.add_argument("--verbose", action="store_true", help="Print extra debug information and traceback details on failure.")
        parser.add_argument("--no-progress", action="store_true", help="Suppress progress lines while the command is running.")


def build_reporter(args: argparse.Namespace) -> ConsoleReporter:
    return ConsoleReporter(verbose=bool(getattr(args, "verbose", False)), progress_enabled=not bool(getattr(args, "no_progress", False)))


def emit_command_start(reporter: ConsoleReporter, command: str, **fields: object) -> None:
    reporter.progress(command, "starting", **fields)


def summarize_loop_result(
    reporter: ConsoleReporter,
    *,
    label: str,
    payload: dict[str, object],
    completion_path: Path,
) -> int:
    status = str(payload.get("status") or "unknown")
    stop_reason = str(payload.get("stop_reason") or "n/a")
    missing_count = len(payload.get("missing_artifacts", [])) if isinstance(payload.get("missing_artifacts"), list) else 0
    iterations = payload.get("iterations", payload.get("iteration_count", "n/a"))
    message = (
        f"{label} finished with status={status} stop_reason={stop_reason} "
        f"iterations={iterations} missing_artifacts={missing_count}."
    )
    if status == "completed":
        reporter.success(message)
        reporter.detail(f"Completion report: {completion_path}")
        return 0
    reporter.warning(message)
    last_error = payload.get("last_error")
    if isinstance(last_error, dict) and last_error:
        reporter.error(
            "Last error: "
            + ", ".join(
                f"{key}={value}" for key, value in last_error.items() if value is not None and value != ""
            )
        )
    reporter.info(f"Completion report: {completion_path}")
    return 1 if status == "failed" else 2


def build_command_artifact_hints(args: argparse.Namespace) -> list[str]:
    hints: list[str] = []
    command = str(getattr(args, "command", ""))
    if command in {"run-agent-loop", "resume-agent-loop", "inspect-agent-loop"} and getattr(args, "output", None):
        output = args.output.resolve()
        hints.extend(
            [
                str(output / "analysis" / "agent_loop" / "loop_state.json"),
                str(output / "analysis" / "agent_loop" / "completion_report.md"),
            ]
        )
    if command in {"run-java-bff-loop", "resume-java-bff-loop", "inspect-java-bff-loop"} and getattr(args, "output", None):
        output = args.output.resolve()
        hints.extend(
            [
                str(output / "analysis" / "java_bff" / "loop" / "loop_state.json"),
                str(output / "analysis" / "java_bff" / "loop" / "completion_report.md"),
            ]
        )
    if command == "doctor-run" and getattr(args, "output", None):
        output = args.output.resolve()
        hints.extend(
            [
                str(output / "analysis" / "doctor" / "doctor_report.json"),
                str(output / "analysis" / "failure_explanations" / "index.md"),
            ]
        )
    if command == "watch-and-review" and getattr(args, "analysis_root", None):
        analysis_root = resolve_analysis_root(args.analysis_root.resolve())
        hints.extend(
            [
                str(analysis_root / "watch_review"),
                str(analysis_root / "handoff"),
            ]
        )
    if command == "validate-provider" and getattr(args, "output", None):
        output = args.output.resolve()
        hints.append(str(output / "analysis" / "provider_validation"))
    if command in {"invoke-llm", "invoke-java-bff"} and getattr(args, "analysis_root", None):
        analysis_root = args.analysis_root.resolve()
        hints.append(str(analysis_root / "llm_runs"))
    return hints


def build_error_hints(args: argparse.Namespace, exc: Exception) -> list[str]:
    command = str(getattr(args, "command", ""))
    hints: list[str] = []
    if isinstance(exc, LlmProviderError):
        hints.append("Run `validate-provider` with the same provider settings to isolate config, endpoint, or response-shape problems.")
    if isinstance(exc, FileNotFoundError):
        hints.append("Check whether the input path or a generated artifact path exists and whether the previous step finished successfully.")
    if type(exc).__name__ == "JSONDecodeError":
        hints.append("Inspect the referenced JSON file or provider response for truncation, HTML error pages, or malformed content.")
    message = str(exc).lower()
    if "api key" in message:
        hints.append("Confirm the provider API key or configured env var is present in this shell session.")
    if "choices[0].message.content" in message or "non-json" in message:
        hints.append("The provider response may not be OpenAI-compatible enough; inspect the saved response artifact or validate-provider debug output.")
    if command in {"run-agent-loop", "resume-agent-loop"}:
        hints.append("Use `inspect-agent-loop` after a stopped run to see the latest phase state and history.")
    if command in {"run-java-bff-loop", "resume-java-bff-loop"}:
        hints.append("Use `inspect-java-bff-loop` after a stopped run to inspect the latest prompt, review, and missing artifacts.")
    if command == "watch-and-review":
        hints.append("If review fails, inspect the generated repair pack under analysis/handoff and retry the same phase with a smaller context.")
    if not hints:
        hints.append("Inspect the command-specific output artifacts and rerun with `--verbose` for the traceback.")
    return hints


def handle_cli_exception(args: argparse.Namespace, reporter: ConsoleReporter, exc: Exception) -> int:
    reporter.exception(
        exc,
        hints=build_error_hints(args, exc),
        artifact_paths=build_command_artifact_hints(args),
    )
    return 1


def dispatch_command(args: argparse.Namespace, parser: argparse.ArgumentParser, reporter: ConsoleReporter) -> int:
    if args.command == "analyze":
        emit_command_start(reporter, "analyze", input=args.input.resolve(), output=args.output.resolve())
        result = analyze_directory(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            strict=args.strict,
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
            profile_path=args.profile.resolve() if args.profile else None,
            snapshot_label=args.snapshot_label,
        )
        error_count = sum(1 for diag in result.diagnostics if diag.severity in {"error", "fatal"})
        warning_count = sum(1 for diag in result.diagnostics if diag.severity == "warning")
        reporter.success(
            f"Analyzed {len(result.files)} file(s), discovered {len(result.queries)} query node(s), "
            f"generated {len(result.artifacts)} artifact(s), errors={error_count}, warnings={warning_count}."
        )
        reporter.detail(f"Executive summary: {args.output.resolve() / 'analysis' / 'executive_summary.md'}")
        if args.strict and error_count:
            reporter.warning("Strict mode detected error/fatal diagnostics. See analysis/markdown/diagnostics for details.")
            return 2
        return 0

    if args.command == "learn":
        emit_command_start(reporter, "learn", input=args.input.resolve(), output=args.output.resolve())
        result = learn_directory(args.input.resolve(), args.output.resolve())
        reporter.success(
            f"Learned from {result['observations']['summary']['xml_file_count']} file(s), "
            f"generated {len(result['artifacts'])} learning artifact(s)."
        )
        return 0

    if args.command == "infer-rules":
        emit_command_start(reporter, "infer-rules", input=args.input.resolve(), output=args.output.resolve())
        result = infer_rules(args.input.resolve(), args.output.resolve())
        reporter.success(
            f"Inferred {len(result['profile'].rules)} rule candidate(s), "
            f"generated {len(result['artifacts'])} rule artifact(s)."
        )
        return 0

    if args.command == "freeze-profile":
        emit_command_start(reporter, "freeze-profile", input=args.input.resolve(), output=args.output.resolve())
        profile = freeze_profile(args.input.resolve(), args.output.resolve(), args.min_confidence)
        reporter.success(
            f"Froze profile with {len(profile.rules)} retained rule(s) at confidence >= {args.min_confidence:.2f}."
        )
        return 0

    if args.command == "validate-profile":
        emit_command_start(reporter, "validate-profile", input=args.input.resolve(), output=args.output.resolve())
        result = validate_profile(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            profile_path=args.profile.resolve(),
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
        )
        classification = result["assessment"]["classification"]
        reporter.success(
            f"Validated profile with classification={classification}, "
            f"generated {len(result['artifacts'])} validation artifact(s)."
        )
        if args.fail_on_regression and classification == "regressed":
            reporter.warning("Validation detected regression. See validation/profile_validation.md for details.")
            return 3
        return 0

    if args.command == "serve-report":
        emit_command_start(reporter, "serve-report", root=args.root.resolve(), host=args.host, port=args.port)
        serve_report(root=args.root.resolve(), host=args.host, port=args.port)
        return 0

    if args.command == "prepare-prompt":
        emit_command_start(reporter, "prepare-prompt", cluster=args.cluster, budget=args.budget)
        result = prepare_prompt_pack_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            budget=args.budget,
            model=args.model,
        )
        reporter.success(
            f"Prepared prompt pack for cluster={result['cluster']['cluster_id']}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "invoke-llm":
        emit_command_start(reporter, "invoke-llm", cluster=args.cluster, stage=args.stage, budget=args.budget)
        result = invoke_llm_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            stage=args.stage,
            budget=args.budget,
            prompt_model=args.prompt_model,
            provider_config_path=args.provider_config.resolve() if args.provider_config else None,
            provider_base_url=args.provider_base_url,
            provider_api_key=args.provider_api_key,
            provider_api_key_env=args.provider_api_key_env,
            provider_model=args.provider_model,
            provider_name=args.provider_name,
            token_limit=args.token_limit,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            review=args.review,
            profile_path=args.profile.resolve() if args.profile else None,
        )
        summary = result["run_summary"]
        reporter.success(
            f"Invoked provider={summary['provider_name']} model={summary['provider_model']} "
            f"for cluster={summary['cluster_id']} stage={summary['stage']} token_limit={summary['token_limit']}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        reporter.detail(f"LLM run summary: {summary.get('prompt_path')}")
        return 0

    if args.command == "validate-provider":
        emit_command_start(reporter, "validate-provider", output=args.output.resolve())
        result = validate_provider_connection(
            output_dir=args.output.resolve(),
            provider_config_path=args.provider_config.resolve() if args.provider_config else None,
            provider_base_url=args.provider_base_url,
            provider_api_key=args.provider_api_key,
            provider_api_key_env=args.provider_api_key_env,
            provider_model=args.provider_model,
            provider_name=args.provider_name,
            token_limit=args.token_limit,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            prompt_text=args.prompt_text,
            expect_json=not args.no_expect_json,
        )
        summary = result["summary"]
        if summary["status"] == "failed":
            reporter.warning(
                f"Validated provider={summary['provider_name']} model={summary['provider_model']} "
                f"status={summary['status']} checks={len(summary.get('checks', []))}."
            )
            reporter.info(f"Provider debug: {summary.get('debug_path') or 'n/a'}")
            return 1
        reporter.success(
            f"Validated provider={summary['provider_name']} model={summary['provider_model']} "
            f"status={summary['status']} checks={len(summary.get('checks', []))}."
        )
        reporter.detail(f"Provider debug: {summary.get('debug_path') or 'n/a'}")
        return 0

    if args.command == "review-llm-response":
        emit_command_start(reporter, "review-llm-response", cluster=args.cluster, stage=args.stage)
        result = review_llm_response_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            response_path=args.response.resolve(),
            stage=args.stage,
            budget=args.budget,
            model=args.model,
            profile_path=args.profile.resolve() if args.profile else None,
        )
        review = result["review"]
        reporter.success(
            f"Reviewed response for cluster={result['cluster']['cluster_id']} stage={review['stage']} "
            f"status={review['status']}, issues={len(review['issues'])}, "
            f"safe_to_apply_candidate={review['safe_to_apply_candidate']}."
        )
        return 0

    if args.command == "propose-rules":
        emit_command_start(reporter, "propose-rules", analysis_root=args.analysis_root.resolve())
        result = propose_rules_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
            min_confidence=args.min_confidence,
            include_needs_review=args.include_needs_review,
        )
        reporter.success(
            f"Proposed {result['proposal_payload']['summary']['accepted_patch_count']} patch(es), "
            f"candidate profile rules={len(result['candidate_profile'].rules)}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "apply-profile-patch":
        emit_command_start(reporter, "apply-profile-patch", output=args.output.resolve())
        profile = apply_profile_patch_bundle(
            patch_bundle_path=args.patch_bundle.resolve(),
            output_path=args.output.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
        )
        reporter.success(f"Wrote merged profile with {len(profile.rules)} rule(s) to {args.output.resolve()}.")
        return 0

    if args.command == "simulate-profile":
        emit_command_start(reporter, "simulate-profile", input=args.input.resolve(), output=args.output.resolve())
        result = simulate_candidate_profile(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            analysis_root=args.analysis_root.resolve() if args.analysis_root else None,
            candidate_profile_path=args.candidate_profile.resolve() if args.candidate_profile else None,
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
        )
        reporter.success(
            f"Simulated candidate profile with classification={result['assessment']['classification']}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "grade-profile":
        emit_command_start(reporter, "grade-profile", profile=args.profile.resolve(), output=args.output.resolve())
        result = grade_profile(
            profile_path=args.profile.resolve(),
            validation_report_path=args.report.resolve(),
            output_dir=args.output.resolve(),
        )
        payload = result["grade_payload"]
        reporter.success(
            f"Graded profile status={payload['current_status']} -> {payload['suggested_status']}, "
            f"readiness={payload['promotion_readiness']}, generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "promote-profile":
        emit_command_start(reporter, "promote-profile", output=args.output.resolve())
        profile = promote_profile(
            profile_path=args.profile.resolve(),
            grade_report_path=args.grade_report.resolve(),
            output_path=args.output.resolve(),
            profile_name=args.profile_name,
        )
        reporter.success(
            f"Promoted profile to status={profile.profile_status} with {len(profile.validation_history)} validation record(s)."
        )
        return 0

    if args.command == "rollback-profile":
        emit_command_start(reporter, "rollback-profile", output=args.output.resolve())
        profile = rollback_profile(
            profile_path=args.profile.resolve(),
            output_path=args.output.resolve(),
            target_profile_path=args.target_profile.resolve() if args.target_profile else None,
            reason=args.reason,
            profile_name=args.profile_name,
        )
        reporter.success(
            f"Rolled back profile to status={profile.profile_status} with {len(profile.lifecycle_history)} lifecycle event(s)."
        )
        return 0

    if args.command == "explain-failure":
        emit_command_start(reporter, "explain-failure", output=args.output.resolve(), scope=args.scope)
        payload = explain_failure_from_output_dir(args.output.resolve(), scope=args.scope)
        reporter.success(
            f"Generated {payload['index']['count']} failure explanation(s) at {payload['index_json_path']}."
        )
        return 0

    if args.command == "emit-company-prompt":
        emit_command_start(reporter, "emit-company-prompt", analysis_root=args.analysis_root.resolve())
        if args.cluster and not args.stage:
            raise ValueError("--stage is required when --cluster is used.")
        payload = export_vscode_cline_pack(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            stage=args.stage,
            prompt_json=args.prompt_json.resolve() if args.prompt_json else None,
            output_dir=args.output_dir.resolve() if args.output_dir else None,
            profile_name=args.profile_name,
        )
        reporter.success(
            f"Generated company prompt pack '{payload['title']}' with {len(payload['written_paths'])} file(s)."
        )
        return 0

    if args.command == "repair-company-prompt":
        emit_command_start(reporter, "repair-company-prompt", review=args.review.resolve())
        payload = export_vscode_cline_pack(
            analysis_root=args.analysis_root.resolve(),
            review_path=args.review.resolve(),
            output_dir=args.output_dir.resolve() if args.output_dir else None,
            profile_name=args.profile_name,
        )
        reporter.success(
            f"Generated repair prompt pack '{payload['title']}' with {len(payload['written_paths'])} file(s)."
        )
        return 0

    if args.command == "export-vscode-cline-pack":
        emit_command_start(reporter, "export-vscode-cline-pack", analysis_root=args.analysis_root.resolve())
        if args.cluster and not args.stage:
            raise ValueError("--stage is required when --cluster is used.")
        payload = export_vscode_cline_pack(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            stage=args.stage,
            prompt_json=args.prompt_json.resolve() if args.prompt_json else None,
            review_path=args.review.resolve() if args.review else None,
            output_dir=args.output_dir.resolve() if args.output_dir else None,
            profile_name=args.profile_name,
        )
        reporter.success(
            f"Exported handoff pack '{payload['title']}' with {len(payload['written_paths'])} file(s)."
        )
        return 0

    if args.command == "doctor-run":
        emit_command_start(reporter, "doctor-run", output=args.output.resolve())
        payload = doctor_run(args.output.resolve())
        reporter.success(
            f"Doctor status={payload['status']} actions={len(payload['recommended_actions'])} report={payload['json_path']}."
        )
        return 0

    if args.command == "watch-and-review":
        emit_command_start(reporter, "watch-and-review", response=args.response.resolve())
        if args.cluster and not args.stage:
            raise ValueError("--stage is required when --cluster is used.")
        payload = watch_and_review(
            analysis_root=args.analysis_root.resolve(),
            response_path=args.response.resolve(),
            cluster_id=args.cluster,
            stage=args.stage,
            prompt_json=args.prompt_json.resolve() if args.prompt_json else None,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            emit_repair_pack=not args.no_repair_pack,
        )
        reporter.success(
            f"Reviewed watched response kind={payload['kind']} status={payload['status']} report={payload['json_path']}."
        )
        return 0

    if args.command == "compile-adaptive-context":
        emit_command_start(reporter, "compile-adaptive-context", analysis_root=args.analysis_root.resolve())
        targets = [int(item.strip()) for item in str(args.targets).split(",") if item.strip()]
        if args.cluster:
            if not args.stage:
                raise ValueError("--stage is required when --cluster is used.")
            payload = compile_adaptive_generic_context(
                analysis_root=args.analysis_root.resolve(),
                cluster_id=args.cluster,
                phase=args.stage,
                prompt_profile=args.prompt_profile,
                targets=targets,
            )
        elif args.prompt_json:
            payload = compile_adaptive_java_context(
                analysis_root=args.analysis_root.resolve(),
                prompt_json=args.prompt_json.resolve(),
                prompt_profile=None if args.prompt_profile == "qwen3-128k-autonomous" else args.prompt_profile,
                targets=targets,
            )
        else:
            raise ValueError("Provide either --cluster with --stage or --prompt-json.")
        paths = write_adaptive_payload(args.analysis_root.resolve(), payload)
        reporter.success(
            f"Generated {len(payload['variants'])} adaptive variant(s) and {len(paths)} artifact(s)."
        )
        return 0

    if args.command == "shrink-prompt":
        emit_command_start(reporter, "shrink-prompt", pack=args.pack_json.resolve(), target=args.target_tokens)
        payload = json.loads(args.pack_json.resolve().read_text(encoding="utf-8"))
        if not isinstance(payload, dict) or "prompt_text" not in payload:
            raise ValueError("pack-json must be a JSON object that contains prompt_text.")
        shrunk = shrink_prompt_text(str(payload["prompt_text"]), args.target_tokens)
        output_root = (args.output_dir.resolve() if args.output_dir else resolve_analysis_root(args.pack_json.resolve().parents[1]))
        adaptive_payload = {
            "generated_at": payload.get("generated_at"),
            "kind": "shrunk_prompt",
            "source_pack": str(args.pack_json.resolve()),
            "variants": [shrunk],
        }
        paths = write_adaptive_payload(output_root, adaptive_payload)
        reporter.success(
            f"Shrank prompt to estimated_tokens={shrunk['estimated_tokens']} target={args.target_tokens}, artifacts={len(paths)}."
        )
        return 0

    if args.command == "compile-context":
        emit_command_start(reporter, "compile-context", cluster=args.cluster, phase=args.phase)
        analysis_root = args.analysis_root.resolve()
        budget = phase_budget_for(args.prompt_profile, args.phase)
        pack = compile_context_pack_from_analysis(
            analysis_root=analysis_root,
            cluster_id=args.cluster,
            phase=args.phase,
            prompt_profile=args.prompt_profile,
        )
        paths = write_context_pack(analysis_root.parent if analysis_root.name == "analysis" else analysis_root, pack)
        reporter.success(
            f"Compiled context pack for cluster={args.cluster} phase={args.phase} "
            f"estimated_tokens={pack.estimated_tokens}/{budget['usable_input_limit']} "
            f"generated {len(paths)} artifact(s)."
        )
        return 0

    if args.command == "run-agent-loop":
        emit_command_start(reporter, "run-agent-loop", input=args.input.resolve(), output=args.output.resolve())
        cline_bridge_command = resolve_cline_bridge_command(
            args,
            output_dir=args.output.resolve(),
            mode="generic",
            runner_mode=args.runner_mode,
        )
        config = LoopConfig(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
            runner_mode=args.runner_mode,
            prompt_profile=args.prompt_profile,
            max_iterations=args.max_iterations,
            max_attempts_per_task=args.max_attempts_per_task,
            provider_config_path=args.provider_config.resolve() if args.provider_config else None,
            provider_base_url=args.provider_base_url,
            provider_api_key=args.provider_api_key,
            provider_api_key_env=args.provider_api_key_env,
            provider_model=args.provider_model,
            provider_name=args.provider_name,
            token_limit=args.token_limit,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            cline_bridge_command=cline_bridge_command,
        )
        payload = run_agent_loop(config, reporter=reporter)
        return summarize_loop_result(
            reporter,
            label="Agent loop",
            payload=payload,
            completion_path=args.output.resolve() / "analysis" / "agent_loop" / "completion_report.md",
        )

    if args.command == "resume-agent-loop":
        emit_command_start(reporter, "resume-agent-loop", output=args.output.resolve())
        inspection = inspect_agent_loop(args.output.resolve())
        persisted_runner_mode = str(inspection["state"]["config"].get("runner_mode") or "provider")
        cline_bridge_command = resolve_cline_bridge_command(
            args,
            output_dir=args.output.resolve(),
            mode="generic",
            runner_mode=persisted_runner_mode,
        )
        payload = resume_agent_loop(
            output_dir=args.output.resolve(),
            config=LoopConfig.from_dict(
                {
                    **inspection["state"]["config"],
                    **{
                        key: value
                        for key, value in {
                            "provider_config_path": str(args.provider_config.resolve()) if args.provider_config else None,
                            "provider_base_url": args.provider_base_url,
                            "provider_api_key": args.provider_api_key,
                            "provider_api_key_env": args.provider_api_key_env,
                            "provider_model": args.provider_model,
                            "provider_name": args.provider_name,
                            "token_limit": args.token_limit,
                            "temperature": args.temperature,
                            "timeout_seconds": args.timeout_seconds,
                            "cline_bridge_command": cline_bridge_command,
                        }.items()
                        if value is not None
                    },
                }
            ),
            reporter=reporter,
        )
        return summarize_loop_result(
            reporter,
            label="Agent loop",
            payload=payload,
            completion_path=args.output.resolve() / "analysis" / "agent_loop" / "completion_report.md",
        )

    if args.command == "inspect-agent-loop":
        payload = inspect_agent_loop(args.output.resolve())
        reporter.success(
            f"Loop status={payload['state']['status']} current_phase={payload['state']['current_phase']} "
            f"history_events={payload['history_count']}."
        )
        return 0

    if args.command == "prepare-java-bff":
        emit_command_start(reporter, "prepare-java-bff", input=args.input.resolve(), output=args.output.resolve())
        payload = prepare_java_bff_from_input(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
            prompt_profile=args.prompt_profile,
            max_sql_chunk_tokens=args.max_sql_chunk_tokens,
        )
        reporter.success(
            f"Prepared Java BFF artifacts with bundles={payload['summary']['bundle_count']} "
            f"chunks={payload['summary']['chunk_count']} prompts={payload['summary']['prompt_count']} "
            f"chunk_token_limit={payload['summary']['chunk_token_limit']}."
        )
        return 0

    if args.command == "compile-java-bff-context":
        emit_command_start(reporter, "compile-java-bff-context", prompt=args.prompt_json.resolve())
        pack = compile_java_bff_context_pack(
            analysis_root=args.analysis_root.resolve(),
            phase_pack_path=args.prompt_json.resolve(),
            prompt_profile=args.prompt_profile,
            max_input_tokens=args.max_input_tokens,
        )
        paths = write_java_bff_context_pack(args.analysis_root.resolve(), pack)
        reporter.success(
            f"Compiled Java BFF context for phase={pack['phase']} bundle={pack['bundle_id']} "
            f"estimated_tokens={pack['estimated_prompt_tokens']}/{pack['budget']['usable_input_limit']} "
            f"missing_inputs={len(pack['missing_inputs'])} artifacts={len(paths)}."
        )
        return 0

    if args.command == "invoke-java-bff":
        emit_command_start(reporter, "invoke-java-bff", prompt=args.prompt_json.resolve())
        result = invoke_java_bff_prompt(
            analysis_root=args.analysis_root.resolve(),
            prompt_json_path=args.prompt_json.resolve(),
            provider_config_path=args.provider_config.resolve() if args.provider_config else None,
            provider_base_url=args.provider_base_url,
            provider_api_key=args.provider_api_key,
            provider_api_key_env=args.provider_api_key_env,
            provider_model=args.provider_model,
            provider_name=args.provider_name,
            token_limit=args.token_limit,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            review=args.review,
        )
        summary = result["run_summary"]
        reporter.success(
            f"Invoked Java BFF provider={summary['provider_name']} phase={summary['phase']} "
            f"bundle={summary['bundle_id']} token_limit={summary['token_limit']}."
        )
        return 0

    if args.command == "review-java-bff-response":
        emit_command_start(reporter, "review-java-bff-response", prompt=args.prompt_json.resolve())
        result = review_java_bff_response_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            prompt_json_path=args.prompt_json.resolve(),
            response_path=args.response.resolve(),
        )
        review = result["review"]
        reporter.success(
            f"Reviewed Java BFF phase={review['phase']} bundle={review['bundle_id']} "
            f"status={review['status']} issues={len(review['issues'])}."
        )
        return 0

    if args.command == "merge-java-bff-phases":
        emit_command_start(reporter, "merge-java-bff-phases", bundle=args.bundle_id)
        result = merge_java_bff_phases(
            analysis_root=args.analysis_root.resolve(),
            bundle_id=args.bundle_id,
        )
        payload = result["implementation_plan"]
        reporter.success(
            f"Merged Java BFF bundle={payload['bundle_id']} status={payload['status']} "
            f"repository_queries={len(payload['repository_plan']['queries'])}."
        )
        return 0

    if args.command == "generate-java-bff-skeleton":
        emit_command_start(reporter, "generate-java-bff-skeleton", bundle=args.bundle_id)
        result = generate_java_bff_skeleton(
            analysis_root=args.analysis_root.resolve(),
            bundle_id=args.bundle_id,
            package_name=args.package_name,
        )
        reporter.success(
            f"Generated Java BFF skeleton for bundle={args.bundle_id} "
            f"artifact_count={len(result['artifacts'])}."
        )
        return 0

    if args.command == "generate-java-bff-starter":
        emit_command_start(reporter, "generate-java-bff-starter", bundle=args.bundle_id)
        result = generate_java_bff_starter(
            analysis_root=args.analysis_root.resolve(),
            bundle_id=args.bundle_id,
            package_name=args.package_name,
        )
        reporter.success(
            f"Generated Java BFF starter for bundle={args.bundle_id} artifact_count={len(result['artifacts'])}."
        )
        return 0

    if args.command == "run-java-bff-loop":
        emit_command_start(reporter, "run-java-bff-loop", input=args.input.resolve(), output=args.output.resolve())
        cline_bridge_command = resolve_cline_bridge_command(
            args,
            output_dir=args.output.resolve(),
            mode="java-bff",
            runner_mode=args.runner_mode,
        )
        config = JavaBffLoopConfig(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
            prompt_profile=args.prompt_profile,
            max_iterations=args.max_iterations,
            max_attempts_per_prompt=args.max_attempts_per_prompt,
            runner_mode=args.runner_mode,
            provider_config_path=args.provider_config.resolve() if args.provider_config else None,
            provider_base_url=args.provider_base_url,
            provider_api_key=args.provider_api_key,
            provider_api_key_env=args.provider_api_key_env,
            provider_model=args.provider_model,
            provider_name=args.provider_name,
            token_limit=args.token_limit,
            temperature=args.temperature,
            timeout_seconds=args.timeout_seconds,
            cline_bridge_command=cline_bridge_command,
            package_name=args.package_name,
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
            max_sql_chunk_tokens=args.max_sql_chunk_tokens,
        )
        payload = run_java_bff_loop(config, reporter=reporter)
        return summarize_loop_result(
            reporter,
            label="Java BFF loop",
            payload=payload,
            completion_path=args.output.resolve() / "analysis" / "java_bff" / "loop" / "completion_report.md",
        )

    if args.command == "resume-java-bff-loop":
        emit_command_start(reporter, "resume-java-bff-loop", output=args.output.resolve())
        inspection = inspect_java_bff_loop(args.output.resolve())
        base_config = JavaBffLoopConfig.from_dict(inspection["state"]["config"])
        cline_bridge_command = resolve_cline_bridge_command(
            args,
            output_dir=args.output.resolve(),
            mode="java-bff",
            runner_mode=base_config.runner_mode,
        )
        if args.provider_config:
            base_config.provider_config_path = args.provider_config.resolve()
        if args.provider_base_url is not None:
            base_config.provider_base_url = args.provider_base_url
        if args.provider_api_key is not None:
            base_config.provider_api_key = args.provider_api_key
        if args.provider_api_key_env is not None:
            base_config.provider_api_key_env = args.provider_api_key_env
        if args.provider_model is not None:
            base_config.provider_model = args.provider_model
        if args.provider_name is not None:
            base_config.provider_name = args.provider_name
        if args.token_limit is not None:
            base_config.token_limit = args.token_limit
        if args.temperature is not None:
            base_config.temperature = args.temperature
        if args.timeout_seconds is not None:
            base_config.timeout_seconds = args.timeout_seconds
        if cline_bridge_command is not None:
            base_config.cline_bridge_command = cline_bridge_command
        if args.package_name is not None:
            base_config.package_name = args.package_name
        payload = resume_java_bff_loop(args.output.resolve(), config=base_config, reporter=reporter)
        return summarize_loop_result(
            reporter,
            label="Java BFF loop",
            payload=payload,
            completion_path=args.output.resolve() / "analysis" / "java_bff" / "loop" / "completion_report.md",
        )

    if args.command == "inspect-java-bff-loop":
        payload = inspect_java_bff_loop(args.output.resolve())
        reporter.success(
            f"Java BFF loop status={payload['state']['status']} "
            f"history_events={payload['history_count']}."
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    reporter = build_reporter(args)
    try:
        return dispatch_command(args, parser, reporter)
    except Exception as exc:  # noqa: BLE001
        return handle_cli_exception(args, reporter, exc)
