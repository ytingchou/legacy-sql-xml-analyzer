from __future__ import annotations

import argparse
from pathlib import Path

from .analyzer import analyze_directory
from .evolution import (
    apply_profile_patch_bundle,
    propose_rules_from_analysis,
    review_llm_response_from_analysis,
    simulate_candidate_profile,
)
from .learning import freeze_profile, infer_rules, learn_directory
from .prompting import prepare_prompt_pack_from_analysis
from .validation import validate_profile
from .web import serve_report


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
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "analyze":
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
        print(
            f"Analyzed {len(result.files)} file(s), discovered {len(result.queries)} query node(s), "
            f"generated {len(result.artifacts)} artifact(s), errors={error_count}, warnings={warning_count}."
        )

        if args.strict and error_count:
            print("Strict mode detected error/fatal diagnostics. See analysis/markdown/diagnostics for details.")
            return 2
        return 0

    if args.command == "learn":
        result = learn_directory(args.input.resolve(), args.output.resolve())
        print(
            f"Learned from {result['observations']['summary']['xml_file_count']} file(s), "
            f"generated {len(result['artifacts'])} learning artifact(s)."
        )
        return 0

    if args.command == "infer-rules":
        result = infer_rules(args.input.resolve(), args.output.resolve())
        print(
            f"Inferred {len(result['profile'].rules)} rule candidate(s), "
            f"generated {len(result['artifacts'])} rule artifact(s)."
        )
        return 0

    if args.command == "freeze-profile":
        profile = freeze_profile(args.input.resolve(), args.output.resolve(), args.min_confidence)
        print(
            f"Froze profile with {len(profile.rules)} retained rule(s) at confidence >= {args.min_confidence:.2f}."
        )
        return 0

    if args.command == "validate-profile":
        result = validate_profile(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            profile_path=args.profile.resolve(),
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
        )
        classification = result["assessment"]["classification"]
        print(
            f"Validated profile with classification={classification}, "
            f"generated {len(result['artifacts'])} validation artifact(s)."
        )
        if args.fail_on_regression and classification == "regressed":
            print("Validation detected regression. See validation/profile_validation.md for details.")
            return 3
        return 0

    if args.command == "serve-report":
        serve_report(root=args.root.resolve(), host=args.host, port=args.port)
        return 0

    if args.command == "prepare-prompt":
        result = prepare_prompt_pack_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            cluster_id=args.cluster,
            budget=args.budget,
            model=args.model,
        )
        print(
            f"Prepared prompt pack for cluster={result['cluster']['cluster_id']}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "review-llm-response":
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
        print(
            f"Reviewed response for cluster={result['cluster']['cluster_id']} stage={review['stage']} "
            f"status={review['status']}, issues={len(review['issues'])}, "
            f"safe_to_apply_candidate={review['safe_to_apply_candidate']}."
        )
        return 0

    if args.command == "propose-rules":
        result = propose_rules_from_analysis(
            analysis_root=args.analysis_root.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
            min_confidence=args.min_confidence,
            include_needs_review=args.include_needs_review,
        )
        print(
            f"Proposed {result['proposal_payload']['summary']['accepted_patch_count']} patch(es), "
            f"candidate profile rules={len(result['candidate_profile'].rules)}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    if args.command == "apply-profile-patch":
        profile = apply_profile_patch_bundle(
            patch_bundle_path=args.patch_bundle.resolve(),
            output_path=args.output.resolve(),
            profile_path=args.profile.resolve() if args.profile else None,
        )
        print(
            f"Wrote merged profile with {len(profile.rules)} rule(s) to {args.output.resolve()}."
        )
        return 0

    if args.command == "simulate-profile":
        result = simulate_candidate_profile(
            input_dir=args.input.resolve(),
            output_dir=args.output.resolve(),
            analysis_root=args.analysis_root.resolve() if args.analysis_root else None,
            candidate_profile_path=args.candidate_profile.resolve() if args.candidate_profile else None,
            entry_file=args.entry_file,
            entry_main_query=args.entry_main_query,
        )
        print(
            f"Simulated candidate profile with classification={result['assessment']['classification']}, "
            f"generated {len(result['artifacts'])} artifact(s)."
        )
        return 0

    parser.error(f"Unsupported command: {args.command}")
