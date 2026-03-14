from __future__ import annotations

import argparse
from pathlib import Path

from .analyzer import analyze_directory
from .learning import freeze_profile, infer_rules, learn_directory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="legacy-sql-xml-analyzer")
    subparsers = parser.add_subparsers(dest="command", required=True)

    analyze_parser = subparsers.add_parser("analyze", help="Analyze SQL XML mappings.")
    analyze_parser.add_argument("--input", required=True, type=Path, help="Input directory that contains XML files.")
    analyze_parser.add_argument("--output", required=True, type=Path, help="Output directory for generated artifacts.")
    analyze_parser.add_argument("--entry-file", help="Optional XML filename to emphasize in artifacts.")
    analyze_parser.add_argument("--entry-main-query", help="Optional main-query name to emphasize in artifacts.")
    analyze_parser.add_argument("--profile", type=Path, help="Optional learned/frozen profile JSON.")
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

    parser.error(f"Unsupported command: {args.command}")
