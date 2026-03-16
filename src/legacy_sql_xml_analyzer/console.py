from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO
import sys


def render_exception_block(
    exc: Exception,
    *,
    hints: list[str] | None = None,
    artifact_paths: list[str] | None = None,
    verbose: bool = False,
) -> str:
    lines = [
        "Execution failed.",
        f"- Type: `{type(exc).__name__}`",
        f"- Message: `{str(exc)}`",
    ]
    if hints:
        lines.extend(["", "Hints:"])
        lines.extend(f"- {hint}" for hint in hints)
    if artifact_paths:
        lines.extend(["", "Relevant Artifacts:"])
        lines.extend(f"- `{path}`" for path in artifact_paths)
    if verbose:
        lines.extend(["", "Traceback:", "```text"])
        lines.extend(
            traceback.format_exception(type(exc), exc, exc.__traceback__)
        )
        lines.append("```")
    else:
        lines.extend(["", "Next Step:", "- Rerun with `--verbose` to print the traceback."])
    return "\n".join(lines).rstrip() + "\n"


@dataclass(slots=True)
class ConsoleReporter:
    verbose: bool = False
    progress_enabled: bool = True
    stdout: TextIO = field(default_factory=lambda: sys.stdout)
    stderr: TextIO = field(default_factory=lambda: sys.stderr)

    def info(self, message: str) -> None:
        print(message, file=self.stdout)

    def success(self, message: str) -> None:
        print(message, file=self.stdout)

    def warning(self, message: str) -> None:
        print(f"[warning] {message}", file=self.stderr)

    def error(self, message: str) -> None:
        print(f"[error] {message}", file=self.stderr)

    def detail(self, message: str) -> None:
        if self.verbose:
            print(f"[verbose] {message}", file=self.stdout)

    def progress(self, label: str, message: str, **fields: object) -> None:
        if not self.progress_enabled:
            return
        formatted_fields = " ".join(
            f"{key}={value}" for key, value in fields.items() if value is not None and value != ""
        )
        line = f"[progress] {label}: {message}"
        if formatted_fields:
            line = f"{line} {formatted_fields}"
        print(line, file=self.stdout)

    def exception(
        self,
        exc: Exception,
        *,
        hints: list[str] | None = None,
        artifact_paths: list[str | Path] | None = None,
    ) -> None:
        normalized_paths = [str(path) for path in artifact_paths or []]
        print(
            render_exception_block(
                exc,
                hints=hints,
                artifact_paths=normalized_paths,
                verbose=self.verbose,
            ),
            file=self.stderr,
        )
