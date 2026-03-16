from __future__ import annotations

import io
import unittest

from legacy_sql_xml_analyzer.console import ConsoleReporter, render_exception_block


class ConsoleTests(unittest.TestCase):
    def test_render_exception_block_includes_hints_and_artifacts(self) -> None:
        exc = ValueError("bad config")
        block = render_exception_block(
            exc,
            hints=["Check provider config."],
            artifact_paths=["/tmp/debug.json"],
            verbose=False,
        )
        self.assertIn("Execution failed.", block)
        self.assertIn("Check provider config.", block)
        self.assertIn("/tmp/debug.json", block)
        self.assertIn("--verbose", block)

    def test_console_reporter_suppresses_progress_when_disabled(self) -> None:
        stdout = io.StringIO()
        stderr = io.StringIO()
        reporter = ConsoleReporter(verbose=False, progress_enabled=False, stdout=stdout, stderr=stderr)
        reporter.progress("loop", "started", phase="scan")
        reporter.warning("warn")
        self.assertEqual("", stdout.getvalue())
        self.assertIn("[warning] warn", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
