from __future__ import annotations

import argparse
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.cli import resolve_cline_bridge_command


class CliBridgeCommandTests(unittest.TestCase):
    def build_args(self, **overrides: object) -> argparse.Namespace:
        payload: dict[str, object] = {
            "cline_bridge_command": None,
            "cline_bridge_profile": None,
            "cline_command": "cline",
            "cline_cwd": None,
            "cline_model": None,
            "cline_config": None,
            "cline_extra_args": None,
            "cline_timeout": None,
            "cline_verbose_output": False,
            "cline_double_check_completion": False,
        }
        payload.update(overrides)
        return argparse.Namespace(**payload)

    def test_resolve_cline_bridge_command_for_generic_profile(self) -> None:
        args = self.build_args(
            cline_bridge_profile="cline-json",
            cline_cwd=Path("/tmp/workspace"),
            cline_model="qwen3",
            cline_verbose_output=True,
        )

        command = resolve_cline_bridge_command(
            args,
            output_dir=Path("/tmp/out"),
            mode="generic",
            runner_mode="cline_bridge",
        )

        self.assertIsNotNone(command)
        assert command is not None
        self.assertIn("tools/cline_bridge.py", command)
        self.assertIn("generic", command)
        self.assertIn("/analysis", command)
        self.assertIn("--command-profile cline-json", command)
        self.assertIn("--cline-command cline", command)
        self.assertIn("--cline-cwd", command)
        self.assertIn("workspace", command)
        self.assertIn("--cline-model qwen3", command)
        self.assertIn("--cline-verbose-output", command)

    def test_resolve_cline_bridge_command_for_java_profile(self) -> None:
        args = self.build_args(
            cline_bridge_profile="cline-json-yolo",
            cline_timeout=900,
        )

        command = resolve_cline_bridge_command(
            args,
            output_dir=Path("/tmp/out"),
            mode="java-bff",
            runner_mode="cline_bridge",
        )

        self.assertIsNotNone(command)
        assert command is not None
        self.assertIn("java-bff", command)
        self.assertIn("/analysis/java_bff", command)
        self.assertIn("--command-profile cline-json-yolo", command)
        self.assertIn("--cline-timeout 900", command)

    def test_resolve_cline_bridge_command_rejects_manual_and_profile_mix(self) -> None:
        args = self.build_args(
            cline_bridge_command="python3 ./tools/cline_bridge.py generic ./out/analysis --command-profile cline-json",
            cline_bridge_profile="cline-json",
        )

        with self.assertRaises(ValueError):
            resolve_cline_bridge_command(
                args,
                output_dir=Path("/tmp/out"),
                mode="generic",
                runner_mode="cline_bridge",
            )

    def test_resolve_cline_bridge_command_requires_cline_bridge_runner(self) -> None:
        args = self.build_args(cline_bridge_profile="cline-json")

        with self.assertRaises(ValueError):
            resolve_cline_bridge_command(
                args,
                output_dir=Path("/tmp/out"),
                mode="generic",
                runner_mode="provider",
            )


if __name__ == "__main__":
    unittest.main()
