from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.schemas import LoopConfig, LoopState, required_artifacts_for_company_mode


class SchemaTests(unittest.TestCase):
    def test_loop_state_defaults_round_trip(self) -> None:
        state = LoopState(run_id="demo", status="running", current_phase="scan")
        payload = state.to_dict()
        restored = LoopState.from_dict(payload)

        self.assertEqual("demo", restored.run_id)
        self.assertEqual("running", restored.status)
        self.assertEqual("scan", restored.current_phase)
        self.assertEqual([], restored.pending_clusters)

    def test_loop_config_round_trip(self) -> None:
        config = LoopConfig(
            input_dir=Path("/tmp/input"),
            output_dir=Path("/tmp/output"),
            runner_mode="provider",
            prompt_profile="qwen3-128k-autonomous",
        )
        restored = LoopConfig.from_dict(config.to_dict())
        self.assertEqual(config.input_dir, restored.input_dir)
        self.assertEqual(config.output_dir, restored.output_dir)
        self.assertEqual(config.runner_mode, restored.runner_mode)

    def test_required_artifacts_for_company_mode_contains_loop_targets(self) -> None:
        artifacts = required_artifacts_for_company_mode()
        self.assertIn("analysis/context_packs", artifacts)
        self.assertIn("grade/profile_grade.json", artifacts)


if __name__ == "__main__":
    unittest.main()
