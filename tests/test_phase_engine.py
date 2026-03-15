from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.phase_engine import (
    apply_phase_status,
    build_initial_state,
    select_next_phase,
    should_stop,
)
from legacy_sql_xml_analyzer.schemas import LoopConfig


class PhaseEngineTests(unittest.TestCase):
    def test_build_initial_state_company_mode(self) -> None:
        config = LoopConfig(input_dir=Path("/tmp/in"), output_dir=Path("/tmp/out"))
        state = build_initial_state(config)
        self.assertEqual("scan", state.current_phase)
        self.assertIn("analysis/index.json", state.required_artifacts)

    def test_select_next_phase_walks_cluster_and_tail_phases(self) -> None:
        config = LoopConfig(input_dir=Path("/tmp/in"), output_dir=Path("/tmp/out"))
        state = build_initial_state(config)
        apply_phase_status(state, "scan", latest_output={"clusters": 1})
        state.pending_clusters = ["reference_target_missing"]
        self.assertEqual("classify", select_next_phase(state))
        apply_phase_status(state, "classify", cluster_id="reference_target_missing")
        self.assertEqual("propose", select_next_phase(state))
        apply_phase_status(state, "propose", cluster_id="reference_target_missing")
        self.assertEqual("verify", select_next_phase(state))
        apply_phase_status(state, "verify", cluster_id="reference_target_missing")
        self.assertEqual("simulate", select_next_phase(state))

    def test_should_stop_when_required_artifacts_complete(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            config = LoopConfig(input_dir=Path("/tmp/in"), output_dir=output_dir)
            state = build_initial_state(config)
            for artifact in state.required_artifacts:
                path = output_dir / artifact
                if artifact.endswith(".json"):
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("{}", encoding="utf-8")
                else:
                    path.mkdir(parents=True, exist_ok=True)
            stop, reason = should_stop(state, output_dir, config.max_iterations)
            self.assertTrue(stop)
            self.assertEqual("all_required_artifacts_completed", reason)


if __name__ == "__main__":
    unittest.main()
