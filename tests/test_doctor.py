from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.doctor import doctor_run
from tests.support import make_analysis_root


class DoctorRunTests(unittest.TestCase):
    def test_doctor_run_writes_report_and_actions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            (analysis_root / "agent_loop").mkdir(parents=True, exist_ok=True)
            (analysis_root / "java_bff" / "loop").mkdir(parents=True, exist_ok=True)
            (analysis_root / "provider_validation" / "latest").mkdir(parents=True, exist_ok=True)
            (analysis_root / "agent_loop" / "completion_report.json").write_text(
                json.dumps({"status": "stopped", "stop_reason": "max_iterations_reached", "missing_artifacts": ["analysis/proposals/rule_proposals.json"]}),
                encoding="utf-8",
            )
            (analysis_root / "java_bff" / "loop" / "completion_report.json").write_text(
                json.dumps({"status": "stopped", "stop_reason": "java_bff_artifacts_incomplete", "missing_artifacts": ["starter_project/manifest.json"]}),
                encoding="utf-8",
            )
            (analysis_root / "provider_validation" / "latest" / "summary.json").write_text(
                json.dumps({"status": "failed", "provider_name": "demo", "provider_model": "qwen3"}),
                encoding="utf-8",
            )

            payload = doctor_run(root)

            self.assertEqual("provider_attention_required", payload["status"])
            self.assertTrue(payload["recommended_actions"])
            self.assertTrue(Path(payload["json_path"]).exists())
            self.assertTrue(Path(payload["md_path"]).exists())


if __name__ == "__main__":
    unittest.main()
