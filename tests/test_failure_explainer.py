from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from legacy_sql_xml_analyzer.failure_explainer import explain_failure_from_output_dir
from tests.support import load_json, make_analysis_root


class FailureExplainerTests(unittest.TestCase):
    def test_explain_failure_writes_index_and_entry_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            loop_root = analysis_root / "agent_loop"
            loop_root.mkdir(parents=True, exist_ok=True)
            (loop_root / "completion_report.json").write_text(
                """{
  "status": "stopped",
  "stop_reason": "max_iterations_reached",
  "missing_artifacts": ["analysis/proposals/rule_proposals.json"]
}
""",
                encoding="utf-8",
            )

            payload = explain_failure_from_output_dir(root)

            index = payload["index"]
            self.assertEqual(1, index["count"])
            explanation = index["explanations"][0]
            self.assertEqual("MAX_ITERATIONS_REACHED", explanation["failure_code"])
            self.assertIn("resume-agent-loop", explanation["recommended_command"])

            explanation_root = analysis_root / "failure_explanations"
            self.assertTrue((explanation_root / "index.json").exists())
            self.assertTrue((explanation_root / "index.md").exists())
            entry_json = next(path for path in explanation_root.glob("*.json") if path.name != "index.json")
            entry_payload = load_json(entry_json)
            self.assertIn("company_llm_prompt", entry_payload)
            self.assertIn("root_cause", entry_payload["company_llm_prompt"])


if __name__ == "__main__":
    unittest.main()
