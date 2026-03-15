from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.agent_loop import inspect_agent_loop, resume_agent_loop, run_agent_loop
from legacy_sql_xml_analyzer.agent_runners import FakeRunner
from legacy_sql_xml_analyzer.schemas import LoopConfig


class AgentLoopTests(unittest.TestCase):
    def test_agent_loop_writes_loop_state_and_completion_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "shared-query.xml").write_text(
                """<sql-mapping>
  <main-query name="SharedMain">
    <sql-body><![CDATA[
      select * from shared_table
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )
            (input_dir / "consumer.xml").write_text(
                """<sql-mapping>
  <main-query name="ConsumerMain">
    <ext-sql-refer-to name="__EXT__" xml="shared" main-query="SharedMain" />
    <sql-body><![CDATA[
      select * from dual __EXT__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            fake_runner = FakeRunner(
                {
                    ("classify", "reference_target_missing"): {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "suspected_root_cause": "The alias does not match a real file name.",
                        "evidence_summary": ["shared has no matching xml file."],
                        "missing_evidence": [],
                        "recommended_next_stage": "propose",
                        "confidence": "high",
                        "insufficient_evidence": False,
                    },
                    ("propose", "reference_target_missing"): {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "root_cause": "The alias shared should map to shared-query.xml.",
                        "proposed_change_type": "profile_rule",
                        "proposed_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {"xml_name": "shared", "mapped_to": "shared-query.xml"},
                        },
                        "confidence": "high",
                        "why": ["The alias points to a single existing file."],
                        "verification_steps": ["Run analyze with the candidate profile."],
                        "risks": ["Another module might use a different alias mapping."],
                        "insufficient_evidence": False,
                    },
                    ("verify", "reference_target_missing"): {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "verdict": "accept",
                        "safe_to_apply": True,
                        "checked_constraints": ["No unsupported rule type was proposed."],
                        "violations": [],
                        "follow_up_actions": ["Run simulation with the candidate profile."],
                        "normalized_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {"xml_name": "shared", "mapped_to": "shared-query.xml"},
                        },
                    },
                }
            )

            config = LoopConfig(
                input_dir=input_dir,
                output_dir=output_dir,
                runner_mode="provider",
                prompt_profile="qwen3-128k-autonomous",
                max_iterations=12,
            )
            payload = run_agent_loop(config, runner=fake_runner)

            self.assertIn(payload["status"], {"completed", "stopped"})
            self.assertTrue((output_dir / "analysis" / "agent_loop" / "loop_state.json").exists())
            self.assertTrue((output_dir / "analysis" / "agent_loop" / "phase_history.json").exists())
            self.assertTrue((output_dir / "analysis" / "agent_loop" / "completion_report.json").exists())
            self.assertTrue((output_dir / "analysis" / "proposals" / "candidate_profile.json").exists())
            self.assertTrue((output_dir / "grade" / "profile_grade.json").exists())

            inspection = inspect_agent_loop(output_dir)
            self.assertGreaterEqual(inspection["history_count"], 1)

    def test_agent_loop_stops_on_iteration_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "broken.xml").write_text(
                """<sql-mapping>
  <main-query name="MissingRef">
    <sql-refer-to name="__MISSING__" main-query="NoSuchMain" />
    <sql-body><![CDATA[
      select * from dual __MISSING__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            fake_runner = FakeRunner(
                {
                    ("classify", "reference_target_missing"): "not valid json",
                }
            )
            config = LoopConfig(
                input_dir=input_dir,
                output_dir=output_dir,
                runner_mode="provider",
                max_iterations=2,
                max_attempts_per_task=1,
            )
            payload = run_agent_loop(config, runner=fake_runner)
            self.assertIn(payload["stop_reason"], {"max_iterations_reached", "phase_sequence_exhausted"})

    def test_resume_agent_loop_uses_persisted_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            (input_dir / "broken.xml").write_text(
                """<sql-mapping>
  <main-query name="MissingRef">
    <sql-refer-to name="__MISSING__" main-query="NoSuchMain" />
    <sql-body><![CDATA[
      select * from dual __MISSING__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            fake_runner = FakeRunner(
                {
                    ("classify", "reference_target_missing"): {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "suspected_root_cause": "No target.",
                        "evidence_summary": ["No target."],
                        "missing_evidence": [],
                        "recommended_next_stage": "insufficient_evidence",
                        "confidence": "low",
                        "insufficient_evidence": True,
                    },
                }
            )
            config = LoopConfig(input_dir=input_dir, output_dir=output_dir, runner_mode="provider", max_iterations=4)
            run_agent_loop(config, runner=fake_runner)
            payload = resume_agent_loop(output_dir=output_dir, runner=fake_runner)
            self.assertIn("status", payload)
            self.assertTrue((output_dir / "analysis" / "agent_loop" / "completion_report.json").exists())


if __name__ == "__main__":
    unittest.main()
