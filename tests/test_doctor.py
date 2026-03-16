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

from legacy_sql_xml_analyzer.doctor import doctor_run, retry_from_doctor
from legacy_sql_xml_analyzer.handoff import export_vscode_cline_pack
from tests.support import load_json, make_analysis_root, write_failure_clusters, write_query_card


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
            write_failure_clusters(
                analysis_root,
                [
                    {
                        "cluster_id": "reference_target_missing",
                        "code": "REFERENCE_TARGET_MISSING",
                        "severity": "error",
                        "task_type": "mapping_inference",
                        "occurrence_count": 1,
                        "files_affected": 1,
                        "queries_affected": 1,
                        "representative_message": "Reference target missing.",
                        "suggested_fix": "Verify target default order.",
                        "common_context_keys": ["query_id"],
                        "sample_diagnostics": [],
                    }
                ],
            )
            write_query_card(analysis_root, "orders.xml:main:OrderSearch", "# OrderSearch\n\nRelevant query card.")
            export_vscode_cline_pack(
                analysis_root,
                cluster_id="reference_target_missing",
                stage="propose",
                profile_name="company-qwen3-propose",
            )

            payload = doctor_run(root)

            self.assertEqual("provider_attention_required", payload["status"])
            self.assertTrue(payload["recommended_actions"])
            self.assertTrue(Path(payload["json_path"]).exists())
            self.assertTrue(Path(payload["md_path"]).exists())
            self.assertIn("response_scoreboard", payload)
            self.assertIn("retry_scoreboard", payload)
            self.assertEqual(1, payload["retry_scoreboard"]["session_count"])
            self.assertIn("phase_queue", payload)

    def test_retry_from_doctor_generates_retry_plan_and_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            write_failure_clusters(
                analysis_root,
                [
                    {
                        "cluster_id": "reference_target_missing",
                        "code": "REFERENCE_TARGET_MISSING",
                        "severity": "error",
                        "task_type": "mapping_inference",
                        "occurrence_count": 1,
                        "files_affected": 1,
                        "queries_affected": 1,
                        "representative_message": "Reference target missing.",
                        "suggested_fix": "Verify target default order.",
                        "common_context_keys": ["query_id"],
                        "sample_diagnostics": [
                            {
                                "source_path": "/tmp/orders.xml",
                                "query_id": "orders.xml:main:OrderSearch",
                                "tag": "sql-refer-to",
                                "message": "Reference target missing.",
                                "context": {"query_id": "orders.xml:main:OrderSearch"},
                                "suggested_fix": "Verify target default order.",
                                "prompt_hint": "",
                            }
                        ],
                    }
                ],
            )
            write_query_card(analysis_root, "orders.xml:main:OrderSearch", "# OrderSearch\n\nRelevant query card.")
            review_root = analysis_root / "llm_reviews"
            review_root.mkdir(parents=True, exist_ok=True)
            (review_root / "reference_target_missing-propose-review.json").write_text(
                json.dumps(
                    {
                        "cluster_id": "reference_target_missing",
                        "stage": "propose",
                        "status": "needs_revision",
                        "issues": [{"code": "MISSING_FIELD", "message": "Missing required field."}],
                        "next_prompt_text": "Return corrected JSON only.",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            payload = retry_from_doctor(root)

            self.assertTrue(Path(payload["json_path"]).exists())
            self.assertTrue(any("pack.json" in item for item in payload["generated_artifacts"]))
            self.assertTrue(any(item.endswith(".adaptive.json") for item in payload["generated_artifacts"]))
            retry_plan = load_json(Path(payload["json_path"]))
            self.assertEqual("reference_target_missing", retry_plan["latest_review_candidate"]["cluster_id"])


if __name__ == "__main__":
    unittest.main()
