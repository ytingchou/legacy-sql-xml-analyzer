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

from legacy_sql_xml_analyzer.handoff import export_vscode_cline_pack, resume_from_handoff
from tests.support import load_json, make_analysis_root, write_failure_clusters, write_query_card


class HandoffPackTests(unittest.TestCase):
    def test_export_generic_handoff_pack(self) -> None:
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
                        "occurrence_count": 2,
                        "files_affected": 1,
                        "queries_affected": 1,
                        "representative_message": "Reference target could not be resolved.",
                        "suggested_fix": "Verify the default target order.",
                        "common_context_keys": ["query_id"],
                        "sample_diagnostics": [
                            {
                                "source_path": "/tmp/orders.xml",
                                "query_id": "orders.xml:main:OrderSearch",
                                "tag": "sql-refer-to",
                                "message": "Reference target missing.",
                                "context": {"query_id": "orders.xml:main:OrderSearch"},
                                "suggested_fix": "Verify the default target order.",
                                "prompt_hint": "",
                            }
                        ],
                    }
                ],
            )
            write_query_card(analysis_root, "orders.xml:main:OrderSearch", "# OrderSearch\n\nRelevant query card.")

            payload = export_vscode_cline_pack(
                analysis_root,
                cluster_id="reference_target_missing",
                stage="propose",
                profile_name="company-qwen3-propose",
            )

            self.assertEqual("generic_cluster", payload["kind"])
            self.assertTrue(payload["written_paths"])
            pack_path = next(Path(item) for item in payload["written_paths"] if item.endswith("pack.json"))
            lifecycle_path = next(Path(item) for item in payload["written_paths"] if item.endswith("lifecycle.json"))
            session_path = next(Path(item) for item in payload["written_paths"] if item.endswith("session.json"))
            pack = load_json(pack_path)
            lifecycle = load_json(lifecycle_path)
            session = load_json(session_path)
            self.assertEqual("company-qwen3-propose", pack["profile_name"])
            self.assertIn("Return JSON only", pack["prompt_text"])
            self.assertEqual("new", lifecycle["state"])
            self.assertEqual("pending_response", session["status"])
            self.assertTrue(session["suggested_commands"]["watch_and_review"])

    def test_export_review_repair_pack(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            review_path = analysis_root / "llm_reviews" / "sample-review.json"
            review_path.parent.mkdir(parents=True, exist_ok=True)
            review_path.write_text(
                json.dumps(
                    {
                        "status": "needs_revision",
                        "repair_prompt_text": "Return corrected JSON only.",
                        "parsed_response": {"cluster_id": "reference_target_missing", "insufficient_evidence": False},
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            payload = export_vscode_cline_pack(
                analysis_root,
                review_path=review_path,
                profile_name="company-qwen3-verify",
            )

            self.assertEqual("review_repair", payload["kind"])
            prompt_path = next(Path(item) for item in payload["written_paths"] if item.endswith("prompt.txt"))
            self.assertEqual("Return corrected JSON only.", prompt_path.read_text(encoding="utf-8"))

    def test_resume_from_handoff_writes_resume_report(self) -> None:
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
            payload = export_vscode_cline_pack(
                analysis_root,
                cluster_id="reference_target_missing",
                stage="propose",
                profile_name="company-qwen3-propose",
            )
            session_path = next(Path(item) for item in payload["written_paths"] if item.endswith("session.json"))
            session = load_json(session_path)
            Path(session["response_path"]).write_text('{"cluster_id":"reference_target_missing"}', encoding="utf-8")

            report = resume_from_handoff(session_path)

            self.assertEqual("review_response", report["next_action"])
            self.assertTrue(Path(report["json_path"]).exists())
            self.assertTrue(Path(report["md_path"]).exists())


if __name__ == "__main__":
    unittest.main()
