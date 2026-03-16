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

from legacy_sql_xml_analyzer.watch_review import watch_and_review
from tests.support import make_analysis_root, write_failure_clusters, write_query_card


class WatchReviewTests(unittest.TestCase):
    def test_watch_and_review_generates_repair_pack_for_generic_response(self) -> None:
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
            response_path = root / "response.json"
            response_path.write_text(json.dumps({"cluster_id": "reference_target_missing"}), encoding="utf-8")

            payload = watch_and_review(
                analysis_root=analysis_root,
                response_path=response_path,
                cluster_id="reference_target_missing",
                stage="propose",
                timeout_seconds=1.0,
                poll_seconds=0.1,
                emit_repair_pack=True,
            )

            self.assertEqual("needs_revision", payload["status"])
            self.assertIsNotNone(payload["repair_pack"])
            self.assertTrue(Path(payload["json_path"]).exists())


if __name__ == "__main__":
    unittest.main()
