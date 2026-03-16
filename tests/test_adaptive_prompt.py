from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.adaptive_prompt import (
    compile_adaptive_generic_context,
    shrink_prompt_text,
    write_adaptive_payload,
)
from tests.support import make_analysis_root, write_failure_clusters, write_query_card


class AdaptivePromptTests(unittest.TestCase):
    def test_compile_adaptive_generic_context_and_write(self) -> None:
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

            payload = compile_adaptive_generic_context(
                analysis_root,
                cluster_id="reference_target_missing",
                phase="propose",
                prompt_profile="qwen3-128k-autonomous",
                targets=[8000, 16000],
            )
            paths = write_adaptive_payload(root, payload)

            self.assertEqual(2, len(payload["variants"]))
            self.assertTrue(any(path.name.endswith(".adaptive.json") for path in paths))

    def test_shrink_prompt_text_reduces_size(self) -> None:
        raw = "\n".join(["Header"] + [f"line {idx}" for idx in range(400)] + ["Return JSON only", '{"ok": true}'])

        payload = shrink_prompt_text(raw, 200)

        self.assertLessEqual(payload["estimated_tokens"], 200)
        self.assertIn("Return JSON only", payload["prompt_text"])


if __name__ == "__main__":
    unittest.main()
