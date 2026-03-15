from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.context_compiler import compile_context_pack_from_analysis, write_context_pack

from tests.support import make_analysis_root, write_failure_clusters, write_query_card


class ContextCompilerTests(unittest.TestCase):
    def test_context_compiler_respects_qwen_budget(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            write_failure_clusters(
                analysis_root,
                [
                    {
                        "cluster_id": "reference_target_missing",
                        "task_type": "mapping_inference",
                        "code": "REFERENCE_TARGET_MISSING",
                        "severity": "error",
                        "representative_message": "Missing external xml target.",
                        "suggested_fix": "Map the alias to a real file.",
                        "sample_diagnostics": [
                            {
                                "query_id": "consumer.xml_main_ConsumerMain",
                                "message": "Missing external xml target.",
                                "context": {"xml": "shared"},
                            }
                        ],
                    }
                ],
            )
            write_query_card(analysis_root, "consumer.xml_main_ConsumerMain", "query card\n" + ("x" * 5000))

            pack = compile_context_pack_from_analysis(
                analysis_root=analysis_root,
                cluster_id="reference_target_missing",
                phase="classify",
                prompt_profile="qwen3-128k-classify",
            )

            self.assertLessEqual(pack.estimated_tokens, pack.max_input_tokens + 5000)
            self.assertTrue(pack.sections)

    def test_context_compiler_excludes_full_index_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = make_analysis_root(root)
            (analysis_root / "index.json").write_text('{"huge":"payload"}', encoding="utf-8")
            write_failure_clusters(
                analysis_root,
                [
                    {
                        "cluster_id": "reference_target_missing",
                        "task_type": "mapping_inference",
                        "code": "REFERENCE_TARGET_MISSING",
                        "severity": "error",
                        "representative_message": "Missing external xml target.",
                        "suggested_fix": None,
                        "sample_diagnostics": [],
                    }
                ],
            )

            pack = compile_context_pack_from_analysis(
                analysis_root=analysis_root,
                cluster_id="reference_target_missing",
                phase="propose",
                prompt_profile="qwen3-128k-propose",
            )
            paths = write_context_pack(root, pack)

            self.assertFalse(any(path.name == "index.json" for path in map(Path, pack.included_artifacts)))
            self.assertEqual(3, len(paths))


if __name__ == "__main__":
    unittest.main()
