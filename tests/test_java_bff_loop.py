from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.java_bff import safe_name
from legacy_sql_xml_analyzer.java_bff_loop import JavaBffFakeRunner, JavaBffLoopConfig, inspect_java_bff_loop, run_java_bff_loop


class JavaBffLoopTests(unittest.TestCase):
    def test_java_bff_loop_completes_with_fake_runner(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            (input_dir / "orders.xml").write_text(
                """<sql-mapping>
  <main-query name="OrderSearch">
    <parameter name="customerId" data_type="String" />
    <sql-body><![CDATA[
select o.order_id, o.status
from orders o
where o.customer_id = :customerId
order by o.order_id desc
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            config = JavaBffLoopConfig(
                input_dir=input_dir,
                output_dir=output_dir,
                runner_mode="provider",
                prompt_profile="qwen3-128k-java-bff",
                max_iterations=16,
                package_name="com.example.loopbff",
            )

            bundle_id = "orders.xml:main:OrderSearch"
            fake_runner = JavaBffFakeRunner(
                responses={
                    "phase-1-plan": {
                        "entry_query_id": bundle_id,
                        "repository_methods": [
                            {
                                "query_id": bundle_id,
                                "method_name": "fetchOrderSearch",
                                "purpose": "Load orders by customer id.",
                                "input_params": ["customerId"],
                                "result_contract": "list",
                            }
                        ],
                        "service_flow": ["Call repository after validation."],
                        "controller_contract_hints": ["Expose a search endpoint."],
                        "risks": [],
                        "open_questions": [],
                    },
                    "phase-2-repository-chunk": {
                        "query_id": bundle_id,
                        "chunk_id": f"{bundle_id}:chunk:01",
                        "method_name": "fetchOrderSearch",
                        "parameter_binding": [
                            {
                                "parameter_name": "customerId",
                                "java_argument_name": "customerId",
                                "binding_note": "Bind directly.",
                            }
                        ],
                        "sql_logic_steps": ["Bind customerId and execute SQL."],
                        "oracle_19c_notes": [],
                        "row_mapping_notes": ["Map columns to DTO."],
                        "manual_review_flags": [],
                        "carry_forward_context": [],
                    },
                    "phase-2-repository-merge": {
                        "query_id": bundle_id,
                        "method_name": "fetchOrderSearch",
                        "repository_logic": ["Combine the chunk logic into one repository method."],
                        "parameter_contract": ["customerId is required."],
                        "sql_chunk_order": [f"{bundle_id}:chunk:01"],
                        "oracle_19c_risks": [],
                        "manual_review_flags": [],
                    },
                    "phase-3-bff-assembly": {
                        "entry_query_id": bundle_id,
                        "service_logic": ["Call repository and map result."],
                        "controller_logic": ["Delegate request to service."],
                        "dto_contract_hints": ["Request carries customerId."],
                        "error_handling": ["Wrap SQL exceptions."],
                        "follow_up_questions": [],
                    },
                    "phase-4-verify": {
                        "bundle_id": bundle_id,
                        "verdict": "ready",
                        "token_budget_check": {"within_limit": True, "recommended_next_prompt": "none"},
                        "oracle_19c_risks": [],
                        "guess_risks": [],
                        "missing_artifacts": [],
                        "final_recommendations": ["Generate code skeletons."],
                    },
                },
                output_dir=output_dir,
            )

            payload = run_java_bff_loop(config, runner=fake_runner)
            self.assertEqual(payload["status"], "completed")
            self.assertEqual(payload["stop_reason"], "all_artifacts_completed")

            inspection = inspect_java_bff_loop(output_dir)
            self.assertGreaterEqual(inspection["history_count"], 1)
            completion_path = output_dir / "analysis" / "java_bff" / "loop" / "completion_report.json"
            skeleton_readme = output_dir / "analysis" / "java_bff" / "skeletons" / safe_name(bundle_id) / "README.md"
            starter_manifest = output_dir / "analysis" / "java_bff" / "skeletons" / safe_name(bundle_id) / "starter_project" / "manifest.json"
            quality_gate = output_dir / "analysis" / "java_bff" / "skeletons" / safe_name(bundle_id) / "starter_project" / "quality_gate.json"
            self.assertTrue(completion_path.exists())
            self.assertTrue(skeleton_readme.exists())
            self.assertTrue(starter_manifest.exists())
            self.assertTrue(quality_gate.exists())


if __name__ == "__main__":
    unittest.main()
