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

from legacy_sql_xml_analyzer.java_bff import prepare_java_bff_from_input, safe_name
from legacy_sql_xml_analyzer.java_bff_context import compile_java_bff_context_pack, write_java_bff_context_pack
from legacy_sql_xml_analyzer.java_bff_runtime import JavaBffClineBridgeRunner
from legacy_sql_xml_analyzer.java_bff_workflow import review_java_bff_response_from_analysis
from tests.support import load_json


class JavaBffContextTests(unittest.TestCase):
    def test_compile_repository_merge_context_uses_accepted_chunk_reviews(self) -> None:
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

            prepare_java_bff_from_input(
                input_dir=input_dir,
                output_dir=output_dir,
                prompt_profile="qwen3-128k-java-bff",
            )

            bundle_id = "orders.xml:main:OrderSearch"
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / safe_name(bundle_id)
            plan_prompt = phase_root / "phase-1-plan.json"
            chunk_prompt = next(path for path in sorted(phase_root.glob(f"{safe_name(bundle_id)}-phase-2-repository-*.json")) if "merge" not in path.name)
            merge_prompt = phase_root / f"{safe_name(bundle_id)}-phase-2-repository-merge.json"

            plan_response = root / "plan.json"
            plan_response.write_text(
                json.dumps(
                    {
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
                        "service_flow": ["Validate request and call repository."],
                        "controller_contract_hints": ["Expose an order-search endpoint."],
                        "risks": [],
                        "open_questions": [],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            chunk_response = root / "chunk.json"
            chunk_response.write_text(
                json.dumps(
                    {
                        "query_id": bundle_id,
                        "chunk_id": f"{bundle_id}:chunk:01",
                        "method_name": "fetchOrderSearch",
                        "parameter_binding": [
                            {
                                "parameter_name": "customerId",
                                "java_argument_name": "customerId",
                                "binding_note": "Bind directly with MapSqlParameterSource.",
                            }
                        ],
                        "sql_logic_steps": ["Bind customerId and execute the analyzed select SQL."],
                        "oracle_19c_notes": ["Keep placeholder names unchanged."],
                        "row_mapping_notes": ["Map order_id and status into the response DTO."],
                        "manual_review_flags": [],
                        "carry_forward_context": [],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                review_java_bff_response_from_analysis(output_dir, plan_prompt, plan_response)["review"]["status"],
                "accepted",
            )
            self.assertEqual(
                review_java_bff_response_from_analysis(output_dir, chunk_prompt, chunk_response)["review"]["status"],
                "accepted",
            )

            pack = compile_java_bff_context_pack(output_dir, merge_prompt)
            paths = write_java_bff_context_pack(output_dir, pack)

            self.assertEqual(pack["phase"], "phase-2-repository-merge")
            self.assertFalse(pack["missing_inputs"])
            self.assertTrue(any("reviews" in artifact for artifact in pack["included_artifacts"]))
            self.assertIn("Accepted Repository Chunk Outputs", pack["prompt_text"])
            self.assertTrue(all(path.exists() for path in paths))

    def test_review_rejects_unknown_repository_chunk_parameter_binding(self) -> None:
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
select o.order_id
from orders o
where o.customer_id = :customerId
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            prepare_java_bff_from_input(input_dir=input_dir, output_dir=output_dir, prompt_profile="qwen3-128k-java-bff")
            bundle_id = "orders.xml:main:OrderSearch"
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / safe_name(bundle_id)
            chunk_prompt = next(path for path in sorted(phase_root.glob(f"{safe_name(bundle_id)}-phase-2-repository-*.json")) if "merge" not in path.name)
            response_path = root / "chunk-invalid.json"
            response_path.write_text(
                json.dumps(
                    {
                        "query_id": bundle_id,
                        "chunk_id": f"{bundle_id}:chunk:01",
                        "method_name": "fetchOrderSearch",
                        "parameter_binding": [
                            {
                                "parameter_name": "unknownParam",
                                "java_argument_name": "unknownParam",
                                "binding_note": "Bind it anyway.",
                            }
                        ],
                        "sql_logic_steps": ["Bind unknownParam and execute SQL."],
                        "oracle_19c_notes": [],
                        "row_mapping_notes": [],
                        "manual_review_flags": [],
                        "carry_forward_context": [],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            review = review_java_bff_response_from_analysis(output_dir, chunk_prompt, response_path)["review"]
            issue_codes = {item["code"] for item in review["issues"]}
            self.assertEqual(review["status"], "needs_revision")
            self.assertIn("JAVA_UNKNOWN_PARAMETER_BINDING", issue_codes)

    def test_review_rejects_ready_verify_with_missing_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            (input_dir / "orders.xml").write_text(
                """<sql-mapping>
  <main-query name="OrderSearch">
    <sql-body><![CDATA[
select o.order_id
from orders o
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            prepare_java_bff_from_input(input_dir=input_dir, output_dir=output_dir, prompt_profile="qwen3-128k-java-bff")
            bundle_id = "orders.xml:main:OrderSearch"
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / safe_name(bundle_id)
            verify_prompt = phase_root / "phase-4-verify.json"
            response_path = root / "verify-invalid.json"
            response_path.write_text(
                json.dumps(
                    {
                        "bundle_id": bundle_id,
                        "verdict": "ready",
                        "token_budget_check": {
                            "within_limit": False,
                            "recommended_next_prompt": "retry",
                        },
                        "oracle_19c_risks": [],
                        "guess_risks": ["The DTO shape is guessed."],
                        "missing_artifacts": ["phase-3-bff-assembly"],
                        "final_recommendations": ["Proceed."],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            review = review_java_bff_response_from_analysis(output_dir, verify_prompt, response_path)["review"]
            issue_codes = {item["code"] for item in review["issues"]}
            self.assertEqual(review["status"], "needs_revision")
            self.assertIn("JAVA_VERIFY_READY_OVER_BUDGET", issue_codes)
            self.assertIn("JAVA_VERIFY_READY_WITH_MISSING_ARTIFACTS", issue_codes)
            self.assertIn("JAVA_VERIFY_READY_WITH_GUESS_RISKS", issue_codes)

    def test_cline_bridge_runner_writes_task_contract_with_context(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()
            (input_dir / "orders.xml").write_text(
                """<sql-mapping>
  <main-query name="OrderSearch">
    <sql-body><![CDATA[
select o.order_id
from orders o
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            prepare_java_bff_from_input(input_dir=input_dir, output_dir=output_dir, prompt_profile="qwen3-128k-java-bff")
            bundle_id = "orders.xml:main:OrderSearch"
            slug = safe_name(bundle_id)
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / slug
            plan_prompt = phase_root / "phase-1-plan.json"

            agent_runs_root = output_dir / "analysis" / "java_bff" / "agent_runs" / slug
            agent_runs_root.mkdir(parents=True, exist_ok=True)
            response_text_path = agent_runs_root / "phase-1-plan.response.txt"
            response_text_path.write_text("{}", encoding="utf-8")
            result_path = agent_runs_root / "phase-1-plan.result.json"
            result_path.write_text(
                json.dumps(
                    {
                        "generated_at": "2026-03-16T00:00:00+00:00",
                        "bundle_id": bundle_id,
                        "phase": "phase-1-plan",
                        "phase_pack_path": str(plan_prompt.resolve()),
                        "response_text_path": str(response_text_path.resolve()),
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            runner = JavaBffClineBridgeRunner()
            result = runner.run_phase_pack(output_dir / "analysis", plan_prompt)
            task_path = output_dir / "analysis" / "java_bff" / "tasks" / slug / "phase-1-plan.json"
            task_payload = load_json(task_path)

            self.assertTrue(task_path.exists())
            self.assertEqual(task_payload["phase"], "phase-1-plan")
            self.assertTrue(Path(task_payload["context_pack_path"]).exists())
            self.assertIn("expected_schema", task_payload)
            self.assertIn("token_budget", task_payload)
            self.assertEqual(result["task_path"], str(task_path.resolve()))


if __name__ == "__main__":
    unittest.main()
