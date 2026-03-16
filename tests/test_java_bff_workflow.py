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
from legacy_sql_xml_analyzer.java_bff_workflow import (
    generate_java_bff_skeleton,
    merge_java_bff_phases,
    review_java_bff_response_from_analysis,
)
from tests.support import load_json


class JavaBffWorkflowTests(unittest.TestCase):
    def test_review_merge_and_generate_java_bff_skeleton(self) -> None:
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
            responses = {
                "phase-1-plan.json": {
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
                    "service_flow": ["Validate request then call the repository."],
                    "controller_contract_hints": ["Expose a search endpoint."],
                    "risks": [],
                    "open_questions": [],
                },
                f"{safe_name(bundle_id)}-phase-2-repository-01.json": {
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
                    "sql_logic_steps": ["Bind customerId and execute the select statement."],
                    "oracle_19c_notes": ["Preserve placeholder names and ordering."],
                    "row_mapping_notes": ["Map columns to response DTO fields."],
                    "manual_review_flags": [],
                    "carry_forward_context": ["The result is list-shaped."],
                },
                f"{safe_name(bundle_id)}-phase-2-repository-merge.json": {
                    "query_id": bundle_id,
                    "method_name": "fetchOrderSearch",
                    "repository_logic": ["Build parameter source, execute SQL, map each row."],
                    "parameter_contract": ["customerId is required."],
                    "sql_chunk_order": [f"{bundle_id}:chunk:01"],
                    "oracle_19c_risks": [],
                    "manual_review_flags": [],
                },
                "phase-3-bff-assembly.json": {
                    "entry_query_id": bundle_id,
                    "service_logic": ["Call repository and wrap the result."],
                    "controller_logic": ["Accept request DTO and delegate to the service."],
                    "dto_contract_hints": ["Request needs customerId; response needs orderId and status."],
                    "error_handling": ["Translate SQL exceptions into service-level failures."],
                    "follow_up_questions": [],
                },
                "phase-4-verify.json": {
                    "bundle_id": bundle_id,
                    "verdict": "ready",
                    "token_budget_check": {
                        "within_limit": True,
                        "recommended_next_prompt": "none",
                    },
                    "oracle_19c_risks": [],
                    "guess_risks": [],
                    "missing_artifacts": [],
                    "final_recommendations": ["Proceed to skeleton generation."],
                },
            }

            for prompt_path in sorted(phase_root.glob("*.json")):
                response_path = root / f"{prompt_path.stem}.response.json"
                response_payload = responses[prompt_path.name]
                response_path.write_text(json.dumps(response_payload, indent=2, ensure_ascii=False), encoding="utf-8")
                result = review_java_bff_response_from_analysis(
                    analysis_root=output_dir,
                    prompt_json_path=prompt_path,
                    response_path=response_path,
                )
                self.assertEqual(result["review"]["status"], "accepted")

            merged = merge_java_bff_phases(output_dir, bundle_id)
            self.assertEqual(merged["implementation_plan"]["status"], "ready")

            skeleton = generate_java_bff_skeleton(output_dir, bundle_id, package_name="com.example.testbff")
            self.assertGreater(len(skeleton["artifacts"]), 0)

            impl_path = output_dir / "analysis" / "java_bff" / "merged" / safe_name(bundle_id) / "implementation_plan.json"
            self.assertTrue(impl_path.exists())
            repo_path = output_dir / "analysis" / "java_bff" / "skeletons" / safe_name(bundle_id) / "src" / "main" / "java" / "com" / "example" / "testbff" / "repository" / "OrderSearchRepository.java"
            self.assertTrue(repo_path.exists())
            repo_text = repo_path.read_text(encoding="utf-8")
            self.assertIn("fetchOrderSearch", repo_text)
            starter_root = output_dir / "analysis" / "java_bff" / "skeletons" / safe_name(bundle_id) / "starter_project"
            self.assertTrue((starter_root / "pom.xml").exists())
            self.assertTrue((starter_root / "dto_contract.json").exists())
            self.assertTrue((starter_root / "verification_checklist.json").exists())
            self.assertTrue((starter_root / "merge_guard.json").exists())
            self.assertTrue((starter_root / "quality_gate.json").exists())
            self.assertTrue((starter_root / "delivery_summary.json").exists())
            quality_gate = load_json(starter_root / "quality_gate.json")
            self.assertTrue(quality_gate["ready_for_delivery"])

            review_root = output_dir / "analysis" / "java_bff" / "reviews" / safe_name(bundle_id)
            self.assertTrue(any(review_root.glob("*-review.json")))
            index_payload = load_json(output_dir / "analysis" / "index.json")
            self.assertTrue(any(item["scope"] == "java_bff" for item in index_payload["artifacts"]))


if __name__ == "__main__":
    unittest.main()
