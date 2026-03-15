from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.java_bff import prepare_java_bff_from_input, safe_name
from legacy_sql_xml_analyzer.prompt_profiles import phase_budget_for
from tests.support import load_json


class JavaBffArtifactTests(unittest.TestCase):
    def test_prepare_java_bff_emits_bundle_cards_and_phase_packs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "orders.xml").write_text(
                """<sql-mapping>
  <main-query name="OrderSearch">
    <sql-refer-to name="__FILTERS__" sub-query="OrderFilters" />
    <parameter name="customerId" data_type="String" />
    <parameter name="startDate" data_type="DateTime" />
    <sql-body><![CDATA[
select o.order_id, o.status, o.created_at
from orders o
where 1 = 1
__FILTERS__
order by o.created_at desc
    ]]></sql-body>
  </main-query>
  <sub-query name="OrderFilters">
    <parameter name="customerId" data_type="String" />
    <parameter name="startDate" data_type="DateTime" />
    <sql-body><![CDATA[
and o.customer_id = :customerId
and o.created_at >= :startDate
    ]]></sql-body>
  </sub-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            payload = prepare_java_bff_from_input(
                input_dir=input_dir,
                output_dir=output_dir,
                prompt_profile="qwen3-128k-java-bff",
            )

            self.assertEqual(payload["summary"]["bundle_count"], 1)
            overview_path = output_dir / "analysis" / "java_bff" / "overview.json"
            chunk_manifest_path = output_dir / "analysis" / "java_bff" / "chunk_manifest.json"
            self.assertTrue(overview_path.exists())
            self.assertTrue(chunk_manifest_path.exists())

            bundle_id = "orders.xml:main:OrderSearch"
            bundle_root = output_dir / "analysis" / "java_bff" / "bundles" / safe_name(bundle_id)
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / safe_name(bundle_id)
            self.assertTrue((bundle_root / "bundle.json").exists())
            self.assertTrue((phase_root / "phase-1-plan.json").exists())
            self.assertTrue((phase_root / "phase-3-bff-assembly.json").exists())
            self.assertTrue((phase_root / "phase-4-verify.json").exists())

            card_path = output_dir / "analysis" / "java_bff" / "implementation_cards" / f"{safe_name(bundle_id)}.json"
            self.assertTrue(card_path.exists())
            card_payload = load_json(card_path)
            self.assertEqual(card_payload["java_bff_logic"]["recommended_repository_style"], "NamedParameterJdbcTemplate")

            plan_payload = load_json(phase_root / "phase-1-plan.json")
            plan_budget = phase_budget_for("qwen3-128k-java-bff", "phase-1-plan")
            self.assertLessEqual(plan_payload["estimated_prompt_tokens"], plan_budget["usable_input_limit"])
            self.assertTrue(plan_payload["safe_for_qwen3"])

            index_payload = load_json(output_dir / "analysis" / "index.json")
            self.assertTrue(any(item["scope"] == "java_bff" for item in index_payload["artifacts"]))

    def test_prepare_java_bff_splits_large_sql_into_multiple_repository_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            filter_lines = "\n".join(
                f"and (o.flag_{index} = :flag{index} or o.alt_flag_{index} = :flag{index})"
                for index in range(1, 40)
            )
            params = "\n".join(
                f'    <parameter name="flag{index}" data_type="String" />'
                for index in range(1, 40)
            )
            (input_dir / "heavy.xml").write_text(
                f"""<sql-mapping>
  <main-query name="HeavySearch">
{params}
    <sql-body><![CDATA[
select o.order_id, o.status
from orders o
where 1 = 1
{filter_lines}
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
                max_sql_chunk_tokens=80,
            )

            bundle_id = "heavy.xml:main:HeavySearch"
            phase_root = output_dir / "analysis" / "java_bff" / "phase_packs" / safe_name(bundle_id)
            repository_chunk_prompts = sorted(phase_root.glob(f"{safe_name(bundle_id)}-phase-2-repository-*.json"))
            repository_chunk_prompts = [path for path in repository_chunk_prompts if "merge" not in path.name]
            self.assertGreater(len(repository_chunk_prompts), 1)

            manifest_payload = load_json(output_dir / "analysis" / "java_bff" / "chunk_manifest.json")
            heavy_chunks = [item for item in manifest_payload["chunks"] if item["query_id"] == bundle_id]
            self.assertGreater(len(heavy_chunks), 1)

            chunk_budget = phase_budget_for("qwen3-128k-java-bff", "phase-2-repository-chunk")
            for path in repository_chunk_prompts:
                payload = load_json(path)
                self.assertLessEqual(payload["estimated_prompt_tokens"], chunk_budget["usable_input_limit"])
                self.assertTrue(payload["safe_for_qwen3"])


if __name__ == "__main__":
    unittest.main()
