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

from legacy_sql_xml_analyzer.analyzer import analyze_directory
from legacy_sql_xml_analyzer.learning import freeze_profile, infer_rules, learn_directory


class AnalyzerIntegrationTests(unittest.TestCase):
    def test_analyze_directory_generates_json_and_markdown_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "shared.xml").write_text(
                """<sql-mapping>
  <sub-query name="BaseFilter">
    <sql-body><![CDATA[
      and status = 'ACTIVE'
    ]]></sql-body>
  </sub-query>
  <main-query name="SharedMain">
    <sql-refer-to name="__BASE__" sub-query="BaseFilter" />
    <sql-body><![CDATA[
      select * from shared_table where 1=1 __BASE__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )
            (input_dir / "main.xml").write_text(
                """<sql-mapping>
  <main-query name="PriceCheck">
    <parameter name=":fPriceCheckRule" data_type="String" sample="ABC" default="ABC" />
    <parameter name=":datasetValue" data_type="String" />
    <sql-refer-to name="__LOCAL__" sub-query="LocalFilter" />
    <ext-sql-refer-to name="__EXT__" xml="shared.xml" main-query="SharedMain" />
    <sql-body><![CDATA[
      select CAST(:datasetValue AS VARCHAR2(20)) dataset_col from dual
      union all
      select * from pricing where rule = :fPriceCheckRule __LOCAL__ __EXT__
    ]]></sql-body>
  </main-query>
  <sub-query name="LocalFilter">
    <sql-body><![CDATA[
      and price > 0
    ]]></sql-body>
  </sub-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            result = analyze_directory(input_dir=input_dir, output_dir=output_dir)

            self.assertEqual(4, len(result.queries))
            self.assertTrue(any(item.query.name == "PriceCheck" for item in result.resolved_queries))

            price_check = next(item for item in result.resolved_queries if item.query.name == "PriceCheck")
            self.assertEqual("resolved", price_check.status)
            self.assertIn("shared_table", price_check.resolved_sql or "")
            self.assertIn("pricing", price_check.resolved_sql or "")
            self.assertIn(":fPriceCheckRule", price_check.sql_stats["parameters"])

            index_path = output_dir / "analysis" / "index.json"
            overview_path = output_dir / "analysis" / "markdown" / "overview.md"
            query_card = output_dir / "analysis" / "markdown" / "queries" / "main.xml_main_PriceCheck.md"
            self.assertTrue(index_path.exists())
            self.assertTrue(overview_path.exists())
            self.assertTrue(query_card.exists())

            payload = json.loads(index_path.read_text(encoding="utf-8"))
            self.assertGreaterEqual(len(payload["artifacts"]), 3)
            self.assertEqual(0, len([item for item in result.diagnostics if item.severity in {"error", "fatal"}]))

    def test_lints_and_diagnostics_are_emitted_for_rule_violations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "broken.xml").write_text(
                """<sql-mapping>
  <main-query name="BrokenDml">
    <parameter name="brokenParam" data_type="Decimal" />
    <sql-body><![CDATA[
      update price_table set memo = 'x'
      -- bad : comment
      where code = :missingParam
    ]]></sql-body>
  </main-query>
  <main-query name="DatasetNoCast">
    <parameter name=":datasetParam" data_type="String" />
    <sql-body><![CDATA[
      select :datasetParam dataset_col from dual
    ]]></sql-body>
  </main-query>
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

            result = analyze_directory(input_dir=input_dir, output_dir=output_dir)
            codes = {diagnostic.code for diagnostic in result.diagnostics}

            self.assertIn("PARAMETER_PREFIX_INVALID", codes)
            self.assertIn("PARAMETER_DATATYPE_INVALID", codes)
            self.assertIn("COMMENT_FORBIDDEN_CHAR", codes)
            self.assertIn("DML_SEMICOLON_MISSING", codes)
            self.assertIn("DATASET_CAST_MISSING", codes)
            self.assertIn("REFERENCE_TARGET_MISSING", codes)
            self.assertIn("SQL_PARAMETER_UNDEFINED", codes)

            diagnostics_dir = output_dir / "analysis" / "markdown" / "diagnostics"
            self.assertTrue(any(path.suffix == ".md" for path in diagnostics_dir.iterdir()))

    def test_cycle_detection_marks_resolution_failed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "cycle.xml").write_text(
                """<sql-mapping>
  <main-query name="A">
    <sql-refer-to name="__B__" main-query="B" />
    <sql-body><![CDATA[
      select * from dual __B__
    ]]></sql-body>
  </main-query>
  <main-query name="B">
    <sql-refer-to name="__A__" main-query="A" />
    <sql-body><![CDATA[
      select * from dual __A__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            result = analyze_directory(input_dir=input_dir, output_dir=output_dir)

            self.assertIn("CYCLE_DETECTED", {diagnostic.code for diagnostic in result.diagnostics})
            status_by_name = {item.query.name: item.status for item in result.resolved_queries}
            self.assertEqual("failed", status_by_name["A"])
            self.assertEqual("failed", status_by_name["B"])

    def test_learning_and_profile_can_fix_external_xml_alias(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            learn_output_dir = root / "learn-output"
            rule_output_dir = root / "rule-output"
            analyze_output_dir = root / "analyze-output"
            frozen_profile_path = root / "profiles" / "company_profile.json"
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

            baseline = analyze_directory(input_dir=input_dir, output_dir=analyze_output_dir / "baseline")
            self.assertIn("REFERENCE_TARGET_MISSING", {diagnostic.code for diagnostic in baseline.diagnostics})

            learn_result = learn_directory(input_dir=input_dir, output_dir=learn_output_dir)
            self.assertEqual(2, learn_result["observations"]["summary"]["xml_file_count"])

            infer_result = infer_rules(learn_output_dir / "learning" / "observations.json", rule_output_dir)
            inferred_profile_path = rule_output_dir / "learning" / "rule_candidates.json"
            self.assertTrue(inferred_profile_path.exists())
            self.assertEqual("shared-query.xml", infer_result["profile"].external_xml_name_map["shared"])

            frozen_profile = freeze_profile(inferred_profile_path, frozen_profile_path, min_confidence=0.8)
            self.assertIn("shared", frozen_profile.external_xml_name_map)

            healed = analyze_directory(
                input_dir=input_dir,
                output_dir=analyze_output_dir / "healed",
                profile_path=frozen_profile_path,
            )
            healed_codes = {diagnostic.code for diagnostic in healed.diagnostics}
            self.assertNotIn("REFERENCE_TARGET_MISSING", healed_codes)
            consumer = next(item for item in healed.resolved_queries if item.query.name == "ConsumerMain")
            self.assertEqual("resolved", consumer.status)
            self.assertIn("shared_table", consumer.resolved_sql or "")

    def test_profile_can_apply_token_wrapper_and_ignore_tag_rules_with_delta_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            learn_output_dir = root / "learn-output"
            rule_output_dir = root / "rule-output"
            analyze_output_dir = root / "analyze-output"
            frozen_profile_path = root / "profiles" / "company_profile.json"
            input_dir.mkdir()

            (input_dir / "shared.xml").write_text(
                """<sql-mapping>
  <meta />
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
  <meta />
  <main-query name="ConsumerMain">
    <ext-sql-refer-to name="__EXT__" xml="shared.xml" main-query="SharedMain" />
    <sql-body><![CDATA[
      select * from dual /*__EXT__*/
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            learn_result = learn_directory(input_dir=input_dir, output_dir=learn_output_dir)
            self.assertIn("meta", learn_result["observations"]["top_level_unknown_tags"])
            self.assertIn("/*{name}*/", learn_result["observations"]["reference_patterns"]["token_patterns"])

            infer_result = infer_rules(learn_output_dir / "learning" / "observations.json", rule_output_dir)
            inferred_profile_path = rule_output_dir / "learning" / "rule_candidates.json"
            self.assertIn("meta", infer_result["profile"].ignore_tags)
            self.assertIn("/*{name}*/", infer_result["profile"].reference_token_patterns)

            freeze_profile(inferred_profile_path, frozen_profile_path, min_confidence=0.8)

            healed = analyze_directory(
                input_dir=input_dir,
                output_dir=analyze_output_dir,
                profile_path=frozen_profile_path,
            )
            consumer = next(item for item in healed.resolved_queries if item.query.name == "ConsumerMain")
            self.assertEqual("resolved", consumer.status)
            self.assertIn("select * from shared_table", consumer.resolved_sql or "")
            self.assertNotIn("/*select * from shared_table*/", consumer.resolved_sql or "")

            codes = {diagnostic.code for diagnostic in healed.diagnostics}
            self.assertNotIn("UNSUPPORTED_TAG", codes)

            applied_rules_path = analyze_output_dir / "analysis" / "applied_rules.json"
            fix_delta_path = analyze_output_dir / "analysis" / "fix_delta.json"
            self.assertTrue(applied_rules_path.exists())
            self.assertTrue(fix_delta_path.exists())

            applied_payload = json.loads(applied_rules_path.read_text(encoding="utf-8"))
            usage_keys = set(applied_payload["usage"])
            self.assertTrue(any(key.startswith("ignore_tag:meta") for key in usage_keys))
            self.assertTrue(any(key.startswith("reference_token_pattern:/*{name}*/") for key in usage_keys))

            delta_payload = json.loads(fix_delta_path.read_text(encoding="utf-8"))
            self.assertLess(delta_payload["delta"]["warning_delta"], 0)
            self.assertLess(delta_payload["delta"]["diagnostics_by_code_delta"]["UNSUPPORTED_TAG"], 0)

    def test_profile_can_resolve_cross_folder_external_xml_with_source_scoped_mapping(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            learn_output_dir = root / "learn-output"
            rule_output_dir = root / "rule-output"
            analyze_output_dir = root / "analyze-output"
            frozen_profile_path = root / "profiles" / "company_profile.json"
            (input_dir / "moduleA" / "query").mkdir(parents=True)
            (input_dir / "moduleA" / "shared").mkdir(parents=True)
            (input_dir / "moduleB" / "query").mkdir(parents=True)
            (input_dir / "moduleB" / "shared").mkdir(parents=True)

            (input_dir / "moduleA" / "shared" / "common.xml").write_text(
                """<sql-mapping>
  <main-query name="SharedMain">
    <sql-body><![CDATA[
      select 'A' as module_name from dual
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )
            (input_dir / "moduleB" / "shared" / "common.xml").write_text(
                """<sql-mapping>
  <main-query name="SharedMain">
    <sql-body><![CDATA[
      select 'B' as module_name from dual
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )
            (input_dir / "moduleA" / "query" / "consumer.xml").write_text(
                """<sql-mapping>
  <main-query name="ConsumerMainA">
    <ext-sql-refer-to name="__EXT__" xml="common" main-query="SharedMain" />
    <sql-body><![CDATA[
      select * from dual __EXT__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )
            (input_dir / "moduleB" / "query" / "consumer.xml").write_text(
                """<sql-mapping>
  <main-query name="ConsumerMainB">
    <ext-sql-refer-to name="__EXT__" xml="common" main-query="SharedMain" />
    <sql-body><![CDATA[
      select * from dual __EXT__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            baseline = analyze_directory(input_dir=input_dir, output_dir=analyze_output_dir / "baseline")
            self.assertIn("REFERENCE_TARGET_MISSING", {diagnostic.code for diagnostic in baseline.diagnostics})

            learn_directory(input_dir=input_dir, output_dir=learn_output_dir)
            infer_result = infer_rules(learn_output_dir / "learning" / "observations.json", rule_output_dir)
            scoped_map = infer_result["profile"].external_xml_scoped_map
            self.assertEqual("moduleA/shared/common.xml", scoped_map["moduleA/query::common"])
            self.assertEqual("moduleB/shared/common.xml", scoped_map["moduleB/query::common"])

            freeze_profile(rule_output_dir / "learning" / "rule_candidates.json", frozen_profile_path, min_confidence=0.8)
            healed = analyze_directory(
                input_dir=input_dir,
                output_dir=analyze_output_dir / "healed",
                profile_path=frozen_profile_path,
            )

            codes = {diagnostic.code for diagnostic in healed.diagnostics}
            self.assertNotIn("REFERENCE_TARGET_MISSING", codes)
            resolved_by_name = {item.query.name: item.resolved_sql or "" for item in healed.resolved_queries}
            self.assertIn("select 'A' as module_name from dual", resolved_by_name["ConsumerMainA"])
            self.assertIn("select 'B' as module_name from dual", resolved_by_name["ConsumerMainB"])


if __name__ == "__main__":
    unittest.main()
