from __future__ import annotations

import json
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.analyzer import analyze_directory
from legacy_sql_xml_analyzer.evolution import (
    apply_profile_patch_bundle,
    propose_rules_from_analysis,
    review_llm_response_from_analysis,
    simulate_candidate_profile,
)
from legacy_sql_xml_analyzer.lifecycle import grade_profile, promote_profile, rollback_profile
from legacy_sql_xml_analyzer.llm_provider import invoke_llm_from_analysis
from legacy_sql_xml_analyzer.learning import freeze_profile, infer_rules, learn_directory
from legacy_sql_xml_analyzer.prompting import prepare_prompt_pack_from_analysis
from legacy_sql_xml_analyzer.validation import validate_profile


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
            executive_summary_path = output_dir / "analysis" / "executive_summary.json"
            executive_dashboard_path = output_dir / "analysis" / "dashboard.html"
            evolution_summary_path = output_dir / "analysis" / "evolution_summary.json"
            evolution_console_path = output_dir / "analysis" / "evolution_console.html"
            llm_effectiveness_path = output_dir / "analysis" / "llm_effectiveness.csv"
            profile_lifecycle_path = output_dir / "analysis" / "profile_lifecycle.csv"
            artifact_catalog_path = output_dir / "analysis" / "schema" / "artifact_catalog.json"
            overview_path = output_dir / "analysis" / "markdown" / "overview.md"
            query_card = output_dir / "analysis" / "markdown" / "queries" / "main.xml_main_PriceCheck.md"
            self.assertTrue(index_path.exists())
            self.assertTrue(executive_summary_path.exists())
            self.assertTrue(executive_dashboard_path.exists())
            self.assertTrue(evolution_summary_path.exists())
            self.assertTrue(evolution_console_path.exists())
            self.assertTrue(llm_effectiveness_path.exists())
            self.assertTrue(profile_lifecycle_path.exists())
            self.assertTrue(artifact_catalog_path.exists())
            self.assertTrue(overview_path.exists())
            self.assertTrue(query_card.exists())

            payload = json.loads(index_path.read_text(encoding="utf-8"))
            executive_payload = json.loads(executive_summary_path.read_text(encoding="utf-8"))
            complexity_csv_path = output_dir / "analysis" / "executive_complexity.csv"
            trend_csv_path = output_dir / "analysis" / "executive_trend.csv"
            self.assertGreaterEqual(len(payload["artifacts"]), 3)
            self.assertTrue(executive_payload["management_summary"])
            self.assertTrue(executive_payload["complexity_summary"]["top_complex_queries"])
            self.assertTrue(complexity_csv_path.exists())
            self.assertTrue(trend_csv_path.exists())
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

            self.assertNotIn("PARAMETER_PREFIX_INVALID", codes)
            self.assertNotIn("PARAMETER_DATATYPE_INVALID", codes)
            self.assertIn("COMMENT_FORBIDDEN_CHAR", codes)
            self.assertIn("DML_SEMICOLON_MISSING", codes)
            self.assertIn("DATASET_CAST_MISSING", codes)
            self.assertIn("REFERENCE_TARGET_MISSING", codes)
            self.assertIn("SQL_PARAMETER_UNDEFINED", codes)

            diagnostics_dir = output_dir / "analysis" / "markdown" / "diagnostics"
            self.assertTrue(any(path.suffix == ".md" for path in diagnostics_dir.iterdir()))

            failure_clusters_path = output_dir / "analysis" / "failure_clusters.json"
            prompt_pack_dir = output_dir / "analysis" / "prompt_packs"
            self.assertTrue(failure_clusters_path.exists())
            clusters_payload = json.loads(failure_clusters_path.read_text(encoding="utf-8"))
            cluster_ids = {item["cluster_id"] for item in clusters_payload["clusters"]}
            self.assertIn("reference_target_missing", cluster_ids)
            self.assertTrue(any(path.suffix == ".txt" for path in prompt_pack_dir.iterdir()))

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

    def test_validate_profile_reports_improvement_for_useful_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            learn_output_dir = root / "learn-output"
            rule_output_dir = root / "rule-output"
            validation_output_dir = root / "validation-output"
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

            learn_directory(input_dir=input_dir, output_dir=learn_output_dir)
            infer_rules(learn_output_dir / "learning" / "observations.json", rule_output_dir)
            freeze_profile(rule_output_dir / "learning" / "rule_candidates.json", frozen_profile_path, min_confidence=0.8)

            result = validate_profile(
                input_dir=input_dir,
                output_dir=validation_output_dir,
                profile_path=frozen_profile_path,
            )

            self.assertEqual("improved", result["assessment"]["classification"])
            self.assertTrue((validation_output_dir / "validation" / "profile_validation.json").exists())
            payload = json.loads((validation_output_dir / "validation" / "profile_validation.json").read_text(encoding="utf-8"))
            self.assertGreater(payload["delta"]["resolved_queries_delta"], 0)
            self.assertLess(payload["delta"]["error_delta"], 0)

    def test_analyze_writes_run_history_snapshots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "sample.xml").write_text(
                """<sql-mapping>
  <main-query name="SnapshotMain">
    <parameter name=":demo" data_type="String" />
    <sql-body><![CDATA[
      select CAST(:demo AS VARCHAR2(20)) demo_col from dual
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            analyze_directory(input_dir=input_dir, output_dir=output_dir, snapshot_label="first-pass")
            analyze_directory(input_dir=input_dir, output_dir=output_dir, snapshot_label="second-pass")

            history_index_path = output_dir / "analysis" / "history" / "index.json"
            latest_path = output_dir / "analysis" / "history" / "latest.json"
            run_snapshot_path = output_dir / "analysis" / "run_snapshot.json"
            self.assertTrue(history_index_path.exists())
            self.assertTrue(latest_path.exists())
            self.assertTrue(run_snapshot_path.exists())

            history_payload = json.loads(history_index_path.read_text(encoding="utf-8"))
            self.assertEqual(2, len(history_payload["snapshots"]))
            self.assertEqual("second-pass", history_payload["snapshots"][-1]["label"])

            latest_payload = json.loads(latest_path.read_text(encoding="utf-8"))
            self.assertEqual("second-pass", latest_payload["label"])

            executive_payload = json.loads((output_dir / "analysis" / "executive_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(2, executive_payload["trend_summary"]["snapshot_count"])
            self.assertIn("stable", executive_payload["trend_summary"]["status_line"].lower())

    def test_prepare_prompt_pack_from_failure_cluster(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            input_dir.mkdir()

            (input_dir / "broken.xml").write_text(
                """<sql-mapping>
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            result = prepare_prompt_pack_from_analysis(
                analysis_root=output_dir,
                cluster_id="reference_target_missing",
                budget="32k",
                model="weak-128k",
            )

            self.assertEqual("reference_target_missing", result["cluster"]["cluster_id"])
            prompt_dir = output_dir / "analysis" / "prompt_packs"
            prompt_text = (prompt_dir / "reference_target_missing-32k-weak-128k.txt").read_text(encoding="utf-8")
            prompt_json = json.loads((prompt_dir / "reference_target_missing-32k-weak-128k.json").read_text(encoding="utf-8"))
            classify_text = (prompt_dir / "reference_target_missing-32k-weak-128k-classify.txt").read_text(encoding="utf-8")
            verify_text = (prompt_dir / "reference_target_missing-32k-weak-128k-verify.txt").read_text(encoding="utf-8")
            bundle_json = json.loads((prompt_dir / "reference_target_missing-32k-weak-128k-bundle.json").read_text(encoding="utf-8"))
            self.assertIn("Return JSON only with this schema:", prompt_text)
            self.assertEqual("mapping_inference", prompt_json["task_type"])
            self.assertEqual("propose", prompt_json["stage"])
            self.assertIn("Stage: classify", classify_text)
            self.assertIn("Stage: verify", verify_text)
            self.assertIn("classify", bundle_json["stages"])
            self.assertIn("verify", bundle_json["stages"])

    def test_review_llm_response_generates_patch_candidate_and_verify_prompt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            response_path = root / "response.json"
            input_dir.mkdir()

            (input_dir / "broken.xml").write_text(
                """<sql-mapping>
  <main-query name="MissingRef">
    <ext-sql-refer-to name="__MISSING__" xml="shared" main-query="SharedMain" />
    <sql-body><![CDATA[
      select * from dual __MISSING__
    ]]></sql-body>
  </main-query>
</sql-mapping>
""",
                encoding="utf-8",
            )

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            response_path.write_text(
                json.dumps(
                    {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "root_cause": "The external xml alias does not match a real file name.",
                        "proposed_change_type": "profile_rule",
                        "proposed_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {
                                "xml_name": "shared",
                                "mapped_to": "shared-query.xml",
                            },
                        },
                        "confidence": "high",
                        "why": ["The alias is stable and points to a single expected target name."],
                        "verification_steps": ["Run analyze again with the merged profile patch."],
                        "risks": ["The mapping could be wrong if another module uses a different shared alias."],
                        "insufficient_evidence": False,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            result = review_llm_response_from_analysis(
                analysis_root=output_dir,
                cluster_id="reference_target_missing",
                response_path=response_path,
                stage="propose",
                budget="32k",
                model="weak-128k",
            )

            review = result["review"]
            self.assertEqual("accepted", review["status"])
            self.assertTrue(review["safe_to_apply_candidate"])
            self.assertEqual("external_xml_name_mapping", review["profile_patch_candidate"]["rule_type"])
            self.assertEqual("next-verify", review["next_prompt_kind"])

            review_dir = output_dir / "analysis" / "llm_reviews"
            review_payload = json.loads((review_dir / "reference_target_missing-propose-review.json").read_text(encoding="utf-8"))
            patch_payload = json.loads((review_dir / "reference_target_missing-propose-profile-patch.json").read_text(encoding="utf-8"))
            verify_prompt = (review_dir / "reference_target_missing-propose-next-verify.txt").read_text(encoding="utf-8")
            index_payload = json.loads((output_dir / "analysis" / "index.json").read_text(encoding="utf-8"))
            self.assertEqual("accepted", review_payload["status"])
            self.assertEqual("external_xml_name_mapping", patch_payload["rule_type"])
            self.assertIn("Prior stage response to reuse:", verify_prompt)
            self.assertTrue(
                any(item["path"].endswith("reference_target_missing-propose-review.json") for item in index_payload["artifacts"])
            )

    def test_propose_rules_and_apply_patch_bundle_generate_candidate_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            response_path = root / "response.json"
            merged_profile_path = root / "profiles" / "merged_profile.json"
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            response_path.write_text(
                json.dumps(
                    {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "root_cause": "The alias shared should map to shared-query.xml.",
                        "proposed_change_type": "profile_rule",
                        "proposed_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {
                                "xml_name": "shared",
                                "mapped_to": "shared-query.xml",
                            },
                        },
                        "confidence": "high",
                        "why": ["The alias is consistent across the observed failure cluster."],
                        "verification_steps": ["Run analyze with the candidate profile."],
                        "risks": ["Another module could reuse the alias differently."],
                        "insufficient_evidence": False,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            review_llm_response_from_analysis(
                analysis_root=output_dir,
                cluster_id="reference_target_missing",
                response_path=response_path,
                stage="propose",
            )

            proposal_result = propose_rules_from_analysis(analysis_root=output_dir, min_confidence=0.7)
            candidate_profile_path = proposal_result["candidate_profile_path"]
            proposal_payload = proposal_result["proposal_payload"]

            self.assertEqual(1, proposal_payload["summary"]["accepted_patch_count"])
            self.assertTrue(candidate_profile_path.exists())
            candidate_profile_payload = json.loads(candidate_profile_path.read_text(encoding="utf-8"))
            self.assertEqual("shared-query.xml", candidate_profile_payload["external_xml_name_map"]["shared"])

            merged_profile = apply_profile_patch_bundle(
                patch_bundle_path=output_dir / "analysis" / "proposals" / "rule_proposals.json",
                output_path=merged_profile_path,
            )
            self.assertIn("shared", merged_profile.external_xml_name_map)

            healed = analyze_directory(
                input_dir=input_dir,
                output_dir=root / "healed-output",
                profile_path=merged_profile_path,
            )
            healed_codes = {diagnostic.code for diagnostic in healed.diagnostics}
            self.assertNotIn("REFERENCE_TARGET_MISSING", healed_codes)

    def test_simulate_candidate_profile_reports_improvement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            simulation_output_dir = root / "simulation-output"
            response_path = root / "response.json"
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            response_path.write_text(
                json.dumps(
                    {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "root_cause": "The alias shared should map to shared-query.xml.",
                        "proposed_change_type": "profile_rule",
                        "proposed_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {
                                "xml_name": "shared",
                                "mapped_to": "shared-query.xml",
                            },
                        },
                        "confidence": "high",
                        "why": ["The alias is consistent across the observed failure cluster."],
                        "verification_steps": ["Run analyze with the candidate profile."],
                        "risks": ["Another module could reuse the alias differently."],
                        "insufficient_evidence": False,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            review_llm_response_from_analysis(
                analysis_root=output_dir,
                cluster_id="reference_target_missing",
                response_path=response_path,
                stage="propose",
            )
            propose_rules_from_analysis(analysis_root=output_dir, min_confidence=0.7)

            simulation_result = simulate_candidate_profile(
                input_dir=input_dir,
                output_dir=simulation_output_dir,
                analysis_root=output_dir,
            )

            self.assertEqual("improved", simulation_result["assessment"]["classification"])
            simulation_payload = json.loads(
                (simulation_output_dir / "simulation" / "profile_simulation.json").read_text(encoding="utf-8")
            )
            self.assertGreater(
                simulation_payload["validation_payload"]["delta"]["resolved_queries_delta"],
                0,
            )

    def test_grade_and_promote_profile_after_simulation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            simulation_output_dir = root / "simulation-output"
            grade_output_dir = root / "grade-output"
            promoted_profile_path = root / "profiles" / "promoted_profile.json"
            response_path = root / "response.json"
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            response_path.write_text(
                json.dumps(
                    {
                        "cluster_id": "reference_target_missing",
                        "problem_type": "mapping_inference",
                        "root_cause": "The alias shared should map to shared-query.xml.",
                        "proposed_change_type": "profile_rule",
                        "proposed_rule_or_fix": {
                            "rule_type": "external_xml_name_mapping",
                            "scope": "global",
                            "payload": {
                                "xml_name": "shared",
                                "mapped_to": "shared-query.xml",
                            },
                        },
                        "confidence": "high",
                        "why": ["The alias is consistent across the observed failure cluster."],
                        "verification_steps": ["Run analyze with the candidate profile."],
                        "risks": ["Another module could reuse the alias differently."],
                        "insufficient_evidence": False,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            review_llm_response_from_analysis(
                analysis_root=output_dir,
                cluster_id="reference_target_missing",
                response_path=response_path,
                stage="propose",
            )
            proposal_result = propose_rules_from_analysis(analysis_root=output_dir, min_confidence=0.7)
            candidate_profile_path = proposal_result["candidate_profile_path"]
            simulate_candidate_profile(
                input_dir=input_dir,
                output_dir=simulation_output_dir,
                analysis_root=output_dir,
            )

            grade_result = grade_profile(
                profile_path=candidate_profile_path,
                validation_report_path=simulation_output_dir / "simulation" / "profile_simulation.json",
                output_dir=grade_output_dir,
            )
            self.assertEqual("trial", grade_result["grade_payload"]["suggested_status"])
            self.assertEqual("promote", grade_result["grade_payload"]["promotion_readiness"])

            promoted_profile = promote_profile(
                profile_path=candidate_profile_path,
                grade_report_path=grade_output_dir / "grade" / "profile_grade.json",
                output_path=promoted_profile_path,
                profile_name="company-candidate",
            )
            self.assertEqual("trial", promoted_profile.profile_status)
            self.assertEqual("company-candidate", promoted_profile.profile_name)
            self.assertEqual(1, len(promoted_profile.validation_history))
            self.assertEqual("improved", promoted_profile.validation_history[0]["assessment_classification"])

    def test_grade_profile_can_promote_trial_to_trusted_after_repeated_improvement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            profile_path = root / "trial_profile.json"
            validation_report_path = root / "validation" / "profile_validation.json"
            grade_output_dir = root / "grade-output"
            validation_report_path.parent.mkdir(parents=True)

            profile_payload = {
                "profile_version": 2,
                "profile_name": "trial-profile",
                "profile_status": "trial",
                "reference_target_default_order": ["sub", "main"],
                "reference_token_patterns": ["{name}"],
                "external_xml_name_map": {"shared": "shared-query.xml"},
                "external_xml_scoped_map": {},
                "ignore_tags": [],
                "rules": [],
                "validation_history": [
                    {
                        "assessment_classification": "improved",
                        "suggested_status": "trial",
                        "promotion_readiness": "promote",
                    }
                ],
            }
            profile_path.write_text(json.dumps(profile_payload, indent=2, ensure_ascii=False), encoding="utf-8")
            validation_report_path.write_text(
                json.dumps(
                    {
                        "assessment": {
                            "classification": "improved",
                            "recommendation": "good",
                            "hard_regressions": [],
                            "soft_regressions": [],
                            "improvements": ["Resolved query count increased."],
                        },
                        "delta": {
                            "resolved_queries_delta": 1,
                            "failed_queries_delta": -1,
                            "error_delta": -1,
                            "warning_delta": 0,
                        },
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            grade_result = grade_profile(
                profile_path=profile_path,
                validation_report_path=validation_report_path,
                output_dir=grade_output_dir,
            )

            self.assertEqual("trusted", grade_result["grade_payload"]["suggested_status"])
            self.assertEqual("promote", grade_result["grade_payload"]["promotion_readiness"])

    def test_rollback_profile_restores_parent_profile_and_writes_history_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            parent_profile_path = root / "profiles" / "candidate_profile.json"
            promoted_profile_path = root / "profiles" / "trusted_profile.json"
            rolled_back_profile_path = root / "profiles" / "rollback_profile.json"
            parent_profile_path.parent.mkdir(parents=True)

            parent_profile_path.write_text(
                json.dumps(
                    {
                        "profile_version": 1,
                        "profile_name": "candidate-profile",
                        "profile_status": "candidate",
                        "reference_target_default_order": ["sub", "main"],
                        "reference_token_patterns": ["{name}"],
                        "external_xml_name_map": {"shared": "shared-query.xml"},
                        "external_xml_scoped_map": {},
                        "ignore_tags": ["custom-node"],
                        "rules": [],
                        "validation_history": [],
                        "lifecycle_history": [],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            promoted_profile_path.write_text(
                json.dumps(
                    {
                        "profile_version": 2,
                        "profile_name": "trusted-profile",
                        "profile_status": "trusted",
                        "parent_profile": str(parent_profile_path.resolve()),
                        "reference_target_default_order": ["main", "sub"],
                        "reference_token_patterns": ["{name}", "/*{name}*/"],
                        "external_xml_name_map": {"shared": "shared-query.xml", "extra": "extra.xml"},
                        "external_xml_scoped_map": {},
                        "ignore_tags": ["custom-node", "vendor-node"],
                        "rules": [],
                        "validation_history": [
                            {
                                "assessment_classification": "improved",
                                "suggested_status": "trial",
                                "promotion_readiness": "promote",
                            }
                        ],
                        "lifecycle_history": [
                            {
                                "event_type": "promote",
                                "from_status": "candidate",
                                "to_status": "trusted",
                            }
                        ],
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            rolled_back = rollback_profile(
                profile_path=promoted_profile_path,
                output_path=rolled_back_profile_path,
                reason="Regression found during validation.",
                profile_name="rolled-back-profile",
            )

            self.assertEqual("candidate", rolled_back.profile_status)
            self.assertEqual("rolled-back-profile", rolled_back.profile_name)
            self.assertEqual({"shared": "shared-query.xml"}, rolled_back.external_xml_name_map)
            self.assertEqual("Regression found during validation.", rolled_back.lifecycle_history[-1]["reason"])
            self.assertTrue((root / "profiles" / "rollback_profile.history.json").exists())
            self.assertTrue((root / "profiles" / "rollback_profile.history.md").exists())

    def test_invoke_llm_generates_evolution_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            provider_config_path = root / "provider.json"
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            provider_config_path.write_text(
                json.dumps(
                    {
                        "name": "company-weak-llm",
                        "base_url": "https://provider.example.com/v1",
                        "model": "weak-128k",
                        "api_key": "test-key",
                        "token_limit": 1024,
                    },
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            fake_response = {
                "id": "resp-123",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "cluster_id": "reference_target_missing",
                                    "problem_type": "mapping_inference",
                                    "root_cause": "shared should map to shared-query.xml",
                                    "proposed_change_type": "profile_rule",
                                    "proposed_rule_or_fix": {
                                        "rule_type": "external_xml_name_mapping",
                                        "scope": "global",
                                        "payload": {"xml_name": "shared", "mapped_to": "shared-query.xml"},
                                    },
                                    "confidence": "high",
                                    "why": ["Observed consistently."],
                                    "verification_steps": ["Run simulate-profile."],
                                    "risks": ["Low."],
                                    "insufficient_evidence": False,
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ],
                "usage": {"completion_tokens": 123},
            }

            with mock.patch(
                "legacy_sql_xml_analyzer.llm_provider._post_json",
                return_value=fake_response,
            ):
                invoke_llm_from_analysis(
                    analysis_root=output_dir,
                    cluster_id="reference_target_missing",
                    provider_config_path=provider_config_path,
                    review=True,
                )

            evolution_payload = json.loads(
                (output_dir / "analysis" / "evolution_summary.json").read_text(encoding="utf-8")
            )
            scoreboard_payload = json.loads(
                (output_dir / "analysis" / "prompt_scoreboard.json").read_text(encoding="utf-8")
            )
            self.assertEqual(1, evolution_payload["headline"]["llm_run_count"])
            self.assertEqual(1, evolution_payload["headline"]["accepted_reviews"])
            self.assertTrue(scoreboard_payload)
            self.assertEqual("company-weak-llm", scoreboard_payload[0]["provider_name"])
            self.assertTrue((output_dir / "analysis" / "evolution_console.html").exists())

    def test_invoke_llm_uses_openai_compatible_provider_and_token_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "input"
            output_dir = root / "output"
            provider_config_path = root / "provider.json"
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

            analyze_directory(input_dir=input_dir, output_dir=output_dir)
            provider_config_path.write_text(
                json.dumps(
                    {
                        "name": "demo-provider",
                        "base_url": "https://llm.example.test/v1",
                        "model": "demo-model",
                        "api_key_env": "TEST_LLM_KEY",
                        "token_limit": 777,
                        "temperature": 0.1,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            fake_response = {
                "id": "chatcmpl-demo",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "cluster_id": "reference_target_missing",
                                    "problem_type": "mapping_inference",
                                    "root_cause": "shared should map to shared-query.xml.",
                                    "proposed_change_type": "profile_rule",
                                    "proposed_rule_or_fix": {
                                        "rule_type": "external_xml_name_mapping",
                                        "scope": "global",
                                        "payload": {
                                            "xml_name": "shared",
                                            "mapped_to": "shared-query.xml",
                                        },
                                    },
                                    "confidence": "high",
                                    "why": ["The alias matches a single observed file."],
                                    "verification_steps": ["Run analyze with the candidate profile."],
                                    "risks": ["Another module could override the alias."],
                                    "insufficient_evidence": False,
                                },
                                ensure_ascii=False,
                            )
                        }
                    }
                ],
                "usage": {
                    "prompt_tokens": 123,
                    "completion_tokens": 45,
                    "total_tokens": 168,
                },
            }

            with mock.patch.dict("os.environ", {"TEST_LLM_KEY": "secret-key"}, clear=False):
                with mock.patch("legacy_sql_xml_analyzer.llm_provider._post_json", return_value=fake_response) as mocked_post:
                    result = invoke_llm_from_analysis(
                        analysis_root=output_dir,
                        cluster_id="reference_target_missing",
                        stage="propose",
                        budget="32k",
                        prompt_model="weak-128k",
                        provider_config_path=provider_config_path,
                        review=True,
                    )

            mocked_post.assert_called_once()
            call_args = mocked_post.call_args.kwargs
            self.assertEqual(777, call_args["payload"]["max_tokens"])
            self.assertEqual("demo-model", call_args["payload"]["model"])

            run_summary = result["run_summary"]
            self.assertEqual(777, run_summary["token_limit"])
            self.assertEqual("demo-model", run_summary["provider_model"])
            self.assertEqual("accepted", result["review"]["review"]["status"])

            llm_runs_root = output_dir / "analysis" / "llm_runs"
            self.assertTrue(any(path.is_dir() for path in llm_runs_root.iterdir()))
            self.assertTrue((llm_runs_root / "index.json").exists())
            latest_run = sorted(path for path in llm_runs_root.iterdir() if path.is_dir())[-1]
            summary_payload = json.loads((latest_run / "run_summary.json").read_text(encoding="utf-8"))
            request_payload = json.loads((latest_run / "request.json").read_text(encoding="utf-8"))
            self.assertEqual(777, summary_payload["token_limit"])
            self.assertEqual("demo-provider", summary_payload["provider_name"])
            self.assertEqual(777, request_payload["request_payload"]["max_tokens"])


if __name__ == "__main__":
    unittest.main()
