from __future__ import annotations

import unittest

from legacy_sql_xml_analyzer.company_prompt_profiles import build_response_template, render_company_prompt


class CompanyPromptProfilesTests(unittest.TestCase):
    def test_build_response_template_preserves_shape(self) -> None:
        schema = {
            "bundle_id": "string",
            "steps": ["string"],
            "proposal": {"rule_type": "string", "payload": {}},
        }

        template = build_response_template(schema)

        self.assertEqual(schema, template)

    def test_render_company_prompt_includes_constraints_and_json_only(self) -> None:
        prompt = render_company_prompt(
            profile_name="company-qwen3-propose",
            title="Company weak-LLM task",
            objective_lines=["Handle one cluster only."],
            evidence_sections=[("Problem Summary", "cluster_id: reference_target_missing")],
            schema={"cluster_id": "string", "insufficient_evidence": False},
        )

        self.assertIn("Allowed actions:", prompt)
        self.assertIn("Forbidden actions:", prompt)
        self.assertIn("Return JSON only", prompt)
        self.assertIn('"cluster_id": "string"', prompt)


if __name__ == "__main__":
    unittest.main()
