from __future__ import annotations

import unittest

from legacy_sql_xml_analyzer.response_normalizer import normalize_response


class ResponseNormalizerTests(unittest.TestCase):
    def test_normalize_fenced_json_with_trailing_comma(self) -> None:
        raw = """```json
{
  "status": "accepted",
  "issues": [],
}
```"""

        result = normalize_response(raw, source="unit-test")

        self.assertIsNotNone(result.normalized_object)
        self.assertEqual("accepted", result.normalized_object["status"])
        self.assertIn("removed_markdown_fences", result.applied_steps)
        self.assertTrue(
            "repaired_common_json_issues" in result.applied_steps or "parsed_python_literal" in result.applied_steps
        )

    def test_normalize_cline_json_events_extracts_final_assistant_json(self) -> None:
        raw = "\n".join(
            [
                '{"type":"status","message":"thinking"}',
                '{"type":"assistant","message":{"content":"{\\"verdict\\": \\"accept\\", \\"safe_to_apply\\": true}"}}',
            ]
        )

        result = normalize_response(raw, source="cline-json")

        self.assertIsNotNone(result.normalized_object)
        self.assertEqual("accept", result.normalized_object["verdict"])
        self.assertTrue(result.normalized_object["safe_to_apply"])
        self.assertEqual("cline_json_event_stream", result.source_type)

    def test_normalize_python_literal_object(self) -> None:
        raw = "{'cluster_id': 'reference_target_missing', 'insufficient_evidence': True}"

        result = normalize_response(raw, source="unit-test")

        self.assertIsNotNone(result.normalized_object)
        self.assertEqual("reference_target_missing", result.normalized_object["cluster_id"])
        self.assertIn("parsed_python_literal", result.applied_steps)


if __name__ == "__main__":
    unittest.main()
