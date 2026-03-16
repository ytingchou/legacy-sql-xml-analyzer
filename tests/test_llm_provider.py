from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from legacy_sql_xml_analyzer.llm_provider import (
    LlmProviderError,
    build_non_json_error_message,
    parse_sse_chat_completion,
    validate_provider_connection,
)


class ProviderValidationTests(unittest.TestCase):
    def test_parse_sse_chat_completion_reconstructs_message_content(self) -> None:
        payload = parse_sse_chat_completion(
            "\n".join(
                [
                    'data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"demo-model","choices":[{"index":0,"delta":{"role":"assistant","content":"Hel"},"finish_reason":null}]}',
                    'data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"demo-model","choices":[{"index":0,"delta":{"content":"lo"},"finish_reason":null}]}',
                    'data: {"id":"chatcmpl-stream","object":"chat.completion.chunk","created":1,"model":"demo-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}}',
                    "data: [DONE]",
                ]
            )
        )
        self.assertEqual("chatcmpl-stream", payload["id"])
        self.assertEqual("Hello", payload["choices"][0]["message"]["content"])
        self.assertEqual("assistant", payload["choices"][0]["message"]["role"])
        self.assertEqual("stop", payload["choices"][0]["finish_reason"])
        self.assertTrue(payload["stream_reconstructed"])
        self.assertEqual(12, payload["usage"]["total_tokens"])

    def test_build_non_json_error_message_mentions_html_gateway(self) -> None:
        message = build_non_json_error_message(
            "https://llm.example.test/v1/chat/completions",
            "<html><title>Sign In</title></html>",
            "text/html; charset=utf-8",
            "Expecting value",
        )
        self.assertIn("Content-Type: text/html; charset=utf-8", message)
        self.assertIn("returned HTML", message)
        self.assertIn("Response preview", message)

    def test_build_non_json_error_message_mentions_sse_stream(self) -> None:
        message = build_non_json_error_message(
            "https://llm.example.test/v1/chat/completions",
            "data: {\"id\":\"evt-1\"}\n\n",
            "text/event-stream",
            "Expecting value",
        )
        self.assertIn("SSE/streaming", message)
        self.assertIn("Content-Type: text/event-stream", message)

    def test_validate_provider_connection_writes_success_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"
            provider_config_path = root / "provider.json"
            provider_config_path.write_text(
                json.dumps(
                    {
                        "name": "demo-provider",
                        "base_url": "https://llm.example.test/v1",
                        "model": "demo-model",
                        "api_key_env": "TEST_PROVIDER_KEY",
                        "token_limit": 321,
                        "temperature": 0.0,
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            fake_response = {
                "id": "chatcmpl-provider-validation",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {"provider_ok": True, "echo": "provider-validation"},
                                ensure_ascii=False,
                            )
                        }
                    }
                ],
                "usage": {"prompt_tokens": 11, "completion_tokens": 9, "total_tokens": 20},
            }

            with mock.patch.dict("os.environ", {"TEST_PROVIDER_KEY": "secret-key"}, clear=False):
                with mock.patch("legacy_sql_xml_analyzer.llm_provider._post_json", return_value=fake_response):
                    result = validate_provider_connection(
                        output_dir=output_dir,
                        provider_config_path=provider_config_path,
                        expect_json=True,
                    )

            summary = result["summary"]
            self.assertEqual("passed", summary["status"])
            self.assertEqual("demo-model", summary["provider_model"])
            self.assertTrue(summary["request_path"])
            self.assertTrue(summary["response_json_path"])
            self.assertTrue(summary["response_text_path"])

            run_dirs = sorted((output_dir / "analysis" / "provider_validation").iterdir())
            self.assertEqual(1, len(run_dirs))
            run_dir = run_dirs[0]
            request_payload = json.loads((run_dir / "request.json").read_text(encoding="utf-8"))
            summary_payload = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
            debug_payload = json.loads((run_dir / "debug.json").read_text(encoding="utf-8"))
            self.assertEqual(321, request_payload["request_payload"]["max_tokens"])
            self.assertEqual("passed", summary_payload["status"])
            self.assertEqual("demo-provider", summary_payload["provider_name"])
            self.assertEqual("provider-validation", debug_payload["parsed_response"]["echo"])

    def test_validate_provider_connection_writes_failure_debug_payload(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "output"
            provider_config_path = root / "provider.json"
            provider_config_path.write_text(
                json.dumps(
                    {
                        "name": "demo-provider",
                        "base_url": "https://llm.example.test/v1",
                        "model": "demo-model",
                        "api_key_env": "TEST_PROVIDER_KEY",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with mock.patch.dict("os.environ", {"TEST_PROVIDER_KEY": "secret-key"}, clear=False):
                with mock.patch(
                    "legacy_sql_xml_analyzer.llm_provider._post_json",
                    side_effect=LlmProviderError(
                        "LLM provider request failed with HTTP 401 at https://llm.example.test/v1/chat/completions. "
                        "Check the API key and whether the provider accepts Bearer authentication."
                    ),
                ):
                    result = validate_provider_connection(
                        output_dir=output_dir,
                        provider_config_path=provider_config_path,
                        expect_json=True,
                    )

            summary = result["summary"]
            self.assertEqual("failed", summary["status"])
            self.assertEqual("authentication", summary["error"]["category"])
            self.assertTrue(any("API key" in hint or "api key" in hint.lower() for hint in summary["troubleshooting_hints"]))

            run_dirs = sorted((output_dir / "analysis" / "provider_validation").iterdir())
            self.assertEqual(1, len(run_dirs))
            run_dir = run_dirs[0]
            self.assertTrue((run_dir / "request.json").exists())
            self.assertTrue((run_dir / "summary.json").exists())
            self.assertTrue((run_dir / "debug.json").exists())
            debug_payload = json.loads((run_dir / "debug.json").read_text(encoding="utf-8"))
            self.assertEqual("failed", debug_payload["status"])
            self.assertEqual("authentication", debug_payload["error"]["category"])


if __name__ == "__main__":
    unittest.main()
