from __future__ import annotations

import json
import shlex
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from legacy_sql_xml_analyzer.cline_bridge import extract_cline_response_text, process_pending_tasks


class ClineBridgeTests(unittest.TestCase):
    def test_extract_cline_response_text_picks_last_assistant_message(self) -> None:
        stdout_text = "\n".join(
            [
                json.dumps({"type": "status", "message": {"content": "planning"}}),
                json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "{\"ok\": false}"}}),
                json.dumps({"type": "assistant", "message": {"role": "assistant", "content": "{\"ok\": true, \"source\": \"cline\"}"}}),
            ]
        )

        self.assertEqual("{\"ok\": true, \"source\": \"cline\"}", extract_cline_response_text(stdout_text))

    def test_process_pending_generic_task_from_stdout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = root / "analysis"
            tasks_root = analysis_root / "agent_tasks"
            tasks_root.mkdir(parents=True)
            task_path = tasks_root / "task-1.json"
            task_path.write_text(
                json.dumps(
                    {
                        "task_id": "task-1",
                        "phase": "propose",
                        "cluster_id": "reference_target_missing",
                        "prompt_text": "Return JSON only.",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            command = (
                f"{shlex.quote(sys.executable)} -c "
                f"\"import json,sys; sys.stdin.read(); print(json.dumps({{'cluster_id':'reference_target_missing','ok':True}}))\""
            )
            processed = process_pending_tasks(
                mode="generic",
                root=analysis_root,
                stdin_command=command,
                command_template=None,
                command_profile=None,
            )

            self.assertEqual(1, processed)
            result_path = analysis_root / "agent_runs" / "task-1.result.json"
            self.assertTrue(result_path.exists())
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual("task-1", payload["task_id"])
            self.assertEqual("reference_target_missing", payload["cluster_id"])
            self.assertTrue(payload["structured_output"]["ok"])

    def test_process_pending_generic_task_from_cline_json_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            analysis_root = root / "analysis"
            tasks_root = analysis_root / "agent_tasks"
            tasks_root.mkdir(parents=True)
            task_path = tasks_root / "task-json.json"
            task_path.write_text(
                json.dumps(
                    {
                        "task_id": "task-json",
                        "phase": "verify",
                        "cluster_id": "reference_target_missing",
                        "prompt_text": "Return JSON only.",
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )
            fake_cline = root / "fake_cline.py"
            fake_cline.write_text(
                "\n".join(
                    [
                        "import json, sys",
                        "prompt = sys.argv[-1]",
                        "print(json.dumps({'type': 'status', 'message': {'content': 'thinking'}}))",
                        "print(json.dumps({'type': 'assistant', 'message': {'role': 'assistant', 'content': json.dumps({'cluster_id': 'reference_target_missing', 'ok': True, 'prompt_seen': prompt})}}))",
                    ]
                ),
                encoding="utf-8",
            )

            processed = process_pending_tasks(
                mode="generic",
                root=analysis_root,
                stdin_command=None,
                command_template=None,
                command_profile="cline-json",
                cline_command=f"{shlex.quote(sys.executable)} {shlex.quote(str(fake_cline))}",
                cline_cwd=root,
            )

            self.assertEqual(1, processed)
            result_path = analysis_root / "agent_runs" / "task-json.result.json"
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual("reference_target_missing", payload["structured_output"]["cluster_id"])
            self.assertTrue(payload["structured_output"]["ok"])
            self.assertEqual("Return JSON only.", payload["structured_output"]["prompt_seen"])

    def test_process_pending_java_task_uses_context_prompt_and_recommended_result_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            java_root = root / "analysis" / "java_bff"
            task_dir = java_root / "tasks" / "orders-xml-main-ordersearch"
            prompt_dir = java_root / "context_packs" / "orders-xml-main-ordersearch"
            task_dir.mkdir(parents=True)
            prompt_dir.mkdir(parents=True)
            prompt_path = prompt_dir / "phase-1-plan.txt"
            prompt_path.write_text("Return JSON only.", encoding="utf-8")
            result_path = java_root / "agent_runs" / "orders-xml-main-ordersearch" / "phase-1-plan.result.json"
            task_path = task_dir / "phase-1-plan.json"
            task_path.write_text(
                json.dumps(
                    {
                        "task_contract_version": "java-bff-task-v1",
                        "task_id": "orders-xml-main-ordersearch:phase-1-plan:phase-1-plan",
                        "bundle_id": "orders.xml:main:OrderSearch",
                        "phase": "phase-1-plan",
                        "context_prompt_path": str(prompt_path.resolve()),
                        "phase_pack_path": str((java_root / "phase_packs" / "orders-xml-main-ordersearch" / "phase-1-plan.json").resolve()),
                        "recommended_result_path": str(result_path.resolve()),
                    },
                    indent=2,
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            command = (
                f"{shlex.quote(sys.executable)} -c "
                f"\"import json,sys; sys.stdin.read(); print(json.dumps({{'entry_query_id':'orders.xml:main:OrderSearch'}}))\""
            )
            processed = process_pending_tasks(
                mode="java-bff",
                root=java_root,
                stdin_command=command,
                command_template=None,
                command_profile=None,
            )

            self.assertEqual(1, processed)
            self.assertTrue(result_path.exists())
            payload = json.loads(result_path.read_text(encoding="utf-8"))
            self.assertEqual("orders.xml:main:OrderSearch", payload["bundle_id"])
            self.assertEqual("phase-1-plan", payload["phase"])
            self.assertTrue(Path(payload["response_text_path"]).exists())


if __name__ == "__main__":
    unittest.main()
