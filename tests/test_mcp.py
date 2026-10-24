import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from delta_history.mcp import MAX_OUTPUT, PROTOCOL_VERSION, Server, serve


def initialize(identifier=1):
    return {"jsonrpc": "2.0", "id": identifier, "method": "initialize", "params": {
        "protocolVersion": PROTOCOL_VERSION, "capabilities": {}, "clientInfo": {"name": "fixture", "version": "1"}}}


def request(method, params=None, identifier=2):
    return {"jsonrpc": "2.0", "id": identifier, "method": method, "params": params or {}}


class McpTests(unittest.TestCase):
    def setUp(self):
        self.sessions = patch("delta_history.mcp.project_sessions", return_value=[{"session_key": "fixture"}]).start()
        self.addCleanup(patch.stopall)
        self.server = Server({}, Path("/tmp/not-created-state"), Path("/tmp/project"))

    def ready(self):
        result = self.server.handle(initialize())
        self.assertEqual(result["result"]["protocolVersion"], PROTOCOL_VERSION)
        self.assertIsNone(self.server.handle({"jsonrpc": "2.0", "method": "notifications/initialized"}))

    def test_handshake_tools_and_project_binding(self):
        self.assertIn("error", self.server.handle(request("tools/list")))
        self.ready()
        tools = self.server.handle(request("tools/list"))["result"]["tools"]
        self.assertEqual({tool["name"] for tool in tools}, {"history_status", "history_sessions", "history_read"})
        self.assertTrue(all(tool["annotations"]["readOnlyHint"] for tool in tools))
        for tool in tools:
            self.assertNotIn("project", tool["inputSchema"]["properties"])
        result = self.server.handle(request("tools/call", {"name": "history_sessions"}))["result"]
        self.assertEqual(json.loads(result["content"][0]["text"]), [{"session_key": "fixture"}])
        self.assertEqual(self.sessions.call_args.args[-1], Path("/tmp/project"))

    def test_unknown_and_malformed_arguments_never_reach_history(self):
        self.ready()
        with patch("delta_history.mcp.project_history") as history:
            for arguments in ({"session": "s", "project": "/other/repo"}, {"session": "s", "messages": "1-21"}, {"session": 4}, {"session": "s", "messages": []}, {}):
                result = self.server.handle(request("tools/call", {"name": "history_read", "arguments": arguments}))
                self.assertEqual(result["error"]["code"], -32602)
            history.assert_not_called()
        result = self.server.handle(request("tools/call", {"name": "exec", "arguments": {}}))
        self.assertEqual(result["error"]["code"], -32602)

    def test_history_read_uses_startup_project_and_redacts_errors(self):
        self.ready()
        with patch("delta_history.mcp.project_history", return_value=[{"role": "user", "text": "synthetic"}]) as history:
            result = self.server.handle(request("tools/call", {"name": "history_read", "arguments": {"session": "fixture", "messages": "1-2"}}))
            self.assertFalse(result["result"]["isError"])
            self.assertEqual(history.call_args.args[2:], (Path("/tmp/project"), "fixture", "1-2"))
        with patch("delta_history.mcp.project_history", side_effect=RuntimeError("secret-value")):
            result = self.server.handle(request("tools/call", {"name": "history_read", "arguments": {"session": "fixture"}}))
            self.assertTrue(result["result"]["isError"])
            self.assertNotIn("secret-value", json.dumps(result))

    def test_status_includes_adapter_only_holds_scoped_to_startup_checkout(self):
        self.ready()
        self.sessions.return_value = []
        with patch("delta_history.adapters.common.adapter_health", return_value={"pending": 0, "halted": 1, "other_project": "must-not-leak"}) as health:
            result = self.server.handle(request("tools/call", {"name": "history_status"}))["result"]
        self.assertFalse(result["isError"])
        value = json.loads(result["content"][0]["text"])
        self.assertEqual(value["health"]["phase"], "needs_review")
        self.assertEqual(value["sessions_count"], 0)
        self.assertEqual(value["adapters"], {"pending": 0, "halted": 1})
        self.assertNotIn("must-not-leak", json.dumps(value))
        health.assert_called_once_with(Path("/tmp/not-created-state"), project="/tmp/project")
        with patch("delta_history.adapters.common.adapter_health", return_value={"pending": 2, "halted": 0}):
            result = self.server.handle(request("tools/call", {"name": "history_status"}))["result"]
        self.assertEqual(json.loads(result["content"][0]["text"])["health"]["phase"], "catching_up")

    def test_stdio_malformed_and_oversized_responses_remain_jsonrpc(self):
        source = io.StringIO("not json\n" + json.dumps(initialize()) + '\n{"jsonrpc":"2.0","method":"notifications/initialized"}\n' + json.dumps(request("tools/call", {"name": "history_sessions"})) + "\n")
        target = io.StringIO()
        self.sessions.return_value = [{"text": "x" * MAX_OUTPUT}]
        self.assertEqual(serve({}, Path("/tmp/state"), Path("/tmp/project"), source, target), 0)
        results = [json.loads(line) for line in target.getvalue().splitlines()]
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["error"]["code"], -32700)
        self.assertEqual(results[-1]["error"]["code"], -32603)

    def test_real_stdio_process_handshake_and_tool_request_without_accounts(self):
        script = """import pathlib
from delta_history import mcp
mcp.project_sessions = lambda config, state, project: [{'session_key': 'synthetic-session'}]
raise SystemExit(mcp.serve({}, pathlib.Path('/tmp/unused-state'), pathlib.Path('/tmp/synthetic-project')))
"""
        messages = [initialize(), {"jsonrpc": "2.0", "method": "notifications/initialized"}, request("tools/list"), request("tools/call", {"name": "history_sessions"}, 3)]
        result = subprocess.run([sys.executable, "-c", script], input="\n".join(json.dumps(item) for item in messages) + "\n", text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        output = [json.loads(line) for line in result.stdout.splitlines()]
        self.assertEqual([item["id"] for item in output], [1, 2, 3])
        self.assertEqual(json.loads(output[-1]["result"]["content"][0]["text"])[0]["session_key"], "synthetic-session")


if __name__ == "__main__":
    unittest.main()
