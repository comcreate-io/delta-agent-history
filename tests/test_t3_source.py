"""Synthetic HTTP fixtures only; these tests never contact a T3 environment."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from t3_delta.t3_source import T3Client, T3Error, T3HTTPError


def rpc_result(request, payload):
    return {"jsonrpc": "2.0", "id": request["id"], "result": payload}


def structured(payload):
    return {"structuredContent": payload, "content": [], "isError": False}


@contextmanager
def fake_mcp(tool_handler, *, sse=False, rpc_handler=None):
    requests = []

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def do_POST(self):
            request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            requests.append((request, dict(self.headers)))
            if request["method"] == "initialize":
                result = rpc_result(request, {"protocolVersion": "2025-06-18", "capabilities": {}, "serverInfo": {"name": "fixture", "version": "1"}})
            elif request["method"] == "notifications/initialized":
                self.send_response(202)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            else:
                result = rpc_result(request, tool_handler(request["params"]["name"], request["params"]["arguments"]))
            if rpc_handler:
                replacement = rpc_handler(request, result)
                if isinstance(replacement, int):
                    self.send_response(replacement)
                    self.send_header("Content-Length", "0")
                    self.send_header("Location", "/must-not-follow")
                    self.end_headers()
                    return
                result = replacement
            if sse:
                response = ": keepalive\r\n\r\nevent: message\r\ndata: " + json.dumps({"jsonrpc": "2.0", "method": "notifications/progress", "params": {}}) + "\r\n\r\nevent: message\r\n"
                response += "".join("data: " + line + "\r\n" for line in json.dumps(result, indent=2).splitlines()) + "\r\n"
                body = response.encode("utf-8")
            else:
                body = json.dumps(result).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream" if sse else "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Mcp-Session-Id", "fixture-session")
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/mcp", requests
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)


def item(identity="message-1", position=0, text="hello", **extra):
    return {"itemId": identity, "sourceThreadId": "thread-1", "position": position,
            "type": "user_message", "text": text, "textTruncated": False,
            "nextTextOffset": None, "updatedAt": "2026-10-23T00:00:00Z",
            "status": "completed", "messageId": identity, **extra}


def timeline(items, *, has_more=False, **extra):
    return {"thread": {"threadId": "thread-1", "updatedAt": "2026-10-23T00:00:00Z"},
            "items": items, "recentRuns": [], "hasMore": has_more,
            "nextPosition": items[-1]["position"] if items else None, **extra}


class T3SourceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.token_file = Path(self.temporary.name) / "t3-session.json"
        self.credential = {
            "token": "synthetic-token.not-a-real-credential", "sessionId": "fixture-session-id",
            "scopes": ["orchestration:read"], "subject": "mcp-client",
            "expiresAt": (datetime.now(timezone.utc) + timedelta(days=1)).isoformat(),
        }
        self.write_credential()

    def write_credential(self):
        self.token_file.write_text(json.dumps(self.credential))
        self.token_file.chmod(0o600)

    def test_initialization_headers_and_structured_payload(self):
        with fake_mcp(lambda name, args: structured({"id": "environment-1"})) as (endpoint, calls):
            client = T3Client(endpoint, self.token_file)
            self.assertEqual(client.environment(), {"id": "environment-1"})
            self.assertEqual(client.environment(), {"id": "environment-1"})
            self.assertEqual([call[0]["method"] for call in calls], ["initialize", "notifications/initialized", "tools/call", "tools/call"])
            for request, headers in calls:
                self.assertEqual(headers["Authorization"], "Bearer " + self.credential["token"])
                if request["method"] != "initialize":
                    self.assertEqual(headers["Mcp-Session-Id"], "fixture-session")
                    self.assertEqual(headers["MCP-Protocol-Version"], "2025-06-18")

    def test_sse_notifications_and_multiline_response_and_text_wrapper(self):
        with fake_mcp(lambda *_: {"content": [{"type": "text", "text": json.dumps({"id": "environment-1"})}]}, sse=True) as (endpoint, _):
            self.assertEqual(T3Client(endpoint, self.token_file).environment(), {"id": "environment-1"})

    def test_project_and_thread_paging_includes_subagents_and_deduplicates(self):
        def handler(name, args):
            self.assertEqual(args["limit"], 100)
            cursor = args.get("cursor", 0)
            if name == "t3_project_list":
                return structured({"projects": [{"id": "p1"}] if not cursor else [{"id": "p1"}, {"id": "p2"}], "nextCursor": 1 if not cursor else None})
            self.assertEqual(name, "t3_thread_list")
            self.assertTrue(args["includeSubagents"])
            self.assertEqual(args["projectId"], "p1")
            return structured({"threads": [{"threadId": "t1" if not cursor else "child-1"}], "nextCursor": 1 if not cursor else None})
        with fake_mcp(handler) as (endpoint, _):
            client = T3Client(endpoint, self.token_file)
            self.assertEqual([p["id"] for p in client.projects()], ["p1", "p2"])
            self.assertEqual([p["threadId"] for p in client.threads("p1")], ["t1", "child-1"])

    def test_read_messages_only_all_pages_and_server_utf16_chunks(self):
        def handler(name, args):
            self.assertEqual(name, "t3_thread_read")
            self.assertEqual(args["view"], "messages")
            self.assertEqual(args["runLimit"], 50)
            if "itemId" in args:
                self.assertEqual(args["textOffset"], 2)  # A + lone high surrogate, not a Python character offset.
                return structured(timeline([item(position=2, text="\ude00B")]))
            if "afterPosition" in args:
                self.assertEqual(args["afterPosition"], 2)
                return structured(timeline([item("plan-1", position=8, text="the plan", type="proposed_plan")]))
            return structured(timeline([item(position=2, text="A\ud83d", textTruncated=True, nextTextOffset=2)], has_more=True))
        with fake_mcp(handler) as (endpoint, _):
            result = T3Client(endpoint, self.token_file).read_thread("thread-1")
            self.assertEqual(len(result["items"]), 2)
            self.assertEqual(result["items"][0]["text"], "A😀B")
            self.assertFalse(result["items"][0]["textTruncated"])
            self.assertIsNone(result["items"][0]["nextTextOffset"])

    def test_mutation_denied_before_any_network_or_token_read(self):
        client = T3Client("http://127.0.0.1:1/mcp", Path("/does-not-exist"))
        for name in ["t3_thread_send", "t3_thread_launch", "t3_project_delete", "tools/call"]:
            with self.subTest(name=name), self.assertRaisesRegex(T3Error, "allowlist"):
                client.call_tool(name, {})

    def test_dedicated_token_security_scope_subject_expiry_and_symlinks(self):
        client = T3Client("http://127.0.0.1:1/mcp", self.token_file)
        self.token_file.chmod(0o644)
        with self.assertRaisesRegex(T3Error, "0600"):
            client.environment()
        for change in [{"scopes": ["orchestration:read", "orchestration:operate"]}, {"subject": "cli-issued-session"}, {"expiresAt": "2000-01-01T00:00:00Z"}, {"expiresAt": "not-a-date"}, {"token": "Bearer\ninvalid"}]:
            with self.subTest(change=change):
                previous = dict(self.credential)
                self.credential.update(change)
                self.write_credential()
                with self.assertRaises(T3Error) as caught:
                    client.environment()
                self.assertNotIn(self.credential["token"], str(caught.exception))
                self.credential = previous
        self.write_credential()
        symlink = self.token_file.with_name("symlink.json")
        symlink.symlink_to(self.token_file)
        with self.assertRaises(T3Error):
            T3Client("http://127.0.0.1:1/mcp", symlink).environment()

    def test_errors_never_echo_server_body_or_credential(self):
        marker = "synthetic-private-output-must-not-escape"
        scenarios = [
            lambda request, result: {"jsonrpc": "2.0", "id": request["id"], "error": {"message": marker}},
            lambda request, result: {**result, "id": request["id"] + 100},
        ]
        for scenario in scenarios:
            with self.subTest(scenario=scenario), fake_mcp(lambda *_: structured({}), rpc_handler=scenario) as (endpoint, _):
                with self.assertRaises(T3Error) as caught:
                    T3Client(endpoint, self.token_file).environment()
                self.assertNotIn(marker, str(caught.exception))
        for payload in [{"isError": True, "content": [{"type": "text", "text": marker}]}, {"structuredContent": [marker]}, {"content": [{"type": "text", "text": marker}]}]:
            with self.subTest(payload=payload), fake_mcp(lambda *_: payload) as (endpoint, _):
                with self.assertRaises(T3Error) as caught:
                    T3Client(endpoint, self.token_file).environment()
                self.assertNotIn(marker, str(caught.exception))

    def test_expired_mcp_session_reinitializes_once_and_redirects_do_not_follow(self):
        attempts = 0
        def expired(request, result):
            nonlocal attempts
            if request["method"] == "tools/call":
                attempts += 1
                if attempts == 1:
                    return 404
            return result
        with fake_mcp(lambda *_: structured({"id": "e1"}), rpc_handler=expired) as (endpoint, calls):
            self.assertEqual(T3Client(endpoint, self.token_file).environment(), {"id": "e1"})
            self.assertEqual(sum(c[0]["method"] == "initialize" for c in calls), 2)
        with fake_mcp(lambda *_: structured({}), rpc_handler=lambda *_: 302) as (endpoint, calls):
            with self.assertRaises(T3HTTPError) as caught:
                T3Client(endpoint, self.token_file).environment()
            self.assertEqual(caught.exception.status, 302)
            self.assertEqual(len(calls), 1)

    def test_byte_and_pagination_budgets_fail_instead_of_clipping(self):
        with fake_mcp(lambda *_: structured({"id": "e1", "data": "x" * 5000})) as (endpoint, _):
            client = T3Client(endpoint, self.token_file)
            client.MAX_RESPONSE_BYTES = 1000
            with self.assertRaisesRegex(T3Error, "byte budget"):
                client.environment()
        with fake_mcp(lambda *_: structured({"projects": [{"id": "p1"}], "nextCursor": 0})) as (endpoint, _):
            with self.assertRaisesRegex(T3Error, "cursor"):
                T3Client(endpoint, self.token_file).projects()
        with fake_mcp(lambda *_: structured(timeline([item(text="x" * 100)]))) as (endpoint, _):
            client = T3Client(endpoint, self.token_file)
            client.MAX_ITEM_BYTES = 50
            with self.assertRaisesRegex(T3Error, "byte budget"):
                client.read_thread("thread-1")
        with fake_mcp(lambda *_: structured({"projects": [{"id": "p1"}, {"id": "p2"}], "nextCursor": None})) as (endpoint, _):
            client = T3Client(endpoint, self.token_file)
            client.MAX_LIST_ITEMS = 1
            with self.assertRaisesRegex(T3Error, "item budget"):
                client.projects()

    def test_owner_and_page_chunk_request_budgets(self):
        info = list(self.token_file.stat())
        info[4] = os.getuid() + 1
        with patch("t3_delta.t3_source.os.fstat", return_value=os.stat_result(info)):
            with self.assertRaisesRegex(T3Error, "owned by"):
                T3Client("http://127.0.0.1:1/mcp", self.token_file).environment()
        with fake_mcp(lambda _name, args: structured({"projects": [], "nextCursor": args.get("cursor", 0) + 1})) as (endpoint, _):
            client = T3Client(endpoint, self.token_file)
            client.MAX_LIST_PAGES = 1
            with self.assertRaisesRegex(T3Error, "page budget"):
                client.projects()
        for field, error in [("MAX_ITEM_CHUNKS", "chunk budget"), ("MAX_TIMELINE_REQUESTS", "request budget")]:
            with self.subTest(field=field), fake_mcp(lambda *_: structured(timeline([item(text="A", textTruncated=True, nextTextOffset=1)]))) as (endpoint, _):
                client = T3Client(endpoint, self.token_file)
                setattr(client, field, 1)
                with self.assertRaisesRegex(T3Error, error):
                    client.read_thread("thread-1")

    def test_metadata_changes_between_pages_are_not_silently_accepted(self):
        def handler(_name, args):
            if "afterPosition" in args:
                return structured(timeline([item("message-2", position=1)], thread={"threadId": "thread-1", "updatedAt": "2026-10-23T00:00:01Z"}))
            return structured(timeline([item()], has_more=True))
        with fake_mcp(handler) as (endpoint, _):
            with self.assertRaisesRegex(T3Error, "changed during pagination"):
                T3Client(endpoint, self.token_file).read_thread("thread-1")

    def test_bad_or_changing_timeline_is_rejected(self):
        for broken in [timeline([item()], has_more=True, nextPosition=None), timeline([item(textTruncated=True, nextTextOffset=0)]), timeline([item(textTruncated=False, nextTextOffset=9)]), timeline([item(), item(position=0)])]:
            with self.subTest(broken=broken), fake_mcp(lambda *_: structured(broken)) as (endpoint, _):
                with self.assertRaises(T3Error):
                    T3Client(endpoint, self.token_file).read_thread("thread-1")
        def changed(_name, args):
            if "itemId" in args:
                return structured(timeline([item(text="B", updatedAt="2026-10-23T00:00:01Z")]))
            return structured(timeline([item(text="A", textTruncated=True, nextTextOffset=1)]))
        with fake_mcp(changed) as (endpoint, _):
            with self.assertRaisesRegex(T3Error, "changed during chunk"):
                T3Client(endpoint, self.token_file).read_thread("thread-1")

    def test_endpoint_rejects_credential_leak_and_remote_cleartext(self):
        for endpoint in ["http://example.com/mcp", "https://user:password@example.com/mcp", "https://example.com/mcp?token=no", "https://example.com/elsewhere", "file:///tmp/mcp"]:
            with self.subTest(endpoint=endpoint), self.assertRaises(T3Error):
                T3Client(endpoint, self.token_file)

    def test_transport_timeout_is_bounded_and_redacted(self):
        def delayed(*_):
            time.sleep(0.15)
            return structured({"id": "e1"})
        with fake_mcp(delayed) as (endpoint, _):
            started = time.monotonic()
            with self.assertRaisesRegex(T3Error, "timed out"):
                T3Client(endpoint, self.token_file, timeout=0.03).environment()
            self.assertLess(time.monotonic() - started, 1)


if __name__ == "__main__":
    unittest.main()
