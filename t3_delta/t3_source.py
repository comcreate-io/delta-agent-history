"""Read-only, bounded access to T3's public Streamable HTTP MCP surface.

The client deliberately reads messages/plans only, never activity/tool/reasoning
items. Timeline positions are mutable offsets, not event cursors. Callers must
reconcile each successful snapshot; polling cannot attach before every mutation.
"""

from __future__ import annotations

import http.client
from datetime import datetime, timezone
import ipaddress
import json
import math
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit


class T3Error(RuntimeError):
    """An intentionally redacted transport, protocol, or budget failure."""


class T3HTTPError(T3Error):
    def __init__(self, status: int):
        self.status = status
        super().__init__(f"T3 MCP HTTP request failed (status {status}).")


def _integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _decode_json(raw: str | bytes) -> dict:
    try:
        value = json.loads(raw)
    except (ValueError, UnicodeError, RecursionError):
        raise T3Error("T3 MCP returned invalid JSON.") from None
    if not isinstance(value, dict):
        raise T3Error("T3 MCP returned a non-object JSON payload.")
    return value


def _text_bytes(text: str) -> int:
    return len(text.encode("utf-8", errors="surrogatepass"))


def _utf16_units(text: str) -> int:
    return len(text.encode("utf-16-le", errors="surrogatepass")) // 2


class T3Client:
    ALLOWED_TOOLS = frozenset({
        "t3_environment_read", "t3_project_list", "t3_project_read",
        "t3_thread_list", "t3_thread_read",
    })
    PROTOCOL_VERSION = "2025-06-18"
    MAX_RESPONSE_BYTES = 8 * 1024 * 1024
    MAX_REQUEST_BYTES = 64 * 1024
    MAX_TOKEN_BYTES = 16 * 1024
    MAX_SSE_EVENTS = 256
    MAX_LIST_PAGES = 200
    MAX_LIST_ITEMS = 10_000
    MAX_TIMELINE_PAGES = 200
    MAX_TIMELINE_ITEMS = 10_000
    MAX_TIMELINE_BYTES = 32 * 1024 * 1024
    MAX_ITEM_BYTES = 4 * 1024 * 1024
    MAX_ITEM_CHUNKS = 128
    MAX_TIMELINE_REQUESTS = 1_000

    def __init__(self, endpoint: str, token_file: Path, timeout: float = 20):
        try:
            parsed = urlsplit(endpoint)
            port = parsed.port
            host = parsed.hostname
        except ValueError:
            raise T3Error("Invalid T3 MCP endpoint.") from None
        if (parsed.scheme not in {"http", "https"} or not host
                or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path.rstrip("/") != "/mcp"
                or any(ord(c) < 33 for c in endpoint)):
            raise T3Error("T3 endpoint must be an HTTP(S) /mcp URL without credentials or query.")
        if parsed.scheme == "http":
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = host.lower() == "localhost"
            if not loopback:
                raise T3Error("Non-local T3 endpoints require HTTPS.")
        if not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise T3Error("T3 timeout must be finite and positive.")
        self.endpoint = endpoint
        self.token_file = Path(token_file)
        self.timeout = float(timeout)
        self._host, self._port = host, port
        self._path = parsed.path
        self._https = parsed.scheme == "https"
        self._session_id: str | None = None
        self._protocol: str | None = None
        self._initialized = False
        self._request_id = 0

    def _token(self) -> str:
        fd = None
        try:
            flags = os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0)
            fd = os.open(self.token_file, flags)
            info = os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) != 0o600):
                raise T3Error("T3 token must be a regular 0600 file owned by the current user.")
            raw = os.read(fd, self.MAX_TOKEN_BYTES + 1)
            if len(raw) > self.MAX_TOKEN_BYTES:
                raise T3Error("T3 token file exceeds its byte budget.")
            credential = _decode_json(raw)
            if credential.get("scopes") != ["orchestration:read"] or credential.get("subject") != "mcp-client":
                raise T3Error("T3 credential must be an mcp-client session with only orchestration:read.")
            expiry = credential.get("expiresAt")
            if expiry is not None:
                try:
                    expires = datetime.fromisoformat(expiry.replace("Z", "+00:00"))
                    if expires.tzinfo is None or expires <= datetime.now(timezone.utc):
                        raise ValueError
                except (TypeError, ValueError, AttributeError):
                    raise T3Error("T3 credential expiry is invalid or has elapsed.") from None
            token = credential.get("token")
            if not isinstance(token, str):
                raise T3Error("T3 credential file does not contain a bearer token.")
            if not token or not re.fullmatch(r"[A-Za-z0-9._~+/=-]+", token):
                raise T3Error("T3 token file is empty or malformed.")
            return token
        except (OSError, UnicodeError):
            raise T3Error("Cannot securely read the dedicated T3 token file.") from None
        finally:
            if fd is not None:
                os.close(fd)

    def _exchange(self, method: str, params: dict | None = None, *, notification: bool = False) -> dict:
        request: dict = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            request["params"] = params
        if not notification:
            self._request_id += 1
            request["id"] = self._request_id
        try:
            body = json.dumps(request, ensure_ascii=True, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, RecursionError):
            raise T3Error("T3 MCP request arguments are not valid JSON.") from None
        if len(body) > self.MAX_REQUEST_BYTES:
            raise T3Error("T3 MCP request exceeds its byte budget.")
        headers = {
            "Authorization": "Bearer " + self._token(),
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._protocol:
            headers["MCP-Protocol-Version"] = self._protocol
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        cls = http.client.HTTPSConnection if self._https else http.client.HTTPConnection
        connection = cls(self._host, self._port, timeout=self.timeout)
        deadline = time.monotonic() + self.timeout
        try:
            connection.request("POST", self._path, body=body, headers=headers)
            transport_socket = connection.sock
            response = connection.getresponse()
            if response.status not in {200, 202, 204}:
                raise T3HTTPError(response.status)
            session = response.getheader("Mcp-Session-Id")
            if session is not None:
                if not session or len(session) > 512 or any(ord(c) < 33 or ord(c) > 126 for c in session):
                    raise T3Error("T3 MCP returned an invalid session header.")
                self._session_id = session
            if notification and response.status in {202, 204}:
                return {}
            if not notification and response.status != 200:
                raise T3Error("T3 MCP request returned no JSON-RPC response.")
            content_type = (response.getheader("Content-Type") or "").split(";", 1)[0].strip().lower()
            if content_type not in {"application/json", "text/event-stream"} and not notification:
                raise T3Error("T3 MCP returned an unsupported content type.")
            total = 0

            def chunks():
                nonlocal total
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise T3Error("T3 MCP response exceeded its time budget.")
                    if transport_socket is not None:
                        transport_socket.settimeout(remaining)
                    part = response.read1(64 * 1024)
                    if not part:
                        return
                    total += len(part)
                    if total > self.MAX_RESPONSE_BYTES:
                        raise T3Error("T3 MCP response exceeds its byte budget.")
                    yield part

            if notification:
                if b"".join(chunks()).strip():
                    raise T3Error("T3 MCP notification returned an unexpected body.")
                return {}
            if content_type == "application/json":
                return self._rpc_result(_decode_json(b"".join(chunks())), request["id"])
            return self._sse_result(chunks(), request["id"])
        except (OSError, http.client.HTTPException, ValueError):
            raise T3Error("T3 MCP transport failed or timed out.") from None
        finally:
            connection.close()

    @staticmethod
    def _rpc_result(message: dict, request_id: int) -> dict:
        if (message.get("jsonrpc") != "2.0" or type(message.get("id")) is not int
                or message["id"] != request_id):
            raise T3Error("T3 MCP returned a mismatched JSON-RPC response.")
        if "error" in message:
            raise T3Error("T3 MCP returned a JSON-RPC error.")
        result = message.get("result")
        if not isinstance(result, dict):
            raise T3Error("T3 MCP returned an invalid JSON-RPC result.")
        return result

    def _sse_result(self, chunks, request_id: int) -> dict:
        pending = b""
        data: list[bytes] = []
        events = 0

        def dispatch():
            nonlocal events, data
            if not data:
                return None
            events += 1
            if events > self.MAX_SSE_EVENTS:
                raise T3Error("T3 MCP SSE stream exceeds its event budget.")
            message = _decode_json(b"\n".join(data))
            data = []
            if "id" not in message and isinstance(message.get("method"), str):
                return None
            return self._rpc_result(message, request_id)

        for part in chunks:
            pending += part
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                line = line.rstrip(b"\r")
                if not line:
                    result = dispatch()
                    if result is not None:
                        return result
                elif line.startswith(b"data:"):
                    value = line[5:]
                    data.append(value[1:] if value.startswith(b" ") else value)
        if pending.startswith(b"data:"):
            data.append(pending[5:].lstrip(b" ").rstrip(b"\r"))
        result = dispatch()
        if result is not None:
            return result
        raise T3Error("T3 MCP SSE stream ended before its JSON-RPC response.")

    def _initialize(self) -> None:
        self._session_id = None
        self._protocol = None
        self._initialized = False
        result = self._exchange("initialize", {
            "protocolVersion": self.PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": {"name": "t3-delta-recorder", "version": "0.1.0"},
        })
        if result.get("protocolVersion") != self.PROTOCOL_VERSION:
            raise T3Error("T3 MCP negotiated an unsupported protocol version.")
        self._protocol = self.PROTOCOL_VERSION
        self._exchange("notifications/initialized", notification=True)
        self._initialized = True

    def call_tool(self, name: str, args: dict) -> dict:
        if name not in self.ALLOWED_TOOLS:
            raise T3Error("T3 MCP tool is outside the recorder's read-only allowlist.")
        if not isinstance(args, dict):
            raise T3Error("T3 MCP tool arguments must be an object.")
        if not self._initialized:
            self._initialize()
        for attempt in range(2):
            try:
                result = self._exchange("tools/call", {"name": name, "arguments": args})
                break
            except T3HTTPError as error:
                if error.status != 404 or attempt:
                    raise
                self._initialize()
        if result.get("isError", False) is not False:
            raise T3Error("T3 MCP tool reported an error.")
        if "structuredContent" in result:
            if not isinstance(result["structuredContent"], dict):
                raise T3Error("T3 MCP tool returned invalid structured content.")
            return result["structuredContent"]
        content = result.get("content")
        if not isinstance(content, list) or not content:
            raise T3Error("T3 MCP tool returned no structured or text content.")
        if any(not isinstance(block, dict) or block.get("type") != "text"
               or not isinstance(block.get("text"), str) for block in content):
            raise T3Error("T3 MCP tool returned an unsupported content wrapper.")
        return _decode_json("\n".join(block["text"] for block in content))

    def environment(self) -> dict:
        return self.call_tool("t3_environment_read", {})

    def _list(self, name: str, key: str, identity: str, args: dict) -> list[dict]:
        cursor = None
        result: list[dict] = []
        seen: set[str] = set()
        for _ in range(self.MAX_LIST_PAGES):
            page = self.call_tool(name, {**args, "limit": 100, **({"cursor": cursor} if cursor is not None else {})})
            rows = page.get(key)
            if not isinstance(rows, list):
                raise T3Error("T3 MCP list response has invalid rows.")
            for row in rows:
                if not isinstance(row, dict) or not isinstance(row.get(identity), str) or not row[identity]:
                    raise T3Error("T3 MCP list response has an invalid identity.")
                if row[identity] not in seen:
                    seen.add(row[identity])
                    result.append(row)
                    if len(result) > self.MAX_LIST_ITEMS:
                        raise T3Error("T3 MCP list exceeds its item budget.")
            if "nextCursor" not in page:
                raise T3Error("T3 MCP list response omits its pagination cursor.")
            next_cursor = page["nextCursor"]
            if next_cursor is None:
                return result
            if not _integer(next_cursor) or next_cursor <= (cursor if cursor is not None else -1):
                raise T3Error("T3 MCP list cursor did not advance.")
            cursor = next_cursor
        raise T3Error("T3 MCP list exceeds its page budget.")

    def projects(self) -> list[dict]:
        return self._list("t3_project_list", "projects", "id", {})

    def threads(self, project_id: str) -> list[dict]:
        return self._list("t3_thread_list", "threads", "threadId", {
            "projectId": project_id, "includeSubagents": True,
        })

    def read_thread(self, thread_id: str) -> dict:
        """Return a bounded complete messages/plans timeline or fail explicitly.

        This is not an atomic event export. In-place changes during pagination
        or chunk reads cause an error so the caller can retry the whole poll.
        """
        calls = 0
        def read(extra: dict) -> dict:
            nonlocal calls
            calls += 1
            if calls > self.MAX_TIMELINE_REQUESTS:
                raise T3Error("T3 timeline exceeds its request budget.")
            return self.call_tool("t3_thread_read", {
                "threadId": thread_id, "view": "messages", "limit": 100,
                "maxCharsPerItem": 50_000, "runLimit": 50, **extra,
            })

        cursor = None
        items: list[dict] = []
        seen: set[tuple[str, str]] = set()
        thread = None
        recent_runs = None
        total_bytes = 0
        for _ in range(self.MAX_TIMELINE_PAGES):
            page = read({"afterPosition": cursor} if cursor is not None else {})
            metadata, rows, runs = page.get("thread"), page.get("items"), page.get("recentRuns")
            if (not isinstance(metadata, dict) or metadata.get("threadId") != thread_id
                    or not isinstance(rows, list) or not isinstance(runs, list)
                    or any(not isinstance(run, dict) for run in runs)):
                raise T3Error("T3 timeline response has invalid metadata or rows.")
            if thread is None:
                thread, recent_runs = metadata, runs
            elif metadata != thread or runs != recent_runs:
                raise T3Error("T3 timeline changed during pagination; retry the poll.")
            last_position = cursor if cursor is not None else -1
            for original in rows:
                if (not isinstance(original, dict) or not isinstance(original.get("itemId"), str)
                        or not original["itemId"]
                        or not isinstance(original.get("sourceThreadId"), str)
                        or not original["sourceThreadId"]
                        or not isinstance(original.get("updatedAt"), str) or not original["updatedAt"]
                        or not _integer(original.get("position")) or original["position"] <= last_position
                        or not isinstance(original.get("textTruncated"), bool)
                        or "text" not in original
                        or original.get("text") is not None and not isinstance(original.get("text"), str)):
                    raise T3Error("T3 timeline contains an invalid item.")
                identity = (original["sourceThreadId"], original["itemId"])
                if identity in seen:
                    raise T3Error("T3 timeline identities changed during pagination; retry the poll.")
                seen.add(identity)
                last_position = original["position"]
                item = dict(original)
                text_parts = [item["text"]] if item.get("text") is not None else []
                item_bytes = sum(_text_bytes(text) for text in text_parts)
                offset = 0
                chunk_count = 1
                while item["textTruncated"]:
                    next_offset = item.get("nextTextOffset")
                    if (not isinstance(item.get("text"), str) or not _integer(next_offset)
                            or next_offset <= offset or next_offset != offset + _utf16_units(item["text"])):
                        raise T3Error("T3 timeline text cursor is invalid or did not advance.")
                    if chunk_count >= self.MAX_ITEM_CHUNKS:
                        raise T3Error("T3 timeline item exceeds its chunk budget.")
                    if item_bytes > self.MAX_ITEM_BYTES:
                        raise T3Error("T3 timeline item exceeds its byte budget.")
                    offset = next_offset
                    continuation = read({"itemId": original["itemId"], "textOffset": offset, "limit": 1})
                    parts = continuation.get("items")
                    if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict):
                        raise T3Error("T3 timeline item disappeared during chunk reads; retry the poll.")
                    item = parts[0]
                    stable_fields = ("itemId", "sourceThreadId", "position", "updatedAt", "status", "type", "messageId")
                    if any(item.get(key) != original.get(key) for key in stable_fields):
                        raise T3Error("T3 timeline item changed during chunk reads; retry the poll.")
                    if not isinstance(item.get("text"), str) or not isinstance(item.get("textTruncated"), bool):
                        raise T3Error("T3 timeline returned an invalid text chunk.")
                    text_parts.append(item["text"])
                    item_bytes += _text_bytes(item["text"])
                    chunk_count += 1
                if item.get("nextTextOffset") is not None:
                    raise T3Error("T3 timeline has an inconsistent terminal text cursor.")
                if item_bytes > self.MAX_ITEM_BYTES:
                    raise T3Error("T3 timeline item exceeds its byte budget.")
                final = dict(original)
                if text_parts:
                    try:
                        final["text"] = "".join(text_parts).encode("utf-16-le", errors="surrogatepass").decode("utf-16-le")
                    except UnicodeError:
                        raise T3Error("T3 timeline text contains invalid UTF-16 chunks.") from None
                final.update(textTruncated=False, nextTextOffset=None)
                items.append(final)
                total_bytes += len(json.dumps(final, ensure_ascii=True).encode("utf-8"))
                if len(items) > self.MAX_TIMELINE_ITEMS or total_bytes > self.MAX_TIMELINE_BYTES:
                    raise T3Error("T3 timeline exceeds its item or byte budget.")
            if not isinstance(page.get("hasMore"), bool) or "nextPosition" not in page:
                raise T3Error("T3 timeline response has invalid pagination metadata.")
            if not page["hasMore"]:
                return {"thread": thread, "items": items, "recentRuns": recent_runs}
            next_cursor = page["nextPosition"]
            if not rows or not _integer(next_cursor) or next_cursor != last_position:
                raise T3Error("T3 timeline cursor did not advance.")
            cursor = next_cursor
        raise T3Error("T3 timeline exceeds its page budget.")
