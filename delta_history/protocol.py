"""Strict, bounded visible-message ingress. This protocol never contains tools."""
import datetime as dt
import json
import re
from pathlib import Path

MAX_LINE_BYTES = 1024 * 1024
MAX_TEXT_BYTES = 256 * 1024
MAX_BATCH_BYTES = 4 * 1024 * 1024
MAX_RECORDS = 100
SLUG = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z")
COMMON = {"schema_version", "type", "source", "source_instance", "session_id", "event_id", "sequence", "created_at"}


def utc_timestamp(value):
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("created_at must be an explicit UTC timestamp")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("created_at must be an explicit UTC timestamp") from None
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError("created_at must be an explicit UTC timestamp")
    return parsed.timestamp()


def identifier(value):
    return isinstance(value, str) and 0 < len(value.encode("utf-8")) <= 256 and not any(ord(c) < 32 or ord(c) == 127 for c in value)


def encode(event):
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def validate_event(event):
    if not isinstance(event, dict) or type(event.get("schema_version")) is not int or event["schema_version"] != 1:
        raise ValueError("Unsupported ingress schema")
    kind = event.get("type")
    extras = {"session.open": {"project"}, "message.completed": {"role", "text", "model"}, "session.close": {"status"}}
    required = {"session.open": {"project"}, "message.completed": {"role", "text"}, "session.close": {"status"}}
    if not isinstance(kind, str) or kind not in extras or set(event) - COMMON - extras[kind] or not COMMON | required[kind] <= set(event):
        raise ValueError("Unknown or incomplete ingress event fields")
    if not all(isinstance(event[k], str) and SLUG.fullmatch(event[k]) for k in ("source", "source_instance")):
        raise ValueError("Source kind and instance must be lowercase slugs")
    if not all(identifier(event[k]) for k in ("session_id", "event_id")):
        raise ValueError("Session and event IDs must be bounded nonempty strings")
    if type(event["sequence"]) is not int or not 1 <= event["sequence"] <= 2 ** 63 - 1:
        raise ValueError("sequence must be a positive integer")
    utc_timestamp(event["created_at"])
    if kind == "session.open":
        if event["sequence"] != 1 or not isinstance(event["project"], str) or not Path(event["project"]).is_absolute() or len(event["project"].encode()) > 4096:
            raise ValueError("session.open requires sequence 1 and an absolute checkout path")
    elif kind == "message.completed":
        if event["role"] not in ("user", "assistant", "plan") or not isinstance(event["text"], str) or not event["text"] or len(event["text"].encode("utf-8")) > MAX_TEXT_BYTES:
            raise ValueError("Messages require a visible role and bounded nonempty text")
        if "model" in event and (not identifier(event["model"])):
            raise ValueError("Model label must be a bounded nonempty string")
    elif event["status"] not in ("completed", "failed", "cancelled"):
        raise ValueError("Invalid session completion status")
    if len(encode(event).encode()) > MAX_LINE_BYTES:
        raise ValueError("Ingress record exceeds line budget")
    return dict(event)


def validate_batch(events):
    result, size = [], 0
    for event in events:
        if len(result) >= MAX_RECORDS:
            raise ValueError("Ingress batch exceeds record budget")
        item = validate_event(event)
        size += len(encode(item).encode()) + 1
        if size > MAX_BATCH_BYTES:
            raise ValueError("Ingress batch exceeds byte budget")
        result.append(item)
    return result


def parse_jsonl(lines):
    result, size = [], 0
    for line in lines:
        raw = line.encode("utf-8") if isinstance(line, str) else line
        if not isinstance(raw, bytes) or len(raw) > MAX_LINE_BYTES:
            raise ValueError("Ingress line exceeds byte budget")
        size += len(raw)
        if size > MAX_BATCH_BYTES or len(result) >= MAX_RECORDS:
            raise ValueError("Ingress batch exceeds budget")
        try:
            def unique(pairs):
                value = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError("Duplicate JSON field")
                    value[key] = item
                return value
            event = json.loads(raw, object_pairs_hook=unique)
        except (ValueError, UnicodeError):
            raise ValueError("Invalid JSONL ingress record") from None
        result.append(validate_event(event))
    return result
