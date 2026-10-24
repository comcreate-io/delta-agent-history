"""Completed visible OpenCode message snapshots supplied by the plugin."""
from .common import AdapterError, Journal, read_payload, required_id, visible_text


def opencode_hook(config, state, instance, stream):
    payload = read_payload(stream)
    if set(payload) != {"session_id", "project", "messages"} or not isinstance(payload["messages"], list) or len(payload["messages"]) > 2000:
        raise AdapterError("Expected a bounded OpenCode completed-message snapshot")
    session = required_id(payload["session_id"])
    messages, seen = [], set()
    for message in payload["messages"]:
        if not isinstance(message, dict) or set(message) - {"id", "role", "text", "model"} or not {"id", "role", "text"} <= set(message):
            raise AdapterError("Invalid completed OpenCode message")
        key = required_id(message["id"])
        if key in seen or message["role"] not in ("user", "assistant"):
            raise AdapterError("Duplicate or nonvisible OpenCode message identity")
        seen.add(key)
        fields = {"role":message["role"], "text":visible_text(message["text"])}
        if "model" in message:
            fields["model"] = required_id(message["model"])
        messages.append((key,fields))
    with Journal(config, state, "opencode", instance, session, payload["project"]) as journal:
        try:
            with journal.transaction():
                old = journal.metadata().get("message_ids", [])
                ids = [key for key,_ in messages]
                if ids[:len(old)] != old:
                    raise AdapterError("Completed OpenCode prefix changed or disappeared")
                journal.open()
                for key,fields in messages:
                    journal.add("message:"+key, "message.completed", **fields)
                journal.set_metadata({"message_ids":ids})
        except AdapterError:
            journal.halt()
            raise
        return journal.flush()
