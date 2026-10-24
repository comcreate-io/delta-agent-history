"""Claude Code UserPromptSubmit/Stop hooks; never read transcript_path."""
from .common import AdapterError, Journal, read_payload, required_id, visible_text


def claude_hook(config, state, instance, stream):
    payload = read_payload(stream)
    kind = payload.get("hook_event_name")
    if kind not in ("UserPromptSubmit", "Stop") or payload.get("agent_id"):
        return {"accepted":False, "ignored":True}
    session = required_id(payload.get("session_id"))
    prompt = required_id(payload.get("prompt_id"))
    text = visible_text(payload.get("prompt" if kind == "UserPromptSubmit" else "last_assistant_message"))
    project = payload.get("cwd")
    if not isinstance(project, str):
        raise AdapterError("Claude hook requires its explicit cwd")
    with Journal(config, state, "claude-code", instance, session, project) as journal:
        try:
            with journal.transaction():
                metadata = journal.metadata()
                key = ("prompt:" if kind == "UserPromptSubmit" else "stop:") + prompt
                duplicate = journal.has(key)
                if kind == "Stop" and not duplicate and metadata.get("active_prompt") != prompt:
                    raise AdapterError("Claude Stop has no matching recorded active prompt")
                journal.open()
                journal.add(key, "message.completed", role="user" if kind == "UserPromptSubmit" else "assistant", text=text)
                if not duplicate:
                    metadata["active_prompt"] = prompt
                    metadata["completed"] = kind == "Stop"
                    journal.set_metadata(metadata)
        except AdapterError:
            journal.halt()
            raise
        return journal.flush()
