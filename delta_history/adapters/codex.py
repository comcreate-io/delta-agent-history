"""Explicit single-run wrapper around public `codex exec --json` output."""
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import uuid

from ..protocol import MAX_LINE_BYTES, MAX_TEXT_BYTES
from .common import AdapterError, Journal, required_id, visible_text


def item_message(item):
    if not isinstance(item, dict):
        raise AdapterError("Malformed Codex item")
    if item.get("type") == "agent_message":
        return "assistant", visible_text(item.get("text"))
    if item.get("type") == "todo_list":
        steps = item.get("items")
        if not isinstance(steps, list) or not steps or len(steps) > 200:
            raise AdapterError("Malformed Codex plan")
        lines = []
        for step in steps:
            if not isinstance(step, dict) or type(step.get("completed")) is not bool:
                raise AdapterError("Malformed Codex plan step")
            lines.append(("[x] " if step["completed"] else "[ ] ") + visible_text(step.get("text")))
        return "plan", visible_text("\n".join(lines))
    return None


def codex_run(config, state, instance, project, codex_binary, prompt_file, model=None):
    binary = Path(codex_binary).expanduser()
    if not binary.is_absolute() or not binary.is_file() or not os.access(binary, os.X_OK):
        raise AdapterError("Supply an absolute executable Codex path")
    if model is not None:
        required_id(model)
    fd = os.open(Path(prompt_file).expanduser(), os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as source:
        if not stat.S_ISREG(os.fstat(source.fileno()).st_mode):
            raise AdapterError("Prompt must be a regular file")
        raw = source.read(MAX_TEXT_BYTES + 1)
    try:
        prompt = visible_text(raw.decode("utf-8"))
    except UnicodeError:
        raise AdapterError("Prompt must be bounded UTF-8 text") from None
    # A new invocation is a new run, even when prompt text is identical.
    # The persisted run ID is never regenerated while consuming this process.
    session = "run:" + str(uuid.uuid4())
    with Journal(config, state, "codex", instance, session, str(project)) as journal:
        with journal.transaction():
            journal.open()
            journal.add("prompt", "message.completed", role="user", text=prompt)
            journal.set_metadata({"run_id":session, "capture":"starting"})
        journal.flush()
        args = [str(binary), "exec", "--json", "--cd", journal.project]
        if model:
            args += ["-m",model]
        args += ["-"]
        # Anonymous temporary stdin avoids write/read pipe deadlocks and argv disclosure.
        with tempfile.TemporaryFile() as input_file:
            input_file.write(raw)
            input_file.seek(0)
            try:
                process = subprocess.Popen(args, stdin=input_file, stdout=subprocess.PIPE)
            except OSError:
                with journal.transaction():
                    journal.add("close", "session.close", status="failed")
                    metadata = journal.metadata()
                    metadata["capture"] = "launch_failed"
                    journal.set_metadata(metadata)
                journal.flush()
                raise AdapterError("Codex could not be started") from None
            capture_failed, turn_completed, turn_failed, thread = False, False, False, None
            total = 0
            try:
                while True:
                    line = process.stdout.readline(MAX_LINE_BYTES + 1)
                    if not line:
                        break
                    total += len(line)
                    if len(line) > MAX_LINE_BYTES or total > 64 * 1024 * 1024:
                        capture_failed = True
                    if capture_failed:
                        continue
                    try:
                        event = json.loads(line)
                        if not isinstance(event, dict):
                            raise AdapterError("Malformed Codex event")
                        kind = event.get("type")
                        if kind == "thread.started":
                            identity = required_id(event.get("thread_id"))
                            if thread is not None and thread != identity:
                                raise AdapterError("Codex changed thread identity")
                            thread = identity
                            with journal.transaction():
                                metadata = journal.metadata()
                                metadata.update({"upstream_thread_id":thread, "capture":"running"})
                                journal.set_metadata(metadata)
                        elif kind == "item.completed":
                            if thread is None or turn_completed or turn_failed:
                                raise AdapterError("Codex item outside its active thread")
                            message = item_message(event.get("item"))
                            if message:
                                fields = {"role":message[0], "text":message[1]}
                                if model:
                                    fields["model"] = model
                                with journal.transaction():
                                    journal.add("item:"+required_id(event["item"].get("id")), "message.completed", **fields)
                                journal.flush()
                                print(message[1], flush=True)
                        elif kind == "turn.completed":
                            if thread is None:
                                raise AdapterError("Codex completed before thread creation")
                            turn_completed = True
                        elif kind in ("turn.failed", "error"):
                            turn_failed = True
                        # Deliberately discard reasoning, tools, file changes and partial items.
                    except Exception:
                        capture_failed = True
                code = process.wait()
            except KeyboardInterrupt:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                code = 130
                turn_failed = True
            finally:
                process.stdout.close()
            complete = code == 0 and turn_completed and not turn_failed and not capture_failed
            try:
                with journal.transaction():
                    journal.add("close", "session.close", status="completed" if complete else "cancelled" if code == 130 else "failed")
                    metadata = journal.metadata()
                    metadata.update({"capture":"completed" if complete else "incomplete", "exit_code":code})
                    journal.set_metadata(metadata)
                journal.flush()
            except Exception:
                capture_failed = True
            if capture_failed:
                journal.halt()
            if not complete or capture_failed:
                print("Delta history: wrapped run or capture was incomplete; inspect local status.", file=sys.stderr)
            # Never relaunch a model to replay capture. Return the process exit status.
            return code if code >= 0 else 128 - code
