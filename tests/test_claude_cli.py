import contextlib
import json
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from delta_history.configuration import initialize


class ClaudeCliTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.project = self.root / "project"
        subprocess.run(["git", "init", "-q", str(self.project)], check=True)
        subprocess.run(["git", "-C", str(self.project), "remote", "add", "origin",
                        "https://github.com/example/synthetic.git"], check=True)
        self.config = self.root / "config.json"
        self.state = self.root / "state"
        self.launched = self.root / "delta-launched"
        binary = self.root / "delta"
        binary.write_text("#!" + sys.executable + "\nfrom pathlib import Path\nPath(" +
                          repr(str(self.launched)) + ").touch()\nraise SystemExit(99)\n")
        binary.chmod(0o700)
        initialize(self.config, projects=[self.project], sources=["claude-code:test"],
                   delta_binary=str(binary), accept_upload=True)
        self.prefix = [sys.executable, "-m", "delta_history", "--config", str(self.config),
                       "--state-dir", str(self.state)]

    def payload(self, event="UserPromptSubmit", text="synthetic visible text"):
        return {"hook_event_name": event, "session_id": "synthetic-session",
                "prompt_id": "synthetic-prompt", "cwd": str(self.project),
                "prompt" if event == "UserPromptSubmit" else "last_assistant_message": text}

    def invoke(self, payload):
        result = subprocess.run(
            self.prefix + ["hook", "claude", "--instance", "test"],
            input=json.dumps(payload), text=True, capture_output=True,
            cwd=Path(__file__).resolve().parents[1], timeout=10,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, "")
        self.assertFalse(self.launched.exists(), "Hooks must never invoke Delta")
        return result

    def test_successful_prompt_stop_and_session_end_enqueue_without_stdout(self):
        for event in ("UserPromptSubmit", "Stop", "SessionEnd"):
            with self.subTest(event=event):
                self.assertEqual(self.invoke(self.payload(event)).stderr, "")
        with contextlib.closing(sqlite3.connect(self.state / "ingress.sqlite3")) as db:
            events = [json.loads(row[0]) for row in db.execute("SELECT body FROM events ORDER BY sequence")]
        self.assertEqual([event["type"] for event in events],
                         ["session.open", "message.completed", "message.completed"])
        self.assertEqual([event["role"] for event in events[1:]], ["user", "assistant"])

    def test_unapproved_scratch_directory_does_not_block_or_create_state(self):
        scratch = self.root / "scratch"
        scratch.mkdir()
        payload = self.payload()
        payload["cwd"] = str(scratch)
        result = self.invoke(payload)
        self.assertEqual(json.loads(result.stderr)["error"], "ValueError")
        self.assertNotIn(str(scratch), result.stderr)
        self.assertNotIn(payload["prompt"], result.stderr)
        self.assertFalse(self.state.exists())

    def test_missing_and_invalid_config_do_not_block_or_leak_values(self):
        self.config.unlink()
        for contents in (None, "private-config-not-json"):
            with self.subTest(contents=contents):
                if contents is not None:
                    self.config.write_text(contents)
                result = self.invoke(self.payload())
                self.assertIn("continuing without blocking Claude", json.loads(result.stderr)["message"])
                self.assertNotIn(str(self.config), result.stderr)
                self.assertNotIn("private-config-not-json", result.stderr)
                self.assertFalse(self.state.exists())

    def test_invalid_payloads_are_rejected_without_blocking_or_state(self):
        missing_prompt_id = self.payload()
        del missing_prompt_id["prompt_id"]
        for payload in (missing_prompt_id, ["private-invalid-payload"]):
            with self.subTest(payload=payload):
                result = self.invoke(payload)
                self.assertEqual(json.loads(result.stderr)["error"], "AdapterError")
                self.assertNotIn("private-invalid-payload", result.stderr)
                self.assertFalse(self.state.exists())

    def test_conflicts_remain_held_with_original_bytes_while_claude_continues(self):
        self.invoke(self.payload())
        with contextlib.closing(sqlite3.connect(self.state / "adapters.sqlite3")) as db:
            before = db.execute("SELECT payload FROM adapter_events ORDER BY sequence").fetchall()
        for text in ("private-conflicting-message", "synthetic visible text"):
            result = self.invoke(self.payload(text=text))
            self.assertEqual(json.loads(result.stderr)["error"], "AdapterError")
            self.assertNotIn(text, result.stderr)
            with contextlib.closing(sqlite3.connect(self.state / "adapters.sqlite3")) as db:
                self.assertEqual(db.execute("SELECT halted FROM adapter_sessions").fetchone()[0], 1)
                self.assertEqual(db.execute("SELECT payload FROM adapter_events ORDER BY sequence").fetchall(), before)

    def test_example_commands_work_with_claude_shell_invocation(self):
        source = Path(__file__).resolve().parents[1]
        example = json.loads((source / "integrations/claude-hooks.example.json").read_text())
        for event in ("UserPromptSubmit", "Stop", "SessionEnd"):
            hook = example["hooks"][event][0]["hooks"][0]
            command = hook["command"].replace("/absolute/path/to/delta-history", shlex.join(self.prefix))
            command = command.replace("--instance workstation", "--instance test")
            result = subprocess.run(["sh", "-c", command], input=json.dumps(self.payload(event)),
                                    text=True, capture_output=True, cwd=source, timeout=10)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertEqual(result.stderr, "")
        self.assertFalse(self.launched.exists())


if __name__ == "__main__":
    unittest.main()
