import contextlib
import copy
import fcntl
import json
import io
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from delta_history import core, protocol
from t3_delta.delta_sink import DeltaConflict, DeltaUnavailable
from t3_delta.delta_sink import DeltaSink


class FakeSink:
    stored = {}
    calls = []
    failure = None
    crash_once = False
    failure_keys = {}

    def __init__(self, state, **kwargs):
        self.kwargs = kwargs

    def ensure_session(self, key, directory):
        self.calls.append(("attach", key))
        self.stored.setdefault(key, {})
        return {"thread_id": "delta-" + key, "reference": "reference", "directory": directory}

    def enqueue(self, key, item, update):
        previous = self.stored[key].get(item)
        if previous and previous != update:
            raise AssertionError("Replay changed immutable update")
        self.stored[key][item] = copy.deepcopy(update)
        self.calls.append(("enqueue", key, item))
        if self.crash_once:
            type(self).crash_once = False
            raise KeyboardInterrupt("simulated process death after sink enqueue")

    def flush(self, key):
        if key in self.failure_keys:
            raise self.failure_keys[key]
        if self.failure:
            raise self.failure
        return {"pending": 0, "visibility": "private", "upload": {"state": "uploaded", "error": None}, "access": {"state": "granted"}, "halted": False}

    def close(self):
        pass


class CoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repo = self.repository("repo")
        self.state = self.root / "state"
        self.config = {"schema_version": 2, "sources": [{"kind": "custom", "instance": "desktop"}, {"kind": "codex", "instance": "desktop"}],
                       "projects_root": str(self.root), "additional_project_paths": [],
                       "allowed_projects": [{"path": str(self.repo), "remote": "https://example.com/team/app.git", "enrolled_after": "2020-01-01T00:00:00Z"}],
                       "allowed_remotes": ["https://example.com/team/app.git"], "delta_binary": "/not-executed/zed-delta"}
        FakeSink.stored, FakeSink.calls, FakeSink.failure, FakeSink.crash_once, FakeSink.failure_keys = {}, [], None, False, {}

    def repository(self, name):
        directory = self.root / name
        subprocess.run(["git", "init", "--quiet", str(directory)], check=True)
        subprocess.run(["git", "-C", str(directory), "remote", "add", "origin", "https://example.com/team/app.git"], check=True)
        return directory

    def event(self, sequence=1, kind="session.open", **extras):
        event = {"schema_version": 1, "type": kind, "source": "custom", "source_instance": "desktop", "session_id": "session:a", "event_id": f"event-{sequence}", "sequence": sequence, "created_at": "2026-01-01T00:00:00Z"}
        event.update({"project": str(self.repo)} if kind == "session.open" else {"role": "assistant", "text": "Visible message"} if kind == "message.completed" else {"status": "completed"})
        event.update(extras)
        return event

    def submit(self, *events):
        return core.submit(self.config, self.state, events)

    def flush(self):
        return core.flush(self.config, self.state, FakeSink)

    def test_local_durable_acceptance_idempotent_and_source_namespaces(self):
        a, b = self.event(), self.event(2, "message.completed")
        self.assertEqual(self.submit(a, b)["accepted"], 2)
        self.assertEqual(self.submit(a, b)["duplicates"], 2)
        self.assertEqual(FakeSink.calls, [])
        c = self.event(source="codex")
        self.assertEqual(self.submit(c)["accepted"], 1)
        rows = core.sessions(self.config, self.state, self.repo)
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]["session_key"], rows[1]["session_key"])
        self.assertNotEqual(core.session_key("a", "b", "c:d"), core.session_key("a", "b:c", "d"))
        self.assertEqual(core.status(self.config, self.state)["health"]["phase"], "catching_up")

    def test_revoked_source_and_existing_hold_release_prior_mount(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        key = core.sessions(self.config, self.state, self.repo)[0]["session_key"]
        self.config["sources"] = [{"kind": "codex", "instance": "desktop"}]
        with patch.object(FakeSink, "release_session", create=True) as release:
            self.assertEqual(self.flush()["halted"], 1)
            release.assert_called_once_with(key)
        with patch.object(FakeSink, "release_session", create=True) as release:
            self.assertEqual(self.flush()["halted"], 1)
            release.assert_called_once_with(key)

    def test_claude_idle_exit_and_resume_keep_identity_and_release_leases(self):
        from delta_history.adapters.claude import claude_hook
        self.config["sources"].append({"kind": "claude-code", "instance": "desktop"})
        def hook(kind, prompt=None, text=None):
            payload = {"hook_event_name": kind, "session_id": "native-resumable", "cwd": str(self.repo)}
            if prompt:
                payload["prompt_id"] = prompt
            if text:
                payload["prompt" if kind == "UserPromptSubmit" else "last_assistant_message"] = text
            return claude_hook(self.config, self.state, "desktop", io.StringIO(json.dumps(payload)))
        hook("UserPromptSubmit", "p1", "first prompt")
        hook("Stop", "p1", "first answer")
        with patch.object(FakeSink, "release_session", create=True) as release:
            self.assertEqual(self.flush()["delivered_sessions"], 1)
            release.assert_called_once()
        key = core.sessions(self.config, self.state, self.repo)[0]["session_key"]
        FakeSink.calls.clear()
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertFalse(FakeSink.calls)
        hook("SessionEnd")
        hook("UserPromptSubmit", "p2", "resumed prompt")
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertTrue(FakeSink.calls)
        hook("Stop", "p2", "resumed answer")
        hook("SessionEnd")
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        sessions = core.sessions(self.config, self.state, self.repo)
        self.assertEqual(len(sessions), 1)
        self.assertEqual(sessions[0]["session_key"], key)
        self.assertEqual(sessions[0]["phase"], "paused")
        self.assertEqual(len(FakeSink.stored[key]), 4)
    def test_conflicting_sequence_or_event_halts_durably_and_never_delivers(self):
        a, b = self.event(), self.event(2, "message.completed")
        self.submit(a, b)
        conflict = self.submit({**b, "text": "changed"})
        self.assertEqual(conflict["halted"], 1)
        self.assertEqual(self.submit(self.event(3, "message.completed"))["rejected"], 1)
        self.assertEqual(self.submit(b)["duplicates"], 1)
        self.assertEqual(self.flush()["halted"], 1)
        self.assertFalse(FakeSink.calls)

    def test_mixed_batch_unopened_session_cannot_rollback_observed_conflict(self):
        message = self.event(2, "message.completed")
        self.submit(self.event(), message)
        result = self.submit({**message, "text": "conflicting body"},
                             self.event(2, "message.completed", source="codex", session_id="unopened-b"))
        self.assertEqual(result["rejected"], 2)
        self.assertEqual(result["halted"], 1)
        self.assertEqual(core.status(self.config, self.state)["halted"], 1)
        self.assertEqual(self.submit(self.event(3, "message.completed"))["rejected"], 1)
        self.assertEqual(self.flush()["halted"], 1)
        self.assertFalse(FakeSink.calls)

    def test_sequence_gaps_wait_for_contiguous_prefix(self):
        self.submit(self.event(), self.event(3, "message.completed"))
        self.assertEqual(self.flush()["catching_up"], 1)
        self.assertFalse(any(c[0] == "enqueue" for c in FakeSink.calls))
        self.submit(self.event(2, "message.completed", role="user", text="first"))
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        values = list(next(iter(FakeSink.stored.values())).values())
        self.assertEqual([v["content"][0]["text"] for v in values], ["first", "Visible message"])

    def test_crash_after_sink_enqueue_replays_identically_after_restart(self):
        self.submit(self.event(), self.event(2, "message.completed", model="example-model"))
        FakeSink.crash_once = True
        with self.assertRaises(KeyboardInterrupt):
            self.flush()
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertEqual(len(next(iter(FakeSink.stored.values()))), 1)
        self.assertEqual(len([c for c in FakeSink.calls if c[0] == "enqueue"]), 2)

    def test_close_terminal_and_duplicate_safe(self):
        close = self.event(3, "session.close")
        self.submit(self.event(), close)
        self.submit(self.event(2, "message.completed"))
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertEqual(core.sessions(self.config, self.state, self.repo)[0]["phase"], "closed")
        self.assertEqual(self.submit(close)["duplicates"], 1)
        self.assertEqual(self.submit(self.event(4, "message.completed"))["halted"], 1)

    def test_spoofed_source_unapproved_clone_old_session_and_symlink_refused(self):
        other = self.repository("other")
        alias = self.root / "alias"
        alias.symlink_to(self.repo)
        for event in (self.event(source="unknown"), self.event(project=str(other)), self.event(project=str(alias)), self.event(created_at="2019-01-01T00:00:00Z")):
            with self.subTest(event=event), self.assertRaises(ValueError):
                self.submit(event)
        self.assertFalse(self.state.exists())

    def test_changed_checkout_origin_or_inode_blocks_initial_attach(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        subprocess.run(["git", "-C", str(self.repo), "remote", "set-url", "origin", "https://example.com/other.git"], check=True)
        self.assertEqual(self.flush()["halted"], 1)
        self.assertFalse(FakeSink.calls)

    def test_fencing_isolated_per_session_and_retryable_retained(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        FakeSink.failure = DeltaUnavailable("test")
        with patch.object(FakeSink, "release_session", create=True) as release:
            self.assertEqual(self.flush()["catching_up"], 1)
            release.assert_not_called()
        FakeSink.failure = None
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        FakeSink.failure = DeltaConflict("fenced")
        with patch.object(FakeSink, "release_session", create=True) as release:
            self.assertEqual(self.flush()["halted"], 1)
            release.assert_called_once()
        self.assertEqual(core.status(self.config, self.state)["halted"], 1)

    def test_concurrent_worker_fails_busy_without_remote_calls(self):
        self.submit(self.event())
        with open(self.state / "writer.lock", "w") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.assertTrue(self.flush()["busy"])
        self.assertFalse(FakeSink.calls)

    def test_sqlite_writer_contention_retries_identical_message_without_halt(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        writer = sqlite3.connect(self.state / "ingress.sqlite3")
        self.addCleanup(writer.close)
        database = core._database
        enqueue = FakeSink.enqueue
        expected = None

        def short_timeout(state):
            db = database(state)
            db.execute("PRAGMA busy_timeout=0")
            return db

        def contested_enqueue(sink, key, item, update):
            nonlocal expected
            enqueue(sink, key, item, update)
            expected = copy.deepcopy(FakeSink.stored)
            writer.execute("BEGIN IMMEDIATE")

        # Let the old permanent-error handler finish after observing contention.
        # Recovery must not invoke it, even if the writer is still busy.
        halt = core._halt
        def release_before_halt(*args):
            writer.rollback()
            return halt(*args)

        try:
            with patch("delta_history.core._database", side_effect=short_timeout), patch.object(FakeSink, "enqueue", contested_enqueue), patch("delta_history.core._halt", side_effect=release_before_halt):
                result = self.flush()
        finally:
            writer.rollback()
        self.assertEqual(result["halted"], 0)
        self.assertEqual(result["catching_up"], 1)
        self.assertEqual(core.status(self.config, self.state)["halted"], 0)
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertEqual(FakeSink.stored, expected)
        self.assertEqual(core.status(self.config, self.state)["pending"], 0)

    def test_non_lock_sqlite_error_still_requires_review(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        FakeSink.failure = sqlite3.OperationalError("unrecognized database failure")
        self.assertEqual(self.flush()["halted"], 1)

    def test_extended_sqlite_busy_and_locked_codes_remain_retryable(self):
        self.submit(self.event(), self.event(2, "message.completed"))
        for code in (sqlite3.SQLITE_BUSY_SNAPSHOT, sqlite3.SQLITE_LOCKED_SHAREDCACHE):
            with self.subTest(code=code):
                failure = sqlite3.OperationalError("synthetic contention")
                failure.sqlite_errorcode = code
                FakeSink.failure = failure
                self.assertEqual(self.flush()["catching_up"], 1)
                self.assertEqual(core.status(self.config, self.state)["halted"], 0)
        FakeSink.failure = None
        self.assertEqual(self.flush()["delivered_sessions"], 1)

    def test_legacy_root_and_symlink_state_never_mutated(self):
        self.state.mkdir()
        old = self.state / "source.sqlite3"
        old.write_bytes(b"preserve legacy bytes")
        with self.assertRaises(ValueError):
            self.submit(self.event())
        self.assertEqual(old.read_bytes(), b"preserve legacy bytes")
        old.unlink()
        self.state.rmdir()
        actual = self.root / "actual"
        actual.mkdir()
        self.state.symlink_to(actual)
        with self.assertRaises(ValueError):
            self.submit(self.event())

    def test_status_and_scoped_listing_do_not_create_files_or_use_delta(self):
        self.assertEqual(core.status(self.config, self.state)["health"]["phase"], "not_started")
        self.assertEqual(core.sessions(self.config, self.state, self.repo), [])
        self.assertFalse(self.state.exists())
        with self.assertRaises(ValueError):
            core.history(self.config, self.state, self.repo, "unknown")
        self.assertFalse(self.state.exists())

    def test_adapter_only_pending_and_halted_are_visible_without_ingress_creation(self):
        from delta_history.adapters.common import Journal
        with Journal(self.config, self.state, "custom", "desktop", "adapter-session", self.repo) as journal:
            with journal.transaction():
                journal.open()
            observed = core.status(self.config, self.state)
            self.assertEqual(observed["adapters"]["pending"], 1)
            self.assertEqual(observed["health"]["phase"], "catching_up")
            journal.halt()
            self.assertEqual(core.status(self.config, self.state)["health"]["phase"], "needs_review")
        self.assertFalse((self.state / "ingress.sqlite3").exists())

    def test_orphan_sink_binding_requires_review_without_reconstructing_ingress(self):
        sink = DeltaSink(self.state / "delta")
        with sink.db:
            sink.db.execute("INSERT INTO sessions(external_id,directory) VALUES(?,?)", ("orphan", str(self.repo)))
        sink.close()
        observed = core.status(self.config, self.state)
        self.assertEqual(observed["orphaned_bindings"], 1)
        self.assertEqual(observed["health"]["phase"], "needs_review")
        result = core.flush(self.config, self.state)
        self.assertEqual(result["halted"], 1)
        self.assertEqual(result["orphaned_bindings"], 1)
    def test_protocol_rejects_hidden_fields_invalid_budgets_and_json_duplicate_keys(self):
        for event in (self.event(extra="hidden"), self.event(sequence=True), self.event(created_at="2026-01-01T00:00:00"), self.event(2, "message.completed", role="tool"), self.event(2, "message.completed", text="x" * (protocol.MAX_TEXT_BYTES + 1))):
            with self.subTest(event=str(event)[:100]), self.assertRaises(ValueError):
                protocol.validate_event(event)
        with self.assertRaises(ValueError):
            protocol.validate_batch([self.event()] * 101)
        with self.assertRaises(ValueError):
            protocol.parse_jsonl(['{"schema_version":1,"schema_version":1}'])
        self.assertEqual(protocol.parse_jsonl([json.dumps(self.event()) + "\n"]), [self.event()])

    def test_concurrent_hooks_serialize_without_dropping_events(self):
        self.submit(self.event())
        events = [self.event(sequence, "message.completed") for sequence in range(2, 12)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(lambda e: self.submit(e), events))
        self.assertEqual(sum(r["accepted"] for r in results), 10)
        self.assertEqual(core.status(self.config, self.state)["accepted"], 11)
        self.assertEqual(self.flush()["delivered_sessions"], 1)
        self.assertEqual(len(next(iter(FakeSink.stored.values()))), 10)

    def test_one_fenced_session_does_not_block_another(self):
        self.submit(self.event(), self.event(2, "message.completed"), self.event(source="codex"), self.event(2, "message.completed", source="codex"))
        FakeSink.failure_keys[core.session_key("custom", "desktop", "session:a")] = DeltaConflict("fence")
        result = self.flush()
        self.assertEqual(result["halted"], 1)
        self.assertEqual(result["delivered_sessions"], 1)

    def test_removed_source_or_replaced_checkout_cannot_attach(self):
        self.submit(self.event())
        self.config["sources"] = [{"kind": "codex", "instance": "desktop"}]
        self.assertEqual(self.flush()["halted"], 1)
        self.assertFalse(FakeSink.calls)

    def test_repository_scoped_bounded_history_reads_no_constructor_or_private_delta_db(self):
        self.submit(self.event(), self.event(2, "message.completed", role="user"), self.event(3, "message.completed"))
        key = core.session_key("custom", "desktop", "session:a")
        sink = DeltaSink(self.state / "delta")
        with sink.db:
            sink.db.execute("INSERT INTO sessions(external_id,directory,thread_id,reference,visibility,upload,access) VALUES(?,?,?,?,?,?,?)", (key, str(self.repo), "synthetic-thread", "ref", "private", json.dumps({"state": "uploaded", "error": None}), json.dumps({"state": "granted"})))
            for sequence, role in ((1, "user_message"), (2, "agent_message")):
                sink.db.execute("INSERT INTO records(external_id,sequence,item_key,body,update_json,acknowledged,disposition) VALUES(?,?,?,?,?,1,'recorded')", (key, sequence, str(sequence), "{}", json.dumps({"sessionUpdate": role})))
        sink.close()
        # Git identity checks are public local Git operations; mock only the Delta read.
        actual_run = subprocess.run
        def dispatched(args, **kwargs):
            if args[0] == self.config["delta_binary"]:
                return subprocess.CompletedProcess(args, 0, '[{"text":"visible"}]', '')
            return actual_run(args, **kwargs)
        with patch("delta_history.core.subprocess.run", side_effect=dispatched) as run:
            self.assertEqual(core.history(self.config, self.state, self.repo, key), [{"text": "visible"}])
            read = next(c.args[0] for c in run.call_args_list if c.args[0][0] == self.config["delta_binary"])
            self.assertEqual(read[-1], "1-2")
        with self.assertRaises(ValueError):
            core.history(self.config, self.state, self.repo, key, "1-21")
        other = self.repository("second")
        self.config["allowed_projects"].append({"path": str(other), "remote": "https://example.com/team/app.git", "enrolled_after": "2020-01-01T00:00:00Z"})
        with self.assertRaises(ValueError):
            core.history(self.config, self.state, other, key)
