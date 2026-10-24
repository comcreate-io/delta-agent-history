import copy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

from t3_delta.engine import Engine
from t3_delta.delta_sink import DeltaUnavailable, DeltaConflict
from t3_delta.cli import read_status, writer_lock


NOW = "2026-10-23T12:00:00+00:00"
FUTURE = "2026-10-23T12:01:00+00:00"
REMOTE = "https://github.com/example/project.git"


class Source:
    def __init__(self, repo):
        self.project = {"id": "project", "workspaceRoot": str(repo)}
        self.thread = {"threadId": "new", "projectId": "project", "createdAt": FUTURE,
                       "worktreePath": None, "status": "completed", "activeRunId": None}
        self.items = [self.message("u", "user_message", "prompt"), self.message("a", "assistant_message", "reply")]
        self.recent_runs = []
        self.read_calls = 0

    def message(self, key, kind, text):
        return {"itemId": key, "type": kind, "text": text, "sourceThreadId": "new", "visibility": "local",
                "status": "completed", "runId": "run", "textTruncated": False, "nextTextOffset": None}

    def environment(self):
        return {"environmentId": "env", "serverVersion": "0.1.0"}

    def projects(self):
        return [self.project]

    def threads(self, _):
        return [self.thread]

    def call_tool(self, *_):
        return {"thread": copy.deepcopy(self.thread)}

    def read_thread(self, _):
        self.read_calls += 1
        return {"thread": copy.deepcopy(self.thread), "items": copy.deepcopy(self.items), "recentRuns": copy.deepcopy(self.recent_runs)}


class Sink:
    def __init__(self):
        self.bindings = []
        self.records = {}
        self.fail = False
        self.fail_flush = False
        self.saved_sessions = {}
        self.acknowledged = set()
        self.delivered = []
        self.flush_calls = []

    def ensure_session(self, external_id, directory):
        self.saved_sessions.setdefault(external_id, {"external_id": external_id, "directory": directory,
                                                    "thread_id": None, "reference": None, "halted": 0})
        if self.saved_sessions[external_id]["halted"]:
            raise DeltaConflict("held")
        if self.fail:
            raise DeltaUnavailable("offline")
        self.bindings.append((external_id, directory))
        self.saved_sessions[external_id].update(thread_id="delta-thread", reference="checkout-reference")
        return copy.deepcopy(self.saved_sessions[external_id])

    def enqueue(self, external_id, key, update):
        binding = (external_id, key)
        if binding in self.records:
            assert self.records[binding] == update
        self.records[binding] = update

    def flush(self, external_id):
        self.flush_calls.append(external_id)
        if self.saved_sessions[external_id]["halted"]:
            raise DeltaConflict("held")
        if self.fail_flush:
            raise DeltaUnavailable("offline")
        for key, update in self.records.items():
            if key[0] == external_id and key not in self.acknowledged:
                self.delivered.append((key, copy.deepcopy(update)))
                self.acknowledged.add(key)
        return {"pending": 0}

    def sessions(self):
        result = []
        for key, saved in self.saved_sessions.items():
            session = copy.deepcopy(saved)
            session["pending"] = sum(k[0] == key and k not in self.acknowledged for k in self.records)
            result.append(session)
        return result


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.repo = root / "repo"
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "remote", "add", "origin", REMOTE], check=True)
        self.source, self.sink = Source(self.repo), Sink()
        self.config = {"environment_id": "env", "allowed_remotes": [REMOTE], "poll_seconds": 10}
        self.state = root / "state"
        self.engine = Engine(self.config, self.state, self.source, self.sink, clock=lambda: NOW)
        self.addCleanup(self.engine.close)

    def scoped_engine(self, enrolled_after=NOW):
        config = {**self.config, "projects_root": str(self.repo.parent), "allowed_projects": [
            {"path": str(self.repo), "remote": REMOTE, "enrolled_after": enrolled_after}]}
        engine = Engine(config, self.state, self.source, self.sink, clock=lambda: NOW)
        self.addCleanup(engine.close)
        return engine

    def extra_scoped_engine(self):
        projects = self.repo.parent / "projects"
        projects.mkdir()
        config = {**self.config, "projects_root": str(projects), "additional_project_paths": [str(self.repo)],
                  "allowed_projects": [{"path": str(self.repo), "remote": REMOTE, "enrolled_after": NOW}]}
        engine = Engine(config, self.state, self.source, self.sink, clock=lambda: NOW)
        self.addCleanup(engine.close)
        return engine

    def test_scope_explicit_extra_repository_does_not_enroll_neighboring_clone(self):
        engine = self.extra_scoped_engine()
        engine.poll()
        self.assertEqual(self.sink.bindings, [("env:new", str(self.repo))])
        neighbor = self.repo.parent / "unapproved-neighbor"
        neighbor.mkdir()
        subprocess.run(["git", "init", "-q", str(neighbor)], check=True)
        subprocess.run(["git", "-C", str(neighbor), "remote", "add", "origin", REMOTE], check=True)
        self.source.project["workspaceRoot"] = str(neighbor)
        engine.poll()
        self.assertEqual(self.sink.bindings, [("env:new", str(self.repo))])

    def test_scope_extra_repository_accepts_its_actual_linked_worktree(self):
        engine = self.extra_scoped_engine()
        subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "--no-gpg-sign", "-qm", "fixture"], check=True)
        linked = self.repo.parent / "linked-extra"
        subprocess.run(["git", "-C", str(self.repo), "worktree", "add", "--detach", "-q", str(linked)], check=True)
        self.source.thread["worktreePath"] = str(linked)
        engine.poll()
        self.assertEqual(self.sink.bindings, [("env:new", str(linked))])

    def test_scope_requires_exact_approved_project_not_another_clone_of_same_remote(self):
        engine = self.scoped_engine()
        other = self.repo.parent / "unapproved-copy"
        other.mkdir()
        subprocess.run(["git", "init", "-q", str(other)], check=True)
        subprocess.run(["git", "-C", str(other), "remote", "add", "origin", REMOTE], check=True)
        self.source.project["workspaceRoot"] = str(other)
        engine.poll()
        self.assertEqual(self.sink.bindings, [])
        self.assertEqual(engine.status()["health"]["phase"], "awaiting_project")

    def test_scope_new_repository_cutoff_does_not_backfill_earlier_pilot_threads(self):
        engine = self.scoped_engine("2026-10-23T12:02:00Z")
        engine.poll()
        self.assertEqual(self.sink.records, {})
        self.source.thread["createdAt"] = "2026-10-23T12:03:00Z"
        engine.poll()
        self.assertEqual(len(self.sink.records), 2)

    def test_scope_checks_actual_remote_even_for_an_approved_directory(self):
        engine = self.scoped_engine()
        subprocess.run(["git", "-C", str(self.repo), "remote", "set-url", "origin", "https://github.com/other/repo.git"], check=True)
        engine.poll()
        self.assertEqual(self.sink.bindings, [])

    def test_scope_rejects_an_unrelated_checkout_binding_with_the_same_remote(self):
        engine = self.scoped_engine()
        other = self.repo.parent / "unrelated"
        other.mkdir()
        subprocess.run(["git", "init", "-q", str(other)], check=True)
        subprocess.run(["git", "-C", str(other), "remote", "add", "origin", REMOTE], check=True)
        self.source.thread["worktreePath"] = str(other)
        engine.poll()
        self.assertEqual(self.sink.bindings, [])
        self.assertEqual(engine.status()["health"]["phase"], "needs_review")

    def test_scope_accepts_real_linked_worktree(self):
        engine = self.scoped_engine()
        subprocess.run(["git", "-C", str(self.repo), "-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null", "commit", "--allow-empty", "--no-gpg-sign", "-qm", "fixture"], check=True)
        linked = self.repo.parent / "linked"
        subprocess.run(["git", "-C", str(self.repo), "worktree", "add", "--detach", "-q", str(linked)], check=True)
        self.source.thread["worktreePath"] = str(linked)
        engine.poll()
        self.assertEqual(self.sink.bindings, [("env:new", str(linked))])
        self.assertEqual(len(self.sink.records), 2)

    def test_scope_partial_attachment_cannot_retry_after_project_is_removed(self):
        engine = self.scoped_engine()
        self.sink.fail = True
        engine.poll()
        self.sink.fail = False
        replacement = {**self.config, "projects_root": str(self.repo.parent), "allowed_projects": [
            {"path": str(self.repo.parent / "not-enrolled"), "remote": REMOTE, "enrolled_after": NOW}]}
        restarted = Engine(replacement, self.state, None, self.sink, clock=lambda: NOW)
        self.addCleanup(restarted.close)
        restarted.retry_delivery()
        self.assertEqual(self.sink.bindings, [])
        self.assertEqual(restarted.status()["health"]["phase"], "needs_review")

    def test_capture_and_restart_do_not_duplicate_or_export_other_item_types(self):
        self.source.items += [self.source.message("thought", "reasoning", "never export"),
                              dict(self.source.message("ancestor", "assistant_message", "inherited"), visibility="inherited")]
        self.engine.poll()
        self.engine.poll()
        self.assertEqual(len(self.sink.records), 2)
        self.assertEqual(len(self.engine.status()["threads"]), 1)
        self.assertNotIn("never export", str(self.sink.records))
        restarted = Engine(self.config, self.state, self.source, self.sink, clock=lambda: NOW)
        try:
            restarted.poll()
            self.assertEqual(len(self.sink.records), 2)
        finally:
            restarted.close()

    def test_old_threads_and_other_repositories_never_attach(self):
        self.source.thread["createdAt"] = "2026-10-22T00:00:00Z"
        self.engine.poll()
        self.assertEqual(self.sink.bindings, [])
        self.source.thread["createdAt"] = FUTURE
        subprocess.run(["git", "-C", str(self.repo), "remote", "set-url", "origin", "https://github.com/another/private.git"], check=True)
        self.engine.poll()
        self.assertEqual(self.sink.bindings, [])

    def test_live_output_waits_for_completion_and_final_text_is_recorded(self):
        self.source.thread.update(status="running", activeRunId="run")
        self.source.items[1]["text"] = "partial"
        self.engine.poll()
        self.assertEqual(len(self.sink.records), 1)
        self.source.thread.update(status="completed", activeRunId=None)
        self.source.items[1]["text"] = "final"
        self.engine.poll()
        self.assertEqual(self.sink.records[("env:new", "message:a")]["content"][0]["text"], '[T3 turn {"runId": "run"}]\nfinal')

    def test_mutated_completed_message_halts_instead_of_overwriting_history(self):
        self.engine.poll()
        self.source.items[1]["text"] = "changed after completion"
        self.engine.poll()
        self.assertEqual(self.engine.status()["health"]["phase"], "needs_review")
        self.assertEqual(self.sink.records[("env:new", "message:a")]["content"][0]["text"], '[T3 turn {"runId": "run"}]\nreply')

    def test_outbox_delivery_continues_without_a_source_client(self):
        self.sink.fail_flush = True
        self.engine.poll()
        self.assertEqual(len(self.sink.records), 2)
        self.assertEqual(self.sink.delivered, [])
        self.sink.fail_flush = False
        self.engine.source = None
        result = self.engine.retry_delivery()
        self.assertEqual(result["delivered_sessions"], 1)
        self.assertEqual(len(self.sink.delivered), 2)
        self.assertEqual(self.source.read_calls, 1)
        self.assertEqual(self.engine.status()["threads"][0]["phase"], "catching_up")

    def test_persisted_sink_session_without_source_ledger_is_drained(self):
        self.sink.ensure_session("env:disappeared", str(self.repo))
        self.sink.enqueue("env:disappeared", "message:complete", {"sessionUpdate": "agent_message", "messageId": "synthetic", "content": [{"type": "text", "text": "completed before source disappeared"}]})
        self.engine.source = None
        self.engine.retry_delivery()
        self.assertEqual(len(self.sink.delivered), 1)
        self.assertEqual(self.engine.status()["threads"], [])
        self.assertEqual(self.engine.status()["problems"][0]["phase"], "catching_up")

    def test_partial_attachment_recovers_while_source_unavailable(self):
        self.sink.fail = True
        self.engine.poll()
        self.assertEqual(len(self.engine.status()["threads"]), 1)
        self.assertIsNone(self.engine.status()["threads"][0]["attached_at"])
        self.assertIsNone(self.sink.sessions()[0]["reference"])
        self.sink.fail = False
        self.engine.source = None
        result = self.engine.retry_delivery()
        self.assertEqual(result["delivered_sessions"], 1)
        self.assertEqual(self.sink.sessions()[0]["reference"], "checkout-reference")
        self.assertEqual(self.engine.status()["threads"][0]["attached_at"], NOW)
        self.assertEqual(self.source.read_calls, 0)
        self.assertEqual(self.engine.status()["problems"][0]["phase"], "catching_up")

    def test_initial_failure_retains_original_inode_and_rejects_reused_path(self):
        original = self.repo.stat()
        original_fingerprint = f"{original.st_dev}:{original.st_ino}"
        self.sink.fail = True
        self.engine.poll()
        self.assertEqual(self.engine.db.execute("SELECT fingerprint FROM threads WHERE id='new'").fetchone()[0], original_fingerprint)
        self.assertIsNone(self.engine.status()["threads"][0]["attached_at"])
        # Retain the old inode so the filesystem cannot reuse it for the replacement.
        self.repo.rename(self.repo.with_name("original-repo"))
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "remote", "add", "origin", REMOTE], check=True)
        replacement = self.repo.stat()
        self.assertNotEqual(f"{replacement.st_dev}:{replacement.st_ino}", original_fingerprint)
        self.sink.fail = False
        self.engine.source = None
        restarted = Engine(self.config, self.state, None, self.sink, clock=lambda: NOW)
        try:
            result = restarted.retry_delivery()
            self.assertEqual(result["halted"], 1)
            self.assertIsNone(self.sink.sessions()[0]["reference"])
            self.assertEqual(self.sink.bindings, [])
            self.assertEqual(restarted.db.execute("SELECT fingerprint FROM threads WHERE id='new'").fetchone()[0], original_fingerprint)
            self.assertEqual(restarted.status()["threads"][0]["phase"], "needs_review")
            self.assertIsNone(restarted.status()["threads"][0]["attached_at"])
        finally:
            restarted.close()

    def test_orphan_incomplete_sink_is_held_without_reconstructing_source_binding(self):
        self.sink.fail = True
        with self.assertRaises(DeltaUnavailable):
            self.sink.ensure_session("env:orphan", str(self.repo))
        self.sink.fail = False
        self.engine.source = None
        result = self.engine.retry_delivery()
        self.assertEqual(result["halted"], 1)
        self.assertIsNone(self.sink.sessions()[0]["reference"])
        self.assertEqual(self.sink.bindings, [])
        self.assertEqual(self.sink.flush_calls, [])
        self.assertEqual(self.engine.status()["threads"], [])
        self.assertEqual(self.engine.status()["problems"][0]["phase"], "needs_review")

    def test_source_discovery_cannot_launder_an_orphan_sink_binding(self):
        self.sink.fail = True
        with self.assertRaises(DeltaUnavailable):
            self.sink.ensure_session("env:new", str(self.repo))
        self.sink.fail = False
        self.engine.poll()
        self.assertEqual(self.engine.status()["threads"], [])
        self.assertEqual(self.engine.status()["health"]["phase"], "needs_review")
        self.assertEqual(self.sink.bindings, [])
        self.assertIsNone(self.sink.sessions()[0]["reference"])
        self.assertEqual(self.source.read_calls, 0)

    def test_missing_project_marks_binding_missing_and_zero_match_is_persisted(self):
        self.sink.fail_flush = True
        self.engine.poll()
        self.sink.fail_flush = False
        self.source.projects = lambda: []
        self.engine.retry_delivery()
        self.engine.poll()
        status = self.engine.status()
        self.assertEqual(len(self.sink.delivered), 2)
        self.assertEqual(status["threads"][0]["phase"], "source_missing")
        self.assertEqual(status["health"]["phase"], "awaiting_project")
        self.assertEqual(status["health"]["matched_projects"], 0)
        restarted = Engine(self.config, self.state, None, self.sink, clock=lambda: NOW)
        try:
            self.assertEqual(restarted.status()["health"]["phase"], "awaiting_project")
            self.assertEqual(restarted.status()["health"]["matched_projects"], 0)
        finally:
            restarted.close()

    def test_missing_thread_retains_outbox_and_stops_claiming_recording(self):
        self.engine.poll()
        self.source.threads = lambda _: []
        self.engine.poll()
        status = self.engine.status()
        self.assertEqual(status["threads"][0]["phase"], "source_missing")
        self.assertEqual(status["health"]["phase"], "catching_up")
        self.assertEqual(status["health"]["matched_projects"], 1)
        self.assertEqual(len(self.sink.records), 2)

    def test_queued_later_user_cannot_jump_over_unexported_active_assistant(self):
        self.source.thread.update(status="running", activeRunId="run")
        self.source.items += [dict(self.source.message("later", "user_message", "queued prompt"), runId="next-run")]
        self.engine.poll()
        self.assertEqual(list(self.sink.records), [("env:new", "message:u")])
        self.source.thread.update(status="completed", activeRunId=None)
        self.engine.poll()
        self.assertEqual([key[1] for key, _ in self.sink.delivered], ["message:u", "message:a", "message:later"])

    def test_nonterminal_assistant_blocks_later_user_even_without_live_thread_status(self):
        self.source.items[1]["status"] = "running"
        self.source.items.append(self.source.message("later", "user_message", "queued"))
        self.engine.poll()
        self.assertEqual(list(self.sink.records), [("env:new", "message:u")])

    def test_review_hold_never_captures_more_but_can_drain_existing_immutable_records(self):
        self.sink.fail_flush = True
        self.engine.poll()
        self.engine._thread_error("new", "immutable history conflict")
        self.sink.fail_flush = False
        self.engine.retry_delivery()
        self.source.items.append(self.source.message("later", "user_message", "must not capture"))
        reads = self.source.read_calls
        self.engine.poll()
        self.assertEqual(len(self.sink.delivered), 2)
        self.assertEqual(self.source.read_calls, reads)
        self.assertEqual(len(self.sink.records), 2)
        self.assertEqual(self.engine.status()["threads"][0]["phase"], "needs_review")

    def test_halted_sink_never_delivers_or_captures_and_hold_is_sticky(self):
        self.engine.poll()
        self.sink.saved_sessions["env:new"]["halted"] = 1
        self.source.items.append(self.source.message("later", "user_message", "must not capture"))
        flushes, reads = len(self.sink.flush_calls), self.source.read_calls
        self.engine.retry_delivery()
        self.engine.poll()
        self.assertEqual(len(self.sink.flush_calls), flushes)
        self.assertEqual(self.source.read_calls, reads)
        self.assertEqual(len(self.sink.records), 2)
        self.assertEqual(self.engine.status()["threads"][0]["phase"], "halted")
        self.source.threads = lambda _: []
        self.engine.poll()
        self.assertEqual(self.engine.status()["threads"][0]["phase"], "halted")

    def test_review_hold_does_not_repair_attachment_and_begin_new_file_capture(self):
        self.sink.fail = True
        self.engine.poll()
        self.engine._thread_error("new", "boundary requires review")
        self.sink.fail = False
        self.engine.retry_delivery()
        self.assertIsNone(self.sink.sessions()[0]["reference"])
        self.assertEqual(self.sink.bindings, [])

    def test_observed_turn_labels_use_matching_run_and_remain_immutable(self):
        self.source.thread.update(model="wrong-current-model", providerInstanceId="wrong-current-provider")
        self.source.recent_runs = [{"runId": "run", "providerInstanceId": "claude-instance", "model": "observed-model"}]
        self.engine.poll()
        text = self.sink.records[("env:new", "message:a")]["content"][0]["text"]
        self.assertEqual(text, '[T3 turn {"model": "observed-model", "providerInstanceId": "claude-instance", "runId": "run"}]\nreply')
        self.assertEqual(self.sink.records[("env:new", "message:u")]["content"][0]["text"], "prompt")
        self.source.recent_runs = []  # Older runs can fall outside the MCP's 50-run window.
        self.engine.poll()
        self.assertEqual(self.engine.status()["problems"], [])
        self.assertEqual(self.sink.records[("env:new", "message:a")]["content"][0]["text"], text)
        self.assertEqual(self.engine.status()["capture"], "completed_messages_and_plans_with_observed_turn_labels")

    def test_missing_run_metadata_has_no_current_thread_provider_fallback(self):
        self.source.thread.update(model="unrelated-model", providerInstanceId="unrelated-provider")
        self.engine.poll()
        text = self.sink.records[("env:new", "message:a")]["content"][0]["text"]
        self.assertEqual(text, '[T3 turn {"runId": "run"}]\nreply')

    def test_post_enqueue_crash_replays_first_observed_label_without_exporting_early(self):
        self.source.recent_runs = [{"runId": "run", "providerInstanceId": "observed-provider", "model": "observed-model"}]
        enqueue = self.sink.enqueue
        crashed = False
        def persist_then_crash(external_id, key, update):
            nonlocal crashed
            enqueue(external_id, key, update)
            if key == "message:a" and not crashed:
                crashed = True
                raise TimeoutError("synthetic crash after durable sink enqueue")
        self.sink.enqueue = persist_then_crash
        self.engine.poll()
        original = copy.deepcopy(self.sink.records[("env:new", "message:a")])
        self.assertIsNone(self.engine.db.execute("SELECT hash FROM exported WHERE thread_id='new' AND item_id='a'").fetchone())
        label = self.engine.db.execute("SELECT label FROM observed_labels WHERE thread_id='new' AND item_id='a'").fetchone()[0]
        self.assertIn("observed-model", label)
        self.source.recent_runs = []
        restarted = Engine(self.config, self.state, self.source, self.sink, clock=lambda: NOW)
        try:
            restarted.poll()
            self.assertEqual(restarted.status()["problems"], [])
            self.assertEqual(self.sink.records[("env:new", "message:a")], original)
            self.assertEqual(len(self.sink.records), 2)
            self.assertEqual(len(self.sink.delivered), 2)
            self.assertIsNotNone(restarted.db.execute("SELECT hash FROM exported WHERE thread_id='new' AND item_id='a'").fetchone())
        finally:
            restarted.close()

    def test_transient_failure_recovers_with_prospective_binding_persisted(self):
        self.sink.fail = True
        self.engine.poll()
        self.assertEqual(self.engine.status()["health"]["phase"], "catching_up")
        self.sink.fail = False
        self.engine.poll()
        self.assertEqual(self.engine.status()["problems"], [])
        self.assertEqual(len(self.sink.records), 2)

    def test_changed_checkout_is_held(self):
        self.engine.poll()
        self.source.thread["worktreePath"] = str(self.repo.parent)
        self.engine.poll()
        self.assertEqual(self.engine.status()["health"]["phase"], "needs_review")

    def test_sensitive_unignored_filename_blocks_import(self):
        (self.repo / ".env").write_text("SYNTHETIC=not-a-secret\n")
        self.engine.poll()
        self.assertEqual(self.sink.bindings, [])
        self.assertEqual(self.engine.status()["health"]["phase"], "needs_review")

    def test_ignored_environment_file_is_not_enrolled(self):
        (self.repo / ".gitignore").write_text(".env\n")
        (self.repo / ".env").write_text("SYNTHETIC=not-a-secret\n")
        self.engine.poll()
        self.assertEqual(len(self.sink.records), 2)

    def test_environment_identity_must_match(self):
        self.config["environment_id"] = "wrong"
        with self.assertRaises(ValueError):
            self.engine.poll()
        self.assertEqual(self.sink.bindings, [])

    def test_status_detects_stale_service_and_writer_lock_is_exclusive(self):
        self.engine.poll()
        self.assertEqual(read_status(self.state, self.config)["health"]["phase"], "stale")
        with writer_lock(self.state):
            with self.assertRaises(BlockingIOError):
                with writer_lock(self.state):
                    self.fail("second writer acquired lock")
        self.assertEqual(os.stat(self.state).st_mode & 0o777, 0o700)
        self.assertEqual(os.stat(self.state / "source.sqlite3").st_mode & 0o777, 0o600)
