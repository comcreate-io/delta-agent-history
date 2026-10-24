import json
from pathlib import Path
import stat
import sqlite3
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from t3_delta.delta_sink import DeltaConflict, DeltaError, DeltaSink, DeltaUnavailable, delivery_health, projected_message_count


class FakeDelta:
    def __init__(self, directory):
        self.directory = str(directory)
        self.calls = []
        self.frontier = 1
        self.behavior = "normal"
        self.contract = {"operation": "contract", "min": 1, "max": 7}
        self.visibility = "private"
        self.receipt_transform = lambda value: value
        self.error = {"code": "app_unavailable", "message": "synthetic private diagnostic", "retryable": True}

    def status(self):
        return {
            "operation": "status", "thread_id": "thread-1", "visibility": self.visibility,
            "upload": {"state": "queued", "organization_id": None, "error": None},
            "access": {"state": "granted", "is_owner": True},
            "worktrees": {"status": "available", "items": [{
                "reference": "history#t=timeline", "checkouts": [{
                    "directory": self.directory, "state": "attached", "active": True,
                }],
            }]},
            "producer": {"source": "t3-code", "external_id": "env:thread", "next_sequence": self.frontier},
        }

    def run(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        assert argv[:2] == ["fake-delta", "--json"]
        args = argv[2:]
        assert kwargs["timeout"] == 0.5
        if args == ["thread", "contract"]:
            value = self.contract
        elif args == ["org", "list"]:
            value = {"organizations": [{"id": "private-upload-org", "name": "Synthetic", "current": True}]}
        elif args[:2] == ["thread", "create"]:
            assert args[args.index("--source") + 1] == "t3-code"
            assert args[args.index("--visibility") + 1] == "private"
            assert args[args.index("--organization") + 1] == "private-upload-org"
            value = {"operation": "created", "thread_id": "thread-1"}
        elif args[:2] == ["worktree", "attach"]:
            value = {"operation": "mounted", "reference": "history#t=timeline", "directory": self.directory}
        elif args[:2] == ["thread", "status"]:
            value = self.status()
        elif args[:2] == ["thread", "show"]:
            assert "--messages" in args
            value = {"messages": [{"text": "synthetic"}]}
        elif args[:2] == ["thread", "append"]:
            assert args[args.index("--acknowledge") + 1] == "server"
            records = [json.loads(line) for line in kwargs["input"].splitlines()]
            answers = []
            for index, record in enumerate(records):
                if self.behavior == "error_first" or (self.behavior == "error_prefix" and index == 1):
                    answers.append(json.dumps({"error": self.error}))
                    break
                duplicate = record["sequence"] < self.frontier
                self.frontier = max(self.frontier, record["sequence"] + 1)
                receipt = {
                    "operation": "appended", "thread_id": "thread-1", "sequence": record["sequence"],
                    "next_sequence": self.frontier, "duplicate": duplicate,
                    "disposition": "duplicate" if duplicate else "recorded",
                    "turn_sequence": record.get("turnSequence"), "version": "actor.42",
                }
                if self.behavior == "fenced":
                    receipt.update(disposition="fenced", duplicate=False)
                answers.append(json.dumps(self.receipt_transform(receipt)))
                if self.behavior == "partial":
                    break
            if self.behavior == "lost":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            return subprocess.CompletedProcess(argv, 1 if self.behavior in {"partial", "error_first", "error_prefix"} else 0, "\n".join(answers) + "\n", "")
        else:
            raise AssertionError(args)
        return subprocess.CompletedProcess(argv, 0, json.dumps(value), "")


class DeltaSinkTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.checkout = self.root / "checkout"
        self.checkout.mkdir()
        self.fake = FakeDelta(self.checkout)
        self.patch = patch("t3_delta.delta_sink.subprocess.run", side_effect=self.fake.run)
        self.patch.start()
        self.sink = DeltaSink(self.root / "state", "fake-delta", timeout=0.5)

    def tearDown(self):
        self.sink.close()
        self.patch.stop()
        self.temp.cleanup()

    def bind(self):
        return self.sink.ensure_session("env:thread", str(self.checkout))

    def message(self, key="m1", text="synthetic"):
        return self.sink.enqueue("env:thread", key, {
            "sessionUpdate": "agent_message", "messageId": key,
            "content": [{"type": "text", "text": text}],
        })

    def test_private_binding_restart_permissions_and_history(self):
        binding = self.bind()
        self.assertEqual(binding["thread_id"], "thread-1")
        self.assertEqual(binding["directory"], str(self.checkout))
        self.assertEqual(binding["organization"], "private-upload-org")
        self.assertEqual(binding["checkout_status"]["state"], "attached")
        self.assertIsNotNone(binding["checked_at"])
        self.assertEqual(stat.S_IMODE((self.root / "state").stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE((self.root / "state/sink.sqlite3").stat().st_mode), 0o600)
        self.message()
        self.sink.close()
        self.sink = DeltaSink(self.root / "state", "fake-delta", timeout=0.5)
        self.bind()
        self.assertEqual(sum(args[2:4] == ["thread", "create"] for args, _ in self.fake.calls), 1)
        self.assertEqual(self.sink.status("env:thread")["pending"], 1)
        self.sink.flush("env:thread")
        self.assertEqual(self.sink.history("env:thread"), {"messages": [{"text": "synthetic"}]})
        self.assertEqual(self.fake.calls[-1][0][-2:], ["--messages", "1-1"])

    def test_success_and_idempotent_key(self):
        self.bind()
        self.assertEqual(self.message(), 1)
        self.assertEqual(self.message(), 1)
        status = self.sink.flush("env:thread")
        self.assertEqual(status["pending"], 0)
        self.assertEqual(status["acknowledged"], 1)
        self.assertEqual(status["upload"]["state"], "queued")
        self.assertEqual(self.message(), 1)

    def test_changed_key_halts_without_reusing_sequence(self):
        self.bind()
        self.message()
        with self.assertRaises(DeltaConflict):
            self.message(text="changed")
        self.assertEqual(self.sink.status("env:thread")["next_sequence"], 2)
        self.assertTrue(self.sink.status("env:thread")["halted"])

    def test_partial_receipt_preserves_pending_tail(self):
        self.bind()
        self.message("m1")
        self.message("m2")
        self.fake.behavior = "partial"
        with self.assertRaises(DeltaUnavailable):
            self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertEqual((status["acknowledged"], status["pending"]), (1, 1))
        versions = self.sink.db.execute("SELECT receipt_version FROM records ORDER BY sequence").fetchall()
        self.assertEqual([row[0] for row in versions], ["actor.42", None])
        self.fake.behavior = "normal"
        self.assertEqual(self.sink.flush("env:thread")["pending"], 0)

    def test_lost_response_replays_identical_payload_after_restart(self):
        self.bind()
        self.message()
        self.fake.behavior = "lost"
        with self.assertRaises(DeltaUnavailable):
            self.sink.flush("env:thread")
        original = next(kwargs["input"] for args, kwargs in self.fake.calls if args[2:4] == ["thread", "append"])
        self.assertEqual(self.sink.status("env:thread")["pending"], 1)
        self.sink.close()
        self.sink = DeltaSink(self.root / "state", "fake-delta", timeout=0.5)
        self.fake.behavior = "normal"
        self.assertEqual(self.sink.flush("env:thread")["pending"], 0)
        replay = [kwargs["input"] for args, kwargs in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.assertEqual(original, replay)
        self.assertEqual(self.sink.db.execute("SELECT disposition FROM records").fetchone()[0], "duplicate")
        self.assertEqual(self.sink.db.execute("SELECT receipt_version FROM records").fetchone()[0], "actor.42")

    def test_retryable_append_error_first_line_preserves_identical_batch(self):
        self.bind()
        self.message("m1")
        self.message("m2")
        self.fake.behavior = "error_first"
        with self.assertRaises(DeltaUnavailable) as raised:
            self.sink.flush("env:thread")
        self.assertNotIn(self.fake.error["message"], str(raised.exception))
        status = self.sink.status("env:thread")
        self.assertEqual((status["pending"], status["acknowledged"], status["halted"]), (2, 0, False))
        original = [kw["input"] for args, kw in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.fake.behavior = "normal"
        self.assertEqual(self.sink.flush("env:thread")["pending"], 0)
        replay = [kw["input"] for args, kw in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.assertEqual(original, replay)

    def test_retryable_append_error_after_prefix_keeps_ack_and_replays_tail(self):
        self.bind()
        self.message("m1")
        self.message("m2")
        self.fake.behavior = "error_prefix"
        with self.assertRaises(DeltaUnavailable):
            self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertEqual((status["pending"], status["acknowledged"], status["halted"]), (1, 1, False))
        versions = self.sink.db.execute("SELECT receipt_version FROM records ORDER BY sequence").fetchall()
        self.assertEqual([row[0] for row in versions], ["actor.42", None])
        original = [kw["input"] for args, kw in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.fake.behavior = "normal"
        self.assertEqual(self.sink.flush("env:thread")["pending"], 0)
        replay = [kw["input"] for args, kw in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.assertEqual(original.splitlines(keepends=True)[1], replay)

    def test_append_nonretryable_unknown_and_malformed_errors_halt(self):
        errors = [
            {"code": "invalid_record", "message": "private", "retryable": False},
            {"code": "not_shared", "reason": "not_shared", "message": "share this thread or record an upload intent before appending records", "retryable": False},
            {"code": "app_unavailable", "message": "private"},
            {"code": "app_unavailable", "message": "private", "retryable": "true"},
            {"code": "future_unknown", "message": "private", "retryable": True},
            {"code": "app_unavailable", "retryable": True},
            {"code": "app_unavailable", "message": "private", "retryable": True, "reason": False},
            "app_unavailable",
        ]
        self.bind()
        self.message()
        for error in errors:
            with self.subTest(error=error):
                self.fake.behavior = "error_first"
                self.fake.error = error
                with self.assertRaises(DeltaConflict):
                    self.sink.flush("env:thread")
                self.assertTrue(self.sink.status("env:thread")["halted"])
                self.assertEqual(self.sink.status("env:thread")["pending"], 1)
                with self.sink.db:
                    self.sink.db.execute("UPDATE sessions SET halted=0,error=NULL")

    def test_command_retryable_error_even_exit_zero_is_transient(self):
        for returncode in (0, 1):
            for command in (["thread", "status", "synthetic"], ["thread", "create"]):
                with self.subTest(returncode=returncode, command=command):
                    result = subprocess.CompletedProcess(command, returncode, json.dumps({"error": self.fake.error}), "private stderr")
                    with patch("t3_delta.delta_sink.subprocess.run", return_value=result):
                        with self.assertRaises(DeltaUnavailable) as raised:
                            self.sink._command(command)
                    self.assertNotIn("private", str(raised.exception))

    def test_command_nonretryable_or_malformed_error_requires_review(self):
        for error in (
            {"code": "invalid_record", "message": "private", "retryable": False},
            {"code": "app_unavailable", "message": "private"},
            {"code": "future_unknown", "message": "private", "retryable": True},
            {"retryable": True},
        ):
            result = subprocess.CompletedProcess([], 0, json.dumps({"error": error}), "private stderr")
            with patch("t3_delta.delta_sink.subprocess.run", return_value=result):
                with self.assertRaises(DeltaConflict):
                    self.sink._command(["thread", "status", "synthetic"])

    def test_nonretryable_append_error_keeps_verified_prefix_before_halt(self):
        self.bind()
        self.message("m1")
        self.message("m2")
        self.fake.behavior = "error_prefix"
        self.fake.error = {"code": "invalid_record", "message": "private", "retryable": False}
        with self.assertRaises(DeltaConflict):
            self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertEqual((status["pending"], status["acknowledged"], status["halted"]), (1, 1, True))

    def test_create_and_status_transient_errors_do_not_halt_binding(self):
        for failed_command in (["thread", "create"], ["thread", "status"]):
            with self.subTest(failed_command=failed_command):
                def fail_selected(argv, **kwargs):
                    if argv[2:4] == failed_command:
                        return subprocess.CompletedProcess(argv, 1, json.dumps({"error": self.fake.error}), "private stderr")
                    return self.fake.run(argv, **kwargs)
                with patch("t3_delta.delta_sink.subprocess.run", side_effect=fail_selected):
                    with self.assertRaises(DeltaUnavailable):
                        self.bind()
                self.assertFalse(self.sink.status("env:thread")["halted"])
        self.bind()
        self.assertEqual(self.sink.status("env:thread")["thread_id"], "thread-1")

    def test_history_empty_and_out_of_range_make_no_read_call(self):
        self.bind()
        count = len(self.fake.calls)
        self.assertEqual(self.sink.history("env:thread"), [])
        self.assertEqual(len(self.fake.calls), count)
        self.message()
        self.sink.flush("env:thread")
        count = len(self.fake.calls)
        self.assertEqual(self.sink.history("env:thread", 2, 21), [])
        self.assertEqual(len(self.fake.calls), count)
        with self.assertRaises(DeltaError):
            self.sink.history("env:thread", True, 20)

    def test_history_excludes_state_records_and_limits_window_to_twenty(self):
        self.bind()
        start = self.sink.enqueue("env:thread", "start", {"sessionUpdate": "state_update", "state": "running"})
        for index in range(23):
            self.sink.enqueue("env:thread", f"m{index}", {
                "sessionUpdate": "user_message" if index % 2 == 0 else "agent_message",
                "messageId": f"m{index}", "content": [{"type": "text", "text": "synthetic"}],
            })
        self.sink.enqueue("env:thread", "idle", {"sessionUpdate": "state_update", "state": "idle"}, start)
        self.sink.flush("env:thread")
        self.sink.history("env:thread", 1, 99)
        self.assertEqual(self.fake.calls[-1][0][-2:], ["--messages", "1-20"])
        self.sink.history("env:thread", 21, 99)
        self.assertEqual(self.fake.calls[-1][0][-2:], ["--messages", "21-23"])

    def test_consecutive_assistant_records_coalesce_in_metadata_and_history(self):
        self.bind()
        self.sink.enqueue("env:thread", "user", {
            "sessionUpdate": "user_message", "messageId": "user", "content": [{"type": "text", "text": "prompt"}],
        })
        self.message("assistant1")
        self.message("assistant2")
        self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertEqual(status["acknowledged"], 3)
        self.assertEqual(status["message_count"], 2)
        self.sink.history("env:thread")
        self.assertEqual(self.fake.calls[-1][0][-2:], ["--messages", "1-2"])

    def test_projected_message_count_ignores_nonmessage_updates(self):
        self.assertEqual(projected_message_count([]), 0)
        self.assertEqual(projected_message_count([
            {"sessionUpdate": "user_message"},
            {"sessionUpdate": "user_message"},
            {"sessionUpdate": "state_update"},
            {"sessionUpdate": "agent_message"},
            {"sessionUpdate": "tool_call_update"},
            {"sessionUpdate": "agent_message"},
            {"sessionUpdate": "user_message"},
        ]), 4)

    def test_each_user_message_stays_distinct_after_assistant_coalescing(self):
        self.bind()
        for index, role in enumerate(("user_message", "agent_message", "agent_message", "user_message", "user_message")):
            self.sink.enqueue("env:thread", f"m{index}", {
                "sessionUpdate": role, "messageId": f"m{index}",
                "content": [{"type": "text", "text": f"synthetic {index}"}],
            })
        self.sink.flush("env:thread")
        self.assertEqual(self.sink.status("env:thread")["message_count"], 4)
        self.sink.history("env:thread", 4, 20)
        self.assertEqual(self.fake.calls[-1][0][-2:], ["--messages", "4-4"])

    def test_existing_database_receipt_column_migrates_without_fabricated_version(self):
        self.bind()
        self.message()
        self.sink.close()
        database = self.root / "state/sink.sqlite3"
        db = sqlite3.connect(database)
        try:
            with db:
                db.execute("ALTER TABLE records DROP COLUMN receipt_version")
        finally:
            db.close()
        self.sink = DeltaSink(self.root / "state", "fake-delta", timeout=0.5)
        row = self.sink.db.execute("SELECT receipt_version,acknowledged FROM records").fetchone()
        self.assertEqual(tuple(row), (None, 0))
        self.sink.flush("env:thread")
        self.assertEqual(self.sink.db.execute("SELECT receipt_version FROM records").fetchone()[0], "actor.42")

    def test_cursor_ahead_or_behind_stops_before_append(self):
        for frontier in (0, 3):
            with self.subTest(frontier=frontier):
                self.bind()
                self.message()
                self.fake.frontier = frontier
                with self.assertRaises(DeltaConflict):
                    self.sink.flush("env:thread")
                self.assertFalse(any(args[2:4] == ["thread", "append"] for args, _ in self.fake.calls))
                self.fake.frontier = 1
                with self.sink.db:
                    self.sink.db.execute("UPDATE sessions SET halted=0,error=NULL")

    def test_lost_acknowledged_history_stops(self):
        self.bind()
        self.message()
        self.sink.flush("env:thread")
        self.message("m2")
        self.fake.frontier = 1
        with self.assertRaises(DeltaConflict):
            self.sink.flush("env:thread")
        self.assertEqual(self.sink.status("env:thread")["pending"], 1)

    def test_fenced_stops_and_preserves_remaining_records(self):
        self.bind()
        turn = self.sink.enqueue("env:thread", "start", {"sessionUpdate": "state_update", "state": "running"})
        self.assertEqual(turn, 1)
        self.sink.enqueue("env:thread", "end", {"sessionUpdate": "state_update", "state": "idle"}, turn)
        self.fake.behavior = "fenced"
        with self.assertRaises(DeltaConflict):
            self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertTrue(status["halted"])
        self.assertEqual(status["acknowledged"], 0)
        self.assertEqual(status["pending"], 1)
        with self.assertRaises(DeltaConflict):
            self.sink.flush("env:thread")

    def test_receipt_wrong_turn_or_thread_or_sequence_never_acks(self):
        for field, value in (("thread_id", "wrong"), ("sequence", True), ("turn_sequence", 1), ("next_sequence", 90), ("duplicate", True)):
            with self.subTest(field=field):
                self.bind()
                self.message()
                self.fake.receipt_transform = lambda r, f=field, v=value: {**r, f: v}
                with self.assertRaises(DeltaConflict):
                    self.sink.flush("env:thread")
                self.assertEqual(self.sink.status("env:thread")["pending"], 1)
                self.fake.frontier = 1
                with self.sink.db:
                    self.sink.db.execute("UPDATE sessions SET halted=0,error=NULL")

    def test_directory_change_and_wrong_remote_checkout_halt(self):
        self.bind()
        other = self.root / "other"
        other.mkdir()
        with self.assertRaises(DeltaConflict):
            self.sink.ensure_session("env:thread", str(other))
        with self.sink.db:
            self.sink.db.execute("UPDATE sessions SET halted=0,error=NULL")
        self.fake.directory = str(other)
        with self.assertRaises(DeltaConflict):
            self.sink.flush("env:thread")

    def test_unknown_contract_or_nonprivate_visibility_refused(self):
        self.fake.contract = {"operation": "contract", "min": 8, "max": 8}
        with self.assertRaises(DeltaConflict):
            self.bind()
        self.assertFalse(any(args[2:4] == ["thread", "create"] for args, _ in self.fake.calls))
        self.fake.contract = {"operation": "contract", "min": 1, "max": 7}
        self.fake.visibility = "organization"
        with self.assertRaises(DeltaConflict):
            self.bind()

    def test_reused_producer_with_missing_ledger_refuses_attach(self):
        self.fake.frontier = 2
        with self.assertRaises(DeltaConflict):
            self.bind()
        self.assertFalse(any(args[2:4] == ["worktree", "attach"] for args, _ in self.fake.calls))

    def test_status_and_sessions_make_no_delta_calls(self):
        self.bind()
        count = len(self.fake.calls)
        self.sink.status()
        self.sink.status("env:thread")
        self.sink.sessions()
        self.assertEqual(count, len(self.fake.calls))

    def test_future_contract_with_overlapping_versions_stops_before_create(self):
        self.fake.contract = {"operation": "contract", "min": 1, "max": 8}
        with self.assertRaises(DeltaConflict):
            self.bind()
        self.assertFalse(any(args[2:4] == ["thread", "create"] for args, _ in self.fake.calls))

    def test_explicit_local_only_null_visibility_can_attach_but_is_catching_up(self):
        self.fake.visibility = None
        original = self.fake.status

        def local_status():
            status = original()
            status["upload"] = None
            status["access"] = {"state": "local_only", "is_owner": None}
            return status

        with patch.object(self.fake, "status", side_effect=local_status):
            binding = self.bind()
        self.assertIsNone(binding["visibility"])
        self.assertEqual(delivery_health(binding), "catching_up")

    def test_initial_cloud_binding_wait_preserves_queue_until_ready(self):
        original_status = self.fake.status

        def initializing_status():
            status = original_status()
            status["visibility"] = None
            status["upload"] = None
            status["access"] = {"state": "local_only", "is_owner": None}
            return status

        with patch.object(self.fake, "status", side_effect=initializing_status):
            self.bind()
            self.message("m1")
            self.message("m2")
            queued = "".join(row[0] for row in self.sink.db.execute("SELECT body FROM records ORDER BY sequence"))
            with self.assertRaisesRegex(DeltaUnavailable, "Private cloud binding is not ready"):
                self.sink.flush("env:thread")
            self.assertFalse(any(args[2:4] == ["thread", "append"] for args, _ in self.fake.calls))
            status = self.sink.status("env:thread")
            self.assertEqual((status["pending"], status["acknowledged"], status["halted"]), (2, 0, False))
            self.assertEqual(delivery_health(status), "catching_up")
        self.assertEqual(self.sink.flush("env:thread")["pending"], 0)
        sent = [kw["input"] for args, kw in self.fake.calls if args[2:4] == ["thread", "append"]][-1]
        self.assertEqual(sent, queued)

    def test_private_visibility_alone_does_not_enable_server_append(self):
        self.bind()
        self.message()
        original_status = self.fake.status
        for unready in ("local_only", "no_upload"):
            with self.subTest(unready=unready):
                def incomplete_status():
                    status = original_status()
                    if unready == "local_only":
                        status["access"] = {"state": "local_only", "is_owner": None}
                    else:
                        status["upload"] = None
                    return status
                with patch.object(self.fake, "status", side_effect=incomplete_status):
                    with self.assertRaises(DeltaUnavailable):
                        self.sink.flush("env:thread")
                self.assertFalse(self.sink.status("env:thread")["halted"])
                self.assertFalse(any(args[2:4] == ["thread", "append"] for args, _ in self.fake.calls))

    def test_failed_upload_reports_review_without_append(self):
        self.bind()
        self.message()
        original_status = self.fake.status

        def failed_upload():
            status = original_status()
            status["upload"] = {"state": "failed", "organization_id": None, "error": {"code": "failed", "message": "synthetic"}}
            return status

        with patch.object(self.fake, "status", side_effect=failed_upload):
            with self.assertRaises(DeltaError):
                self.sink.flush("env:thread")
        status = self.sink.status("env:thread")
        self.assertEqual(status["pending"], 1)
        self.assertEqual(delivery_health(status), "needs_review")
        self.assertFalse(any(args[2:4] == ["thread", "append"] for args, _ in self.fake.calls))

    def test_missing_visibility_or_granted_unknown_visibility_halts(self):
        original = self.fake.status
        for missing in (True, False):
            with self.subTest(missing=missing):
                def invalid_status():
                    status = original()
                    if missing:
                        del status["visibility"]
                    else:
                        status["visibility"] = None
                        status["access"] = {"state": "granted", "is_owner": True}
                    return status
                with patch.object(self.fake, "status", side_effect=invalid_status):
                    with self.assertRaises(DeltaConflict):
                        self.bind()
                with self.sink.db:
                    self.sink.db.execute("UPDATE sessions SET halted=0,error=NULL")

    def test_invalid_upload_state_halts(self):
        original = self.fake.status

        def invalid_status():
            status = original()
            status["upload"] = {"state": "invented", "organization_id": None, "error": None}
            return status

        with patch.object(self.fake, "status", side_effect=invalid_status):
            with self.assertRaises(DeltaConflict):
                self.bind()

    def test_all_remembered_status_updates_checked_at(self):
        self.bind()
        with patch("t3_delta.delta_sink.time.time", return_value=12345.0):
            self.sink._remember_status("env:thread", self.fake.status())
        self.assertEqual(self.sink.status("env:thread")["checked_at"], 12345.0)

    def test_delivery_health_requires_observed_private_ready_state(self):
        ready = {
            "visibility": "private", "pending": 0, "halted": False,
            "upload": {"state": "uploaded", "organization_id": None, "error": None},
            "access": {"state": "granted", "is_owner": True},
        }
        self.assertIsNone(delivery_health(ready))
        for change in (
            {"halted": True}, {"failed": True}, {"visibility": "organization"},
            {"visibility": "unlisted"}, {"visibility": "future"},
            {"upload": {"state": "failed", "error": None}},
            {"upload": {"state": "uploaded", "error": {"code": "failed", "message": "synthetic"}}},
        ):
            with self.subTest(change=change):
                self.assertEqual(delivery_health({**ready, **change}), "needs_review")
        for change in (
            {"pending": 1}, {"visibility": None}, {"upload": None},
            {"upload": {"state": "queued", "error": None}},
            {"upload": {"state": "uploading", "error": None}},
            {"upload": {"state": "future", "error": None}},
            {"access": {"state": "local_only", "is_owner": None}},
        ):
            with self.subTest(change=change):
                self.assertEqual(delivery_health({**ready, **change}), "catching_up")

    def test_invalid_turn_and_unsupported_reasoning_rejected(self):
        self.bind()
        with self.assertRaises(DeltaError):
            self.sink.enqueue("env:thread", "thought", {"sessionUpdate": "agent_thought", "content": []})
        with self.assertRaises(DeltaError):
            self.sink.enqueue("env:thread", "idle", {"sessionUpdate": "state_update", "state": "idle"})
        with self.assertRaises(DeltaError):
            self.sink.enqueue("env:thread", "bad", {"sessionUpdate": "state_update", "state": "idle"}, True)


if __name__ == "__main__":
    unittest.main()
