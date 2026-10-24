import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from delta_history import legacy
from t3_delta.engine import Engine
from t3_delta.delta_sink import DeltaSink


class LegacyRecallTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repo = self.root / "project"
        self.repo.mkdir()
        subprocess.run(["git", "init", "-q", str(self.repo)], check=True)
        self.remote = "https://github.com/example/project.git"
        subprocess.run(["git", "-C", str(self.repo), "remote", "add", "origin", self.remote], check=True)
        rule = {"path": str(self.repo), "remote": self.remote, "enrolled_after": "2026-01-01T00:00:00Z"}
        self.old = {"environment_id": "synthetic", "allowed_remotes": [self.remote],
                    "allowed_projects": [rule], "projects_root": str(self.root)}
        self.state = self.root / "old-state"
        engine = Engine(self.old, self.state, None, None)
        with engine.db:
            engine.db.execute("INSERT INTO threads VALUES(?,?,?,?,?,?,?,?)", (
                "thread", "synthetic:thread", str(self.repo), "2026-01-02T00:00:00Z", "recording", None,
                "2026-01-02T00:00:00Z", "synthetic-fingerprint"))
        engine.close()
        sink = DeltaSink(self.state)
        with sink.db:
            sink.db.execute("INSERT INTO sessions(external_id,directory,thread_id,visibility,upload,access) VALUES(?,?,?,?,?,?)", (
                "synthetic:thread", str(self.repo), "delta-thread", "private",
                json.dumps({"state": "uploaded", "error": None}), json.dumps({"state": "granted"})))
            for sequence, role in ((1, "user_message"), (2, "agent_message")):
                update = json.dumps({"sessionUpdate": role})
                sink.db.execute("INSERT INTO records(external_id,sequence,item_key,body,update_json,acknowledged,disposition) VALUES(?,?,?,?,?,1,'recorded')", (
                    "synthetic:thread", sequence, str(sequence), "frozen-synthetic-body", update))
            sink.db.execute("INSERT INTO records(external_id,sequence,item_key,body,update_json,acknowledged) VALUES(?,?,?,?,?,0)", (
                "synthetic:thread", 3, "pending", "frozen-pending-body", json.dumps({"sessionUpdate": "agent_message"})))
        sink.close()
        self.path = self.root / "legacy.json"
        self.path.write_text(json.dumps(self.old))
        self.config = {**self.old, "legacy_t3": {"config": str(self.path), "state": str(self.state)}}

    def test_readback_keeps_original_ledger_and_pending_bytes_unchanged(self):
        before = {name: (self.state / name).read_bytes() for name in ("source.sqlite3", "sink.sqlite3")}
        rows = legacy.sessions(self.config, self.repo)
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0]["key"].startswith("legacy-t3:"))
        with patch("delta_history.legacy.t3_history", return_value=[]) as read:
            self.assertEqual(legacy.history(self.config, self.repo, rows[0]["key"]), [])
            read.assert_called_once_with(self.state, self.old, "thread", "1-20")
        self.assertEqual(before, {name: (self.state / name).read_bytes() for name in before})

    def test_readback_requires_both_scopes_and_explicit_link(self):
        self.assertEqual(legacy.sessions(self.old, self.repo), [])
        with self.assertRaises(ValueError):
            legacy.sessions({**self.config, "allowed_projects": []}, self.repo)
        self.path.write_text(json.dumps({**self.old, "allowed_projects": []}))
        with self.assertRaises(ValueError):
            legacy.sessions(self.config, self.repo)

    def test_unavailable_link_and_unknown_session_fail_without_mutation(self):
        with self.assertRaises(ValueError):
            legacy.history(self.config, self.repo, "legacy-t3:missing")
        self.path.unlink()
        with self.assertRaises(ValueError):
            legacy.sessions(self.config, self.repo)
