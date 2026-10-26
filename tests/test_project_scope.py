import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from delta_history.configuration import initialize, load
from delta_history.enrollment import enroll
from delta_history.cli import drain_run
from t3_delta import cli as t3_cli
from t3_delta.project_scope import read_registry
from t3_delta.engine import Engine, utc_now


class ProjectScopeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repos = []
        for name in ("existing", "new"):
            repo = self.root / name
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            subprocess.run(["git", "-C", str(repo), "remote", "add", "origin", "https://example.com/"+name+".git"], check=True)
            self.repos.append(repo)
        binary = self.root / "zed-delta"
        binary.write_text("#!/bin/sh\nexit 99\n")
        binary.chmod(0o700)
        self.path = self.root / "generic.json"
        initialize(self.path, projects=[self.repos[0]], sources=["generic:local"], delta_binary=str(binary), accept_upload=True)
        base = load(self.path)
        self.registry = self.root / "private/projects.json"
        base["project_enrollment_file"] = str(self.registry)
        self.path.write_text(json.dumps(base))
        self.legacy = self.root / "t3.json"
        self.legacy.write_text(json.dumps({**base, "environment_id": "synthetic-environment"}))

    def test_managed_config_enrollment_is_shared_without_changing_base_or_cutoffs(self):
        before = self.path.read_bytes()
        original = load(self.path)["allowed_projects"][0]
        self.path.chmod(0o444)
        alias = self.root / "managed.json"
        alias.symlink_to(self.path)
        self.assertFalse(enroll(alias, self.repos[1], check=True)["enrolled"])
        self.assertFalse(self.registry.parent.exists())
        result = enroll(alias, self.repos[1], accept_upload=True)
        self.assertEqual(result["scope_storage"], "shared_registry")
        self.assertTrue(result["workers_reload_on_next_cycle"])
        self.assertFalse(result["drainer_reload_required"])
        self.assertEqual(self.path.read_bytes(), before)
        for config in (load(alias), t3_cli.load_config(self.legacy)):
            self.assertEqual(config["allowed_projects"][0], original)
            self.assertEqual(len(config["allowed_projects"]), 2)
            self.assertEqual(config["allowed_projects"][1], read_registry(self.registry)["projects"][0])
        recorded = self.registry.read_bytes()
        self.assertTrue(enroll(alias, self.repos[1])["already_enrolled"])
        self.assertEqual(self.registry.read_bytes(), recorded)

    def test_registry_refuses_symlinks_fifos_shared_permissions_and_base_conflicts(self):
        enroll(self.path, self.repos[1], accept_upload=True)
        original = self.registry.read_bytes()
        self.registry.chmod(0o644)
        with self.assertRaises(ValueError):
            load(self.path)
        self.registry.chmod(0o600)
        self.registry.chmod(0o700)
        with self.assertRaises(ValueError):
            enroll(self.path, self.repos[1])
        self.assertEqual(self.registry.read_bytes(), original)
        self.registry.chmod(0o600)
        value = {"schema_version": 1, "projects": [{**load(self.path)["allowed_projects"][0], "enrolled_after": "2000-01-01T00:00:00Z"}]}
        self.registry.write_text(json.dumps(value))
        for loader, path in ((load, self.path), (t3_cli.load_config, self.legacy)):
            with self.assertRaises(ValueError):
                loader(path)
        self.registry.unlink()
        target = self.root / "preserve.json"
        target.write_bytes(original)
        self.registry.symlink_to(target)
        with self.assertRaises((ValueError, OSError)):
            load(self.path)
        self.registry.unlink()
        os.mkfifo(self.registry, 0o600)
        with self.assertRaises(ValueError):
            load(self.path)
        self.assertEqual(target.read_bytes(), original)

    def test_generic_worker_reloads_registry_between_cycles(self):
        initial = load(self.path)
        observed = []
        def drain(config, state):
            observed.append(len(config["allowed_projects"]))
            return {}
        def pause(_):
            if len(observed) == 1:
                enroll(self.path, self.repos[1], accept_upload=True)
            else:
                raise KeyboardInterrupt
        with patch("delta_history.cli.drain_once", side_effect=drain), patch("delta_history.cli.time") as timer, contextlib.redirect_stdout(io.StringIO()):
            timer.sleep.side_effect = pause
            with self.assertRaises(KeyboardInterrupt):
                drain_run(initial, self.root / "unused", config_path=self.path)
        self.assertEqual(observed, [1, 2])

    def test_registry_growth_limit_rejects_before_instruction_or_scope_writes(self):
        from delta_history.enrollment import MAX_FILE_BYTES
        self.registry.parent.mkdir(mode=0o700)
        value = {"schema_version": 1, "projects": [{"path": str(self.root / "future"),
                 "remote": "https://example.com/", "enrolled_after": utc_now()}]}
        value["projects"][0]["remote"] += "x" * (MAX_FILE_BYTES - len(json.dumps(value, indent=2).encode()) - 64)
        self.registry.write_text(json.dumps(value, indent=2))
        self.registry.chmod(0o600)
        before = self.registry.read_bytes()
        load(self.path)
        with self.assertRaises(ValueError):
            enroll(self.path, self.repos[1], accept_upload=True)
        self.assertEqual(self.registry.read_bytes(), before)
        self.assertFalse((self.repos[1] / "AGENTS.md").exists())
        self.assertFalse((self.repos[1] / "CLAUDE.md").exists())
        load(self.path)

    def test_new_registry_project_is_captured_by_original_t3_engine(self):
        from test_engine import Source, Sink
        source, sink = Source(self.repos[1]), Sink()
        source.environment = lambda: {"environmentId": "synthetic-environment", "serverVersion": "0.1.0"}
        state = self.root / "original-t3-state"
        engine = Engine(t3_cli.load_config(self.legacy), state, source, sink)
        engine.poll()
        self.assertEqual(sink.bindings, [])
        engine.close()
        enroll(self.path, self.repos[1], accept_upload=True)
        source.thread["createdAt"] = utc_now()
        engine = Engine(t3_cli.load_config(self.legacy), state, source, sink)
        try:
            engine.poll()
            self.assertEqual(sink.bindings, [("synthetic-environment:new", str(self.repos[1]))])
            self.assertEqual(len(sink.delivered), 2)
            self.assertEqual(engine.status()["threads"][0]["phase"], "recording")
        finally:
            engine.close()

    def test_t3_worker_reloads_scope_without_replacing_sink_or_environment(self):
        initial = t3_cli.load_config(self.legacy)
        observed, created = [], []
        class Engine:
            def __init__(self, config, state, source, sink):
                self.config = config
                created.append(config["environment_id"])
            def retry_delivery(self): pass
            def poll(self): observed.append(len(self.config["allowed_projects"]))
            def close(self): pass
            def set_health(self, *args): raise AssertionError("Unexpected worker failure")
            def set_progress(self, *args): pass
        def pause(_):
            if len(observed) == 1:
                enroll(self.path, self.repos[1], accept_upload=True)
            else:
                raise KeyboardInterrupt
        with patch.object(t3_cli, "Engine", Engine), patch.object(t3_cli, "DeltaSink") as sink, patch.object(t3_cli, "T3Client"), patch.object(t3_cli, "endpoint", return_value="unused"), patch.object(t3_cli, "time") as timer:
            timer.sleep.side_effect = pause
            with self.assertRaises(KeyboardInterrupt):
                t3_cli.run(initial, self.root / "state", False, config_path=self.legacy)
        self.assertEqual(observed, [1, 2])
        self.assertEqual(created, ["synthetic-environment", "synthetic-environment"])
        self.assertEqual(sink.call_count, 1)
