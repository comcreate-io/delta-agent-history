import contextlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from t3_delta.cli import main
from t3_delta.engine import Engine, remote_key
from t3_delta.setup import SetupError, initialize


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.project = self.repository("projects/app")
        self.binary = self.root / "fake-executable"
        self.binary.write_text("#!/bin/sh\nexit 99\n")
        self.binary.chmod(0o700)
        self.target = self.root / "config/recorder.json"
        self.kwargs = {"projects": [self.project], "environment_id": "synthetic-environment",
                       "runtime_file": self.root / "server-runtime.json", "t3_binary": str(self.binary),
                       "delta_binary": str(self.binary), "accept_upload": True}

    def repository(self, name, origin="https://github.com/example/app.git"):
        path = self.root / name
        path.mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(path)], check=True)
        if origin:
            subprocess.run(["git", "-C", str(path), "remote", "add", "origin", origin], check=True)
        return path

    def test_symlinked_repository_input_records_the_canonical_checkout(self):
        alias = self.root / "project-alias"
        alias.symlink_to(self.project, target_is_directory=True)
        initialize(self.target, **{**self.kwargs, "projects": [alias]})
        config = json.loads(self.target.read_text())
        self.assertEqual(config["allowed_projects"][0]["path"], str(self.project.resolve()))
        engine = Engine(config, self.root / "state", None, None)
        engine.close()

    def test_offline_explicit_config_is_engine_compatible_private_and_prospective(self):
        other = self.repository("other/another", "git@github.com:example/another.git")
        with patch("t3_delta.setup.utc_now", return_value="2026-01-01T00:00:00+00:00"):
            result = initialize(self.target, **{**self.kwargs, "projects": [self.project, other]})
        config = json.loads(self.target.read_text())
        self.assertFalse(result["recording_started"])
        self.assertFalse(result["credential_issued"])
        self.assertEqual(stat.S_IMODE(self.target.stat().st_mode), 0o600)
        self.assertEqual(stat.S_IMODE(self.target.parent.stat().st_mode), 0o700)
        self.assertEqual(config["additional_project_paths"], [str(other)])
        self.assertEqual(config["allowed_projects"][0]["enrolled_after"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(config["t3_binary"], str(self.binary))
        engine = Engine(config, self.root / "state", None, None)
        self.addCleanup(engine.close)
        self.assertIsNotNone(engine.project_cutoff(str(other), remote_key("git@github.com:example/another.git")))
        self.assertIsNone(engine.project_cutoff(str(self.project.parent / "unapproved"), remote_key("https://github.com/example/app.git")))
        self.assertFalse((self.root / "token.json").exists())

    def test_upload_consent_is_required_and_creates_nothing(self):
        with self.assertRaisesRegex(SetupError, "--accept-upload"):
            initialize(self.target, **{**self.kwargs, "accept_upload": False})
        self.assertFalse(self.target.parent.exists())

    def test_refuses_existing_config_and_dangling_symlink(self):
        self.target.parent.mkdir()
        self.target.write_text("preserve me")
        with self.assertRaisesRegex(SetupError, "already exists"):
            initialize(self.target, **self.kwargs)
        self.assertEqual(self.target.read_text(), "preserve me")
        self.target.unlink()
        victim = self.root / "uncreated"
        self.target.symlink_to(victim)
        with self.assertRaisesRegex(SetupError, "already exists"):
            initialize(self.target, **self.kwargs)
        self.assertFalse(victim.exists())

    def test_rejects_nonroot_missing_origin_and_credential_remote(self):
        child = self.project / "child"
        child.mkdir()
        missing = self.repository("missing", None)
        credential = self.repository("credential", "https://token@github.com/example/app.git")
        for directory in (child, missing, credential):
            with self.subTest(directory=directory), self.assertRaises(SetupError):
                initialize(self.target, **{**self.kwargs, "projects": [directory]})
        self.assertFalse(self.target.exists())

    def test_sensitive_filename_and_duplicate_root_are_rejected(self):
        (self.project / ".env").write_text("synthetic only")
        with self.assertRaises(SetupError):
            initialize(self.target, **self.kwargs)
        (self.project / ".env").unlink()
        with self.assertRaisesRegex(SetupError, "only once"):
            initialize(self.target, **{**self.kwargs, "projects": [self.project, self.project]})

    def test_ignored_file_is_outside_engine_filename_screen(self):
        (self.project / ".gitignore").write_text(".env\n")
        (self.project / ".env").write_text("synthetic only")
        initialize(self.target, **self.kwargs)
        self.assertTrue(self.target.is_file())

    def test_never_auto_selects_git_delta_and_missing_binary_does_not_create_config(self):
        kwargs = {**self.kwargs, "delta_binary": None}
        with patch("t3_delta.setup.shutil.which", return_value=None) as which:
            with self.assertRaisesRegex(SetupError, "zed-delta"):
                initialize(self.target, **kwargs)
            which.assert_called_once_with("zed-delta")
        self.assertFalse(self.target.exists())

    def test_cli_init_does_not_require_existing_config_or_authenticate(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = main(["--config", str(self.target), "init", "--project", str(self.project),
                           "--environment-id", "synthetic-environment", "--t3-runtime-file", str(self.root / "runtime.json"),
                           "--t3-binary", str(self.binary), "--delta-binary", str(self.binary),
                           "--t3-base-dir", str(self.root / "custom-base"), "--accept-upload"])
        self.assertEqual(result, 0)
        self.assertFalse(json.loads(output.getvalue())["recording_started"])
        self.assertEqual(json.loads(self.target.read_text())["t3_base_dir"], str(self.root / "custom-base"))

    def test_cli_feedback_is_actionable_without_remote_output(self):
        output = io.StringIO()
        with contextlib.redirect_stderr(output):
            result = main(["--config", str(self.target), "init", "--project", str(self.project),
                           "--environment-id", "synthetic", "--t3-runtime-file", str(self.root / "runtime.json")])
        self.assertEqual(result, 2)
        self.assertIn("--accept-upload", json.loads(output.getvalue())["message"])
        self.assertNotIn("https://", output.getvalue())
