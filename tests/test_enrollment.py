import contextlib
import fcntl
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from delta_history.cli import main
from delta_history.configuration import initialize, load
from delta_history.enrollment import BEGIN, END, enroll


class EnrollmentTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.first = self.repository("projects/first")
        self.new = self.repository("projects/new")
        self.config = self.root / "config/config.json"
        self.binary = self.root / "zed-delta"
        self.binary.write_text("#!/bin/sh\nexit 99\n")
        self.binary.chmod(0o700)
        initialize(self.config, projects=[self.first], sources=["custom:desktop"],
                   delta_binary=str(self.binary), accept_upload=True)

    def repository(self, name):
        path = self.root / name
        subprocess.run(["git", "init", "-q", str(path)], check=True)
        subprocess.run(["git", "-C", str(path), "remote", "add", "origin",
                        "https://example.com/team/" + path.name + ".git"], check=True)
        return path

    def test_additive_enrollment_and_idempotent_instructions(self):
        old = load(self.config)
        old["poll_seconds"] = 17
        self.config.write_text(json.dumps(old))
        original = b"# Existing project rules\r\n\r\nPreserve my conventions.\r\n"
        (self.new / "AGENTS.md").write_bytes(original)
        result = enroll(self.config, self.new, accept_upload=True)
        actual = load(self.config)
        self.assertEqual(actual["allowed_projects"][:1], old["allowed_projects"])
        for key in ("sources", "projects_root", "delta_binary", "poll_seconds"):
            self.assertEqual(actual[key], old[key])
        self.assertEqual(actual["allowed_projects"][-1]["path"], str(self.new))
        self.assertFalse(result["drainer_reload_required"])
        self.assertTrue(result["workers_reload_on_next_cycle"])
        self.assertFalse(result["recording_started"])
        self.assertTrue((self.new / "AGENTS.md").read_bytes().startswith(original))
        for name in ("AGENTS.md", "CLAUDE.md"):
            self.assertEqual((self.new / name).read_text().count(BEGIN), 1)
        snapshots = {p: p.read_bytes() for p in (self.config, self.new / "AGENTS.md", self.new / "CLAUDE.md")}
        repeated = enroll(self.config, self.new)
        self.assertTrue(repeated["already_enrolled"])
        self.assertFalse(repeated["config_changed"])
        self.assertEqual(repeated["instruction_files_updated"], [])
        self.assertEqual({p: p.read_bytes() for p in snapshots}, snapshots)
        self.assertEqual(stat.S_IMODE(self.config.stat().st_mode), 0o600)

    def test_check_and_missing_consent_have_no_scope_or_instruction_writes(self):
        before = self.config.read_bytes()
        self.assertFalse(enroll(self.config, self.new, check=True)["enrolled"])
        self.assertTrue(enroll(self.config, self.first, check=True)["enrolled"])
        self.assertFalse(self.config.with_name("config.json.enroll.lock").exists())
        with self.assertRaises(ValueError):
            enroll(self.config, self.new)
        self.assertEqual(self.config.read_bytes(), before)
        self.assertFalse((self.new / "AGENTS.md").exists())

    def test_outside_root_is_exactly_enrolled_without_broadening_root(self):
        outside = self.repository("elsewhere/selected")
        original = load(self.config)
        enroll(self.config, outside, accept_upload=True)
        actual = load(self.config)
        self.assertEqual(actual["projects_root"], original["projects_root"])
        self.assertEqual(actual["additional_project_paths"], [str(outside)])
        self.assertEqual(len(actual["allowed_projects"]), 2)

    def test_sensitive_file_origin_change_and_symlinks_are_refused(self):
        before = self.config.read_bytes()
        (self.new / ".env").write_text("synthetic")
        with self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        (self.new / ".env").unlink()
        alias = self.root / "alias"
        alias.symlink_to(self.new, target_is_directory=True)
        with self.assertRaises(ValueError):
            enroll(self.config, alias, accept_upload=True)
        subprocess.run(["git", "-C", str(self.first), "remote", "set-url", "origin",
                        "https://example.com/changed.git"], check=True)
        with self.assertRaises(ValueError):
            enroll(self.config, self.first, accept_upload=True)
        self.assertEqual(self.config.read_bytes(), before)

    def test_preflight_rejects_instruction_symlink_fifo_and_bad_markers(self):
        before = self.config.read_bytes()
        agents = self.new / "AGENTS.md"
        agents.write_text("existing instructions")
        claude = self.new / "CLAUDE.md"
        outside = self.root / "outside.md"
        outside.write_text("preserve")
        claude.symlink_to(outside)
        with self.assertRaises(OSError):
            enroll(self.config, self.new, accept_upload=True)
        self.assertEqual(agents.read_text(), "existing instructions")
        self.assertEqual(outside.read_text(), "preserve")
        claude.unlink()
        os.mkfifo(claude)
        with self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        claude.unlink()
        claude.write_text(BEGIN)
        with self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        self.assertEqual(self.config.read_bytes(), before)
        self.assertEqual(agents.read_text(), "existing instructions")

    def test_managed_config_is_readonly_and_lowercase_files_are_preserved(self):
        alias = self.root / "managed.json"
        alias.symlink_to(self.config)
        self.assertTrue(enroll(alias, self.first, check=True)["enrolled"])
        with self.assertRaises(OSError):
            enroll(alias, self.new, accept_upload=True)
        (self.new / "agents.md").write_text("lowercase rules")
        (self.new / "claude.md").write_text("lowercase Claude rules")
        enroll(self.config, self.new, accept_upload=True)
        self.assertNotIn("AGENTS.md", os.listdir(self.new))
        self.assertTrue((self.new / "agents.md").read_text().startswith("lowercase rules"))

    def test_existing_block_updates_only_managed_text(self):
        text = "prefix\n" + BEGIN + "\nold content\n" + END + "\nsuffix\n"
        (self.new / "AGENTS.md").write_text(text)
        enroll(self.config, self.new, accept_upload=True)
        actual = (self.new / "AGENTS.md").read_text()
        self.assertTrue(actual.startswith("prefix\n" + BEGIN))
        self.assertTrue(actual.endswith(END + "\nsuffix\n"))
        self.assertNotIn("old content", actual)

    def test_instruction_growth_limit_preserves_existing_files_and_config(self):
        from delta_history.enrollment import MAX_FILE_BYTES
        before = self.config.read_bytes()
        agents = self.new / "AGENTS.md"
        agents.write_bytes(b"x" * (MAX_FILE_BYTES - 1))
        with self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        self.assertEqual(agents.stat().st_size, MAX_FILE_BYTES - 1)
        self.assertEqual(self.config.read_bytes(), before)
        self.assertFalse((self.new / "CLAUDE.md").exists())

    def test_locked_enrollment_and_concurrent_edit_never_clobber_config(self):
        before = self.config.read_bytes()
        with open(self.config.with_name("config.json.enroll.lock"), "w") as lock:
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(ValueError):
                enroll(self.config, self.new, accept_upload=True)
        from delta_history.enrollment import _replace
        def edited(path, snapshot, data, **kwargs):
            if path == self.config:
                self.config.write_bytes(before + b"\n")
            return _replace(path, snapshot, data, **kwargs)
        with patch("delta_history.enrollment._replace", side_effect=edited), self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        self.assertEqual(self.config.read_bytes(), before + b"\n")
        self.assertEqual(len(load(self.config)["allowed_projects"]), 1)

    def test_cli_check_exit_and_enrollment_dispatch(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(["--config", str(self.config), "enroll", "--project", str(self.new), "--check"]), 2)
        self.assertFalse(json.loads(output.getvalue())["enrolled"])
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(["--config", str(self.config), "enroll", "--project", str(self.new), "--accept-upload"]), 0)
        self.assertTrue(json.loads(output.getvalue())["enrolled"])

    def test_cli_reports_consent_requirement_without_exposing_config(self):
        with contextlib.redirect_stderr(io.StringIO()) as error:
            self.assertEqual(main(["--config", str(self.config), "enroll", "--project", str(self.new)]), 2)
        self.assertIn("--accept-upload", json.loads(error.getvalue())["message"])
        self.assertNotIn(str(self.new), error.getvalue())
        self.assertNotIn(str(self.binary), error.getvalue())

    def test_linked_worktree_reuses_enrollment_but_changed_origin_is_refused(self):
        subprocess.run(["git", "-C", str(self.first), "-c", "user.name=Fixture",
                        "-c", "user.email=fixture@example.invalid", "-c", "core.hooksPath=/dev/null",
                        "commit", "--allow-empty", "--no-gpg-sign", "-qm", "fixture"], check=True)
        linked = self.root / "linked"
        subprocess.run(["git", "-C", str(self.first), "worktree", "add", "--detach", "-q", str(linked)], check=True)
        before = self.config.read_bytes()
        self.assertTrue(enroll(self.config, linked)["already_enrolled"])
        self.assertEqual(self.config.read_bytes(), before)
        self.assertTrue((linked / "AGENTS.md").is_file())
        subprocess.run(["git", "-C", str(self.first), "remote", "set-url", "origin",
                        "https://example.com/changed.git"], check=True)
        with self.assertRaises(ValueError):
            enroll(self.config, linked, accept_upload=True)
        self.assertEqual(self.config.read_bytes(), before)

    def test_readonly_managed_check_and_nonregular_config_do_not_block(self):
        self.config.chmod(0o444)
        self.assertTrue(enroll(self.config, self.first, check=True)["enrolled"])
        with self.assertRaises(ValueError):
            enroll(self.config, self.new, accept_upload=True)
        pipe = self.root / "config-pipe"
        os.mkfifo(pipe)
        with self.assertRaises(ValueError):
            enroll(pipe, self.first, check=True)


if __name__ == "__main__":
    unittest.main()
