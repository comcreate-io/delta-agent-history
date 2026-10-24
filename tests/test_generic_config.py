import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest

from delta_history.configuration import initialize, inspect_scope, load


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.repo = self.root / "repo"
        subprocess.run(["git", "init", "--quiet", str(self.repo)], check=True)
        subprocess.run(["git", "-C", str(self.repo), "remote", "add", "origin", "https://example.com/team/project.git"], check=True)
        self.binary = self.root / "zed-delta"
        self.binary.write_text("#!/bin/sh\nexit 99\n")
        self.binary.chmod(0o700)
        self.path = self.root / "config/config.json"

    def init(self, **kw):
        return initialize(self.path, **{"projects": [self.repo], "sources": ["custom:desktop"], "delta_binary": str(self.binary), "accept_upload": True, **kw})

    def test_neutral_offline_configuration_and_private_creation(self):
        result = self.init()
        config = load(self.path)
        self.assertEqual(config["schema_version"], 2)
        self.assertNotIn("environment_id", config)
        self.assertNotIn("t3_runtime_file", config)
        self.assertEqual(config["sources"], [{"kind": "custom", "instance": "desktop"}])
        self.assertFalse(result["recording_started"])
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o600)
        self.assertEqual(inspect_scope(config, self.repo), (str(self.repo), str(self.repo)))
        with self.assertRaises(ValueError):
            self.init()

    def test_requires_consent_sources_and_canonical_checkout(self):
        with self.assertRaises(ValueError):
            self.init(accept_upload=False)
        for source in ([], ["Bad:desktop"], ["custom:desktop", "custom:desktop"], ["custom:a:b"]):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.init(sources=source)
        alias = self.root / "alias"
        alias.symlink_to(self.repo)
        with self.assertRaises(ValueError):
            self.init(projects=[alias])
        self.assertFalse(self.path.exists())

    def test_scope_refuses_unapproved_same_remote_clone_and_symlink(self):
        self.init()
        config = load(self.path)
        other = self.root / "other"
        subprocess.run(["git", "init", "--quiet", str(other)], check=True)
        subprocess.run(["git", "-C", str(other), "remote", "add", "origin", "https://example.com/team/project.git"], check=True)
        with self.assertRaises(ValueError):
            inspect_scope(config, other)
        alias = self.root / "alias"
        alias.symlink_to(self.repo)
        with self.assertRaises(ValueError):
            inspect_scope(config, alias)

    def test_legacy_link_explicit_existing_and_never_read_contents(self):
        old = self.root / "legacy.json"
        old.write_text("not JSON, not read during setup")
        state = self.root / "oldstate"
        state.mkdir()
        self.init(legacy_t3={"config": str(old), "state": str(state)})
        self.assertEqual(load(self.path)["legacy_t3"], {"config": str(old), "state": str(state)})
