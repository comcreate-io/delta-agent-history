import contextlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from t3_delta.cli import history, read_status, load_config, main, run
from t3_delta.delta_sink import DeltaSink
from t3_delta.engine import Engine


class ConfigTests(unittest.TestCase):
    def test_nonproject_configuration_change_exits_for_supervisor_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            config = {"environment_id": "env", "delta_binary": "old"}
            with patch("t3_delta.cli.DeltaSink"), patch("t3_delta.cli.Engine") as engine, patch(
                    "t3_delta.cli.load_config", return_value={**config, "delta_binary": "new"}):
                self.assertEqual(run(config, Path(directory), False, Path(directory)/"config.json"), 3)
                engine.return_value.poll.assert_not_called()
                engine.return_value.close.assert_called_once()

    def test_remote_only_configuration_is_rejected_before_authentication(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            path.write_text(json.dumps({"environment_id": "env", "allowed_remotes": ["https://github.com/example/project.git"]}))
            with self.assertRaises(ValueError):
                load_config(path)
            with patch('t3_delta.cli.issue_credential') as issue:
                self.assertEqual(main(['--config', str(path), 'auth']), 2)
                issue.assert_not_called()

    def test_empty_or_nonlist_scope_and_relative_root_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.json"
            base = {"environment_id": "env", "allowed_remotes": ["https://github.com/example/project.git"], "projects_root": directory, "allowed_projects": [{"path": directory}]}
            for change in ({"allowed_projects": []}, {"allowed_projects": "all"}, {"projects_root": "relative"}):
                path.write_text(json.dumps({**base, **change}))
                with self.assertRaises(ValueError):
                    load_config(path)


class StatusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.state = Path(self.temp.name)
        self.config = {'environment_id': 'env', 'allowed_remotes': ['https://github.com/example/repo.git']}
        engine = Engine(self.config, self.state, None, None)
        engine.set_health('watching')
        engine.close()
        sink = DeltaSink(self.state)
        with sink.db:
            sink.db.execute("INSERT INTO sessions(external_id,directory,thread_id,visibility,upload,access) VALUES(?,?,?,?,?,?)", ('env:thread','/tmp/synthetic','delta-thread','private',json.dumps({'state':'uploaded','error':None}),json.dumps({'state':'granted'})))
        sink.close()

    def test_upload_failure_and_unverified_visibility_cannot_report_watching(self):
        self.assertEqual(read_status(self.state,self.config)['health']['phase'],'watching')
        with contextlib.closing(sqlite3.connect(self.state/'sink.sqlite3')) as db, db:
            db.execute('UPDATE sessions SET upload=?',(json.dumps({'state':'failed','error':'synthetic'}),))
        self.assertEqual(read_status(self.state,self.config)['health']['phase'],'needs_review')
        with contextlib.closing(sqlite3.connect(self.state/'sink.sqlite3')) as db, db:
            db.execute('UPDATE sessions SET visibility=NULL,upload=?',(json.dumps({'state':'queued','error':None}),))
        self.assertEqual(read_status(self.state,self.config)['health']['phase'],'catching_up')

    def test_small_transcript_clamps_requested_range(self):
        with contextlib.closing(sqlite3.connect(self.state/'source.sqlite3')) as db, db:
            db.execute("INSERT INTO threads VALUES('thread','env:thread','/tmp/synthetic','now','recording',NULL,'now','fingerprint')")
        with contextlib.closing(sqlite3.connect(self.state/'sink.sqlite3')) as db, db:
            for sequence in (1,2):
                db.execute("INSERT INTO records(external_id,sequence,item_key,body,update_json,acknowledged,disposition) VALUES(?,?,?,?,?,1,'recorded')", ('env:thread',sequence,str(sequence),'{}',json.dumps({'sessionUpdate':'user_message' if sequence==1 else 'agent_message'})))
        with patch('t3_delta.cli.subprocess.run') as run:
            run.return_value.returncode=0
            run.return_value.stdout='[]'
            self.assertEqual(history(self.state,self.config,'thread','1-20'),[])
            self.assertEqual(run.call_args.args[0][-1],'1-2')
        with patch('t3_delta.cli.subprocess.run') as run:
            result=history(self.state,self.config,'thread','3-20')
            self.assertEqual(result['messages'],[])
            run.assert_not_called()

    def test_progress_preserves_completed_timestamp_and_still_expires(self):
        current = ['2026-10-23T12:00:00+00:00']
        engine = Engine(self.config, self.state, None, None, clock=lambda: current[0])
        self.addCleanup(engine.close)
        engine.set_health('watching')
        current[0] = '2026-10-23T12:01:30+00:00'
        engine.set_progress('source_capture')
        with patch('t3_delta.cli.utc_now', return_value=current[0]):
            status = read_status(self.state, self.config)
        self.assertEqual(status['health']['phase'], 'catching_up')
        self.assertEqual(status['health']['checked_at'], '2026-10-23T12:00:00+00:00')
        self.assertEqual(status['progress'], {'stage':'source_capture', 'updated_at':current[0]})
        with patch('t3_delta.cli.utc_now', return_value='2026-10-23T12:02:31+00:00'):
            self.assertEqual(read_status(self.state, self.config)['health']['phase'], 'stale')

    def test_progress_cannot_hide_source_or_cloud_failure(self):
        current = ['2026-10-23T12:00:00+00:00']
        engine = Engine(self.config, self.state, None, None, clock=lambda: current[0])
        self.addCleanup(engine.close)
        engine.set_health('unavailable', 'T3HTTPError')
        current[0] = '2026-10-23T12:01:30+00:00'
        engine.set_progress('retry_delivery')
        with patch('t3_delta.cli.utc_now', return_value=current[0]):
            health = read_status(self.state, self.config)['health']
        self.assertEqual(health['phase'], 'unavailable')
        self.assertEqual(health['error'], 'T3HTTPError')
        current[0] = '2026-10-23T12:00:00+00:00'
        engine.set_health('watching')
        current[0] = '2026-10-23T12:01:30+00:00'
        engine.set_progress('source_capture')
        with contextlib.closing(sqlite3.connect(self.state/'sink.sqlite3')) as db, db:
            db.execute('UPDATE sessions SET upload=?', (json.dumps({'state':'failed','error':'synthetic'}),))
        with patch('t3_delta.cli.utc_now', return_value=current[0]):
            self.assertEqual(read_status(self.state, self.config)['health']['phase'], 'needs_review')

    def test_status_without_progress_keeps_existing_stale_detection(self):
        engine = Engine(self.config, self.state, None, None,
                        clock=lambda: '2026-10-23T12:00:00+00:00')
        self.addCleanup(engine.close)
        engine.set_health('watching')
        with patch('t3_delta.cli.utc_now', return_value='2026-10-23T12:01:01+00:00'):
            self.assertEqual(read_status(self.state, self.config)['health']['phase'], 'stale')
