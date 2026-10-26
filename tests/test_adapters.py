import concurrent.futures
import contextlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from delta_history.adapters.claude import claude_hook
from delta_history.adapters.codex import codex_run
from delta_history.adapters.common import AdapterError, adapter_health, read_payload, retry_pending
from delta_history.adapters.opencode import opencode_hook
from delta_history.core import submit


class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.project = self.root / "project"
        self.project.mkdir()
        subprocess.run(["git", "init", "-q", str(self.project)], check=True)
        self.remote = "https://github.com/example/synthetic.git"
        subprocess.run(["git", "-C", str(self.project), "remote", "add", "origin", self.remote], check=True)
        self.state = self.root / "state"
        self.config = {"schema_version":2, "sources":[{"kind":kind,"instance":"test"} for kind in ("claude-code","codex","opencode")],
            "projects_root":str(self.root), "additional_project_paths":[],
            "allowed_remotes":[self.remote], "allowed_projects":[{"path":str(self.project),"remote":self.remote,"enrolled_after":"2000-01-01T00:00:00Z"}],
            "delta_binary":str(Path(shutil.which("true")).resolve())}

    def hook(self, kind="UserPromptSubmit", prompt="p1", text="hello"):
        value = {"hook_event_name":kind, "session_id":"s1", "prompt_id":prompt, "cwd":str(self.project)}
        value["prompt" if kind == "UserPromptSubmit" else "last_assistant_message"] = text
        return io.StringIO(json.dumps(value))

    def records(self):
        with contextlib.closing(sqlite3.connect(self.state / "adapters.sqlite3")) as db:
            return [json.loads(row[0]) for row in db.execute("SELECT payload FROM adapter_events ORDER BY rowid")]

    def snapshot(self, messages):
        return io.StringIO(json.dumps({"session_id":"o1", "project":str(self.project), "messages":messages}))

    def fake_codex(self, events, exit_code=0, stderr_message=None, launch_file=None):
        executable = self.root / "codex-fake"
        executable.write_text("#!" + sys.executable + "\nimport json,sys\nassert sys.argv[1:3] == ['exec','--json']\nassert sys.argv[-1] == '-'\nassert '--dangerously-bypass-approvals-and-sandbox' not in sys.argv\nassert sys.stdin.read() == 'review this project'\n" +
            ("with open("+repr(str(launch_file))+",'a') as launches: launches.write('run\\n')\n" if launch_file else "") +
            ("print("+repr(stderr_message)+",file=sys.stderr)\n" if stderr_message else "") +
            "\n".join("print(" + repr(json.dumps(event)) + ")" for event in events) + f"\nsys.exit({exit_code})\n")
        executable.chmod(0o700)
        prompt = self.root / "prompt.txt"
        prompt.write_text("review this project")
        return executable,prompt

    def test_claude_duplicate_callback_preserves_frozen_bytes(self):
        claude_hook(self.config,self.state,"test",self.hook())
        first = self.records()
        claude_hook(self.config,self.state,"test",self.hook())
        self.assertEqual(first,self.records())
        self.assertEqual([1,2],[e["sequence"] for e in first])

    def test_same_prompt_text_in_distinct_turns_is_not_deduplicated(self):
        claude_hook(self.config,self.state,"test",self.hook())
        claude_hook(self.config,self.state,"test",self.hook("Stop",text="answer"))
        claude_hook(self.config,self.state,"test",self.hook(prompt="p2"))
        claude_hook(self.config,self.state,"test",self.hook("Stop",prompt="p2",text="answer"))
        self.assertEqual(["user","assistant","user","assistant"],[e["role"] for e in self.records()[1:]])

    def test_claude_missing_prompt_id_fails_closed(self):
        payload = json.loads(self.hook().read())
        del payload["prompt_id"]
        with self.assertRaises(AdapterError):
            claude_hook(self.config,self.state,"test",io.StringIO(json.dumps(payload)))
        self.assertFalse(self.state.exists())

    def test_claude_conflicting_callback_halts_adapter(self):
        claude_hook(self.config,self.state,"test",self.hook())
        with self.assertRaises(AdapterError):
            claude_hook(self.config,self.state,"test",self.hook(text="changed"))
        with contextlib.closing(sqlite3.connect(self.state / "adapters.sqlite3")) as db:
            self.assertEqual(1,db.execute("SELECT halted FROM adapter_sessions").fetchone()[0])
        self.assertEqual("hello",self.records()[-1]["text"])

    def test_stop_without_prompt_is_refused(self):
        with self.assertRaises(AdapterError):
            claude_hook(self.config,self.state,"test",self.hook("Stop",text="answer"))
        self.assertEqual([],self.records())

    def test_stop_failure_is_not_an_assistant_message(self):
        result = claude_hook(self.config,self.state,"test",self.hook("StopFailure",text="API ERROR SECRET"))
        self.assertTrue(result["ignored"])
        self.assertFalse(self.state.exists())

    def test_source_instance_is_checked_before_state_or_model(self):
        with self.assertRaises(AdapterError):
            claude_hook(self.config,self.state,"not-enrolled",self.hook())
        self.assertFalse(self.state.exists())

    def test_legacy_state_boundary_is_unchanged_before_adapter_journal_creation(self):
        self.state.mkdir(mode=0o750)
        legacy = self.state / "source.sqlite3"
        legacy.write_bytes(b"legacy sentinel bytes")
        legacy.chmod(0o640)
        before = (self.state.stat().st_mode,legacy.stat().st_mode,legacy.read_bytes())
        with self.assertRaises(ValueError):
            claude_hook(self.config,self.state,"test",self.hook())
        self.assertEqual(before,(self.state.stat().st_mode,legacy.stat().st_mode,legacy.read_bytes()))
        self.assertFalse((self.state / "adapters.sqlite3").exists())
        self.assertEqual([legacy],list(self.state.iterdir()))

    def test_adapter_health_is_readonly_and_project_scoped(self):
        self.assertEqual({"pending":0,"halted":0},adapter_health(self.state))
        self.assertFalse(self.state.exists())
        with patch("delta_history.adapters.common.submit_events",side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                claude_hook(self.config,self.state,"test",self.hook())
        self.assertEqual(2,adapter_health(self.state,self.project)["pending"])
        self.assertEqual(0,adapter_health(self.state,self.root / "unrelated")["pending"])

    def test_concurrent_hooks_allocate_one_monotonic_stream(self):
        def invoke(index):
            return claude_hook(self.config,self.state,"test",self.hook(prompt=f"p{index}"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            list(pool.map(invoke,range(10)))
        records = self.records()
        self.assertEqual(list(range(1,12)),[e["sequence"] for e in records])
        self.assertEqual(11,len({e["event_id"] for e in records}))

    def test_crash_after_local_submit_replays_identical_events(self):
        submitted = []
        def crashing(config,state,events):
            submitted.extend(events)
            submit(config,state,events)
            raise RuntimeError("simulated crash before delivered marker")
        with patch("delta_history.adapters.common.submit_events",side_effect=crashing):
            with self.assertRaises(RuntimeError):
                claude_hook(self.config,self.state,"test",self.hook())
        frozen = self.records()
        claude_hook(self.config,self.state,"test",self.hook())
        self.assertEqual(frozen,self.records())
        self.assertEqual(frozen,submitted)
        with contextlib.closing(sqlite3.connect(self.state / "ingress.sqlite3")) as db:
            self.assertEqual(2,db.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    def test_autonomous_drainer_replays_persisted_events_without_callback_or_model(self):
        with patch("delta_history.adapters.common.submit_events",side_effect=RuntimeError("crash before core submission")):
            with self.assertRaises(RuntimeError):
                claude_hook(self.config,self.state,"test",self.hook())
        frozen = self.records()
        self.assertFalse((self.state / "ingress.sqlite3").exists())
        with patch("delta_history.adapters.codex.codex_run",side_effect=AssertionError("must never run a model")):
            result = retry_pending(self.config,self.state)
        self.assertEqual(2,result["submitted"])
        self.assertEqual(frozen,self.records())
        self.assertEqual({"pending":0,"halted":0},adapter_health(self.state))
        with contextlib.closing(sqlite3.connect(self.state / "ingress.sqlite3")) as db:
            self.assertEqual(frozen,[json.loads(row[0]) for row in db.execute("SELECT body FROM events ORDER BY sequence")])
        self.assertEqual(0,retry_pending(self.config,self.state)["sessions"])

    def test_drainer_does_not_create_state_when_absent(self):
        self.assertEqual(0,retry_pending(self.config,self.state)["sessions"])
        self.assertFalse(self.state.exists())

    def test_drainer_preserves_holds_and_checks_current_source_scope(self):
        with patch("delta_history.adapters.common.submit_events",side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                claude_hook(self.config,self.state,"test",self.hook())
        self.config["sources"] = [{"kind":"codex","instance":"test"}]
        result = retry_pending(self.config,self.state)
        self.assertEqual(1,result["halted"])
        self.assertEqual(["AdapterError"],result["errors"])
        self.assertEqual(2,adapter_health(self.state)["pending"])
        self.config["sources"].append({"kind":"claude-code","instance":"test"})
        self.assertEqual(1,retry_pending(self.config,self.state)["halted"])
        self.assertFalse((self.state / "ingress.sqlite3").exists())

    def test_drainer_refuses_replaced_checkout_before_first_core_submission(self):
        with patch("delta_history.adapters.common.submit_events",side_effect=RuntimeError("crash")):
            with self.assertRaises(RuntimeError):
                claude_hook(self.config,self.state,"test",self.hook())
        self.project.rename(self.root / "original-project")
        self.project.mkdir()
        subprocess.run(["git","init","-q",str(self.project)],check=True)
        subprocess.run(["git","-C",str(self.project),"remote","add","origin",self.remote],check=True)
        result = retry_pending(self.config,self.state)
        self.assertEqual(1,result["halted"])
        self.assertFalse((self.state / "ingress.sqlite3").exists())

    def test_hooks_never_wait_for_delta_cloud(self):
        with patch("t3_delta.delta_sink.DeltaSink.__init__",side_effect=AssertionError("cloud forbidden")):
            self.assertTrue(claude_hook(self.config,self.state,"test",self.hook())["accepted"])

    def test_local_ingress_refusal_does_not_acknowledge_adapter_events(self):
        with patch("delta_history.adapters.common.submit_events",return_value={"accepted":0,"duplicates":0,"rejected":2,"halted":1}):
            with self.assertRaises(AdapterError):
                claude_hook(self.config,self.state,"test",self.hook())
        self.assertEqual({"pending":2,"halted":1},adapter_health(self.state))

    def test_hook_input_budget_and_duplicate_json_fields(self):
        with self.assertRaises(AdapterError):
            read_payload(io.StringIO('{"session_id":"a","session_id":"b"}'))
        with self.assertRaises(AdapterError):
            read_payload(io.StringIO("x" * (4 * 1024 * 1024 + 1)))

    def test_opencode_snapshot_duplicates_and_appends(self):
        messages = [{"id":"m1","role":"user","text":"hello"}]
        opencode_hook(self.config,self.state,"test",self.snapshot(messages))
        frozen = self.records()
        opencode_hook(self.config,self.state,"test",self.snapshot(messages))
        self.assertEqual(frozen,self.records())
        messages.append({"id":"m2","role":"assistant","text":"answer","model":"provider/model"})
        opencode_hook(self.config,self.state,"test",self.snapshot(messages))
        self.assertEqual("provider/model",self.records()[-1]["model"])

    def test_opencode_changed_or_truncated_prefix_fails_closed(self):
        messages = [{"id":"m1","role":"user","text":"hello"}]
        opencode_hook(self.config,self.state,"test",self.snapshot(messages))
        with self.assertRaises(AdapterError):
            opencode_hook(self.config,self.state,"test",self.snapshot([]))
        self.assertEqual(2,len(self.records()))

    def test_opencode_raw_tool_role_is_rejected(self):
        with self.assertRaises(AdapterError):
            opencode_hook(self.config,self.state,"test",self.snapshot([{"id":"m1","role":"tool","text":"secret"}]))

    def test_codex_fake_binary_captures_only_completed_visible_items(self):
        events = [{"type":"thread.started","thread_id":"upstream"},
            {"type":"item.completed","item":{"id":"r","type":"reasoning","text":"PRIVATE_REASONING"}},
            {"type":"item.completed","item":{"id":"t","type":"command_execution","aggregated_output":"PRIVATE_TOOL"}},
            {"type":"item.started","item":{"id":"a","type":"agent_message","text":"partial"}},
            {"type":"item.completed","item":{"id":"a","type":"agent_message","text":"visible"}},
            {"type":"item.completed","item":{"id":"plan","type":"todo_list","items":[{"text":"check code","completed":True}]}},
            {"type":"turn.completed"}]
        binary,prompt = self.fake_codex(events)
        out,err = io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            self.assertEqual(0,codex_run(self.config,self.state,"test",self.project,binary,prompt,"model"))
        records = self.records()
        self.assertEqual(["user","assistant","plan"],[e["role"] for e in records if e["type"] == "message.completed"])
        self.assertEqual("completed",records[-1]["status"])
        self.assertNotIn("PRIVATE",json.dumps(records)+out.getvalue()+err.getvalue())
        with contextlib.closing(sqlite3.connect(self.state / "adapters.sqlite3")) as db:
            self.assertEqual("upstream",json.loads(db.execute("SELECT metadata FROM adapter_sessions").fetchone()[0])["upstream_thread_id"])

    def test_codex_preserves_failed_process_exit_code(self):
        binary,prompt = self.fake_codex([{"type":"thread.started","thread_id":"upstream"},{"type":"turn.failed","error":{"message":"SECRET ERROR"}}],7)
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(7,codex_run(self.config,self.state,"test",self.project,binary,prompt))
        self.assertEqual("failed",self.records()[-1]["status"])
        self.assertNotIn("SECRET",json.dumps(self.records()))

    def test_codex_busy_ingress_preserves_answers_and_frozen_retry_without_model(self):
        events = [{"type":"thread.started","thread_id":"upstream"},
            {"type":"item.completed","item":{"id":"r","type":"reasoning","text":"PRIVATE_REASONING"}},
            {"type":"item.completed","item":{"id":"a","type":"agent_message","text":"first answer"}},
            {"type":"item.completed","item":{"id":"t","type":"command_execution","aggregated_output":"PRIVATE_TOOL"}},
            {"type":"item.completed","item":{"id":"b","type":"agent_message","text":"final answer"}},
            {"type":"turn.completed"}]
        launches = self.root / "model-launches"
        binary,prompt = self.fake_codex(events,launch_file=launches)
        contention = self.root / "contention.sqlite3"
        with contextlib.closing(sqlite3.connect(contention)) as owner, contextlib.closing(sqlite3.connect(contention,timeout=0)) as contender:
            owner.execute("CREATE TABLE held(value)")
            owner.commit()
            owner.execute("BEGIN IMMEDIATE")
            calls = 0
            def locked_submit(config,state,records):
                nonlocal calls
                calls += 1
                if calls > 1:
                    contender.execute("INSERT INTO held VALUES(1)")
                    self.fail("The fixture writer lock was not enforced")
                return submit(config,state,records)
            out,err = io.StringIO(),io.StringIO()
            with patch("delta_history.adapters.common.submit_events",side_effect=locked_submit), contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                self.assertEqual(0,codex_run(self.config,self.state,"test",self.project,binary,prompt))
        self.assertEqual("first answer\nfinal answer\n",out.getvalue())
        self.assertNotIn("PRIVATE",out.getvalue()+err.getvalue()+json.dumps(self.records()))
        frozen = self.records()
        self.assertEqual(["review this project","first answer","final answer"],[e["text"] for e in frozen if e["type"] == "message.completed"])
        self.assertEqual("completed",frozen[-1]["status"])
        self.assertEqual({"pending":3,"halted":0},adapter_health(self.state))
        with patch("delta_history.adapters.codex.codex_run",side_effect=AssertionError("replay must never launch a model")):
            result = retry_pending(self.config,self.state)
        self.assertEqual(3,result["submitted"])
        self.assertEqual(0,result["halted"])
        self.assertEqual(frozen,self.records())
        self.assertEqual({"pending":0,"halted":0},adapter_health(self.state))
        self.assertEqual("run\n",launches.read_text())
        with contextlib.closing(sqlite3.connect(self.state / "ingress.sqlite3")) as db:
            self.assertEqual(frozen,[json.loads(row[0]) for row in db.execute("SELECT body FROM events ORDER BY sequence")])

    def test_codex_conflicting_completion_still_halts_without_changed_or_later_text(self):
        events = [{"type":"thread.started","thread_id":"upstream"},
            {"type":"item.completed","item":{"id":"a","type":"agent_message","text":"original answer"}},
            {"type":"item.completed","item":{"id":"a","type":"agent_message","text":"changed answer"}},
            {"type":"item.completed","item":{"id":"b","type":"agent_message","text":"later answer"}},
            {"type":"turn.completed"}]
        binary,prompt = self.fake_codex(events)
        out,err = io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
            self.assertEqual(0,codex_run(self.config,self.state,"test",self.project,binary,prompt))
        self.assertEqual("original answer\n",out.getvalue())
        self.assertNotIn("changed answer",json.dumps(self.records()))
        self.assertNotIn("later answer",json.dumps(self.records()))
        self.assertEqual(1,adapter_health(self.state)["halted"])
        self.assertIn("incomplete",err.getvalue())

    def test_codex_partial_stream_is_not_completed(self):
        binary,prompt = self.fake_codex([{"type":"thread.started","thread_id":"upstream"},{"type":"item.started","item":{"id":"a","type":"agent_message","text":"unfinished"}}])
        with contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(0,codex_run(self.config,self.state,"test",self.project,binary,prompt))
        self.assertEqual("failed",self.records()[-1]["status"])
        self.assertNotIn("unfinished",json.dumps(self.records()))

    def test_codex_unenrolled_source_never_launches_binary(self):
        binary,prompt = self.fake_codex([])
        with patch("delta_history.adapters.codex.subprocess.Popen",side_effect=AssertionError("must not launch")):
            with self.assertRaises(AdapterError):
                codex_run(self.config,self.state,"other",self.project,binary,prompt)

    def test_codex_fifo_prompt_is_refused_without_blocking_or_model(self):
        binary,_ = self.fake_codex([])
        fifo = self.root / "prompt-fifo"
        os.mkfifo(fifo,0o600)
        with patch("delta_history.adapters.codex.subprocess.Popen",side_effect=AssertionError("must not launch")):
            with self.assertRaisesRegex(AdapterError,"regular file"):
                codex_run(self.config,self.state,"test",self.project,binary,fifo)
        self.assertFalse(self.state.exists())

    def test_codex_stderr_is_inherited_but_never_ingested(self):
        binary,prompt = self.fake_codex([{"type":"thread.started","thread_id":"upstream"},{"type":"turn.completed"}],stderr_message="synthetic upstream permission diagnostic")
        config = self.root / "config.json"
        config.write_text(json.dumps(self.config))
        result = subprocess.run([sys.executable,"-m","delta_history","--config",str(config),"--state-dir",str(self.state),"codex","--instance","test","--project",str(self.project),"--codex-binary",str(binary),"--prompt-file",str(prompt)],capture_output=True,text=True,timeout=10)
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertIn("synthetic upstream permission diagnostic",result.stderr)
        self.assertNotIn("diagnostic",json.dumps(self.records()))

    @unittest.skipUnless(shutil.which("node"),"Node needed for integration fixture")
    def test_opencode_plugin_filters_parts_and_stops_at_streaming_assistant(self):
        plugin = (Path(__file__).parent.parent / "integrations/opencode-plugin.mjs").resolve().as_uri()
        captured = self.root / "plugin-output.json"
        binary = self.root / "literal history '$()'"
        binary.write_text("#!"+sys.executable+"\nimport pathlib,sys\npathlib.Path("+repr(str(captured))+").write_text(sys.stdin.read())\nassert sys.argv[1:] == ['hook','opencode','--instance','test']\n")
        binary.chmod(0o700)
        script = "import { DeltaHistoryPlugin } from " + json.dumps(plugin) + ";\n" + "process.env.DELTA_HISTORY_EXECUTABLE=" + json.dumps(str(binary)) + ";\nprocess.env.DELTA_HISTORY_INSTANCE='test';\ndelete process.env.DELTA_HISTORY_CONFIG;\ndelete process.env.DELTA_HISTORY_STATE_DIR;\n" + """
          const text=(id,value,type='text')=>({id:'p'+id,sessionID:'s',messageID:id,type,text:value});
          const rows=[
            {info:{id:'u',sessionID:'s',role:'user'},parts:[text('u','hello'),text('u','secret','reasoning')]},
            {info:{id:'a',sessionID:'s',role:'assistant',time:{completed:1}},parts:[text('a','answer'),{...text('a','synthetic'),synthetic:true},text('a','tool','tool')]},
            {info:{id:'active',sessionID:'s',role:'assistant',time:{}},parts:[text('active','partial')]},
            {info:{id:'later',sessionID:'s',role:'user'},parts:[text('later','must wait')]}
          ];
          let reads=0;
          const plugin=await DeltaHistoryPlugin({directory:'/repo',client:{session:{messages:async()=>{reads++; return {data:rows};}}}});
          await plugin.event({event:{type:'session.idle',properties:{sessionID:'s'}}});
          if(reads!==0) throw new Error('Old session was read');
          await plugin.event({event:{type:'session.created',properties:{info:{id:'s',directory:'/repo'}}}});
          await plugin.event({event:{type:'session.idle',properties:{sessionID:'s'}}});
          if(reads!==1) throw new Error('New session was not read');
        """
        subprocess.run([shutil.which("node"),"--input-type=module","-e",script],check=True,capture_output=True,timeout=10)
        self.assertEqual({"session_id":"s","project":"/repo","messages":[{"id":"u","role":"user","text":"hello"},{"id":"a","role":"assistant","text":"answer"}]},json.loads(captured.read_text()))


if __name__ == "__main__":
    unittest.main()
