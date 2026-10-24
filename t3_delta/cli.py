import argparse
import contextlib
import fcntl
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlsplit

from .delta_sink import DeltaSink, delivery_health, projected_message_count
from .engine import Engine, timestamp, utc_now
from .t3_source import T3Client
from .setup import SetupError, initialize


def private_directory(path):
    path = Path(path)
    if path.is_symlink():
        raise ValueError("State directory must not be a symlink")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.stat().st_uid != os.getuid():
        raise ValueError("State directory belongs to another user")
    path.chmod(0o700)
    return path


@contextlib.contextmanager
def writer_lock(state):
    path = private_directory(state) / "recorder.lock"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def load_config(path):
    config = json.loads(Path(path).read_text())
    for key in ("environment_id", "allowed_remotes"):
        if not config.get(key):
            raise ValueError("Recorder configuration is incomplete")
    if (not isinstance(config.get("allowed_projects"), list)
            or not config["allowed_projects"]
            or not isinstance(config.get("projects_root"), str)
            or not Path(config["projects_root"]).is_absolute()):
        raise ValueError("Exact project paths and a projects root are required; use init to enroll repositories")
    seconds = config.get("poll_seconds", 10)
    if type(seconds) is not int or not 5 <= seconds <= 300:
        raise ValueError("Poll interval must be 5 through 300 seconds")
    return config


def endpoint(config):
    value = config.get("endpoint")
    if not value:
        runtime = json.loads(Path(config["t3_runtime_file"]).expanduser().read_text())
        value = runtime["origin"].rstrip("/") + "/mcp"
    parsed = urlsplit(value)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("Endpoint must not contain credentials")
    return value


def issue_credential(config, state):
    """Issue a dedicated supported read-only MCP session; never echo its token."""
    state = private_directory(state)
    with writer_lock(state):
        args = [config.get("t3_binary", "t3"), "auth", "session", "issue", "--scope", "orchestration:read",
                "--subject", "mcp-client", "--ttl", "30d", "--label", "T3 Delta recorder", "--json"]
        if config.get("t3_base_dir"):
            args += ["--base-dir", str(Path(config["t3_base_dir"]).expanduser())]
        result = subprocess.run(args, capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError("Could not issue the recorder's scoped T3 credential")
        credential = json.loads(result.stdout)
        if credential.get("scopes") != ["orchestration:read"] or credential.get("subject") != "mcp-client" or not isinstance(credential.get("token"), str):
            raise ValueError("Issued credential did not match the approved read-only scope")
        fd, temporary = tempfile.mkstemp(prefix=".credential-", dir=state)
        try:
            with os.fdopen(fd, "w") as stream:
                json.dump(credential, stream)
                stream.flush()
                os.fsync(stream.fileno())
            client = T3Client(endpoint(config), Path(temporary))
            environment = client.environment()
            if environment.get("environmentId") != config["environment_id"]:
                raise ValueError("New credential reached a different T3 environment")
            Path(temporary).replace(state / "token.json")
        finally:
            Path(temporary).unlink(missing_ok=True)
        return {"authorized": True, "scopes": credential["scopes"], "expires_at": credential.get("expiresAt"), "environment_id": environment["environmentId"]}


def read_status(state, config):
    result = {"health": {"phase": "not_started"}, "threads": [], "problems": [],
              "scope": config["allowed_remotes"], "tool_outputs": "disabled", "historical_backfill": False,
              "projects_root": config.get("projects_root"),
              "additional_project_paths": config.get("additional_project_paths", []),
              "approved_project_count": len(config["allowed_projects"]) if "allowed_projects" in config else None,
              "credential_present": (state / "token.json").is_file()}
    if (state / "source.sqlite3").is_file():
        with contextlib.closing(sqlite3.connect((state / "source.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            settings = dict(db.execute("SELECT key,value FROM settings"))
            if settings.get("health"):
                result["health"] = json.loads(settings["health"])
            result["enrolled_at"] = settings.get("enrolled_at")
            result["matched_projects"] = int(settings.get("matched_projects", "0"))
            result["threads"] = [dict(row) for row in db.execute("SELECT id AS thread_id,external_id,directory,NULLIF(attached_at,'') AS attached_at,phase,last_error,last_seen FROM threads")]
            result["problems"] = [dict(row) for row in db.execute("SELECT * FROM problems")]
        checked = result["health"].get("checked_at")
        if checked and timestamp(utc_now()) - timestamp(checked) > max(60, 3 * config.get("poll_seconds", 10)):
            result["health"]["phase"] = "stale"
        if result["health"].get("phase") not in ("stale", "unavailable") and any(p["phase"] in ("needs_review", "halted") for p in result["problems"]):
            result["health"]["phase"] = "needs_review"
        elif result["health"].get("phase") not in ("stale", "unavailable") and result["problems"]:
            result["health"]["phase"] = "catching_up"
    result["delta"] = []
    if (state / "sink.sqlite3").is_file():
        with contextlib.closing(sqlite3.connect((state / "sink.sqlite3").resolve().as_uri() + "?mode=ro", uri=True, timeout=3)) as db:
            db.row_factory = sqlite3.Row
            for row in db.execute("SELECT external_id,thread_id,directory,reference,upload,access,last_ack,checked_at,visibility,halted,error FROM sessions"):
                item = dict(row)
                for field in ("upload", "access"):
                    item[field] = json.loads(item[field]) if item[field] else None
                item["pending"] = db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=0", (item["external_id"],)).fetchone()[0]
                item["acknowledged"] = db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced'", (item["external_id"],)).fetchone()[0]
                item["message_count"] = projected_message_count(json.loads(r[0]) for r in db.execute("SELECT update_json FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced' ORDER BY sequence", (item["external_id"],)))
                result["delta"].append(item)
        phases = {delivery_health(s) for s in result["delta"]}
        if result["health"].get("phase") not in ("stale", "unavailable"):
            if "needs_review" in phases:
                result["health"]["phase"] = "needs_review"
            elif "catching_up" in phases and result["health"].get("phase") != "needs_review":
                result["health"]["phase"] = "catching_up"
    return result


def history(state, config, thread_id, message_range):
    match = re.fullmatch(r"([1-9][0-9]*)-([1-9][0-9]*)", message_range)
    if not match or not 0 <= int(match[2]) - int(match[1]) < 20:
        raise ValueError("Request a range of at most 20 messages")
    status = read_status(state, config)
    source = next((t for t in status["threads"] if t["thread_id"] == thread_id), None)
    if not source:
        raise ValueError("Thread is not enrolled in this recorder")
    session = next((s for s in status["delta"] if s["external_id"] == source["external_id"]), None)
    if not session or not session["thread_id"]:
        raise ValueError("Thread does not yet have a Delta binding")
    first, last = int(match[1]), min(int(match[2]), session["message_count"])
    if first > last:
        return {"messages": [], "acknowledged_messages": session["message_count"]}
    result = subprocess.run([config.get("delta_binary", "zed-delta"), "--json", "thread", "show", session["thread_id"], "--format", "messages", "--messages", f"{first}-{last}"], capture_output=True, text=True, timeout=30)
    if result.returncode or len(result.stdout.encode()) > 2 * 1024 * 1024:
        raise RuntimeError("Delta history request failed or exceeded its output budget")
    return json.loads(result.stdout)


def run(config, state, once):
    with writer_lock(state):
        sink = DeltaSink(state, binary=config.get("delta_binary", "zed-delta"), organization=config.get("delta_organization"))
        engine = Engine(config, state, None, sink)
        failures = 0
        try:
            while True:
                try:
                    # Delivery must continue even if T3 is offline or no longer lists a thread.
                    engine.retry_delivery()
                    engine.source = T3Client(endpoint(config), state / "token.json")
                    engine.poll()
                    failures = 0
                except Exception as error:
                    engine.set_health("unavailable", type(error).__name__)
                    failures += 1
                    print(json.dumps({"phase": "unavailable", "error": type(error).__name__}), flush=True)
                    if once:
                        return 2
                if once:
                    return 0 if read_status(state, config)["health"]["phase"] == "watching" else 2
                time.sleep(min(300, config.get("poll_seconds", 10) * 2 ** min(failures, 5)))
        finally:
            engine.close()
            sink.close()


def main(argv=None):
    os.umask(0o077)
    parser = argparse.ArgumentParser(description="Private, prospective T3-to-Delta recorder")
    parser.add_argument("--config", type=Path, default=Path.home() / ".config/t3-delta-recorder/config.json")
    parser.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/t3-delta-recorder")
    sub = parser.add_subparsers(dest="command", required=True)
    init = sub.add_parser("init", help="Create an explicit repository configuration offline; does not authenticate or record")
    init.add_argument("--project", type=Path, action="append", required=True, help="Exact Git repository root to enroll; repeat for each repository")
    init.add_argument("--environment-id", required=True, help="Expected environmentId from T3's t3_environment_read tool")
    init.add_argument("--t3-runtime-file", type=Path, required=True, help="T3's server-runtime.json path")
    init.add_argument("--t3-base-dir", type=Path, help="Custom T3 base directory used when issuing the scoped credential")
    init.add_argument("--t3-binary", help="T3 executable path (default: t3 on PATH)")
    init.add_argument("--delta-binary", help="Zed Delta executable path (default: zed-delta on PATH; never auto-selects Git delta)")
    init.add_argument("--delta-organization", help="Optional explicit Delta organization ID for private uploads")
    init.add_argument("--accept-upload", action="store_true", help="Consent to future private uploads of conversations and checkout files, including tracked and nonignored data. Review files first; filename screening is not a secret scanner.")
    sub.add_parser("auth", help="Issue and validate a dedicated orchestration:read credential")
    sub.add_parser("run", help="Watch approved new T3 threads in the background")
    sub.add_parser("once", help="Run one capture/retry cycle")
    status = sub.add_parser("status", help="Read local recorder health; does not fetch conversations")
    status.add_argument("--thread")
    history_parser = sub.add_parser("history", help="Read a bounded range from an enrolled Delta conversation")
    history_parser.add_argument("--thread", required=True)
    history_parser.add_argument("--messages", default="1-20")
    args = parser.parse_args(argv)
    try:
        if args.command == "init":
            value = initialize(args.config, projects=args.project, environment_id=args.environment_id,
                               runtime_file=args.t3_runtime_file, t3_binary=args.t3_binary,
                               delta_binary=args.delta_binary, accept_upload=args.accept_upload,
                               delta_organization=args.delta_organization, t3_base_dir=args.t3_base_dir)
            print(json.dumps(value, indent=2))
            return 0
        config = load_config(args.config)
        state = args.state_dir.expanduser()
        if args.command == "auth":
            value = issue_credential(config, state)
        elif args.command in ("run", "once"):
            return run(config, state, args.command == "once")
        elif args.command == "history":
            value = history(state, config, args.thread, args.messages)
        else:
            value = read_status(state, config)
            if args.thread:
                ids = {t["external_id"] for t in value["threads"] if t["thread_id"] == args.thread}
                value["threads"] = [t for t in value["threads"] if t["thread_id"] == args.thread]
                value["delta"] = [s for s in value["delta"] if s["external_id"] in ids]
        print(json.dumps(value, indent=2, ensure_ascii=True))
        return 0
    except SetupError as error:
        print(json.dumps({"error": "SetupError", "message": str(error)}), file=sys.stderr)
        return 2
    except Exception as error:
        print(json.dumps({"error": type(error).__name__, "message": "Recorder request failed; inspect status and local configuration."}), file=sys.stderr)
        return 2
