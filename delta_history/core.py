"""Private prospective ingress and a single fenced Delta delivery worker.

Submission is local and transactional. It never waits on Delta or implies upload.
"""
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess

from t3_delta.delta_sink import DeltaSink, DeltaConflict, DeltaUnavailable, delivery_health, projected_message_count
from t3_delta.engine import checkout_identity, check_filenames, common_directory, utc_now
from .configuration import inspect_scope, validate
from .protocol import encode, utc_timestamp, validate_batch


class GenericSink(DeltaSink):
    SOURCE = "delta-agent-history"


def session_key(source, source_instance, session_id):
    identity = json.dumps([source, source_instance, session_id], ensure_ascii=True, separators=(",", ":"))
    return "dah:" + hashlib.sha256(identity.encode()).hexdigest()


def _state(state, create=False):
    path = Path(state).expanduser()
    if path.is_symlink():
        raise ValueError("Generic state must not be a symlink")
    for legacy in ("source.sqlite3", "sink.sqlite3"):
        if (path / legacy).exists() or (path / legacy).is_symlink():
            raise ValueError("Legacy T3 state is separate; select a fresh generic state directory")
    for child in ("ingress.sqlite3", "delta", "writer.lock"):
        if (path / child).is_symlink():
            raise ValueError("Generic state entries must not be symlinks")
    if create:
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        if path.stat().st_uid != os.getuid():
            raise ValueError("Generic state must belong to the current user")
        path.chmod(0o700)
    return path


def _database(state):
    path = _state(state, True) / "ingress.sqlite3"
    fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    os.close(fd)
    path.chmod(0o600)
    db = sqlite3.connect(path, timeout=5)
    db.row_factory = sqlite3.Row
    db.executescript("""
      CREATE TABLE IF NOT EXISTS sessions(
        key TEXT PRIMARY KEY, source TEXT NOT NULL, source_instance TEXT NOT NULL,
        session_id TEXT NOT NULL, directory TEXT NOT NULL, project TEXT NOT NULL,
        remote TEXT NOT NULL, common_dir TEXT NOT NULL, fingerprint TEXT NOT NULL,
        opened_at TEXT NOT NULL, accepted_at TEXT NOT NULL,
        exported_sequence INTEGER NOT NULL DEFAULT 1, close_sequence INTEGER,
        attached INTEGER NOT NULL DEFAULT 0, halted INTEGER NOT NULL DEFAULT 0,
        error TEXT, phase TEXT NOT NULL DEFAULT 'queued');
      CREATE TABLE IF NOT EXISTS events(
        session_key TEXT NOT NULL, sequence INTEGER NOT NULL, event_id TEXT NOT NULL,
        body TEXT NOT NULL, accepted_at TEXT NOT NULL, exported INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(session_key,sequence), UNIQUE(session_key,event_id));
    """)
    return db


@contextlib.contextmanager
def _readonly(path):
    if path.is_symlink():
        raise ValueError("Recorder databases must not be symlinks")
    db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def _binding(config, project):
    directory, approved = inspect_scope(config, project)
    _, remote = checkout_identity(directory)
    check_filenames(directory)
    info = Path(directory).stat()
    rule = next(r for r in config["allowed_projects"] if r["path"] == approved)
    return {"directory": directory, "project": approved, "remote": remote,
            "common_dir": str(common_directory(directory)), "fingerprint": f"{info.st_dev}:{info.st_ino}",
            "cutoff": utc_timestamp(rule["enrolled_after"])}


def _verify(config, row):
    if (row["source"], row["source_instance"]) not in {(s["kind"], s["instance"]) for s in config["sources"]}:
        raise ValueError("Source binding is no longer enrolled")
    actual = _binding(config, row["directory"])
    if any(actual[k] != row[k] for k in ("directory", "project", "remote", "common_dir", "fingerprint")):
        raise ValueError("Immutable repository binding changed")


def _halt(db, key, error):
    db.execute("UPDATE sessions SET halted=1,error=?,phase='halted' WHERE key=?", (error, key))


def _sqlite_busy(error):
    code = getattr(error, "sqlite_errorcode", None)
    return (isinstance(error, sqlite3.OperationalError) and isinstance(code, int)
            and (code & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED))


def submit(config, state, events):
    validate(config)
    batch = validate_batch(events)
    if not batch:
        raise ValueError("Submit at least one ingress event")
    permitted = {(s["kind"], s["instance"]) for s in config["sources"]}
    bindings = {}
    for event in batch:
        if (event["source"], event["source_instance"]) not in permitted:
            raise ValueError("Source kind and instance are not enrolled")
        if event["type"] == "session.open":
            binding = _binding(config, event["project"])
            if utc_timestamp(event["created_at"]) < binding["cutoff"] or utc_timestamp(utc_now()) < binding["cutoff"]:
                raise ValueError("Session predates prospective enrollment")
            bindings[session_key(event["source"], event["source_instance"], event["session_id"])] = binding
    result = {"accepted": 0, "duplicates": 0, "rejected": 0, "halted": 0, "sessions": []}
    db = _database(state)
    try:
        db.execute("BEGIN IMMEDIATE")
        touched = set()
        for event in batch:
            key = session_key(event["source"], event["source_instance"], event["session_id"])
            touched.add(key)
            body, now = encode(event), utc_now()
            row = db.execute("SELECT * FROM sessions WHERE key=?", (key,)).fetchone()
            previous = db.execute("SELECT sequence,event_id,body FROM events WHERE session_key=? AND (sequence=? OR event_id=?)", (key, event["sequence"], event["event_id"])).fetchall()
            if previous:
                if len(previous) == 1 and previous[0]["body"] == body:
                    result["duplicates"] += 1
                else:
                    _halt(db, key, "Conflicting immutable event identity or sequence")
                    result["rejected"] += 1
                continue
            if row is None:
                if event["type"] != "session.open":
                    # A semantic rejection must not roll back a conflict already
                    # observed in an unrelated session in this batch.
                    result["rejected"] += 1
                    continue
                b = bindings[key]
                db.execute("INSERT INTO sessions(key,source,source_instance,session_id,directory,project,remote,common_dir,fingerprint,opened_at,accepted_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                           (key, event["source"], event["source_instance"], event["session_id"], b["directory"], b["project"], b["remote"], b["common_dir"], b["fingerprint"], event["created_at"], now))
                row = db.execute("SELECT * FROM sessions WHERE key=?", (key,)).fetchone()
            else:
                if row["halted"]:
                    result["rejected"] += 1
                    continue
                try:
                    _verify(config, row)
                except Exception:
                    _halt(db, key, "Repository binding is unavailable or changed")
                    result["rejected"] += 1
                    continue
                if event["type"] == "session.open" or event["sequence"] == 1 or (row["close_sequence"] is not None and (event["sequence"] > row["close_sequence"] or event["type"] == "session.close")):
                    _halt(db, key, "Event conflicts with the opened or terminal session")
                    result["rejected"] += 1
                    continue
            rule = next(r for r in config["allowed_projects"] if r["path"] == row["project"])
            if utc_timestamp(event["created_at"]) < max(utc_timestamp(rule["enrolled_after"]), utc_timestamp(row["opened_at"])):
                _halt(db, key, "Event predates its prospective session boundary")
                result["rejected"] += 1
                continue
            if event["type"] == "session.close":
                later = db.execute("SELECT 1 FROM events WHERE session_key=? AND sequence>?", (key, event["sequence"])).fetchone()
                if later:
                    _halt(db, key, "Close sequence precedes accepted messages")
                    result["rejected"] += 1
                    continue
                db.execute("UPDATE sessions SET close_sequence=? WHERE key=?", (event["sequence"], key))
            db.execute("INSERT INTO events(session_key,sequence,event_id,body,accepted_at,exported) VALUES(?,?,?,?,?,?)", (key, event["sequence"], event["event_id"], body, now, int(event["type"] == "session.open")))
            result["accepted"] += 1
        result["sessions"] = sorted(touched)
        result["halted"] = sum(bool(row[0]) for key in touched if (row := db.execute("SELECT halted FROM sessions WHERE key=?", (key,)).fetchone()))
        db.commit()
        return result
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()


def _update(key, event):
    identity = hashlib.sha256((key + "\0" + event["event_id"]).encode()).hexdigest()
    text = event["text"]
    if event["role"] == "plan":
        text = "[Proposed plan]\n" + text
    if event.get("model"):
        text = "[Model " + event["model"] + "]\n" + text
    return "event:" + identity, {"sessionUpdate": "user_message" if event["role"] == "user" else "agent_message", "messageId": "dah:" + identity, "content": [{"type": "text", "text": text}]}


def flush(config, state, sink_factory=None):
    validate(config)
    state = _state(state, True)
    fd = os.open(state / "writer.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    report = {"sessions": 0, "delivered_sessions": 0, "catching_up": 0, "halted": 0, "orphaned_bindings": 0, "busy": False}
    db = sink = None
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            report["busy"] = True
            return report
        db = _database(state)
        factory = sink_factory or GenericSink
        sink = factory(state / "delta", binary=config["delta_binary"], organization=config.get("delta_organization"), producer_name="Delta Agent History")
        originals = db.execute("SELECT * FROM sessions ORDER BY accepted_at,key").fetchall()
        if hasattr(sink, "sessions"):
            known = {row["key"] for row in originals}
            # A lost ingress binding never authorizes reconstructing a checkout.
            report["orphaned_bindings"] = sum(s["external_id"] not in known for s in sink.sessions())
            report["halted"] += report["orphaned_bindings"]
        for original in originals:
            report["sessions"] += 1
            key = original["key"]
            if original["halted"]:
                report["halted"] += 1
                continue
            try:
                _verify(config, original)
                bound = sink.ensure_session(key, original["directory"])
                if not isinstance(bound, dict) or not bound.get("thread_id") or not bound.get("reference") or bound.get("directory") != original["directory"]:
                    raise ValueError("Delta attachment receipt is incomplete")
                with db:
                    db.execute("UPDATE sessions SET attached=1 WHERE key=?", (key,))
                expected = original["exported_sequence"] + 1
                gap = False
                for row in db.execute("SELECT * FROM events WHERE session_key=? AND sequence>? ORDER BY sequence", (key, original["exported_sequence"])).fetchall():
                    if row["sequence"] != expected:
                        gap = True
                        break
                    # Re-read hold before each new enqueue: concurrent hooks can halt.
                    if db.execute("SELECT halted FROM sessions WHERE key=?", (key,)).fetchone()[0]:
                        raise ValueError("Session halted while delivering")
                    event = json.loads(row["body"])
                    if event["type"] == "message.completed":
                        item, update = _update(key, event)
                        sink.enqueue(key, item, update)
                    with db:
                        db.execute("UPDATE events SET exported=1 WHERE session_key=? AND sequence=?", (key, row["sequence"]))
                        db.execute("UPDATE sessions SET exported_sequence=? WHERE key=?", (row["sequence"], key))
                    expected += 1
                delivery = sink.flush(key)
                if not isinstance(delivery, dict) or type(delivery.get("pending")) is not int or delivery["pending"] < 0:
                    raise ValueError("Delta delivery status is incomplete")
                if delivery.get("halted"):
                    raise DeltaConflict("Delta producer is halted")
                phase = "waiting_for_gap" if gap else "catching_up" if delivery["pending"] or delivery_health(delivery) == "catching_up" else "closed" if original["close_sequence"] is not None and expected > original["close_sequence"] else "recording"
                if delivery_health(delivery) == "needs_review":
                    raise ValueError("Delta upload requires review")
                with db:
                    db.execute("UPDATE sessions SET phase=?,error=NULL WHERE key=? AND halted=0", (phase, key))
                report["catching_up" if phase in ("waiting_for_gap", "catching_up") else "delivered_sessions"] += 1
            except Exception as error:
                if _sqlite_busy(error):
                    # A concurrent local writer can also block status updates.
                    # Preserve the session and replay the same durable event later.
                    report["catching_up"] += 1
                    continue
                transient = isinstance(error, (DeltaUnavailable, TimeoutError, ConnectionError, subprocess.TimeoutExpired))
                with db:
                    if transient:
                        db.execute("UPDATE sessions SET phase='catching_up',error=? WHERE key=? AND halted=0", (type(error).__name__, key))
                    else:
                        _halt(db, key, type(error).__name__)
                report["catching_up" if transient else "halted"] += 1
        return report
    finally:
        if sink is not None:
            sink.close()
        if db is not None:
            db.close()
        os.close(fd)


def _metadata(state):
    state = _state(state)
    if not (state / "ingress.sqlite3").is_file():
        return []
    with _readonly(state / "ingress.sqlite3") as db:
        rows = []
        for row in db.execute("SELECT * FROM sessions ORDER BY accepted_at,key"):
            entry = dict(row)
            entry["session_key"] = entry["key"]
            entry["pending"] = db.execute("SELECT COUNT(*) FROM events WHERE session_key=? AND exported=0", (entry["key"],)).fetchone()[0]
            entry["accepted"] = db.execute("SELECT COUNT(*) FROM events WHERE session_key=?", (entry["key"],)).fetchone()[0]
            entry["halted"] = bool(entry["halted"])
            rows.append(entry)
    sinkpath = state / "delta" / "sink.sqlite3"
    if sinkpath.is_file():
        with _readonly(sinkpath) as db:
            for entry in rows:
                row = db.execute("SELECT * FROM sessions WHERE external_id=?", (entry["key"],)).fetchone()
                if not row:
                    continue
                meta = dict(row)
                meta["pending"] = db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=0", (entry["key"],)).fetchone()[0]
                meta["acknowledged"] = db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced'", (entry["key"],)).fetchone()[0]
                meta["message_count"] = projected_message_count(json.loads(r[0]) for r in db.execute("SELECT update_json FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced' ORDER BY sequence", (entry["key"],)))
                for field in ("upload", "access"):
                    meta[field] = json.loads(meta[field]) if meta[field] else None
                entry["delta"] = {k: meta[k] for k in ("thread_id", "reference", "visibility", "upload", "access", "pending", "acknowledged", "message_count", "halted", "error")}
                entry["pending"] += meta["pending"]
                entry["halted"] = entry["halted"] or bool(meta["halted"])
    return rows


def status(config, state):
    from .adapters.common import adapter_health
    validate(config)
    rows = _metadata(state)
    halted = sum(r["halted"] for r in rows)
    sinkpath = _state(state) / "delta" / "sink.sqlite3"
    orphaned = 0
    if sinkpath.is_file():
        known = {r["key"] for r in rows}
        with _readonly(sinkpath) as db:
            orphaned = sum(r[0] not in known for r in db.execute("SELECT external_id FROM sessions"))
    halted += orphaned
    pending = sum(r["pending"] for r in rows)
    adapters = adapter_health(state)
    halted += adapters["halted"]
    pending += adapters["pending"]
    waiting = any(r["phase"] in ("queued", "catching_up", "waiting_for_gap") or not r.get("delta") for r in rows)
    for row in rows:
        if row.get("delta"):
            health = delivery_health(row["delta"])
            if health == "needs_review" and not row["halted"]:
                halted += 1
            waiting = waiting or health == "catching_up"
    phase = "needs_review" if halted else "catching_up" if pending or waiting else "not_started" if not rows else "watching"
    return {"health": {"phase": phase}, "sessions": rows, "accepted": sum(r["accepted"] for r in rows), "pending": pending, "halted": halted, "orphaned_bindings": orphaned, "adapters": adapters}


def sessions(config, state, project):
    validate(config)
    _, approved = inspect_scope(config, project)
    return [r for r in _metadata(state) if r["project"] == approved]


def history(config, state, project, session, messages="1-20"):
    match = re.fullmatch(r"([1-9][0-9]*)-([1-9][0-9]*)", messages)
    if not match or not 0 <= int(match[2]) - int(match[1]) < 20:
        raise ValueError("Request an inclusive range of at most 20 messages")
    row = next((r for r in sessions(config, state, project) if r["session_key"] == session), None)
    if not row:
        raise ValueError("Session does not belong to the approved repository")
    delta = row.get("delta")
    if not delta or not delta.get("thread_id"):
        raise ValueError("Session has no recorded Delta binding")
    first, last = int(match[1]), min(int(match[2]), delta["message_count"])
    if first > last:
        return []
    result = subprocess.run([config["delta_binary"], "--json", "thread", "show", delta["thread_id"], "--format", "messages", "--messages", f"{first}-{last}"], capture_output=True, text=True, timeout=30)
    if result.returncode or len(result.stdout.encode()) > 2 * 1024 * 1024:
        raise ValueError("Delta history read failed or exceeded output budget")
    return json.loads(result.stdout)
