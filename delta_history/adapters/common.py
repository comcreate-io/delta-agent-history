"""Private adapter journal. Freeze events before submitting to the local core.

No adapter imports application transcript stores or makes Delta network calls.
"""
import contextlib
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3

from ..configuration import resolve_project, validate
from ..protocol import MAX_BATCH_BYTES, MAX_TEXT_BYTES, encode, identifier, validate_event

MAX_INPUT = 4 * 1024 * 1024


class AdapterError(ValueError):
    """A sanitized adapter input or identity failure."""


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read_payload(stream):
    raw = stream.read(MAX_INPUT + 1)
    if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_INPUT:
        raise AdapterError("Hook input exceeds its byte budget")
    try:
        def unique(pairs):
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate key")
                result[key] = value
            return result
        value = json.loads(raw, object_pairs_hook=unique)
    except (ValueError, UnicodeError):
        raise AdapterError("Invalid hook JSON") from None
    if not isinstance(value, dict):
        raise AdapterError("Hook input must be an object")
    return value


def required_id(value):
    if not identifier(value):
        raise AdapterError("A stable upstream identifier is required")
    return value


def visible_text(value):
    if not isinstance(value, str) or not value or len(value.encode()) > MAX_TEXT_BYTES:
        raise AdapterError("Visible message text is absent or exceeds its byte budget")
    return value


def check_source(config, source, instance, project):
    validate(config)
    if {"kind": source, "instance": instance} not in config["sources"]:
        raise AdapterError("Source instance is not enrolled")
    return resolve_project(config, str(project))


def submit_events(config, state, events):
    from ..core import submit
    return submit(config, state, events)


def adapter_health(state, project=None):
    """Read metadata counts only, optionally limited to an exact checkout path."""
    path = Path(state) / "adapters.sqlite3"
    if Path(state).is_symlink() or path.is_symlink():
        raise AdapterError("Adapter journal must not be a symlink")
    if not path.is_file():
        return {"pending":0, "halted":0}
    query = " WHERE project=?" if project is not None else ""
    parameters = (str(project),) if project is not None else ()
    with contextlib.closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True,timeout=3)) as db:
        halted = db.execute("SELECT COUNT(*) FROM adapter_sessions"+query+(" AND" if query else " WHERE")+" halted=1",parameters).fetchone()[0]
        pending = db.execute("SELECT COUNT(*) FROM adapter_events e JOIN adapter_sessions s ON e.source=s.source AND e.instance=s.instance AND e.session=s.session WHERE e.delivered=0"+(" AND s.project=?" if project is not None else ""),parameters).fetchone()[0]
    return {"pending":pending, "halted":halted}


def retry_pending(config, state):
    """Replay only frozen existing events. Never discover sources or run a model."""
    validate(config)
    from ..core import _state
    state = _state(state)
    path = state / "adapters.sqlite3"
    report = {"sessions":0, "submitted":0, "halted":0, "unavailable":0, "errors":[]}
    if state.is_symlink() or path.is_symlink():
        raise AdapterError("Adapter journal must not be a symlink")
    if not path.is_file():
        return report
    if state.stat().st_uid != os.getuid() or path.stat().st_uid != os.getuid():
        raise AdapterError("Adapter journal must belong to the current user")
    with contextlib.closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True,timeout=3)) as db:
        sessions = db.execute("""SELECT s.source,s.instance,s.session,s.project,s.halted
            FROM adapter_sessions s WHERE EXISTS(SELECT 1 FROM adapter_events e
            WHERE e.source=s.source AND e.instance=s.instance AND e.session=s.session
            AND e.delivered=0) ORDER BY s.rowid""").fetchall()
    for source,instance,session,project,halted in sessions:
        report["sessions"] += 1
        if halted:
            report["halted"] += 1
            continue
        try:
            with Journal(config,state,source,instance,session,project,existing=True) as journal:
                result = journal.flush()
                report["submitted"] += result["submitted"]
        except Exception as error:
            # Each failed binding remains pending. Scope/identity failures require review;
            # lock contention and I/O errors can retry in a later worker cycle.
            category = type(error).__name__
            if category not in report["errors"]:
                report["errors"].append(category)
            if isinstance(error, ValueError):
                try:
                    with contextlib.closing(sqlite3.connect(path.resolve().as_uri()+"?mode=rw",uri=True,timeout=3)) as db:
                        with db:
                            db.execute("UPDATE adapter_sessions SET halted=1 WHERE source=? AND instance=? AND session=?",(source,instance,session))
                    report["halted"] += 1
                except Exception as hold_error:
                    report["unavailable"] += 1
                    if type(hold_error).__name__ not in report["errors"]:
                        report["errors"].append(type(hold_error).__name__)
            else:
                report["unavailable"] += 1
    return report


class Journal:
    def __init__(self, config, state, source, instance, session, project, *, existing=False):
        # Share the core's generic/legacy boundary before any directory mutation.
        # Local import avoids the core -> adapter-health import cycle.
        from ..core import _state
        self.config, self.state = config, _state(state)
        self.source, self.instance, self.session = source, instance, required_id(session)
        self.project = check_source(config, source, instance, project)
        directory_stat = Path(self.project).stat()
        self.fingerprint = f"{directory_stat.st_dev}:{directory_stat.st_ino}"
        if self.state.is_symlink():
            raise AdapterError("Adapter state must not be a symlink")
        if not existing:
            self.state.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.state.stat().st_uid != os.getuid():
            raise AdapterError("Adapter state belongs to another user")
        self.state.chmod(0o700)
        path = self.state / "adapters.sqlite3"
        fd = os.open(path, os.O_RDWR | os.O_NOFOLLOW | (0 if existing else os.O_CREAT), 0o600)
        try:
            if os.fstat(fd).st_uid != os.getuid():
                raise AdapterError("Adapter journal belongs to another user")
            os.fchmod(fd, 0o600)
        finally:
            os.close(fd)
        self.db = sqlite3.connect(path.resolve().as_uri()+"?mode=rw",uri=True,timeout=3)
        self.db.execute("PRAGMA busy_timeout=3000")
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS adapter_sessions(
            source TEXT, instance TEXT, session TEXT, project TEXT NOT NULL,
            fingerprint TEXT,
            metadata TEXT NOT NULL DEFAULT '{}', halted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source,instance,session));
          CREATE TABLE IF NOT EXISTS adapter_events(
            source TEXT, instance TEXT, session TEXT, upstream_key TEXT, sequence INTEGER,
            body TEXT NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(source,instance,session,upstream_key),
            UNIQUE(source,instance,session,sequence));
        """)
        if "fingerprint" not in {row[1] for row in self.db.execute("PRAGMA table_info(adapter_sessions)")}:
            self.db.execute("ALTER TABLE adapter_sessions ADD COLUMN fingerprint TEXT")
        self.key = source, instance, session

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.db.close()

    @contextlib.contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.execute("SELECT project,halted,fingerprint FROM adapter_sessions WHERE source=? AND instance=? AND session=?", self.key).fetchone()
            if row and (row[0] != self.project or row[1] or row[2] != self.fingerprint):
                raise AdapterError("Adapter session is halted or checkout identity changed")
            if not row:
                self.db.execute("INSERT INTO adapter_sessions(source,instance,session,project,fingerprint) VALUES(?,?,?,?,?)", (*self.key,self.project,self.fingerprint))
            yield
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def metadata(self):
        row = self.db.execute("SELECT metadata FROM adapter_sessions WHERE source=? AND instance=? AND session=?", self.key).fetchone()
        return json.loads(row[0]) if row else {}

    def set_metadata(self, value):
        self.db.execute("UPDATE adapter_sessions SET metadata=? WHERE source=? AND instance=? AND session=?", (encode(value), *self.key))

    def halt(self):
        with self.db:
            self.db.execute("UPDATE adapter_sessions SET halted=1 WHERE source=? AND instance=? AND session=?", self.key)

    def add(self, upstream_key, kind, **body):
        """Called within transaction. Duplicate checks ignore local receipt time."""
        required_id(upstream_key)
        semantic = encode({"type":kind, **body})
        existing = self.db.execute("SELECT body FROM adapter_events WHERE source=? AND instance=? AND session=? AND upstream_key=?", (*self.key,upstream_key)).fetchone()
        if existing:
            if existing[0] != semantic:
                raise AdapterError("An upstream event changed after completion")
            return False
        sequence = self.db.execute("SELECT COALESCE(MAX(sequence),0)+1 FROM adapter_events WHERE source=? AND instance=? AND session=?", self.key).fetchone()[0]
        if sequence == 1 and kind != "session.open":
            raise AdapterError("Adapter session has not opened")
        payload = validate_event({"schema_version":1, "type":kind, "source":self.source,
            "source_instance":self.instance, "session_id":self.session,
            "event_id":"event:"+hashlib.sha256(upstream_key.encode()).hexdigest(),
            "sequence":sequence, "created_at":now(), **body})
        self.db.execute("INSERT INTO adapter_events(source,instance,session,upstream_key,sequence,body,payload) VALUES(?,?,?,?,?,?,?)", (*self.key,upstream_key,sequence,semantic,encode(payload)))
        return True

    def has(self, upstream_key):
        return self.db.execute("SELECT 1 FROM adapter_events WHERE source=? AND instance=? AND session=? AND upstream_key=?", (*self.key,upstream_key)).fetchone() is not None

    def open(self):
        return self.add("session:open", "session.open", project=self.project)

    def flush(self):
        """Submit local-only batches; a crash after submit safely replays frozen bytes."""
        try:
            return self._flush()
        except AdapterError:
            self.halt()
            raise

    def _flush(self):
        count = 0
        while True:
            with self.transaction():
                rows = self.db.execute("SELECT upstream_key,payload FROM adapter_events WHERE source=? AND instance=? AND session=? AND delivered=0 ORDER BY sequence LIMIT 100", self.key).fetchall()
                if not rows:
                    return {"accepted": True, "submitted": count, "cloud_upload": "not_checked"}
                batch, size = [], 0
                for key, payload in rows:
                    budget = len(payload.encode()) + 1
                    if batch and size + budget > MAX_BATCH_BYTES:
                        break
                    batch.append((key, json.loads(payload)))
                    size += budget
                result = submit_events(self.config, self.state, [event for _,event in batch])
                if not isinstance(result, dict) or result.get("rejected", 0) or result.get("halted", 0) or result.get("accepted", 0) + result.get("duplicates", 0) != len(batch):
                    raise AdapterError("Local ingress refused an adapter event")
                self.db.executemany("UPDATE adapter_events SET delivered=1 WHERE source=? AND instance=? AND session=? AND upstream_key=?", [(*self.key,key) for key,_ in batch])
                count += len(batch)
