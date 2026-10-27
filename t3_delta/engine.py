import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
from urllib.parse import urlsplit
from .delta_sink import delivery_health


def utc_now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def timestamp(value):
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


PROGRESS_STAGES = frozenset({
    "retry_delivery", "delivery_complete", "source_environment",
    "source_projects", "source_threads", "source_capture", "idle",
})


def project_health(health, progress, now, poll_seconds=10):
    """Project worker liveness without refreshing completed source evidence."""
    health = dict(health)
    limit = max(60, 3 * poll_seconds)
    current = timestamp(now)

    def recent(value):
        return value is not None and 0 <= current - timestamp(value) <= limit

    checked = health.get("checked_at")
    alive = (isinstance(progress, dict) and progress.get("stage") in PROGRESS_STAGES
             and recent(progress.get("updated_at")))
    observed = checked or (progress.get("updated_at") if isinstance(progress, dict) else None)
    if observed and not recent(checked) and not alive:
        health["phase"] = "stale"
    elif alive and health.get("phase") not in ("unavailable", "needs_review", "halted"):
        # Progress means work is advancing, not that capture/upload is current.
        if progress["stage"] != "idle" or not recent(checked):
            health["phase"] = "catching_up"
    return health


def remote_key(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Missing repository identity")
    value = value.strip()
    if value.startswith("git@") and ":" in value:
        host, path = value[4:].split(":", 1)
    else:
        parsed = urlsplit(value)
        if parsed.scheme not in ("https", "ssh") or not parsed.hostname or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("Unsupported repository identity")
        if parsed.scheme == "https" and parsed.username:
            raise ValueError("Repository remote contains credentials")
        host, path = parsed.hostname, parsed.path
    return (host + "/" + path.strip("/").removesuffix(".git")).lower()


def git_value(directory, *args):
    result = subprocess.run(["git", "-C", str(directory), *args], capture_output=True, text=True, timeout=15)
    if result.returncode:
        raise ValueError("Repository inspection failed")
    return result.stdout.strip()


def checkout_identity(directory):
    directory = Path(directory).resolve(strict=True)
    root = Path(git_value(directory, "rev-parse", "--show-toplevel")).resolve()
    if root != directory:
        raise ValueError("Checkout must be a repository root")
    return str(root), remote_key(git_value(directory, "config", "--get", "remote.origin.url"))


def common_directory(directory):
    return Path(git_value(directory, "rev-parse", "--path-format=absolute", "--git-common-dir")).resolve(strict=True)


def check_filenames(directory):
    result = subprocess.run(["git", "-C", directory, "ls-files", "-z", "--cached", "--others", "--exclude-standard"], capture_output=True, timeout=15)
    if result.returncode:
        raise ValueError("Cannot verify import boundaries")
    for raw in result.stdout.split(b"\0"):
        name = Path(os.fsdecode(raw)).name.lower()
        if any(marker in name for marker in ("example", "sample", "template", "fixture", ".test.")):
            continue
        if (name == ".env" or name.startswith(".env.") or name in ("credentials.json", ".credentials.json", ".mcp.json", "id_rsa", "id_ed25519") or name.endswith((".p12", ".pfx", ".key", ".pem"))):
            raise ValueError("Sensitive-looking filename requires review before attaching")


class Engine:
    def __init__(self, config, state_dir, source, sink, clock=utc_now):
        self.config, self.source, self.sink, self.clock = config, source, sink, clock
        self.state_dir = Path(state_dir)
        if self.state_dir.is_symlink():
            raise ValueError("State directory must not be a symlink")
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.state_dir, 0o700)
        database = self.state_dir / "source.sqlite3"
        fd = os.open(database, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        os.chmod(database, 0o600)
        self.db = sqlite3.connect(database)
        self.db.executescript("""
          CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY, value TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS threads(id TEXT PRIMARY KEY, external_id TEXT NOT NULL, directory TEXT NOT NULL,
            attached_at TEXT NOT NULL, phase TEXT NOT NULL, last_error TEXT, last_seen TEXT, fingerprint TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS exported(thread_id TEXT NOT NULL,item_id TEXT NOT NULL,hash TEXT NOT NULL,
            PRIMARY KEY(thread_id,item_id));
          CREATE TABLE IF NOT EXISTS problems(thread_id TEXT PRIMARY KEY,error TEXT NOT NULL,phase TEXT NOT NULL,at TEXT NOT NULL);
          CREATE TABLE IF NOT EXISTS observed_labels(thread_id TEXT NOT NULL,item_id TEXT NOT NULL,label TEXT,
            PRIMARY KEY(thread_id,item_id));
        """)
        if "label" not in {row[1] for row in self.db.execute("PRAGMA table_info(exported)")}:
            self.db.execute("ALTER TABLE exported ADD COLUMN label TEXT")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO observed_labels(thread_id,item_id,label) SELECT thread_id,item_id,label FROM exported")
            self.db.execute("INSERT OR IGNORE INTO settings VALUES('enrolled_at',?)", (clock(),))
        self.cutoff = self.db.execute("SELECT value FROM settings WHERE key='enrolled_at'").fetchone()[0]
        self.allowed = {remote_key(v) for v in config["allowed_remotes"]}
        if not self.allowed:
            raise ValueError("An explicit repository allowlist is required")
        self.projects_root = Path(config["projects_root"]).resolve(strict=True) if config.get("projects_root") else None
        self.additional_paths = set()
        for value in config.get("additional_project_paths", []):
            path = Path(value)
            if not path.is_absolute() or str(path) != str(path.resolve()):
                raise ValueError("Additional project paths must be exact canonical paths")
            self.additional_paths.add(str(path))
        self.projects = None
        if "allowed_projects" in config:
            if not self.projects_root or not isinstance(config["allowed_projects"], list) or not config["allowed_projects"]:
                raise ValueError("Exact project scope requires a root and approved projects")
            self.projects = {}
            for rule in config["allowed_projects"]:
                path = Path(rule["path"])
                remote = remote_key(rule["remote"])
                timestamp(rule["enrolled_after"])
                if (not path.is_absolute() or str(path) != str(path.resolve())
                        or (not path.is_relative_to(self.projects_root) and str(path) not in self.additional_paths)
                        or remote not in self.allowed
                        or str(path) in self.projects):
                    raise ValueError("Invalid or duplicate approved project boundary")
                self.projects[str(path)] = {**rule, "remote_key": remote}

    def project_cutoff(self, directory, remote):
        if remote not in self.allowed:
            return None
        if self.projects_root and not Path(directory).is_relative_to(self.projects_root) and directory not in self.additional_paths:
            return None
        if self.projects is None:
            return timestamp(self.cutoff)
        rule = self.projects.get(directory)
        if not rule or rule["remote_key"] != remote:
            return None
        return max(timestamp(self.cutoff), timestamp(rule["enrolled_after"]))

    def scoped_checkout(self, directory, remote, project_directory=None):
        if self.projects is None:
            return
        candidates = [project_directory] if project_directory else self.projects
        for candidate in candidates:
            try:
                root, identity = checkout_identity(candidate)
                if (self.project_cutoff(root, identity) is not None and identity == remote
                        and common_directory(directory) == common_directory(root)):
                    return
            except (OSError, ValueError, subprocess.SubprocessError):
                continue
        raise ValueError("Checkout is not a linked worktree of an approved project")

    def close(self):
        self.db.close()

    def set_health(self, phase, error=None, *, matched_projects=None):
        with self.db:
            if matched_projects is not None:
                self.db.execute("INSERT OR REPLACE INTO settings VALUES('matched_projects',?)", (str(matched_projects),))
            matched = self.db.execute("SELECT value FROM settings WHERE key='matched_projects'").fetchone()
            data = {"phase": phase, "checked_at": self.clock(), "error": error,
                    "matched_projects": int(matched[0]) if matched else 0}
            self.db.execute("INSERT OR REPLACE INTO settings VALUES('health',?)", (json.dumps(data),))

    def set_progress(self, stage):
        if stage not in PROGRESS_STAGES:
            raise ValueError("Unknown recorder progress stage")
        with self.db:
            self.db.execute("INSERT OR REPLACE INTO settings VALUES('progress',?)",
                            (json.dumps({"stage": stage, "updated_at": self.clock()}),))

    @staticmethod
    def _transient(error):
        return any(base.__name__ in ("DeltaUnavailable", "T3Unavailable", "T3Error", "TimeoutExpired", "TimeoutError", "URLError", "ConnectionError") for base in type(error).__mro__)

    def retry_delivery(self):
        """Drain immutable outbox records without requiring any T3 discovery.

        Incomplete attachments recover only after local allowlist/boundary
        checks. Source holds never authorize new capture, but do not discard
        completed immutable records already queued before a hold.
        """
        self.set_progress("retry_delivery")
        report = {"sessions": 0, "delivered_sessions": 0, "catching_up": 0, "halted": 0}
        for session in self.sink.sessions():
            self.set_progress("retry_delivery")
            report["sessions"] += 1
            external_id = session["external_id"]
            prefix = self.config["environment_id"] + ":"
            if not external_id.startswith(prefix) or not external_id[len(prefix):]:
                self._thread_error("sink:" + external_id, "Persisted session belongs to a different source environment")
                report["halted"] += 1
                continue
            thread_id = external_id[len(prefix):]
            if session.get("halted"):
                self._thread_error(thread_id, "Delta session is halted; preserve queued records for review", phase="halted")
                report["halted"] += 1
                continue
            held = self.db.execute("SELECT phase FROM problems WHERE thread_id=?", (thread_id,)).fetchone()
            binding_phase = self.db.execute("SELECT phase FROM threads WHERE id=?", (thread_id,)).fetchone()
            if (any(row and row[0] in ("needs_review", "halted") for row in (held, binding_phase))
                    and (not session.get("thread_id") or not session.get("reference"))):
                # Repairing a mount starts new file capture. A review hold
                # permits only delivery of records already durably queued.
                report["halted"] += 1
                continue
            try:
                if getattr(self.sink, "connection_owned_mounts", False):
                    # Reconnecting a mount resumes file capture, even when no
                    # T3 server is reachable. Revalidate full persisted bindings
                    # against current scope before any such connection starts.
                    previous = self.db.execute("SELECT directory,fingerprint FROM threads WHERE external_id=?", (external_id,)).fetchone()
                    if not previous:
                        raise ValueError("Sink session has no immutable source binding")
                    directory, remote = checkout_identity(session["directory"])
                    if remote not in self.allowed:
                        raise ValueError("Persisted checkout is outside the repository allowlist")
                    self.scoped_checkout(directory, remote)
                    info = Path(directory).stat()
                    if previous[0] != directory or previous[1] != f"{info.st_dev}:{info.st_ino}":
                        raise ValueError("Persisted checkout binding changed")
                    if (session.get("thread_id") and session.get("reference")
                            and session.get("pending") == 0
                            and delivery_health(session) is None):
                        # No outbox work exists. Live source discovery below is
                        # what can authorize resumed capture; old empty sessions
                        # must not starve it behind serial remote status timeouts.
                        report["delivered_sessions"] += 1
                        continue
                if not session.get("thread_id") or not session.get("reference"):
                    previous = self.db.execute("SELECT directory,fingerprint FROM threads WHERE external_id=?", (external_id,)).fetchone()
                    if not previous:
                        raise ValueError("Incomplete sink session has no immutable source binding")
                    directory, remote = checkout_identity(session["directory"])
                    if remote not in self.allowed:
                        raise ValueError("Persisted checkout is outside the repository allowlist")
                    self.scoped_checkout(directory, remote)
                    info = Path(directory).stat()
                    if previous[0] != directory or previous[1] != f"{info.st_dev}:{info.st_ino}":
                        raise ValueError("Persisted checkout binding changed")
                    check_filenames(directory)
                    attached = self.sink.ensure_session(external_id, directory)
                    self._mark_attached(thread_id, attached)
                if getattr(self.sink, "connection_owned_mounts", False):
                    held_source = any(row and row[0] in ("needs_review", "halted", "source_missing")
                                      for row in (held, binding_phase))
                    delivery = self.sink.flush(external_id, maintain_mount=not held_source)
                else:
                    delivery = self.sink.flush(external_id)
                if not isinstance(delivery, dict) or not isinstance(delivery.get("pending"), int) or isinstance(delivery["pending"], bool) or delivery["pending"] < 0:
                    raise ValueError("Delta delivery status is incomplete")
                if delivery["pending"]:
                    self._thread_error(thread_id, "Immutable records remain queued for Delta delivery", transient=True)
                    report["catching_up"] += 1
                else:
                    report["delivered_sessions"] += 1
                    # A successful sink acknowledgement is not evidence that
                    # the T3 thread/project still exists or is being captured.
                    self._thread_error(thread_id, "Awaiting current T3 source binding verification", transient=True)
            except Exception as error:
                phase = "source_missing" if isinstance(error, FileNotFoundError) else "halted" if type(error).__name__ == "DeltaConflict" else None
                self._thread_error(thread_id, type(error).__name__, self._transient(error), phase=phase)
                report["catching_up" if self._transient(error) or phase == "source_missing" else "halted"] += 1
        self.set_progress("delivery_complete")
        return report

    def _mark_attached(self, thread_id, binding):
        if (not isinstance(binding, dict) or not isinstance(binding.get("thread_id"), str)
                or not binding["thread_id"] or not isinstance(binding.get("reference"), str) or not binding["reference"]):
            raise ValueError("Delta attachment has not been confirmed")
        with self.db:
            self.db.execute("UPDATE threads SET attached_at=?,phase='attached' WHERE id=? AND attached_at=''", (self.clock(), thread_id))

    def poll(self):
        self.set_progress("source_environment")
        env = self.source.environment()
        if env.get("environmentId") != self.config["environment_id"]:
            raise ValueError("T3 environment identity differs from the configured environment")
        if env.get("serverVersion") != "0.1.0":
            raise ValueError("T3 server version needs compatibility review")
        matched_projects = 0
        seen_threads = set()
        self.set_progress("source_projects")
        for project in self.source.projects():
            self.set_progress("source_projects")
            try:
                directory, remote = checkout_identity(project["workspaceRoot"])
            except (OSError, ValueError, subprocess.SubprocessError):
                continue
            cutoff = self.project_cutoff(directory, remote)
            if cutoff is None:
                continue
            matched_projects += 1
            self.set_progress("source_threads")
            for listed in self.source.threads(project["id"]):
                self.set_progress("source_threads")
                if timestamp(listed["createdAt"]) < cutoff:
                    continue
                thread_id = listed["threadId"]
                seen_threads.add(thread_id)
                if listed.get("status") == "rolled_back":
                    self._thread_error(thread_id, "T3 history was rolled back; recording requires review")
                    continue
                try:
                    self.set_progress("source_capture")
                    self._capture(project, listed)
                except Exception as error:
                    self._thread_error(thread_id, type(error).__name__, self._transient(error),
                                       phase="source_missing" if isinstance(error, FileNotFoundError) else None)
                finally:
                    self.set_progress("source_threads")
        known_threads = {row[0] for row in self.db.execute("SELECT id FROM threads")}
        prefix = self.config["environment_id"] + ":"
        known_threads.update(s["external_id"][len(prefix):] for s in self.sink.sessions()
                             if s["external_id"].startswith(prefix))
        for thread_id in known_threads - seen_threads:
            self._thread_error(thread_id, "T3 project or thread is no longer discoverable", phase="source_missing")
        self.set_health("watching" if matched_projects else "awaiting_project", matched_projects=matched_projects)
        self.set_progress("idle")

    def _thread_error(self, thread_id, error, transient=False, *, phase=None):
        phase = phase or ("catching_up" if transient else "needs_review")
        if phase in ("needs_review", "halted", "source_missing") and hasattr(self.sink, "release_session"):
            try:
                self.sink.release_session(self.config["environment_id"] + ":" + thread_id)
            except Exception:
                pass
        previous = self.db.execute("SELECT phase FROM problems WHERE thread_id=?", (thread_id,)).fetchone()
        binding = self.db.execute("SELECT phase FROM threads WHERE id=?", (thread_id,)).fetchone()
        existing = {row[0] for row in (previous, binding) if row}
        if "halted" in existing and phase != "halted" or "needs_review" in existing and phase not in ("needs_review", "halted"):
            return
        with self.db:
            self.db.execute("UPDATE threads SET phase=?,last_error=? WHERE id=?", (phase, error, thread_id))
            self.db.execute("INSERT OR REPLACE INTO problems VALUES(?,?,?,?)", (thread_id, error, phase, self.clock()))

    def _capture(self, project, listed):
        thread_id = listed["threadId"]
        previous = self.db.execute("SELECT external_id,directory,phase,fingerprint FROM threads WHERE id=?", (thread_id,)).fetchone()
        problem = self.db.execute("SELECT phase FROM problems WHERE thread_id=?", (thread_id,)).fetchone()
        if problem and problem[0] in ("needs_review", "halted") or previous and previous[2] in ("needs_review", "halted"):
            return
        external_id = f"{self.config['environment_id']}:{thread_id}"
        existing_sink = next((s for s in self.sink.sessions() if s["external_id"] == external_id), None)
        if existing_sink and existing_sink.get("halted"):
            self._thread_error(thread_id, "Delta session is halted; no further source capture", phase="halted")
            return
        if existing_sink and not previous:
            raise ValueError("Persisted sink session has no immutable source binding; preserve the old recording")
        metadata = self.source.call_tool("t3_thread_read", {"threadId": thread_id, "view": "messages", "limit": 1, "maxCharsPerItem": 1, "runLimit": 1})["thread"]
        if metadata.get("projectId") != project["id"]:
            raise ValueError("T3 thread changed project")
        directory, remote = checkout_identity(metadata.get("worktreePath") or project["workspaceRoot"])
        if remote not in self.allowed:
            raise ValueError("T3 checkout is outside the repository allowlist")
        self.scoped_checkout(directory, remote, str(Path(project["workspaceRoot"]).resolve()))
        stat = Path(directory).stat()
        fingerprint = f"{stat.st_dev}:{stat.st_ino}"
        if previous and (previous[0] != external_id or previous[1] != directory or previous[3] != fingerprint):
            raise ValueError("T3 checkout binding changed; preserve the old recording")
        if not previous:
            check_filenames(directory)
        with self.db:
            # Commit path identity before the sink can create any durable
            # partial session or start external checkout/file recording.
            self.db.execute("INSERT OR IGNORE INTO threads(id,external_id,directory,attached_at,phase,fingerprint) VALUES(?,?,?,'','attaching',?)", (thread_id, external_id, directory, fingerprint))
        binding = self.sink.ensure_session(external_id, directory)
        self._mark_attached(thread_id, binding)
        detail = self.source.read_thread(thread_id)
        if detail["thread"].get("worktreePath") != metadata.get("worktreePath"):
            raise ValueError("T3 checkout changed during capture")
        items = detail["items"]
        runs = {run["runId"]: run for run in detail.get("recentRuns", []) if isinstance(run, dict) and isinstance(run.get("runId"), str)}
        live = detail["thread"].get("status") in ("preparing", "queued", "starting", "running", "waiting")
        for item in items:
            if item.get("sourceThreadId") != thread_id or item.get("visibility") != "local":
                continue
            if item.get("type") not in ("user_message", "assistant_message", "proposed_plan"):
                continue
            old = self.db.execute("SELECT hash,label FROM exported WHERE thread_id=? AND item_id=?", (thread_id, item["itemId"])).fetchone()
            if item.get("status") not in ("completed", "failed", "cancelled", "interrupted"):
                if old:
                    raise ValueError("Previously exported completed T3 message is no longer terminal")
                break
            if not old and live and item.get("runId") == detail["thread"].get("activeRunId") and item["type"] != "user_message":
                break
            text = item.get("text")
            if not isinstance(text, str) or not text:
                if old:
                    raise ValueError("Previously exported completed T3 message text disappeared")
                continue
            if item.get("textTruncated") or item.get("nextTextOffset") is not None:
                raise ValueError("T3 message text is incomplete")
            if item["type"] == "proposed_plan":
                text = "[T3 proposed plan]\n" + text
            observed_label = self.db.execute("SELECT label FROM observed_labels WHERE thread_id=? AND item_id=?", (thread_id, item["itemId"])).fetchone()
            label = old[1] if old else observed_label[0] if observed_label else None
            if not old and not observed_label and item["type"] != "user_message" and isinstance(item.get("runId"), str) and item["runId"]:
                observed = {"runId": item["runId"]}
                run = runs.get(item["runId"], {})
                for key in ("providerInstanceId", "model"):
                    if isinstance(run.get(key), str) and run[key]:
                        observed[key] = run[key]
                label = "[T3 turn " + json.dumps(observed, sort_keys=True, ensure_ascii=True) + "]\n"
            if not old and not observed_label:
                # This receipt-independent observation must survive a crash
                # after sink.enqueue persisted the immutable message body.
                with self.db:
                    self.db.execute("INSERT OR IGNORE INTO observed_labels(thread_id,item_id,label) VALUES(?,?,?)", (thread_id, item["itemId"], label))
            if label:
                text = label + text
            update = {"sessionUpdate": "user_message" if item["type"] == "user_message" else "agent_message",
                      "messageId": f"t3:{thread_id}:{item['itemId']}", "content": [{"type": "text", "text": text}]}
            digest = hashlib.sha256(json.dumps(update, sort_keys=True).encode()).hexdigest()
            if old and old[0] != digest:
                raise ValueError("Previously exported completed T3 message changed; review required")
            if not old:
                self.sink.enqueue(external_id, f"message:{item['itemId']}", update)
                with self.db:
                    self.db.execute("INSERT INTO exported(thread_id,item_id,hash,label) VALUES(?,?,?,?)", (thread_id, item["itemId"], digest, label))
        delivery = self.sink.flush(external_id)
        if not isinstance(delivery, dict) or not isinstance(delivery.get("pending"), int) or isinstance(delivery["pending"], bool) or delivery["pending"] < 0:
            raise ValueError("Delta delivery status is incomplete")
        if delivery["pending"]:
            self._thread_error(thread_id, "Immutable records remain queued for Delta delivery", transient=True)
            return binding, delivery
        with self.db:
            self.db.execute("UPDATE threads SET phase='recording',last_error=NULL,last_seen=? WHERE id=?", (self.clock(), thread_id))
            self.db.execute("DELETE FROM problems WHERE thread_id=?", (thread_id,))
        return binding, delivery

    def status(self):
        row = self.db.execute("SELECT value FROM settings WHERE key='health'").fetchone()
        health = json.loads(row[0]) if row else {"phase": "not_started"}
        row = self.db.execute("SELECT value FROM settings WHERE key='progress'").fetchone()
        progress = json.loads(row[0]) if row else None
        health = project_health(health, progress, self.clock(), self.config.get("poll_seconds", 10))
        threads = [dict(zip(("thread_id", "external_id", "directory", "attached_at", "phase", "last_error", "last_seen"), r)) for r in self.db.execute("SELECT id,external_id,directory,attached_at,phase,last_error,last_seen FROM threads")]
        for thread in threads:
            thread["attached_at"] = thread["attached_at"] or None
        problems = [dict(zip(("thread_id", "error", "phase", "at"), r)) for r in self.db.execute("SELECT * FROM problems")]
        if health.get("phase") not in ("stale", "unavailable") and any(t["phase"] in ("needs_review", "halted") for t in problems):
            health["phase"] = "needs_review"
        elif health.get("phase") not in ("stale", "unavailable", "awaiting_project") and problems:
            health["phase"] = "catching_up"
        return {"health": health, "progress": progress, "enrolled_at": self.cutoff, "scope": sorted(self.allowed), "threads": threads,
                "problems": problems,
                "capture": "completed_messages_and_plans_with_observed_turn_labels", "tool_outputs": "disabled", "coverage": "file history begins after attachment; no native T3 rewind"}
