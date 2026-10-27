"""Durable Delta recording sink. Mutations require the caller's process lock.

Only the public Delta CLI is used. No private Delta database is inspected.
"""

import atexit
import json
import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import time


# Contract 8 mounts belong to their CLI connection. Keep them for the worker
# process, including across generic drain cycles which reopen their ledgers.
_MOUNTS = {}
MAX_MOUNTS = 32


def stop_mounts(keys=None):
    for key in list(_MOUNTS if keys is None else keys):
        process = _MOUNTS.pop(key, None)
        if process is None:
            continue
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stdout:
            process.stdout.close()


atexit.register(stop_mounts)


class DeltaError(RuntimeError):
    pass


class DeltaUnavailable(DeltaError):
    pass


class DeltaConflict(DeltaError):
    pass


def _integer(value):
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _retryable_cli_error(error):
    """Trust only an explicit, well-formed retry instruction from the v7 API."""
    # Published v7 error-codes.json, pinned with the recorder's protocol reference.
    codes = {
        "authentication_required", "wrong_account", "not_visible", "organization_required",
        "not_shared", "invalid_reference", "invalid_record", "sequence_gap", "watch_reset",
        "history_reset", "reload_required", "checkout_inactive", "session_ended",
        "incompatible_app", "app_unavailable", "invalid_invocation", "failed",
    }
    return (
        isinstance(error, dict)
        and isinstance(error.get("code"), str) and error["code"] in codes
        and isinstance(error.get("message"), str)
        and ("reason" not in error or isinstance(error["reason"], str))
        and error.get("retryable") is True
    )


def projected_message_count(updates):
    """Count rows in the passive message projection, not append receipts.

    Delta keeps each user message and coalesces consecutive assistant messages,
    even with distinct IDs. State/tool records do not add message projection rows.
    """
    count = 0
    previous = None
    for update in updates:
        role = update.get("sessionUpdate")
        if role not in {"user_message", "agent_message"}:
            continue
        if role == "user_message" or role != previous:
            count += 1
        previous = role
    return count


def delivery_health(session):
    """Classify observed delivery evidence, without inferring cloud ACKs.

    ``None`` means no delivery warning in the recorded observation. Callers must
    separately account for source coverage and observation freshness.
    """
    visibility = session.get("visibility")
    upload = session.get("upload")
    if session.get("halted") or session.get("failed"):
        return "needs_review"
    if visibility is not None and visibility != "private":
        return "needs_review"
    if isinstance(upload, dict) and (upload.get("state") == "failed" or upload.get("error") is not None):
        return "needs_review"
    access = session.get("access")
    if (
        session.get("pending", 0)
        or visibility != "private"
        or not isinstance(upload, dict)
        or upload.get("state") != "uploaded"
        or not isinstance(access, dict)
        or access.get("state") != "granted"
    ):
        return "catching_up"
    return None


class DeltaSink:
    SOURCE = "t3-code"
    connection_owned_mounts = True

    def __init__(self, state_dir: Path, binary: str = "zed-delta", timeout=30, *, organization=None, producer_name="T3 Code"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.state_dir.is_symlink():
            raise DeltaError("Recorder state directory must not be a symlink")
        self.state_dir.chmod(0o700)
        self.binary = binary
        self.timeout = timeout
        self.organization = organization
        self.producer_name = producer_name
        database = self.state_dir / "sink.sqlite3"
        if database.is_symlink():
            raise DeltaError("Recorder database must not be a symlink")
        fd = os.open(database, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        os.close(fd)
        database.chmod(0o600)
        self.db = sqlite3.connect(database, timeout=timeout)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                external_id TEXT PRIMARY KEY, directory TEXT NOT NULL,
                thread_id TEXT, reference TEXT, next_sequence INTEGER NOT NULL DEFAULT 1,
                halted INTEGER NOT NULL DEFAULT 0, error TEXT,
                upload TEXT, access TEXT, last_ack REAL, attached_at REAL,
                organization TEXT, checkout_status TEXT, checked_at REAL, visibility TEXT
            );
            CREATE TABLE IF NOT EXISTS records (
                external_id TEXT NOT NULL REFERENCES sessions(external_id),
                sequence INTEGER NOT NULL, item_key TEXT NOT NULL, body TEXT NOT NULL,
                update_json TEXT NOT NULL, turn_sequence INTEGER,
                acknowledged INTEGER NOT NULL DEFAULT 0, disposition TEXT, receipt_version TEXT,
                PRIMARY KEY(external_id, sequence), UNIQUE(external_id, item_key)
            );
        """)
        # Local pilot state can survive incremental recorder upgrades.
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(sessions)")}
        for column, sql_type in (("organization", "TEXT"), ("checkout_status", "TEXT"), ("checked_at", "REAL"), ("visibility", "TEXT")):
            if column not in columns:
                self.db.execute(f"ALTER TABLE sessions ADD COLUMN {column} {sql_type}")
        record_columns = {row[1] for row in self.db.execute("PRAGMA table_info(records)")}
        if "receipt_version" not in record_columns:
            self.db.execute("ALTER TABLE records ADD COLUMN receipt_version TEXT")
        self.db.commit()

    def close(self):
        self.db.close()

    def release_session(self, external_id):
        session = self._session(external_id)
        stop_mounts([(self.binary, session["reference"], session["directory"])])

    def _run(self, args, *, input=None):
        try:
            return subprocess.run(
                [self.binary, "--json", *args], input=input,
                capture_output=True, text=True, timeout=self.timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise DeltaUnavailable("Delta command unavailable or timed out; pending records retained") from exc

    def _command(self, args):
        result = self._run(args)
        try:
            value = json.loads(result.stdout)
        except (TypeError, ValueError) as exc:
            raise DeltaConflict("Delta returned invalid JSON; no successful operation assumed") from exc
        if isinstance(value, dict) and "error" in value:
            if _retryable_cli_error(value["error"]):
                raise DeltaUnavailable("Delta reported a retryable command failure; existing state retained")
            raise DeltaConflict("Delta returned a nonretryable or unrecognized error; review required")
        if result.returncode:
            raise DeltaConflict("Delta command failed without a recognized retry instruction; review required")
        return value

    def _contract(self):
        value = self._command(["thread", "contract"])
        if not isinstance(value, dict) or value.get("operation") != "contract":
            raise DeltaConflict("Unknown Delta recording contract")
        low, high = value.get("min"), value.get("max")
        if not _integer(low) or not _integer(high) or low > high or low > 8 or high < 6 or high > 8:
            raise DeltaConflict("Delta recording contract is incompatible; recorder supports 6–8")
        return min(high, 8)

    def _mount(self, session):
        key = (self.binary, session["reference"], session["directory"])
        previous = _MOUNTS.get(key)
        if previous is not None and previous.poll() is None:
            return
        if previous is not None:
            if previous.stdout:
                previous.stdout.close()
            del _MOUNTS[key]
        # Stale exited children must not consume the process-wide lease budget.
        stop_mounts([item for item, process in _MOUNTS.items() if process.poll() is not None])
        if len(_MOUNTS) >= MAX_MOUNTS:
            raise DeltaUnavailable("Delta mount connection budget reached; pending records retained")
        try:
            process = subprocess.Popen([
                self.binary, "--json", "worktree", "mount", session["reference"],
                session["directory"], "--existing-checkout",
            ], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        except OSError as error:
            raise DeltaUnavailable("Delta mount command unavailable; existing binding retained") from error
        try:
            deadline = time.monotonic() + self.timeout
            output = b""
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                while b"\n" not in output:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0 or not selector.select(remaining):
                        raise DeltaUnavailable("Delta mount readiness timed out; existing binding retained")
                    chunk = os.read(process.stdout.fileno(), 65536)
                    if not chunk or len(output) + len(chunk) > 65536:
                        raise DeltaUnavailable("Delta mount readiness missing or exceeded its budget")
                    output += chunk
            mounted = json.loads(output.split(b"\n", 1)[0])
            if isinstance(mounted, dict) and "error" in mounted:
                if _retryable_cli_error(mounted["error"]):
                    raise DeltaUnavailable("Delta mount is temporarily unavailable; existing binding retained")
                raise DeltaConflict("Delta mount refused; existing binding retained for review")
            if (not isinstance(mounted, dict) or mounted.get("operation") != "mounted"
                    or mounted.get("reference") != session["reference"]
                    or mounted.get("directory") != session["directory"]):
                raise DeltaConflict("Delta mount receipt differs from the persisted checkout binding")
            if process.poll() is not None:
                raise DeltaUnavailable("Delta mount connection ended before readiness verification")
            _MOUNTS[key] = process
        except BaseException:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()
            raise

    def _attach_v8(self, session, preliminary):
        external_id = session["external_id"]
        if not session["reference"]:
            # A prior attach may have succeeded before its local receipt was
            # saved. Reuse exactly one matching existing worktree when present.
            trees = preliminary.get("worktrees", {})
            matches = {tree["reference"] for tree in trees.get("items", [])
                       if isinstance(tree, dict) and isinstance(tree.get("reference"), str)
                       and any(isinstance(c, dict) and c.get("directory") == session["directory"]
                               for c in tree.get("checkouts", []))}
            if len(matches) > 1:
                raise DeltaConflict("Existing checkout has ambiguous Delta worktree bindings")
            if matches:
                reference = matches.pop()
            else:
                listed = self._command(["worktree", "list"])
                if (not isinstance(listed, dict) or listed.get("operation") != "listed"
                        or not isinstance(listed.get("worktrees"), list)):
                    raise DeltaError("Invalid Delta local worktree inventory")
                local = {tree["reference"] for tree in listed["worktrees"]
                         if isinstance(tree, dict) and isinstance(tree.get("reference"), str)
                         and any(isinstance(c, dict) and c.get("directory") == session["directory"]
                                 for c in tree.get("directories", []))}
                if len(local) > 1:
                    raise DeltaConflict("Existing checkout has ambiguous local worktree bindings")
                if local:
                    reference = local.pop()
                else:
                    created = self._command(["worktree", "create", session["directory"]])
                    if (not isinstance(created, dict) or created.get("operation") != "created"
                            or not isinstance(created.get("worktree_id"), str) or not created["worktree_id"]
                            or created.get("reference") != created["worktree_id"]):
                        raise DeltaError("Invalid Delta worktree creation receipt")
                    reference = created["reference"]
            # Persist before membership/mount so retries never replace the binding.
            with self.db:
                self.db.execute("UPDATE sessions SET reference=? WHERE external_id=?", (reference, external_id))
            session = self._session(external_id)
        if session["attached_at"] is None:
            attached = self._command(["worktree", "attach", session["reference"], "--thread", session["thread_id"]])
            def history(reference):
                return reference.removeprefix("delta://worktree/").split("#", 1)[0]
            if (not isinstance(attached, dict) or attached.get("operation") != "attached"
                    or not isinstance(attached.get("reference"), str) or not attached["reference"]
                    or history(attached["reference"]) != history(session["reference"])
                    or attached.get("thread_id") != session["thread_id"]):
                raise DeltaError("Invalid Delta worktree membership receipt")
            with self.db:
                self.db.execute("UPDATE sessions SET reference=? WHERE external_id=?", (attached["reference"], external_id))
            session = self._session(external_id)
        self._mount(session)
        with self.db:
            self.db.execute("UPDATE sessions SET attached_at=COALESCE(attached_at,?) WHERE external_id=?", (time.time(), external_id))

    def _session(self, external_id):
        row = self.db.execute("SELECT * FROM sessions WHERE external_id=?", (external_id,)).fetchone()
        if row is None:
            raise DeltaError("Unknown recorder session")
        return dict(row)

    def _upload_organization(self):
        if self.organization:
            return self.organization
        value = self._command(["org", "list"])
        organizations = value.get("organizations") if isinstance(value, dict) else None
        if not isinstance(organizations, list) or not all(
            isinstance(org, dict) and isinstance(org.get("id"), str) and org["id"]
            and type(org.get("current")) is bool for org in organizations
        ):
            raise DeltaError("Invalid organization list; private cloud upload not configured")
        current = [org for org in organizations if org["current"]]
        choices = current if current else organizations
        if len(choices) != 1:
            raise DeltaError("Select one Delta organization before starting private recording")
        return choices[0]["id"]

    def _conflict(self, external_id, message):
        with self.db:
            self.db.execute("UPDATE sessions SET halted=1,error=? WHERE external_id=?", (message, external_id))
        raise DeltaConflict(message)

    def _live_status(self, session):
        value = self._command([
            "thread", "status", session["thread_id"], "--source", self.SOURCE,
            "--external-id", session["external_id"],
        ])
        if not isinstance(value, dict) or value.get("operation") != "status" or value.get("thread_id") != session["thread_id"]:
            self._conflict(session["external_id"], "Delta returned a different or invalid thread status")
        # v7 ThreadState explicitly permits null before the cloud record exists.
        # Missing fields or granted cloud access with unknown visibility are not
        # evidence that the requested private visibility actually took effect.
        access = value.get("access")
        if (
            not isinstance(access, dict)
            or access.get("state") not in {"local_only", "granted"}
            or "is_owner" not in access
            or (access["is_owner"] is not None and type(access["is_owner"]) is not bool)
        ):
            self._conflict(session["external_id"], "Invalid Delta access status")
        if "visibility" not in value or (value["visibility"] is None and access["state"] != "local_only"):
            self._conflict(session["external_id"], "Delta cloud visibility is unknown")
        if value["visibility"] is not None and value["visibility"] != "private":
            self._conflict(session["external_id"], "Pilot thread visibility is not private")
        upload = value.get("upload")
        if "upload" not in value or (upload is not None and not self._valid_upload(upload)):
            self._conflict(session["external_id"], "Invalid Delta upload status")
        producer = value.get("producer")
        if not isinstance(producer, dict) or producer.get("source") != self.SOURCE or producer.get("external_id") != session["external_id"] or not _integer(producer.get("next_sequence")):
            self._conflict(session["external_id"], "Delta producer identity or cursor differs from persisted binding")
        return value

    @staticmethod
    def _valid_upload(upload):
        if not isinstance(upload, dict) or upload.get("state") not in {"queued", "uploading", "uploaded", "failed"}:
            return False
        if "organization_id" not in upload or "error" not in upload:
            return False
        organization = upload["organization_id"]
        if organization is not None and (not isinstance(organization, str) or not organization):
            return False
        error = upload["error"]
        return error is None or (
            isinstance(error, dict) and isinstance(error.get("code"), str)
            and isinstance(error.get("message"), str)
            and ("reason" not in error or isinstance(error["reason"], str))
        )

    def _checkout(self, session, value, *, connection_owned=False):
        trees = value.get("worktrees")
        if not isinstance(trees, dict) or trees.get("status") != "available" or not isinstance(trees.get("items"), list):
            raise DeltaUnavailable("Delta checkout inspection unavailable")
        for tree in trees["items"]:
            if not isinstance(tree, dict) or tree.get("reference") != session["reference"]:
                continue
            for checkout in tree.get("checkouts", []):
                if not isinstance(checkout, dict) or checkout.get("directory") != session["directory"]:
                    continue
                ready = (checkout.get("state") == "attached" and checkout.get("active", True) is True
                         or connection_owned and checkout.get("state") == "detached" and checkout.get("active") is True)
                if ready:
                    with self.db:
                        self.db.execute("UPDATE sessions SET checkout_status=?,checked_at=? WHERE external_id=?", (_json(checkout), time.time(), session["external_id"]))
                    return
                if connection_owned and checkout.get("state") in ("attached", "detached", "inactive"):
                    raise DeltaUnavailable("Delta mount is not active in thread status yet; binding retained")
        self._conflict(session["external_id"], "Delta checkout/reference no longer matches the attached directory")

    def ensure_session(self, external_id: str, directory: str):
        if not isinstance(external_id, str) or not external_id:
            raise DeltaError("A stable external session ID is required")
        directory = str(Path(directory).resolve(strict=True))
        if not Path(directory).is_dir():
            raise DeltaError("Checkout directory does not exist")
        row = self.db.execute("SELECT directory FROM sessions WHERE external_id=?", (external_id,)).fetchone()
        if row and row[0] != directory:
            self._conflict(external_id, "T3 checkout changed; explicit new binding required")
        with self.db:
            self.db.execute("INSERT OR IGNORE INTO sessions(external_id,directory) VALUES (?,?)", (external_id, directory))
        session = self._session(external_id)
        if session["halted"]:
            raise DeltaConflict(session["error"])
        contract = self._contract()
        if not session["organization"]:
            organization = self._upload_organization()
            with self.db:
                self.db.execute("UPDATE sessions SET organization=? WHERE external_id=?", (organization, external_id))
            session = self._session(external_id)
        if not session["thread_id"]:
            created = self._command([
                "thread", "create", "--source", self.SOURCE, "--external-id", external_id,
                "--organization", session["organization"], "--visibility", "private",
            ])
            if not isinstance(created, dict) or created.get("operation") != "created" or not isinstance(created.get("thread_id"), str) or not created["thread_id"]:
                raise DeltaError("Invalid Delta create receipt; retry only with the same producer identity")
            with self.db:
                self.db.execute("UPDATE sessions SET thread_id=? WHERE external_id=?", (created["thread_id"], external_id))
            session = self._session(external_id)
        # A reused producer with a lost local ledger must not attach/upload a new checkout.
        preliminary = self._live_status(session)
        if preliminary["producer"]["next_sequence"] > session["next_sequence"]:
            self._conflict(external_id, "Delta producer already has history absent from this outbox")
        if contract == 8:
            self._attach_v8(session, preliminary)
            session = self._session(external_id)
        elif not session["reference"]:
            attached = self._command(["worktree", "attach", directory, "--thread", session["thread_id"]])
            if not isinstance(attached, dict) or attached.get("operation") != "mounted" or attached.get("directory") != directory or not isinstance(attached.get("reference"), str) or not attached["reference"]:
                raise DeltaError("Invalid Delta attach receipt; no checkout tracking assumed")
            with self.db:
                self.db.execute("UPDATE sessions SET reference=?,attached_at=? WHERE external_id=?", (attached["reference"], time.time(), external_id))
            session = self._session(external_id)
        live = self._live_status(session)
        self._checkout(session, live, connection_owned=contract == 8)
        self._remember_status(external_id, live)
        return self._metadata(self._session(external_id))

    def enqueue(self, external_id: str, key: str, update: dict, turn_sequence=None):
        if not isinstance(key, str) or not key or not isinstance(update, dict):
            raise DeltaError("A stable item key and update object are required")
        kind = update.get("sessionUpdate")
        if kind not in {"user_message", "agent_message", "tool_call_update", "state_update"}:
            raise DeltaError("Unsupported Delta update type")
        if kind in {"user_message", "agent_message"} and (not isinstance(update.get("messageId"), str) or not update["messageId"] or not isinstance(update.get("content"), list)):
            raise DeltaError("Messages require messageId and content")
        if kind == "state_update" and update.get("state") not in {"running", "idle"}:
            raise DeltaError("Unsupported execution state")
        if kind == "tool_call_update" and (not isinstance(update.get("toolCallId"), str) or not update["toolCallId"] or update.get("status") not in {"in_progress", "completed", "failed"}):
            raise DeltaError("Tools require toolCallId and supported status")
        encoded_update = _json(update)
        session = self._session(external_id)
        if session["halted"]:
            raise DeltaConflict(session["error"])
        prior = self.db.execute("SELECT * FROM records WHERE external_id=? AND item_key=?", (external_id, key)).fetchone()
        sequence = prior["sequence"] if prior else session["next_sequence"]
        if kind == "state_update" and update["state"] == "running" and turn_sequence is None:
            turn_sequence = sequence
        if turn_sequence is not None and (not _integer(turn_sequence) or turn_sequence > sequence or (turn_sequence == sequence and not (kind == "state_update" and update["state"] == "running"))):
            raise DeltaError("Invalid turn sequence")
        if kind == "state_update" and turn_sequence is None:
            raise DeltaError("Idle state requires its running turn sequence")
        if prior:
            if prior["update_json"] != encoded_update or prior["turn_sequence"] != turn_sequence:
                self._conflict(external_id, "An immutable queued item changed; no sequence reused")
            return sequence
        body = {"schemaVersion": 1, "sequence": sequence, "update": update}
        if turn_sequence is not None:
            body["turnSequence"] = turn_sequence
        line = _json(body) + "\n"
        if len(line.encode("utf-8")) > 16 * 1024 * 1024:
            raise DeltaError("Delta JSONL record exceeds 16 MiB")
        with self.db:
            self.db.execute("INSERT INTO records(external_id,sequence,item_key,body,update_json,turn_sequence) VALUES (?,?,?,?,?,?)", (external_id, sequence, key, line, encoded_update, turn_sequence))
            self.db.execute("UPDATE sessions SET next_sequence=? WHERE external_id=?", (sequence + 1, external_id))
        return sequence

    def _remember_status(self, external_id, live):
        with self.db:
            self.db.execute("UPDATE sessions SET upload=?,access=?,visibility=?,checked_at=? WHERE external_id=?", (_json(live.get("upload")), _json(live.get("access")), live.get("visibility"), time.time(), external_id))

    def flush(self, external_id: str, *, maintain_mount=True):
        session = self._session(external_id)
        if session["halted"]:
            raise DeltaConflict(session["error"])
        if not session["thread_id"] or not session["reference"]:
            raise DeltaError("Session attachment has not completed")
        contract = self._contract()
        if contract == 8 and maintain_mount:
            if session["attached_at"] is None:
                self._attach_v8(session, self._live_status(session))
                session = self._session(external_id)
            else:
                self._mount(session)
        live = self._live_status(session)
        self._checkout(session, live, connection_owned=contract == 8)
        self._remember_status(external_id, live)
        # Private creation/attachment can return before its asynchronous cloud
        # binding exists. Server-ack append must wait for observed granted access;
        # interpreting the resulting not_shared refusal as retryable is unsafe.
        upload = live["upload"]
        if isinstance(upload, dict) and upload["state"] == "failed":
            raise DeltaError("Private cloud upload failed; pending records retained for review")
        if live["visibility"] != "private" or live["access"]["state"] != "granted" or upload is None:
            raise DeltaUnavailable("Private cloud binding is not ready; pending records retained")
        rows = self.db.execute("SELECT * FROM records WHERE external_id=? AND acknowledged=0 ORDER BY sequence", (external_id,)).fetchall()
        frontier = rows[0]["sequence"] if rows else session["next_sequence"]
        if not frontier <= live["producer"]["next_sequence"] <= session["next_sequence"]:
            self._conflict(external_id, "Delta sequence frontier conflicts with durable outbox")
        if not rows:
            return self.status(external_id)
        batch, size = [], 0
        for row in rows:
            length = len(row["body"].encode("utf-8"))
            if batch and size + length > 2_000_000:
                break
            batch.append(row)
            size += length
        result = self._run([
            "thread", "append", session["thread_id"], "--source", self.SOURCE,
            "--external-id", external_id, "--producer-name", self.producer_name, "--acknowledge", "server",
        ], input="".join(row["body"] for row in batch))
        acknowledged = 0
        previous_frontier = live["producer"]["next_sequence"]
        fenced = False
        invalid = False
        retryable_failure = False
        for line in result.stdout.splitlines():
            try:
                receipt = json.loads(line)
                if isinstance(receipt, dict) and "error" in receipt:
                    retryable_failure = _retryable_cli_error(receipt["error"])
                    invalid = not retryable_failure
                    break
                if acknowledged >= len(batch):
                    invalid = True
                    break
                row = batch[acknowledged]
                disposition = receipt.get("disposition")
                valid = (
                    receipt.get("operation") == "appended" and receipt.get("thread_id") == session["thread_id"]
                    and type(receipt.get("sequence")) is int and receipt["sequence"] == row["sequence"]
                    and _integer(receipt.get("next_sequence")) and row["sequence"] < receipt["next_sequence"] <= session["next_sequence"]
                    and receipt["next_sequence"] >= previous_frontier
                    and type(receipt.get("duplicate")) is bool and disposition in {"recorded", "duplicate", "fenced"}
                    and receipt["duplicate"] == (disposition == "duplicate")
                    and isinstance(receipt.get("version"), str) and bool(receipt["version"])
                    and "turn_sequence" in receipt and receipt["turn_sequence"] == row["turn_sequence"]
                    and (receipt.get("turn_sequence") is None or _integer(receipt["turn_sequence"]))
                )
            except (ValueError, AttributeError, TypeError):
                valid = False
            if not valid:
                invalid = True
                break
            with self.db:
                # Retain the receipt's opaque version as observed evidence. It is
                # not asserted to be a file snapshot for this individual record.
                self.db.execute("UPDATE records SET acknowledged=1,disposition=?,receipt_version=? WHERE external_id=? AND sequence=?", (disposition, receipt["version"], external_id, row["sequence"]))
                self.db.execute("UPDATE sessions SET last_ack=?,error=NULL WHERE external_id=?", (time.time(), external_id))
            acknowledged += 1
            previous_frontier = receipt["next_sequence"]
            if disposition == "fenced":
                fenced = True
                break
        if fenced:
            self._conflict(external_id, "Delta fenced this producer turn; recording stopped for review")
        if invalid:
            self._conflict(external_id, "Invalid Delta receipt; only verified prefix acknowledged")
        if retryable_failure:
            raise DeltaUnavailable("Delta reported a retryable delivery failure; unacknowledged records retained for identical replay")
        if result.returncode or acknowledged != len(batch):
            raise DeltaUnavailable("Delta delivery incomplete; unacknowledged records retained for identical replay")
        # Upload state is an observation, never inferred from transcript receipts.
        try:
            self._remember_status(external_id, self._live_status(self._session(external_id)))
        except DeltaUnavailable:
            pass
        return self.status(external_id)

    def _metadata(self, session):
        session = dict(session)
        session["halted"] = bool(session["halted"])
        for field in ("upload", "access", "checkout_status"):
            session[field] = json.loads(session[field]) if session[field] else None
        session["pending"] = self.db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=0", (session["external_id"],)).fetchone()[0]
        session["acknowledged"] = self.db.execute("SELECT COUNT(*) FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced'", (session["external_id"],)).fetchone()[0]
        session["message_count"] = self._message_count(session["external_id"])
        return session

    def _message_count(self, external_id):
        return projected_message_count(
            json.loads(row[0]) for row in self.db.execute(
                "SELECT update_json FROM records WHERE external_id=? AND acknowledged=1 AND disposition!='fenced' ORDER BY sequence",
                (external_id,),
            )
        )

    def sessions(self):
        return [self._metadata(row) for row in self.db.execute("SELECT * FROM sessions ORDER BY external_id").fetchall()]

    def status(self, external_id=None):
        if external_id is not None:
            return self._metadata(self._session(external_id))
        sessions = self.sessions()
        return {"sessions": sessions, "pending": sum(s["pending"] for s in sessions), "halted": sum(s["halted"] for s in sessions)}

    def history(self, external_id, first=1, last=20):
        session = self._session(external_id)
        if not session["thread_id"]:
            raise DeltaError("Delta thread has not been created")
        if not _integer(first) or not _integer(last) or last < first:
            raise DeltaError("History requires a positive inclusive message range")
        # Receipt count differs from the visible projection: consecutive assistant
        # messages coalesce, even when they have distinct message IDs.
        known = self._message_count(external_id)
        if first > known:
            return []
        last = min(last, first + 19, known)
        return self._command([
            "thread", "show", session["thread_id"], "--format", "messages",
            "--messages", f"{first}-{last}",
        ])
