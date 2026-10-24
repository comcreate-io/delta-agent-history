# Architecture

The bridge has two public integration boundaries: read-only T3 MCP for source conversations, and Zed Delta's supported CLI for recording and retrieval. It never reads either application's private database. Its own SQLite ledgers are recorder-owned implementation state.

## Source and scope

The runtime file normally supplies T3's loopback origin. The recorder uses a dedicated `orchestration:read`, `mcp-client` session and verifies the expected environment ID. It reads project, thread, and bounded conversation metadata through supported tools. Endpoint validation rejects credentials in URLs and nonloopback cleartext HTTP destinations; explicitly configured HTTPS endpoints are also permitted.

Setup writes exact path-and-origin rules for chosen repositories. The engine resolves the actual checkout and checks common-directory membership for linked worktrees. A matching remote alone does not authorize another clone. Initial attachment screens tracked and nonignored filenames for common sensitive-looking names. This heuristic does not inspect contents or replace repository review.

Only conversations created after the effective cutoff are eligible. That cutoff is the later of repository enrollment and first recorder run. Existing conversations are not backfilled. Completed user and assistant messages, visible plans, and available run/model labels are eligible. Hidden reasoning, attachments, and raw tool activity are excluded.

Snapshots must remain consistent while paging. Truncated text is recovered through bounded supported reads. An active assistant turn blocks later records from overtaking it. Previously completed content that changes unexpectedly causes review rather than silent replacement.

## Durable recording

`source.sqlite3` tracks source bindings, attachment fingerprints, observed labels, exported hashes, and source problems. `sink.sqlite3` tracks Delta bindings, immutable queued payloads, acknowledgements, and separately observed cloud state. State directories and credential files are private to the current user. A writer lock prevents concurrent recorder processes using the same state directory.

Each source conversation is identified by T3 environment and thread ID. The sink creates a private Delta conversation using producer name `t3-code`, attaches the actual existing checkout, and validates the returned producer and checkout binding. It does not take turn ownership in T3 or request organization sharing.

The sink waits for private cloud initialization before delivery. Local-only initialization can be transient and is not a completed upload. Immutable JSONL records have stable sequence numbers and item keys. Receipt validation, duplicate-safe replay, and the durable outbox allow restart recovery without rewriting original payloads.

Queued delivery can continue when T3 is temporarily unavailable or no longer lists a source thread. Source problems do not authorize new capture or attachment. Conflicting identity, fencing, unsupported versions, and invalid receipts fail closed for review. Known retryable upstream errors preserve the queue.

## Health and history

`t3-delta status` reads local recorder ledgers without fetching transcripts. Distinguish:

- Service/process availability.
- Source observation and checkout attachment times.
- Pending records and transcript acknowledgements.
- Delta visibility, access, and cloud upload state.
- Independently verified file recovery.

`t3-delta history` uses the saved T3-to-Delta binding and public CLI for a bounded message range. Consecutive assistant records can become one Delta message, so message count is a projection rather than raw record count. The command clamps range ends to acknowledged coverage.

The skill wraps those commands with an agent workflow. It does not change capture eligibility, background behavior, or restore semantics.

## Lifecycle and limits

`init` enrolls offline and refuses overwrites. `auth` explicitly issues and validates a T3 credential. `run` owns polling and can upload. Service renderers generate per-user systemd or launchd files; activation is separate.

The poller processes sessions serially. With many child conversations, a cycle can exceed its nominal interval and appear stale or catching up. A running process is not proof of current backup.

Files are observed after checkout attachment. Early changes can be missed. Git worktrees isolate concurrent tasks; separate conversations sharing one checkout do not. No restore operation is exposed. An opaque receipt version is not assumed to be a valid historical-version argument for another command. Restoring files would not rewind T3. See [compatibility](compatibility.md).
