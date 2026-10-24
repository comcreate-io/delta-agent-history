# Delta Agent History design

Status: approved implementation direction for version 0.2. Support and live
verification limits are maintained in [compatibility](compatibility.md).

## Outcome

Make recording and project recall independent of the coding agent, model provider,
and user interface. Keep Delta as the history backend. Support Linux and macOS.
Retain the existing T3 integration as one adapter rather than making it the core.

Recommended public name: **Delta Agent History**, repository
`comcreate-io/delta-agent-history`, command `delta-history`. Retain `t3-delta`
compatibility entrypoints and document the repository move. Do not switch the
existing installed service or rewrite its state as part of a branding change.

## Separate capture from recall

Capture needs a supported event feed, hook, SDK, or explicit export from the
source application. A common protocol makes these integrations possible; it does
not grant access to every application's full conversation stream.

Recall uses the same repository-scoped CLI regardless of where the messages
originated. A local stdio MCP server exposes read-only status, session listing,
and bounded history tools for clients supporting that transport. MCP tool access
alone does not establish passive transcript capture. A desktop client requiring
remote MCP needs a separately reviewed authenticated deployment; this release
must not expose a local history server to the network automatically.

## Architecture

1. **Neutral recorder core:** exact repository enrollment, private attachments,
   immutable outbox, receipt validation, retries, status, and bounded retrieval.
   Generic operation requires Delta, not a T3 account, runtime file, or credential.
2. **Versioned ingress:** JSON Lines over stdin or an explicitly selected private
   input file. Hooks and wrappers submit structured events through a literal
   executable. No terminal scraping, arbitrary plugin execution, home-directory
   discovery, or private provider history/credential reads.
3. **Thin source adapters:** translate documented application events into ingress
   records. T3 retains its own read-only API discovery, pagination consistency,
   version checks, terminal-prefix ordering, and model-label handling.
4. **Neutral recall:** a CLI, optional read-only MCP server, portable skill, and
   instruction block. Filter by the locally approved repository before fetching
   history. Content from one harness can inform work in another.

## Integration targets and honest coverage

| Source | Proposed capture path | Verification requirement |
|---|---|---|
| T3 Code | Existing read-only MCP adapter | Preserve current live-tested behavior and replay compatibility. |
| Claude Code | Documented prompt/completion hooks | Validate exact event/version shapes; state whether coverage is final answers only. Do not infer full streamed history from a Stop event. |
| Codex CLI | Wrapper around documented `exec --json` output, with the submitted prompt | Scope initially to wrapped noninteractive runs, not all interactive CLI or desktop sessions. |
| OpenCode | Documented plugin events and supported message reads | Reconcile streamed parts into complete ordered messages; verify the chosen plugin/API version. |
| Custom agents and other harnesses | Stable ingress protocol and reference producer | Any producer can integrate by supplying the required events; it is not automatically connected merely by being installed. |
| ChatGPT and other desktop apps | Supported integration or explicitly supplied export | No claim of complete background capture until an official usable feed and its coverage are verified. Exported text does not recreate historical checkout states. |

Hook availability establishes a candidate integration path, not a tested adapter.
Published support labels must distinguish live verified, fixture tested,
reference integration, and unsupported automatic capture.

## Ingress contract

Define a versioned schema for `session.open`, `message.completed`, and
`session.close`. Require source kind, configured source-instance ID, stable session
and event IDs, monotonic sequence, and explicit timestamps. Completed messages
contain a visible role and text, with optional source/model labels. Reject hidden
reasoning, raw tool output, attachments, malformed/unknown event shapes, and
over-budget input.

At session opening, bind the actual Git checkout to a locally approved path and
origin, verify linked-worktree membership where applicable, and capture the
directory fingerprint before attachment. Caller-supplied paths, source names,
timestamps, and titles never authorize enrollment. Preserve receipt time as well
as source time; an imported timestamp is not proof of prior file coverage.

Persist accepted events privately before acknowledging local ingestion. Local
acceptance, Delta acknowledgement, completed upload, and recoverability must
remain separate states. Identical duplicates are safe; conflicting IDs or bodies
halt that session. Sequence gaps wait for repair rather than silently dropping
records. Concurrent hooks serialize through a bounded local queue, not concurrent
mutations of the existing writer ledger.

Start with prospective recording only. Any later historical import must be an
explicit separate mode with transcript-only coverage and its own privacy review.

## Compatibility and migration

Existing T3 recordings are identified by producer `t3-code`, external ID
`environment:thread`, message IDs prefixed `t3:`, frozen labels, and immutable
queued JSONL. Keep every one of those unchanged on existing bindings.

New generic sessions use a fresh producer and source-instance-qualified session
keys. Persist producer identity per binding or isolate legacy source state. Never
globally replace the sink's producer constant and replay old data under the new
identity. Schema migration must be transactional, preserve pending payload bytes,
and support an explicit backup/rollback procedure. Keep old configuration and
command aliases working without starting a second worker or expanding scope.

## First implementation batch

1. Introduce the normalized core and versioned ingress, with a synthetic producer
   that records a disposable repository without T3 installed.
2. Add neutral CLI recall and read-only local MCP, then prove a second client can
   retrieve the first producer's messages within the same repository boundary.
3. Preserve T3 as a compatibility adapter. Add the documented Claude hook,
   Codex exec, and OpenCode integrations with explicit coverage labels and setup.
4. Update skills, Linux/macOS service tooling, packaging, migration instructions,
   and public naming after verification. Preserve the installed legacy service.

Required checks: the existing suite; multi-source ID collision tests; duplicate,
sequence-gap, interrupted-delivery and conflicting-body tests; unapproved-path
and cross-repository recall refusals; producer migration with pending records;
input budgets; synthetic adapter fixtures; stdio MCP protocol tests; Ubuntu and
macOS package/service CI. Live account and desktop verification are reported
separately. Historical point-in-time recovery remains outside this batch.

## Primary references checked

- [Existing recorder source](../t3_delta/engine.py) and [Delta sink](../t3_delta/delta_sink.py).
- [Claude Code hook reference](https://code.claude.com/docs/en/hooks).
- [Codex noninteractive JSONL output](https://learn.chatgpt.com/docs/non-interactive-mode).
- [OpenCode plugin events](https://opencode.ai/docs/plugins/).
- [MCP tools specification](https://modelcontextprotocol.io/specification/2025-06-18/server/tools).
