# Ingress v1

The machine-readable [schema](../schemas/event.schema.json) and runtime validator
specify a visible-message protocol, independent of model provider and harness.
Runtime checks also enforce UTF-8 byte budgets, scope, ordering and immutable
identity. JSON Schema validation alone does not authorize an upload.

Every event includes `schema_version:1`, `type`, `source`, `source_instance`,
`session_id`, `event_id`, positive integer `sequence`, and UTC `created_at`.
Source kind and instance are lowercase slugs of at most 64 characters, explicitly
enrolled in config. Session and event IDs are bounded to 256 UTF-8 bytes.

| Type | Extra required fields | Meaning |
|---|---|---|
| `session.open` | `project`, sequence 1 | Bind an approved actual checkout before messages. |
| `message.completed` | `role`, `text` | Visible user, assistant or plan text; optional `model`. |
| `session.close` | `status` | Completed, failed or cancelled; no subsequent new messages. |

No tool output, hidden reasoning, attachment, credential or arbitrary metadata
fields are accepted. Only complete visible text is eligible. Current limits:
1 MiB per record,256 KiB message text,4 MiB and 100 records per ingestion batch.
Ingress and MCP JSON requests are limited to 64 nested arrays or objects;
brackets inside quoted text do not count toward this limit.
Unknown fields/types, duplicate JSON keys, malformed input and over-budget data
are rejected. JSON Schema character limits do not replace runtime byte limits.

```json
{"schema_version":1,"type":"session.open","source":"generic","source_instance":"local","session_id":"example-session","event_id":"open","sequence":1,"created_at":"2026-01-01T00:00:00+00:00","project":"/absolute/approved/project"}
```

This is structural illustration. Use a current timestamp after enrollment and
actual approved path. [The reference producer](../examples/producer.py) emits a
fresh synthetic session with no harness dependency.

## Delivery semantics

`delta-history ingest` accepts JSONL on stdin. `--file PATH` requires an explicitly
chosen regular private file owned by the current user and refuses symlinks.
It does not search provider caches or import old session stores. Stdout contains
acknowledgement metadata, not transcript echoes. The drainer runs separately.

Persisted identity is qualified by source kind, instance and session ID. Event IDs
and sequences are immutable. Resubmitting the same canonical event is safe;
changing its body, ID/sequence association, project binding or terminal boundary
halts that session. Gaps wait for missing records. New messages require an already
accepted open event. Session close can wait behind a gap, but cannot precede
already accepted later messages.

Adapters must freeze timestamps, event IDs and text before acknowledgement and
retry the identical payload after an interruption. A retry is not a new event.
Do not deduplicate legitimate repeated prompts by content hash alone. Hooks use a
private correlation journal and bounded local submissions; they do not wait for
Delta's network acknowledgement.

Local acceptance, sink enqueue, Delta acknowledgement and completed upload are
separate observations. Neither source labels nor caller timestamps prove capture
coverage. Checkout file history starts after attachment. Historical transcript
backfill is not provided by this prospective interface.
