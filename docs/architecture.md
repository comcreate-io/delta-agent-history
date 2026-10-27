# Architecture

The public integration boundaries are versioned visible-message ingress, optional
documented source adapters, read-only local MCP/CLI recall, and Delta's public CLI.
No component reads private provider databases, raw session stores or credentials.
All SQLite journals below are owned by this recorder.

## Neutral capture and delivery

Generic config schema 2 enrolls exact canonical checkout/origin pairs and source
kind/instance pairs. Linked worktrees must share the approved Git common directory.
`session.open` establishes immutable repository identity and directory fingerprint
before any attachment. Local policy and actual Git metadata, not a caller's title,
path label or timestamp, authorize capture.

`ingress.sqlite3` stores canonical event bodies, local receipt times, ordering and
export progress. Hooks submit locally with transactional serialization. Their
separate `adapters.sqlite3` journal freezes upstream correlations and generated
bodies before submission. The single drainer uses a writer lock and writes the
existing proven Delta sink ledger under `state/delta/`. An identical event is a
duplicate; conflicting identities halt. Missing sequences wait. A close event
prevents additional messages beyond its terminal sequence.

New Delta bindings use producer `delta-agent-history` and qualified hashed session
IDs. The sink waits for private cloud initialization, validates attachment and
producer identity, and preserves immutable outbox sequences and duplicate-safe
replay. Local acceptance is separate from sink export, acknowledgement and upload.
A malformed/unknown upstream contract fails closed. Supported contracts are 6–8; contract 8 requires connection-owned worktree mounts.

## Recall

Repository-scoped session listing returns qualified keys and metadata. History
uses the saved Delta binding, up to 20 messages per request. Consecutive assistant
records can coalesce into one Delta message; acknowledgements count records,
not displayed messages. The MCP server is stdio-only and binds one approved
repository at launch. Its three tools expose status, sessions and bounded reads.
It provides no capture, enrollment, authentication, shell, restore or network
listener. Imported history is untrusted evidence, not instructions or authority.

## Legacy T3 compatibility

Original `t3_delta` modules and commands keep their defaults and behavior. Their
producer is still `t3-code`, external IDs still `environment:thread`, and message
IDs, frozen labels and pending JSONL remain unchanged. No generic worker writes
that state. An explicit [legacy link](migration.md) reads only the original
recorder's own metadata and public Delta history. Both old and new repository
scopes must allow the selected checkout. T3 remains an optional source integration.

## Limits

Adapters see only documented events. Their source labels do not prove identity,
and no common protocol grants access to all desktop conversations. File coverage
begins after attachment; replaying transcript text cannot recover earlier files.
Shared checkouts share changes. No historical restore is exposed. The drainer can
process sessions serially; queue and cloud observations should be inspected
separately from process availability. See [compatibility](compatibility.md).
