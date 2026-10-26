# Compatibility and evidence

This is an experimental 0.3 release. Harness independence is a protocol boundary,
not a promise that every application provides complete automatic capture.

| Component | Support boundary |
|---|---|
| Python | 3.11+, standard-library runtime and POSIX locking. |
| Linux/macOS | Package, unit, service serialization and installed-command CI. |
| Delta | Tested baseline 0.21.0, public recording contracts 6–7 only. |
| Generic producer | Strict ingress v1; live Linux private upload and separate-client MCP readback. |
| Claude Code | Fixture-tested documented hooks, prompts and final Stop text; prompt IDs required. |
| Codex | Fixture-tested wrapper for noninteractive `exec --json`; not general desktop/interactive capture. |
| OpenCode | Fixture-tested SDK/plugin snapshots of complete visible messages. |
| Optional T3 | Original 0.1.0 read-only MCP adapter, live Linux evidence, unchanged legacy identities. |
| MCP clients | Read-only stdio tools, bound to one enrolled repository. |
| Closed desktop apps | No complete automatic capture claimed without a verified official feed. |
| Windows | Unsupported POSIX locking/service implementation. |

Upstream [Delta requirements](https://delta.dev/docs/installation) currently include
glibc Linux x86_64/aarch64 or macOS 13+ Apple Silicon. The bridge does not license,
install or grant access to upstream applications. Portable Python does not add
Intel Mac support to the Delta app.

## What is tested

A disposable public checkout and synthetic generic producer were recorded with
private visibility and completed upload on Linux, then read through a separate
stdio MCP process. Local duplicate submissions were verified without extra
messages. No T3 credential was required. Delta adds paragraph separators to its
public transcript projection; readback compared visible text and roles.

The original T3 Linux integration exercised private creation, checkout attachment,
server acknowledgement, upload, bounded readback, immutable replay and controlled
latest-state recovery. New integrations use synthetic hooks/events, a fake Codex
executable, OpenCode SDK stubs, real local stdio MCP conversations, and temporary
Git checkouts. No CI credentials, production sessions or paid model runs are used.

Fixtures establish parser, ordering, scope, retry and protocol behavior. They do
not establish live account or desktop integration. In particular, complete live
Claude/Codex/OpenCode capture and actual Mac desktop upload remain unverified.
Do not label a hook as full transcript coverage when it provides only final text.

The supported Delta contract remains6–7. Newer schemas need authoritative review,
new tests and exact-version live checks before widening support. Adapter versions
and event requirements are described in [adapter setup](adapters.md).

## Remaining limits

No hidden reasoning, raw tool output or attachments are accepted by generic
capture. File coverage starts after attachment and can miss early edits. Multiple
sessions in one checkout share files. Use linked worktrees for independent work.
Current delivery can process sessions serially; inspect pending/gap/adapter holds
separately from process availability. Automatic credential renewal and remote MCP
hosting are not included. Existing T3 credentials retain their 30-day renewal flow.

Checkout bindings currently retain filesystem device and inode numbers. Some
filesystem remounts can change the device number while leaving the checkout
intact, producing a review hold. Automatic remount recovery is not implemented.
Preserve the state and verify exact paths, Git identities and filesystem evidence
before deliberate revalidation. Do not remove the guard or clear all holds:
replacement checkouts and immutable-message conflicts also require review.

No automatic restore is exposed. Latest-state recovery has a narrow Linux proof;
arbitrary historical point recovery through this bridge remains unverified.
Opaque receipt versions are not CLI historical-version arguments. Recover into a
separate checkout or patch and verify before applying; restoring files does not
rewind any harness's conversation.

Report issues with versions, architecture, sanitized error types and a synthetic
reproduction. Never attach tokens, raw histories, config, private databases or
customer code to public issues.
