# Codebase review: 2026-10-25

Baseline: `fe018dabd7aaa01b92ead150e7dadcd6f3bf5589`.
Changes are local on `fix/recorder-review-hardening`.

Two independent reviews covered correctness/recovery and
security/portability/integration behavior. A separate final verification reviewed
the combined changes. All reproductions used synthetic temporary fixtures. No
live credentials, recorder state, model runs or cloud operations were needed.

## Correctness and recovery

**Worst finding: P1, temporary SQLite contention permanently halted a session.**
`delta_history/core.py` classified an ingress writer lock as a permanent failure
after the sink had accepted a message. The regression holds a real SQLite writer
lock across the delivery bookkeeping step. It reproduces the original halt and
now verifies eventual identical replay without losing the message.

The fix retries only SQLite `BUSY` and `LOCKED` codes, including extended codes.
It avoids writing error metadata while another writer may still hold the lock.
Unknown database errors and actual immutable conflicts still require review.

**P2, healthy serial legacy polls appeared stale.** `t3_delta/engine.py` and
`t3_delta/cli.py` now expose a bounded worker progress stage and timestamp,
separate from the last completed verification or failure timestamp. Recent
progress reports `catching_up`; it does not prove current source capture or
cloud upload. Stopped progress still expires, and source, upload and review
failures remain visible.

**P2, remount-only device-number changes create checkout review holds.**
The persisted device/inode guard remains unchanged. Automatic remount recovery
is intentionally not implemented: removing the guard would weaken protection
against replaced checkouts. A supported recovery path needs durable filesystem
identity or explicit audited revalidation, while preserving unrelated holds.
The current limitation is documented in `docs/compatibility.md`.

## Security, portability and integration

**Worst finding: P2, a temporary queue failure suppressed Codex answers.**
`delta_history/adapters/codex.py` previously treated a transient ingress lock as
an incomplete capture, suppressing the current answer and skipping later ones.
The fix displays supported completed text after durable adapter acceptance,
continues journaling after transient ingress contention, and retains pending
records for the independent drainer. The regression verifies two visible
answers, unchanged frozen events, eventual replay and exactly one model launch.
Changed completion identities still hold capture.

**P2, deeply nested JSON terminated the stdio MCP server.**
`delta_history/cli.py` now enforces an explicit 64-container depth limit before
JSON decoding. Quoted brackets and escaped quotes do not consume nesting depth.
Malformed or excessive nesting produces JSON-RPC `-32700`, and the server accepts
the next valid request. An installed-package check exposed Python 3.14 decoder
behavior that differed from 3.13; the explicit limit removes that dependency.

## Verification and limits

The original 176-test suite passed before changes. Regressions reproduced the
SQLite halt, suppressed Codex output and MCP crash before their fixes. Focused
checks exercise real SQLite contention, durable replay, sticky conflicts, worker
progress and stopped-worker detection, source/upload errors, JSON depth
boundaries, quoted text, duplicate keys and nonfinite numbers.

Final parser verification independently passed on Python 3.13.15 and 3.14.8,
including stdio recovery. The wheel installed outside the checkout and all four
command entrypoints passed their help checks.

The final full suite passed **191 tests** on Python 3.13.15. `nix build --no-link`
also passed, running the complete suite with Python 3.14.8. Wheel and source
distribution builds passed. `git diff --check` passed. These results cover the
local branch; no hosted CI run or publication occurred for these changes.

Long individual synchronous operations can still exceed the progress threshold;
the latest stage identifies where work stopped advancing. Automatic credential
renewal, automatic remount recovery, live new-adapter account integration,
macOS desktop upload and historical point recovery remain unverified or outside
this change. The running installation is not changed by this review branch.
