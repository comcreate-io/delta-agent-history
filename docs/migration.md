# Migration from the T3 recorder

Version 0.2 introduces a neutral package and commands. It does not rewrite legacy
state or automatically change an installed worker. Keep the original service
running if you still want automatic T3 capture.

## Preserved behavior

- `t3-delta` and `t3-delta-service` retain original entrypoints and defaults.
- `delta-history t3 ...` delegates to the old CLI, including its global flags.
- Original config: `~/.config/t3-delta-recorder/config.json`.
- Original state: `~/.local/state/t3-delta-recorder/`.
- Original producer `t3-code`, external/message IDs, labels, sequence numbers and
  pending payload bytes are not migrated or replayed under the new producer.
- Generic state lives separately. Selecting legacy state for generic ingestion
  or delivery is rejected. Never delete the old ledgers to bypass this check.

See [legacy T3 setup](t3-legacy.md) for its original authentication and capture flow.
Its tested T3 version remains 0.1.0. The generic recorder has no T3 dependency.

## Shared recall of existing T3 sessions

Opt in when initializing a new generic config:

```sh
delta-history init \
  --project /absolute/path/to/project \
  --source generic:local \
  --delta-binary /absolute/path/to/Zed-Delta-CLI \
  --legacy-t3-config "$HOME/.config/t3-delta-recorder/config.json" \
  --legacy-t3-state "$HOME/.local/state/t3-delta-recorder" \
  --accept-upload
```

Both paths must be explicitly supplied. Nothing is automatically discovered.
An explicitly selected symlinked config, such as a Home Manager file, is resolved
to its regular target at setup. That target is pinned in the neutral config;
relink deliberately if a declarative update replaces it or garbage collection
removes it. State directories must remain canonical and separate.
Shared `sessions --project PATH` includes eligible legacy bindings with
`legacy-t3:` keys; shared history and MCP route those reads to the original
binding. The selected repository must be allowed by both configs and its current
Git identity must agree. Missing or changed linked checkouts may not be available.
A missing linked recorder is reported as a coverage error, not an empty success.

The link grants read-only access to the original recorder's own source/sink
ledgers and public Delta history. It never reads T3's private database, issues a
T3 token, attaches a checkout, migrates state or drains its pending outbox.

## Services and package updates

Keep legacy and generic services on separate state directories and labels. Two
service definitions must not mutate the same writer state. Rename or relocate a
virtual environment only after stopping its services and regenerating their paths.
The public project/package is now `delta-agent-history`; the compatibility
commands are shipped in the new package, not a separate shim installation.
Use a fresh virtual environment for version 0.2 rather than installing the old
and new distribution names together: both ship the compatibility modules and
commands. For example, run `python3 -m venv .venv-0.2`, then
`.venv-0.2/bin/python -m pip install .`. Leave state outside that environment.
For declarative installations, replace the old package declaration with the new
one while retaining original config/state; do not add both conflicting packages
to the same profile. Change service executable paths deliberately after stopping
the affected worker, then verify its original bindings and pending queue.

Back up a stopped worker's entire state before a deliberate future migration.
Do not copy it into generic state. Rollback means returning to the compatible
package and original config/state, not attempting to synthesize old receipts.
