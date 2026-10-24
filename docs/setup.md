# Setup on Linux and macOS

Use Python 3.11+, Git, and a compatible [Delta installation](https://delta.dev/docs/installation).
Install upstream applications through their own channels. Delta currently requires
glibc Linux on x86_64/aarch64 or macOS 13+ on Apple Silicon. The bridge does not
provide account access or support unsupported upstream hardware.

## Install

```sh
git clone https://github.com/comcreate-io/delta-agent-history.git
cd delta-agent-history
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
delta-history --help
```

No global Python install or root access is needed. Some Linux distributions
package venv support separately. Use your supported Python environment. On NixOS,
use the included flake and declarative configuration.

Keep a service's installation path stable. GUI agents may not inherit terminal
PATH; supply the full `.venv/bin/delta-history` path where necessary. Wheels also
include skills, instructions, schema and integration files under
`share/delta-agent-history/` inside the environment prefix.

## Enroll repositories and producer instances

Review actual roots, origins, tracked files, nested checkouts, and nonignored
untracked files before authorizing upload. The filename gate is a heuristic, not
a content scanner or complete inventory. Review examples and fixtures too.

```sh
delta-history init \
  --project /absolute/path/to/project \
  --source generic:local \
  --source claude-code:laptop \
  --source codex:laptop \
  --source opencode:laptop \
  --delta-binary /absolute/path/to/Zed-Delta-CLI \
  --accept-upload
```

Omit unused sources. Repeat `--project` for each repository. Source kind and
instance are lowercase slugs; labels are not authentication or authorization.
The actual Git identity and locally configured scope remain authoritative.

`init` writes a new private config and refuses overwrites. It does not issue a
harness credential, upload, or activate a service. Generic operation needs no T3
runtime, environment ID, or credential. Complete Delta's own sign-in and browser
handoff yourself. If its CLI is named `delta`, pass the actual absolute path;
automatic discovery only looks for `zed-delta` to avoid the unrelated Git pager.
An optional `--delta-organization ID` selects the private account context.

## Paths and operation

| Item | Generic default |
|---|---|
| Config | `~/.config/delta-agent-history/config.json` |
| Private state | `~/.local/state/delta-agent-history/` |
| Adapter correlation journal | Inside that private state directory |
| Existing T3 state | Separate and unchanged; see [migration](migration.md) |

For custom locations put global flags before the subcommand on every call:

```sh
delta-history --config /absolute/config.json --state-dir /absolute/private-state status
```

Try the reference producer and inspect metadata:

```sh
python examples/producer.py --project /absolute/path/to/project | delta-history ingest
delta-history once
delta-history sessions --project /absolute/path/to/project
delta-history run
```

`ingest` is local only. `once` and `run` may attach the checkout and upload accepted
messages. `run` drains events, so configure an [adapter](adapters.md) for real
capture. Set up [services](services.md) after foreground verification.

Recording is prospective relative to enrollment, with local receipt times retained.
A supplied timestamp or exported transcript cannot recreate previous checkout
states. Gaps wait for missing sequences; conflicting IDs halt the session.

## Changes and troubleshooting

Stop the drainer before deliberately editing its config. Review new checkouts,
add exact path/origin/current enrollment timestamps, and preserve source IDs and
journals. Never erase a conflict by deleting state. Use a new reviewed source
instance when intentionally starting a separate producer. Legacy T3 renewal uses
its own stop/auth/start procedure.

If capture is absent, check the adapter installation, executable path, enrolled
source instance, and actual root. For pending upload check Delta sign-in, versions,
queue gaps and status. A running process is not proof of upload. Do not post
config, databases, prompt files, raw transcripts, or tokens in public reports.

## Nix

```sh
nix build
nix run . -- --help
nix develop
```

The flake supplies neutral and compatibility commands on x86_64/aarch64 Linux and
aarch64 Darwin. Configure persistent packages/services through NixOS, Home Manager
or nix-darwin. Nothing is enrolled or activated automatically.
