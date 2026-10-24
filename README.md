# Delta Agent History

Record coding sessions from different agents into private [Delta](https://delta.dev)
histories, then recall relevant work from another harness. Keep your preferred
editor, model, or coding interface. The core does not require T3, Claude, or Codex.

Independent ComCreate project. MIT licensed. Linux and macOS. Python 3.11+ with no
third-party Python runtime dependencies. Delta and Git are separate prerequisites.
Delta remains the history backend; this does not replace Git or backups.

## Integration paths

| Source or client | Integration | Coverage |
|---|---|---|
| Custom agents | Versioned JSONL over stdin | Reference producer and strict synthetic tests. |
| Claude Code | Supported prompt and Stop hooks | Prompts and final answers; stable prompt IDs required. |
| Codex CLI | Wrapper around `exec --json` | Wrapped noninteractive runs, not arbitrary desktop/interactive sessions. |
| OpenCode | Plugin using supported SDK message reads | Completed visible messages with immutable prefix checks. |
| T3 Code | Original optional read-only adapter | Existing commands, state and producer identities preserved. |
| Compatible local MCP clients | Project-bound read-only stdio tools | Recall sessions recorded by these integrations. |
| ChatGPT and other desktop apps | Supported integration or explicit producer | Complete passive capture requires a verified event feed and is not claimed here. |

The neutral core has live Linux private-upload/readback evidence through a
separate stdio MCP client. Source adapters have synthetic fixture tests. The
original T3 adapter retains live Linux evidence. Live account integration for new
source adapters and macOS desktop capture remains unverified. See [compatibility](docs/compatibility.md). MCP tool
access does not automatically expose the client's complete transcript.

## Quick start

Install and sign into [Zed Delta](https://delta.dev/docs/installation) first. Its
CLI may be `delta` or `zed-delta`; do not confuse it with Git's diff pager.
Upstream currently requires glibc Linux or an Apple Silicon Mac on macOS 13+.

```sh
git clone https://github.com/comcreate-io/delta-agent-history.git
cd delta-agent-history
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .

delta-history init \
  --project /absolute/path/to/reviewed-project \
  --source generic:local \
  --delta-binary /absolute/path/to/Zed-Delta-CLI \
  --accept-upload
```

Review the repository before giving upload consent. Then try the synthetic
producer against that approved checkout:

```sh
python examples/producer.py --project /absolute/path/to/reviewed-project | delta-history ingest
delta-history status
delta-history once
delta-history sessions --project /absolute/path/to/reviewed-project
```

`ingest` acknowledges private **local queue acceptance**, without contacting Delta.
`once` attempts delivery. `run` continuously drains the queue. Hooks and plugins
provide capture; the drainer does not discover arbitrary conversations. Read a
session using its returned qualified `key`:

```sh
delta-history history --project /absolute/path/to/reviewed-project \
  --session SESSION_KEY --messages 1-20
```

Read [setup](docs/setup.md), [adapters](docs/adapters.md),
[background services](docs/services.md), and [agent/MCP recall](docs/agents.md).
Nix packaging and original `t3-delta` commands are included.

## Existing T3 users

Previously named `t3-delta-recorder`. Original configuration, services, credentials,
Delta bindings, and queued payloads remain unchanged. `t3-delta` and
`t3-delta-service` remain compatibility commands. `delta-history t3 ...` delegates
to that adapter. Do not point the generic worker at the old state directory.
[Explicit read-only linking](docs/migration.md) exposes existing T3 sessions through
shared recall without migrating them.

## Privacy and recovery

Private Delta history is cloud storage. Completed visible messages and attached
checkout contents can leave your machine, including tracked and nonignored
untracked files. Enrollment is explicit by repository and source instance. The
filename heuristic is not a secret scanner; ignoring a tracked secret does not
remove it from upload coverage.

Reasoning, attachments, raw tool output, and credentials are excluded from generic
ingress. File coverage begins after attachment. Shared checkouts share files; use
Git worktrees for independent tasks. Local acceptance, Delta acknowledgement,
cloud upload, and verified recovery are separate facts.

There is no automatic restore command. Historical point-in-time recovery remains
unverified. See [architecture](docs/architecture.md), [ingress](docs/ingress.md),
[security](SECURITY.md), and [contributing](CONTRIBUTING.md).
