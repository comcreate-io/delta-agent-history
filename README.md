# T3 Delta recorder

Keep working in [T3 Code](https://github.com/pingdotgg/t3code). Record new project conversations and checkout history privately in [Zed Delta](https://delta.dev), then let your coding agent recall relevant prior work when you need it.

This is a small, independent bridge maintained by ComCreate. It works alongside Git and does not replace commits, branches, or backups. It is not an official T3 or Zed integration.

**Experimental release 0.1.0.** The adapter was exercised with T3 0.1.0 and Delta 0.21.0 (recording contracts 6–7). It deliberately rejects unsupported recording contracts. Linux has live integration evidence; macOS support includes portable code, service tooling, and CI coverage, but has not been verified end to end with the desktop apps. Read the [compatibility and verification limits](docs/compatibility.md).

## What you get

- A Python recorder with no third-party runtime dependencies.
- Explicit enrollment of Git repositories, matched by checkout path and origin.
- One private Delta conversation for each eligible new T3 conversation.
- Durable delivery and retry ledgers, scoped read-only T3 authentication, and bounded history retrieval.
- Linux systemd and macOS launchd service setup.
- An optional agent skill and recall instruction for Codex, Claude Code, or another harness that supports skills.

Recording runs automatically while the service is active. Recall is performed by an agent when relevant; the instruction makes that a normal part of resuming work, debugging, and revisiting decisions. It does not inject every old transcript into every task.

## Install and try it

You need Python 3.11+, Git, T3 Code with its CLI, and an authenticated Zed Delta account with access to the app. Install T3 and [Delta](https://delta.dev/docs/installation) through their upstream distribution channels first. Delta currently requires macOS 13+ on Apple Silicon or glibc Linux on x86_64/aarch64. This repository does not bundle either application.

On Linux or macOS, create an isolated Python environment:

```sh
git clone https://github.com/comcreate-io/t3-delta-recorder.git
cd t3-delta-recorder
python3 -m venv .venv
.venv/bin/python -m pip install .
```

Ask your agent inside T3 to call `t3_environment_read` and return the `environmentId`. Review the checkout you intend to upload, including tracked files and nonignored untracked files. Then substitute your actual environment ID, repository root, runtime file, and executable paths:

```sh
.venv/bin/t3-delta init \
  --project /absolute/path/to/project \
  --environment-id YOUR_T3_ENVIRONMENT_ID \
  --t3-runtime-file "$HOME/.t3/userdata/server-runtime.json" \
  --t3-binary /absolute/path/to/t3 \
  --delta-binary /absolute/path/to/zed-delta \
  --accept-upload
.venv/bin/t3-delta auth
.venv/bin/t3-delta run
```

`init` only creates local configuration. `auth` explicitly issues a dedicated read-only T3 credential. `run` starts recording and may upload eligible conversations and checkout data. Keep it running, open the enrolled repository as a T3 project, and create a **new** conversation. Existing conversations are not backfilled, even if you add a new message to them.

In another terminal, check what has been captured:

```sh
.venv/bin/t3-delta status
.venv/bin/t3-delta history --thread YOUR_T3_THREAD_ID --messages 1-20
```

Use the source T3 `thread_id` shown by status. It is different from the Delta conversation ID. Read [the setup guide](docs/setup.md) before enrolling more repositories, [background service setup](docs/services.md) to keep recording after the terminal closes, and [agent setup](docs/agents.md) to enable routine recall.

If Zed's executable is named `delta` on your system, supply its absolute path with `--delta-binary`. A different program named `delta` is a popular Git diff pager. The recorder does not auto-select that name.

Nix users can use the included flake. See [setup](docs/setup.md#nix).

## Upload and recovery boundaries

“Private” describes Delta's visibility setting. It does **not** mean local-only storage. Eligible conversation messages and checkout files can leave your machine. The bridge never deliberately shares a conversation with your organization. Its filename check is a heuristic, not a secret scanner; `.gitignore` does not protect secrets that are already tracked.

Capture is prospective. File history begins after Delta attaches the checkout, so edits made before attachment can be absent. Hidden reasoning, attachments, and raw tool outputs are excluded from the conversation export. Several conversations using one checkout also share that checkout's files; use separate Git worktrees for independent work.

A transcript acknowledgement, a completed cloud upload, and a verified file restore are separate facts. Latest-state file recovery has a controlled Linux proof. Recovery to an arbitrary earlier point through this bridge remains unverified. The recorder has no automatic restore command and does not rewind T3 conversations.

## Contribute

Run the standard-library test suite from the repository root:

```sh
python3 -m unittest discover -s tests -v
```

See [architecture](docs/architecture.md) for protocol boundaries and [compatibility](docs/compatibility.md) before changing an upstream version or recording contract. Please use synthetic repositories and transcripts in tests and issues. Never include credentials, client inventories, or private recording state.

Licensed under [MIT](LICENSE).
