# Set up T3 Delta recording

## Prerequisites

- Linux or macOS, Python 3.11+, and Git on `PATH`.
- T3 Code and its supported `t3` CLI. The tested T3 version is 0.1.0.
- Zed Delta, tested at 0.21.0, with its CLI and an account allowed to use it.
- One or more actual Git repository roots with a supported, credential-free `origin` remote.

Install Zed Delta through its [official installation guide](https://delta.dev/docs/installation), which currently requires macOS 13+ on Apple Silicon or glibc Linux on x86_64/aarch64. Install its CLI using the app's “Install CLI” action or the upstream manual path instructions.

Zed Delta and the Git diff pager named `delta` are different tools. Locate Zed's executable and pass its absolute path. The recorder's default name is `zed-delta`; if upstream installs it as `delta`, specify that path explicitly. Complete upstream `auth login` and any browser consent or “Open Delta” handoff yourself. Being signed into GitHub alone does not prove the Delta desktop app is signed in.

Check [compatibility](compatibility.md) before substituting newer versions. Account availability and upstream platform support are separate from this bridge's Python portability.

## Install the Python package

```sh
git clone https://github.com/comcreate-io/t3-delta-recorder.git
cd t3-delta-recorder
python3 -m venv .venv
.venv/bin/python -m pip install .
.venv/bin/t3-delta --help
```

Keep the repository and virtual environment at a stable path if you install a background service. No global Python installation or administrator privileges are required for this bridge. On a system managed declaratively, install it through that system's package configuration instead of modifying its global environment.

## Select and review repository scope

Open each intended project at its Git root. `git -C /path/to/project rev-parse --show-toplevel` should return that root. Review its `origin`, tracked files, ignored files, and nonignored untracked files before authorizing upload.

The filename gate refuses common sensitive-looking names, but does not inspect contents or prove a repository is safe. Files named as examples, samples, templates, fixtures, or tests are allowed by that heuristic. Review them too. A tracked secret is still tracked when added to `.gitignore`.

Enrollment is explicit. There is no automatic discovery of your home directory or project tree. Repeat `--project` for each repository in the initial configuration. A second clone with the same remote is not automatically enrolled. A genuine linked Git worktree is permitted only when its Git common directory belongs to an approved repository.

## Create configuration

Ask your agent in T3 to call the supported `t3_environment_read` tool and give you its `environmentId`. Do not extract this from T3's private database or copy provider credentials.

The usual runtime file is `$HOME/.t3/userdata/server-runtime.json`. If you run a custom T3 data directory, select that instance's runtime file and executable, and supply `--t3-base-dir /absolute/path/to/custom-base` during `init` so later credential issuance reaches the same instance. Confirm these refer to the same running environment.

```sh
.venv/bin/t3-delta init \
  --project /absolute/path/to/first-project \
  --project /absolute/path/to/second-project \
  --environment-id YOUR_T3_ENVIRONMENT_ID \
  --t3-runtime-file "$HOME/.t3/userdata/server-runtime.json" \
  --t3-binary /absolute/path/to/t3 \
  --delta-binary /absolute/path/to/zed-delta \
  --accept-upload
```

Omit the second `--project` if you only want one. Optionally supply `--delta-organization ORGANIZATION_ID` to choose the account organization explicitly; visibility remains private. Without it, the Delta CLI must be able to resolve the account context.

`init` validates local checkout identities and filename boundaries, resolves executables, and writes an exclusive local config. It does not issue a credential, contact Delta, upload files, or install a service. It refuses to overwrite an existing config, including a symlink. It records an enrollment timestamp for each selected repository. [The example configuration](../examples/config.example.json) illustrates the shape; initialize your own paths and current timestamps instead of copying its placeholders unchanged.

The same defaults are used on Linux and macOS:

| Purpose | Default path |
|---|---|
| Configuration | `~/.config/t3-delta-recorder/config.json` |
| Private recorder state | `~/.local/state/t3-delta-recorder/` |
| Scoped T3 credential | `~/.local/state/t3-delta-recorder/token.json` |

Use global options **before** the subcommand when selecting other locations:

```sh
.venv/bin/t3-delta --config /absolute/path/config.json \
  --state-dir /absolute/path/private-state status
```

Use those same paths for every later command and service configuration. Do not commit the config or state directory: they contain local project metadata, and the state contains a credential and recorded messages.

## Authenticate and record

With T3 running and Delta signed in:

```sh
.venv/bin/t3-delta auth
.venv/bin/t3-delta run
```

`auth` issues a dedicated 30-day T3 session with the `orchestration:read` scope and `mcp-client` subject, validates the expected environment, and stores it privately. It does not use your model-provider credentials. Never paste its token into an issue or agent conversation.

Leave `run` active. Then create a **new T3 conversation** under an enrolled project root and exchange a few messages. Eligibility starts after both repository enrollment and the recorder's first run. Old conversations are not backfilled, and posting to an old conversation does not enroll it.

There is a delay between discovering a conversation, creating its private Delta history, attaching its checkout, and completing upload. File coverage starts only after attachment and can miss early edits. For independent tasks, select distinct Git worktrees in T3 before agents start changing files.

## Verify the result

```sh
.venv/bin/t3-delta status
.venv/bin/t3-delta history --thread YOUR_T3_THREAD_ID --messages 1-20
```

In status, `threads` contains source T3 IDs and directories. Match `threads.external_id` to `delta.external_id` to inspect the Delta binding, `message_count`, pending queue, acknowledgements, visibility, upload state, and attachment time. Do not infer cloud backup from the service being alive or an acknowledgement alone. History reads are bounded to 20 messages per request and clamp the requested end to captured messages.

Once this works in the foreground, use [systemd or launchd setup](services.md). Install the [agent skill and recall instruction](agents.md) if you want agents to consult prior project work automatically.

## Add repositories later

Stop the recorder before manually editing its configuration. Add the canonical path, exact origin, and a current UTC `enrolled_after` timestamp to `allowed_projects`; include its remote in `allowed_remotes`. If outside `projects_root`, include its canonical path in `additional_project_paths`. Do not turn the exact allowlist into a wildcard or backdate enrollment to import old conversations.

Review the repository's upload contents first. Resume the recorder and create a new T3 conversation. Alternatively, initialize a separate config and state pair for a separate recorder instance. `init` refuses overwrites so it cannot silently change an existing enrollment.

## Troubleshooting

| Observation | Check |
|---|---|
| No recorded conversation | Was it created after the first recorder run? Is T3 bound to the approved root, rather than an umbrella directory or duplicate clone? |
| Filename or scope review hold | Review the checkout and configuration. Do not remove the gate simply to make status green. |
| `unavailable` | Confirm the correct T3 instance is running, Delta is signed in, and executable/runtime paths still exist. |
| `catching_up` or `stale` | Check pending items and timestamps. Serial polling can take longer with many subagents; this does not by itself prove data loss or a crash. |
| `needs_review` or halted Delta session | Preserve the state, inspect compatibility and checkout identity, and investigate. Do not delete the ledgers or fabricate acknowledgement state. |
| No history match | This means no captured match, not that the project has no prior work. Continue from current source. |
| Credential expired | Stop the service, run `t3-delta auth` with the same config/state, then restart it. Credential renewal is explicit. |
| Newer upstream contract rejected | Check [compatibility](compatibility.md); review and test the adapter before widening support. |

Avoid posting raw logs, transcripts, tokens, config files, or state databases publicly. Error output is intentionally terse; share sanitized error types, versions, and reproduction steps using synthetic data.

## Nix

The repository includes a flake for declarative or scoped use. From this checkout:

```sh
nix build
nix run . -- --help
```

Configure persistent installation through your NixOS or Home Manager setup. The package does not automatically enroll projects, issue credentials, or activate a service. Follow the same explicit setup and upload review above.
