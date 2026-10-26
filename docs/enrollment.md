# Enroll new projects and install agent instructions

After the initial [setup](setup.md), add one repository to the existing generic
configuration without replacing other projects or sources:

```sh
delta-history enroll --project /absolute/path/to/new-project --check
delta-history enroll --project /absolute/path/to/new-project --accept-upload
```

Run the second command after reviewing the repository and consenting to private
Delta uploads of visible conversations and tracked/nonignored checkout files.
The check is read-only: exit 0 means enrolled, exit 2 with `enrolled: false` means
not enrolled. An error instead of that result means setup or review is needed.
Git initialization and a supported credential-free `origin` must already exist.

Enrollment is offline. It adds the exact path, origin and a new prospective cutoff
to the current configuration. Existing projects, source identities, enrollment
dates, Delta settings and legacy links remain unchanged. A repository outside
`projects_root` gets one explicit additional path; its neighboring folders are
not enrolled. A linked worktree of an approved repository reuses that approval.

The command adds a managed Delta section to `AGENTS.md` and `CLAUDE.md` in the
selected checkout. Existing content outside that section is preserved, including
line endings. Existing lowercase filenames are reused. Conflicting case variants,
malformed managed markers, symlinks, nonregular files and sensitive-looking
filenames require review. Repeated enrollment keeps the original cutoff and
does not duplicate instructions. An already enrolled repository can install the
instructions without a new upload flag:

```sh
delta-history enroll --project /absolute/path/to/already-approved-project
```

Configuration is private and updated atomically under an enrollment lock. Files
are preflighted and instructions are published before config. An interruption
may leave instructions installed while enrollment is still missing; rerun after
reviewing the reported error. Existing JSON is never replaced with partial JSON.
Concurrent edits are checked before replacement. Do not delete state or reset
dates to retry enrollment.

Version 0.3 workers reload project scope between passes. After upgrading an older
worker, restart it once to enable that behavior. Use your configured capture
hook, plugin or wrapper.
The command does not start a service, authenticate, create a cloud history,
backfill conversations or configure a new source adapter. Recording begins only
when eligible events reach the configured integration and drainer.

## Make the workflow discoverable to agents

A new checkout cannot supply instructions before its files exist. Install
`skills/delta-enroll/` in the harness's supported skills directory and append
[the new-project instruction](../instructions/project-enrollment.md) to the
operator's existing global `AGENTS.md` and `CLAUDE.md` (or equivalent guidance).
Preserve existing global guidance. This is a one-time setup; the `enroll` command
then installs the project-level sections. It does not edit global files itself.

The same skill works on Linux and macOS with shell-capable agents. Each operator
needs their own Delta sign-in, initial config and source integration. Committing
a project's instructions does not enroll it for another user. With a custom
config, put `--config PATH` before the subcommand; portable project instructions
contain no machine-specific paths or credentials.

## Share new enrollment with the original T3 recorder

Both workers can explicitly select the same `project_enrollment_file`, a canonical
absolute path to a private mode-0600 JSON file. For a new generic installation,
pass `--project-enrollment-file /absolute/private/projects.json` to `init`. Add the
same field to the original T3 config through its existing configuration workflow.
A missing registry means no additional projects; `enroll` creates it on first use.
The schema is `{"schema_version": 1, "projects": [...]}`, with each project using
`path`, `remote` and an explicit UTC `enrolled_after` timestamp.

For Nix/Home Manager, keep the base config immutable and declare this registry
path in both generated configs. The `enroll` command writes only that explicitly
delegated private file, preserving Nix symlinks and all base enrollment dates.
It adds managed project instructions in the selected checkout. Both workers read
the new exact rules at their next pass, without a system rebuild for each project.
An entry must not contradict a base project's origin or cutoff. Symlinks, special
files, shared permissions and malformed registry data fail closed.

Selecting one registry in both configs authorizes that shared scope explicitly.
A generic config alone does not change another worker's enrollment. The original
T3 service, environment, producer IDs, state, credentials, cutoff and pending
payloads stay separate; enrollment does not migrate or replay its old recordings.
Non-project T3 configuration changes still require a service restart. Without an
explicit registry, writable generic config uses the original additive update;
managed config without a registry remains read-only to enrollment.
