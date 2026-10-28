# Capture adapters

Adapters translate supported application events into local, durable ingress. They never read private transcript databases, model-provider caches, or account credentials. They do not wait for Delta cloud delivery. Run `delta-history run` separately to attach checkouts and deliver accepted records.

All three integrations below are **fixture-tested reference integrations**, not live account or desktop certifications. T3's separate compatibility adapter retains its existing Linux live evidence. No automatic capture is claimed for ChatGPT, an arbitrary desktop app, or unwrapped Codex interactive sessions.

## Enroll first

Initialize generic configuration using the actual canonical repository root and an explicit source instance. This example enables all three adapters under the local name `workstation`:

```sh
delta-history init \
  --project /absolute/path/to/repository \
  --source claude-code:workstation \
  --source codex:workstation \
  --source opencode:workstation \
  --delta-binary /absolute/path/to/zed-delta \
  --accept-upload
delta-history run
```

Keep `run` active in a separate terminal or install the generic per-user service. Review tracked and nonignored checkout files before enrollment. Private visibility means cloud uploads can occur. Adapters cannot enroll a new source or repository by supplying an ID or path.

Generic defaults are `~/.config/delta-agent-history/config.json` and `~/.local/state/delta-agent-history/`. For custom locations, put `--config PATH --state-dir PATH` **before** the subcommand in every command and hook invocation. Keep generic and legacy T3 state separate.

Choose a source-instance name unique to the producing installation. Two independent machines must not reuse an instance name and submit unrelated sessions under the same upstream IDs.

## Claude Code

Merge the `hooks` entries from [claude-hooks.example.json](../integrations/claude-hooks.example.json) into your existing Claude settings. Replace the executable with the absolute installed `delta-history` path and `workstation` with the enrolled instance. Do not overwrite the rest of your settings.

The example uses a command string and a five-second local timeout. Quote an executable path containing spaces. Prompt text is passed only on stdin; it is never interpolated into the shell command. `delta-history hook claude` exits successfully on capture errors, including missing configuration, unapproved repositories, and recorder review holds. Failures emit a sanitized diagnostic on stderr; successful hooks produce no stdout, so capture receipts do not become prompt context. This behavior does not depend on an `onFailure` setting. A successful hook exit means Claude may continue, not that recording succeeded. Other commands, including `ingest` and `hook opencode`, retain their failure exit codes.

Your Claude version must supply the documented common `prompt_id` field and Stop's `last_assistant_message`. Payloads without a stable prompt ID are rejected rather than assigned an invented identity. Use a current supported version and verify a synthetic conversation before relying on capture. See the [official hooks reference](https://code.claude.com/docs/en/hooks).

Coverage is submitted prompt text and the final assistant text supplied by `Stop`, for the main agent only. Prompts can originate from Claude's own scheduled or background mechanisms as well as typed user input; the hook captures what that supported event supplies. Intermediate assistant messages, tools, reasoning, attachments, subagents, interrupted responses, and API-error text are not captured. `StopFailure` is deliberately ignored.

Sessions open lazily at the first captured prompt. There is no transcript backfill. A Stop event completes a turn, not the whole session. After accepted messages finish uploading, the worker releases the idle checkout mount. SessionEnd also marks the adapter inactive without closing its immutable ingress identity. The next prompt, including a native resumed session, resumes the same producer and reconnects file capture. No transcript path is read and no new producer ID is invented for a resumed session. The adapter requires the Stop's prompt ID to match a recorded active prompt. Identical callbacks replay safely. Different turns with identical prompt text are preserved as distinct turns. Changed text for an already completed prompt or Stop causes an adapter review hold. Multiple Stop continuations sharing one prompt ID have no separate documented completion ID in this integration; changed repeated completions fail closed rather than guessing their order.

The local journal can wait briefly on another hook process, but never on Delta upload. A hook timeout or nonblocking error can mean coverage is incomplete. Check status; do not infer success from Claude continuing normally. The adapter never uses `transcript_path` even when Claude includes it in hook input.

## Codex CLI

This adapter is an explicit wrapper for a new noninteractive run. Put the reviewed prompt in a UTF-8 file, then run:

```sh
delta-history codex \
  --instance workstation \
  --project /absolute/path/to/repository \
  --codex-binary /absolute/path/to/codex \
  --prompt-file /absolute/path/to/prompt.txt
```

Optionally add `--model MODEL`. The wrapper invokes `codex exec --json --cd PROJECT [-m MODEL] -` with the prompt on stdin. It adds no approval or sandbox bypass flags. Your existing Codex configuration and its normal noninteractive permissions remain relevant. The wrapper does not automatically turn ordinary `codex` interactive sessions or desktop conversations into captured sessions.

The source contract is the documented [noninteractive JSONL stream](https://learn.chatgpt.com/docs/non-interactive-mode) and official SDK [item](https://github.com/openai/codex/blob/main/sdk/typescript/src/items.ts) and [event](https://github.com/openai/codex/blob/main/sdk/typescript/src/events.ts) types. It captures the submitted user prompt, `item.completed` agent messages, and completed visible todo-list plans. It discards reasoning, command output, tool calls, file-change events, and partial items. Assistant/plan text is shown in the terminal. Codex stderr is inherited unchanged so permission and failure diagnostics remain visible; the wrapper never stores it or adds it to the recorded payload. Your terminal or an independently configured supervisor can still log that upstream output. Raw JSONL is not forwarded. Run this wrapper interactively, not as a logging service that redirects inherited stderr to a permanent log.

The wrapper persists its own run UUID before launching Codex and records the upstream thread ID when `thread.started` arrives. Each explicit invocation is a different run, even for identical prompt files. Item IDs deduplicate completed events within that run. An absent or malformed terminal stream is marked incomplete, not successful capture. Codex's process exit status is preserved, so exit 0 by itself does not prove capture completed. The wrapper never relaunches Codex to replay a recording; already frozen ingress is the retry boundary.

This code was exercised with a fake executable, not a live model. The host CLI inspected during implementation was Codex 0.164.0. Prompt input must be a regular UTF-8 file, not a symlink, FIFO, or device. Input messages are limited to 256 KiB, stream lines to 1 MiB, and consumed stream coverage to 64 MiB; a budget violation stops capture while the process is drained without forwarding its raw output. Delta attachment occurs asynchronously and can miss edits before attachment.

## OpenCode

Copy [opencode-plugin.mjs](../integrations/opencode-plugin.mjs) into the project's `.opencode/plugins/` or your user plugin directory `~/.config/opencode/plugins/`, **naming the installed file `delta-history.js`**. OpenCode v1.19.4 auto-discovers `.js` and `.ts` filenames, not `.mjs`. For a project installation from this repository root:

```sh
mkdir -p /absolute/path/to/repository/.opencode/plugins
test ! -e /absolute/path/to/repository/.opencode/plugins/delta-history.js && \
  cp integrations/opencode-plugin.mjs /absolute/path/to/repository/.opencode/plugins/delta-history.js
```

Preserve an existing file and compare before replacing it. The plugin uses built-in Node-compatible modules and the SDK client supplied by OpenCode. It does not install dependencies or start a second server.

Set these variables in the environment that launches OpenCode:

```sh
export DELTA_HISTORY_EXECUTABLE=/absolute/path/to/delta-history
export DELTA_HISTORY_INSTANCE=workstation
# Optional when using nondefault locations:
export DELTA_HISTORY_CONFIG=/absolute/path/to/config.json
export DELTA_HISTORY_STATE_DIR=/absolute/path/to/private-state
opencode
```

Use quoted assignments if a path contains spaces. The executable must be an absolute reviewed path. The plugin spawns it with literal arguments and JSON on stdin, with no shell or model-generated command string.

The integration follows the official [plugin event API](https://opencode.ai/docs/plugins/) and [session message SDK](https://opencode.ai/docs/sdk/), with message shapes checked against [OpenCode v1.19.4 types](https://github.com/anomalyco/opencode/blob/v1.19.4/packages/sdk/js/src/gen/types.gen.ts).

Only main sessions created while this plugin instance is active are eligible. Old sessions are not imported; after restarting OpenCode, create a new session for continued capture. On `session.idle`, the plugin reads that session's messages through `client.session.messages`, preserves order, and submits only visible user and completed assistant text. Synthetic/ignored text, reasoning, tools, attachments, assistant error output, and subagent sessions are excluded. An incomplete assistant stops the stable prefix so later rows cannot overtake it.

Snapshots use stable session/message IDs. Previously accepted completed messages must remain an identical ordered prefix; changed, removed, reordered, or truncated history causes a review hold. This includes a source-side revert or compaction that removes captured messages. The adapter does not fabricate a replacement history. Snapshot limits are 2,000 message entries, 256 KiB per visible message, and 4 MiB per hook payload. A local subprocess is bounded to five seconds and failure does not change OpenCode's completion decision. SDK/account behavior still needs live verification.

## Journals, retry, and health

Adapters allocate sequence numbers transactionally in private `adapters.sqlite3` state. Upstream IDs select immutable semantic records; the emitted timestamp, sequence, event ID, and bytes are frozen before local core submission. Concurrent hook processes serialize through SQLite. A crash after core submission but before marking delivery replays the same events safely on a later callback. Do not delete the adapter journal to clear a hold.

Local acceptance is separate from Delta acknowledgement, cloud upload, and verified recovery. The generic `once`/`run` worker first retries already frozen pending adapter events, even when no new hook callback arrives, then drains the Delta queue. It rechecks current source enrollment and repository identity; holds remain sticky. It does not generate new event times or bodies, discover missing source messages, or rerun models. A stopped source application cannot supply messages that were never captured. Never use private application history files as an undocumented retry feed.

Inspect `delta-history status` for core and adapter health, then use the repository-scoped `sessions` and bounded `history` commands. For a held adapter, preserve state, check source IDs and coverage, and investigate using a synthetic reproduction. Never attach private journals or transcripts to a public issue.
