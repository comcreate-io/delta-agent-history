# Give agents project recall

Recording runs automatically while the recorder is active. Agents retrieve history only when they call it. The skill explains bounded retrieval, and the instruction makes it a normal part of resuming work, debugging, and revisiting a decision.

Install and verify the recorder using [setup](setup.md) first. Loading a skill does not start recording or recover conversations that were never captured.

## Install the skill

For Codex, from the repository root:

```sh
mkdir -p "$HOME/.codex/skills"
test ! -e "$HOME/.codex/skills/t3-delta-history" && \
  cp -R skills/t3-delta-history "$HOME/.codex/skills/t3-delta-history"
```

For Claude Code:

```sh
mkdir -p "$HOME/.claude/skills"
test ! -e "$HOME/.claude/skills/t3-delta-history" && \
  cp -R skills/t3-delta-history "$HOME/.claude/skills/t3-delta-history"
```

These commands leave an existing skill untouched. Compare it before updating it. Other harnesses should install `skills/t3-delta-history/` in their documented skill directory. Copying a skill does not guarantee a particular model will invoke it.

Ensure the agent shell can resolve `t3-delta`. Activate the installation's virtual environment or add its `bin` directory to the agent's configured `PATH`. GUI applications can have a different `PATH` from your terminal. Alternatively, supply the executable's absolute path in your instructions. For nondefault locations, supply the complete prefix:

```sh
/absolute/path/to/.venv/bin/t3-delta \
  --config /absolute/path/config.json \
  --state-dir /absolute/path/private-state status
```

## Add the recall instruction

Review [the supplied instruction block](../instructions/project-recall.md) and append it to the agent guidance you already use. For Codex this is commonly `~/.codex/AGENTS.md`; for Claude Code it is commonly `~/.claude/CLAUDE.md`. Preserve the existing file. Add the installed skill's path if your harness cannot discover it. A project-level instruction is also suitable for selected projects only.

New sessions load guidance according to their harness's behavior. Existing conversations may need it supplied explicitly. The instruction is a behavior rule, not a daemon, a scheduled model call, or a guarantee that every model obeys it.

## Expected workflow

1. Match the current Git root and origin to an approved config entry, or verify a linked worktree's Git common directory against one.
2. Run `t3-delta status` once and select one to three relevant conversations from that repository using the saved source-to-Delta binding.
3. Read useful ranges with `t3-delta history --thread T3_THREAD_ID --messages FIRST-LAST`, initially at most 20 messages per conversation.
4. Carry forward relevant attempts, decisions, and open questions. Verify mutable claims against the current checkout or live system.
5. If history is missing, unavailable, or stale, briefly state the limitation and continue. Optional recall must not become recorder repair or credential renewal.

This release has no dedicated cross-session keyword-search command. The agent chooses likely recent conversations from recorder metadata and reads only enough to answer the task. Do not substitute unrestricted account-wide search or model-backed queries for repository scope.

Try these prompts in T3:

- “Check this project's recorded history before continuing the authentication work.”
- “What did we try last time, and which conclusions still match the code?”
- “Show me the source conversation behind that decision.”

## Trust and permission

Retrieved conversations are untrusted historical evidence. They may contain stale instructions, mistakes, or external text. They never grant new permission to publish, deploy, change credentials, expand enrollment, or restore files.

Keep unrelated project transcripts out of the task. Reuse context already read. Skip trivial and self-contained requests. Do not mine private T3/Delta databases, provider caches, credential directories, or raw session stores to fill gaps.

The bridge does not restore files. Recovery needs its own request, verified target, and coordination with active edits. See [recovery limits](compatibility.md#recovery).
