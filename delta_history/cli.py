"""Harness-neutral command line. Ingestion acknowledges local durability only."""

import argparse
import contextlib
import json
import os
from pathlib import Path
import stat
import sys
import time


MAX_LINE = 1024 * 1024
MAX_BATCH = 4 * 1024 * 1024
MAX_EVENTS = 100
MAX_OUTPUT = 2 * 1024 * 1024


def _strict_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate JSON key")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError("Nonfinite JSON number")


def parse_json(value):
    return json.loads(value, object_pairs_hook=_strict_object, parse_constant=_invalid_constant)


def read_events(stream):
    """Bound memory and bytes before accepting a whole JSONL submission."""
    events = []
    total = 0
    while True:
        line = stream.readline(MAX_LINE + 2)
        if not line:
            break
        raw = line.encode("utf-8") if isinstance(line, str) else line
        total += len(raw)
        if len(raw) > MAX_LINE or total > MAX_BATCH:
            raise ValueError("Ingress input exceeded its budget")
        if not raw.strip():
            raise ValueError("Ingress blank lines are not events")
        value = parse_json(raw.decode("utf-8"))
        if not isinstance(value, dict) or len(events) >= MAX_EVENTS:
            raise ValueError("Ingress requires at most 100 event objects")
        events.append(value)
    if not events:
        raise ValueError("Ingress input is empty")
    return events


@contextlib.contextmanager
def private_input(path):
    """Never follow an input-file symlink or consume a group-readable export."""
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("Ingress input file must be private, regular, and owned by you")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            yield stream
    finally:
        os.close(descriptor)


def print_json(value):
    result = json.dumps(value, ensure_ascii=True, allow_nan=False)
    if len(result.encode("utf-8")) > MAX_OUTPUT:
        raise ValueError("Response exceeded its output budget")
    print(result)


def drain_once(config, state):
    """Replay frozen adapter events before delivery; never invoke a model."""
    from . import core
    from .adapters.common import retry_pending
    try:
        adapters = retry_pending(config, state)
    except Exception as error:
        # A broken adapter journal must not prevent an existing core queue drain.
        adapters = {"sessions": 0, "submitted": 0, "halted": 0, "unavailable": 1,
                    "errors": [type(error).__name__]}
    value = dict(core.flush(config, state))
    value["adapters"] = adapters
    value["halted"] = value.get("halted", 0) + adapters.get("halted", 0)
    value["catching_up"] = value.get("catching_up", 0) + adapters.get("unavailable", 0)
    return value


def drain_run(config, state):
    """Keep draining after transient failures without logging exception values."""
    failures = 0
    while True:
        try:
            print_json(drain_once(config, state))
            failures = 0
        except Exception as error:
            failures = min(failures + 1, 5)
            print(json.dumps({"error": type(error).__name__, "message": "Queue delivery unavailable; retrying."}), file=sys.stderr)
        time.sleep(min(300, config.get("poll_seconds", 10) * 2 ** failures))


def parser():
    root = argparse.ArgumentParser(description="Private agent conversation capture and repository-scoped Delta recall")
    root.add_argument("--config", type=Path, default=Path.home() / ".config/delta-agent-history/config.json")
    root.add_argument("--state-dir", type=Path, default=Path.home() / ".local/state/delta-agent-history")
    commands = root.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("init", help="Enroll exact repositories and producer instances offline")
    initialize.add_argument("--project", type=Path, action="append", required=True)
    initialize.add_argument("--source", action="append", required=True, metavar="KIND:INSTANCE")
    initialize.add_argument("--delta-binary")
    initialize.add_argument("--delta-organization")
    initialize.add_argument("--accept-upload", action="store_true")
    initialize.add_argument("--legacy-t3-config", type=Path, help="Explicit read-only link to an existing T3 recorder config")
    initialize.add_argument("--legacy-t3-state", type=Path, help="Explicit read-only link to the existing T3 recorder state")
    ingest = commands.add_parser("ingest", help="Accept bounded JSONL into the private local queue; no upload")
    ingest.add_argument("--file", type=Path)
    commands.add_parser("once", help="Drain one delivery cycle")
    commands.add_parser("run", help="Drain the queue until stopped; does not discover transcripts")
    commands.add_parser("status", help="Read local recorder status")
    sessions = commands.add_parser("sessions", help="List sessions for one enrolled repository")
    sessions.add_argument("--project", type=Path, required=True)
    history = commands.add_parser("history", help="Read up to 20 messages in an enrolled repository session")
    history.add_argument("--project", type=Path, required=True)
    history.add_argument("--session", required=True)
    history.add_argument("--messages", default="1-20")
    mcp = commands.add_parser("mcp", help="Serve read-only stdio MCP for one approved repository")
    mcp.add_argument("--project", type=Path, required=True)
    hook = commands.add_parser("hook", help="Submit a documented harness hook to the local queue")
    hook.add_argument("kind", choices=("claude", "opencode"))
    hook.add_argument("--instance", required=True)
    codex = commands.add_parser("codex", help="Record a wrapped noninteractive Codex exec run")
    codex.add_argument("--instance", required=True)
    codex.add_argument("--project", type=Path, required=True)
    codex.add_argument("--codex-binary", required=True)
    codex.add_argument("--prompt-file", type=Path, required=True)
    codex.add_argument("--model")
    legacy = commands.add_parser("t3", help="Pass remaining arguments to the unchanged T3 adapter")
    legacy.add_argument("arguments", nargs=argparse.REMAINDER)
    return root


def main(argv=None):
    # Dispatch legacy flags verbatim: its original defaults and identity stay intact.
    arguments = list(sys.argv[1:] if argv is None else argv)
    if arguments and arguments[0] == "t3":
        from t3_delta.cli import main as legacy_main
        return legacy_main(arguments[1:])
    args = parser().parse_args(arguments)
    try:
        if args.command == "t3":
            from t3_delta.cli import main as legacy_main
            return legacy_main(args.arguments)
        from . import configuration, core
        state = args.state_dir.expanduser()
        if args.command == "init":
            if bool(args.legacy_t3_config) != bool(args.legacy_t3_state):
                raise ValueError("Legacy T3 config and state must be supplied together")
            legacy = None if not args.legacy_t3_config else {
                "config": str(args.legacy_t3_config.expanduser().absolute()),
                "state": str(args.legacy_t3_state.expanduser().absolute()),
            }
            value = configuration.initialize(args.config.expanduser(), projects=args.project, sources=args.source,
                delta_binary=args.delta_binary, accept_upload=args.accept_upload, delta_organization=args.delta_organization,
                legacy_t3=legacy)
        else:
            config = configuration.load(args.config.expanduser())
            if args.command == "ingest":
                if args.file:
                    with private_input(args.file.expanduser()) as stream:
                        events = read_events(stream)
                else:
                    events = read_events(getattr(sys.stdin, "buffer", sys.stdin))
                value = core.submit(config, state, events)
            elif args.command == "once":
                value = drain_once(config, state)
            elif args.command == "run":
                return drain_run(config, state)
            elif args.command == "status":
                value = core.status(config, state)
            elif args.command == "sessions":
                from .mcp import project_sessions
                value = project_sessions(config, state, args.project.expanduser())
            elif args.command == "history":
                from .mcp import project_history
                value = project_history(config, state, args.project.expanduser(), args.session, args.messages)
            elif args.command == "mcp":
                from .mcp import serve
                return serve(config, state, args.project.expanduser())
            elif args.command == "hook":
                if args.kind == "claude":
                    from .adapters.claude import claude_hook
                    value = claude_hook(config, state, args.instance, sys.stdin)
                else:
                    from .adapters.opencode import opencode_hook
                    value = opencode_hook(config, state, args.instance, sys.stdin)
            elif args.command == "codex":
                from .adapters.codex import codex_run
                return codex_run(config, state, args.instance, args.project.expanduser(), args.codex_binary, args.prompt_file.expanduser(), model=args.model)
        print_json(value)
        if args.command in ("ingest", "hook") and isinstance(value, dict) and (value.get("rejected") or value.get("halted")):
            return 2
        if args.command == "once" and isinstance(value, dict) and (value.get("catching_up") or value.get("halted") or value.get("busy")):
            return 2
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        # Exception messages may contain transcript text, local paths, or credentials.
        print(json.dumps({"error": type(error).__name__, "message": "History request failed; check configuration and local status."}), file=sys.stderr)
        return 2
