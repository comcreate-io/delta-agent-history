"""Explicit, read-only recall of the original recorder's T3 bindings.

Nothing is discovered, migrated, or replayed here. Legacy producer identities and
outbox bytes remain owned by the original worker and its original configuration.
"""

import hashlib
from pathlib import Path

from .configuration import inspect_scope
from t3_delta.cli import load_config, read_status, history as t3_history
from t3_delta.engine import common_directory, checkout_identity


def _source(config, project):
    # Even an absent legacy source must not bypass the caller's project boundary.
    actual, _ = inspect_scope(config, project)
    link = config.get("legacy_t3")
    if link is None:
        return None
    path, state = Path(link["config"]), Path(link["state"])
    if path.is_symlink() or state.is_symlink() or not path.is_file() or not state.is_dir():
        raise ValueError("The explicitly linked legacy recorder is unavailable")
    original = load_config(path)
    inspect_scope(original, actual)
    return original, state, actual


def _key(external_id):
    return "legacy-t3:" + hashlib.sha256(external_id.encode()).hexdigest()


def _select(config, project):
    source = _source(config, project)
    if source is None:
        return None, []
    original, state, actual = source
    observed = read_status(state, original)
    common = common_directory(actual)
    expected_remote = checkout_identity(actual)[1]
    bindings = {item["external_id"]: item for item in observed["delta"]}
    selected = []
    for item in observed["threads"]:
        binding = bindings.get(item["external_id"])
        if not binding or not binding.get("thread_id"):
            continue
        try:
            directory = item["directory"]
            if (binding["directory"] != directory
                    or checkout_identity(directory)[1] != expected_remote
                    or common_directory(directory) != common):
                continue
            inspect_scope(config, directory)
            inspect_scope(original, directory)
        except (OSError, ValueError):
            continue
        selected.append({
            "key": _key(item["external_id"]),
            "session_id": item["thread_id"],
            "source": "t3-code",
            "source_instance": original["environment_id"],
            "project": directory,
            "phase": item["phase"],
            "message_count": binding["message_count"],
            "pending": binding["pending"],
            "acknowledged": binding["acknowledged"],
            "visibility": binding["visibility"],
            "upload": binding["upload"],
            "last_ack": binding["last_ack"],
            "attached_at": item["attached_at"],
            "legacy": True,
        })
    return (original, state), selected


def sessions(config, project):
    """Return only bindings inside both explicitly approved repository scopes."""
    return _select(config, project)[1]


def history(config, project, session, messages="1-20"):
    source, items = _select(config, project)
    match = next((item for item in items if item["key"] == session), None)
    if source is None or match is None:
        raise ValueError("Legacy session is not available in this approved repository")
    original, state = source
    return t3_history(state, original, match["session_id"], messages)
