"""Offline configuration for explicitly selected repository checkouts."""

import json
import os
from pathlib import Path
import shutil

from .engine import check_filenames, checkout_identity, git_value, utc_now


class SetupError(ValueError):
    """Safe, actionable setup feedback that contains no command output."""


def executable(value, default):
    candidate = str(Path(value).expanduser()) if value else shutil.which(default)
    if not candidate:
        raise SetupError(f"Cannot locate {default}; supply its explicit executable path.")
    if not os.path.isabs(candidate):
        candidate = shutil.which(candidate)
    if not candidate:
        raise SetupError(f"Cannot locate {default}; supply its explicit executable path.")
    try:
        path = Path(candidate).resolve(strict=True)
    except OSError:
        raise SetupError(f"Cannot locate {default}; supply its explicit executable path.") from None
    if not path.is_file() or not os.access(path, os.X_OK):
        raise SetupError(f"The selected {default} path must be an executable file.")
    return str(path)


def initialize(config_path, *, projects, environment_id, runtime_file, t3_binary=None,
               delta_binary=None, accept_upload=False, delta_organization=None, t3_base_dir=None):
    """Write an exclusive private config. Never authenticate, upload, or run a service."""
    if not accept_upload:
        raise SetupError("Review the repositories, then pass --accept-upload to allow future private Delta uploads of conversations and checkout files, including tracked and nonignored data.")
    if not environment_id or not environment_id.strip() or environment_id != environment_id.strip():
        raise SetupError("Supply the expected T3 environment ID with --environment-id.")
    rules = []
    seen = set()
    cutoff = utc_now()
    for value in projects:
        try:
            directory, _ = checkout_identity(Path(value).expanduser())
            if directory in seen:
                raise SetupError("Select each repository root only once.")
            # Reuse the engine's supported remote validation and filename boundary.
            remote = git_value(directory, "config", "--get", "remote.origin.url")
            check_filenames(directory)
        except SetupError:
            raise
        except Exception:
            raise SetupError("Each --project must be a readable Git root with a supported, credential-free origin and no sensitive-looking tracked or nonignored filenames. Review the selected repository.") from None
        seen.add(directory)
        rules.append({"path": directory, "remote": remote, "enrolled_after": cutoff})
    if not rules:
        raise SetupError("Select at least one repository with --project.")
    root = Path(rules[0]["path"]).parent
    config = {
        "environment_id": environment_id,
        "t3_runtime_file": str(Path(runtime_file).expanduser().resolve()),
        "t3_binary": executable(t3_binary, "t3"),
        # Do not fall back to `delta`: that is commonly the Git diff pager.
        "delta_binary": executable(delta_binary, "zed-delta"),
        "poll_seconds": 10,
        "projects_root": str(root),
        "additional_project_paths": [r["path"] for r in rules if not Path(r["path"]).is_relative_to(root)],
        "allowed_remotes": sorted({r["remote"] for r in rules}),
        "allowed_projects": rules,
    }
    if delta_organization:
        config["delta_organization"] = delta_organization
    if t3_base_dir:
        config["t3_base_dir"] = str(Path(t3_base_dir).expanduser().resolve())
    target = Path(config_path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if target.parent.stat().st_uid != os.getuid():
        raise SetupError("The configuration directory must belong to the current user.")
    # O_EXCL refuses existing files and symlinks, including dangling symlinks.
    try:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        raise SetupError("Configuration already exists; choose another --config path or review it manually.") from None
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(config, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return {"initialized": True, "config": str(target.resolve()),
            "approved_project_count": len(rules), "enrolled_after": cutoff,
            "recording_started": False, "credential_issued": False}
