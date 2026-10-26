"""Explicit local enrollment without harness-specific credentials or discovery."""
import json
import os
from pathlib import Path

from t3_delta.engine import checkout_identity, check_filenames, git_value, remote_key, utc_now, common_directory
from t3_delta.setup import executable, SetupError
from .protocol import SLUG, utc_timestamp
from t3_delta.project_scope import registry_path, with_project_registry


def validate(config):
    if not isinstance(config, dict) or config.get("schema_version") != 2:
        raise ValueError("Generic history requires configuration schema_version 2")
    sources = config.get("sources")
    if not isinstance(sources, list) or not sources:
        raise ValueError("Enroll at least one source kind and instance")
    seen = set()
    for source in sources:
        if not isinstance(source, dict) or set(source) != {"kind", "instance"} or not all(isinstance(source[k], str) and SLUG.fullmatch(source[k]) for k in ("kind", "instance")):
            raise ValueError("Invalid enrolled source")
        pair = source["kind"], source["instance"]
        if pair in seen:
            raise ValueError("Duplicate enrolled source")
        seen.add(pair)
    root = Path(config.get("projects_root", ""))
    if not root.is_absolute() or str(root) != str(root.resolve()):
        raise ValueError("projects_root must be an exact canonical path")
    extras = config.get("additional_project_paths", [])
    if not isinstance(extras, list) or any(not isinstance(p, str) or not Path(p).is_absolute() or str(Path(p).resolve()) != p for p in extras):
        raise ValueError("Invalid additional repository boundaries")
    remotes = config.get("allowed_remotes")
    rules = config.get("allowed_projects")
    if not isinstance(remotes, list) or not remotes or not isinstance(rules, list) or not rules:
        raise ValueError("An exact repository allowlist is required")
    allowed = {remote_key(r) for r in remotes}
    paths = set()
    for rule in rules:
        if not isinstance(rule, dict) or set(rule) != {"path", "remote", "enrolled_after"}:
            raise ValueError("Invalid repository rule")
        path = Path(rule["path"])
        if not path.is_absolute() or str(path) != str(path.resolve()) or str(path) in paths or (not path.is_relative_to(root) and str(path) not in extras) or remote_key(rule["remote"]) not in allowed:
            raise ValueError("Invalid exact repository boundary")
        utc_timestamp(rule["enrolled_after"])
        paths.add(str(path))
    binary = config.get("delta_binary")
    if not isinstance(binary, str) or not Path(binary).is_absolute():
        raise ValueError("delta_binary must be an absolute executable path")
    legacy = config.get("legacy_t3")
    if legacy is not None:
        if not isinstance(legacy, dict) or set(legacy) != {"config", "state"} or any(not isinstance(v, str) or not Path(v).is_absolute() or str(Path(v).resolve()) != v for v in legacy.values()):
            raise ValueError("legacy_t3 requires exact canonical absolute config and state paths")
    registry_path(config)
    return config


def load(path):
    base = validate(json.loads(Path(path).expanduser().read_text()))
    return validate(with_project_registry(base))


def inspect_scope(config, project):
    """Return (actual checkout, approved root) after checking current Git identity."""
    path = Path(project).expanduser()
    if not path.is_absolute() or str(path) != str(path.resolve(strict=True)):
        raise ValueError("Project must be a canonical absolute checkout, not a symlink")
    actual, remote = checkout_identity(path)
    common = common_directory(actual)
    for rule in config["allowed_projects"]:
        try:
            approved, current = checkout_identity(rule["path"])
            if current == remote_key(rule["remote"]) == remote and common_directory(approved) == common:
                return actual, approved
        except (OSError, ValueError):
            continue
    raise ValueError("Project is not an approved repository or its linked worktree")


def resolve_project(config, project):
    return inspect_scope(config, project)[0]


def initialize(config_path, *, projects, sources, delta_binary=None, accept_upload=False, delta_organization=None, legacy_t3=None, project_enrollment_file=None):
    if not accept_upload:
        raise SetupError("Pass --accept-upload after reviewing the selected repositories. Future recording uploads visible conversations and tracked/nonignored checkout files privately to Delta.")
    enrollments = []
    for source in sources:
        pieces = source.split(":") if isinstance(source, str) else []
        if len(pieces) != 2:
            raise SetupError("Sources use KIND:INSTANCE, for example codex:workstation.")
        enrollments.append({"kind": pieces[0], "instance": pieces[1]})
    cutoff, rules = utc_now(), []
    try:
        for value in projects:
            original = Path(value).expanduser()
            if not original.is_absolute() or str(original) != str(original.resolve()):
                raise ValueError("Use canonical absolute repository roots, not symlinks")
            directory, _ = checkout_identity(original)
            check_filenames(directory)
            rules.append({"path": directory, "remote": git_value(directory, "config", "--get", "remote.origin.url"), "enrolled_after": cutoff})
    except Exception:
        raise SetupError("Each project must be a canonical absolute Git root with a supported credential-free origin and reviewed filenames.") from None
    if not rules:
        raise SetupError("Enroll at least one explicit repository root.")
    root = Path(rules[0]["path"]).parent
    config = {"schema_version": 2, "sources": enrollments, "projects_root": str(root),
              "additional_project_paths": [r["path"] for r in rules if not Path(r["path"]).is_relative_to(root)],
              "allowed_projects": rules, "allowed_remotes": sorted({r["remote"] for r in rules}),
              "delta_binary": executable(delta_binary, "zed-delta")}
    if delta_organization:
        config["delta_organization"] = delta_organization
    if project_enrollment_file is not None:
        config["project_enrollment_file"] = str(Path(project_enrollment_file).expanduser())
    if legacy_t3 is not None:
        config["legacy_t3"] = {k: str(Path(v).expanduser().resolve()) for k, v in legacy_t3.items()}
        if not Path(config["legacy_t3"].get("config", "")).is_file() or not Path(config["legacy_t3"].get("state", "")).is_dir():
            raise SetupError("Legacy T3 config and state must already exist; they are linked read-only, never migrated.")
    validate(config)
    target = Path(config_path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if target.parent.stat().st_uid != os.getuid():
        raise SetupError("Configuration directory must belong to the current user")
    try:
        fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        raise SetupError("Configuration already exists; use another path or review it manually.") from None
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(config, stream, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        target.unlink(missing_ok=True)
        raise
    return {"initialized": True, "config": str(target.resolve()), "approved_project_count": len(rules),
            "source_count": len(enrollments), "enrolled_after": cutoff, "recording_started": False}
