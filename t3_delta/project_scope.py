"""An explicit private project registry shared by independently configured workers."""

import copy
import datetime as dt
import json
import os
from pathlib import Path
import stat

from .engine import remote_key, timestamp


MAX_REGISTRY_BYTES = 1024 * 1024


def registry_path(config):
    value = config.get("project_enrollment_file")
    if value is None:
        return None
    path = Path(value) if isinstance(value, str) else Path()
    if not path.is_absolute() or str(path) != str(path.resolve()):
        raise ValueError("Project registry must be an exact canonical absolute path")
    return path


def validate_registry(value):
    if (not isinstance(value, dict) or set(value) != {"schema_version", "projects"}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or not isinstance(value["projects"], list)):
        raise ValueError("Invalid project registry")
    seen = set()
    for rule in value["projects"]:
        if not isinstance(rule, dict) or set(rule) != {"path", "remote", "enrolled_after"}:
            raise ValueError("Invalid registry project")
        path = Path(rule["path"])
        if not path.is_absolute() or str(path) != str(path.resolve()) or str(path) in seen:
            raise ValueError("Registry projects must be unique canonical absolute roots")
        remote_key(rule["remote"])
        cutoff = dt.datetime.fromisoformat(rule["enrolled_after"].replace("Z", "+00:00"))
        if cutoff.tzinfo is None or cutoff.utcoffset() != dt.timedelta(0):
            raise ValueError("Registry enrollment requires an explicit UTC cutoff")
        seen.add(str(path))
    return value


def read_registry(path):
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    except FileNotFoundError:
        return {"schema_version": 1, "projects": []}
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("Project registry must be a private owned regular file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(MAX_REGISTRY_BYTES + 1)
        if len(data) > MAX_REGISTRY_BYTES:
            raise ValueError("Project registry exceeds its byte limit")
        return validate_registry(json.loads(data))
    finally:
        os.close(fd)


def merge_registry(config, registry):
    validate_registry(registry)
    result = copy.deepcopy(config)
    known = {rule["path"]: rule for rule in config["allowed_projects"]}
    root = Path(config["projects_root"])
    for rule in registry["projects"]:
        old = known.get(rule["path"])
        if old:
            if (remote_key(old["remote"]) != remote_key(rule["remote"])
                    or timestamp(old["enrolled_after"]) != timestamp(rule["enrolled_after"])):
                raise ValueError("Registry conflicts with a base repository binding")
            continue
        result["allowed_projects"].append(copy.deepcopy(rule))
        known[rule["path"]] = rule
        if remote_key(rule["remote"]) not in {remote_key(r) for r in result["allowed_remotes"]}:
            result["allowed_remotes"].append(rule["remote"])
        if not Path(rule["path"]).is_relative_to(root):
            if rule["path"] not in result.setdefault("additional_project_paths", []):
                result["additional_project_paths"].append(rule["path"])
    return result


def with_project_registry(config):
    path = registry_path(config)
    return merge_registry(config, read_registry(path)) if path is not None else config
