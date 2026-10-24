"""Render per-user services without starting them or changing recorder state."""

import argparse
import os
from pathlib import Path
import plistlib
import sys
import tempfile


LABEL = "io.comcreate.t3-delta-recorder"
SERVICE_NAME = "t3-delta-recorder.service"
SERVICE_PATH = "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
LINUX_SERVICE_PATH = SERVICE_PATH + ":/run/current-system/sw/bin:/nix/var/nix/profiles/default/bin"


def absolute_path(value):
    """Do not expand variables or let a path add service-file directives."""
    value = os.fspath(value)
    if not value.startswith("/") or any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError("Service paths must be absolute and contain no control characters")
    if ".." in Path(value).parts:
        raise ValueError("Service paths must not contain parent-directory components")
    return value


def _systemd_argument(value):
    # ExecStart has its own quoting and expands both specifiers and variables.
    value = value.replace("\\", "\\\\").replace('"', '\\"')
    return '"' + value.replace("%", "%%").replace("$", "$$") + '"'


def render_service(platform, executable, config, state_dir):
    """Return UTF-8 bytes. All command arguments are literal, absolute paths."""
    executable, config, state_dir = map(absolute_path, (executable, config, state_dir))
    arguments = [executable, "--config", config, "--state-dir", state_dir, "run"]
    log_path = str(Path(state_dir) / "recorder.log")
    if platform == "darwin":
        return plistlib.dumps({
            "Label": LABEL,
            "ProgramArguments": arguments,
            "RunAtLoad": True,
            "KeepAlive": {"SuccessfulExit": False},
            "ThrottleInterval": 15,
            "Umask": 0o077,
            "EnvironmentVariables": {"PATH": SERVICE_PATH},
            "StandardOutPath": log_path,
            "StandardErrorPath": log_path,
        }, sort_keys=True)
    if platform != "linux":
        raise ValueError("Supported service platforms are linux and darwin")
    # StandardOutput's path is a scalar, not an ExecStart argument. Reject the
    # ambiguous quoting/escape characters rather than guessing at its grammar.
    if any(char in state_dir for char in ('"', "\\")) or state_dir.endswith(" "):
        raise ValueError("Linux service state paths cannot contain double quotes, backslashes, or trailing spaces")
    log_path = log_path.replace("%", "%%")
    command = " ".join(_systemd_argument(value) for value in arguments)
    return (
        "[Unit]\n"
        "Description=Private T3 to Delta recorder\n"
        "After=graphical-session.target\n\n"
        "[Service]\n"
        "Type=simple\n"
        f"ExecStart={command}\n"
        "Restart=on-failure\n"
        "RestartSec=10\n"
        "UMask=0077\n"
        f"Environment=PATH={LINUX_SERVICE_PATH}\n"
        f"StandardOutput=append:{log_path}\n"
        f"StandardError=append:{log_path}\n\n"
        "[Install]\n"
        "WantedBy=default.target\n"
    ).encode("utf-8")


def write_service(output, content, force=False):
    """Write a private file; do not follow symlinks or create directories."""
    output = Path(absolute_path(output))
    if output.is_symlink() or (output.exists() and not output.is_file()):
        raise ValueError("Service output must be a regular file, not a symlink")
    if not force:
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        return
    fd, temporary = tempfile.mkstemp(prefix=".t3-delta-service-", dir=output.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
        os.replace(temporary, output)
    finally:
        Path(temporary).unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render a user service without loading or starting it")
    parser.add_argument("--platform", choices=("linux", "darwin"), required=True)
    parser.add_argument("--executable", required=True, help="Absolute installed t3-delta executable")
    parser.add_argument("--config", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true", help="Replace an existing regular service file")
    args = parser.parse_args(argv)
    try:
        content = render_service(args.platform, args.executable, args.config, args.state_dir)
        write_service(args.output, content, force=args.force)
    except (OSError, ValueError):
        print("Service rendering failed. Check absolute paths, output permissions, and --force for an existing file.", file=sys.stderr)
        return 2
    print("Service file written. It has not been loaded or started.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
