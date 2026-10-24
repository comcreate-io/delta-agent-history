# Run the recorder in your login session

Complete the README setup, explicit repository enrollment, scoped authentication,
and foreground verification first. The examples below assume the recorder's
Python virtual environment is activated and `command -v t3-delta` returns an
absolute executable path. Keep that environment in a permanent location: moving
or deleting it breaks the installed service.

These are **per-user login services**, not system-wide daemons. T3 Code and Zed
Delta must be running and signed in for capture and upload to make progress.
Starting this service does not launch or authenticate either application. When
they are unavailable, the recorder waits and retries. Inspect `t3-delta status`
after returning to your desktop.

The renderer only writes a service file. It never loads, starts, stops, or enables
a service. Do not run these commands with `sudo` or install both templates.
If your machine already has a recorder service, use that service's existing
configuration instead of installing a second copy.

## Paths and logs

Both platforms can use the package defaults:

```sh
mkdir -p "$HOME/.local/state/t3-delta-recorder"
chmod 700 "$HOME/.local/state/t3-delta-recorder"
```

The state directory must belong to you. Choose a local private directory, not a
symlink, shared folder, or cloud-synced folder. The service appends stdout and
stderr to `recorder.log` inside this directory with umask `077`. The renderer
does not create that directory. Logs contain sanitized status and error classes;
the state databases contain private conversation records. Neither belongs in a
Git repository.

GUI login services do not read interactive shell startup files. `init` resolves
`t3_binary` and `delta_binary` to absolute paths; verify they still point to the
correct executables after an application update. The templates provide standard
system executable directories for Git, including `/usr/bin` on macOS and the
usual Nix system profiles on Linux. If Git exists only in a custom shell PATH,
adjust the generated service's `PATH` explicitly before loading it. On macOS,
make sure `/usr/bin/git --version` succeeds (Apple's command-line tools may need
to be installed first).

Paths with spaces and shell metacharacters are passed as literal arguments.
Relative paths, control characters, and `..` components are rejected. Linux
state paths additionally reject double quotes, backslashes, and trailing spaces
because log-path directives use different parsing from command arguments.
The renderer refuses to overwrite a file unless `--force` is supplied and always
refuses a symlink output. Creating a new service file does not replace any
already loaded service; reload it explicitly after editing.

## Linux with a systemd user manager

Requires systemd 240 or newer for append-only file logging. From the activated
Python environment:

```sh
mkdir -p "$HOME/.config/systemd/user"
python -m t3_delta.services \
  --platform linux \
  --executable "$(command -v t3-delta)" \
  --config "$HOME/.config/t3-delta-recorder/config.json" \
  --state-dir "$HOME/.local/state/t3-delta-recorder" \
  --output "$HOME/.config/systemd/user/t3-delta-recorder.service"

systemd-analyze --user verify "$HOME/.config/systemd/user/t3-delta-recorder.service"
systemctl --user daemon-reload
systemctl --user enable --now t3-delta-recorder.service
systemctl --user status t3-delta-recorder.service
t3-delta status
```

The service starts with your user manager, normally at login. It does not require
or enable lingering; it has nothing to capture when your desktop applications
are closed. `Restart=on-failure` retries a failed process after ten seconds.

Read logs, stop, or restart:

```sh
tail -n 50 "$HOME/.local/state/t3-delta-recorder/recorder.log"
systemctl --user stop t3-delta-recorder.service
systemctl --user start t3-delta-recorder.service
systemctl --user restart t3-delta-recorder.service
```

To regenerate the same file, add `--force` to the renderer command, then run
`systemctl --user daemon-reload` and `systemctl --user restart
t3-delta-recorder.service`. Do not use `--force` to replace an unrelated service.

Uninstall only the service:

```sh
systemctl --user disable --now t3-delta-recorder.service
rm "$HOME/.config/systemd/user/t3-delta-recorder.service"
systemctl --user daemon-reload
```

See the authoritative [systemd service command rules](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html)
and [output/logging directives](https://www.freedesktop.org/software/systemd/man/latest/systemd.exec.html).

## Linux without systemd

The recorder can run directly in any Linux desktop terminal:

```sh
t3-delta \
  --config "$HOME/.config/t3-delta-recorder/config.json" \
  --state-dir "$HOME/.local/state/t3-delta-recorder" \
  run
```

Use Ctrl+C to stop it. For unattended operation, have your distribution's user
session supervisor run the same executable and arguments, using a private state
directory, restrictive umask, and a restart delay. No root access or system-wide
init integration is needed. This project does not install an OpenRC, runit,
s6, or desktop-autostart configuration.

## macOS LaunchAgent

Run these commands in Terminal while logged in to the desktop account that runs
T3 and Delta. This installs a LaunchAgent in your own GUI session.

```sh
mkdir -p "$HOME/Library/LaunchAgents"
python -m t3_delta.services \
  --platform darwin \
  --executable "$(command -v t3-delta)" \
  --config "$HOME/.config/t3-delta-recorder/config.json" \
  --state-dir "$HOME/.local/state/t3-delta-recorder" \
  --output "$HOME/Library/LaunchAgents/io.comcreate.t3-delta-recorder.plist"

plutil -lint "$HOME/Library/LaunchAgents/io.comcreate.t3-delta-recorder.plist"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/io.comcreate.t3-delta-recorder.plist"
launchctl print "gui/$(id -u)/io.comcreate.t3-delta-recorder"
t3-delta status
```

`RunAtLoad` starts the recorder when loaded or at subsequent GUI logins.
`KeepAlive.SuccessfulExit=false` restarts an unsuccessful process, throttled to
fifteen seconds. The plist supplies a separate `ProgramArguments` entry for each
argument, without a shell. User folders protected by macOS privacy controls may
require permission for the actual recorder/Python process. Approve only the
folders you intend to enroll; a permission failure is not a reason to run it as
root.

Read logs, stop, or start again:

```sh
tail -n 50 "$HOME/.local/state/t3-delta-recorder/recorder.log"
launchctl bootout "gui/$(id -u)/io.comcreate.t3-delta-recorder"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/io.comcreate.t3-delta-recorder.plist"
```

To restart a currently loaded agent:

```sh
launchctl kickstart -k "gui/$(id -u)/io.comcreate.t3-delta-recorder"
```

To replace its definition, first boot it out, regenerate using `--force`, lint,
and bootstrap the replacement. `bootstrap` normally rejects an already loaded
label. Use `launchctl print` to inspect the existing job rather than installing a
duplicate or repeatedly retrying it.

Uninstall only the LaunchAgent:

```sh
launchctl bootout "gui/$(id -u)/io.comcreate.t3-delta-recorder"
rm "$HOME/Library/LaunchAgents/io.comcreate.t3-delta-recorder.plist"
```

Apple documents the agent lifecycle and plist keys in
[Creating Launch Daemons and Agents](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html)
and its [launchd.plist reference source](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5).
Use the `man launchctl` and `man launchd.plist` pages shipped with your macOS
release for the current `bootstrap`, `bootout`, `print`, and `kickstart` commands.
The renderer's macOS XML and argument serialization are tested on Linux; real
macOS service startup and T3/Delta capture require verification on a Mac.

## Stopping is not deleting history

Stopping or uninstalling a service preserves the recorder's durable queue,
credentials, and Delta attachments. It does not delete a Delta thread, detach a
checkout, rewind files, or revoke your T3 session. Restarting uses the existing
state, including pending delivery. Do not remove the state directory as a routine
fix for an error: doing so loses delivery and identity bookkeeping.

Logs are not automatically rotated. Stop the recorder before archiving or
truncating a large `recorder.log`, preserve the directory permissions, then
restart it. Keep private logs and databases out of public support reports.
