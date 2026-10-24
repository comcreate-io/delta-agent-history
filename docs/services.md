# Background delivery on Linux and macOS

First verify enrollment, local ingestion, and foreground delivery. The service
runs the **generic queue drainer**. Configure a hook, wrapper or plugin separately
for capture. The service does not discover arbitrary applications' transcripts.
Existing T3 polling services remain separate and unchanged.

Examples assume an activated virtual environment in a permanent location. Use
absolute executable/config/state paths. GUI sessions do not load your interactive
shell startup files. Delta's path is stored in config; Git must also be available
on the service PATH. Templates include standard system paths and Linux Nix profiles.

```sh
mkdir -p "$HOME/.local/state/delta-agent-history"
chmod 700 "$HOME/.local/state/delta-agent-history"
```

Private logs append to `recorder.log` inside this directory; SQLite journals also
contain private messages. Do not use a shared/cloud-synced directory or symlink.
The renderer only writes a mode 600 file and never activates a job. It refuses
existing files unless `--force` is explicit, and rejects symlink outputs.

## Linux systemd user service

Requires systemd 240+ for append-only file logging. No sudo or lingering is needed:

```sh
mkdir -p "$HOME/.config/systemd/user"
delta-history-service --platform linux \
  --executable "$(command -v delta-history)" \
  --config "$HOME/.config/delta-agent-history/config.json" \
  --state-dir "$HOME/.local/state/delta-agent-history" \
  --output "$HOME/.config/systemd/user/delta-agent-history.service"
systemd-analyze --user verify "$HOME/.config/systemd/user/delta-agent-history.service"
systemctl --user daemon-reload
systemctl --user enable --now delta-agent-history.service
systemctl --user status delta-agent-history.service
```

Stop/start/restart with `systemctl --user stop|start|restart delta-agent-history`.
After changing the definition, run daemon-reload and restart. To uninstall only
the service, disable it with `--now`, remove that unit file, then daemon-reload.

On Linux without systemd, use `delta-history run` in a terminal, or configure your
user-session supervisor to launch the same literal arguments with restrictive
umask and restart delay. This project does not install OpenRC/runit/s6 integration.

## macOS LaunchAgent

Run in your own logged-in GUI account, never with sudo:

```sh
mkdir -p "$HOME/Library/LaunchAgents"
delta-history-service --platform darwin \
  --executable "$(command -v delta-history)" \
  --config "$HOME/.config/delta-agent-history/config.json" \
  --state-dir "$HOME/.local/state/delta-agent-history" \
  --output "$HOME/Library/LaunchAgents/io.comcreate.delta-agent-history.plist"
plutil -lint "$HOME/Library/LaunchAgents/io.comcreate.delta-agent-history.plist"
launchctl bootstrap "gui/$(id -u)" "$HOME/Library/LaunchAgents/io.comcreate.delta-agent-history.plist"
launchctl print "gui/$(id -u)/io.comcreate.delta-agent-history"
```

Stop with `launchctl bootout "gui/$(id -u)/io.comcreate.delta-agent-history"`.
Start again with bootstrap. Restart a loaded job with
`launchctl kickstart -k "gui/$(id -u)/io.comcreate.delta-agent-history"`.
For replacement, boot it out, render with `--force`, lint, then bootstrap.
To uninstall only the LaunchAgent, boot it out and remove its plist.

The job starts at GUI login and restarts failed runs. Arguments are distinct plist
entries, not shell strings. `/usr/bin/git --version` must work; install Apple's
command-line tools if needed. macOS privacy controls may require approval for the
actual Python process to access the enrolled folder. Do not solve that by running
as root. Unit CI is separate from live desktop upload verification.

## Lifecycle and logs

```sh
tail -n 50 "$HOME/.local/state/delta-agent-history/recorder.log"
delta-history status
```

Stop the service before deliberately changing identity/configuration or archiving
large logs. Logs are not automatically rotated. A service being alive is not proof
of ingestion, acknowledgement or cloud upload. Delta must be available and signed
in; transient delivery failures retain queued data.

Stopping preserves queues and Delta attachments. It neither detaches existing
checkouts nor deletes cloud history or rewinds files. Do not remove state to clear
a conflict. Keep the original T3 worker on its own original state and label.
