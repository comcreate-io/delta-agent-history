# Contributing

Use Python 3.11 or newer and Git. Runtime code uses the Python standard library.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m unittest discover -s tests -v
```

On NixOS, use `nix develop` instead of installing global tools.

Tests must use temporary repositories and synthetic transcripts. Do not connect
CI to a real T3 or Delta account. Never commit credentials, recorder state,
personal paths, customer inventories, or real conversation fixtures.

Preserve immutable replay, exact repository scope, private visibility, and
read-only T3 permissions. Protocol updates need authoritative upstream evidence,
new compatibility tests, and an explicit review of newly accepted output. Do not
silently broaden the contract range to make an upstream version pass.

Test neutral ingress, source adapters, project-bound MCP recall and unchanged
legacy replay behavior. Preserve qualified source identities, local-only hooks,
sequence gaps and sticky conflicts. Never rely on model invocation to capture a
session. Node is required for the OpenCode plugin fixture test; no real provider
account or upstream model run is needed.

For platform changes, test both service renderers and the Ubuntu/macOS CI matrix.
Mocked tests do not establish live account, upload, or historical restore behavior.
Report those checks separately in your pull request.
