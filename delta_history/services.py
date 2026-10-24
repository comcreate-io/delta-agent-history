"""Neutral queue-drainer services; legacy T3 services remain unchanged."""

import argparse
import sys

from t3_delta.services import render_service as _legacy_render, write_service


LABEL = "io.comcreate.delta-agent-history"
SERVICE_NAME = "delta-agent-history.service"


def render_service(platform, executable, config, state_dir):
    content = _legacy_render(platform, executable, config, state_dir)
    if platform == "darwin":
        import plistlib
        value = plistlib.loads(content)
        value["Label"] = LABEL
        return plistlib.dumps(value, sort_keys=True)
    return content.replace(b"Description=Private T3 to Delta recorder", b"Description=Private Delta Agent History queue drainer")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Render a Delta Agent History login service without starting it")
    parser.add_argument("--platform", choices=("linux", "darwin"), required=True)
    parser.add_argument("--executable", required=True, help="Absolute delta-history executable")
    parser.add_argument("--config", required=True)
    parser.add_argument("--state-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args(argv)
    try:
        content = render_service(args.platform, args.executable, args.config, args.state_dir)
        write_service(args.output, content, force=args.force)
    except (OSError, ValueError) as error:
        print('{"error":"ServiceRenderFailed","message":"Check absolute paths, output permissions, and overwrite opt-in."}', file=sys.stderr)
        return 2
    print('{"written":true,"activated":false}')
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
