"""Emit synthetic visible messages through ingress v1 without a harness SDK."""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import uuid


def main():
    parser = argparse.ArgumentParser(description="Emit synthetic JSONL for an explicitly reviewed checkout")
    parser.add_argument("--project", type=Path, required=True)
    parser.add_argument("--instance", default="local")
    args = parser.parse_args()
    session = str(uuid.uuid4())
    events = [
        ("session.open", {"project": str(args.project.resolve())}),
        ("message.completed", {"role": "user", "text": "This is a synthetic integration check."}),
        ("message.completed", {"role": "assistant", "text": "The reference producer supplied visible text without a coding harness."}),
        ("session.close", {"status": "completed"}),
    ]
    for sequence, (kind, fields) in enumerate(events, start=1):
        print(json.dumps({"schema_version": 1, "type": kind, "source": "generic",
            "source_instance": args.instance, "session_id": session,
            "event_id": str(sequence), "sequence": sequence,
            "created_at": datetime.now(timezone.utc).isoformat(), **fields}))


if __name__ == "__main__":
    main()
