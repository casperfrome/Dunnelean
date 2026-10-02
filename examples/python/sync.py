"""Submit a MySQL/Doris job through the installed, embedded Python library."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from uuid import uuid4

from dunnelean import Engine


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", type=Path, help="UTF-8 job JSON, such as examples/mysql-to-doris.json")
    parser.add_argument("--state", type=Path, default=Path("var/python-example.sqlite"), help="Exclusive SQLite state path")
    parser.add_argument("--request-id", help="Reuse a saved ID to find the same execution; use a new ID to rerun")
    parser.add_argument("--timeout", type=float, default=300, help="Local waiting deadline in seconds")
    parser.add_argument("--validate-only", action="store_true", help="Check configuration, connections and table schemas")
    parser.add_argument("--cancel", action="store_true", help="Request cancellation immediately after submission")
    return parser.parse_args()


def main() -> int:
    args = arguments()
    spec = json.loads(args.job.read_text(encoding="utf-8"))
    if args.request_id is not None:
        spec["request_id"] = args.request_id
    elif "request_id" not in spec:
        spec["request_id"] = str(uuid4())
    print("request_id=" + str(spec["request_id"]), flush=True)

    # Context exit closes the engine, cancels active jobs and waits for their outcomes.
    with Engine(args.state) as engine:
        engine.ready()
        print("state_store_id=" + engine.state_store_id, flush=True)
        validation = engine.validate(spec)
        if args.validate_only:
            print(json.dumps(validation, ensure_ascii=False, indent=2))
            return 0
        submitted = engine.submit(spec)
        print("run_id=" + submitted["run_id"], flush=True)
        if args.cancel:
            engine.cancel_request(spec["request_id"])
        result = engine.wait_for_completion(submitted["run_id"], timeout=args.timeout, poll_interval=0.5)
        summary = {key: result[key] for key in (
            "run_id", "state", "rows_committed", "server_affected_rows", "partial_write", "commit_unknown", "error",
        )}
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if result["state"] == "SUCCEEDED" or (args.cancel and result["state"] == "CANCELLED") else 1


if __name__ == "__main__":
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    raise SystemExit(main())
