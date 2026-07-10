#!/usr/bin/env python3
"""Start a finite historical seed through the API and wait for completion."""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def request_json(
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"} if body is not None else {},
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.load(response)
    except HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {response_body}") from exc
    except URLError as exc:
        raise RuntimeError(f"Failed to connect to {url}: {exc.reason}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--total-records", type=int, default=1_296_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=1_800.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_url = args.base_url.rstrip("/")
    start_url = f"{base_url}/api/v1/stock/generate"
    status_url = f"{start_url}/status"
    payload = {
        "mode": "historical",
        "totalRecords": args.total_records,
        "days": args.days,
        "batchSize": args.batch_size,
    }

    try:
        start_response = request_json(start_url, method="POST", payload=payload)
        generation = start_response["generation"]
        if generation["mode"] != "historical":
            raise RuntimeError("A non-historical generation task is already running")
        if generation["requestedRecords"] != args.total_records:
            raise RuntimeError("A historical seed with different settings is running")

        deadline = time.monotonic() + args.timeout
        last_inserted = -1
        while True:
            status = request_json(status_url)
            inserted = int(status["insertedRecords"])
            if inserted != last_inserted:
                print(
                    f"seed progress: {inserted}/{args.total_records}",
                    file=sys.stderr,
                    flush=True,
                )
                last_inserted = inserted

            state = status["state"]
            if state == "completed":
                if inserted != args.total_records:
                    raise RuntimeError(
                        f"Seed completed with {inserted}/{args.total_records} rows"
                    )
                json.dump(status, sys.stdout, separators=(",", ":"))
                sys.stdout.write("\n")
                return 0
            if state in {"failed", "stopped"}:
                raise RuntimeError(
                    f"Historical seed ended with state={state}, error={status['error']}"
                )
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Historical seed exceeded {args.timeout:g}s timeout"
                )

            time.sleep(args.poll_interval)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        print(f"seed failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
