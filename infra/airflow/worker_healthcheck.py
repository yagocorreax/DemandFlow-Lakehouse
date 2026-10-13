#!/usr/bin/env python3
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlsplit


def celery_worker_is_running() -> bool:
    for process in Path("/proc").iterdir():
        if not process.name.isdigit():
            continue
        try:
            command = (process / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", errors="replace"
            )
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        normalized = command.lower()
        if "celery" in normalized and "worker" in normalized:
            return True
    return False


def endpoint_is_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


def broker_endpoint() -> tuple[str, int]:
    broker_url = os.environ.get("AIRFLOW__CELERY__BROKER_URL", "")
    parsed = urlsplit(broker_url)
    if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
        raise ValueError("unsupported or missing Celery broker URL")
    return parsed.hostname, parsed.port or 6379


def main() -> int:
    try:
        broker_host, broker_port = broker_endpoint()
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    checks = (
        (celery_worker_is_running(), "Celery worker process is not running"),
        (
            endpoint_is_reachable(broker_host, broker_port),
            "Celery broker is not reachable",
        ),
        (
            endpoint_is_reachable("127.0.0.1", 8793),
            "Airflow worker log server is not reachable",
        ),
    )
    failures = [message for healthy, message in checks if not healthy]
    if failures:
        print("; ".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
