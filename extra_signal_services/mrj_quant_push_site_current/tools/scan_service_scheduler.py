#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from guanlan.core.services.calendar import is_trading_day  # noqa: E402


def run_systemctl(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["systemctl", *args],
        check=False,
        capture_output=True,
        text=True,
    )


def service_is_active(service_name: str) -> bool:
    result = run_systemctl("is-active", service_name)
    return result.returncode == 0 and result.stdout.strip() == "active"


def start_service_if_needed(service_name: str) -> int:
    today = date.today()
    if not is_trading_day(today):
        print(f"[skip] {today.isoformat()} is not a trading day")
        return 0

    if service_is_active(service_name):
        print(f"[ok] {service_name} already active")
        return 0

    result = run_systemctl("start", service_name)
    if result.returncode == 0:
        print(f"[ok] started {service_name}")
        return 0

    print(f"[error] failed to start {service_name}: {result.stderr.strip()}")
    return result.returncode


def stop_service_if_needed(service_name: str) -> int:
    if not service_is_active(service_name):
        print(f"[ok] {service_name} already inactive")
        return 0

    result = run_systemctl("stop", service_name)
    if result.returncode == 0:
        print(f"[ok] stopped {service_name}")
        return 0

    print(f"[error] failed to stop {service_name}: {result.stderr.strip()}")
    return result.returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start/stop quant scan service on schedule")
    parser.add_argument("action", choices=["start", "stop"])
    parser.add_argument("--service", default="quant-scan.service")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.action == "start":
        return start_service_if_needed(args.service)
    return stop_service_if_needed(args.service)


if __name__ == "__main__":
    raise SystemExit(main())
