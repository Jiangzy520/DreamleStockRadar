#!/usr/bin/env python3
"""Minimal MyQuant example for reading futures tick data."""

from __future__ import annotations

import argparse
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read futures tick data from MyQuant."
    )
    parser.add_argument("--token", required=True, help="MyQuant token")
    parser.add_argument(
        "--symbol",
        default="SHFE.rb2605",
        help="Futures contract code, for example SHFE.rb2605 or DCE.i2605",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=50,
        help="Number of ticks to fetch. Official single-call max is 33000.",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="Optional end time, for example '2026-03-20 15:00:00'",
    )
    parser.add_argument(
        "--fields",
        default="",
        help="Optional comma-separated fields. Leave empty to request default fields.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        from gm.api import history_n, set_token
    except ImportError:
        print(
            "MyQuant SDK is not installed. Install it first, then rerun this script.",
            file=sys.stderr,
        )
        return 1

    set_token(args.token)

    kwargs = {
        "symbol": args.symbol,
        "frequency": "tick",
        "count": args.count,
        "end_time": args.end_time,
        "df": True,
    }
    if args.fields.strip():
        kwargs["fields"] = args.fields

    ticks = history_n(**kwargs)
    if ticks is None or len(ticks) == 0:
        print("No tick data returned.")
        return 0

    print(ticks.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
