#!/usr/bin/env python3
"""Minimal MyQuant example for reading 1-minute futures bars."""

from __future__ import annotations

import argparse
import sys


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read 1-minute futures bars from MyQuant."
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
        default=20,
        help="Number of 1-minute bars to fetch",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="Optional end time, for example '2026-03-20 15:00:00'",
    )
    parser.add_argument(
        "--fields",
        default="symbol,eob,open,high,low,close,volume",
        help="Comma-separated fields to request",
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

    bars = history_n(
        symbol=args.symbol,
        frequency="60s",
        count=args.count,
        end_time=args.end_time,
        fields=args.fields,
        df=True,
    )

    if bars is None or len(bars) == 0:
        print("No 1-minute futures data returned.")
        return 0

    print(bars.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
