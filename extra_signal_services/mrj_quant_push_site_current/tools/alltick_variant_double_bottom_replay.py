#!/usr/bin/env python3
"""
Replay recorded AllTick ticks with the Variant Double Bottom strategy.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.alltick_variant_double_bottom_core import (  # noqa: E402
    CHINA_TZ,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_WATCHLIST_PATH,
    TickStore,
    china_day_bounds,
    detect_variant_double_bottom,
    load_json_config,
    load_watchlist,
)


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / ".guanlan" / "alltick" / "variant_double_bottom_replay"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay local AllTick ticks for Variant Double Bottom")
    parser.add_argument("--config-file", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--watchlist-file", default=str(DEFAULT_WATCHLIST_PATH))
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_days(start_day: date, end_day: date) -> list[date]:
    result: list[date] = []
    current = start_day
    while current <= end_day:
        result.append(current)
        current += timedelta(days=1)
    return result


def main() -> int:
    args = parse_args()
    start_day = parse_date(args.start_date)
    end_day = parse_date(args.end_date)
    if end_day < start_day:
        raise SystemExit("end-date cannot be earlier than start-date")

    config = load_json_config(Path(args.config_file).expanduser().resolve())
    watch_items = load_watchlist(Path(args.watchlist_file).expanduser().resolve())
    store = TickStore(Path(args.db_path).expanduser().resolve())

    try:
        rows = []
        for trading_day in iter_days(start_day, end_day):
            start_ms, end_ms = china_day_bounds(trading_day)
            for symbol, item in watch_items.items():
                ticks = store.load_ticks(symbol, start_ms=start_ms, end_ms=end_ms)
                if not ticks:
                    print(f"[{trading_day}] {symbol}: no ticks")
                    continue
                signal = detect_variant_double_bottom(ticks, config)
                if signal:
                    rows.append(signal.to_dict())
                    print(
                        f"[{trading_day}] {symbol} {item.name}: "
                        f"{signal.signal_time.strftime('%H:%M:%S.%f')[:-3]} "
                        f"price={signal.signal_price:.3f}"
                    )
                else:
                    print(f"[{trading_day}] {symbol} {item.name}: no signal")

        output_dir = Path(args.output_dir).expanduser().resolve()
        output_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")
        output_path = output_dir / f"variant_double_bottom_replay_{stamp}.csv"
        headers = list(rows[0].keys()) if rows else [
            "symbol",
            "name",
            "trading_day",
            "confirm_mode",
            "signal_time",
            "signal_price",
            "open_price",
            "r1_time",
            "r1_price",
            "r2_time",
            "r2_price",
            "l1_time",
            "l1_price",
            "l2_time",
            "l2_price",
        ]
        with output_path.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)

        print(f"[csv] {output_path}")
        return 0
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
