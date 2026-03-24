#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.alltick_variant_double_bottom_core import CHINA_TZ, TradeTick, detect_variant_double_bottom, load_json_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay Eastmoney local tick history with unified strategy")
    parser.add_argument("--source-csv", required=True)
    parser.add_argument("--tick-db", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output-csv", required=True)
    return parser.parse_args()


def load_targets(path: Path) -> dict[str, dict[str, str]]:
    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            trading_day = str(row.get("trading_day") or "").strip()
            symbol = str(row.get("symbol") or "").strip()
            name = str(row.get("name") or symbol).strip()
            if trading_day and symbol:
                grouped[trading_day][symbol] = name
    return grouped


def load_ticks_for_day(conn: sqlite3.Connection, symbol: str, trading_day: str) -> list[TradeTick]:
    start = f"{trading_day} 00:00:00.000"
    end = f"{trading_day} 23:59:59.999"
    rows = conn.execute(
        """
        SELECT symbol, name, seq, tick_time_ms, price, volume, turnover, trade_direction, received_at_ms, raw_json
        FROM trade_ticks
        WHERE symbol = ? AND tick_time_text >= ? AND tick_time_text <= ?
        ORDER BY tick_time_ms ASC, seq ASC
        """,
        (symbol, start, end),
    ).fetchall()
    result: list[TradeTick] = []
    for row in rows:
        result.append(
            TradeTick(
                symbol=str(row[0]),
                name=str(row[1]),
                seq=str(row[2]),
                tick_time_ms=int(row[3]),
                price=float(row[4]),
                volume=float(row[5]),
                turnover=float(row[6]),
                trade_direction=int(row[7]),
                received_at_ms=int(row[8]),
                raw_json=str(row[9]),
            )
        )
    return result


def extract_references(ticks: list[TradeTick], trading_day: str) -> tuple[float | None, float | None]:
    for tick in ticks:
        if tick.dt.strftime("%Y-%m-%d") != trading_day:
            continue
        if tick.dt.hour < 9 or (tick.dt.hour == 9 and tick.dt.minute < 30):
            continue
        try:
            payload = json.loads(tick.raw_json)
        except Exception:
            continue
        open_price = float(payload.get("open_price") or 0) or None
        prev_close = float(payload.get("pre_close") or payload.get("prev_close") or payload.get("preClose") or 0) or None
        if open_price and prev_close:
            return open_price, prev_close
    return None, None


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = [
        "symbol",
        "name",
        "trading_day",
        "confirm_mode",
        "signal_time",
        "signal_price",
        "open_price",
        "prev_close_price",
        "a_threshold_time",
        "a_threshold_price",
        "r1_time",
        "r1_price",
        "r2_time",
        "r2_price",
        "l1_time",
        "l1_price",
        "l2_time",
        "l2_price",
        "a_gain_pct",
    ]
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    source_csv = Path(args.source_csv).expanduser().resolve()
    tick_db = Path(args.tick_db).expanduser().resolve()
    config_path = Path(args.config).expanduser().resolve()
    output_csv = Path(args.output_csv).expanduser().resolve()

    config = load_json_config(config_path)
    targets = load_targets(source_csv)
    conn = sqlite3.connect(tick_db.as_posix(), timeout=10)
    results: list[dict[str, object]] = []
    missing: list[str] = []
    try:
        for trading_day, symbols in sorted(targets.items()):
            for symbol, name in sorted(symbols.items()):
                ticks = load_ticks_for_day(conn, symbol, trading_day)
                if not ticks:
                    missing.append(f"{trading_day} {symbol}: no ticks")
                    continue
                open_price, prev_close = extract_references(ticks, trading_day)
                if not open_price or not prev_close:
                    missing.append(f"{trading_day} {symbol}: missing refs")
                    continue
                signal = detect_variant_double_bottom(ticks, config, open_price, prev_close)
                if signal is None or signal.trading_day.isoformat() != trading_day:
                    continue
                row = signal.to_dict()
                row["a_threshold_time"] = row.get("r1_time", "")
                row["a_threshold_price"] = row.get("r1_price", "")
                try:
                    a_gain_pct = ((float(row["r1_price"]) / float(row["prev_close_price"])) - 1.0) * 100.0
                    row["a_gain_pct"] = round(a_gain_pct, 4)
                except Exception:
                    row["a_gain_pct"] = ""
                row["name"] = name or row.get("name") or symbol
                results.append(row)
    finally:
        conn.close()

    results.sort(key=lambda item: (str(item.get("trading_day") or ""), str(item.get("signal_time") or ""), str(item.get("symbol") or "")))
    write_csv(output_csv, results)
    print(f"output={output_csv}")
    print(f"signals={len(results)}")
    print(f"missing={len(missing)}")
    for line in missing[:20]:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
