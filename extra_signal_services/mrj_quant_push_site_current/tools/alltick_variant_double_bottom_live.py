#!/usr/bin/env python3
"""
Live tick recorder and real-time detector for the Variant Double Bottom strategy.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import websocket

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.alltick_variant_double_bottom_core import (  # noqa: E402
    ALLTICK_WS_URL,
    CHINA_TZ,
    DEFAULT_CONFIG_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_ENV_PATH,
    DEFAULT_FILTER_REPORT_PATH,
    DEFAULT_SIGNAL_PATH,
    FinancialFilterResult,
    TickStore,
    append_csv_row,
    china_day_bounds,
    detect_variant_double_bottom,
    evaluate_financial_filters,
    fetch_latest_http_ticks,
    fetch_today_open_price,
    get_alltick_token,
    load_json_config,
    load_watchlist,
    normalize_symbol,
    parse_trade_tick_message,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Live Variant Double Bottom monitor")
    parser.add_argument("--config-file", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--watchlist-file", default="")
    parser.add_argument("--db-path", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--env-file", default=str(DEFAULT_ENV_PATH))
    parser.add_argument("--token", default="")
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_PATH))
    parser.add_argument("--filter-report-path", default=str(DEFAULT_FILTER_REPORT_PATH))
    parser.add_argument("--check-filters-only", action="store_true")
    parser.add_argument("--run-seconds", type=int, default=0, help="0 means run forever")
    parser.add_argument("--seed-http", action="store_true")
    return parser.parse_args()


def print_filter_summary(results: list[FinancialFilterResult]) -> None:
    print("[filters]")
    for row in results:
        status = "PASS" if row.passed else "FAIL"
        print(
            f"  {row.symbol} {row.name} {status}"
            f" eps={row.eps_basic if row.eps_basic is not None else '-'}"
            f" pe={row.pe_ttm if row.pe_ttm is not None else '-'}"
            f" forecast_date={row.forecast_notice_date or '-'}"
            f" forecast_types={row.forecast_types or '-'}"
            f" reasons={row.reasons or '-'}"
        )


def main() -> int:
    args = parse_args()
    config = load_json_config(Path(args.config_file).expanduser().resolve())
    watchlist_path = Path(args.watchlist_file).expanduser().resolve() if args.watchlist_file else Path(str(config["watchlist_file"])).expanduser().resolve()
    watch_items = load_watchlist(watchlist_path)
    filter_results = list(evaluate_financial_filters(watch_items).values())
    print_filter_summary(filter_results)
    from tools.alltick_variant_double_bottom_core import save_filter_report
    save_filter_report(filter_results, Path(args.filter_report_path).expanduser().resolve())

    tradable = {
        result.symbol: watch_items[result.symbol]
        for result in filter_results
        if result.passed
    }
    if args.check_filters_only:
        return 0
    if not tradable:
        print("[exit] no tradable symbols after filters")
        return 0

    token = get_alltick_token(Path(args.env_file).expanduser().resolve(), args.token)
    store = TickStore(Path(args.db_path).expanduser().resolve())
    signal_path = Path(args.signal_path).expanduser().resolve()
    today = datetime.now(CHINA_TZ).date()
    start_ms, end_ms = china_day_bounds(today)
    ticks_by_symbol = {
        symbol: store.load_ticks(symbol, start_ms=start_ms, end_ms=end_ms)
        for symbol in tradable
    }
    open_price_map = {
        symbol: (ticks_by_symbol[symbol][0].price if ticks_by_symbol[symbol] else None)
        for symbol in tradable
    }
    for symbol in tradable:
        if open_price_map[symbol] is None:
            try:
                open_price_map[symbol] = fetch_today_open_price(token, symbol)
            except Exception:
                open_price_map[symbol] = None

    emitted = {
        (tick_list[0].symbol, tick_list[0].dt.date())
        for tick_list in ticks_by_symbol.values()
        if tick_list and detect_variant_double_bottom(tick_list, config, open_price_map.get(tick_list[0].symbol))
    }

    if args.seed_http:
        try:
            seeded = fetch_latest_http_ticks(token, list(tradable.keys()), tradable)
            inserted = store.save_ticks(seeded)
            print(f"[seed] inserted={inserted}")
            for tick in seeded:
                ticks_by_symbol.setdefault(tick.symbol, []).append(tick)
                ticks_by_symbol[tick.symbol].sort(key=lambda item: (item.tick_time_ms, item.seq))
                if open_price_map.get(tick.symbol) is None:
                    open_price_map[tick.symbol] = tick.price
        except Exception as exc:
            print(f"[seed-skip] {exc}")

    last_scan_at: dict[str, float] = defaultdict(float)

    def maybe_emit_signal(symbol: str) -> None:
        scan_interval = max(float(config["scan_interval_seconds"]), 0.2)
        now_ts = time.time()
        if now_ts - last_scan_at[symbol] < scan_interval:
            return
        last_scan_at[symbol] = now_ts

        ticks = ticks_by_symbol.get(symbol) or []
        if not ticks:
            return
        open_price = open_price_map.get(symbol) or ticks[0].price
        signal = detect_variant_double_bottom(ticks, config, open_price)
        if not signal:
            return
        key = (signal.symbol, signal.trading_day)
        if key in emitted:
            return
        emitted.add(key)
        row = signal.to_dict()
        append_csv_row(signal_path, row, row.keys())
        print(
            "[signal]"
            f" {signal.symbol} {signal.name}"
            f" {signal.signal_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
            f" price={signal.signal_price:.3f}"
            f" R1={signal.r1_price:.3f}"
            f" R2={signal.r2_price:.3f}"
            f" L1={signal.l1_price:.3f}"
            f" L2={signal.l2_price:.3f}"
        )

    started_at = time.time()
    reconnect_count = 0
    total_inserted = 0
    heartbeat_seq = 2
    subscribe_payload = {
        "cmd_id": 22004,
        "seq_id": 1,
        "trace": f"variant-double-bottom-sub-{int(started_at)}",
        "data": {"symbol_list": [{"code": normalize_symbol(symbol)} for symbol in sorted(tradable)]},
    }

    try:
        while True:
            if args.run_seconds > 0 and time.time() - started_at >= args.run_seconds:
                print(f"[done] inserted={total_inserted} reconnects={reconnect_count}")
                return 0

            ws = None
            try:
                ws = websocket.create_connection(f"{ALLTICK_WS_URL}?token={token}", timeout=10)
                ws.settimeout(1)
                ws.send(json.dumps(subscribe_payload, ensure_ascii=False, separators=(",", ":")))
                last_ping_at = time.time()
                last_heartbeat_at = time.time()
                print(f"[open] subscribed {len(tradable)} symbols")

                while True:
                    now = time.time()
                    if args.run_seconds > 0 and now - started_at >= args.run_seconds:
                        print(f"[done] inserted={total_inserted} reconnects={reconnect_count}")
                        return 0

                    if now - last_ping_at >= 10:
                        ws.ping()
                        last_ping_at = now

                    if now - last_heartbeat_at >= 15:
                        heartbeat = {
                            "cmd_id": 22000,
                            "seq_id": heartbeat_seq,
                            "trace": f"variant-double-bottom-hb-{heartbeat_seq}",
                            "data": {},
                        }
                        heartbeat_seq += 1
                        ws.send(json.dumps(heartbeat, ensure_ascii=False, separators=(",", ":")))
                        last_heartbeat_at = now

                    try:
                        raw_message = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue

                    message = json.loads(raw_message)
                    cmd_id = int(message.get("cmd_id", 0) or 0)
                    if cmd_id == 22005:
                        print(f"[ack] {message}")
                        continue
                    if cmd_id == 22001:
                        continue

                    ticks = parse_trade_tick_message(message, tradable)
                    if not ticks:
                        continue

                    inserted = store.save_ticks(ticks)
                    total_inserted += inserted
                    for tick in ticks:
                        ticks_by_symbol.setdefault(tick.symbol, []).append(tick)
                        ticks_by_symbol[tick.symbol].sort(key=lambda item: (item.tick_time_ms, item.seq))
                        if open_price_map.get(tick.symbol) is None:
                            open_price_map[tick.symbol] = tick.price
                        maybe_emit_signal(tick.symbol)

                    latest = ticks[-1]
                    print(
                        "[tick]"
                        f" {latest.symbol}"
                        f" {latest.dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                        f" price={latest.price:.3f}"
                        f" volume={latest.volume:.0f}"
                        f" inserted={inserted}"
                        f" total={total_inserted}"
                    )
            except KeyboardInterrupt:
                print(f"[stop] inserted={total_inserted} reconnects={reconnect_count}")
                return 0
            except Exception as exc:
                reconnect_count += 1
                print(f"[reconnect] {exc}")
                time.sleep(min(15, 2 + reconnect_count))
            finally:
                if ws is not None:
                    try:
                        ws.close()
                    except Exception:
                        pass
    finally:
        store.close()


if __name__ == "__main__":
    raise SystemExit(main())
