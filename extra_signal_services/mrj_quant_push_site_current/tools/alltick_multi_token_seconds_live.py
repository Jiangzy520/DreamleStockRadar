#!/usr/bin/env python3
"""
Eastmoney live quote collector with second-bar persistence.

Compatibility goals:
1. Keep the old CLI/config shape so existing launch scripts still work.
2. Continue writing the same SQLite / CSV outputs consumed by the local web UI.
3. Leave the downstream pattern logic unchanged by only replacing the quote source.

Notes:
1. Eastmoney public quotes are polled in batches rather than streamed via websocket.
2. The collector converts quote snapshots into synthetic ticks. This keeps the
   existing second-bar and signal pipeline working without changing strategy code.
"""

from __future__ import annotations

import argparse
import csv
import json
import queue
import sqlite3
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Sequence

import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.alltick_variant_double_bottom_core import (  # noqa: E402
    CHINA_TZ,
    DEFAULT_CONFIG as CORE_DEFAULT_CONFIG,
    StrategySignal,
    TickStore,
    TimeBar,
    TradeTick,
    WatchItem,
    aggregate_ticks,
    append_csv_row,
    detect_variant_double_bottom,
    eastmoney_secid,
    make_session,
    normalize_symbol,
)


DEFAULT_MANAGER_DIR = PROJECT_ROOT / ".guanlan" / "alltick_manager"
DEFAULT_API_FILE = DEFAULT_MANAGER_DIR / "apis.txt"
DEFAULT_WATCHLIST_FILE = DEFAULT_MANAGER_DIR / "watchlist.csv"
DEFAULT_ASSIGNMENT_FILE = DEFAULT_MANAGER_DIR / "stock_assignments.csv"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "tools" / "alltick_multi_token_seconds.json"
DEFAULT_TICK_DB_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "multi_token_ticks.sqlite3"
DEFAULT_BAR_DB_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "multi_token_seconds.sqlite3"
DEFAULT_SIGNAL_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "multi_token_variant_double_bottom_signals.csv"

EASTMONEY_BATCH_QUOTE_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
EASTMONEY_BATCH_FIELDS = "f12,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18,f124"
EASTMONEY_UT = "fa5fd1943c7b386f172d6893dbfba10b"

DEFAULT_CONFIG = {
    **CORE_DEFAULT_CONFIG,
    "distribution_mode": "assignment",
    "max_symbols_per_api": 120,
    "persist_intervals": [1, 5],
    "assignment_file": str(DEFAULT_ASSIGNMENT_FILE),
    "api_file": str(DEFAULT_API_FILE),
    "watchlist_file": str(DEFAULT_WATCHLIST_FILE),
    "log_each_tick": False,
    "quote_timeout_seconds": 10,
    "quote_provider": "eastmoney",
}


@dataclass(frozen=True)
class QuoteBatch:
    label: str
    watch_items: dict[str, WatchItem]
    secids: tuple[str, ...]


@dataclass
class QuoteState:
    price: float = 0.0
    cum_volume_shares: float = 0.0
    cum_turnover: float = 0.0
    last_tick_ms: int = 0
    seq: int = 0


@dataclass(frozen=True)
class WorkerEvent:
    event_type: str
    worker_name: str
    token_suffix: str
    ticks: tuple[TradeTick, ...] = ()
    message: str = ""


class SecondBarStore:
    """SQLite store for locally aggregated second bars."""

    def __init__(self, path: Path = DEFAULT_BAR_DB_PATH) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS second_bars (
                symbol TEXT NOT NULL,
                code TEXT NOT NULL,
                interval_seconds INTEGER NOT NULL,
                bucket_start_ms INTEGER NOT NULL,
                bucket_start_text TEXT NOT NULL,
                bucket_end_ms INTEGER NOT NULL,
                bucket_end_text TEXT NOT NULL,
                open_price REAL NOT NULL,
                high_price REAL NOT NULL,
                low_price REAL NOT NULL,
                close_price REAL NOT NULL,
                volume REAL NOT NULL,
                turnover REAL NOT NULL,
                high_time_text TEXT NOT NULL,
                close_time_text TEXT NOT NULL,
                source_tick_count INTEGER NOT NULL,
                updated_at_text TEXT NOT NULL,
                PRIMARY KEY(symbol, interval_seconds, bucket_start_ms)
            );
            CREATE INDEX IF NOT EXISTS idx_second_bars_symbol_interval_time
            ON second_bars(symbol, interval_seconds, bucket_start_ms);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def save_bars(self, symbol: str, interval_seconds: int, bars: Sequence[TimeBar]) -> int:
        if not bars:
            return 0
        normalized = normalize_symbol(symbol)
        code = normalized.split(".", 1)[0]
        updated_at_text = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        rows = [
            (
                normalized,
                code,
                int(interval_seconds),
                int(bar.start_dt.timestamp() * 1000),
                bar.start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                int(bar.end_dt.timestamp() * 1000),
                bar.end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                float(bar.open_price),
                float(bar.high_price),
                float(bar.low_price),
                float(bar.close_price),
                float(bar.volume),
                float(bar.turnover),
                bar.high_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                bar.close_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                0,
                updated_at_text,
            )
            for bar in bars
        ]
        before = self.conn.total_changes
        self.conn.executemany(
            """
            INSERT OR REPLACE INTO second_bars (
                symbol, code, interval_seconds, bucket_start_ms,
                bucket_start_text, bucket_end_ms, bucket_end_text,
                open_price, high_price, low_price, close_price,
                volume, turnover, high_time_text, close_time_text,
                source_tick_count, updated_at_text
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return self.conn.total_changes - before


def resolve_project_path(raw: str | Path, fallback: Path) -> Path:
    text = str(raw or "").strip()
    if not text:
        return fallback.expanduser().resolve()
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def load_config(path: Path) -> dict:
    config = DEFAULT_CONFIG.copy()
    if not path.exists():
        return config
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return config
    if isinstance(loaded, dict):
        config.update(loaded)
    return config


def load_watch_items_csv(path: Path) -> dict[str, WatchItem]:
    if not path.exists():
        raise FileNotFoundError(f"Watchlist file not found: {path}")
    items: dict[str, WatchItem] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = str(row.get("symbol") or row.get("code") or "").strip()
            if not raw:
                continue
            try:
                symbol = normalize_symbol(raw)
            except Exception:
                continue
            name = str(row.get("name") or row.get("股票名称") or symbol).strip() or symbol
            items[symbol] = WatchItem(symbol=symbol, name=name)
    return items


def load_watch_items_from_assignment(path: Path) -> dict[str, WatchItem]:
    if not path.exists():
        return {}
    items: dict[str, WatchItem] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = str(row.get("symbol") or row.get("code") or "").strip()
            if not raw:
                continue
            try:
                symbol = normalize_symbol(raw)
            except Exception:
                continue
            name = str(row.get("name") or symbol).strip() or symbol
            items[symbol] = WatchItem(symbol=symbol, name=name)
    return items


def load_emitted_keys(path: Path) -> set[tuple[str, str]]:
    if not path.exists():
        return set()
    result: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            symbol = str(row.get("symbol") or "").strip()
            trading_day = str(row.get("trading_day") or "").strip()
            if symbol and trading_day:
                result.add((symbol, trading_day))
    return result


def trim_watch_items(items: dict[str, WatchItem], symbol_limit: int) -> dict[str, WatchItem]:
    if symbol_limit <= 0 or len(items) <= symbol_limit:
        return items
    return dict(list(items.items())[:symbol_limit])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Eastmoney live quote polling / second-bar signal scanner")
    parser.add_argument("--config-file", default=str(DEFAULT_CONFIG_PATH))
    parser.add_argument("--distribution-mode", choices=["balanced", "assignment"], default="")
    parser.add_argument("--assignment-file", default="")
    parser.add_argument("--api-file", default="")
    parser.add_argument("--watchlist-file", default="")
    parser.add_argument("--max-symbols-per-api", type=int, default=0)
    parser.add_argument("--max-active-apis", type=int, default=0)
    parser.add_argument("--symbol-limit", type=int, default=0)
    parser.add_argument("--tick-db-path", default=str(DEFAULT_TICK_DB_PATH))
    parser.add_argument("--bar-db-path", default=str(DEFAULT_BAR_DB_PATH))
    parser.add_argument("--signal-path", default=str(DEFAULT_SIGNAL_PATH))
    parser.add_argument("--run-seconds", type=int, default=0, help="0 means keep running")
    parser.add_argument("--seed-http", action="store_true", help="kept for compatibility; no extra action is needed")
    return parser.parse_args()


def build_watch_items(config: dict, args: argparse.Namespace) -> dict[str, WatchItem]:
    distribution_mode = args.distribution_mode or str(config.get("distribution_mode") or "assignment")
    assignment_path = resolve_project_path(
        args.assignment_file or str(config.get("assignment_file") or DEFAULT_ASSIGNMENT_FILE),
        DEFAULT_ASSIGNMENT_FILE,
    )
    watchlist_path = resolve_project_path(
        args.watchlist_file or str(config.get("watchlist_file") or DEFAULT_WATCHLIST_FILE),
        DEFAULT_WATCHLIST_FILE,
    )

    if distribution_mode == "assignment":
        items = load_watch_items_from_assignment(assignment_path)
        if not items:
            items = load_watch_items_csv(watchlist_path)
    else:
        items = load_watch_items_csv(watchlist_path)

    items = trim_watch_items(items, args.symbol_limit)
    if not items:
        raise RuntimeError("No watchlist symbols available")
    return items


def build_quote_batches(
    watch_items: dict[str, WatchItem],
    batch_size: int,
    max_active_batches: int = 0,
) -> list[QuoteBatch]:
    size = max(int(batch_size or 0), 1)
    ordered_items = list(watch_items.values())
    batches: list[QuoteBatch] = []
    for start in range(0, len(ordered_items), size):
        chunk = ordered_items[start:start + size]
        batch_items = {item.symbol: item for item in chunk}
        batches.append(
            QuoteBatch(
                label=f"batch{len(batches) + 1:03d}",
                watch_items=batch_items,
                secids=tuple(eastmoney_secid(item.symbol) for item in chunk),
            )
        )
    if max_active_batches > 0:
        return batches[:max_active_batches]
    return batches


def queue_event(event_queue: queue.Queue[WorkerEvent], event: WorkerEvent) -> None:
    while True:
        try:
            event_queue.put(event, timeout=1)
            return
        except queue.Full:
            continue


def infer_trade_direction(previous_price: float, current_price: float) -> int:
    if previous_price <= 0:
        return 0
    if current_price > previous_price:
        return 1
    if current_price < previous_price:
        return -1
    return 0


def fetch_batch_quotes(
    session: requests.Session,
    batch: QuoteBatch,
    timeout_seconds: float,
) -> list[dict]:
    response = session.get(
        EASTMONEY_BATCH_QUOTE_URL,
        params={
            "secids": ",".join(batch.secids),
            "fields": EASTMONEY_BATCH_FIELDS,
            "ut": EASTMONEY_UT,
            "invt": "2",
            "fltt": "2",
            "np": "1",
        },
        timeout=timeout_seconds,
    )
    response.raise_for_status()
    payload = response.json()
    if int(payload.get("rc", 0) or 0) != 0:
        raise RuntimeError(f"Eastmoney rc={payload.get('rc')} payload={payload}")
    return list(((payload.get("data") or {}).get("diff") or []))


def build_synthetic_tick(
    item: WatchItem,
    raw_quote: dict,
    state: QuoteState,
) -> TradeTick | None:
    price = float(raw_quote.get("f2") or 0)
    if price <= 0:
        return None

    timestamp_seconds = int(float(raw_quote.get("f124") or 0) or 0)
    tick_time_ms = timestamp_seconds * 1000 if timestamp_seconds > 0 else int(time.time() * 1000)
    open_price = float(raw_quote.get("f17") or 0)
    pre_close = float(raw_quote.get("f18") or 0)

    cum_volume_shares = float(raw_quote.get("f5") or 0) * 100.0
    cum_turnover = float(raw_quote.get("f6") or 0)

    first_seen = state.seq == 0
    volume_reset = cum_volume_shares < state.cum_volume_shares
    turnover_reset = cum_turnover < state.cum_turnover

    if first_seen or volume_reset:
        delta_volume = 0.0
    else:
        delta_volume = max(cum_volume_shares - state.cum_volume_shares, 0.0)

    if first_seen or turnover_reset:
        delta_turnover = 0.0
    else:
        delta_turnover = max(cum_turnover - state.cum_turnover, 0.0)

    changed = first_seen or price != state.price or delta_volume > 0 or delta_turnover > 0
    state.price = price
    state.cum_volume_shares = max(cum_volume_shares, 0.0)
    state.cum_turnover = max(cum_turnover, 0.0)

    if not changed:
        return None

    state.seq += 1
    state.last_tick_ms = tick_time_ms
    return TradeTick(
        symbol=item.symbol,
        name=item.name,
        seq=f"em-{tick_time_ms}-{state.seq}",
        tick_time_ms=tick_time_ms,
        price=price,
        volume=delta_volume,
        turnover=delta_turnover,
        trade_direction=0,
        received_at_ms=int(time.time() * 1000),
        raw_json=json.dumps(
            {
                "provider": "eastmoney",
                "symbol": item.symbol,
                "name": item.name,
                "open_price": open_price,
                "pre_close": pre_close,
                "cum_volume_shares": cum_volume_shares,
                "cum_turnover": cum_turnover,
                "snapshot": raw_quote,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )


def worker_loop(
    worker_name: str,
    batch: QuoteBatch,
    event_queue: queue.Queue[WorkerEvent],
    stop_event: threading.Event,
    run_seconds: int,
    started_at: float,
    poll_interval_seconds: float,
    timeout_seconds: float,
) -> None:
    session = make_session()
    states: dict[str, QuoteState] = {}
    watch_items_by_code = {
        item.symbol.split(".", 1)[0]: item
        for item in batch.watch_items.values()
    }
    suffix = worker_name[-3:]
    round_count = 0

    try:
        queue_event(
            event_queue,
            WorkerEvent(
                event_type="status",
                worker_name=worker_name,
                token_suffix=suffix,
                message=f"[open] symbols={len(batch.watch_items)} provider=eastmoney",
            ),
        )
        while not stop_event.is_set():
            if run_seconds > 0 and time.time() - started_at >= run_seconds:
                break

            round_count += 1
            round_started_at = time.time()
            emitted_ticks: list[TradeTick] = []

            try:
                rows = fetch_batch_quotes(session, batch, timeout_seconds)
                for row in rows:
                    code = str(row.get("f12") or "").strip().zfill(6)
                    item = watch_items_by_code.get(code)
                    if item is None:
                        continue
                    state = states.setdefault(item.symbol, QuoteState())
                    previous_price = state.price
                    tick = build_synthetic_tick(item, row, state)
                    if tick is None:
                        continue
                    if previous_price > 0:
                        trade_direction = infer_trade_direction(previous_price, tick.price)
                        tick = TradeTick(
                            symbol=tick.symbol,
                            name=tick.name,
                            seq=tick.seq,
                            tick_time_ms=tick.tick_time_ms,
                            price=tick.price,
                            volume=tick.volume,
                            turnover=tick.turnover,
                            trade_direction=trade_direction,
                            received_at_ms=tick.received_at_ms,
                            raw_json=tick.raw_json,
                        )
                    emitted_ticks.append(tick)

                if emitted_ticks:
                    queue_event(
                        event_queue,
                        WorkerEvent(
                            event_type="ticks",
                            worker_name=worker_name,
                            token_suffix=suffix,
                            ticks=tuple(emitted_ticks),
                        ),
                    )

                if round_count == 1 or round_count % 30 == 0:
                    queue_event(
                        event_queue,
                        WorkerEvent(
                            event_type="status",
                            worker_name=worker_name,
                            token_suffix=suffix,
                            message=(
                                f"[poll] round={round_count}"
                                f" symbols={len(batch.watch_items)}"
                                f" returned={len(rows)}"
                                f" emitted={len(emitted_ticks)}"
                            ),
                        ),
                    )
            except Exception as exc:
                queue_event(
                    event_queue,
                    WorkerEvent(
                        event_type="status",
                        worker_name=worker_name,
                        token_suffix=suffix,
                        message=f"[poll-error] {exc}",
                    ),
                )
                if stop_event.wait(min(max(poll_interval_seconds, 1.0), 5.0)):
                    break
                continue

            elapsed = time.time() - round_started_at
            sleep_seconds = max(poll_interval_seconds - elapsed, 0.05)
            if stop_event.wait(sleep_seconds):
                break
    finally:
        session.close()
        queue_event(
            event_queue,
            WorkerEvent(
                event_type="done",
                worker_name=worker_name,
                token_suffix=suffix,
                message="[done]",
            ),
        )


def extract_open_price_from_tick(tick: TradeTick) -> float | None:
    try:
        payload = json.loads(tick.raw_json)
    except Exception:
        return None
    value = float(payload.get("open_price") or 0)
    return value if value > 0 else None


def extract_prev_close_from_tick(tick: TradeTick) -> float | None:
    try:
        payload = json.loads(tick.raw_json)
    except Exception:
        return None
    value = float(
        payload.get("pre_close")
        or payload.get("prev_close")
        or payload.get("preClose")
        or 0
    )
    return value if value > 0 else None


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config_file).expanduser().resolve())
    watch_items = build_watch_items(config, args)

    batch_size = args.max_symbols_per_api or int(config.get("max_symbols_per_api") or 120)
    quote_batches = build_quote_batches(watch_items, batch_size=batch_size, max_active_batches=args.max_active_apis)
    if not quote_batches:
        raise RuntimeError("No Eastmoney quote batches available")

    tick_db_path = Path(args.tick_db_path).expanduser().resolve()
    bar_db_path = Path(args.bar_db_path).expanduser().resolve()
    signal_path = Path(args.signal_path).expanduser().resolve()

    persist_intervals = [
        max(int(value), 1)
        for value in (config.get("persist_intervals") or [1, 5])
        if str(value).strip()
    ]
    persist_intervals = sorted(set(persist_intervals))
    poll_interval_seconds = max(float(config.get("scan_interval_seconds") or 1), 0.2)
    timeout_seconds = max(float(config.get("quote_timeout_seconds") or 10), 1.0)

    print(
        "[start]"
        " provider=eastmoney"
        f" batches={len(quote_batches)}"
        f" symbols={len(watch_items)}"
        f" batch_size={batch_size}"
        f" interval={poll_interval_seconds}"
        f" tick_db={tick_db_path}"
        f" bar_db={bar_db_path}"
        f" signal_path={signal_path}"
    )

    tick_store = TickStore(tick_db_path)
    bar_store = SecondBarStore(bar_db_path)
    event_queue: queue.Queue[WorkerEvent] = queue.Queue(maxsize=20000)
    stop_event = threading.Event()
    today = datetime.now(CHINA_TZ).date()

    ticks_by_symbol: dict[str, list[TradeTick]] = {}
    tick_keys_by_symbol: dict[str, set[tuple[int, str]]] = {}
    open_price_map: dict[str, float] = {}
    prev_close_map: dict[str, float] = {}
    last_scan_at: dict[str, float] = defaultdict(float)
    last_saved_bucket_ms: dict[tuple[str, int], int] = {}
    name_map = {symbol: item.name for symbol, item in watch_items.items()}
    emitted = load_emitted_keys(signal_path)

    stats = {
        "inserted_ticks": 0,
        "saved_bars": 0,
        "signals": 0,
        "events": 0,
    }

    day_start_ms = int(datetime.combine(today, datetime.min.time(), tzinfo=CHINA_TZ).timestamp() * 1000)
    day_end_ms = day_start_ms + 24 * 60 * 60 * 1000

    def ensure_symbol_loaded(symbol: str) -> None:
        if symbol in ticks_by_symbol:
            return
        history = tick_store.load_ticks(symbol, start_ms=day_start_ms, end_ms=day_end_ms)
        ticks_by_symbol[symbol] = history
        tick_keys_by_symbol[symbol] = {(tick.tick_time_ms, tick.seq) for tick in history}
        if history:
            open_price = extract_open_price_from_tick(history[-1])
            if open_price and open_price > 0:
                open_price_map[symbol] = open_price
            else:
                open_price_map[symbol] = history[0].price
            for tick in reversed(history):
                prev_close = extract_prev_close_from_tick(tick)
                if prev_close and prev_close > 0:
                    prev_close_map[symbol] = prev_close
                    break

    def persist_bars_for_symbol(symbol: str) -> None:
        ticks = ticks_by_symbol.get(symbol) or []
        if not ticks:
            return
        for interval in persist_intervals:
            bars = aggregate_ticks(ticks, interval)
            if len(bars) > 1:
                bars = bars[:-1]
            if not bars:
                continue
            key = (symbol, interval)
            if key in last_saved_bucket_ms:
                lookback_start = last_saved_bucket_ms[key] - interval * 1000
                subset = [
                    bar
                    for bar in bars
                    if int(bar.start_dt.timestamp() * 1000) >= lookback_start
                ]
            else:
                subset = bars
            stats["saved_bars"] += bar_store.save_bars(symbol, interval, subset)
            last_saved_bucket_ms[key] = int(bars[-1].start_dt.timestamp() * 1000)

    def ensure_open_price(symbol: str) -> float | None:
        cached = open_price_map.get(symbol)
        if cached and cached > 0:
            return cached
        ticks = ticks_by_symbol.get(symbol) or []
        for tick in reversed(ticks):
            open_price = extract_open_price_from_tick(tick)
            if open_price and open_price > 0:
                open_price_map[symbol] = open_price
                return open_price
        if ticks:
            open_price_map[symbol] = ticks[0].price
            return ticks[0].price
        return None

    def ensure_prev_close(symbol: str) -> float | None:
        cached = prev_close_map.get(symbol)
        if cached and cached > 0:
            return cached
        ticks = ticks_by_symbol.get(symbol) or []
        for tick in reversed(ticks):
            prev_close = extract_prev_close_from_tick(tick)
            if prev_close and prev_close > 0:
                prev_close_map[symbol] = prev_close
                return prev_close
        return None

    def maybe_emit_signal(symbol: str) -> None:
        ticks = ticks_by_symbol.get(symbol) or []
        if not ticks:
            return
        scan_interval = max(float(config.get("scan_interval_seconds") or 1), 0.2)
        now_ts = time.time()
        if now_ts - last_scan_at[symbol] < scan_interval:
            return
        last_scan_at[symbol] = now_ts
        open_price = ensure_open_price(symbol)
        if not open_price:
            return
        prev_close_price = ensure_prev_close(symbol) or open_price
        signal = detect_variant_double_bottom(ticks, config, open_price, prev_close_price)
        if not signal:
            return
        key = (signal.symbol, signal.trading_day.isoformat())
        if key in emitted:
            return
        emitted.add(key)
        row = signal.to_dict()
        append_csv_row(signal_path, row, row.keys())
        stats["signals"] += 1
        print(
            "[signal]"
            f" {signal.symbol} {name_map.get(signal.symbol, signal.name)}"
            f" {signal.signal_time.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
            f" price={signal.signal_price:.3f}"
            f" R1={signal.r1_time.strftime('%H:%M:%S')}/{signal.r1_price:.3f}"
            f" R2={signal.r2_time.strftime('%H:%M:%S')}/{signal.r2_price:.3f}"
            f" L1={signal.l1_time.strftime('%H:%M:%S')}/{signal.l1_price:.3f}"
            f" L2={signal.l2_time.strftime('%H:%M:%S')}/{signal.l2_price:.3f}"
        )

    threads: list[threading.Thread] = []
    started_at = time.time()
    try:
        for batch in quote_batches:
            thread = threading.Thread(
                target=worker_loop,
                args=(
                    batch.label,
                    batch,
                    event_queue,
                    stop_event,
                    args.run_seconds,
                    started_at,
                    poll_interval_seconds,
                    timeout_seconds,
                ),
                daemon=True,
            )
            thread.start()
            threads.append(thread)

        active_workers = len(threads)
        while active_workers > 0:
            try:
                event = event_queue.get(timeout=1)
            except queue.Empty:
                if args.run_seconds > 0 and time.time() - started_at >= args.run_seconds:
                    break
                continue

            stats["events"] += 1
            if event.event_type == "done":
                active_workers -= 1
                print(f"[worker] {event.worker_name} tag=*{event.token_suffix} {event.message}")
                continue

            if event.event_type == "status":
                print(f"[worker] {event.worker_name} tag=*{event.token_suffix} {event.message}")
                continue

            if event.event_type != "ticks":
                continue

            changed_symbols: set[str] = set()
            inserted_batch: list[TradeTick] = []
            for tick in event.ticks:
                if tick.dt.date() != today:
                    continue
                symbol = tick.symbol
                ensure_symbol_loaded(symbol)
                tick_key = (tick.tick_time_ms, tick.seq)
                keys = tick_keys_by_symbol.setdefault(symbol, set())
                if tick_key in keys:
                    continue
                keys.add(tick_key)
                ticks_by_symbol.setdefault(symbol, []).append(tick)
                inserted_batch.append(tick)
                changed_symbols.add(symbol)
                open_price = extract_open_price_from_tick(tick)
                if open_price and open_price > 0:
                    open_price_map[symbol] = open_price
                prev_close = extract_prev_close_from_tick(tick)
                if prev_close and prev_close > 0:
                    prev_close_map[symbol] = prev_close

            if inserted_batch:
                inserted = tick_store.save_ticks(inserted_batch)
                stats["inserted_ticks"] += inserted
                if bool(config.get("log_each_tick")):
                    latest = inserted_batch[-1]
                    print(
                        "[tick]"
                        f" worker={event.worker_name}"
                        f" tag=*{event.token_suffix}"
                        f" {latest.symbol}"
                        f" {latest.dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}"
                        f" price={latest.price:.3f}"
                        f" batch={len(inserted_batch)}"
                        f" inserted={inserted}"
                        f" total={stats['inserted_ticks']}"
                    )

            for symbol in changed_symbols:
                ticks_by_symbol[symbol].sort(key=lambda item: (item.tick_time_ms, item.seq))
                persist_bars_for_symbol(symbol)
                maybe_emit_signal(symbol)
    except KeyboardInterrupt:
        print("[stop] keyboard interrupt")
    finally:
        stop_event.set()
        for thread in threads:
            thread.join(timeout=3)
        tick_store.close()
        bar_store.close()

    print(
        "[done]"
        " provider=eastmoney"
        f" batches={len(quote_batches)}"
        f" symbols={len(watch_items)}"
        f" inserted_ticks={stats['inserted_ticks']}"
        f" saved_bars={stats['saved_bars']}"
        f" signals={stats['signals']}"
        f" events={stats['events']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
