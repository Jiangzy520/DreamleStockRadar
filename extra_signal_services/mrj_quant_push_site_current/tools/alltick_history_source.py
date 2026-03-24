#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable
from urllib.parse import quote

from strategies.script.stock_intraday_pattern_watch import MinuteBar, floor_minute
from tools.alltick_variant_double_bottom_core import (
    ALLTICK_HTTP_KLINE_URL,
    CHINA_TZ,
    infer_suffix,
    make_session,
    normalize_symbol,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANAGER_API_PATH = PROJECT_ROOT / ".guanlan" / "alltick_manager" / "apis.txt"
DEFAULT_MANAGER_WATCHLIST_PATH = PROJECT_ROOT / ".guanlan" / "alltick_manager" / "watchlist.csv"
DEFAULT_CACHE_DB_PATH = PROJECT_ROOT / ".guanlan" / "alltick_manager" / "alltick_kline_cache.sqlite3"
MINUTE_KLINE_TYPE = 1
DAILY_KLINE_TYPE = 8
MAX_QUERY_KLINE_NUM = 500
DEFAULT_PER_TOKEN_INTERVAL_SECONDS = 6.0


@dataclass(frozen=True)
class CacheWarmResult:
    total_tokens: int
    active_tokens: int
    fetched_symbols: int
    cache_hit_symbols: int
    fetch_errors: dict[str, str]


def normalize_symbol_code(raw: str) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        raise ValueError("Empty symbol")
    if "." in text:
        code = text.split(".", 1)[0].strip()
        if len(code) == 6 and code.isdigit():
            return code
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 6:
        return digits
    raise ValueError(f"Unsupported symbol: {raw}")


def load_tokens(path: Path = DEFAULT_MANAGER_API_PATH) -> list[str]:
    if not path.exists():
        return []
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        token = raw.strip()
        if not token or token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def load_watchlist_name_map(path: Path = DEFAULT_MANAGER_WATCHLIST_PATH) -> dict[str, str]:
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = row.get("symbol") or row.get("code") or ""
            name = str(row.get("name") or "").strip()
            try:
                code = normalize_symbol_code(raw)
            except ValueError:
                continue
            result[code] = name or code
    return result


def load_symbols_from_watchlist(path: Path = DEFAULT_MANAGER_WATCHLIST_PATH) -> list[str]:
    name_map = load_watchlist_name_map(path)
    return list(name_map.keys())


def balanced_token_buckets(symbols: list[str], tokens: list[str]) -> list[tuple[str, list[str]]]:
    if not symbols or not tokens:
        return []
    active_count = min(len(symbols), len(tokens))
    active_tokens = tokens[:active_count]
    buckets = [[] for _ in range(active_count)]
    for idx, symbol in enumerate(symbols):
        buckets[idx % active_count].append(symbol)
    return list(zip(active_tokens, buckets))


class AllTickKlineCache:
    def __init__(self, path: Path = DEFAULT_CACHE_DB_PATH) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS alltick_kline_cache (
                    symbol TEXT NOT NULL,
                    code TEXT NOT NULL,
                    kline_type INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    trading_day TEXT NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    volume REAL NOT NULL,
                    turnover REAL NOT NULL,
                    fetched_at TEXT NOT NULL,
                    PRIMARY KEY(symbol, kline_type, timestamp)
                );
                CREATE INDEX IF NOT EXISTS idx_alltick_cache_symbol_type_day
                ON alltick_kline_cache(symbol, kline_type, trading_day);
                """
            )
            self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def _row_to_day(self, timestamp: int) -> str:
        return datetime.fromtimestamp(timestamp, tz=CHINA_TZ).date().isoformat()

    def save_rows(self, symbol: str, kline_type: int, rows: list[dict]) -> int:
        normalized = normalize_symbol(symbol)
        code = normalized.split(".", 1)[0]
        now_text = datetime.now(CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S")
        payload: list[tuple] = []
        for row in rows:
            timestamp = int(row.get("timestamp") or 0)
            if timestamp <= 0:
                continue
            payload.append(
                (
                    normalized,
                    code,
                    int(kline_type),
                    timestamp,
                    self._row_to_day(timestamp),
                    float(row.get("open_price") or 0),
                    float(row.get("high_price") or 0),
                    float(row.get("low_price") or 0),
                    float(row.get("close_price") or 0),
                    float(row.get("volume") or 0),
                    float(row.get("turnover") or 0),
                    now_text,
                )
            )
        if not payload:
            return 0
        with self._lock:
            before = self.conn.total_changes
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO alltick_kline_cache (
                    symbol, code, kline_type, timestamp, trading_day,
                    open_price, high_price, low_price, close_price,
                    volume, turnover, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            self.conn.commit()
            return self.conn.total_changes - before

    def date_span(self, symbol: str, kline_type: int) -> tuple[date | None, date | None]:
        normalized = normalize_symbol(symbol)
        with self._lock:
            row = self.conn.execute(
                """
                SELECT MIN(trading_day) AS min_day, MAX(trading_day) AS max_day
                FROM alltick_kline_cache
                WHERE symbol = ? AND kline_type = ?
                """,
                (normalized, int(kline_type)),
            ).fetchone()
        if not row or not row["min_day"] or not row["max_day"]:
            return None, None
        return (
            datetime.strptime(str(row["min_day"]), "%Y-%m-%d").date(),
            datetime.strptime(str(row["max_day"]), "%Y-%m-%d").date(),
        )

    def load_daily_rows(self, symbol: str, start_date: date, end_date: date) -> list[sqlite3.Row]:
        normalized = normalize_symbol(symbol)
        query_start = (start_date - timedelta(days=20)).isoformat()
        query_end = end_date.isoformat()
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT *
                FROM alltick_kline_cache
                WHERE symbol = ? AND kline_type = ? AND trading_day BETWEEN ? AND ?
                ORDER BY timestamp ASC
                """,
                (normalized, DAILY_KLINE_TYPE, query_start, query_end),
            ).fetchall()
        return list(rows)

    def load_trading_days(self, symbol: str, start_date: date, end_date: date) -> list[date]:
        normalized = normalize_symbol(symbol)
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT DISTINCT trading_day
                FROM alltick_kline_cache
                WHERE symbol = ? AND kline_type = ? AND trading_day BETWEEN ? AND ?
                ORDER BY trading_day ASC
                """,
                (normalized, DAILY_KLINE_TYPE, start_date.isoformat(), end_date.isoformat()),
            ).fetchall()
        return [
            datetime.strptime(str(row["trading_day"]), "%Y-%m-%d").date()
            for row in rows
        ]

    def load_intraday_bars(self, symbol: str, trading_day: date) -> list[MinuteBar]:
        normalized = normalize_symbol(symbol)
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT timestamp, open_price, high_price, low_price, close_price, volume, turnover
                FROM alltick_kline_cache
                WHERE symbol = ? AND kline_type = ? AND trading_day = ?
                ORDER BY timestamp ASC
                """,
                (normalized, MINUTE_KLINE_TYPE, trading_day.isoformat()),
            ).fetchall()
        bars: list[MinuteBar] = []
        for row in rows:
            dt = floor_minute(
                datetime.fromtimestamp(int(row["timestamp"]), tz=CHINA_TZ).replace(tzinfo=None)
            )
            close_price = float(row["close_price"] or 0)
            bars.append(
                MinuteBar(
                    dt=dt,
                    open=float(row["open_price"] or 0) or close_price,
                    high=float(row["high_price"] or 0) or close_price,
                    low=float(row["low_price"] or 0) or close_price,
                    close=close_price,
                    volume=float(row["volume"] or 0),
                    amount=float(row["turnover"] or 0),
                    avg_price=0.0,
                )
            )
        return bars


class AllTickHistorySource:
    def __init__(
        self,
        api_file: Path = DEFAULT_MANAGER_API_PATH,
        cache_db_path: Path = DEFAULT_CACHE_DB_PATH,
        request_interval_seconds: float = DEFAULT_PER_TOKEN_INTERVAL_SECONDS,
        max_concurrent_tokens: int = 0,
        progress_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.api_file = Path(api_file).expanduser().resolve()
        self.cache = AllTickKlineCache(Path(cache_db_path).expanduser().resolve())
        self.request_interval_seconds = max(float(request_interval_seconds), 0.0)
        self.max_concurrent_tokens = max(int(max_concurrent_tokens), 0)
        self.progress_callback = progress_callback
        self.tokens = load_tokens(self.api_file)

    def log(self, message: str) -> None:
        if self.progress_callback:
            self.progress_callback(message)

    def close(self) -> None:
        self.cache.close()

    def _is_minute_cache_covered(self, symbol: str, start_date: date, end_date: date) -> bool:
        min_day, max_day = self.cache.date_span(symbol, MINUTE_KLINE_TYPE)
        if min_day is None or max_day is None:
            return False
        return min_day <= start_date and max_day >= end_date

    def _is_daily_cache_covered(self, symbol: str, start_date: date, end_date: date) -> bool:
        min_day, max_day = self.cache.date_span(symbol, DAILY_KLINE_TYPE)
        if min_day is None or max_day is None:
            return False
        return min_day <= start_date - timedelta(days=10) and max_day >= end_date

    def _fetch_kline_rows(self, token: str, symbol: str, kline_type: int, query_kline_num: int) -> list[dict]:
        query = {
            "trace": f"replay-{symbol}-{kline_type}-{time.time_ns()}",
            "data": {
                "code": normalize_symbol(symbol),
                "kline_type": int(kline_type),
                "query_kline_num": int(query_kline_num),
                "adjust_type": 0,
            },
        }
        url = (
            f"{ALLTICK_HTTP_KLINE_URL}?token={quote(token)}"
            f"&query={quote(json.dumps(query, ensure_ascii=False, separators=(',', ':')))}"
        )
        base_sleep = max(self.request_interval_seconds, 6.0)
        last_exc: Exception | None = None
        for attempt in range(1, 4):
            session = make_session()
            try:
                response = session.get(url, timeout=30)
                response.raise_for_status()
                payload = response.json()
                rows = ((payload.get("data") or {}).get("kline_list") or [])
                return rows if isinstance(rows, list) else []
            except Exception as exc:
                last_exc = exc
                if attempt >= 3:
                    raise
                time.sleep(base_sleep * attempt)
            finally:
                session.close()
        if last_exc is not None:
            raise last_exc
        return []

    def _token_worker(
        self,
        token_index: int,
        token: str,
        symbols: list[str],
        start_date: date,
        end_date: date,
    ) -> tuple[int, int, dict[str, str]]:
        fetched = 0
        cache_hits = 0
        errors: dict[str, str] = {}
        for symbol in symbols:
            need_daily = not self._is_daily_cache_covered(symbol, start_date, end_date)
            need_minute = not self._is_minute_cache_covered(symbol, start_date, end_date)
            if not need_daily and not need_minute:
                cache_hits += 1
                continue

            if need_daily:
                try:
                    rows = self._fetch_kline_rows(token, symbol, DAILY_KLINE_TYPE, MAX_QUERY_KLINE_NUM)
                    self.cache.save_rows(symbol, DAILY_KLINE_TYPE, rows)
                except Exception as exc:
                    errors[symbol] = f"daily fetch failed: {exc}"
                    self.log(f"[alltick-error] token#{token_index} {symbol} 日线拉取失败: {exc}")
                    continue
                fetched += 1
                if need_minute and self.request_interval_seconds > 0:
                    time.sleep(self.request_interval_seconds)

            if need_minute:
                try:
                    rows = self._fetch_kline_rows(token, symbol, MINUTE_KLINE_TYPE, MAX_QUERY_KLINE_NUM)
                    self.cache.save_rows(symbol, MINUTE_KLINE_TYPE, rows)
                except Exception as exc:
                    errors[symbol] = f"minute fetch failed: {exc}"
                    self.log(f"[alltick-error] token#{token_index} {symbol} 分钟线拉取失败: {exc}")
                    continue
                fetched += 1

            if self.request_interval_seconds > 0:
                time.sleep(self.request_interval_seconds)
        return fetched, cache_hits, errors

    def warm_cache(self, symbols: list[str], start_date: date, end_date: date) -> CacheWarmResult:
        if not self.tokens:
            raise RuntimeError(f"未找到 AllTick API 文件: {self.api_file}")

        normalized_symbols = [normalize_symbol_code(symbol) for symbol in symbols]
        buckets = balanced_token_buckets(normalized_symbols, self.tokens)
        if self.max_concurrent_tokens > 0:
            buckets = buckets[: self.max_concurrent_tokens]
        active_tokens = len(buckets)
        total_fetched = 0
        total_cache_hits = 0
        errors: dict[str, str] = {}
        self.log(
            f"[alltick] 开始预热缓存: symbols={len(normalized_symbols)} total_tokens={len(self.tokens)} active_tokens={active_tokens}"
        )

        with ThreadPoolExecutor(max_workers=max(active_tokens, 1)) as executor:
            future_map = {
                executor.submit(self._token_worker, idx, token, bucket, start_date, end_date): (idx, token, bucket)
                for idx, (token, bucket) in enumerate(buckets, start=1)
            }
            for future in as_completed(future_map):
                idx, _, bucket = future_map[future]
                try:
                    fetched, cache_hits, bucket_errors = future.result()
                except Exception as exc:
                    for symbol in bucket:
                        errors[symbol] = str(exc)
                    self.log(f"[alltick-error] token#{idx} worker 失败: {exc}")
                    continue
                total_fetched += fetched
                total_cache_hits += cache_hits
                errors.update(bucket_errors)

        self.log(
            f"[alltick] 预热完成: fetched_requests={total_fetched} cache_hits={total_cache_hits} fetch_errors={len(errors)}"
        )
        return CacheWarmResult(
            total_tokens=len(self.tokens),
            active_tokens=active_tokens,
            fetched_symbols=total_fetched,
            cache_hit_symbols=total_cache_hits,
            fetch_errors=errors,
        )

    def load_daily_close_map(self, symbol: str, start_date: date, end_date: date) -> dict[date, float]:
        rows = self.cache.load_daily_rows(symbol, start_date, end_date)
        close_map: dict[date, float] = {}
        for row in rows:
            day = datetime.strptime(str(row["trading_day"]), "%Y-%m-%d").date()
            close_map[day] = float(row["close_price"] or 0)
        return close_map

    def iter_trading_days(self, symbol: str, start_date: date, end_date: date) -> list[date]:
        return self.cache.load_trading_days(symbol, start_date, end_date)

    def load_intraday_bars(self, symbol: str, trading_day: date) -> list[MinuteBar]:
        return self.cache.load_intraday_bars(symbol, trading_day)

    @property
    def cache_db_path(self) -> Path:
        return self.cache.path


__all__ = [
    "AllTickHistorySource",
    "CacheWarmResult",
    "DEFAULT_CACHE_DB_PATH",
    "DEFAULT_MANAGER_API_PATH",
    "DEFAULT_MANAGER_WATCHLIST_PATH",
    "DEFAULT_PER_TOKEN_INTERVAL_SECONDS",
    "load_symbols_from_watchlist",
    "load_watchlist_name_map",
    "normalize_symbol_code",
]
