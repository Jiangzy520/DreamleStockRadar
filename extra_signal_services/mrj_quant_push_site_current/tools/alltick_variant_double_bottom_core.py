#!/usr/bin/env python3
"""
Core helpers for the Variant Double Bottom intraday strategy.

Notes:
1. AllTick stock trade-tick is real-time only. Historical replay here uses
   locally recorded ticks from the SQLite store.
2. The financial hard filters are implemented for current-day screening.
   They are not point-in-time safe enough for a multi-year historical
   backtest yet.
"""

from __future__ import annotations

import csv
import json
import os
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

try:
    import pandas_market_calendars as mcal
except Exception:  # pragma: no cover - optional runtime fallback
    mcal = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "tools" / "alltick_variant_double_bottom.json"
DEFAULT_WATCHLIST_PATH = PROJECT_ROOT / "tools" / "alltick_variant_double_bottom_watchlist.txt"
DEFAULT_DB_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "variant_double_bottom_ticks.sqlite3"
DEFAULT_SIGNAL_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "variant_double_bottom_signals.csv"
DEFAULT_FILTER_REPORT_PATH = PROJECT_ROOT / ".guanlan" / "alltick" / "variant_double_bottom_filter_report.csv"

CHINA_TZ = ZoneInfo("Asia/Shanghai")
ALLTICK_HTTP_TRADE_TICK_URL = "https://quote.alltick.co/quote-stock-b-api/trade-tick"
ALLTICK_HTTP_KLINE_URL = "https://quote.alltick.co/quote-stock-b-api/kline"
ALLTICK_WS_URL = "wss://quote.alltick.co/quote-stock-b-ws-api"
SINA_FINANCE_URL = "https://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022"
EASTMONEY_STOCK_GET_URL = "https://push2.eastmoney.com/api/qt/stock/get"
EASTMONEY_DAILY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_YJYG_URL = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
PRELOSS_TYPES = {"首亏", "续亏", "增亏"}

DEFAULT_CONFIG = {
    "shape_bar_seconds": 5,
    "peak_left_units": 1,
    "peak_right_units": 1,
    "r1_open_gain_pct": 2.5,
    "confirm_mode": "tick",
    "buy_deadline": "14:30:00",
    "max_signals_per_day": 1,
    "scan_interval_seconds": 1,
    "seed_latest_http": False,
    "watchlist_file": str(DEFAULT_WATCHLIST_PATH),
}


@dataclass(frozen=True)
class WatchItem:
    symbol: str
    name: str


@dataclass(frozen=True)
class TradeTick:
    symbol: str
    name: str
    seq: str
    tick_time_ms: int
    price: float
    volume: float
    turnover: float
    trade_direction: int
    received_at_ms: int
    raw_json: str

    @property
    def dt(self) -> datetime:
        return datetime.fromtimestamp(self.tick_time_ms / 1000, tz=CHINA_TZ)


@dataclass(frozen=True)
class TimeBar:
    start_dt: datetime
    end_dt: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    turnover: float
    high_dt: datetime
    close_dt: datetime


@dataclass(frozen=True)
class PatternPoint:
    label: str
    bar_index: int
    bar_start: datetime
    point_time: datetime
    price: float


@dataclass(frozen=True)
class StrategySignal:
    symbol: str
    name: str
    trading_day: date
    confirm_mode: str
    signal_time: datetime
    signal_price: float
    open_price: float
    prev_close_price: float
    r1_time: datetime
    r1_price: float
    r2_time: datetime
    r2_price: float
    l1_time: datetime
    l1_price: float
    l2_time: datetime
    l2_price: float

    def to_dict(self) -> dict[str, str | float]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "trading_day": self.trading_day.isoformat(),
            "confirm_mode": self.confirm_mode,
            "signal_time": self.signal_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "signal_price": round(self.signal_price, 6),
            "open_price": round(self.open_price, 6),
            "prev_close_price": round(self.prev_close_price, 6),
            "r1_time": self.r1_time.strftime("%H:%M:%S"),
            "r1_price": round(self.r1_price, 6),
            "r2_time": self.r2_time.strftime("%H:%M:%S"),
            "r2_price": round(self.r2_price, 6),
            "l1_time": self.l1_time.strftime("%H:%M:%S"),
            "l1_price": round(self.l1_price, 6),
            "l2_time": self.l2_time.strftime("%H:%M:%S"),
            "l2_price": round(self.l2_price, 6),
        }


@dataclass(frozen=True)
class FinancialFilterResult:
    symbol: str
    name: str
    passed: bool
    eps_basic: float | None
    eps_report_date: str
    pe_ttm: float | None
    forecast_notice_date: str
    forecast_types: str
    reasons: str

    def to_dict(self) -> dict[str, str | float]:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "passed": "yes" if self.passed else "no",
            "eps_basic": "" if self.eps_basic is None else round(self.eps_basic, 6),
            "eps_report_date": self.eps_report_date,
            "pe_ttm": "" if self.pe_ttm is None else round(self.pe_ttm, 6),
            "forecast_notice_date": self.forecast_notice_date,
            "forecast_types": self.forecast_types,
            "reasons": self.reasons,
        }


def now_ms() -> int:
    return int(datetime.now(CHINA_TZ).timestamp() * 1000)


def load_env_file(path: Path = DEFAULT_ENV_PATH) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values

    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = raw.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def get_alltick_token(path: Path = DEFAULT_ENV_PATH, explicit_token: str = "") -> str:
    if explicit_token.strip():
        return explicit_token.strip()

    env_token = os.getenv("ALLTICK_TOKEN", "").strip()
    if env_token:
        return env_token

    file_token = load_env_file(path).get("ALLTICK_TOKEN", "").strip()
    if file_token:
        return file_token

    raise RuntimeError("Missing ALLTICK_TOKEN in environment or .env")


def load_json_config(path: Path = DEFAULT_CONFIG_PATH) -> dict:
    if not path.exists():
        return DEFAULT_CONFIG.copy()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return DEFAULT_CONFIG.copy()

    merged = DEFAULT_CONFIG.copy()
    merged.update(data)
    return merged


def infer_suffix(code: str) -> str:
    if code.startswith(("6", "5", "9")):
        return "SH"
    if code.startswith(("0", "2", "3")):
        return "SZ"
    if code.startswith(("4", "8")):
        return "BJ"
    raise ValueError(f"Unsupported code prefix: {code}")


def normalize_symbol(raw: str) -> str:
    text = raw.strip().upper()
    if not text:
        raise ValueError("Empty symbol")

    if "." in text:
        code, suffix = text.split(".", 1)
        if len(code) == 6 and suffix in {"SH", "SZ", "BJ"}:
            return f"{code}.{suffix}"

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) == 6:
        return f"{digits}.{infer_suffix(digits)}"

    raise ValueError(f"Unsupported symbol format: {raw}")


def symbol_code(symbol: str) -> str:
    return normalize_symbol(symbol).split(".", 1)[0]


def symbol_suffix(symbol: str) -> str:
    return normalize_symbol(symbol).split(".", 1)[1]


def eastmoney_secid(symbol: str) -> str:
    code = symbol_code(symbol)
    if code.startswith(("6", "5", "9")):
        return f"1.{code}"
    return f"0.{code}"


def sina_paper_code(symbol: str) -> str:
    suffix = symbol_suffix(symbol).lower()
    return f"{suffix}{symbol_code(symbol)}"


def floor_interval(dt_value: datetime, seconds: int) -> datetime:
    timestamp = int(dt_value.timestamp())
    floored = timestamp - (timestamp % seconds)
    return datetime.fromtimestamp(floored, tz=CHINA_TZ)


def aggregate_ticks(ticks: Sequence[TradeTick], interval_seconds: int) -> list[TimeBar]:
    if not ticks:
        return []

    ordered = sorted(ticks, key=lambda item: (item.tick_time_ms, item.seq))
    bars: list[TimeBar] = []
    bucket_start = floor_interval(ordered[0].dt, interval_seconds)
    bucket_end = bucket_start + timedelta(seconds=interval_seconds)
    bucket_ticks: list[TradeTick] = []

    def flush(current_ticks: list[TradeTick], start_dt: datetime, end_dt: datetime) -> None:
        if not current_ticks:
            return
        prices = [item.price for item in current_ticks]
        high_tick = max(current_ticks, key=lambda item: (item.price, -item.tick_time_ms))
        close_tick = current_ticks[-1]
        bars.append(
            TimeBar(
                start_dt=start_dt,
                end_dt=end_dt,
                open_price=current_ticks[0].price,
                high_price=max(prices),
                low_price=min(prices),
                close_price=close_tick.price,
                volume=sum(item.volume for item in current_ticks),
                turnover=sum(item.turnover for item in current_ticks),
                high_dt=high_tick.dt,
                close_dt=close_tick.dt,
            )
        )

    for tick in ordered:
        current_bucket = floor_interval(tick.dt, interval_seconds)
        if current_bucket != bucket_start:
            flush(bucket_ticks, bucket_start, bucket_end)
            bucket_ticks = []
            bucket_start = current_bucket
            bucket_end = bucket_start + timedelta(seconds=interval_seconds)
        bucket_ticks.append(tick)

    flush(bucket_ticks, bucket_start, bucket_end)
    return bars


def load_watchlist(path: Path = DEFAULT_WATCHLIST_PATH) -> dict[str, WatchItem]:
    items: dict[str, WatchItem] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = raw.strip()
        if not text or text.startswith("#"):
            continue

        parts = [part.strip() for part in text.replace("\t", ",").split(",") if part.strip()]
        if not parts:
            continue

        symbol = normalize_symbol(parts[0])
        name = parts[1] if len(parts) > 1 else symbol
        items[symbol] = WatchItem(symbol=symbol, name=name)
    return items


def make_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


def recent_report_periods(reference_day: date, count: int = 8) -> list[str]:
    periods: list[str] = []
    year = reference_day.year
    while len(periods) < count:
        for suffix in ("1231", "0930", "0630", "0331"):
            candidate = date(year, int(suffix[:2]), int(suffix[2:]))
            if candidate <= reference_day:
                periods.append(f"{year}{suffix}")
                if len(periods) >= count:
                    break
        year -= 1
    return periods


def next_trading_day(value: date) -> date:
    if mcal is None:
        day = value + timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        return day

    calendar = mcal.get_calendar("XSHG")
    schedule = calendar.schedule(
        start_date=value + timedelta(days=1),
        end_date=value + timedelta(days=14),
    )
    if schedule.empty:
        day = value + timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        return day
    return schedule.index[0].date()


def fetch_today_open_price(token: str, symbol: str, session: requests.Session | None = None) -> float | None:
    own_session = session is None
    session = session or make_session()
    try:
        query = {
            "trace": "variant-double-bottom-open",
            "data": {
                "code": normalize_symbol(symbol),
                "kline_type": 1,
                "query_kline_num": 240,
                "adjust_type": 0,
            },
        }
        url = (
            f"{ALLTICK_HTTP_KLINE_URL}?token={quote(token)}"
            f"&query={quote(json.dumps(query, ensure_ascii=False, separators=(',', ':')))}"
        )
        response = session.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
        rows = ((payload.get("data") or {}).get("kline_list") or [])
        if not rows:
            return None

        latest_day = max(
            datetime.fromtimestamp(int(row["timestamp"]), tz=CHINA_TZ).date()
            for row in rows
        )
        same_day = [
            row for row in rows
            if datetime.fromtimestamp(int(row["timestamp"]), tz=CHINA_TZ).date() == latest_day
        ]
        if not same_day:
            return None
        first_bar = min(same_day, key=lambda item: int(item["timestamp"]))
        return float(first_bar.get("open_price") or 0) or None
    finally:
        if own_session:
            session.close()


def get_r1_intraday_gain_pct(config: dict) -> float:
    raw = config.get("r1_intraday_gain_pct")
    if raw is None:
        raw = config.get("r1_open_gain_pct", 2.5)
    try:
        return float(raw)
    except Exception:
        return 2.5


def fetch_daily_reference_prices(
    symbol: str,
    trading_day: date | None = None,
    session: requests.Session | None = None,
) -> tuple[float | None, float | None]:
    target_day = trading_day or datetime.now(CHINA_TZ).date()
    own_session = session is None
    session = session or make_session()
    try:
        payload = None
        request_params = {
            "secid": eastmoney_secid(symbol),
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "0",
            "beg": (target_day - timedelta(days=15)).strftime("%Y%m%d"),
            "end": target_day.strftime("%Y%m%d"),
            "lmt": "32",
        }
        for _ in range(2):
            try:
                response = session.get(
                    EASTMONEY_DAILY_KLINE_URL,
                    params=request_params,
                    timeout=20,
                )
                response.raise_for_status()
                payload = response.json()
                break
            except Exception:
                payload = None
        if payload is None:
            try:
                response = requests.get(
                    EASTMONEY_DAILY_KLINE_URL,
                    params=request_params,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=20,
                )
                response.raise_for_status()
                payload = response.json()
            except Exception:
                return None, None
        data = payload.get("data") or {}
        raw_rows = data.get("klines") or []
        if not raw_rows:
            return None, None

        parsed_rows: list[tuple[date, float | None, float | None]] = []
        for raw_row in raw_rows:
            parts = str(raw_row).split(",")
            if len(parts) < 3:
                continue
            try:
                row_day = datetime.strptime(parts[0], "%Y-%m-%d").date()
            except ValueError:
                continue
            try:
                row_open = float(parts[1]) if parts[1] not in {"", "-"} else None
            except Exception:
                row_open = None
            try:
                row_close = float(parts[2]) if parts[2] not in {"", "-"} else None
            except Exception:
                row_close = None
            parsed_rows.append((row_day, row_open, row_close))

        if not parsed_rows:
            return None, None

        previous_close: float | None = None
        target_open: float | None = None
        for index, (row_day, row_open, _row_close) in enumerate(parsed_rows):
            if row_day != target_day:
                continue
            target_open = row_open
            if index > 0:
                previous_close = parsed_rows[index - 1][2]
            elif data.get("preKPrice") not in (None, "", "-"):
                try:
                    previous_close = float(data["preKPrice"])
                except Exception:
                    previous_close = None
            break

        if previous_close is None:
            prior_rows = [row for row in parsed_rows if row[0] < target_day and row[2] is not None]
            if prior_rows:
                previous_close = prior_rows[-1][2]

        return target_open, previous_close
    finally:
        if own_session:
            session.close()


def fetch_dynamic_pe_ttm(symbol: str, session: requests.Session | None = None) -> float | None:
    own_session = session is None
    session = session or make_session()
    try:
        response = session.get(
            EASTMONEY_STOCK_GET_URL,
            params={
                "fltt": "2",
                "invt": "2",
                "fields": "f57,f58,f162,f9,f115",
                "secid": eastmoney_secid(symbol),
            },
            timeout=20,
        )
        response.raise_for_status()
        data = (response.json().get("data") or {})
        for field in ("f162", "f9", "f115"):
            value = data.get(field)
            if value in (None, "", "-"):
                continue
            return float(value)
        return None
    finally:
        if own_session:
            session.close()


def fetch_latest_basic_eps(symbol: str, session: requests.Session | None = None) -> tuple[str, float | None]:
    own_session = session is None
    session = session or make_session()
    try:
        response = session.get(
            SINA_FINANCE_URL,
            params={
                "paperCode": sina_paper_code(symbol),
                "source": "gjzb",
                "type": "0",
                "page": "1",
                "num": "20",
            },
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        report_map = (((payload.get("result") or {}).get("data") or {}).get("report_list") or {})
        if not report_map:
            return "", None

        latest_report = max(report_map.keys())
        rows = report_map.get(latest_report, {}).get("data", [])
        for row in rows:
            if row.get("item_field") == "EPSBASIC" or row.get("item_title") == "基本每股收益":
                value = row.get("item_value")
                if value in (None, "", "-"):
                    return latest_report, None
                return latest_report, float(value)
        return latest_report, None
    finally:
        if own_session:
            session.close()


def fetch_latest_forecast_map(
    symbols: Sequence[str],
    reference_day: date,
    session: requests.Session | None = None,
) -> dict[str, tuple[date, set[str]]]:
    own_session = session is None
    session = session or make_session()
    codes = {symbol_code(symbol) for symbol in symbols}
    result: dict[str, tuple[date, set[str]]] = {}
    try:
        for period in recent_report_periods(reference_day):
            params = {
                "sortColumns": "NOTICE_DATE,SECURITY_CODE",
                "sortTypes": "-1,-1",
                "pageSize": "500",
                "pageNumber": "1",
                "reportName": "RPT_PUBLIC_OP_NEWPREDICT",
                "columns": "ALL",
                "filter": f"(REPORT_DATE='{period[:4]}-{period[4:6]}-{period[6:]}')",
            }
            first_response = session.get(EASTMONEY_YJYG_URL, params=params, timeout=20)
            first_response.raise_for_status()
            first_payload = first_response.json()
            first_result = first_payload.get("result") or {}
            total_pages = int(first_result.get("pages") or 0)
            rows = list(first_result.get("data") or [])
            for page in range(2, total_pages + 1):
                params["pageNumber"] = str(page)
                page_response = session.get(EASTMONEY_YJYG_URL, params=params, timeout=20)
                page_response.raise_for_status()
                rows.extend(((page_response.json().get("result") or {}).get("data") or []))

            filtered_rows = [
                row for row in rows
                if str(row.get("SECURITY_CODE", "")).zfill(6) in codes
            ]
            if not filtered_rows:
                continue

            by_code: dict[str, list[dict]] = {}
            for row in filtered_rows:
                code = str(row.get("SECURITY_CODE", "")).zfill(6)
                by_code.setdefault(code, []).append(row)

            for code, code_rows in by_code.items():
                if code in result:
                    continue
                latest_notice = max(
                    datetime.fromisoformat(str(row["NOTICE_DATE"]).replace(" ", "T")).date()
                    for row in code_rows
                )
                latest_rows = [
                    row for row in code_rows
                    if datetime.fromisoformat(str(row["NOTICE_DATE"]).replace(" ", "T")).date() == latest_notice
                ]
                result[code] = (
                    latest_notice,
                    {str(row.get("PREDICT_TYPE", "")).strip() for row in latest_rows if str(row.get("PREDICT_TYPE", "")).strip()},
                )
            if len(result) >= len(codes):
                break
        return result
    finally:
        if own_session:
            session.close()


def evaluate_financial_filters(
    watch_items: dict[str, WatchItem],
    reference_day: date | None = None,
) -> dict[str, FinancialFilterResult]:
    reference_day = reference_day or datetime.now(CHINA_TZ).date()
    session = make_session()
    forecast_map = fetch_latest_forecast_map(list(watch_items.keys()), reference_day, session=session)
    results: dict[str, FinancialFilterResult] = {}
    try:
        for symbol, item in watch_items.items():
            reasons: list[str] = []
            eps_report_date, eps_basic = fetch_latest_basic_eps(symbol, session=session)
            pe_ttm = fetch_dynamic_pe_ttm(symbol, session=session)

            if eps_basic is None:
                reasons.append("missing_eps")
            elif eps_basic < 0:
                reasons.append("eps_negative")

            if pe_ttm is None:
                reasons.append("missing_pe_ttm")
            elif pe_ttm < 0:
                reasons.append("pe_ttm_negative")

            forecast_notice_date = ""
            forecast_types = ""
            forecast_info = forecast_map.get(symbol_code(symbol))
            if forecast_info:
                notice_day, types = forecast_info
                forecast_notice_date = notice_day.isoformat()
                forecast_types = ",".join(sorted(types))
                if types & PRELOSS_TYPES:
                    next_day = next_trading_day(notice_day)
                    if reference_day in {notice_day, next_day}:
                        reasons.append("recent_preloss_forecast")

            results[symbol] = FinancialFilterResult(
                symbol=symbol,
                name=item.name,
                passed=not reasons,
                eps_basic=eps_basic,
                eps_report_date=eps_report_date,
                pe_ttm=pe_ttm,
                forecast_notice_date=forecast_notice_date,
                forecast_types=forecast_types,
                reasons=",".join(reasons),
            )
        return results
    finally:
        session.close()


def append_csv_row(path: Path, row: dict[str, str | float], headers: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(headers))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def save_filter_report(results: Sequence[FinancialFilterResult], path: Path = DEFAULT_FILTER_REPORT_PATH) -> None:
    headers = list(results[0].to_dict().keys()) if results else list(FinancialFilterResult.__dataclass_fields__.keys())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for result in results:
            writer.writerow(result.to_dict())


def parse_trade_tick_message(payload: dict, watch_items: dict[str, WatchItem]) -> list[TradeTick]:
    data = payload.get("data")
    raw_ticks: list[dict] = []
    if isinstance(data, dict):
        if isinstance(data.get("tick_list"), list):
            raw_ticks.extend(item for item in data["tick_list"] if isinstance(item, dict))
        elif isinstance(data.get("tick"), dict):
            raw_ticks.append(data["tick"])
        elif {"code", "tick_time", "price"} <= set(data.keys()):
            raw_ticks.append(data)
    elif isinstance(data, list):
        raw_ticks.extend(item for item in data if isinstance(item, dict))

    result: list[TradeTick] = []
    for raw_tick in raw_ticks:
        try:
            symbol = normalize_symbol(str(raw_tick.get("code", "")))
            item = watch_items.get(symbol, WatchItem(symbol=symbol, name=symbol))
            seq = str(raw_tick.get("seq", "")).strip()
            tick_time_ms = int(raw_tick.get("tick_time", 0) or 0)
            if not seq or tick_time_ms <= 0:
                continue
            result.append(
                TradeTick(
                    symbol=symbol,
                    name=item.name,
                    seq=seq,
                    tick_time_ms=tick_time_ms,
                    price=float(raw_tick.get("price", 0) or 0),
                    volume=float(raw_tick.get("volume", 0) or 0),
                    turnover=float(raw_tick.get("turnover", 0) or 0),
                    trade_direction=int(raw_tick.get("trade_direction", 0) or 0),
                    received_at_ms=now_ms(),
                    raw_json=json.dumps(raw_tick, ensure_ascii=False, separators=(",", ":")),
                )
            )
        except Exception:
            continue
    return result


def fetch_latest_http_ticks(
    token: str,
    symbols: Sequence[str],
    watch_items: dict[str, WatchItem],
    session: requests.Session | None = None,
) -> list[TradeTick]:
    own_session = session is None
    session = session or make_session()
    try:
        query = {
            "trace": f"variant-double-bottom-seed-{int(datetime.now(CHINA_TZ).timestamp())}",
            "data": {"symbol_list": [{"code": normalize_symbol(symbol)} for symbol in symbols]},
        }
        url = (
            f"{ALLTICK_HTTP_TRADE_TICK_URL}?token={quote(token)}"
            f"&query={quote(json.dumps(query, ensure_ascii=False, separators=(',', ':')))}"
        )
        response = session.get(url, timeout=20)
        response.raise_for_status()
        return parse_trade_tick_message(response.json(), watch_items)
    finally:
        if own_session:
            session.close()


class TickStore:
    """SQLite-backed raw tick store for the strategy."""

    def __init__(self, path: Path = DEFAULT_DB_PATH) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS trade_ticks (
                symbol TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                seq TEXT NOT NULL,
                tick_time_ms INTEGER NOT NULL,
                tick_time_text TEXT NOT NULL,
                price REAL NOT NULL,
                volume REAL NOT NULL,
                turnover REAL NOT NULL,
                trade_direction INTEGER NOT NULL,
                received_at_ms INTEGER NOT NULL,
                received_at_text TEXT NOT NULL,
                raw_json TEXT NOT NULL,
                PRIMARY KEY(symbol, tick_time_ms, seq)
            );
            CREATE INDEX IF NOT EXISTS idx_variant_tick_symbol_time
            ON trade_ticks(symbol, tick_time_ms);
            """
        )
        self.conn.commit()

    def close(self) -> None:
        self.conn.close()

    def save_ticks(self, ticks: Sequence[TradeTick]) -> int:
        rows = [
            (
                tick.symbol,
                symbol_code(tick.symbol),
                tick.name,
                tick.seq,
                tick.tick_time_ms,
                tick.dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                tick.price,
                tick.volume,
                tick.turnover,
                tick.trade_direction,
                tick.received_at_ms,
                datetime.fromtimestamp(tick.received_at_ms / 1000, tz=CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                tick.raw_json,
            )
            for tick in ticks
        ]
        before = self.conn.total_changes
        self.conn.executemany(
            """
            INSERT OR IGNORE INTO trade_ticks (
                symbol, code, name, seq, tick_time_ms, tick_time_text,
                price, volume, turnover, trade_direction,
                received_at_ms, received_at_text, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        self.conn.commit()
        return self.conn.total_changes - before

    def load_ticks(
        self,
        symbol: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> list[TradeTick]:
        sql = [
            """
            SELECT symbol, name, seq, tick_time_ms, price, volume, turnover,
                   trade_direction, received_at_ms, raw_json
            FROM trade_ticks
            WHERE symbol = ?
            """
        ]
        params: list[object] = [normalize_symbol(symbol)]
        if start_ms is not None:
            sql.append("AND tick_time_ms >= ?")
            params.append(start_ms)
        if end_ms is not None:
            sql.append("AND tick_time_ms < ?")
            params.append(end_ms)
        sql.append("ORDER BY tick_time_ms ASC, seq ASC")
        rows = self.conn.execute("\n".join(sql), params).fetchall()
        return [
            TradeTick(
                symbol=row["symbol"],
                name=row["name"],
                seq=row["seq"],
                tick_time_ms=int(row["tick_time_ms"]),
                price=float(row["price"]),
                volume=float(row["volume"]),
                turnover=float(row["turnover"]),
                trade_direction=int(row["trade_direction"]),
                received_at_ms=int(row["received_at_ms"]),
                raw_json=row["raw_json"],
            )
            for row in rows
        ]


def china_day_bounds(day: date) -> tuple[int, int]:
    start_dt = datetime.combine(day, dt_time.min, tzinfo=CHINA_TZ)
    end_dt = start_dt + timedelta(days=1)
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000)


def local_peak_indices(
    bars: Sequence[TimeBar],
    left_units: int,
    right_units: int,
) -> list[int]:
    result: list[int] = []
    if len(bars) < left_units + right_units + 1:
        return result

    for idx in range(left_units, len(bars) - right_units):
        center = bars[idx]
        left_bars = bars[idx - left_units:idx]
        right_bars = bars[idx + 1:idx + 1 + right_units]
        if not left_bars or not right_bars:
            continue

        left_up = all(
            left_bars[offset].close_price < (left_bars[offset + 1].close_price if offset + 1 < len(left_bars) else center.close_price)
            for offset in range(len(left_bars))
        )
        right_down = all(
            (center.close_price if offset == 0 else right_bars[offset - 1].close_price) > right_bars[offset].close_price
            for offset in range(len(right_bars))
        )
        if not (left_up and right_down):
            continue

        if all(center.high_price >= bar.high_price for bar in left_bars + list(right_bars)) and any(
            center.high_price > bar.high_price for bar in left_bars + list(right_bars)
        ):
            result.append(idx)

    return result


def first_threshold_point_from_bars(
    bars: Sequence[TimeBar],
    reference_price: float,
    config: dict,
) -> PatternPoint | None:
    if reference_price <= 0:
        return None

    threshold_price = reference_price * (1 + get_r1_intraday_gain_pct(config) / 100)
    market_open = dt_time(9, 30)
    cutoff_time = dt_time.fromisoformat(str(config["buy_deadline"]))

    for index, bar in enumerate(bars):
        bar_time = bar.start_dt.timetz().replace(tzinfo=None)
        if bar_time < market_open:
            continue
        if bar_time >= cutoff_time:
            return None
        if bar.high_price >= threshold_price:
            return PatternPoint(
                label="R1",
                bar_index=index,
                bar_start=bar.start_dt,
                point_time=bar.start_dt,
                price=bar.high_price,
            )
    return None


def first_threshold_point_from_ticks(
    ticks: Sequence[TradeTick],
    bars: Sequence[TimeBar],
    reference_price: float,
    config: dict,
) -> PatternPoint | None:
    if reference_price <= 0:
        return None

    threshold_price = reference_price * (1 + get_r1_intraday_gain_pct(config) / 100)
    market_open = dt_time(9, 30)
    cutoff_time = dt_time.fromisoformat(str(config["buy_deadline"]))

    for tick in sorted(ticks, key=lambda item: (item.tick_time_ms, item.seq)):
        tick_time = tick.dt.timetz().replace(tzinfo=None)
        if tick_time < market_open:
            continue
        if tick_time >= cutoff_time:
            return None
        if tick.price < threshold_price:
            continue
        for index, bar in enumerate(bars):
            if bar.start_dt <= tick.dt < bar.end_dt:
                return PatternPoint(
                    label="R1",
                    bar_index=index,
                    bar_start=bar.start_dt,
                    point_time=tick.dt,
                    price=tick.price,
                )
        if bars:
            last_index = len(bars) - 1
            return PatternPoint(
                label="R1",
                bar_index=last_index,
                bar_start=bars[last_index].start_dt,
                point_time=tick.dt,
                price=tick.price,
            )
        return None
    return None


def find_pattern_points(
    bars: Sequence[TimeBar],
    reference_price: float,
    config: dict,
    initial_r1: PatternPoint | None = None,
) -> tuple[PatternPoint, PatternPoint, PatternPoint, PatternPoint] | None:
    if reference_price <= 0:
        return None

    left_units = int(config["peak_left_units"])
    right_units = int(config["peak_right_units"])
    cutoff_time = dt_time.fromisoformat(str(config["buy_deadline"]))
    peak_indices = local_peak_indices(bars, left_units, right_units)

    def point_for(index: int, label: str) -> PatternPoint:
        bar = bars[index]
        return PatternPoint(
            label=label,
            bar_index=index,
            bar_start=bar.start_dt,
            point_time=bar.high_dt,
            price=bar.high_price,
        )

    r1: PatternPoint | None = initial_r1 or first_threshold_point_from_bars(bars, reference_price, config)
    r2: PatternPoint | None = None
    l1: PatternPoint | None = None
    l2: PatternPoint | None = None
    if r1 is None:
        return None

    for index in peak_indices:
        bar = bars[index]
        if bar.high_dt.timetz().replace(tzinfo=None) >= cutoff_time:
            break
        if index <= r1.bar_index:
            continue

        peak = point_for(index, "")
        if r2 is None:
            if peak.price < r1.price:
                r2 = point_for(index, "R2")
            continue

        if l1 is None:
            if peak.price > r2.price:
                l1 = point_for(index, "L1")
            continue

        if l2 is None:
            if peak.price > l1.price:
                l1 = point_for(index, "L1")
            elif peak.price < l1.price:
                l2 = point_for(index, "L2")
                break

    if all(point is not None for point in (r1, r2, l1, l2)):
        return r1, r2, l1, l2
    return None


def first_breakout_tick(
    ticks: Sequence[TradeTick],
    level: float,
    after_dt: datetime,
    cutoff_time: dt_time,
) -> TradeTick | None:
    for tick in sorted(ticks, key=lambda item: (item.tick_time_ms, item.seq)):
        if tick.dt <= after_dt:
            continue
        if tick.dt.timetz().replace(tzinfo=None) >= cutoff_time:
            return None
        if tick.price > level:
            return tick
    return None


def first_breakout_minute_close(
    ticks: Sequence[TradeTick],
    level: float,
    after_dt: datetime,
    cutoff_time: dt_time,
) -> TimeBar | None:
    minute_ticks = [tick for tick in ticks if tick.dt > after_dt]
    bars = aggregate_ticks(minute_ticks, 60)
    if len(bars) > 1:
        bars = bars[:-1]
    for bar in bars:
        if bar.end_dt.timetz().replace(tzinfo=None) >= cutoff_time:
            return None
        if bar.close_price > level:
            return bar
    return None


def detect_variant_double_bottom(
    ticks: Sequence[TradeTick],
    config: dict,
    open_price: float | None = None,
    prev_close_price: float | None = None,
) -> StrategySignal | None:
    ordered_ticks = sorted(ticks, key=lambda item: (item.tick_time_ms, item.seq))
    if not ordered_ticks:
        return None

    open_price = open_price or ordered_ticks[0].price
    prev_close_price = prev_close_price or open_price
    shape_seconds = int(config["shape_bar_seconds"])
    shape_bars = aggregate_ticks(ordered_ticks, shape_seconds)
    if len(shape_bars) > 1:
        shape_bars = shape_bars[:-1]
    if len(shape_bars) < int(config["peak_left_units"]) + int(config["peak_right_units"]) + 4:
        return None

    initial_r1 = first_threshold_point_from_ticks(ordered_ticks, shape_bars, prev_close_price, config)
    points = find_pattern_points(shape_bars, prev_close_price, config, initial_r1=initial_r1)
    if not points:
        return None
    r1, r2, l1, l2 = points

    right_units = int(config["peak_right_units"])
    confirm_bar_index = min(l2.bar_index + right_units, len(shape_bars) - 1)
    confirm_after_dt = shape_bars[confirm_bar_index].end_dt
    cutoff_time = dt_time.fromisoformat(str(config["buy_deadline"]))
    confirm_mode = str(config["confirm_mode"]).strip().lower()

    if confirm_mode == "minute_close":
        minute_bar = first_breakout_minute_close(ordered_ticks, l2.price, confirm_after_dt, cutoff_time)
        if not minute_bar:
            return None
        signal_time = minute_bar.close_dt
        signal_price = minute_bar.close_price
    else:
        trigger_tick = first_breakout_tick(ordered_ticks, l2.price, confirm_after_dt, cutoff_time)
        if not trigger_tick:
            return None
        signal_time = trigger_tick.dt
        signal_price = trigger_tick.price

    return StrategySignal(
        symbol=ordered_ticks[0].symbol,
        name=ordered_ticks[0].name,
        trading_day=signal_time.date(),
        confirm_mode=confirm_mode,
        signal_time=signal_time,
        signal_price=signal_price,
        open_price=open_price,
        prev_close_price=prev_close_price,
        r1_time=r1.point_time,
        r1_price=r1.price,
        r2_time=r2.point_time,
        r2_price=r2.price,
        l1_time=l1.point_time,
        l1_price=l1.price,
        l2_time=l2.point_time,
        l2_price=l2.price,
    )
