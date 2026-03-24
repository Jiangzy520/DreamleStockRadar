#!/usr/bin/env python3
"""
Replay the 1-minute local-peak breakout strategy on recent history.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tools.tq_peak_breakout_watch import (  # noqa: E402
    DEFAULT_CONFIG,
    MinuteBar,
    WATCHLIST_PATH,
    filter_peaks,
    floor_minute,
    load_json,
    load_watchlist,
    local_peak_indices,
    pct,
)


DEFAULT_OUTPUT_DIR = PROJECT_ROOT / ".guanlan" / "replay_results"
CONFIG_PATH = PROJECT_ROOT / "tools" / "tq_peak_breakout_watch.json"
DEFAULT_TEST_CODE = "000001"
THREAD_LOCAL = threading.local()


@dataclass
class ReplaySignalRecord:
    code: str
    name: str
    trading_day: str
    signal_time: str
    breakout_close: float
    left2_close: float
    signal_pct: float
    speed_pct: float
    volume_ratio: float
    right1_time: str
    right1_close: float
    right1_gain_pct: float
    right2_time: str
    right2_close: float
    right2_gain_pct: float
    left1_time: str
    left1_close: float
    left1_gain_pct: float
    left2_time: str
    left2_gain_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay left2 breakout on 1-minute history")
    parser.add_argument("--start-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--watchlist-file",
        default=str(WATCHLIST_PATH),
        help="watchlist csv-like file, default is tq_peak_breakout_watchlist.txt",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=8,
        help="parallel worker count",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="result directory",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="stdout preview row limit",
    )
    return parser.parse_args()


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def load_watch_items(path: Path) -> dict[str, str]:
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    config["watchlist_file"] = str(path)
    return {code: item.name for code, item in load_watchlist(config).items()}


def market_for_code(code: str) -> int:
    if code.startswith(("6", "5", "9")):
        return 1
    return 0


def choose_available_host() -> tuple[str, int]:
    preferred = [
        ("上证云成都电信一", "218.6.170.47", 7709),
        ("上海电信主站Z1", "180.153.18.170", 7709),
        ("北京联通主站Z1", "202.108.253.130", 7709),
    ]
    tried = preferred + [host for host in hq_hosts if host not in preferred]
    for _name, host, port in tried:
        api = TdxHq_API(raise_exception=True)
        try:
            with api.connect(host, port, time_out=2):
                rows = api.get_security_bars(TDXParams.KLINE_TYPE_1MIN, 0, DEFAULT_TEST_CODE, 0, 2)
                if rows:
                    return host, port
        except Exception:
            continue
    raise RuntimeError("No available TDX HQ host found")


def get_thread_api(host: str, port: int) -> TdxHq_API:
    api = getattr(THREAD_LOCAL, "api", None)
    if api is not None:
        return api

    api = TdxHq_API(raise_exception=True)
    if not api.connect(host, port, time_out=5):
        raise RuntimeError(f"failed to connect TDX host {host}:{port}")
    THREAD_LOCAL.api = api
    return api


def fetch_kline_pages(
    api: TdxHq_API,
    category: int,
    market: int,
    code: str,
    pages: int,
    page_size: int = TDXParams.MAX_KLINE_COUNT,
) -> list[dict]:
    rows: list[dict] = []
    for page in range(pages):
        chunk = api.get_security_bars(category, market, code, page * page_size, page_size)
        if not chunk:
            break
        rows.extend(chunk)
        if len(chunk) < page_size:
            break

    dedup: dict[str, dict] = {}
    for row in rows:
        dedup[row["datetime"]] = row
    return [dedup[key] for key in sorted(dedup)]


def load_daily_close_map(api: TdxHq_API, symbol: str, start_date: date, end_date: date) -> dict[date, float]:
    min_day = start_date - timedelta(days=20)
    rows = fetch_kline_pages(api, TDXParams.KLINE_TYPE_DAILY, market_for_code(symbol), symbol, pages=1)
    close_map: dict[date, float] = {}
    for row in rows:
        trading_day = datetime.strptime(str(row["datetime"]), "%Y-%m-%d %H:%M").date()
        if min_day <= trading_day <= end_date:
            close_map[trading_day] = float(row["close"])
    return close_map


def get_prev_close(close_map: dict[date, float], trading_day: date) -> float:
    previous_days = [value for value in close_map if value < trading_day]
    if not previous_days:
        return 0.0
    return close_map[max(previous_days)]


def fetch_intraday_bars(api: TdxHq_API, symbol: str, start_date: date, end_date: date) -> list[MinuteBar]:
    rows = fetch_kline_pages(api, TDXParams.KLINE_TYPE_1MIN, market_for_code(symbol), symbol, pages=2)
    bars: list[MinuteBar] = []
    for row in rows:
        dt = floor_minute(datetime.strptime(str(row["datetime"]), "%Y-%m-%d %H:%M"))
        if not (start_date <= dt.date() <= end_date):
            continue
        close_price = float(row["close"] or 0)
        if close_price <= 0:
            continue
        open_price = float(row["open"] or 0) or close_price
        high_price = float(row["high"] or 0) or close_price
        low_price = float(row["low"] or 0) or close_price
        bars.append(
            MinuteBar(
                dt=dt,
                open=open_price,
                high=high_price,
                low=low_price,
                close=close_price,
                volume=float(row["vol"] or 0),
                amount=float(row["amount"] or 0),
            )
        )
    return bars


def iter_daily_bars(bars: Iterable[MinuteBar], start_date: date, end_date: date) -> dict[date, list[MinuteBar]]:
    grouped: dict[date, list[MinuteBar]] = {}
    for bar in bars:
        trading_day = bar.dt.date()
        if start_date <= trading_day <= end_date:
            grouped.setdefault(trading_day, []).append(bar)
    return grouped


def analyze_completed_bars(
    code: str,
    name: str,
    pre_close: float,
    completed: list[MinuteBar],
    config: dict,
) -> ReplaySignalRecord | None:
    lookback_bars = int(config["lookback_bars"])
    if len(completed) < max(25, lookback_bars // 3):
        return None

    completed = completed[-lookback_bars:]
    if len(completed) < 8 or pre_close <= 0:
        return None

    confirm_bar = completed[-1]
    previous_bar = completed[-2]

    peaks = local_peak_indices(completed, int(config["peak_radius"]))
    peaks = filter_peaks(
        completed,
        peaks,
        int(config["min_peak_gap_bars"]),
        float(config["min_pullback_pct"]),
    )
    peaks = [idx for idx in peaks if idx < len(completed) - 1]
    if len(peaks) < 4:
        return None

    right1_idx, right2_idx, left1_idx, left2_idx = peaks[-4:]
    if not (right1_idx < right2_idx < left1_idx < left2_idx < len(completed) - 1):
        return None

    right1 = completed[right1_idx]
    right2 = completed[right2_idx]
    left1 = completed[left1_idx]
    left2 = completed[left2_idx]

    gains = [
        pct(right1.close, pre_close),
        pct(right2.close, pre_close),
        pct(left1.close, pre_close),
        pct(left2.close, pre_close),
    ]
    if max(gains) < float(config["min_pattern_peak_gain_pct"]):
        return None

    breakout_level = left2.close
    if previous_bar.close > breakout_level:
        return None
    if confirm_bar.close <= breakout_level:
        return None

    volume_window = completed[
        max(0, len(completed) - 1 - int(config["breakout_volume_window"])):len(completed) - 1
    ]
    if not volume_window:
        return None

    avg_volume = sum(bar.volume for bar in volume_window) / len(volume_window)
    if avg_volume <= 0:
        return None

    volume_ratio = confirm_bar.volume / avg_volume
    if volume_ratio < float(config["breakout_min_volume_ratio"]):
        return None

    speed_pct = (confirm_bar.close - previous_bar.close) / pre_close * 100
    if speed_pct < float(config["breakout_min_speed_pct"]):
        return None

    return ReplaySignalRecord(
        code=code,
        name=name,
        trading_day=confirm_bar.dt.date().isoformat(),
        signal_time=confirm_bar.dt.strftime("%Y-%m-%d %H:%M"),
        breakout_close=confirm_bar.close,
        left2_close=left2.close,
        signal_pct=pct(confirm_bar.close, pre_close),
        speed_pct=speed_pct,
        volume_ratio=volume_ratio,
        right1_time=right1.dt.strftime("%H:%M"),
        right1_close=right1.close,
        right1_gain_pct=gains[0],
        right2_time=right2.dt.strftime("%H:%M"),
        right2_close=right2.close,
        right2_gain_pct=gains[1],
        left1_time=left1.dt.strftime("%H:%M"),
        left1_close=left1.close,
        left1_gain_pct=gains[2],
        left2_time=left2.dt.strftime("%H:%M"),
        left2_gain_pct=gains[3],
    )


def replay_symbol(
    symbol: str,
    name: str,
    start_date: date,
    end_date: date,
    config: dict,
    host: str,
    port: int,
) -> list[ReplaySignalRecord]:
    api = get_thread_api(host, port)
    close_map = load_daily_close_map(api, symbol, start_date, end_date)
    if not close_map:
        return []

    bars = fetch_intraday_bars(api, symbol, start_date, end_date)
    if not bars:
        return []

    signals: list[ReplaySignalRecord] = []
    last_signal_at: datetime | None = None
    cooldown = timedelta(minutes=int(config["signal_cooldown_minutes"]))

    for trading_day, day_bars in sorted(iter_daily_bars(bars, start_date, end_date).items()):
        pre_close = get_prev_close(close_map, trading_day)
        if pre_close <= 0 or len(day_bars) < 8:
            continue
        if max(pct(bar.close, pre_close) for bar in day_bars) < float(config["min_pattern_peak_gain_pct"]):
            continue

        last_signal_at = None
        for end_idx in range(8, len(day_bars) + 1):
            signal = analyze_completed_bars(symbol, name, pre_close, day_bars[:end_idx], config)
            if not signal:
                continue
            signal_dt = datetime.strptime(signal.signal_time, "%Y-%m-%d %H:%M")
            if last_signal_at and signal_dt - last_signal_at < cooldown:
                continue
            signals.append(signal)
            last_signal_at = signal_dt

    return signals


def save_results(records: list[ReplaySignalRecord], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"tq_peak_breakout_replay_{stamp}.csv"
    headers = list(asdict(records[0]).keys()) if records else list(ReplaySignalRecord.__dataclass_fields__.keys())
    with csv_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in records:
            writer.writerow(asdict(row))
    return csv_path


def main() -> int:
    args = parse_args()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    if end_date < start_date:
        raise SystemExit("end-date cannot be earlier than start-date")

    watch_names = load_watch_items(Path(args.watchlist_file).expanduser().resolve())
    config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
    symbols = sorted(watch_names.keys())
    host, port = choose_available_host()

    start_ts = time.time()
    records: list[ReplaySignalRecord] = []
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=max(args.max_workers, 1)) as executor:
        future_map = {
            executor.submit(replay_symbol, symbol, name, start_date, end_date, config, host, port): symbol
            for symbol, name in watch_names.items()
        }
        for index, future in enumerate(as_completed(future_map), start=1):
            symbol = future_map[future]
            try:
                symbol_records = future.result()
                records.extend(symbol_records)
                print(f"[{index}/{len(symbols)}] {symbol}: {len(symbol_records)} hits")
            except Exception as exc:
                failures.append(f"{symbol}: {exc}")
                print(f"[{index}/{len(symbols)}] {symbol}: error {exc}")

    records.sort(key=lambda item: (item.signal_time, item.code))
    csv_path = save_results(records, Path(args.output_dir).expanduser().resolve())

    print()
    print(f"scan_range={start_date}..{end_date}")
    print(f"tdx_host={host}:{port}")
    print(f"symbols={len(symbols)} total_hits={len(records)} failures={len(failures)} elapsed={time.time() - start_ts:.1f}s")
    print(f"csv={csv_path}")
    if failures:
        print("failures_preview=")
        for row in failures[:10]:
            print(row)
    if records:
        print("preview=")
        for row in records[:args.limit]:
            print(
                f"{row.signal_time} | {row.code} {row.name} | 收盘={row.breakout_close:.3f} | "
                f"左二={row.left2_close:.3f} | 涨幅={row.signal_pct:.2f}% | "
                f"涨速={row.speed_pct:.2f}% | 放量={row.volume_ratio:.2f}x"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
