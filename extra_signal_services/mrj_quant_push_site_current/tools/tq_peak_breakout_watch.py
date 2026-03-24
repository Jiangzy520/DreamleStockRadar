#!/usr/bin/env python3
"""
Minimal TQ strategy for 1-minute local-peak breakout alerts.

Strategy assumptions:
1. "right1/right2/left1/left2" are the latest four confirmed local peaks
   in chronological order on completed 1-minute bars.
2. Peak detection uses 1-minute close, not high.
3. Breakout confirmation requires the latest completed 1-minute close to
   cross above left2.close.
4. Speed is measured as 1-minute close delta divided by yesterday close.
5. Volume expansion compares the breakout bar volume to the average of the
   previous N completed 1-minute bars.

Data source:
- Quotes and 1-minute bars: Sina public interfaces.
- Alert/log channel: TDX TPythClient.dll via TQ strategy runtime.

The script avoids numpy/pandas so it can run with a minimal Windows Python.
"""

from __future__ import annotations

import ctypes
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import quote as url_quote
from urllib.request import Request, urlopen


CONFIG_PATH = Path(__file__).with_suffix(".json")
WATCHLIST_PATH = Path(__file__).with_name("tq_peak_breakout_watchlist.txt")
DEFAULT_SOURCE_LIST = Path(r"Z:\home\jzy\桌面\量化\图片2_股票去重清单_复核版.txt")
DLL_PATH = Path(__file__).resolve().parents[1] / "TPythClient.dll"

HTTP_HEADERS = {
    "Referer": "https://finance.sina.com.cn",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}

DEFAULT_CONFIG = {
    "watchlist_file": str(WATCHLIST_PATH),
    "source_watchlist_file": str(DEFAULT_SOURCE_LIST),
    "quote_chunk_size": 180,
    "scan_interval_seconds": 5,
    "scan_after_second": 6,
    "quote_prefilter_pct": 1.50,
    "history_workers": 16,
    "lookback_bars": 120,
    "peak_radius": 1,
    "min_peak_gap_bars": 2,
    "min_pullback_pct": 0.30,
    "min_pattern_peak_gain_pct": 2.00,
    "breakout_min_speed_pct": 0.20,
    "breakout_min_volume_ratio": 1.80,
    "breakout_volume_window": 5,
    "signal_cooldown_minutes": 20,
    "debug": True,
}


@dataclass
class WatchItem:
    code: str
    symbol: str
    name: str


@dataclass
class Quote:
    code: str
    name: str
    dt: datetime
    pre_close: float
    price: float
    total_volume: float
    total_amount: float


@dataclass
class MinuteBar:
    dt: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


@dataclass
class Signal:
    code: str
    name: str
    signal_time: datetime
    pre_close: float
    breakout_close: float
    breakout_volume: float
    left2_close: float
    speed_pct: float
    volume_ratio: float
    right1_gain_pct: float
    right2_gain_pct: float
    left1_gain_pct: float
    left2_gain_pct: float
    peaks: list[MinuteBar]


def python_version_number() -> int:
    return int(f"{sys.version_info.major}{sys.version_info.minor}")


def pct(value: float, base: float) -> float:
    if base <= 0:
        return 0.0
    return (value / base - 1.0) * 100.0


def floor_minute(value: datetime) -> datetime:
    return value.replace(second=0, microsecond=0)


def infer_suffix(code: str) -> str:
    if code.startswith(("6", "9", "5")):
        return "SH"
    if code.startswith(("0", "2", "3")):
        return "SZ"
    if code.startswith(("4", "8")):
        return "BJ"
    raise ValueError(f"Unsupported code prefix: {code}")


def to_sina_symbol(code: str) -> str:
    if code.startswith(("6", "9", "5")):
        return f"sh{code}"
    if code.startswith(("0", "2", "3")):
        return f"sz{code}"
    if code.startswith(("4", "8")):
        return f"bj{code}"
    return code


def is_trading_time(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False

    current = now.time()
    return (
        dt_time(9, 30) <= current <= dt_time(11, 30)
        or dt_time(13, 0) <= current <= dt_time(15, 0)
    )


def make_request(url: str, encoding: str) -> str:
    request = Request(url, headers=HTTP_HEADERS)
    with urlopen(request, timeout=15) as response:
        return response.read().decode(encoding, errors="ignore")


def load_json(path: Path, default_value: dict) -> dict:
    if not path.exists():
        path.write_text(
            json.dumps(default_value, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return default_value.copy()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_value.copy()

    merged = default_value.copy()
    merged.update(data)
    return merged


def ensure_watchlist_file(config: dict) -> Path:
    watchlist_path = Path(str(config["watchlist_file"]))
    if watchlist_path.exists():
        return watchlist_path

    source_path = Path(str(config.get("source_watchlist_file", "")))
    if not source_path.exists():
        raise FileNotFoundError(
            f"Watchlist file missing: {watchlist_path}; source file missing: {source_path}"
        )

    lines: list[str] = []
    for raw in source_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = raw.strip()
        if len(text) < 6 or not text[:6].isdigit():
            continue
        code = text[:6]
        name = text[6:].strip()
        symbol = f"{code}.{infer_suffix(code)}"
        lines.append(f"{symbol},{name}")

    watchlist_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return watchlist_path


def load_watchlist(config: dict) -> dict[str, WatchItem]:
    path = ensure_watchlist_file(config)
    result: dict[str, WatchItem] = {}
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = raw.strip()
        if not text or text.startswith("#"):
            continue

        parts = [segment.strip() for segment in text.split(",", 1)]
        symbol = parts[0]
        if "." not in symbol:
            if len(symbol) != 6 or not symbol.isdigit():
                continue
            symbol = f"{symbol}.{infer_suffix(symbol)}"
        code, _suffix = symbol.split(".", 1)
        name = parts[1] if len(parts) > 1 else code
        result[code] = WatchItem(code=code, symbol=symbol, name=name)
    return result


class TdxBridge:
    def __init__(self, run_mode: int = 0) -> None:
        if not DLL_PATH.exists():
            raise FileNotFoundError(f"Missing DLL: {DLL_PATH}")

        self.dll = ctypes.CDLL(str(DLL_PATH))
        self.dll.InitConnect.restype = ctypes.c_char_p
        self.dll.SetResToMain.restype = ctypes.c_char_p
        self.dll.CloseConnect.restype = None
        self.run_mode = run_mode
        self.run_id = -1
        self.connected = False

    def connect(self) -> None:
        script_path = str(Path(__file__).resolve()).encode("utf-8")
        dll_path = str(DLL_PATH).encode("utf-8")
        raw = self.dll.InitConnect(
            script_path,
            dll_path,
            self.run_mode,
            python_version_number(),
        )
        if not raw:
            raise RuntimeError("InitConnect returned empty pointer")

        payload = json.loads(raw.decode("utf-8"))
        if payload.get("ErrorId") not in {"0", "12"}:
            raise RuntimeError(f"InitConnect failed: {payload}")

        self.run_id = int(payload.get("run_id", "-1"))
        if self.run_id < 0:
            raise RuntimeError(f"Invalid run_id: {payload}")
        self.connected = True

    def close(self) -> None:
        if self.connected:
            self.dll.CloseConnect(self.run_id, self.run_mode)
            self.connected = False

    def _send_raw(self, payload: str) -> dict:
        if not self.connected:
            raise RuntimeError("TdxBridge not connected")

        raw = self.dll.SetResToMain(
            self.run_id,
            self.run_mode,
            payload.encode("utf-8"),
            30000,
        )
        if not raw:
            raise RuntimeError(f"SetResToMain returned empty pointer: {payload[:40]}")
        return json.loads(raw.decode("utf-8"))

    def send_message(self, message: str) -> dict:
        return self._send_raw(f"MSG||{message}")

    def send_warn(
        self,
        stock: str,
        signal_time: datetime,
        price: float,
        pre_close: float,
        volume: float,
        reason: str,
    ) -> dict:
        payload = "|".join(
            [
                stock,
                signal_time.strftime("%Y-%m-%d %H:%M:%S"),
                f"{price:.3f}",
                f"{pre_close:.3f}",
                str(int(volume)),
                "0",
                "0",
                reason[:25],
            ]
        )
        return self._send_raw(f"WARN||{payload}")


def fetch_quotes(codes: Iterable[str], chunk_size: int) -> dict[str, Quote]:
    code_list = list(codes)
    result: dict[str, Quote] = {}
    for offset in range(0, len(code_list), max(chunk_size, 1)):
        chunk = code_list[offset:offset + chunk_size]
        sina_symbols = [to_sina_symbol(code) for code in chunk]
        url = f"https://hq.sinajs.cn/list={','.join(sina_symbols)}"
        text = make_request(url, "gbk")

        for line in text.strip().split(";"):
            if '="' not in line:
                continue
            left, _, right = line.partition('="')
            payload = right.rstrip('"').strip()
            if not payload:
                continue

            symbol = left.rsplit("_", 1)[-1]
            code = symbol[2:]
            fields = payload.split(",")
            if len(fields) < 32:
                continue

            try:
                quote_dt = datetime.strptime(
                    f"{fields[30]} {fields[31]}",
                    "%Y-%m-%d %H:%M:%S",
                )
                quote = Quote(
                    code=code,
                    name=fields[0].strip() or code,
                    dt=quote_dt,
                    pre_close=float(fields[2] or 0),
                    price=float(fields[3] or 0),
                    total_volume=float(fields[8] or 0),
                    total_amount=float(fields[9] or 0),
                )
                if quote.pre_close > 0 and quote.price > 0:
                    result[code] = quote
            except Exception:
                continue
    return result


def fetch_intraday_bars(code: str) -> list[MinuteBar]:
    symbol = to_sina_symbol(code)
    url = (
        "https://quotes.sina.cn/cn/api/jsonp_v2.php/"
        "var%20_tdx_peak_breakout=/CN_MarketData.getKLineData"
        f"?symbol={url_quote(symbol)}&scale=1&ma=no&datalen=240"
    )
    text = make_request(url, "utf-8")
    match = re.search(r"=\((.*)\);?\s*$", text.strip(), re.S)
    if not match:
        return []

    try:
        rows = json.loads(match.group(1))
    except Exception:
        return []

    bars: list[MinuteBar] = []
    for row in rows:
        try:
            dt_value = datetime.strptime(row["day"], "%Y-%m-%d %H:%M:%S")
            open_price = float(row.get("open", 0) or 0)
            close_price = float(row.get("close", 0) or 0)
            high_price = float(row.get("high", 0) or 0)
            low_price = float(row.get("low", 0) or 0)
            volume = float(row.get("volume", 0) or 0)
            amount = float(row.get("amount", 0) or 0)
            if open_price <= 0:
                open_price = close_price
            if high_price <= 0:
                high_price = close_price
            if low_price <= 0:
                low_price = close_price
            if close_price <= 0:
                continue

            bars.append(
                MinuteBar(
                    dt=floor_minute(dt_value),
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    amount=amount,
                )
            )
        except Exception:
            continue

    bars.sort(key=lambda item: item.dt)
    return bars


def local_peak_indices(bars: list[MinuteBar], radius: int) -> list[int]:
    result: list[int] = []
    if len(bars) < radius * 2 + 1:
        return result

    for idx in range(radius, len(bars) - radius):
        value = bars[idx].close
        left = bars[idx - radius:idx]
        right = bars[idx + 1:idx + radius + 1]
        if all(value >= bar.close for bar in left + right) and (
            any(value > bar.close for bar in left) or any(value > bar.close for bar in right)
        ):
            result.append(idx)
    return result


def filter_peaks(
    bars: list[MinuteBar],
    peak_indices: list[int],
    min_gap_bars: int,
    min_pullback_pct: float,
) -> list[int]:
    result: list[int] = []
    for idx in peak_indices:
        if not result:
            result.append(idx)
            continue

        prev_idx = result[-1]
        if idx - prev_idx < min_gap_bars:
            if bars[idx].close >= bars[prev_idx].close:
                result[-1] = idx
            continue

        trough_window = bars[prev_idx + 1:idx]
        if not trough_window:
            continue

        trough_candidates = [bar.low for bar in trough_window if bar.low > 0]
        if not trough_candidates:
            continue
        trough = min(trough_candidates)
        pullback_pct = (bars[prev_idx].close - trough) / bars[prev_idx].close * 100 if bars[prev_idx].close > 0 else 0.0
        if pullback_pct < min_pullback_pct:
            continue

        result.append(idx)
    return result


def analyze_signal(
    code: str,
    name: str,
    quote: Quote,
    bars: list[MinuteBar],
    now: datetime,
    config: dict,
) -> Signal | None:
    current_minute = floor_minute(now)
    completed = [bar for bar in bars if bar.dt < current_minute and bar.dt.date() == now.date()]
    lookback_bars = int(config["lookback_bars"])
    if len(completed) < max(25, lookback_bars // 3):
        return None

    completed = completed[-lookback_bars:]
    if len(completed) < 8:
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
        pct(right1.close, quote.pre_close),
        pct(right2.close, quote.pre_close),
        pct(left1.close, quote.pre_close),
        pct(left2.close, quote.pre_close),
    ]
    if max(gains) < float(config["min_pattern_peak_gain_pct"]):
        return None

    breakout_level = left2.close
    if previous_bar.close > breakout_level:
        return None
    if confirm_bar.close <= breakout_level:
        return None

    volume_window = completed[max(0, len(completed) - 1 - int(config["breakout_volume_window"])):len(completed) - 1]
    if not volume_window:
        return None

    avg_volume = sum(bar.volume for bar in volume_window) / len(volume_window)
    if avg_volume <= 0:
        return None

    volume_ratio = confirm_bar.volume / avg_volume
    if volume_ratio < float(config["breakout_min_volume_ratio"]):
        return None

    speed_pct = (confirm_bar.close - previous_bar.close) / quote.pre_close * 100 if quote.pre_close > 0 else 0.0
    if speed_pct < float(config["breakout_min_speed_pct"]):
        return None

    return Signal(
        code=code,
        name=name,
        signal_time=confirm_bar.dt,
        pre_close=quote.pre_close,
        breakout_close=confirm_bar.close,
        breakout_volume=confirm_bar.volume,
        left2_close=left2.close,
        speed_pct=speed_pct,
        volume_ratio=volume_ratio,
        right1_gain_pct=gains[0],
        right2_gain_pct=gains[1],
        left1_gain_pct=gains[2],
        left2_gain_pct=gains[3],
        peaks=[right1, right2, left1, left2],
    )


class PeakBreakoutWatcher:
    def __init__(self) -> None:
        self.config = load_json(CONFIG_PATH, DEFAULT_CONFIG)
        self.watchlist = load_watchlist(self.config)
        self.bridge = TdxBridge(run_mode=0)
        self.last_scan_key = ""
        self.active_day: date | None = None
        self.sent_keys: set[str] = set()
        self.last_sent_at: dict[str, datetime] = {}

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        safe = f"[{timestamp}] {message}"
        try:
            self.bridge.send_message(safe.replace("\n", " | "))
        except Exception:
            pass
        print(safe)

    def reset_daily_state_if_needed(self, now: datetime) -> None:
        if self.active_day != now.date():
            self.active_day = now.date()
            self.sent_keys.clear()
            self.last_sent_at.clear()

    def should_send(self, signal: Signal) -> bool:
        key = f"{signal.code}:{signal.signal_time:%Y%m%d%H%M}:{signal.left2_close:.3f}"
        if key in self.sent_keys:
            return False

        last_time = self.last_sent_at.get(signal.code)
        cooldown = timedelta(minutes=int(self.config["signal_cooldown_minutes"]))
        if last_time and signal.signal_time - last_time < cooldown:
            return False
        return True

    def mark_sent(self, signal: Signal) -> None:
        key = f"{signal.code}:{signal.signal_time:%Y%m%d%H%M}:{signal.left2_close:.3f}"
        self.sent_keys.add(key)
        self.last_sent_at[signal.code] = signal.signal_time

    def format_signal_message(self, signal: Signal) -> str:
        right1, right2, left1, left2 = signal.peaks
        return (
            f"{signal.name} {signal.code} 命中 1分突破左二 | "
            f"右一={right1.dt:%H:%M}/{right1.close:.3f}/{signal.right1_gain_pct:.2f}% | "
            f"右二={right2.dt:%H:%M}/{right2.close:.3f}/{signal.right2_gain_pct:.2f}% | "
            f"左一={left1.dt:%H:%M}/{left1.close:.3f}/{signal.left1_gain_pct:.2f}% | "
            f"左二={left2.dt:%H:%M}/{left2.close:.3f}/{signal.left2_gain_pct:.2f}% | "
            f"确认收盘={signal.breakout_close:.3f} | "
            f"1分涨速={signal.speed_pct:.2f}% | "
            f"放量={signal.volume_ratio:.2f}x"
        )

    def scan_once(self, now: datetime) -> None:
        quotes = fetch_quotes(
            self.watchlist.keys(),
            int(self.config["quote_chunk_size"]),
        )
        if not quotes:
            self.log("未获取到行情数据，跳过本轮扫描")
            return

        prefilter_pct = float(self.config["quote_prefilter_pct"])
        candidates: list[tuple[WatchItem, Quote]] = []
        for code, item in self.watchlist.items():
            quote = quotes.get(code)
            if not quote:
                continue
            if pct(quote.price, quote.pre_close) < prefilter_pct:
                continue
            candidates.append((item, quote))

        if self.config.get("debug", True):
            self.log(
                f"扫描开始: 全部={len(self.watchlist)} 预筛后={len(candidates)}"
            )

        alerts: list[Signal] = []
        max_workers = max(int(self.config["history_workers"]), 1)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map = {
                executor.submit(fetch_intraday_bars, item.code): (item, quote)
                for item, quote in candidates
            }
            for future in as_completed(future_map):
                item, quote = future_map[future]
                try:
                    bars = future.result()
                except Exception:
                    continue

                signal = analyze_signal(
                    item.code,
                    item.name or quote.name,
                    quote,
                    bars,
                    now,
                    self.config,
                )
                if signal and self.should_send(signal):
                    alerts.append(signal)

        alerts.sort(key=lambda item: (item.signal_time, item.code))
        for signal in alerts:
            self.mark_sent(signal)
            short_reason = "1分收盘突破左二 放量涨速达标"
            try:
                self.bridge.send_warn(
                    stock=f"{signal.code}.{infer_suffix(signal.code)}",
                    signal_time=signal.signal_time,
                    price=signal.breakout_close,
                    pre_close=signal.pre_close,
                    volume=signal.breakout_volume,
                    reason=short_reason,
                )
            except Exception:
                pass
            self.log(self.format_signal_message(signal))

        if self.config.get("debug", True):
            self.log(f"扫描完成: 预警={len(alerts)}")

    def run(self) -> None:
        self.bridge.connect()
        self.log(
            f"策略启动: 股票池={len(self.watchlist)} 扫描间隔={self.config['scan_interval_seconds']}秒"
        )
        while True:
            now = datetime.now()
            self.reset_daily_state_if_needed(now)

            if not is_trading_time(now):
                time.sleep(max(int(self.config["scan_interval_seconds"]), 5))
                continue

            if now.second < int(self.config["scan_after_second"]):
                time.sleep(1)
                continue

            scan_key = now.strftime("%Y%m%d%H%M")
            if scan_key == self.last_scan_key:
                time.sleep(1)
                continue

            self.last_scan_key = scan_key
            try:
                self.scan_once(now)
            except Exception as exc:
                self.log(f"扫描异常: {exc}")

            time.sleep(max(int(self.config["scan_interval_seconds"]), 1))


def main() -> int:
    watcher = PeakBreakoutWatcher()
    try:
        watcher.run()
    finally:
        watcher.bridge.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
