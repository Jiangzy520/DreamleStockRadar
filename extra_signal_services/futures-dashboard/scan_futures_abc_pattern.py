#!/usr/bin/env python3
"""Scan domestic futures 1-minute bars for a simple A/B/C candlestick pattern.

Pattern default:
1. A is bearish: A.close < A.open
2. B is bullish: B.close > B.open
3. C is bearish: C.close < C.open
4. C low stays at or above B low: C.low >= B.low
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import akshare as ak
import pandas as pd
import requests


OUTPUT_SNAPSHOT = "futures_abc_snapshot.csv"
OUTPUT_MATCHES = "futures_abc_matches.csv"
OUTPUT_EVENTS = "futures_abc_match_events.csv"
STALE_MINUTE_BAR_MAX_DAYS = 10
SINA_MINUTE_OPENAPI_URL = "https://stock2.finance.sina.com.cn/futures/api/openapi.php/InnerFuturesNewService.getFewMinLine"
SINA_MINUTE_JSONP_URL = "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/=/InnerFuturesNewService.getFewMinLine"
REQUEST_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Referer": "https://vip.stock.finance.sina.com.cn/",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan all domestic futures continuous contracts for a 1-minute A/B/C pattern."
    )
    parser.add_argument(
        "--trade-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="Trade date for futures variety list, format YYYYMMDD. Default: today.",
    )
    parser.add_argument(
        "--history-date",
        default="",
        help=(
            "Only analyze minute bars for the given date, format YYYYMMDD. "
            "Example: 20260320. Use ALL to keep all returned bars."
        ),
    )
    parser.add_argument(
        "--period",
        default="1",
        choices=["1", "5", "15", "30", "60"],
        help="Minute bar period. Default: 1.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only scan the first N varieties for quick testing. Default: 0 means all.",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to write CSV outputs into. Default: current directory.",
    )
    parser.add_argument(
        "--c-low-ref",
        default="b_high",
        choices=["b_high", "b_low"],
        help=(
            "Deprecated compatibility option kept for old commands. "
            "The current strategy always enforces C.low >= B.low."
        ),
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Rescan in a loop and only print newly found match events.",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Watch mode scan interval in seconds. Default: 60.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help=(
            "Optional comma-separated continuous codes to scan, "
            "for example RB0,HC0,CU0,IF0. Default: all varieties."
        ),
    )
    parser.add_argument(
        "--skip-existing-on-start",
        action="store_true",
        help="In watch mode, seed existing events first and only print future new events.",
    )
    parser.add_argument(
        "--request-sleep",
        type=float,
        default=0.25,
        help="Sleep seconds between symbol requests to reduce Sina rate-limit risk. Default: 0.25.",
    )
    return parser.parse_args()


def get_futures_varieties(trade_date: str) -> pd.DataFrame:
    rules = ak.futures_rule(date=trade_date)
    rules = rules[~rules["品种"].astype(str).str.contains("期权", na=False)].copy()
    rules = rules[["交易所", "品种", "代码"]].drop_duplicates().reset_index(drop=True)
    rules["代码"] = rules["代码"].astype(str).str.upper().str.strip()
    rules = rules[rules["代码"].str.fullmatch(r"[A-Z]+")].copy()
    rules["连续代码"] = rules["代码"] + "0"
    return rules.sort_values(["交易所", "代码"]).reset_index(drop=True)


def is_bearish(bar: pd.Series) -> bool:
    return float(bar["close"]) < float(bar["open"])


def is_bullish(bar: pd.Series) -> bool:
    return float(bar["close"]) > float(bar["open"])


def match_abc_pattern(
    a: pd.Series,
    b: pd.Series,
    c: pd.Series,
    c_low_ref: str = "b_high",
) -> bool:
    _ = c_low_ref  # kept for backward compatibility with existing commands
    return (
        is_bearish(a)
        and is_bullish(b)
        and is_bearish(c)
        and float(c["low"]) >= float(b["low"])
    )


def filter_by_history_date(minute_df: pd.DataFrame, history_date: str) -> pd.DataFrame:
    if not history_date:
        return minute_df
    target_date = pd.to_datetime(history_date, format="%Y%m%d", errors="coerce")
    if pd.isna(target_date):
        raise ValueError(f"invalid history_date: {history_date}")
    return minute_df[minute_df["datetime"].dt.date == target_date.date()].reset_index(drop=True)


def validate_minute_bar_freshness(minute_df: pd.DataFrame, trade_date: str) -> None:
    if minute_df.empty or "datetime" not in minute_df.columns:
        return
    target_trade_date = pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce")
    if pd.isna(target_trade_date):
        return
    latest_bar_at = minute_df["datetime"].max()
    if pd.isna(latest_bar_at):
        return
    stale_days = (target_trade_date.date() - latest_bar_at.date()).days
    if stale_days <= STALE_MINUTE_BAR_MAX_DAYS:
        return
    raise ValueError(
        "stale minute bars: "
        f"latest {latest_bar_at.strftime('%Y-%m-%d %H:%M:%S')} "
        f"({stale_days} days behind trade date {target_trade_date.strftime('%Y-%m-%d')})"
    )


def _normalize_sina_minute_payload(data: object) -> pd.DataFrame:
    if not isinstance(data, list) or not data:
        return pd.DataFrame()

    temp_df = pd.DataFrame(data)
    rename_map = {
        "d": "datetime",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "p": "hold",
    }
    temp_df = temp_df.rename(columns=rename_map)
    required_columns = ["datetime", "open", "high", "low", "close", "volume", "hold"]
    for column in required_columns:
        if column not in temp_df.columns:
            temp_df[column] = pd.NA
    temp_df = temp_df[required_columns].copy()
    for column in ["open", "high", "low", "close", "volume", "hold"]:
        temp_df[column] = pd.to_numeric(temp_df[column], errors="coerce")
    return temp_df


def fetch_minute_bars(symbol: str, period: str) -> pd.DataFrame:
    params = {"symbol": symbol, "type": period}

    response = requests.get(
        SINA_MINUTE_OPENAPI_URL,
        params=params,
        headers=REQUEST_HEADERS,
        timeout=15,
    )
    response.raise_for_status()
    payload = response.json()
    data = (((payload or {}).get("result") or {}).get("data") or [])
    minute_df = _normalize_sina_minute_payload(data)
    if not minute_df.empty:
        return minute_df

    response = requests.get(
        SINA_MINUTE_JSONP_URL,
        params=params,
        headers=REQUEST_HEADERS,
        timeout=15,
    )
    response.raise_for_status()
    text = response.text.strip()
    eq_index = text.find("=")
    if eq_index < 0:
        raise ValueError(f"unexpected Sina payload for {symbol}")
    payload_text = text[eq_index + 1 :].strip().rstrip(";")
    minute_df = _normalize_sina_minute_payload(json.loads(payload_text))
    if minute_df.empty:
        raise ValueError(f"empty minute bars for {symbol}")
    return minute_df


def analyze_symbol(
    symbol: str,
    period: str,
    c_low_ref: str,
    history_date: str,
    trade_date: str,
) -> tuple[dict, list[dict]]:
    minute_df = fetch_minute_bars(symbol=symbol, period=period)
    if minute_df.empty or len(minute_df) < 3:
        raise ValueError(f"not enough minute bars for {symbol}")

    minute_df = minute_df.copy()
    minute_df["datetime"] = pd.to_datetime(minute_df["datetime"], errors="coerce")
    minute_df = minute_df.dropna(subset=["datetime"]).reset_index(drop=True)
    validate_minute_bar_freshness(minute_df=minute_df, trade_date=trade_date)
    minute_df = filter_by_history_date(minute_df, history_date=history_date)
    if len(minute_df) < 3:
        raise ValueError(f"not enough valid minute bars for {symbol}")

    match_indexes = []
    event_rows = []
    for idx in range(2, len(minute_df)):
        a = minute_df.iloc[idx - 2]
        b = minute_df.iloc[idx - 1]
        c = minute_df.iloc[idx]
        if match_abc_pattern(a, b, c, c_low_ref=c_low_ref):
            match_indexes.append(idx)
            event_rows.append(
                {
                    "连续代码": symbol,
                    "A_time": a["datetime"],
                    "A_open": float(a["open"]),
                    "A_high": float(a["high"]),
                    "A_low": float(a["low"]),
                    "A_close": float(a["close"]),
                    "B_time": b["datetime"],
                    "B_open": float(b["open"]),
                    "B_high": float(b["high"]),
                    "B_low": float(b["low"]),
                    "B_close": float(b["close"]),
                    "C_time": c["datetime"],
                    "C_open": float(c["open"]),
                    "C_high": float(c["high"]),
                    "C_low": float(c["low"]),
                    "C_close": float(c["close"]),
                }
            )

    latest_idx = len(minute_df) - 1
    latest_three_match = bool(match_indexes and match_indexes[-1] == latest_idx)
    last_match_idx: Optional[int] = match_indexes[-1] if match_indexes else None

    latest_a = minute_df.iloc[-3]
    latest_b = minute_df.iloc[-2]
    latest_c = minute_df.iloc[-1]

    result = {
        "连续代码": symbol,
        "bars_count": len(minute_df),
        "latest_bar_time": latest_c["datetime"],
        "latest_price": float(latest_c["close"]),
        "latest_three_match": latest_three_match,
        "match_count": len(match_indexes),
        "last_match_time": (
            minute_df.iloc[last_match_idx]["datetime"] if last_match_idx is not None else pd.NaT
        ),
        "A_time": latest_a["datetime"],
        "A_open": float(latest_a["open"]),
        "A_high": float(latest_a["high"]),
        "A_low": float(latest_a["low"]),
        "A_close": float(latest_a["close"]),
        "B_time": latest_b["datetime"],
        "B_open": float(latest_b["open"]),
        "B_high": float(latest_b["high"]),
        "B_low": float(latest_b["low"]),
        "B_close": float(latest_b["close"]),
        "C_time": latest_c["datetime"],
        "C_open": float(latest_c["open"]),
        "C_high": float(latest_c["high"]),
        "C_low": float(latest_c["low"]),
        "C_close": float(latest_c["close"]),
    }
    return result, event_rows


def resolve_history_date(args: argparse.Namespace) -> str:
    if str(args.history_date).upper() == "ALL":
        return ""
    if args.history_date:
        return args.history_date
    if args.watch:
        return args.trade_date
    return ""


def filter_varieties_by_symbols(varieties: pd.DataFrame, symbols_arg: str) -> pd.DataFrame:
    if not symbols_arg.strip():
        return varieties
    symbols = {item.strip().upper() for item in symbols_arg.split(",") if item.strip()}
    return varieties[varieties["连续代码"].isin(symbols)].reset_index(drop=True)


def build_event_key(event: pd.Series | dict) -> str:
    def fmt(value: object) -> str:
        if pd.isna(value):
            return ""
        return pd.Timestamp(value).strftime("%Y-%m-%d %H:%M:%S")

    return "|".join(
        [
            str(event["连续代码"]),
            fmt(event["A_time"]),
            fmt(event["B_time"]),
            fmt(event["C_time"]),
        ]
    )


def scan_once(
    args: argparse.Namespace,
    varieties: pd.DataFrame,
    verbose: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    history_date = resolve_history_date(args)
    rows = []
    event_rows = []

    for index, (_, row) in enumerate(varieties.iterrows()):
        if index > 0 and args.request_sleep > 0:
            time.sleep(args.request_sleep)
        symbol = row["连续代码"]
        try:
            analyzed, symbol_events = analyze_symbol(
                symbol=symbol,
                period=args.period,
                c_low_ref=args.c_low_ref,
                history_date=history_date,
                trade_date=args.trade_date,
            )
            analyzed["交易所"] = row["交易所"]
            analyzed["品种"] = row["品种"]
            analyzed["代码"] = row["代码"]
            rows.append(analyzed)
            for event in symbol_events:
                event["交易所"] = row["交易所"]
                event["品种"] = row["品种"]
                event["代码"] = row["代码"]
                event_rows.append(event)
            if verbose:
                print(
                    f"[OK] {symbol:<5} latest={analyzed['latest_price']:<10} "
                    f"latest_three_match={analyzed['latest_three_match']}"
                )
        except Exception as exc:  # noqa: BLE001
            rows.append(
                {
                    "交易所": row["交易所"],
                    "品种": row["品种"],
                    "代码": row["代码"],
                    "连续代码": symbol,
                    "error": str(exc),
                }
            )
            if verbose:
                print(f"[ERR] {symbol:<5} {exc}")

    result_df = pd.DataFrame(rows)
    scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not result_df.empty:
        result_df["scan_time"] = scan_time

    if "latest_three_match" in result_df.columns:
        matches_df = result_df[result_df["latest_three_match"] == True].copy()
    else:
        matches_df = pd.DataFrame(columns=result_df.columns)
    if not matches_df.empty:
        matches_df = matches_df.sort_values(["交易所", "代码"]).reset_index(drop=True)

    events_df = pd.DataFrame(event_rows)
    if not events_df.empty:
        events_df["scan_time"] = scan_time
        events_df = events_df.sort_values(["交易所", "代码", "C_time"]).reset_index(drop=True)

    return result_df, matches_df, events_df


def write_outputs(
    output_dir: Path,
    result_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> tuple[Path, Path, Path]:
    snapshot_path = output_dir / OUTPUT_SNAPSHOT
    matches_path = output_dir / OUTPUT_MATCHES
    events_path = output_dir / OUTPUT_EVENTS
    result_df.to_csv(snapshot_path, index=False, encoding="utf-8-sig")
    matches_df.to_csv(matches_path, index=False, encoding="utf-8-sig")
    events_df.to_csv(events_path, index=False, encoding="utf-8-sig")
    return snapshot_path, matches_path, events_path


def scan_error_count(result_df: pd.DataFrame) -> int:
    if result_df.empty or "error" not in result_df.columns:
        return 0
    return int(result_df["error"].fillna("").astype(str).str.strip().ne("").sum())


def scan_all_failed(result_df: pd.DataFrame) -> bool:
    return len(result_df) > 0 and scan_error_count(result_df) == len(result_df)


def print_scan_summary(
    snapshot_path: Path,
    matches_path: Path,
    events_path: Path,
    result_df: pd.DataFrame,
    matches_df: pd.DataFrame,
    events_df: pd.DataFrame,
) -> None:
    print()
    print(f"Snapshot saved to: {snapshot_path}")
    print(f"Matches saved to:  {matches_path}")
    print(f"Events saved to:   {events_path}")
    print(f"Scanned varieties: {len(result_df)}")
    print(f"Matched latest A/B/C: {len(matches_df)}")
    print(f"Matched history A/B/C: {len(events_df)}")
    error_count = scan_error_count(result_df)
    if len(result_df) > 0 and error_count == len(result_df):
        print("All symbols failed in this run. The Sina source may be rate-limiting your IP.")


def print_new_events(events_df: pd.DataFrame, seen_event_keys: set[str]) -> int:
    if events_df.empty:
        return 0

    new_rows = []
    for _, event in events_df.iterrows():
        event_key = build_event_key(event)
        if event_key in seen_event_keys:
            continue
        seen_event_keys.add(event_key)
        new_rows.append(event)

    for event in new_rows:
        print(
            "[NEW] "
            f"{event['连续代码']:<5} {event['交易所']} {event['品种']} "
            f"A={pd.Timestamp(event['A_time']).strftime('%H:%M:%S')} "
            f"B={pd.Timestamp(event['B_time']).strftime('%H:%M:%S')} "
            f"C={pd.Timestamp(event['C_time']).strftime('%H:%M:%S')} "
            f"B.low={event['B_low']} C.low={event['C_low']}"
        )

    return len(new_rows)


def run_watch(args: argparse.Namespace, varieties: pd.DataFrame, output_dir: Path) -> None:
    seen_event_keys: set[str] = set()
    cycle = 0
    history_date = resolve_history_date(args)
    print(
        f"Watch mode started: interval={args.interval}s, "
        f"trade_date={args.trade_date}, history_date={history_date or 'ALL'}"
    )

    while True:
        cycle += 1
        cycle_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            result_df, matches_df, events_df = scan_once(args=args, varieties=varieties, verbose=False)
            error_count = scan_error_count(result_df)
            all_failed = scan_all_failed(result_df)
            if all_failed:
                print(
                    f"[CYCLE {cycle}] {cycle_time} scanned={len(result_df)} "
                    f"history_matches=0 new_matches=0 errors={error_count}"
                )
                print("[WARN] All symbols failed. The Sina source may be rate-limiting your IP. Keeping previous outputs.")
                continue
            if cycle == 1 and args.skip_existing_on_start and not events_df.empty:
                for _, event in events_df.iterrows():
                    seen_event_keys.add(build_event_key(event))
            snapshot_path, matches_path, events_path = write_outputs(
                output_dir=output_dir,
                result_df=result_df,
                matches_df=matches_df,
                events_df=events_df,
            )
            new_count = 0
            if not (cycle == 1 and args.skip_existing_on_start):
                new_count = print_new_events(events_df=events_df, seen_event_keys=seen_event_keys)
            print(
                f"[CYCLE {cycle}] {cycle_time} scanned={len(result_df)} "
                f"history_matches={len(events_df)} new_matches={new_count} errors={error_count}"
            )
            print(f"[FILES] {snapshot_path} | {matches_path} | {events_path}")
        except KeyboardInterrupt:
            print("\nWatch stopped.")
            break
        except Exception as exc:  # noqa: BLE001
            print(f"[CYCLE {cycle}] {cycle_time} failed: {exc}")

        try:
            time.sleep(args.interval)
        except KeyboardInterrupt:
            print("\nWatch stopped.")
            break


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    varieties = get_futures_varieties(args.trade_date)
    varieties = filter_varieties_by_symbols(varieties=varieties, symbols_arg=args.symbols)
    if args.limit > 0:
        varieties = varieties.head(args.limit).copy()

    if args.watch:
        run_watch(args=args, varieties=varieties, output_dir=output_dir)
        return

    result_df, matches_df, events_df = scan_once(args=args, varieties=varieties, verbose=True)
    if scan_all_failed(result_df):
        print("All symbols failed in this run. The Sina source may be rate-limiting your IP. Keeping previous outputs.")
        return
    snapshot_path, matches_path, events_path = write_outputs(
        output_dir=output_dir,
        result_df=result_df,
        matches_df=matches_df,
        events_df=events_df,
    )
    print_scan_summary(
        snapshot_path=snapshot_path,
        matches_path=matches_path,
        events_path=events_path,
        result_df=result_df,
        matches_df=matches_df,
        events_df=events_df,
    )


if __name__ == "__main__":
    main()
