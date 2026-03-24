#!/usr/bin/env python3
"""Read MyQuant futures tick data, aggregate to 1-minute bars, then scan the A/B/C pattern."""

from __future__ import annotations

import argparse
import sys

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate MyQuant futures tick data into 1-minute bars and scan the A/B/C pattern."
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
        default=5000,
        help="Number of ticks to fetch. Adjust this to cover the period you need.",
    )
    parser.add_argument(
        "--end-time",
        default=None,
        help="Optional end time, for example '2026-03-20 15:00:00'",
    )
    parser.add_argument(
        "--history-date",
        default="",
        help="Only keep 1-minute bars from this date, format YYYYMMDD.",
    )
    return parser.parse_args()


def is_bearish(bar: pd.Series) -> bool:
    return float(bar["close"]) < float(bar["open"])


def is_bullish(bar: pd.Series) -> bool:
    return float(bar["close"]) > float(bar["open"])


def match_abc_pattern(a: pd.Series, b: pd.Series, c: pd.Series) -> bool:
    return (
        is_bearish(a)
        and is_bullish(b)
        and is_bearish(c)
        and float(c["low"]) >= float(b["low"])
    )


def detect_time_column(df: pd.DataFrame) -> str:
    for candidate in ["created_at", "eob", "bob", "datetime"]:
        if candidate in df.columns:
            return candidate
    raise ValueError(f"Unable to find tick timestamp column, available columns: {list(df.columns)}")


def detect_price_column(df: pd.DataFrame) -> str:
    for candidate in ["price", "last_price", "last", "close"]:
        if candidate in df.columns:
            return candidate
    raise ValueError(f"Unable to find tick price column, available columns: {list(df.columns)}")


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
    ticks = history_n(
        symbol=args.symbol,
        frequency="tick",
        count=args.count,
        end_time=args.end_time,
        df=True,
    )
    if ticks is None or len(ticks) == 0:
        print("No tick data returned.")
        return 0

    ticks = ticks.copy()
    time_col = detect_time_column(ticks)
    price_col = detect_price_column(ticks)

    ticks[time_col] = pd.to_datetime(ticks[time_col], errors="coerce")
    ticks[price_col] = pd.to_numeric(ticks[price_col], errors="coerce")
    ticks = ticks.dropna(subset=[time_col, price_col]).sort_values(time_col).reset_index(drop=True)
    if ticks.empty:
        print("No valid tick rows after cleanup.")
        return 0

    minute_df = (
        ticks.set_index(time_col)[price_col]
        .resample("1min")
        .ohlc()
        .rename(columns={"open": "open", "high": "high", "low": "low", "close": "close"})
        .dropna()
        .reset_index()
        .rename(columns={time_col: "datetime"})
    )

    if "volume" in ticks.columns:
        volume_df = (
            pd.to_numeric(ticks["volume"], errors="coerce")
            .fillna(0)
            .groupby(ticks[time_col].dt.floor("1min"))
            .last()
            .reset_index()
        )
        volume_df.columns = ["datetime", "volume"]
        minute_df = minute_df.merge(volume_df, on="datetime", how="left")
    else:
        minute_df["volume"] = 0

    if args.history_date:
        target_date = pd.to_datetime(args.history_date, format="%Y%m%d", errors="raise").date()
        minute_df = minute_df[minute_df["datetime"].dt.date == target_date].reset_index(drop=True)

    if len(minute_df) < 3:
        print("Not enough 1-minute bars after aggregation.")
        return 0

    matches: list[dict] = []
    for idx in range(2, len(minute_df)):
        a = minute_df.iloc[idx - 2]
        b = minute_df.iloc[idx - 1]
        c = minute_df.iloc[idx]
        if match_abc_pattern(a, b, c):
            matches.append(
                {
                    "symbol": args.symbol,
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

    print("1-minute bars:")
    print(minute_df.tail(10).to_string(index=False))
    print()
    print(f"matches: {len(matches)}")

    if matches:
        print(pd.DataFrame(matches).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
