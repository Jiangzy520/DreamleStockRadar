#!/usr/bin/env python3
"""Minimal akshare futures 1-minute A/B/C pattern scanner."""

from __future__ import annotations

from datetime import datetime

import akshare as ak
import pandas as pd


TRADE_DATE = "20260320"


def get_symbols(trade_date: str) -> list[tuple[str, str, str, str]]:
    df = ak.futures_rule(date=trade_date)
    df = df[~df["品种"].astype(str).str.contains("期权", na=False)].copy()
    df = df[["交易所", "品种", "代码"]].drop_duplicates().reset_index(drop=True)
    df["代码"] = df["代码"].astype(str).str.upper().str.strip()
    df = df[df["代码"].str.fullmatch(r"[A-Z]+")].copy()
    df["连续代码"] = df["代码"] + "0"
    return list(df[["交易所", "品种", "代码", "连续代码"]].itertuples(index=False, name=None))


def scan_one(symbol: str, history_date: str) -> list[dict]:
    df = ak.futures_zh_minute_sina(symbol=symbol, period="1")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).reset_index(drop=True)
    target_date = datetime.strptime(history_date, "%Y%m%d").date()
    df = df[df["datetime"].dt.date == target_date].reset_index(drop=True)

    events = []
    for i in range(2, len(df)):
        a = df.iloc[i - 2]
        b = df.iloc[i - 1]
        c = df.iloc[i]
        if not (a["close"] < a["open"]):
            continue
        if not (b["close"] > b["open"]):
            continue
        if not (c["close"] < c["open"]):
            continue
        if not (c["low"] >= b["low"]):
            continue
        events.append(
            {
                "连续代码": symbol,
                "A_time": a["datetime"],
                "B_time": b["datetime"],
                "C_time": c["datetime"],
                "A_high": a["high"],
                "B_high": b["high"],
                "B_low": b["low"],
                "C_low": c["low"],
            }
        )
    return events


def main() -> None:
    all_events = []
    for exchange, name, code, symbol in get_symbols(TRADE_DATE):
        try:
            events = scan_one(symbol, TRADE_DATE)
        except Exception as exc:  # noqa: BLE001
            print(f"[ERR] {symbol:<5} {exc}")
            continue
        for event in events:
            event["交易所"] = exchange
            event["品种"] = name
            event["代码"] = code
            all_events.append(event)
        print(f"[OK] {symbol:<5} matches={len(events)}")

    result = pd.DataFrame(all_events)
    if not result.empty:
        result = result.sort_values(["交易所", "代码", "C_time"]).reset_index(drop=True)
    print()
    print(result.to_string(index=False) if not result.empty else "No matches.")


if __name__ == "__main__":
    main()
