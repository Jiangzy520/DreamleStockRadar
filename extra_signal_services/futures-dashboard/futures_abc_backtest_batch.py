#!/usr/bin/env python3
"""Simple batch backtest for the futures A-bear / B-bull / C-bear pattern.

Assumptions:
- Signal is formed on the current completed 1-minute bar C.
- Enter long at the next bar open.
- Exit at the open after holding N bars.
- One position at a time per symbol.
- Commission is applied on both entry and exit as a simple rate.
- Pattern also requires C.low >= B.low.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd


DEFAULT_SYMBOLS = [
    "AG0",
    "AU0",
    "CU0",
    "AL0",
    "NI0",
    "RB0",
    "HC0",
    "RU0",
    "BU0",
    "SC0",
    "A0",
    "C0",
    "I0",
    "J0",
    "JM0",
    "L0",
    "M0",
    "P0",
    "PP0",
    "V0",
    "Y0",
    "CF0",
    "FG0",
    "MA0",
    "OI0",
    "RM0",
    "SA0",
    "SF0",
    "SM0",
    "SR0",
    "TA0",
    "UR0",
    "PR0",
]


@dataclass
class Trade:
    symbol: str
    exchange: str
    name: str
    code: str
    a_time: str
    b_time: str
    c_time: str
    a_high: float
    b_high: float
    b_low: float
    c_low: float
    c_close: float
    signal_time: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    hold_bars: int
    gross_return_pct: float
    net_return_pct: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a simple multi-symbol futures A/B/C backtest.")
    parser.add_argument("--trade-date", default="20260320", help="Use bars from this date, format YYYYMMDD.")
    parser.add_argument(
        "--symbols",
        default=",".join(DEFAULT_SYMBOLS),
        help="Comma-separated continuous symbols. Default: current watch symbols.",
    )
    parser.add_argument(
        "--hold-bars",
        type=int,
        default=1,
        help="Exit at the open after holding this many 1-minute bars. Default: 1.",
    )
    parser.add_argument(
        "--commission-rate",
        type=float,
        default=0.0002,
        help="Simple commission rate applied on both entry and exit. Default: 0.0002.",
    )
    parser.add_argument(
        "--request-sleep",
        type=float,
        default=0.35,
        help="Sleep seconds between symbol requests to reduce rate-limit risk. Default: 0.35.",
    )
    parser.add_argument(
        "--output-dir",
        default="backtests",
        help="Directory to write CSV reports into. Default: backtests",
    )
    return parser.parse_args()


def parse_symbols(symbols_arg: str) -> list[str]:
    return [item.strip().upper() for item in symbols_arg.split(",") if item.strip()]


def load_symbol_meta(trade_date: str, symbols: list[str]) -> dict[str, dict[str, str]]:
    fallback = {
        symbol: {
            "exchange": "",
            "name": symbol,
            "code": symbol[:-1] if symbol.endswith("0") else symbol,
        }
        for symbol in symbols
    }
    try:
        rules = ak.futures_rule(date=trade_date)
    except Exception:
        return fallback

    rules = rules[~rules["品种"].astype(str).str.contains("期权", na=False)].copy()
    rules = rules[["交易所", "品种", "代码"]].drop_duplicates().reset_index(drop=True)
    rules["代码"] = rules["代码"].astype(str).str.upper().str.strip()
    rules = rules[rules["代码"].str.fullmatch(r"[A-Z]+")].copy()
    rules["连续代码"] = rules["代码"] + "0"

    meta = fallback.copy()
    for _, row in rules.iterrows():
        symbol = str(row["连续代码"]).strip().upper()
        if symbol not in meta:
            continue
        meta[symbol] = {
            "exchange": str(row["交易所"]).strip(),
            "name": str(row["品种"]).strip(),
            "code": str(row["代码"]).strip().upper(),
        }
    return meta


def load_minute_data(symbol: str, trade_date: str) -> pd.DataFrame:
    df = ak.futures_zh_minute_sina(symbol=symbol, period="1").copy()
    if df.empty:
        raise ValueError("no minute data returned")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["datetime", "open", "high", "low", "close"]).reset_index(drop=True)

    target_date = pd.to_datetime(trade_date, format="%Y%m%d", errors="raise").date()
    df = df[df["datetime"].dt.date == target_date].reset_index(drop=True)
    if len(df) < 4:
        raise ValueError("not enough valid bars after filtering by trade date")
    return df


def add_signal(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    a = df.shift(2)
    b = df.shift(1)
    c = df
    signal = (
        (a["close"] < a["open"])
        & (b["close"] > b["open"])
        & (c["close"] < c["open"])
        & (c["low"] >= b["low"])
    )
    df["abc_signal"] = signal.fillna(False)
    return df


def build_trades(
    df: pd.DataFrame,
    symbol: str,
    symbol_meta: dict[str, str],
    hold_bars: int,
    commission_rate: float,
) -> list[Trade]:
    trades: list[Trade] = []
    i = 2
    last_index = len(df) - 1
    while i < last_index:
        if not bool(df.iloc[i]["abc_signal"]):
            i += 1
            continue

        a_bar = df.iloc[i - 2]
        b_bar = df.iloc[i - 1]
        c_bar = df.iloc[i]
        entry_idx = i + 1
        exit_idx = entry_idx + hold_bars
        if exit_idx > last_index:
            break

        entry_bar = df.iloc[entry_idx]
        exit_bar = df.iloc[exit_idx]
        entry_price = float(entry_bar["open"])
        exit_price = float(exit_bar["open"])
        gross_return = (exit_price / entry_price) - 1.0
        net_return = gross_return - (commission_rate * 2.0)

        trades.append(
            Trade(
                symbol=symbol,
                exchange=symbol_meta.get("exchange", ""),
                name=symbol_meta.get("name", symbol),
                code=symbol_meta.get("code", symbol),
                a_time=pd.Timestamp(a_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                b_time=pd.Timestamp(b_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                c_time=pd.Timestamp(c_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                a_high=float(a_bar["high"]),
                b_high=float(b_bar["high"]),
                b_low=float(b_bar["low"]),
                c_low=float(c_bar["low"]),
                c_close=float(c_bar["close"]),
                signal_time=pd.Timestamp(c_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                entry_time=pd.Timestamp(entry_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                exit_time=pd.Timestamp(exit_bar["datetime"]).strftime("%Y-%m-%d %H:%M:%S"),
                entry_price=entry_price,
                exit_price=exit_price,
                hold_bars=hold_bars,
                gross_return_pct=gross_return * 100.0,
                net_return_pct=net_return * 100.0,
            )
        )
        i = exit_idx + 1
    return trades


def summarize_symbol(symbol: str, trades: list[Trade], bars_count: int, raw_signals: int) -> dict[str, object]:
    returns = [trade.net_return_pct for trade in trades]
    win_count = sum(1 for value in returns if value > 0)
    avg_return = sum(returns) / len(returns) if returns else 0.0
    total_return = sum(returns)
    return {
        "symbol": symbol,
        "bars": bars_count,
        "raw_signals": raw_signals,
        "trades": len(trades),
        "wins": win_count,
        "win_rate_pct": (win_count / len(trades) * 100.0) if trades else 0.0,
        "avg_net_return_pct": avg_return,
        "total_net_return_pct": total_return,
    }


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    symbols = parse_symbols(args.symbols)
    symbol_meta = load_symbol_meta(trade_date=args.trade_date, symbols=symbols)
    output_dir = Path(args.output_dir).resolve()

    per_symbol_rows: list[dict[str, object]] = []
    trade_rows: list[dict[str, object]] = []
    failed_rows: list[dict[str, object]] = []

    for index, symbol in enumerate(symbols):
        if index > 0:
            time.sleep(args.request_sleep)
        try:
            minute_df = load_minute_data(symbol=symbol, trade_date=args.trade_date)
            minute_df = add_signal(minute_df)
            trades = build_trades(
                df=minute_df,
                symbol=symbol,
                symbol_meta=symbol_meta.get(symbol, {}),
                hold_bars=args.hold_bars,
                commission_rate=args.commission_rate,
            )
            raw_signals = int(minute_df["abc_signal"].sum())
            per_symbol_rows.append(
                summarize_symbol(symbol=symbol, trades=trades, bars_count=len(minute_df), raw_signals=raw_signals)
            )
            trade_rows.extend(asdict(item) for item in trades)
            print(
                f"[OK] {symbol:<5} bars={len(minute_df):<4} "
                f"signals={raw_signals:<3} trades={len(trades):<3}"
            )
        except Exception as exc:  # noqa: BLE001
            failed_rows.append({"symbol": symbol, "error": f"{type(exc).__name__}: {exc}"})
            print(f"[ERR] {symbol:<5} {type(exc).__name__}: {exc}", file=sys.stderr)

    summary_path = output_dir / f"futures_abc_backtest_summary_{args.trade_date}.csv"
    trades_path = output_dir / f"futures_abc_backtest_trades_{args.trade_date}.csv"
    failed_path = output_dir / f"futures_abc_backtest_failed_{args.trade_date}.csv"

    if per_symbol_rows:
        write_csv(summary_path, per_symbol_rows, list(per_symbol_rows[0].keys()))
    if trade_rows:
        write_csv(trades_path, trade_rows, list(trade_rows[0].keys()))
    if failed_rows:
        write_csv(failed_path, failed_rows, list(failed_rows[0].keys()))

    total_trades = len(trade_rows)
    total_wins = sum(1 for row in trade_rows if float(row["net_return_pct"]) > 0)
    avg_return = sum(float(row["net_return_pct"]) for row in trade_rows) / total_trades if total_trades else 0.0
    total_return = sum(float(row["net_return_pct"]) for row in trade_rows)

    print()
    print(f"trade_date: {args.trade_date}")
    print(f"symbols_ok: {len(per_symbol_rows)}")
    print(f"symbols_failed: {len(failed_rows)}")
    print(f"total_trades: {total_trades}")
    print(f"win_rate_pct: {(total_wins / total_trades * 100.0) if total_trades else 0.0:.2f}")
    print(f"avg_net_return_pct: {avg_return:.4f}")
    print(f"total_net_return_pct: {total_return:.4f}")
    print(f"summary_csv: {summary_path}")
    print(f"trades_csv: {trades_path}")
    if failed_rows:
        print(f"failed_csv: {failed_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
