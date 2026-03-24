#!/usr/bin/env python3
"""Minimal AKQuant backtest example for the simple A-bear / B-bull / C-bear futures pattern.

Data source:
- 1-minute futures bars are fetched from AkShare / Sina minute API.

Strategy assumptions for this minimal example:
- Buy 1 lot when the A/B/C pattern appears on the current completed 1-minute bar.
- Exit after holding for N bars.
- Pattern also requires C.low >= B.low.
"""

from __future__ import annotations

import argparse
import sys

import akshare as ak
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run an AKQuant backtest on the 1-minute futures A/B/C pattern."
    )
    parser.add_argument("--symbol", default="RB0", help="Continuous futures code, for example RB0")
    parser.add_argument(
        "--history-date",
        default="",
        help="Only use bars from this date, format YYYYMMDD. Example: 20260320",
    )
    parser.add_argument("--count", type=int, default=0, help="Use only the last N bars. Default: all")
    parser.add_argument("--trade-size", type=int, default=1, help="Lots per entry. Default: 1")
    parser.add_argument(
        "--exit-after-bars",
        type=int,
        default=1,
        help="Exit after holding this many bars. Default: 1",
    )
    parser.add_argument("--initial-cash", type=float, default=1_000_000.0, help="Initial cash")
    parser.add_argument(
        "--commission-rate",
        type=float,
        default=0.0002,
        help="Commission rate for the backtest. Default: 0.0002",
    )
    parser.add_argument("--multiplier", type=float, default=10.0, help="Contract multiplier")
    parser.add_argument("--margin-ratio", type=float, default=0.10, help="Margin ratio")
    parser.add_argument("--tick-size", type=float, default=1.0, help="Minimum price tick")
    return parser.parse_args()


def load_minute_data(symbol: str, history_date: str, count: int) -> pd.DataFrame:
    df = ak.futures_zh_minute_sina(symbol=symbol, period="1").copy()
    if df.empty:
        raise ValueError(f"no minute data returned for {symbol}")

    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).reset_index(drop=True)

    if history_date:
        target = pd.to_datetime(history_date, format="%Y%m%d", errors="raise").date()
        df = df[df["datetime"].dt.date == target].reset_index(drop=True)

    if count > 0:
        df = df.tail(count).reset_index(drop=True)

    if len(df) < 3:
        raise ValueError(f"not enough 1-minute bars for {symbol}")

    numeric_cols = ["open", "high", "low", "close", "volume"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=numeric_cols).reset_index(drop=True)
    if len(df) < 3:
        raise ValueError(f"not enough valid numeric bars for {symbol}")

    return df


def add_abc_signal(df: pd.DataFrame) -> pd.DataFrame:
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

    df["abc_signal"] = signal.fillna(False).astype(float)
    return df


def build_backtest_frame(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    bt = df[["datetime", "open", "high", "low", "close", "volume", "abc_signal"]].copy()
    bt = bt.rename(columns={"datetime": "date"})
    bt["symbol"] = symbol
    return bt


def main() -> int:
    args = parse_args()

    try:
        from akquant import InstrumentConfig, Strategy, run_backtest
    except ImportError:
        print("AKQuant is not installed. Install it first, then rerun this script.", file=sys.stderr)
        return 1

    minute_df = load_minute_data(
        symbol=args.symbol,
        history_date=args.history_date,
        count=args.count,
    )
    minute_df = add_abc_signal(minute_df)
    bt_df = build_backtest_frame(minute_df, symbol=args.symbol)

    class FuturesABCStrategy(Strategy):
        def __init__(self, trade_size: int, exit_after_bars: int) -> None:
            super().__init__()
            self.trade_size = trade_size
            self.exit_after_bars = exit_after_bars

        def on_bar(self, bar) -> None:
            position = self.get_position(bar.symbol)
            signal = float(bar.extra.get("abc_signal", 0.0))

            if position == 0 and signal > 0.5:
                self.buy(symbol=bar.symbol, quantity=self.trade_size)
                return

            if position > 0 and self.hold_bar(bar.symbol) >= self.exit_after_bars:
                self.sell(symbol=bar.symbol, quantity=position)

    strategy = FuturesABCStrategy(
        trade_size=args.trade_size,
        exit_after_bars=args.exit_after_bars,
    )

    result = run_backtest(
        strategy=strategy,
        data=bt_df,
        symbols=[args.symbol],
        initial_cash=args.initial_cash,
        commission_rate=args.commission_rate,
        execution_mode="NextOpen",
        timezone="Asia/Shanghai",
        start_time=bt_df["date"].min(),
        end_time=bt_df["date"].max(),
        instruments_config={
            args.symbol: InstrumentConfig(
                symbol=args.symbol,
                asset_type="FUTURES",
                multiplier=args.multiplier,
                margin_ratio=args.margin_ratio,
                tick_size=args.tick_size,
                lot_size=1,
            )
        },
    )

    print(f"symbol: {args.symbol}")
    print(f"bars: {len(bt_df)}")
    print(f"signals: {int(bt_df['abc_signal'].sum())}")
    print(f"start: {bt_df['date'].min()}")
    print(f"end:   {bt_df['date'].max()}")
    print()
    print("metrics:")
    print(result.metrics_df.to_string(index=False))
    print()

    if hasattr(result, "trades_df") and result.trades_df is not None and not result.trades_df.empty:
        print("trades:")
        print(result.trades_df.to_string(index=False))
    else:
        print("trades: no trades")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
