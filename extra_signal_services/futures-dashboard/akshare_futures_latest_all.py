#!/usr/bin/env python3
"""Fetch latest prices for all domestic futures continuous contracts with AkShare."""

from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd
import requests


SINA_HEADERS = {
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
SINA_TIMEOUT_SECONDS = 8


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query latest prices for all domestic futures continuous contracts."
    )
    parser.add_argument(
        "--trade-date",
        default=datetime.now().strftime("%Y%m%d"),
        help="Trade date used for the futures variety list, format YYYYMMDD.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only query the first N varieties for quick testing. Default: all.",
    )
    parser.add_argument(
        "--output",
        default="futures_latest_all.csv",
        help="CSV output path. Default: futures_latest_all.csv in the current directory.",
    )
    return parser.parse_args()


def get_varieties(trade_date: str) -> pd.DataFrame:
    rules = ak.futures_rule(date=trade_date)
    rules = rules[~rules["品种"].astype(str).str.contains("期权", na=False)].copy()
    rules = rules[["交易所", "品种", "代码"]].drop_duplicates().reset_index(drop=True)
    rules["代码"] = rules["代码"].astype(str).str.upper().str.strip()
    rules = rules[rules["代码"].str.fullmatch(r"[A-Z]+")].copy()
    rules["连续代码"] = rules["代码"] + "0"
    return rules.sort_values(["交易所", "代码"]).reset_index(drop=True)


def market_for_exchange(exchange: str) -> str:
    return "CFFEX" if str(exchange) == "中金所" else "CF"


def normalize_time(value: object) -> str:
    text = str(value).strip()
    if text.isdigit() and len(text) == 6:
        return f"{text[0:2]}:{text[2:4]}:{text[4:6]}"
    return text


def parse_number(value: object) -> float | str:
    text = str(value).strip()
    if not text:
        return ""
    try:
        return float(text)
    except ValueError:
        return text


def build_quote_row(
    row: pd.Series,
    *,
    name: object,
    date_text: object,
    time_text: object,
    open_price: object,
    high_price: object,
    low_price: object,
    current_price: object,
    hold: object,
    volume: object,
) -> dict:
    return {
        "日期": str(date_text).strip() or datetime.now().strftime("%Y-%m-%d"),
        "交易所": row["交易所"],
        "品种": row["品种"],
        "代码": row["代码"],
        "连续代码": row["连续代码"],
        "名称": str(name).strip(),
        "时间": normalize_time(time_text),
        "开盘": parse_number(open_price),
        "最高": parse_number(high_price),
        "最低": parse_number(low_price),
        "最新价": parse_number(current_price),
        "持仓量": parse_number(hold),
        "成交量": parse_number(volume),
    }


def fetch_one_via_akshare(row: pd.Series) -> dict:
    market = market_for_exchange(row["交易所"])
    df = ak.futures_zh_spot(symbol=row["连续代码"], market=market)
    if df.empty:
        raise ValueError("empty realtime quote")

    quote = df.iloc[0]
    return build_quote_row(
        row,
        name=quote.get("symbol", ""),
        date_text=datetime.now().strftime("%Y-%m-%d"),
        time_text=quote.get("time", ""),
        open_price=quote.get("open"),
        high_price=quote.get("high"),
        low_price=quote.get("low"),
        current_price=quote.get("current_price"),
        hold=quote.get("hold"),
        volume=quote.get("volume"),
    )


def fetch_raw_sina_fields(symbol: str) -> list[str]:
    response = requests.get(
        "https://hq.sinajs.cn",
        params={"list": f"nf_{symbol}"},
        headers=SINA_HEADERS,
        timeout=SINA_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    response.encoding = "gbk"
    parts = response.text.split('"')
    payload = parts[1] if len(parts) >= 2 else ""
    fields = [item.strip() for item in payload.split(",")] if payload else []
    if not fields or not any(fields):
        raise ValueError("empty quote payload")
    return fields


def fetch_one_via_sina(row: pd.Series) -> dict:
    market = market_for_exchange(row["交易所"])
    fields = fetch_raw_sina_fields(row["连续代码"])
    if market == "CFFEX":
        if len(fields) < 39:
            raise ValueError(f"unexpected CFFEX payload length: {len(fields)}")
        return build_quote_row(
            row,
            name=fields[-1],
            date_text=fields[36],
            time_text=fields[37],
            open_price=fields[0],
            high_price=fields[1],
            low_price=fields[2],
            current_price=fields[3],
            hold=fields[6],
            volume=fields[4],
        )

    if len(fields) < 15:
        raise ValueError(f"unexpected commodity payload length: {len(fields)}")
    return build_quote_row(
        row,
        name=fields[0],
        date_text=fields[17] if len(fields) > 17 else datetime.now().strftime("%Y-%m-%d"),
        time_text=fields[1],
        open_price=fields[2],
        high_price=fields[3],
        low_price=fields[4],
        current_price=fields[8],
        hold=fields[13],
        volume=fields[14],
    )


def fetch_one(row: pd.Series) -> dict:
    try:
        return fetch_one_via_akshare(row)
    except Exception as exc:
        try:
            return fetch_one_via_sina(row)
        except Exception as fallback_exc:
            raise fallback_exc from exc


def main() -> int:
    args = parse_args()
    varieties = get_varieties(args.trade_date)
    if args.limit > 0:
        varieties = varieties.head(args.limit).copy()

    rows: list[dict] = []
    errors: list[dict] = []

    for _, row in varieties.iterrows():
        try:
            result = fetch_one(row)
            rows.append(result)
            print(
                f"[OK] {result['连续代码']:<5} {result['名称']:<12} "
                f"{result['时间']} latest={result['最新价']}"
            )
        except Exception as exc:  # noqa: BLE001
            errors.append(
                {
                    "交易所": row["交易所"],
                    "品种": row["品种"],
                    "代码": row["代码"],
                    "连续代码": row["连续代码"],
                    "错误": f"{type(exc).__name__}: {exc}",
                }
            )
            print(f"[ERR] {row['连续代码']:<5} {type(exc).__name__}: {exc}")

    out_path = Path(args.output).expanduser().resolve()
    result_df = pd.DataFrame(rows)
    result_df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print()
    print(f"成功: {len(result_df)}")
    print(f"失败: {len(errors)}")
    print(f"结果文件: {out_path}")

    if errors:
        err_path = out_path.with_name(out_path.stem + "_errors.csv")
        pd.DataFrame(errors).to_csv(err_path, index=False, encoding="utf-8-sig")
        print(f"错误文件: {err_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
