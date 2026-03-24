# -*- coding: utf-8 -*-
"""
股票分时买点脚本历史回放工具

用途：
1. 用 AllTick `/kline` 历史 K 线回放 stock_intraday_pattern_watch.py 的信号逻辑
2. 先将历史 K 线落本地缓存，再从缓存回放，减少重复拉取
3. 为参数校准提供离线验证入口
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / ".guanlan" / "replay_results"

from strategies.script.stock_intraday_pattern_watch import (
    MinuteBar,
    PatternWatcher,
    Signal,
    SymbolState,
    floor_minute,
    load_config,
)
from tools.alltick_history_source import (
    AllTickHistorySource,
    DEFAULT_CACHE_DB_PATH,
    DEFAULT_MANAGER_API_PATH,
    DEFAULT_MANAGER_WATCHLIST_PATH,
    DEFAULT_PER_TOKEN_INTERVAL_SECONDS,
    load_symbols_from_watchlist,
    load_watchlist_name_map,
    normalize_symbol_code,
)


class ReplayEngine:
    """最小脚本引擎桩，仅用于回放时接收日志。"""

    def __init__(self) -> None:
        self.logs: list[str] = []

    def write_script_log(self, msg: str) -> None:
        self.logs.append(msg)


@dataclass
class ReplaySignalRecord:
    """单条历史信号记录。"""

    symbol: str
    name: str
    trading_day: str
    signal_time: str
    buy_time: str
    signal_pct: float
    pattern_type: str
    buy_type: str
    strength: str
    entry_price: float
    stop_loss: float
    invalidation: float
    trigger_level: float
    left1_pct: float
    left1_time: str
    right1_time: str
    right1_price: float
    right2_time: str
    right2_price: float
    left1_point_time: str
    left1_price: float
    left2_time: str
    left2_price: float
    right1_volume_ok: bool
    session_vwap: float
    reason: str


@dataclass
class ReplayDayResult:
    """单日回放汇总。"""

    symbol: str
    trading_day: str
    bar_count: int
    signal_count: int
    skipped_reason: str = ""


@dataclass
class ReplayRunResult:
    """整轮回放结果。"""

    generated_at: str
    data_source: str
    cache_db: str
    total_api_count: int
    active_api_count: int
    fetch_error_count: int
    symbols: list[str]
    start_date: str
    end_date: str
    total_signals: int
    day_results: list[ReplayDayResult]
    signal_records: list[ReplaySignalRecord]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="回放分时买点脚本的历史信号")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=[],
        help="股票代码，默认读取 strategies/script/stock_intraday_pattern_watch.json",
    )
    parser.add_argument(
        "--date",
        help="单日回放，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--start-date",
        help="起始日期，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--end-date",
        help="结束日期，格式 YYYY-MM-DD",
    )
    parser.add_argument(
        "--limit-signals",
        type=int,
        default=20,
        help="最多显示多少条命中信号",
    )
    parser.add_argument(
        "--config",
        help="可选 JSON 字符串，覆盖部分脚本参数，例如 '{\"strategy1_right1_min_pct\":2.3}'",
    )
    parser.add_argument(
        "--strategy",
        choices=["all", "strategy1", "strategy2"],
        default="all",
        help="指定只回放哪套策略，默认 all",
    )
    parser.add_argument(
        "--watchlist-file",
        default="",
        help="从自选股文件读取股票代码；不传且本地管理器 watchlist 存在时默认使用它",
    )
    parser.add_argument(
        "--api-file",
        default=str(DEFAULT_MANAGER_API_PATH),
        help="AllTick API 文本文件路径，默认使用本地管理器导出的 apis.txt",
    )
    parser.add_argument(
        "--cache-db",
        default=str(DEFAULT_CACHE_DB_PATH),
        help="AllTick K 线 SQLite 缓存路径",
    )
    parser.add_argument(
        "--request-interval-seconds",
        type=float,
        default=DEFAULT_PER_TOKEN_INTERVAL_SECONDS,
        help="同一个 API 两次 /kline 请求之间的间隔秒数，默认 6 秒",
    )
    parser.add_argument(
        "--max-concurrent-tokens",
        type=int,
        default=0,
        help="限制同时参与拉取的 token 数量；0 表示当前所需 token 全部参与",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="结果导出目录，默认 .guanlan/replay_results",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="只打印结果，不导出 json/csv",
    )
    return parser.parse_args()


def parse_date(value: str) -> datetime.date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def get_prev_close(close_map: dict[datetime.date, float], trading_day: datetime.date) -> float:
    """查找指定交易日的前一交易日收盘价。"""
    previous_days = [day for day in close_map if day < trading_day]
    if not previous_days:
        return 0.0
    return close_map[max(previous_days)]


def make_watcher(config_override: dict[str, Any] | None = None) -> PatternWatcher:
    engine = ReplayEngine()
    watcher = PatternWatcher(engine)
    config = load_config()
    if config_override:
        config.update(config_override)
    watcher.config = config
    watcher.engine = engine
    return watcher


def replay_symbol_day(
    watcher: PatternWatcher,
    symbol: str,
    symbol_name: str,
    trading_day: datetime.date,
    prev_close: float,
    bars: list[MinuteBar],
) -> tuple[list[Signal], int]:
    if not bars or prev_close <= 0:
        return [], len(bars)

    state = SymbolState(
        code=symbol,
        name=symbol_name,
        session_date=trading_day,
        pre_close=prev_close,
    )

    signals: list[Signal] = []
    for i in range(1, len(bars) + 1):
        state.bars = [replace(bar) for bar in bars[:i]]

        signal1, signal2 = watcher.evaluate_signals(state)
        signal = watcher.merge_signals(signal1, signal2)
        if not signal:
            continue

        key = f"{signal.pattern_type}:{signal.buy_type}"
        if watcher.can_alert(state, key, signal.signal_time):
            watcher.mark_alert(state, key, signal.signal_time)
            signals.append(signal)

    return signals, len(bars)


def signal_to_record(signal: Signal, trading_day: datetime.date, prev_close: float) -> ReplaySignalRecord:
    """Signal 对象转换为可导出记录。"""
    signal_pct = (signal.entry_price / prev_close - 1) * 100 if prev_close > 0 else 0.0
    return ReplaySignalRecord(
        symbol=signal.code,
        name=signal.name,
        trading_day=trading_day.isoformat(),
        signal_time=signal.signal_time.strftime("%Y-%m-%d %H:%M"),
        buy_time=signal.buy_time or signal.signal_time.strftime("%H:%M"),
        signal_pct=signal_pct,
        pattern_type=signal.pattern_type,
        buy_type=signal.buy_type,
        strength=signal.strength,
        entry_price=signal.entry_price,
        stop_loss=signal.stop_loss,
        invalidation=signal.invalidation,
        trigger_level=signal.trigger_level,
        left1_pct=signal.left1_pct,
        left1_time=signal.left1_time,
        right1_time=signal.right1_time,
        right1_price=signal.right1_price,
        right2_time=signal.right2_time,
        right2_price=signal.right2_price,
        left1_point_time=signal.left1_point_time,
        left1_price=signal.left1_price,
        left2_time=signal.left2_time,
        left2_price=signal.left2_price,
        right1_volume_ok=signal.right1_volume_ok,
        session_vwap=signal.session_vwap,
        reason=signal.reason,
    )


def run_replay(
    symbols: list[str],
    start_date: datetime.date,
    end_date: datetime.date,
    config_override: dict[str, Any] | None = None,
    progress_callback=None,
    api_file: Path | None = None,
    cache_db_path: Path | None = None,
    watchlist_path: Path | None = None,
    request_interval_seconds: float = DEFAULT_PER_TOKEN_INTERVAL_SECONDS,
    max_concurrent_tokens: int = 0,
) -> ReplayRunResult:
    """执行历史回放，返回结构化结果。"""
    watcher = make_watcher(config_override)
    day_results: list[ReplayDayResult] = []
    signal_records: list[ReplaySignalRecord] = []
    watchlist_name_map = load_watchlist_name_map(watchlist_path or DEFAULT_MANAGER_WATCHLIST_PATH)
    source = AllTickHistorySource(
        api_file=api_file or DEFAULT_MANAGER_API_PATH,
        cache_db_path=cache_db_path or DEFAULT_CACHE_DB_PATH,
        request_interval_seconds=request_interval_seconds,
        max_concurrent_tokens=max_concurrent_tokens,
        progress_callback=progress_callback,
    )

    try:
        warm_result = source.warm_cache(symbols, start_date, end_date)

        for symbol in symbols:
            symbol_name = watchlist_name_map.get(symbol, symbol)
            close_map = source.load_daily_close_map(symbol, start_date, end_date)
            trading_days = source.iter_trading_days(symbol, start_date, end_date)
            if not trading_days:
                skipped_reason = warm_result.fetch_errors.get(symbol) or "AllTick 缓存中没有该区间日线"
                day_results.append(
                    ReplayDayResult(
                        symbol=symbol,
                        trading_day=f"{start_date.isoformat()}~{end_date.isoformat()}",
                        bar_count=0,
                        signal_count=0,
                        skipped_reason=skipped_reason,
                    )
                )
                if progress_callback:
                    progress_callback(f"{symbol}: {skipped_reason}")
                continue

            for trading_day in trading_days:
                prev_close = get_prev_close(close_map, trading_day)
                if prev_close <= 0:
                    day_results.append(
                        ReplayDayResult(
                            symbol=symbol,
                            trading_day=trading_day.isoformat(),
                            bar_count=0,
                            signal_count=0,
                            skipped_reason="缺少前收",
                        )
                    )
                    if progress_callback:
                        progress_callback(f"{symbol} {trading_day}: 缺少前收，跳过")
                    continue

                bars = source.load_intraday_bars(symbol, trading_day)
                if not bars:
                    min_day, max_day = source.cache.date_span(symbol, 1)
                    parts = ["AllTick 1分钟历史未命中本地缓存"]
                    if min_day and max_day:
                        parts.append(f"当前缓存范围 {min_day} ~ {max_day}")
                    if symbol in warm_result.fetch_errors:
                        parts.append(f"最近拉取失败: {warm_result.fetch_errors[symbol]}")
                    skipped_reason = "；".join(parts)
                    day_results.append(
                        ReplayDayResult(
                            symbol=symbol,
                            trading_day=trading_day.isoformat(),
                            bar_count=0,
                            signal_count=0,
                            skipped_reason=skipped_reason,
                        )
                    )
                    if progress_callback:
                        progress_callback(f"{symbol} {trading_day}: {skipped_reason}")
                    continue

                signals, bar_count = replay_symbol_day(
                    watcher,
                    symbol,
                    symbol_name,
                    trading_day,
                    prev_close,
                    bars,
                )
                day_results.append(
                    ReplayDayResult(
                        symbol=symbol,
                        trading_day=trading_day.isoformat(),
                        bar_count=bar_count,
                        signal_count=len(signals),
                    )
                )

                if progress_callback:
                    progress_callback(f"{symbol} {trading_day}: bars={bar_count} signals={len(signals)}")

                for signal in signals:
                    record = signal_to_record(signal, trading_day, prev_close)
                    signal_records.append(record)
                    if progress_callback:
                        progress_callback(
                            f"{record.signal_time} | {record.symbol} | {record.pattern_type} | "
                            f"{record.buy_type} | 强度={record.strength} | "
                            f"信号价={record.entry_price:.3f} | 涨幅={record.signal_pct:.2f}% | "
                            f"止损={record.stop_loss:.3f} | "
                            f"触发位={record.trigger_level:.3f} | {record.reason}"
                        )
    finally:
        source.close()

    return ReplayRunResult(
        generated_at=datetime.now().isoformat(timespec="seconds"),
        data_source="alltick:/kline",
        cache_db=str(source.cache_db_path),
        total_api_count=warm_result.total_tokens,
        active_api_count=warm_result.active_tokens,
        fetch_error_count=len(warm_result.fetch_errors),
        symbols=symbols,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        total_signals=len(signal_records),
        day_results=day_results,
        signal_records=signal_records,
    )


def save_replay_result(result: ReplayRunResult, output_dir: Path) -> tuple[Path, Path]:
    """导出回放结果到 json/csv。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"history_signal_result_{stamp}.json"
    csv_path = output_dir / f"history_signal_result_{stamp}.csv"

    json_path.write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    headers = [
        "symbol",
        "name",
        "trading_day",
        "signal_time",
        "buy_time",
        "signal_pct",
        "pattern_type",
        "buy_type",
        "strength",
        "entry_price",
        "stop_loss",
        "invalidation",
        "trigger_level",
        "left1_pct",
        "left1_time",
        "right1_time",
        "right1_price",
        "right2_time",
        "right2_price",
        "left1_point_time",
        "left1_price",
        "left2_time",
        "left2_price",
        "right1_volume_ok",
        "session_vwap",
        "reason",
        "左一开始",
        "右二结束",
        "确认购买时间",
    ]

    def _compose_day_time(trading_day: str, hhmm_or_hhmmss: str) -> str:
        text = (hhmm_or_hhmmss or "").strip()
        if not text:
            return ""
        return f"{trading_day} {text}"

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in result.signal_records:
            row_dict = asdict(row)
            row_dict["左一开始"] = _compose_day_time(
                row_dict.get("trading_day", ""),
                row_dict.get("left1_point_time", "") or row_dict.get("left1_time", ""),
            )
            row_dict["右二结束"] = _compose_day_time(
                row_dict.get("trading_day", ""),
                row_dict.get("right2_time", ""),
            )
            row_dict["确认购买时间"] = row_dict.get("signal_time", "") or _compose_day_time(
                row_dict.get("trading_day", ""),
                row_dict.get("buy_time", ""),
            )
            writer.writerow(row_dict)

    return json_path, csv_path


def print_signal(signal: Signal) -> None:
    print(
        f"{signal.signal_time:%Y-%m-%d %H:%M} | "
        f"{signal.code} | {signal.pattern_type} | {signal.buy_type} | "
        f"强度={signal.strength} | "
        f"进场={signal.entry_price:.3f} | 止损={signal.stop_loss:.3f} | "
        f"触发位={signal.trigger_level:.3f} | "
        f"{signal.reason}"
    )


def main() -> None:
    args = parse_args()

    if args.date:
        start_date = end_date = parse_date(args.date)
    else:
        if not args.start_date or not args.end_date:
            raise SystemExit("请提供 --date，或者同时提供 --start-date 和 --end-date")
        start_date = parse_date(args.start_date)
        end_date = parse_date(args.end_date)

    if end_date < start_date:
        raise SystemExit("结束日期不能早于开始日期")

    config_symbols = load_config().get("symbols", [])
    symbols = [normalize_symbol_code(item) for item in args.symbols]
    watchlist_path = None
    if args.watchlist_file:
        watchlist_path = Path(args.watchlist_file).expanduser().resolve()
        symbols = load_symbols_from_watchlist(watchlist_path)
    elif not symbols and DEFAULT_MANAGER_WATCHLIST_PATH.exists():
        watchlist_path = DEFAULT_MANAGER_WATCHLIST_PATH
        symbols = load_symbols_from_watchlist(watchlist_path)
    elif not symbols:
        symbols = [normalize_symbol_code(item) for item in config_symbols]
    if not symbols:
        raise SystemExit("没有可回放的股票代码")
    if watchlist_path is not None:
        print(f"使用自选股文件: {watchlist_path} | 股票数={len(symbols)}")

    config_override: dict[str, Any] = {}
    if args.config:
        import json
        config_override = json.loads(args.config)
    if args.strategy == "strategy1":
        config_override.update({
            "enable_strategy_one": True,
            "enable_strategy_two": False,
        })
    elif args.strategy == "strategy2":
        config_override.update({
            "enable_strategy_one": False,
            "enable_strategy_two": True,
        })

    result = run_replay(
        symbols,
        start_date,
        end_date,
        config_override,
        api_file=Path(args.api_file).expanduser().resolve(),
        cache_db_path=Path(args.cache_db).expanduser().resolve(),
        watchlist_path=watchlist_path,
        request_interval_seconds=args.request_interval_seconds,
        max_concurrent_tokens=args.max_concurrent_tokens,
    )

    print(
        f"数据源={result.data_source} | cache_db={result.cache_db} | "
        f"total_api={result.total_api_count} | active_api={result.active_api_count} | "
        f"fetch_errors={result.fetch_error_count}"
    )

    printed_signals = 0
    for day in result.day_results:
        if day.skipped_reason:
            print(f"{day.symbol} {day.trading_day}: {day.skipped_reason}，跳过")
        else:
            print(f"{day.symbol} {day.trading_day}: bars={day.bar_count} signals={day.signal_count}")

    for record in result.signal_records:
        if printed_signals >= args.limit_signals:
            continue
        print(
            f"{record.signal_time} | {record.symbol} | {record.pattern_type} | {record.buy_type} | "
            f"强度={record.strength} | 信号价={record.entry_price:.3f} | "
            f"涨幅={record.signal_pct:.2f}% | "
            f"止损={record.stop_loss:.3f} | 触发位={record.trigger_level:.3f} | {record.reason}"
        )
        printed_signals += 1

    if not args.no_export:
        json_path, csv_path = save_replay_result(result, Path(args.output_dir).expanduser().resolve())
        print(f"导出完成: {json_path}")
        print(f"导出完成: {csv_path}")

    print(f"回放完成: symbols={len(symbols)} total_signals={result.total_signals}")


if __name__ == "__main__":
    main()
