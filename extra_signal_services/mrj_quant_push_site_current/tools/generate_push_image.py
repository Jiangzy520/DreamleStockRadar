#!/usr/bin/env python3
"""从实时信号 CSV 自动生成推送图片（PNG）。"""

from __future__ import annotations

import argparse
import csv
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from PIL import Image, ImageDraw, ImageFont


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MANAGER_DIR = PROJECT_ROOT / ".guanlan" / "alltick_manager"
DEFAULT_ALLTICK_DIR = PROJECT_ROOT / ".guanlan" / "alltick"

DEFAULT_SIGNAL_FILE = DEFAULT_ALLTICK_DIR / "multi_token_variant_double_bottom_signals.csv"
DEFAULT_WATCHLIST_FILE = DEFAULT_MANAGER_DIR / "watchlist.csv"
DEFAULT_API_FILE = DEFAULT_MANAGER_DIR / "apis.txt"
DEFAULT_ASSIGNMENT_FILE = DEFAULT_MANAGER_DIR / "stock_assignments.csv"
DEFAULT_OUTPUT = DEFAULT_ALLTICK_DIR / "push_snapshot.png"


def _count_non_empty_text_lines(path: Path) -> int:
    if not path.exists():
        return 0
    count = 0
    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def _count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def _assignment_stats(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    assigned_symbols = 0
    assigned_apis: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            symbol = str(row.get("symbol") or row.get("code") or "").strip()
            api = str(row.get("api") or "").strip()
            if symbol:
                assigned_symbols += 1
            if api:
                assigned_apis.add(api)
    return assigned_symbols, len(assigned_apis)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_time(trading_day: str, hhmmss: str) -> str:
    td = _safe_text(trading_day)
    tm = _safe_text(hhmmss)
    if td and tm:
        return f"{td} {tm}"
    if td:
        return td
    return tm


def _load_signals(path: Path, limit: int) -> tuple[int, list[dict[str, str]]]:
    if not path.exists():
        return 0, []
    rows: list[dict[str, str]] = []
    total = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for raw in csv.DictReader(f):
            total += 1
            symbol = _safe_text(raw.get("symbol"))
            name = _safe_text(raw.get("name"))
            trading_day = _safe_text(raw.get("trading_day"))
            signal_time = _safe_text(raw.get("signal_time"))
            signal_price = _safe_text(raw.get("signal_price"))
            rows.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "l1": f"{_safe_text(raw.get('l1_time'))}@{_safe_text(raw.get('l1_price'))}",
                    "l2": f"{_safe_text(raw.get('l2_time'))}@{_safe_text(raw.get('l2_price'))}",
                    "r1": f"{_safe_text(raw.get('r1_time'))}@{_safe_text(raw.get('r1_price'))}",
                    "r2": f"{_safe_text(raw.get('r2_time'))}@{_safe_text(raw.get('r2_price'))}",
                    "buy_time": _normalize_time(trading_day, signal_time),
                    "buy_price": signal_price,
                }
            )

    if limit > 0:
        rows = rows[-limit:]
    rows.reverse()  # 最新在上面
    return total, rows


def _load_font(size: int, bold: bool = False) -> ImageFont.ImageFont:
    candidates: list[Path] = []
    if bold:
        candidates.extend(
            [
                Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc"),
                Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Bold.ttc"),
                Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
            ]
        )
    else:
        candidates.extend(
            [
                Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
                Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
                Path("/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc"),
                Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            ]
        )

    for p in candidates:
        if p.exists():
            try:
                return ImageFont.truetype(str(p), size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def _digest(files: list[Path]) -> str:
    chunks: list[str] = []
    for path in files:
        if not path.exists():
            chunks.append(f"{path}:missing")
            continue
        st = path.stat()
        chunks.append(f"{path}:{int(st.st_mtime)}:{st.st_size}")
    return "|".join(chunks)


def render_image(
    output: Path,
    stats: dict[str, int],
    signals: list[dict[str, str]],
    generated_at: str,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)

    title_font = _load_font(42, bold=True)
    sub_font = _load_font(28, bold=True)
    text_font = _load_font(24, bold=False)
    cell_font = _load_font(23, bold=False)

    margin = 28
    top_h = 170
    header_h = 50
    row_h = 46

    headers = [
        ("股票代码", 145),
        ("名称", 190),
        ("左1", 190),
        ("左2", 190),
        ("右1", 190),
        ("右2", 190),
        ("确认买入时间", 220),
        ("买入价", 120),
    ]
    table_width = sum(w for _, w in headers)
    width = margin * 2 + table_width
    height = margin * 2 + top_h + header_h + row_h * max(len(signals), 1) + 20

    image = Image.new("RGB", (width, height), "#11151b")
    draw = ImageDraw.Draw(image)

    # 顶部背景卡
    draw.rounded_rectangle(
        [margin, margin, width - margin, margin + top_h - 14],
        radius=16,
        fill="#18202a",
        outline="#2e3f54",
        width=2,
    )
    draw.text((margin + 18, margin + 16), "实时策略推送", fill="#ffffff", font=title_font)
    draw.text((margin + 20, margin + 76), f"生成时间: {generated_at}", fill="#9fb2c8", font=text_font)

    stat_line = (
        f"自选:{stats['watchlist']}  "
        f"API总:{stats['api_total']}  已分配API:{stats['assigned_api']}  剩余API:{stats['api_remaining']}  "
        f"已扫描:{stats['assigned_symbol']}  信号总:{stats['signal_total']}"
    )
    draw.text((margin + 20, margin + 114), stat_line, fill="#41cfff", font=sub_font)

    # 表头
    y0 = margin + top_h
    draw.rectangle([margin, y0, width - margin, y0 + header_h], fill="#1d2733")
    x = margin
    for head, w in headers:
        draw.text((x + 10, y0 + 10), head, fill="#d7e9ff", font=text_font)
        x += w
        draw.line([(x, y0), (x, y0 + header_h + row_h * max(len(signals), 1))], fill="#273445", width=1)

    # 数据行
    if not signals:
        draw.rectangle([margin, y0 + header_h, width - margin, y0 + header_h + row_h], fill="#141b24")
        draw.text((margin + 10, y0 + header_h + 10), "暂无信号", fill="#9fb2c8", font=cell_font)
    else:
        for i, row in enumerate(signals):
            y = y0 + header_h + i * row_h
            draw.rectangle(
                [margin, y, width - margin, y + row_h],
                fill=("#141b24" if i % 2 == 0 else "#10161e"),
            )
            values = [
                row["symbol"],
                row["name"],
                row["l1"],
                row["l2"],
                row["r1"],
                row["r2"],
                row["buy_time"],
                row["buy_price"],
            ]
            x = margin
            for idx, value in enumerate(values):
                color = "#f0f6ff"
                if idx in (2, 3, 4, 5):
                    color = "#9ec5ff"
                elif idx == 6:
                    color = "#6fe0ff"
                elif idx == 7:
                    color = "#7fffb7"
                draw.text((x + 10, y + 10), value or "--", fill=color, font=cell_font)
                x += headers[idx][1]

    image.save(output)


def run_once(args: argparse.Namespace) -> tuple[str, int]:
    watchlist_count = _count_csv_rows(args.watchlist_file)
    api_total_count = _count_non_empty_text_lines(args.api_file)
    assigned_symbol_count, assigned_api_count = _assignment_stats(args.assignment_file)
    signal_total, rows = _load_signals(args.signal_file, args.rows)
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    render_image(
        output=args.output,
        stats={
            "watchlist": watchlist_count,
            "api_total": api_total_count,
            "assigned_symbol": assigned_symbol_count,
            "assigned_api": assigned_api_count,
            "api_remaining": max(api_total_count - assigned_api_count, 0),
            "signal_total": signal_total,
        },
        signals=rows,
        generated_at=now_text,
    )
    return now_text, len(rows)


def generate_snapshot(
    rows: int = 24,
    output: Path = DEFAULT_OUTPUT,
    signal_file: Path = DEFAULT_SIGNAL_FILE,
    watchlist_file: Path = DEFAULT_WATCHLIST_FILE,
    api_file: Path = DEFAULT_API_FILE,
    assignment_file: Path = DEFAULT_ASSIGNMENT_FILE,
) -> dict[str, Any]:
    args = argparse.Namespace(
        signal_file=signal_file,
        watchlist_file=watchlist_file,
        api_file=api_file,
        assignment_file=assignment_file,
        output=output,
        rows=rows,
    )
    generated_at, rendered_rows = run_once(args)
    return {
        "generated_at": generated_at,
        "rows": rendered_rows,
        "output": str(output),
        "exists": output.exists(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="自动生成实时策略推送图片")
    parser.add_argument("--signal-file", type=Path, default=DEFAULT_SIGNAL_FILE)
    parser.add_argument("--watchlist-file", type=Path, default=DEFAULT_WATCHLIST_FILE)
    parser.add_argument("--api-file", type=Path, default=DEFAULT_API_FILE)
    parser.add_argument("--assignment-file", type=Path, default=DEFAULT_ASSIGNMENT_FILE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--rows", type=int, default=24, help="图片显示最近多少条信号")
    parser.add_argument("--watch", action="store_true", help="持续监控并自动重新生成")
    parser.add_argument("--interval", type=float, default=2.0, help="watch 模式下刷新间隔（秒）")
    args = parser.parse_args()

    if not args.watch:
        ts, rows = run_once(args)
        print(f"[ok] {ts} 生成完成: {args.output} (rows={rows})")
        return

    watch_files = [args.signal_file, args.watchlist_file, args.api_file, args.assignment_file]
    last_state = ""
    while True:
        state = _digest(watch_files)
        if state != last_state:
            ts, rows = run_once(args)
            print(f"[watch] {ts} 更新图片: {args.output} (rows={rows})")
            last_state = state
        time.sleep(max(args.interval, 0.2))


if __name__ == "__main__":
    main()
