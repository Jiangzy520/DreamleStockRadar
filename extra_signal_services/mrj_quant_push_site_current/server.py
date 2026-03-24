# -*- coding: utf-8 -*-
"""
Guanlan Web Dashboard server.

This module exposes a deployable web entry for the existing desktop system.
When vnpy/runtime deps are unavailable, it automatically falls back to mock mode
so UI and deployment flow can still be validated.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import re
import subprocess
import threading
import time
from collections import deque
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
import sys
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify, render_template, request, send_file, stream_with_context

# Ensure project root is importable when launched as `python webapp/server.py`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

MODULES: dict[str, str] = {
    "accounts": "账户管理",
    "ai-settings": "AI 配置",
    "portfolio-strategy": "组合策略",
    "cta": "CTA 策略",
    "script": "脚本策略",
    "paper": "模拟交易",
    "advisor": "辅助交易",
    "chart": "实时图表",
    "backtest": "策略回测",
    "risk": "风控管理",
}

ALLTICK_MANAGER_DIR = PROJECT_ROOT / ".guanlan" / "alltick_manager"
ALLTICK_DIR = PROJECT_ROOT / ".guanlan" / "alltick"
RUNTIME_DIR = PROJECT_ROOT / ".guanlan" / "runtime"
REALTIME_SIGNAL_CSV = ALLTICK_DIR / "multi_token_variant_double_bottom_signals.csv"
WATCHLIST_CSV = ALLTICK_MANAGER_DIR / "watchlist.csv"
API_TXT = ALLTICK_MANAGER_DIR / "apis.txt"
ASSIGNMENT_CSV = ALLTICK_MANAGER_DIR / "stock_assignments.csv"
PUSH_SNAPSHOT_PNG = ALLTICK_DIR / "push_snapshot.png"
WATCHLIST_BACKUP_DIR = ALLTICK_MANAGER_DIR / "watchlist_backups"
WATCHLIST_DAILY_DIR = ALLTICK_MANAGER_DIR / "daily_watchlists"
WATCHLIST_IMPORT_DIR = ALLTICK_MANAGER_DIR / "watchlist_imports"
SCAN_LOG_FILE = RUNTIME_DIR / "scan.log"
WEB_LOG_FILE = RUNTIME_DIR / "web.log"
CHINA_TZ = ZoneInfo("Asia/Shanghai")
SCAN_SERVICE_UNIT = "quant-scan.service"
SCAN_OPEN_TIMER_UNIT = "quant-scan-market-open.timer"
SCAN_PREOPEN_TIME = dt_time(9, 20)
SCAN_STOP_TIME = dt_time(15, 5)
API_VALID_DAYS = 6

try:
    from tools.generate_push_image import generate_snapshot
except Exception as exc:  # noqa: BLE001
    generate_snapshot = None
    PUSH_SNAPSHOT_IMPORT_ERROR = str(exc)
else:
    PUSH_SNAPSHOT_IMPORT_ERROR = ""

try:
    from tools.watchlist_image_ocr import extract_codes_from_image
except Exception as exc:  # noqa: BLE001
    extract_codes_from_image = None
    WATCHLIST_OCR_IMPORT_ERROR = str(exc)
else:
    WATCHLIST_OCR_IMPORT_ERROR = ""


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


def _latest_project_file(pattern: str) -> Path | None:
    matches = [item for item in PROJECT_ROOT.glob(pattern) if item.is_file()]
    if not matches:
        return None
    return max(matches, key=lambda item: (item.stat().st_mtime, item.name))


def _format_countdown_text(seconds: int) -> str:
    if seconds <= 0:
        return "已过期"
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, secs = divmod(remainder, 60)
    if days > 0:
        return f"{days}天 {hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _api_expiry_snapshot(valid_days: int = API_VALID_DAYS) -> dict[str, Any]:
    source = _latest_project_file("alltick_apis_*.txt")
    if source is None and API_TXT.exists():
        source = API_TXT

    if source is None:
        return {
            "api_valid_days": valid_days,
            "api_source_file": "",
            "api_created_at": "",
            "api_expire_at": "",
            "api_expire_countdown_seconds": -1,
            "api_expire_countdown_text": "未找到 API 时间源",
            "api_expire_status": "未知",
            "api_expire_basis": "当前项目中没有找到 API 导入时间文件",
        }

    stat_dt = datetime.fromtimestamp(source.stat().st_mtime, CHINA_TZ)
    created_at = stat_dt
    match = re.search(r"alltick_apis_(\d{8})\.txt$", source.name)
    if match:
        try:
            file_day = datetime.strptime(match.group(1), "%Y%m%d").date()
        except ValueError:
            file_day = None
        if file_day and stat_dt.date() != file_day:
            created_at = datetime.combine(file_day, dt_time(0, 0), tzinfo=CHINA_TZ)

    expire_at = created_at + timedelta(days=valid_days)
    remaining_seconds = int((expire_at - datetime.now(CHINA_TZ)).total_seconds())
    countdown_seconds = max(remaining_seconds, 0)
    return {
        "api_valid_days": valid_days,
        "api_source_file": source.name,
        "api_created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
        "api_expire_at": expire_at.strftime("%Y-%m-%d %H:%M:%S"),
        "api_expire_countdown_seconds": countdown_seconds,
        "api_expire_countdown_text": _format_countdown_text(remaining_seconds),
        "api_expire_status": "有效中" if remaining_seconds > 0 else "已过期",
        "api_expire_basis": "按当前 API 批次导入时间推算",
    }


def _infer_suffix(code: str) -> str:
    if code.startswith(("5", "6", "9")):
        return "SH"
    if code.startswith(("0", "2", "3")):
        return "SZ"
    if code.startswith(("4", "8")):
        return "BJ"
    raise ValueError(f"不支持的股票代码: {code}")


def _normalize_symbol(raw: str) -> str:
    text = str(raw or "").strip().upper()
    if not text:
        raise ValueError("空股票代码")

    if "." in text:
        code, suffix = text.split(".", 1)
        if len(code) == 6 and code.isdigit() and suffix in {"SH", "SZ", "BJ"}:
            return f"{code}.{suffix}"

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"股票代码格式不正确: {raw}")
    return f"{digits}.{_infer_suffix(digits)}"


def _load_watchlist_rows(path: Path = WATCHLIST_CSV) -> list[dict[str, str]]:
    if not path.exists():
        return []
    rows: list[dict[str, str]] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            symbol = str(row.get("symbol") or row.get("code") or "").strip()
            name = str(row.get("name") or "").strip()
            if not symbol:
                continue
            try:
                normalized = _normalize_symbol(symbol)
            except Exception:
                continue
            code = normalized.split(".", 1)[0]
            rows.append({"code": code, "symbol": normalized, "name": name or code})
    return rows


def _watchlist_codes_text(rows: list[dict[str, str]]) -> str:
    values: list[str] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol:
            continue
        name = str(row.get("name") or "").strip()
        values.append(f"{symbol},{name}" if name and name != row.get("code") else symbol)
    return "\n".join(values)


def _latest_backup_meta() -> dict[str, str]:
    if not WATCHLIST_BACKUP_DIR.exists():
        return {"name": "", "updated_at": ""}
    files = [item for item in WATCHLIST_BACKUP_DIR.glob("watchlist_*.txt") if item.is_file()]
    if not files:
        return {"name": "", "updated_at": ""}
    latest = max(files, key=lambda item: item.stat().st_mtime)
    return {
        "name": latest.name,
        "updated_at": datetime.fromtimestamp(latest.stat().st_mtime, CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _next_weekday_date(base_dt: datetime | None = None) -> datetime:
    probe = (base_dt or datetime.now(CHINA_TZ)).date() + timedelta(days=1)
    while probe.weekday() >= 5:
        probe += timedelta(days=1)
    return datetime.combine(probe, dt_time(0, 0), tzinfo=CHINA_TZ)


def _load_api_tokens(path: Path = API_TXT) -> list[str]:
    if not path.exists():
        return []
    tokens: list[str] = []
    with path.open("r", encoding="utf-8-sig", errors="ignore") as f:
        for line in f:
            token = line.strip()
            if token:
                tokens.append(token)
    return tokens


def _write_assignment_rows_one_to_one(rows: list[dict[str, str]], tokens: list[str], path: Path = ASSIGNMENT_CSV) -> dict[str, int]:
    path.parent.mkdir(parents=True, exist_ok=True)
    assigned = 0
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["stock_seq", "code", "symbol", "name", "api", "api_seq", "status"])
        writer.writeheader()
        for idx, row in enumerate(rows, start=1):
            api = tokens[idx - 1] if idx - 1 < len(tokens) else ""
            if api:
                assigned += 1
            writer.writerow(
                {
                    "stock_seq": idx,
                    "code": row["code"],
                    "symbol": row["symbol"],
                    "name": row.get("name") or row["code"],
                    "api": api,
                    "api_seq": idx if api else "",
                    "status": "slot_1_of_1" if api else "unassigned",
                }
            )
    return {"assigned_count": assigned, "unassigned_count": max(len(rows) - assigned, 0)}


def _write_daily_watchlist(rows: list[dict[str, str]], target_day: datetime) -> dict[str, str]:
    WATCHLIST_DAILY_DIR.mkdir(parents=True, exist_ok=True)
    day_token = target_day.strftime("%Y%m%d")
    csv_path = WATCHLIST_DAILY_DIR / f"watchlist_{day_token}.csv"
    txt_path = WATCHLIST_DAILY_DIR / f"watchlist_{day_token}.txt"
    assignment_path = WATCHLIST_DAILY_DIR / f"stock_assignments_{day_token}.csv"
    _write_watchlist_rows(rows, csv_path)
    txt_path.write_text(_watchlist_codes_text(rows) + ("\n" if rows else ""), encoding="utf-8")
    assignment_meta = _write_assignment_rows_one_to_one(rows, _load_api_tokens(), assignment_path)
    return {
        "csv_name": csv_path.name,
        "txt_name": txt_path.name,
        "assignment_name": assignment_path.name,
        "assigned_count": str(assignment_meta["assigned_count"]),
        "unassigned_count": str(assignment_meta["unassigned_count"]),
    }


def _watchlist_editor_snapshot() -> dict[str, Any]:
    now = datetime.now(CHINA_TZ)
    next_trade_day = _next_weekday_date(now)
    rows = _load_watchlist_rows(WATCHLIST_CSV)
    latest_backup = _latest_backup_meta()
    return {
        "today_date": now.strftime("%Y-%m-%d"),
        "today_label": now.strftime("%Y年%m月%d日"),
        "next_trade_day": next_trade_day.strftime("%Y-%m-%d"),
        "next_trade_label": next_trade_day.strftime("%Y年%m月%d日"),
        "watchlist_count": len(rows),
        "watchlist_updated_at": _file_updated_text(WATCHLIST_CSV),
        "backup_file_name": latest_backup["name"],
        "backup_updated_at": latest_backup["updated_at"],
        "codes_text": _watchlist_codes_text(rows),
        "updated_at": now.strftime("%Y-%m-%d %H:%M:%S"),
    }


def _parse_watchlist_editor_text(text: str, existing_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    existing_names = {row["symbol"]: row.get("name") or row["code"] for row in existing_rows}
    existing_names_by_code = {row["code"]: row.get("name") or row["code"] for row in existing_rows}
    rows: list[dict[str, str]] = []
    seen: set[str] = set()

    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        parts = [part.strip() for part in re.split(r"[,\t]+", line) if part.strip()] if ("," in line or "\t" in line) else line.split()
        if not parts:
            continue

        code_tokens = [item for item in parts if re.fullmatch(r"(?:\d{6}|\d{6}\.(?:SH|SZ|BJ))", item, re.IGNORECASE)]
        if code_tokens and len(code_tokens) == len(parts):
            for item in code_tokens:
                try:
                    symbol = _normalize_symbol(item)
                except Exception:
                    continue
                if symbol in seen:
                    continue
                seen.add(symbol)
                code = symbol.split(".", 1)[0]
                rows.append({"code": code, "symbol": symbol, "name": existing_names.get(symbol, existing_names_by_code.get(code, code))})
            continue

        match = re.search(r"(?<!\d)(\d{6}(?:\.(?:SH|SZ|BJ))?)(?!\d)", line, re.IGNORECASE)
        if not match:
            continue

        raw_code = match.group(1)
        try:
            symbol = _normalize_symbol(raw_code)
        except Exception:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        code = symbol.split(".", 1)[0]
        name_text = f"{line[:match.start()]} {line[match.end():]}".strip()
        name = re.sub(r"\s+", "", name_text)
        name = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9*]+", "", name)
        if not name:
            name = existing_names.get(symbol, existing_names_by_code.get(code, code))
        rows.append({"code": code, "symbol": symbol, "name": name})

    return sorted(rows, key=lambda item: item["code"])


def _write_watchlist_rows(rows: list[dict[str, str]], path: Path = WATCHLIST_CSV) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["code", "symbol", "name"])
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "code": row["code"],
                    "symbol": row["symbol"],
                    "name": row.get("name") or row["code"],
                }
            )


def _write_watchlist_backup(rows: list[dict[str, str]]) -> dict[str, str]:
    WATCHLIST_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")
    path = WATCHLIST_BACKUP_DIR / f"watchlist_{stamp}.txt"
    path.write_text(_watchlist_codes_text(rows) + ("\n" if rows else ""), encoding="utf-8")
    return {
        "file_name": path.name,
        "updated_at": datetime.fromtimestamp(path.stat().st_mtime, CHINA_TZ).strftime("%Y-%m-%d %H:%M:%S"),
    }


def _save_uploaded_watchlist_images(files: list[Any]) -> tuple[Path, list[Path]]:
    stamp = datetime.now(CHINA_TZ).strftime("%Y%m%d_%H%M%S")
    target_dir = WATCHLIST_IMPORT_DIR / stamp
    target_dir.mkdir(parents=True, exist_ok=True)
    saved: list[Path] = []
    for idx, file_storage in enumerate(files, start=1):
        raw_name = Path(str(file_storage.filename or f"image_{idx}.png")).name
        safe_name = re.sub(r"[^0-9A-Za-z._-]+", "_", raw_name) or f"image_{idx}.png"
        path = target_dir / f"{idx:02d}_{safe_name}"
        file_storage.save(path)
        saved.append(path)
    return target_dir, saved


def _extract_watchlist_codes_from_images(paths: list[Path]) -> dict[str, Any]:
    if extract_codes_from_image is None:
        raise RuntimeError(f"OCR 组件不可用: {WATCHLIST_OCR_IMPORT_ERROR}")

    file_summaries: list[dict[str, Any]] = []
    raw_total = 0
    unique_rows: list[dict[str, str]] = []
    seen: set[str] = set()
    names_by_code: dict[str, str] = {}
    existing_rows = _load_watchlist_rows(WATCHLIST_CSV)
    existing_names = {row["code"]: row.get("name") or row["code"] for row in existing_rows}

    for path in paths:
        item = extract_codes_from_image(path)
        file_rows = item.get("rows") or [{"code": code, "name": ""} for code in item.get("codes", [])]
        raw_total += len(file_rows)
        summary_rows: list[dict[str, str]] = []
        for row in file_rows:
            code = str(row.get("code") or "").strip()
            name = str(row.get("name") or "").strip()
            if not code:
                continue
            summary_rows.append({"code": code, "name": name})
            if name:
                names_by_code.setdefault(code, name)
        file_summaries.append(
            {
                "file_name": item["file_name"],
                "code_count": len(summary_rows),
                "codes": [row["code"] for row in summary_rows],
                "rows": summary_rows,
            }
        )
        for row in summary_rows:
            code = row["code"]
            if code in seen:
                continue
            seen.add(code)
            symbol = _normalize_symbol(code)
            unique_rows.append(
                {
                    "code": code,
                    "symbol": symbol,
                    "name": names_by_code.get(code) or existing_names.get(code, code),
                }
            )

    rows = sorted(unique_rows, key=lambda item: item["code"])
    duplicates_removed = max(raw_total - len(rows), 0)

    return {
        "codes_text": _watchlist_codes_text(rows),
        "codes_count": len(rows),
        "raw_total_count": raw_total,
        "duplicates_removed": duplicates_removed,
        "file_count": len(paths),
        "file_summaries": file_summaries,
    }


def _file_updated_text(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def _tail_text_file(path: Path, lines: int = 120, max_chars: int = 50000) -> str:
    if not path.exists():
        return ""
    try:
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            buffer = deque(f, maxlen=max(1, lines))
    except Exception:
        return ""
    text = "".join(buffer)
    if len(text) > max_chars:
        text = text[-max_chars:]
    return text


def _is_process_running(keyword: str) -> bool:
    if not keyword:
        return False
    try:
        result = subprocess.run(
            ["pgrep", "-af", keyword],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return False
    return bool(result.stdout.strip())


def _systemctl_value(command: str, unit: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", command, unit],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return ""
    return (result.stdout or result.stderr).strip()


def _systemd_unit_exists(unit: str) -> bool:
    try:
        result = subprocess.run(
            ["systemctl", "cat", unit],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return False
    return result.returncode == 0


def _is_within_scan_window(now: datetime | None = None) -> bool:
    now = now or datetime.now(CHINA_TZ)
    if now.weekday() >= 5:
        return False
    start_at = datetime.combine(now.date(), SCAN_PREOPEN_TIME, tzinfo=CHINA_TZ)
    stop_at = datetime.combine(now.date(), SCAN_STOP_TIME, tzinfo=CHINA_TZ)
    return start_at <= now < stop_at


def _reload_scan_service_if_needed(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(CHINA_TZ)
    if not _systemd_unit_exists(SCAN_SERVICE_UNIT):
        return {
            "attempted": False,
            "restarted": False,
            "reason": "service_missing",
            "message": "当前环境未检测到扫描服务",
        }

    active_before = _systemctl_value("is-active", SCAN_SERVICE_UNIT) == "active"
    in_window = _is_within_scan_window(now)
    if not active_before and not in_window:
        return {
            "attempted": False,
            "restarted": False,
            "reason": "outside_window",
            "message": "当前不在扫描时段，新池子将在下次自动启动时生效",
        }

    try:
        result = subprocess.run(
            ["systemctl", "restart", SCAN_SERVICE_UNIT],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # noqa: BLE001
        return {
            "attempted": True,
            "restarted": False,
            "reason": "restart_failed",
            "message": f"扫描服务重载失败: {exc}",
        }

    active_after = _systemctl_value("is-active", SCAN_SERVICE_UNIT) == "active"
    if result.returncode == 0 and active_after:
        return {
            "attempted": True,
            "restarted": True,
            "reason": "ok",
            "message": "扫描脚本已重载，当前立即切换到新池子",
        }
    error_text = (result.stderr or result.stdout or "").strip()
    return {
        "attempted": True,
        "restarted": False,
        "reason": "restart_failed",
        "message": f"扫描服务重载失败: {error_text or '未知错误'}",
    }


def _next_weekday_start(now: datetime) -> datetime:
    current_day = now.date()
    offset = 0
    while True:
        if offset > 0:
            current_day = now.date() + timedelta(days=offset)
        if current_day.weekday() < 5:
            return datetime.combine(current_day, SCAN_PREOPEN_TIME, tzinfo=CHINA_TZ)
        offset += 1


def _format_countdown(seconds: int) -> str:
    if seconds <= 0:
        return "00:00:00"
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _auto_start_snapshot(now: datetime | None = None) -> dict[str, Any]:
    now = now or datetime.now(CHINA_TZ)
    timer_enabled = _systemctl_value("is-enabled", SCAN_OPEN_TIMER_UNIT) == "enabled"
    timer_active = _systemctl_value("is-active", SCAN_OPEN_TIMER_UNIT) == "active"
    service_enabled_text = _systemctl_value("is-enabled", SCAN_SERVICE_UNIT)

    if not timer_enabled:
        return {
            "enabled": False,
            "timer_active": timer_active,
            "service_enabled": service_enabled_text,
            "status_text": "未开启",
            "phase_text": "自动预启动未开启",
            "schedule_text": "交易日 09:20 自动启动（开盘前 10 分钟）",
            "next_start_at": "",
            "countdown_seconds": -1,
            "countdown_text": "未开启",
        }

    today_start = datetime.combine(now.date(), SCAN_PREOPEN_TIME, tzinfo=CHINA_TZ)
    today_stop = datetime.combine(now.date(), SCAN_STOP_TIME, tzinfo=CHINA_TZ)
    weekday = now.weekday()

    if weekday < 5 and now < today_start:
        countdown_seconds = max(0, int((today_start - now).total_seconds()))
        return {
            "enabled": True,
            "timer_active": timer_active,
            "service_enabled": service_enabled_text,
            "status_text": "已开启",
            "phase_text": "等待自动启动",
            "schedule_text": "交易日 09:20 自动启动（开盘前 10 分钟）",
            "next_start_at": today_start.strftime("%Y-%m-%d %H:%M:%S"),
            "countdown_seconds": countdown_seconds,
            "countdown_text": _format_countdown(countdown_seconds),
        }

    if weekday < 5 and today_start <= now < today_stop:
        next_start = _next_weekday_start(now + timedelta(days=1))
        return {
            "enabled": True,
            "timer_active": timer_active,
            "service_enabled": service_enabled_text,
            "status_text": "已开启",
            "phase_text": "扫描时段中",
            "schedule_text": "交易日 09:20 自动启动（开盘前 10 分钟）",
            "next_start_at": next_start.strftime("%Y-%m-%d %H:%M:%S"),
            "countdown_seconds": 0,
            "countdown_text": "扫描时段中",
        }

    next_start = _next_weekday_start(now + timedelta(days=1))
    phase_text = "今日已收盘，等待下个交易日" if weekday < 5 else "休市中，等待下个交易日"
    countdown_seconds = max(0, int((next_start - now).total_seconds()))
    return {
        "enabled": True,
        "timer_active": timer_active,
        "service_enabled": service_enabled_text,
        "status_text": "已开启",
        "phase_text": phase_text,
        "schedule_text": "交易日 09:20 自动启动（开盘前 10 分钟）",
        "next_start_at": next_start.strftime("%Y-%m-%d %H:%M:%S"),
        "countdown_seconds": countdown_seconds,
        "countdown_text": _format_countdown(countdown_seconds),
    }


def _script_log_snapshot(lines: int = 120) -> dict[str, Any]:
    scan_log_text = _tail_text_file(SCAN_LOG_FILE, lines=lines)
    web_log_text = _tail_text_file(WEB_LOG_FILE, lines=min(lines, 80))
    scan_updated_at = _file_updated_text(SCAN_LOG_FILE)
    web_updated_at = _file_updated_text(WEB_LOG_FILE)
    signal_updated_at = _file_updated_text(REALTIME_SIGNAL_CSV)
    scan_running = _is_process_running("alltick_multi_token_seconds_live.py")
    web_running = _is_process_running("webapp/server.py")
    auto_start = _auto_start_snapshot()

    if scan_running:
        scan_status = "运行中"
    elif auto_start["enabled"] and auto_start["phase_text"] == "等待自动启动":
        scan_status = "等待 09:20 自动启动"
    elif auto_start["enabled"] and auto_start["phase_text"] == "扫描时段中":
        scan_status = "应已启动但当前未运行"
    elif auto_start["enabled"]:
        scan_status = "今日扫描已结束"
    else:
        scan_status = "未运行"

    latest_scan_line = ""
    for line in reversed(scan_log_text.splitlines()):
        line = line.strip()
        if line:
            latest_scan_line = line
            break

    return {
        "scan_running": scan_running,
        "web_running": web_running,
        "scan_status": scan_status,
        "scan_log_updated_at": scan_updated_at,
        "web_log_updated_at": web_updated_at,
        "signal_file_updated_at": signal_updated_at,
        "scan_log_tail": scan_log_text,
        "web_log_tail": web_log_text,
        "latest_scan_line": latest_scan_line,
        "auto_start_enabled": auto_start["enabled"],
        "auto_start_timer_active": auto_start["timer_active"],
        "auto_start_status": auto_start["status_text"],
        "auto_start_phase": auto_start["phase_text"],
        "auto_start_schedule": auto_start["schedule_text"],
        "next_auto_start_at": auto_start["next_start_at"],
        "auto_start_countdown_seconds": auto_start["countdown_seconds"],
        "auto_start_countdown_text": auto_start["countdown_text"],
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _assignment_stats(path: Path) -> tuple[int, int]:
    """Return (assigned_symbol_count, assigned_api_count)."""
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


def _load_realtime_signals(path: Path, limit: int = 200) -> tuple[int, list[dict[str, Any]]]:
    if not path.exists():
        return 0, []
    try:
        raw_limit = int(limit)
    except Exception:
        raw_limit = 200
    # limit <= 0 代表返回全部；否则限制最大 10000 条
    safe_limit = min(raw_limit, 10000) if raw_limit > 0 else 0
    if safe_limit > 0:
        buffer: list[dict[str, Any]] | deque[dict[str, Any]] = deque(maxlen=safe_limit)
    else:
        buffer = []
    total = 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            total += 1
            symbol = str(row.get("symbol") or "").strip()
            signal_time = str(row.get("signal_time") or "").strip()
            trading_day = str(row.get("trading_day") or "").strip()
            item = {
                "key": f"{symbol}|{trading_day}|{signal_time}",
                "symbol": symbol,
                "name": str(row.get("name") or "").strip(),
                "trading_day": trading_day,
                "signal_time": signal_time,
                "signal_price": str(row.get("signal_price") or "").strip(),
                "r1_time": str(row.get("r1_time") or "").strip(),
                "r1_price": str(row.get("r1_price") or "").strip(),
                "r2_time": str(row.get("r2_time") or "").strip(),
                "r2_price": str(row.get("r2_price") or "").strip(),
                "l1_time": str(row.get("l1_time") or "").strip(),
                "l1_price": str(row.get("l1_price") or "").strip(),
                "l2_time": str(row.get("l2_time") or "").strip(),
                "l2_price": str(row.get("l2_price") or "").strip(),
            }
            buffer.append(item)
    return total, list(buffer)


def _load_realtime_snapshot(limit: int = 200) -> dict[str, Any]:
    watchlist_count = _count_csv_rows(WATCHLIST_CSV)
    api_total_count = _count_non_empty_text_lines(API_TXT)
    assigned_symbol_count, assigned_api_count = _assignment_stats(ASSIGNMENT_CSV)
    signal_total, signals = _load_realtime_signals(REALTIME_SIGNAL_CSV, limit=limit)
    latest_signal = signals[-1] if signals else None
    signal_file_updated_at = _file_updated_text(REALTIME_SIGNAL_CSV)
    api_expiry = _api_expiry_snapshot()
    return {
        "watchlist_count": watchlist_count,
        "api_total_count": api_total_count,
        "assigned_symbol_count": assigned_symbol_count,
        "assigned_api_count": assigned_api_count,
        "api_remaining_count": max(api_total_count - assigned_api_count, 0),
        "signal_total_count": signal_total,
        "signals": signals,
        "latest_signal": latest_signal,
        "signal_file_updated_at": signal_file_updated_at,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        **api_expiry,
    }


def _realtime_stream_events(limit: int = 200, interval: float = 1.0):
    last_fingerprint = ""
    keepalive_count = 0
    while True:
        payload = _load_realtime_snapshot(limit=limit)
        latest_signal = payload.get("latest_signal") or {}
        latest_key = str(latest_signal.get("key") or "").strip()
        fingerprint = "|".join(
            [
                str(payload.get("signal_total_count", 0)),
                latest_key,
                str(payload.get("signal_file_updated_at") or ""),
            ]
        )
        if fingerprint != last_fingerprint:
            last_fingerprint = fingerprint
            body = json.dumps({"ok": True, "data": payload}, ensure_ascii=False)
            yield f"event: snapshot\ndata: {body}\n\n"
            keepalive_count = 0
        else:
            keepalive_count += 1
            if keepalive_count >= 15:
                yield f": keepalive {int(time.time())}\n\n"
                keepalive_count = 0
        time.sleep(interval)


def _push_snapshot_meta(path: Path = PUSH_SNAPSHOT_PNG) -> dict[str, Any]:
    if not path.exists():
        return {
            "exists": False,
            "url": "",
            "generated_at": "",
            "size_bytes": 0,
        }
    stat = path.stat()
    generated_at = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "exists": True,
        "url": f"/api/realtime/push-image/file?ts={int(stat.st_mtime)}",
        "generated_at": generated_at,
        "size_bytes": stat.st_size,
    }


def _generate_push_snapshot(rows: int = 24) -> dict[str, Any]:
    if generate_snapshot is None:
        return {
            "ok": False,
            "error": f"推送图组件不可用: {PUSH_SNAPSHOT_IMPORT_ERROR}",
        }
    try:
        result = generate_snapshot(rows=rows, output=PUSH_SNAPSHOT_PNG)
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": f"生成推送图失败: {exc}",
        }
    return {
        "ok": True,
        "data": {
            **result,
            **_push_snapshot_meta(PUSH_SNAPSHOT_PNG),
        },
    }


def _fmt_dt(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def _json_safe(value: Any) -> Any:
    """Convert runtime objects to JSON-safe structures."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return _fmt_dt(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "model_dump"):
        try:
            return _json_safe(value.model_dump())
        except Exception:  # noqa: BLE001
            return str(value)
    enum_value = getattr(value, "value", None)
    if enum_value is not None:
        return _json_safe(enum_value)
    return str(value)


class RuntimeBridge:
    """Bridge desktop runtime data to HTTP API."""

    def __init__(self, auto_connect: bool = True) -> None:
        self.mode = "mock"
        self.reason = ""
        self._lock = threading.Lock()
        self._logs: deque[dict[str, str]] = deque(maxlen=500)
        self._risk_cache: dict[str, dict[str, Any]] = {}
        self._portfolio_contract_cache: dict[str, dict[str, Any]] = {}
        self._portfolio_total_cache: dict[str, dict[str, Any]] = {}
        self._mock_connected_envs: list[str] = []

        self._app_engine = None
        self._main_engine = None
        self._event_engine = None
        self._EVENT_LOG = None
        self._EVENT_PM_CONTRACT = None
        self._EVENT_PM_PORTFOLIO = None
        self._EVENT_RISK_RULE = None
        self._mock_cta_strategies: dict[str, dict[str, Any]] = {
            "DemoCta001": {
                "strategy_name": "DemoCta001",
                "class_name": "DemoCtaStrategy",
                "vt_symbol": "IF2506.CFFEX",
                "gateway_name": "模拟账户A",
                "inited": False,
                "trading": False,
                "params": {"fast": 6, "slow": 20},
                "state": {"pos": 0, "hot": "IF2506.CFFEX"},
                "vars": {"signal": "WAIT"},
            }
        }
        self._mock_portfolio_strategies: dict[str, dict[str, Any]] = {
            "DemoPortfolio001": {
                "strategy_name": "DemoPortfolio001",
                "class_name": "DemoPortfolioStrategy",
                "symbols": ["rb", "hc"],
                "vt_symbols": ["rb2510.SHFE", "hc2510.SHFE"],
                "gateway_name": "模拟账户A",
                "inited": False,
                "trading": False,
                "params": {"rebalance_sec": 30},
                "state": {"hot": "rb2510.SHFE,hc2510.SHFE"},
                "vars": {"last_action": "NONE"},
            }
        }
        self._mock_scripts: dict[str, dict[str, Any]] = {
            "intraday_watch": {
                "script_name": "intraday_watch",
                "script_path": "strategies/script/stock_intraday_pattern_watch.py",
                "active": False,
            }
        }

        try:
            # Import here to keep server runnable without vnpy stack.
            from guanlan.core.app import AppEngine
            from guanlan.core.bootstrap import ensure_default_engines
            from guanlan.core.trader.pnl.engine import EVENT_PM_CONTRACT, EVENT_PM_PORTFOLIO
            from vnpy.trader.event import EVENT_LOG
            from vnpy_riskmanager.base import EVENT_RISK_RULE

            self._app_engine = AppEngine.instance()
            self._main_engine = self._app_engine.main_engine
            ensure_default_engines(self._main_engine)
            self._event_engine = self._app_engine.event_engine

            self._EVENT_LOG = EVENT_LOG
            self._EVENT_PM_CONTRACT = EVENT_PM_CONTRACT
            self._EVENT_PM_PORTFOLIO = EVENT_PM_PORTFOLIO
            self._EVENT_RISK_RULE = EVENT_RISK_RULE

            self._register_events()

            if auto_connect:
                self._app_engine.auto_connect()

            self.mode = "live"
            self._append_log("INFO", "Web", "运行于 live 模式")
        except Exception as exc:  # noqa: BLE001
            self.mode = "mock"
            self.reason = str(exc)
            self._append_log("WARNING", "Web", f"运行于 mock 模式: {exc}")

    def _register_events(self) -> None:
        if not self._event_engine:
            return
        self._event_engine.register(self._EVENT_LOG, self._on_log_event)
        self._event_engine.register(self._EVENT_PM_CONTRACT, self._on_pm_contract_event)
        self._event_engine.register(self._EVENT_PM_PORTFOLIO, self._on_pm_portfolio_event)
        self._event_engine.register(self._EVENT_RISK_RULE, self._on_risk_event)

    def _append_log(self, level: str, source: str, msg: str) -> None:
        with self._lock:
            self._logs.append(
                {
                    "time": datetime.now().strftime("%H:%M:%S"),
                    "level": level,
                    "source": source,
                    "msg": msg,
                }
            )

    def _on_log_event(self, event: Any) -> None:
        data = getattr(event, "data", None)
        level_int = getattr(data, "level", 20)
        level_map = {10: "DEBUG", 20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}
        self._append_log(
            level_map.get(level_int, str(level_int)),
            getattr(data, "gateway_name", "") or "System",
            getattr(data, "msg", ""),
        )

    def _on_pm_contract_event(self, event: Any) -> None:
        data = getattr(event, "data", {}) or {}
        key = f"{data.get('reference', '')}|{data.get('vt_symbol', '')}|{data.get('gateway_name', '')}"
        with self._lock:
            self._portfolio_contract_cache[key] = dict(data)

    def _on_pm_portfolio_event(self, event: Any) -> None:
        data = getattr(event, "data", {}) or {}
        key = f"{data.get('reference', '')}|{data.get('gateway_name', '')}"
        with self._lock:
            self._portfolio_total_cache[key] = dict(data)

    def _on_risk_event(self, event: Any) -> None:
        data = getattr(event, "data", {}) or {}
        name = data.get("name", "")
        if not name:
            return
        with self._lock:
            self._risk_cache[name] = dict(data)

    def list_envs(self) -> list[str]:
        if self.mode == "live":
            from guanlan.core.setting import account
            config = account.load_config()
            return list(account.get_accounts(config).keys())
        return ["模拟账户A", "模拟账户B"]

    def favorites(self) -> list[dict[str, str]]:
        if self.mode == "live":
            from guanlan.core.setting import contract as contract_setting

            contracts = contract_setting.load_contracts()
            favorites = contract_setting.load_favorites()
            rows: list[dict[str, str]] = []
            for symbol_key in favorites:
                item = contracts.get(symbol_key, {})
                if not item:
                    continue
                vt_symbol = item.get("vt_symbol", "")
                symbol = vt_symbol.split(".", 1)[0] if "." in vt_symbol else symbol_key
                rows.append(
                    {
                        "key": symbol_key,
                        "symbol": symbol,
                        "exchange": item.get("exchange", ""),
                        "name": item.get("name", symbol_key),
                    }
                )
            return rows

        return [
            {"key": "IF", "symbol": "IF2506", "exchange": "CFFEX", "name": "股指期货"},
            {"key": "rb", "symbol": "rb2510", "exchange": "SHFE", "name": "螺纹钢"},
            {"key": "OI", "symbol": "OI605", "exchange": "CZCE", "name": "菜籽油"},
        ]

    def connect(self, env_name: str) -> dict[str, Any]:
        if self.mode == "live":
            self._app_engine.connect(env_name)
            self._append_log("INFO", "Web", f"发起连接: {env_name}")
            return {"ok": True, "mode": self.mode}

        if env_name not in self._mock_connected_envs:
            self._mock_connected_envs.append(env_name)
        self._append_log("INFO", "Mock", f"模拟连接: {env_name}")
        return {"ok": True, "mode": self.mode}

    def disconnect(self, env_name: str) -> dict[str, Any]:
        if self.mode == "live":
            self._app_engine.disconnect(env_name)
            self._append_log("INFO", "Web", f"断开连接: {env_name}")
            return {"ok": True, "mode": self.mode}

        if env_name in self._mock_connected_envs:
            self._mock_connected_envs.remove(env_name)
        self._append_log("INFO", "Mock", f"模拟断开: {env_name}")
        return {"ok": True, "mode": self.mode}

    def _build_mock_home(self) -> dict[str, Any]:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        symbols = ["IF2506", "rb2510", "OI605", "AU2408", "CU2507"]
        orders = []
        trades = []
        for i in range(8):
            symbol = random.choice(symbols)
            direction = random.choice(["多", "空"])
            status = random.choice(["未成交", "部分成交", "全部成交"])
            gateway = random.choice(self._mock_connected_envs or ["模拟账户A"])
            price = round(random.uniform(3200, 4600), 2)
            volume = random.randint(1, 5)
            traded = volume if status == "全部成交" else random.randint(0, max(0, volume - 1))
            orders.append(
                {
                    "orderid": f"M{i+1:04d}",
                    "symbol": symbol,
                    "direction": direction,
                    "offset": random.choice(["开", "平"]),
                    "price": price,
                    "volume": volume,
                    "traded": traded,
                    "status": status,
                    "datetime": now,
                    "reference": "手动交易",
                    "gateway_name": gateway,
                    "vt_orderid": f"{gateway}.M{i+1:04d}",
                }
            )
            if traded > 0:
                trades.append(
                    {
                        "tradeid": f"T{i+1:04d}",
                        "orderid": f"M{i+1:04d}",
                        "symbol": symbol,
                        "direction": direction,
                        "offset": random.choice(["开", "平"]),
                        "price": price,
                        "volume": traded,
                        "datetime": now,
                        "reference": "手动交易",
                        "gateway_name": gateway,
                    }
                )

        accounts = [
            {
                "accountid": env,
                "balance": 1000000.0 + random.randint(-50000, 50000),
                "frozen": random.randint(0, 50000),
                "available": 900000.0 + random.randint(-50000, 50000),
                "gateway_name": env,
                "vt_accountid": f"{env}.{env}",
            }
            for env in (self._mock_connected_envs or ["模拟账户A"])
        ]

        positions = []
        for i, symbol in enumerate(symbols[:4]):
            gateway = random.choice(self._mock_connected_envs or ["模拟账户A"])
            direction = random.choice(["多", "空"])
            vol = random.randint(1, 8)
            positions.append(
                {
                    "symbol": symbol,
                    "direction": direction,
                    "volume": vol,
                    "yd_volume": max(vol - 1, 0),
                    "frozen": random.randint(0, max(0, vol - 1)),
                    "price": round(random.uniform(3000, 4500), 2),
                    "pnl": round(random.uniform(-8000, 9000), 2),
                    "gateway_name": gateway,
                    "vt_positionid": f"{gateway}.{symbol}.{i}",
                }
            )

        portfolio_contracts = [
            {
                "reference": "ScriptTrader",
                "vt_symbol": "IF2506.CFFEX",
                "gateway_name": random.choice(self._mock_connected_envs or ["模拟账户A"]),
                "open_pos": 0,
                "last_pos": 2,
                "trading_pnl": 1220.5,
                "holding_pnl": -90.5,
                "total_pnl": 1130.0,
                "commission": 46.0,
            }
        ]

        portfolio_totals = [
            {
                "reference": "ScriptTrader",
                "gateway_name": random.choice(self._mock_connected_envs or ["模拟账户A"]),
                "trading_pnl": 1220.5,
                "holding_pnl": -90.5,
                "total_pnl": 1130.0,
                "commission": 46.0,
            }
        ]

        risk_rules = [
            {
                "name": "order_size_rule",
                "parameters": {"active": True, "order_size_limit": 10},
                "variables": {"order_size_count": random.randint(0, 10)},
            },
            {
                "name": "daily_limit_rule",
                "parameters": {"active": True, "daily_order_limit": 200},
                "variables": {"daily_order_count": random.randint(0, 200)},
            },
        ]

        with self._lock:
            logs = list(self._logs)

        return {
            "mode": self.mode,
            "reason": self.reason,
            "timestamp": now,
            "connected_envs": list(self._mock_connected_envs),
            "gateways": list(self._mock_connected_envs or ["模拟账户A"]),
            "accounts": accounts,
            "positions": positions,
            "orders": orders,
            "trades": trades,
            "portfolio_contracts": portfolio_contracts,
            "portfolio_totals": portfolio_totals,
            "risk_rules": risk_rules,
            "logs": logs,
        }

    def _build_live_home(self) -> dict[str, Any]:
        me = self._main_engine

        accounts = [
            {
                "accountid": acc.accountid,
                "balance": acc.balance,
                "frozen": acc.frozen,
                "available": acc.available,
                "gateway_name": acc.gateway_name,
                "vt_accountid": acc.vt_accountid,
            }
            for acc in me.get_all_accounts()
        ]

        positions = [
            {
                "symbol": pos.symbol,
                "direction": pos.direction.value if pos.direction else "",
                "volume": pos.volume,
                "yd_volume": pos.yd_volume,
                "frozen": pos.frozen,
                "price": pos.price,
                "pnl": pos.pnl,
                "gateway_name": pos.gateway_name,
                "vt_positionid": pos.vt_positionid,
            }
            for pos in me.get_all_positions()
        ]

        orders_raw = sorted(
            me.get_all_orders(),
            key=lambda o: getattr(o, "datetime", None) or datetime.min,
            reverse=True,
        )[:200]
        orders = []
        for order in orders_raw:
            orders.append(
                {
                    "orderid": order.orderid,
                    "symbol": order.symbol,
                    "direction": order.direction.value if order.direction else "",
                    "offset": order.offset.value,
                    "price": order.price,
                    "volume": order.volume,
                    "traded": order.traded,
                    "status": order.status.value,
                    "datetime": _fmt_dt(order.datetime),
                    "reference": getattr(order, "reference", ""),
                    "gateway_name": order.gateway_name,
                    "vt_orderid": order.vt_orderid,
                }
            )

        trades_raw = sorted(
            me.get_all_trades(),
            key=lambda t: getattr(t, "datetime", None) or datetime.min,
            reverse=True,
        )[:200]
        trades = []
        for trade in trades_raw:
            trades.append(
                {
                    "tradeid": trade.tradeid,
                    "orderid": trade.orderid,
                    "symbol": trade.symbol,
                    "direction": trade.direction.value if trade.direction else "",
                    "offset": trade.offset.value,
                    "price": trade.price,
                    "volume": trade.volume,
                    "datetime": _fmt_dt(trade.datetime),
                    "reference": getattr(trade, "reference", ""),
                    "gateway_name": trade.gateway_name,
                }
            )

        with self._lock:
            portfolio_contracts = list(self._portfolio_contract_cache.values())
            portfolio_totals = list(self._portfolio_total_cache.values())
            risk_rules = list(self._risk_cache.values())
            logs = list(self._logs)

        if not portfolio_contracts:
            portfolio_engine = me.get_engine("portfolio")
            if portfolio_engine and hasattr(portfolio_engine, "contract_results"):
                portfolio_contracts = [obj.get_data() for obj in portfolio_engine.contract_results.values()]
            if portfolio_engine and hasattr(portfolio_engine, "portfolio_results"):
                portfolio_totals = [obj.get_data() for obj in portfolio_engine.portfolio_results.values()]

        if not risk_rules:
            risk_engine = me.get_engine("RiskManager")
            if risk_engine and hasattr(risk_engine, "get_all_rule_names"):
                for name in risk_engine.get_all_rule_names():
                    risk_rules.append(risk_engine.get_rule_data(name))

        return {
            "mode": self.mode,
            "reason": self.reason,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "connected_envs": list(self._app_engine.connected_envs),
            "gateways": list(self._app_engine.connected_envs),
            "accounts": accounts,
            "positions": positions,
            "orders": orders,
            "trades": trades,
            "portfolio_contracts": portfolio_contracts,
            "portfolio_totals": portfolio_totals,
            "risk_rules": risk_rules,
            "logs": logs,
        }

    def home(self) -> dict[str, Any]:
        if self.mode == "live":
            try:
                return self._build_live_home()
            except Exception as exc:  # noqa: BLE001
                self._append_log("ERROR", "Web", f"live 快照失败，回退 mock: {exc}")
                self.mode = "mock"
                self.reason = str(exc)
        return self._build_mock_home()

    def send_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.mode == "live":
            from guanlan.core.utils.symbol_converter import SymbolConverter
            from vnpy.trader.constant import Direction, Exchange, Offset, OrderType
            from vnpy.trader.object import OrderRequest

            direction_map = {"多": Direction.LONG, "空": Direction.SHORT, "LONG": Direction.LONG, "SHORT": Direction.SHORT}
            offset_map = {
                "开": Offset.OPEN,
                "平": Offset.CLOSE,
                "平今": Offset.CLOSETODAY,
                "平昨": Offset.CLOSEYESTERDAY,
                "OPEN": Offset.OPEN,
                "CLOSE": Offset.CLOSE,
                "CLOSETODAY": Offset.CLOSETODAY,
                "CLOSEYESTERDAY": Offset.CLOSEYESTERDAY,
            }
            type_map = {"限价": OrderType.LIMIT, "市价": OrderType.MARKET, "LIMIT": OrderType.LIMIT, "MARKET": OrderType.MARKET}

            symbol = str(payload.get("symbol", "")).strip()
            exchange_value = str(payload.get("exchange", "")).strip()
            gateway = str(payload.get("gateway", "")).strip()
            if not symbol or not exchange_value or not gateway:
                return {"ok": False, "error": "symbol/exchange/gateway 必填"}

            exchange = Exchange(exchange_value)
            exchange_symbol = SymbolConverter.to_exchange(symbol, exchange)

            req = OrderRequest(
                symbol=exchange_symbol,
                exchange=exchange,
                direction=direction_map.get(str(payload.get("direction", "多")), Direction.LONG),
                offset=offset_map.get(str(payload.get("offset", "开")), Offset.OPEN),
                type=type_map.get(str(payload.get("type", "限价")), OrderType.LIMIT),
                price=float(payload.get("price", 0)),
                volume=float(payload.get("volume", 1)),
                reference="手动交易",
            )
            vt_orderid = self._main_engine.send_order(req, gateway)
            self._append_log("INFO", "Web", f"发送委托 {exchange_symbol}.{exchange_value} -> {vt_orderid}")
            return {"ok": bool(vt_orderid), "vt_orderid": vt_orderid or ""}

        self._append_log("INFO", "Mock", f"模拟下单: {payload}")
        return {"ok": True, "vt_orderid": f"MOCK.{random.randint(10000, 99999)}"}

    def cancel_all(self) -> dict[str, Any]:
        if self.mode == "live":
            count = 0
            for order in self._main_engine.get_all_active_orders():
                req = order.create_cancel_request()
                self._main_engine.cancel_order(req, order.gateway_name)
                count += 1
            self._append_log("INFO", "Web", f"全撤完成，数量: {count}")
            return {"ok": True, "count": count}

        self._append_log("INFO", "Mock", "模拟全撤")
        return {"ok": True, "count": random.randint(0, 4)}

    def chat(self, payload: dict[str, Any]) -> dict[str, Any]:
        message = str(payload.get("message", "")).strip()
        model = str(payload.get("model", "")).strip()
        if not message:
            return {"ok": False, "error": "message 不能为空"}

        if self.mode == "live":
            try:
                from guanlan.core.services.ai import chat_sync
                content = chat_sync(message=message, model=model or None)
                return {"ok": True, "content": content}
            except Exception as exc:  # noqa: BLE001
                return {"ok": False, "error": f"AI 调用失败: {exc}"}

        return {"ok": True, "content": f"Mock AI 回答（{model or 'default'}）: 已收到“{message}”"}

    def get_strategies(self, kind: str) -> dict[str, Any]:
        """Return strategy list for cta/portfolio/script."""
        if kind not in {"cta", "portfolio", "script"}:
            return {"ok": False, "error": "不支持的策略类型"}

        if self.mode != "live":
            if kind == "cta":
                rows = list(self._mock_cta_strategies.values())
            elif kind == "portfolio":
                rows = list(self._mock_portfolio_strategies.values())
            else:
                rows = list(self._mock_scripts.values())
            return {"ok": True, "kind": kind, "rows": rows}

        try:
            if kind == "cta":
                engine = self._main_engine.get_engine("CtaStrategy")
                if not engine:
                    return {"ok": True, "kind": kind, "rows": []}

                rows: list[dict[str, Any]] = []
                for strategy in engine.strategies.values():
                    data = strategy.get_data()
                    rows.append(
                        {
                            "strategy_name": data.get("strategy_name", ""),
                            "class_name": data.get("class_name", ""),
                            "vt_symbol": data.get("vt_symbol", ""),
                            "gateway_name": data.get("gateway_name", ""),
                            "inited": bool(data.get("inited", False)),
                            "trading": bool(data.get("trading", False)),
                            "params": _json_safe(data.get("params", {})),
                            "state": _json_safe(data.get("state", {})),
                            "vars": _json_safe(data.get("vars", {})),
                        }
                    )
                rows.sort(key=lambda x: x["strategy_name"])
                return {"ok": True, "kind": kind, "rows": rows}

            if kind == "portfolio":
                engine = self._main_engine.get_engine("PortfolioStrategy")
                if not engine:
                    return {"ok": True, "kind": kind, "rows": []}

                rows = []
                for strategy in engine.strategies.values():
                    data = strategy.get_data()
                    rows.append(
                        {
                            "strategy_name": data.get("strategy_name", ""),
                            "class_name": data.get("class_name", ""),
                            "symbols": _json_safe(data.get("symbols", [])),
                            "vt_symbols": _json_safe(data.get("vt_symbols", [])),
                            "gateway_name": data.get("gateway_name", ""),
                            "inited": bool(data.get("inited", False)),
                            "trading": bool(data.get("trading", False)),
                            "params": _json_safe(data.get("params", {})),
                            "state": _json_safe(data.get("state", {})),
                            "vars": _json_safe(data.get("vars", {})),
                        }
                    )
                rows.sort(key=lambda x: x["strategy_name"])
                return {"ok": True, "kind": kind, "rows": rows}

            engine = self._main_engine.get_engine("ScriptTrader")
            if not engine:
                return {"ok": True, "kind": kind, "rows": []}

            rows = []
            for script_name, runner in engine.scripts.items():
                rows.append(
                    {
                        "script_name": script_name,
                        "script_path": getattr(runner, "script_path", ""),
                        "active": bool(getattr(runner, "strategy_active", False)),
                    }
                )
            rows.sort(key=lambda x: x["script_name"])
            return {"ok": True, "kind": kind, "rows": rows}
        except Exception as exc:  # noqa: BLE001
            self._append_log("ERROR", "Web", f"{kind} 策略列表读取失败: {exc}")
            return {"ok": False, "error": f"{kind} 策略列表读取失败: {exc}"}

    def strategy_action(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Execute strategy action for cta/portfolio/script."""
        action = str(payload.get("action", "")).strip()
        strategy_name = str(payload.get("strategy_name", "")).strip()
        if kind not in {"cta", "portfolio", "script"}:
            return {"ok": False, "error": "不支持的策略类型"}
        if not action:
            return {"ok": False, "error": "action 不能为空"}

        if self.mode != "live":
            return self._mock_strategy_action(kind=kind, action=action, strategy_name=strategy_name)

        try:
            if kind == "cta":
                engine = self._main_engine.get_engine("CtaStrategy")
                if not engine:
                    return {"ok": False, "error": "CTA 引擎未加载"}
                return self._do_cta_action(engine=engine, action=action, strategy_name=strategy_name)

            if kind == "portfolio":
                engine = self._main_engine.get_engine("PortfolioStrategy")
                if not engine:
                    return {"ok": False, "error": "组合策略引擎未加载"}
                return self._do_portfolio_action(engine=engine, action=action, strategy_name=strategy_name)

            engine = self._main_engine.get_engine("ScriptTrader")
            if not engine:
                return {"ok": False, "error": "脚本引擎未加载"}
            return self._do_script_action(engine=engine, action=action, strategy_name=strategy_name)
        except KeyError:
            return {"ok": False, "error": f"策略不存在: {strategy_name}"}
        except Exception as exc:  # noqa: BLE001
            self._append_log("ERROR", "Web", f"策略操作失败 kind={kind} action={action}: {exc}")
            return {"ok": False, "error": f"策略操作失败: {exc}"}

    def _mock_strategy_action(self, kind: str, action: str, strategy_name: str) -> dict[str, Any]:
        data_map = {
            "cta": self._mock_cta_strategies,
            "portfolio": self._mock_portfolio_strategies,
            "script": self._mock_scripts,
        }
        items = data_map[kind]

        def _set_all(key: str, value: bool) -> None:
            for row in items.values():
                row[key] = value

        if kind in {"cta", "portfolio"}:
            if action == "reload":
                self._append_log("INFO", "Mock", f"{kind} 策略类已刷新")
                return {"ok": True}
            if action == "init_all":
                _set_all("inited", True)
                return {"ok": True}
            if action == "start_all":
                _set_all("inited", True)
                _set_all("trading", True)
                return {"ok": True}
            if action == "stop_all":
                _set_all("trading", False)
                return {"ok": True}

            row = items.get(strategy_name)
            if not row:
                return {"ok": False, "error": f"策略不存在: {strategy_name}"}
            if action == "init":
                row["inited"] = True
            elif action == "start":
                row["inited"] = True
                row["trading"] = True
            elif action == "stop":
                row["trading"] = False
            elif action == "reset":
                row["trading"] = False
            else:
                return {"ok": False, "error": f"未知动作: {action}"}
            return {"ok": True}

        if action == "start_all":
            _set_all("active", True)
            return {"ok": True}
        if action == "stop_all":
            _set_all("active", False)
            return {"ok": True}

        row = items.get(strategy_name)
        if not row:
            return {"ok": False, "error": f"脚本不存在: {strategy_name}"}
        if action == "start":
            row["active"] = True
        elif action == "stop":
            row["active"] = False
        else:
            return {"ok": False, "error": f"未知动作: {action}"}
        return {"ok": True}

    def _do_cta_action(self, engine: Any, action: str, strategy_name: str) -> dict[str, Any]:
        if action == "reload":
            engine.reload_strategy_class()
            return {"ok": True}
        if action == "init_all":
            engine.init_all_strategies()
            return {"ok": True}
        if action == "start_all":
            engine.start_all_strategies()
            return {"ok": True}
        if action == "stop_all":
            engine.stop_all_strategies()
            return {"ok": True}
        if not strategy_name:
            return {"ok": False, "error": "strategy_name 不能为空"}
        if action == "init":
            engine.init_strategy(strategy_name)
            return {"ok": True}
        if action == "start":
            engine.start_strategy(strategy_name)
            return {"ok": True}
        if action == "stop":
            engine.stop_strategy(strategy_name)
            return {"ok": True}
        if action == "reset":
            engine.reset_strategy(strategy_name)
            return {"ok": True}
        return {"ok": False, "error": f"未知动作: {action}"}

    def _do_portfolio_action(self, engine: Any, action: str, strategy_name: str) -> dict[str, Any]:
        if action == "reload":
            engine.reload_strategy_class()
            return {"ok": True}
        if action == "init_all":
            engine.init_all_strategies()
            return {"ok": True}
        if action == "start_all":
            engine.start_all_strategies()
            return {"ok": True}
        if action == "stop_all":
            engine.stop_all_strategies()
            return {"ok": True}
        if not strategy_name:
            return {"ok": False, "error": "strategy_name 不能为空"}
        if action == "init":
            engine.init_strategy(strategy_name)
            return {"ok": True}
        if action == "start":
            engine.start_strategy(strategy_name)
            return {"ok": True}
        if action == "stop":
            engine.stop_strategy(strategy_name)
            return {"ok": True}
        if action == "reset":
            engine.reset_strategy(strategy_name)
            return {"ok": True}
        return {"ok": False, "error": f"未知动作: {action}"}

    def _do_script_action(self, engine: Any, action: str, strategy_name: str) -> dict[str, Any]:
        if action == "start_all":
            engine.start_all_scripts()
            return {"ok": True}
        if action == "stop_all":
            engine.stop_all_scripts()
            return {"ok": True}
        if not strategy_name:
            return {"ok": False, "error": "strategy_name 不能为空"}
        if action == "start":
            engine.start_script(strategy_name)
            return {"ok": True}
        if action == "stop":
            engine.stop_script(strategy_name)
            return {"ok": True}
        return {"ok": False, "error": f"未知动作: {action}"}


def create_app(auto_connect: bool = True) -> Flask:
    app = Flask(__name__, template_folder="templates", static_folder="static")
    bridge = RuntimeBridge(auto_connect=auto_connect)
    app.config["bridge"] = bridge

    @app.after_request
    def disable_cache(resp):  # type: ignore[no-untyped-def]
        # Prevent stale frontend assets during active development/deployment switch.
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    @app.get("/")
    def index() -> Any:
        return render_template("index.html")

    @app.get("/push")
    def push_page() -> Any:
        """仅展示实时策略推送的轻量页面。"""
        return render_template("push.html")

    @app.get("/module/<slug>")
    def module_page(slug: str) -> Any:
        if slug not in MODULES:
            return render_template("module.html", title="模块不存在", slug="", modules=MODULES), 404
        runtime = app.config["bridge"]
        return render_template(
            "module.html",
            title=MODULES[slug],
            slug=slug,
            modules=MODULES,
            mode=runtime.mode.upper(),
        )

    @app.get("/api/meta")
    def api_meta() -> Any:
        runtime = app.config["bridge"]
        return jsonify(
            {
                "ok": True,
                "mode": runtime.mode,
                "reason": runtime.reason,
                "envs": runtime.list_envs(),
                "favorites": runtime.favorites(),
            }
        )

    @app.get("/api/home")
    def api_home() -> Any:
        runtime = app.config["bridge"]
        return jsonify({"ok": True, "data": runtime.home()})

    @app.post("/api/connect")
    def api_connect() -> Any:
        payload = request.get_json(silent=True) or {}
        env = str(payload.get("env", "")).strip()
        if not env:
            return jsonify({"ok": False, "error": "env 不能为空"}), 400
        runtime = app.config["bridge"]
        return jsonify(runtime.connect(env))

    @app.post("/api/disconnect")
    def api_disconnect() -> Any:
        payload = request.get_json(silent=True) or {}
        env = str(payload.get("env", "")).strip()
        if not env:
            return jsonify({"ok": False, "error": "env 不能为空"}), 400
        runtime = app.config["bridge"]
        return jsonify(runtime.disconnect(env))

    @app.post("/api/order")
    def api_order() -> Any:
        payload = request.get_json(silent=True) or {}
        runtime = app.config["bridge"]
        result = runtime.send_order(payload)
        return jsonify(result), (200 if result.get("ok") else 400)

    @app.post("/api/order/cancel_all")
    def api_cancel_all() -> Any:
        runtime = app.config["bridge"]
        return jsonify(runtime.cancel_all())

    @app.post("/api/ai/chat")
    def api_ai_chat() -> Any:
        payload = request.get_json(silent=True) or {}
        runtime = app.config["bridge"]
        result = runtime.chat(payload)
        return jsonify(result), (200 if result.get("ok") else 400)

    @app.get("/api/realtime/signals")
    def api_realtime_signals() -> Any:
        raw_limit = request.args.get("limit", "200")
        try:
            limit = int(raw_limit)
        except Exception:
            limit = 200
        data = _load_realtime_snapshot(limit=limit)
        return jsonify({"ok": True, "data": data})

    @app.get("/api/realtime/stream")
    def api_realtime_stream() -> Any:
        raw_limit = request.args.get("limit", "200")
        try:
            limit = int(raw_limit)
        except Exception:
            limit = 200

        headers = {
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
        return Response(
            stream_with_context(_realtime_stream_events(limit=limit)),
            mimetype="text/event-stream",
            headers=headers,
        )

    @app.get("/api/realtime/push-image")
    def api_push_image_meta() -> Any:
        return jsonify({"ok": True, "data": _push_snapshot_meta(PUSH_SNAPSHOT_PNG)})

    @app.post("/api/realtime/push-image/generate")
    def api_push_image_generate() -> Any:
        payload = request.get_json(silent=True) or {}
        try:
            rows = int(payload.get("rows", 24))
        except Exception:
            rows = 24
        rows = max(1, min(rows, 200))
        result = _generate_push_snapshot(rows=rows)
        return jsonify(result), (200 if result.get("ok") else 500)

    @app.get("/api/realtime/push-image/file")
    def api_push_image_file() -> Any:
        if not PUSH_SNAPSHOT_PNG.exists():
            result = _generate_push_snapshot(rows=24)
            if not result.get("ok"):
                return jsonify(result), 500
        return send_file(PUSH_SNAPSHOT_PNG, mimetype="image/png", max_age=0)

    @app.get("/api/realtime/logs")
    def api_realtime_logs() -> Any:
        raw_lines = request.args.get("lines", "120")
        try:
            lines = int(raw_lines)
        except Exception:
            lines = 120
        lines = max(20, min(lines, 400))
        data = _script_log_snapshot(lines=lines)
        return jsonify({"ok": True, "data": data})

    @app.get("/api/realtime/watchlist")
    def api_realtime_watchlist() -> Any:
        return jsonify({"ok": True, "data": _watchlist_editor_snapshot()})

    @app.post("/api/realtime/watchlist/backup")
    def api_realtime_watchlist_backup() -> Any:
        rows = _load_watchlist_rows(WATCHLIST_CSV)
        backup_meta = _write_watchlist_backup(rows)
        payload = _watchlist_editor_snapshot()
        payload.update(
            {
                "backup_file_name": backup_meta["file_name"],
                "backup_updated_at": backup_meta["updated_at"],
            }
        )
        return jsonify({"ok": True, "message": f"已备份 {len(rows)} 只股票代码", "data": payload})

    @app.post("/api/realtime/watchlist/extract-images")
    def api_realtime_watchlist_extract_images() -> Any:
        files = request.files.getlist("images")
        valid_files = [item for item in files if getattr(item, "filename", "")]
        if not valid_files:
            return jsonify({"ok": False, "error": "请先选择至少一张图片"}), 400
        try:
            upload_dir, saved_files = _save_uploaded_watchlist_images(valid_files)
            result = _extract_watchlist_codes_from_images(saved_files)
        except Exception as exc:  # noqa: BLE001
            return jsonify({"ok": False, "error": f"图片提取失败: {exc}"}), 500

        snapshot = _watchlist_editor_snapshot()
        snapshot.update(
            {
                "codes_text": result["codes_text"],
                "ocr_file_count": result["file_count"],
                "ocr_codes_count": result["codes_count"],
                "ocr_raw_total_count": result["raw_total_count"],
                "ocr_duplicates_removed": result["duplicates_removed"],
                "ocr_file_summaries": result["file_summaries"],
                "ocr_import_dir": str(upload_dir.relative_to(PROJECT_ROOT)),
            }
        )
        return jsonify(
            {
                "ok": True,
                "message": (
                    f"已从 {result['file_count']} 张图片提取 {result['codes_count']} 只股票，"
                    f"去重 {result['duplicates_removed']} 个重复代码"
                ),
                "data": snapshot,
            }
        )

    @app.post("/api/realtime/watchlist/save")
    def api_realtime_watchlist_save() -> Any:
        try:
            payload = request.get_json(silent=True) or {}
            raw_text = str(payload.get("codes_text") or "").strip()
            existing_rows = _load_watchlist_rows(WATCHLIST_CSV)
            rows = _parse_watchlist_editor_text(raw_text, existing_rows)
            if not rows:
                return jsonify({"ok": False, "error": "没有识别到可保存的股票代码"}), 400

            previous_backup_meta = _write_watchlist_backup(existing_rows)
            _write_watchlist_rows(rows, WATCHLIST_CSV)
            assignment_meta = _write_assignment_rows_one_to_one(rows, _load_api_tokens(), ASSIGNMENT_CSV)
            next_trade_day = _next_weekday_date()
            daily_meta = _write_daily_watchlist(rows, next_trade_day)
            reload_meta = _reload_scan_service_if_needed()
            snapshot = _watchlist_editor_snapshot()
            snapshot.update(
                {
                    "backup_file_name": previous_backup_meta["file_name"],
                    "backup_updated_at": previous_backup_meta["updated_at"],
                    "daily_watchlist_csv": daily_meta["csv_name"],
                    "daily_watchlist_txt": daily_meta["txt_name"],
                    "daily_assignment_csv": daily_meta["assignment_name"],
                    "assigned_count": assignment_meta["assigned_count"],
                    "unassigned_count": assignment_meta["unassigned_count"],
                    "scan_reload_attempted": reload_meta["attempted"],
                    "scan_reload_restarted": reload_meta["restarted"],
                    "scan_reload_message": reload_meta["message"],
                }
            )
            message_parts = [
                f"已覆盖保存 {len(rows)} 只自选股票",
                f"旧池子已备份为 {previous_backup_meta['file_name']}",
                f"已同步生成 {snapshot['next_trade_label']} 自选池",
                f"已分配 {assignment_meta['assigned_count']} 只，未分配 {assignment_meta['unassigned_count']} 只",
                reload_meta["message"],
            ]
            return jsonify(
                {
                    "ok": True,
                    "message": "，".join(part for part in message_parts if part),
                    "data": snapshot,
                }
            )
        except Exception as exc:  # noqa: BLE001
            return jsonify({"ok": False, "error": f"保存失败: {exc}"}), 500

    @app.get("/api/strategies/<kind>")
    def api_strategy_list(kind: str) -> Any:
        runtime = app.config["bridge"]
        result = runtime.get_strategies(kind)
        return jsonify(result), (200 if result.get("ok") else 400)

    @app.post("/api/strategies/<kind>/action")
    def api_strategy_action(kind: str) -> Any:
        payload = request.get_json(silent=True) or {}
        runtime = app.config["bridge"]
        result = runtime.strategy_action(kind, payload)
        return jsonify(result), (200 if result.get("ok") else 400)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Guanlan web dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8768, type=int)
    parser.add_argument("--no-auto-connect", action="store_true")
    args = parser.parse_args()

    app = create_app(auto_connect=not args.no_auto_connect)
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
