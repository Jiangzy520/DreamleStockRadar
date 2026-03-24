#!/usr/bin/env python3
"""Local web dashboard for futures prices and A/B/C pattern monitoring."""

from __future__ import annotations

import csv
import json
import os
import signal
import subprocess
import sys
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

import requests


BASE_DIR = Path(__file__).resolve().parent

PRICES_CSV = BASE_DIR / "futures_latest_all.csv"
PRICE_ERRORS_CSV = BASE_DIR / "futures_latest_all_errors.csv"
SNAPSHOT_CSV = BASE_DIR / "futures_abc_snapshot.csv"
MATCHES_CSV = BASE_DIR / "futures_abc_matches.csv"
EVENTS_CSV = BASE_DIR / "futures_abc_match_events.csv"
BACKTEST_DIR = BASE_DIR / "backtests"
LIVE_WATCH_DIR = BASE_DIR / "night_watch_live"
WATCH_LOG_NAME = "watch.log"
WATCH_PID_NAME = "watch.pid"
WATCH_INTERVAL_SECONDS = 90
# Keep empty to scan all domestic continuous futures varieties.
WATCH_SYMBOLS: list[str] = []
EXCLUDED_PRICE_CODES = {"IF", "IH", "IC", "IM", "T", "TF", "TL", "TS"}
BACKTEST_DAYS_BACK = 3
BACKTEST_HOLD_BARS = 1
AUTO_START_PREOPEN_MINUTES = 10
AUTO_START_CHECK_INTERVAL_SECONDS = 30
AUTO_START_SESSION_OPENS = [
    {"hour": 9, "minute": 0, "label": "日盘开盘"},
    {"hour": 13, "minute": 30, "label": "午盘开盘"},
    {"hour": 21, "minute": 0, "label": "夜盘开盘"},
]
AUTO_START_STATE = {
    "last_attempt_key": "",
    "last_attempt_monotonic": 0.0,
}
AUTO_START_LOCK = threading.Lock()

ALLTICK_CONFIG_PATH = BASE_DIR / "alltick_tokens.local.json"
ALLTICK_BASE_URL = "https://quote.alltick.co/quote-b-api/trade-tick"
ALLTICK_CACHE_TTL_SECONDS = 25
STOCK_PANEL_URL = os.environ.get("STOCK_PANEL_URL", "http://127.0.0.1:18080/push")
PUBLIC_GITHUB_MODE = os.environ.get("PUBLIC_GITHUB_MODE", "1").strip().lower() not in {"0", "false", "no"}
ALLTICK_SYMBOL_MAP = [
    {"domestic_code": "AG", "label": "白银", "alltick_codes": ["Silver"]},
    {"domestic_code": "AU", "label": "黄金", "alltick_codes": ["GOLD"]},
    {"domestic_code": "AL", "label": "铝", "alltick_codes": ["Aluminum"]},
    {"domestic_code": "CU", "label": "铜", "alltick_codes": ["COPPER"]},
    {"domestic_code": "NI", "label": "镍", "alltick_codes": ["Nickel"]},
    {"domestic_code": "SC", "label": "原油", "alltick_codes": ["UKOIL", "USOIL"]},
]

PROBE_TIMEOUT_SECONDS = 6
PROBE_CACHE_TTL_SECONDS = 45
PROBE_HEADERS = {
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
AKSHARE_PROBE_CACHE: dict[str, object] = {
    "checked_at_monotonic": 0.0,
    "result": {
        "ok": False,
        "status": "未探测",
        "note": "接口探测尚未开始",
        "request_ms": None,
        "symbol": "",
        "quote_name": "",
        "quote_time": "",
        "current_price": "",
    },
}
AKSHARE_PROBE_CACHE_LOCK = threading.Lock()
ALLTICK_PROBE_CACHE: dict[str, object] = {
    "checked_at_monotonic": 0.0,
    "result": {
        "ok": False,
        "status": "未配置",
        "note": "AllTick token 尚未配置",
        "request_ms": None,
        "representative": "",
        "tick_datetime": "",
        "current_price": "",
        "tick_delay_seconds": None,
        "tick_delay_text": "-",
        "tick_delay_note": "缺少 AllTick tick 时间。",
        "tick_delay_level": "bad",
    },
}
ALLTICK_PROBE_CACHE_LOCK = threading.Lock()


HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>期货盯盘面板</title>
  <style>
    :root {
      --bg: #f6f0e4;
      --panel: rgba(255, 250, 242, 0.88);
      --panel-strong: #fff9ee;
      --ink: #102826;
      --muted: #5a6c68;
      --line: rgba(16, 40, 38, 0.12);
      --brand: #0f766e;
      --brand-2: #d97706;
      --good: #166534;
      --bad: #b91c1c;
      --chip: #efe4cf;
      --shadow: 0 18px 50px rgba(16, 40, 38, 0.08);
      --radius: 24px;
      --radius-sm: 14px;
    }

    * { box-sizing: border-box; }
    body {
      margin: 0;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(217, 119, 6, 0.18), transparent 30%),
        radial-gradient(circle at left center, rgba(15, 118, 110, 0.18), transparent 28%),
        linear-gradient(180deg, #f5eee0 0%, #f7f2e8 35%, #f3ecdf 100%);
      font-family: "Noto Sans SC", "PingFang SC", "Microsoft YaHei", sans-serif;
      min-height: 100vh;
    }

    .shell {
      max-width: 1680px;
      margin: 0 auto;
      padding: 28px 20px 60px;
    }

    .hero {
      background: linear-gradient(135deg, rgba(16,40,38,0.96), rgba(15,118,110,0.92));
      color: #f7f4eb;
      border-radius: 30px;
      padding: 28px;
      box-shadow: var(--shadow);
      position: relative;
      overflow: hidden;
    }

    .hero::after {
      content: "";
      position: absolute;
      inset: auto -80px -120px auto;
      width: 260px;
      height: 260px;
      background: radial-gradient(circle, rgba(245, 158, 11, 0.25), transparent 65%);
      pointer-events: none;
    }

    .hero h1 {
      margin: 0 0 10px;
      font-size: clamp(28px, 4vw, 42px);
      line-height: 1.04;
      letter-spacing: 0.02em;
    }

    .hero p {
      margin: 0;
      max-width: 860px;
      color: rgba(247, 244, 235, 0.84);
      font-size: 15px;
      line-height: 1.6;
    }

    .hero-row {
      margin-top: 22px;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }

    .chip {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 10px 14px;
      border-radius: 999px;
      background: rgba(255, 249, 238, 0.12);
      border: 1px solid rgba(255, 249, 238, 0.14);
      color: #fff7ea;
      font-size: 13px;
    }

    .toolbar {
      margin-top: 18px;
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
    }

    button,
    .toolbar-link {
      border: 0;
      border-radius: 999px;
      padding: 12px 18px;
      font-size: 14px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 120ms ease, opacity 120ms ease, box-shadow 120ms ease;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      text-decoration: none;
    }

    button:hover,
    .toolbar-link:hover { transform: translateY(-1px); }
    button:disabled { opacity: 0.65; cursor: wait; transform: none; }

    .primary {
      background: linear-gradient(135deg, #f59e0b, #d97706);
      color: #fffdf8;
      box-shadow: 0 10px 24px rgba(217, 119, 6, 0.22);
    }

    .secondary {
      background: rgba(255, 249, 238, 0.12);
      color: #fff7ea;
      border: 1px solid rgba(255, 249, 238, 0.14);
    }

    .toolbar-link.secondary {
      background: rgba(255, 249, 238, 0.12);
      color: #fff7ea;
      border: 1px solid rgba(255, 249, 238, 0.14);
    }
    .public-doc {
      margin-top: 18px;
      padding: 22px;
      border-radius: 24px;
      background: rgba(255, 250, 242, 0.88);
      border: 1px solid var(--line);
      box-shadow: var(--shadow);
    }

    .public-doc h2 {
      margin: 0 0 8px;
      font-size: 24px;
      color: var(--ink);
    }

    .public-doc p {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
      line-height: 1.8;
    }

    .public-doc-grid {
      margin-top: 16px;
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 14px;
    }

    .public-doc-item {
      padding: 16px;
      border-radius: 18px;
      background: rgba(247, 241, 229, 0.92);
      border: 1px solid rgba(16, 40, 38, 0.1);
    }

    .public-doc-item strong {
      display: block;
      margin-bottom: 6px;
      font-size: 16px;
    }

    .public-doc-item span {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }

    .public-doc-tip {
      margin-top: 14px;
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(15, 118, 110, 0.08);
      border: 1px solid rgba(15, 118, 110, 0.14);
      color: #295b56;
      font-size: 13px;
      line-height: 1.7;
    }

    .grid {
      margin-top: 22px;
      display: grid;
      gap: 18px;
      grid-template-columns: repeat(12, minmax(0, 1fr));
    }

    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(12px);
      overflow: hidden;
    }

    .metric {
      padding: 20px;
      min-height: 148px;
      position: relative;
    }

    .metric::before {
      content: "";
      position: absolute;
      inset: 0 auto auto 0;
      width: 100%;
      height: 4px;
      background: linear-gradient(90deg, var(--brand), var(--brand-2));
      opacity: 0.75;
    }

    .metric h2 {
      margin: 0 0 10px;
      font-size: 14px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: var(--muted);
    }

    .metric .value {
      font-size: clamp(30px, 4vw, 44px);
      line-height: 1;
      font-weight: 800;
      margin-bottom: 10px;
    }

    .metric .note {
      font-size: 14px;
      line-height: 1.55;
      color: var(--muted);
    }

    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-5 { grid-column: span 5; }
    .span-6 { grid-column: span 6; }
    .span-7 { grid-column: span 7; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }

    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      padding: 20px 22px 12px;
      border-bottom: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(255,249,238,0.8), rgba(255,249,238,0.18));
    }

    .section-head h3 {
      margin: 0;
      font-size: 18px;
      letter-spacing: 0.01em;
    }

    .section-head .meta {
      color: var(--muted);
      font-size: 13px;
      white-space: nowrap;
    }

    .section-tools {
      display: flex;
      align-items: center;
      justify-content: flex-end;
      gap: 10px;
      flex-wrap: wrap;
    }

    .collapse-toggle {
      appearance: none;
      border: 1px solid rgba(15, 118, 110, 0.2);
      background: rgba(255, 255, 255, 0.72);
      color: var(--text);
      border-radius: 999px;
      padding: 6px 12px;
      font-size: 12px;
      font-weight: 700;
      line-height: 1;
      cursor: pointer;
      transition: background 160ms ease, border-color 160ms ease, transform 160ms ease;
    }

    .collapse-toggle:hover {
      background: rgba(255, 255, 255, 0.92);
      border-color: rgba(15, 118, 110, 0.34);
      transform: translateY(-1px);
    }

    .collapse-toggle:focus-visible {
      outline: 2px solid rgba(15, 118, 110, 0.28);
      outline-offset: 2px;
    }

    .collapsible-card.is-collapsed .section-head {
      border-bottom: none;
    }

    .collapsible-card.is-collapsed .collapsible-body {
      display: none;
    }

    .table-wrap {
      overflow: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      min-width: 900px;
    }

    table.fit-table {
      min-width: 0;
      table-layout: fixed;
    }

    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      font-size: 13px;
      white-space: nowrap;
    }

    th {
      position: sticky;
      top: 0;
      z-index: 1;
      background: #f7f1e5;
      color: var(--muted);
      font-weight: 700;
    }

    tr:hover td {
      background: rgba(15, 118, 110, 0.05);
    }

    table.fit-table th,
    table.fit-table td {
      padding: 10px 12px;
      font-size: 12px;
      line-height: 1.35;
      vertical-align: top;
    }

    table.fit-table td {
      white-space: normal;
      overflow-wrap: anywhere;
    }

    .datetime-cell {
      display: inline-flex;
      flex-direction: column;
      gap: 2px;
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }

    .datetime-cell .date-line,
    .datetime-cell .time-line {
      display: block;
    }

    .number-cell {
      white-space: nowrap;
      font-variant-numeric: tabular-nums;
    }

    .empty {
      padding: 28px 22px;
      color: var(--muted);
      font-size: 14px;
    }

    .log {
      margin: 0;
      padding: 18px 20px 24px;
      background: #111d1c;
      color: #d8efe8;
      min-height: 280px;
      overflow: auto;
      font-family: "JetBrains Mono", "Cascadia Mono", "SFMono-Regular", monospace;
      font-size: 12px;
      line-height: 1.6;
    }

    .formula {
      padding: 18px 22px 24px;
      display: grid;
      gap: 8px;
      font-size: 14px;
      line-height: 1.65;
    }

    .formula code {
      background: var(--chip);
      padding: 3px 6px;
      border-radius: 8px;
      font-size: 13px;
    }

    .flash {
      position: fixed;
      right: 18px;
      bottom: 18px;
      min-width: 220px;
      max-width: min(420px, calc(100vw - 36px));
      background: rgba(16, 40, 38, 0.95);
      color: #f7f4eb;
      padding: 14px 16px;
      border-radius: 16px;
      box-shadow: var(--shadow);
      opacity: 0;
      transform: translateY(8px);
      transition: opacity 180ms ease, transform 180ms ease;
      pointer-events: none;
    }

    .flash.show {
      opacity: 1;
      transform: translateY(0);
    }

    .status-good { color: var(--good); }
    .status-warn { color: var(--brand-2); }
    .status-bad { color: var(--bad); }

    .muted { color: var(--muted); }

    .error-banner {
      display: none;
      margin: 18px 0 0;
      padding: 14px 16px;
      border-radius: 16px;
      background: rgba(185, 28, 28, 0.12);
      border: 1px solid rgba(185, 28, 28, 0.22);
      color: #7f1d1d;
      font-size: 14px;
      line-height: 1.5;
    }

    .error-banner.show {
      display: block;
    }

    @media (max-width: 1100px) {
      .span-3, .span-4, .span-5, .span-6, .span-7, .span-8 { grid-column: span 12; }
      .hero { padding: 22px; }
      .public-doc-grid { grid-template-columns: 1fr; }
      .section-head {
        align-items: flex-start;
      }
      .section-tools {
        width: 100%;
        justify-content: space-between;
      }

      table.fit-table {
        min-width: 760px;
        table-layout: auto;
      }

      table.fit-table th,
      table.fit-table td {
        padding: 12px 14px;
        font-size: 13px;
      }
    }
  </style>
</head>
<body class="public-github">
  <div class="shell">
    <section class="hero">
      <h1>期货盯盘面板</h1>
      <p>当前本地公开版已经隐藏真实行情记录、策略命中、夜盘任务状态和回测结果，仅保留页面结构与入口说明，方便上传 GitHub。</p>
      <div class="hero-row">
        <span class="chip">回测：三交易日前历史</span>
        <span class="chip">自动预启动：开盘前 10 分钟</span>
      </div>
      <div class="toolbar">
        <button id="runAllBtn" class="primary">全部刷新</button>
        <button id="runScanBtn" class="secondary">重跑今天历史</button>
        <button id="runPricesBtn" class="secondary">更新全部最新价</button>
        <button id="runBacktestBtn" class="secondary">回测三天前历史</button>
        <a class="toolbar-link secondary" href="__STOCK_PANEL_URL__">返回股票信号页</a>
        <button id="refreshBtn" class="secondary">刷新页面</button>
      </div>
      <div id="loadError" class="error-banner"></div>
    </section>

    <section class="public-doc">
      <h2>公开版文档信息</h2>
      <p>
        这份期货信号页已切换为 GitHub 公开展示模式。真实行情记录、命中事件、回测结果、夜盘任务与生成日志已从页面展示中移除，
        只保留风格、布局和入口说明。
      </p>
      <div class="public-doc-grid">
        <div class="public-doc-item">
          <strong>页面结构保留</strong>
          <span>保留期货盯盘页的整体样式、入口按钮和模块布局，方便公开说明项目能力。</span>
        </div>
        <div class="public-doc-item">
          <strong>敏感记录移除</strong>
          <span>价格快照、历史命中、夜盘任务、回测数据、表格记录和日志面板均不在公开页显示。</span>
        </div>
        <div class="public-doc-item">
          <strong>私有环境恢复</strong>
          <span>如需恢复真实数据，请在私有环境中重新接入你的 CSV、日志、任务进程和行情服务。</span>
        </div>
      </div>
      <div class="public-doc-tip">
        当前页面适合直接提交到 GitHub 或给他人预览前端结构，不会再从这份公开版中读出任何真实交易或策略信息。
      </div>
    </section>

    <section class="grid sensitive-section">
      <article class="card metric span-3">
        <h2>最新价覆盖</h2>
        <div class="value" id="metricPrices">-</div>
        <div class="note" id="metricPricesNote">读取中...</div>
      </article>
      <article class="card metric span-3">
        <h2>历史命中</h2>
        <div class="value" id="metricEvents">-</div>
        <div class="note" id="metricEventsNote">读取中...</div>
      </article>
      <article class="card metric span-3">
        <h2>最新三根命中</h2>
        <div class="value" id="metricMatches">-</div>
        <div class="note" id="metricMatchesNote">读取中...</div>
      </article>
      <article class="card metric span-3">
        <h2>夜盘任务</h2>
        <div class="value" id="metricWatch">-</div>
        <div class="note" id="metricWatchNote">读取中...</div>
      </article>

      <article class="card metric span-3">
        <h2>AkShare接口</h2>
        <div class="value" id="metricAkshareApi">-</div>
        <div class="note" id="metricAkshareApiNote">读取中...</div>
      </article>

      <article class="card metric span-3">
        <h2>AkShare Tick延迟</h2>
        <div class="value" id="metricAkshareTick">-</div>
        <div class="note" id="metricAkshareTickNote">读取中...</div>
      </article>

      <article class="card metric span-3">
        <h2>AllTick接口</h2>
        <div class="value" id="metricAlltickApi">-</div>
        <div class="note" id="metricAlltickApiNote">读取中...</div>
      </article>

      <article class="card metric span-3">
        <h2>AllTick Tick延迟</h2>
        <div class="value" id="metricAlltickTick">-</div>
        <div class="note" id="metricAlltickTickNote">读取中...</div>
      </article>

      <article class="card span-7">
        <div class="section-head">
          <h3>今天命中事件</h3>
          <div class="meta" id="eventsMeta">-</div>
        </div>
        <div class="table-wrap" id="eventsTableWrap"></div>
      </article>

      <article class="card span-5 collapsible-card is-collapsed" id="strategyCard">
        <div class="section-head">
          <h3>策略定义</h3>
          <div class="section-tools">
            <div class="meta">1分钟 K 线</div>
            <button type="button" class="collapse-toggle" data-collapse-target="strategyCard" aria-expanded="false">展开面板</button>
          </div>
        </div>
        <div class="collapsible-body">
          <div class="formula">
            <div><code>A.close &lt; A.open</code> A 为阴线</div>
            <div><code>B.close &gt; B.open</code> B 为阳线</div>
            <div><code>C.close &lt; C.open</code> C 为阴线</div>
            <div><code>C.low &gt;= B.low</code> C 线最低价不能低于 B 线最低价</div>
            <div><code>当前版本不再要求 B 突破 A</code></div>
          </div>
        </div>
      </article>

      <article class="card span-12">
        <div class="section-head">
          <h3>三交易日前历史明细</h3>
          <div class="meta" id="backtestMeta">-</div>
        </div>
        <div class="formula" id="backtestSummaryNote">还没有历史明细。</div>
        <div class="table-wrap" id="backtestSummaryWrap"></div>
      </article>

      <article class="card span-12">
        <div class="section-head">
          <h3>全部期货最新价（不含股指/国债）</h3>
          <div class="meta" id="pricesMeta">-</div>
        </div>
        <div class="table-wrap" id="pricesTableWrap"></div>
      </article>

      <article class="card span-6">
        <div class="section-head">
          <h3>最新三根命中</h3>
          <div class="meta" id="matchesMeta">-</div>
        </div>
        <div class="table-wrap" id="matchesTableWrap"></div>
      </article>

      <article class="card span-6">
        <div class="section-head">
          <h3>分钟线异常品种</h3>
          <div class="meta" id="snapshotErrorsMeta">-</div>
        </div>
        <div class="table-wrap" id="snapshotErrorsWrap"></div>
      </article>

      <article class="card span-6">
        <div class="section-head">
          <h3>最新价异常品种</h3>
          <div class="meta" id="priceErrorsMeta">-</div>
        </div>
        <div class="table-wrap" id="priceErrorsWrap"></div>
      </article>

      <article class="card span-6">
        <div class="section-head">
          <h3>夜盘任务日志</h3>
          <div class="meta" id="watchMeta">-</div>
        </div>
        <pre class="log" id="watchLog">加载中...</pre>
      </article>
    </section>
  </div>

  <div id="flash" class="flash"></div>
  <script id="initialData" type="application/json">__INITIAL_DATA__</script>

  <script>
    const state = { loading: false };
    const API_BASE = window.location.pathname === "/" ? "" : window.location.pathname.replace(/\/$/, "");
    const initialData = JSON.parse(document.getElementById("initialData").textContent);

    function apiUrl(path) {
      return `${API_BASE}${path}`;
    }

    function setLoadError(message = "") {
      const el = document.getElementById("loadError");
      if (!message) {
        el.textContent = "";
        el.className = "error-banner";
        return;
      }
      el.textContent = message;
      el.className = "error-banner show";
    }

    function showFlash(message, type = "info") {
      const el = document.getElementById("flash");
      el.textContent = message;
      el.className = "flash show";
      if (type === "error") {
        el.style.background = "rgba(153, 27, 27, 0.96)";
      } else if (type === "success") {
        el.style.background = "rgba(21, 94, 60, 0.96)";
      } else {
        el.style.background = "rgba(16, 40, 38, 0.95)";
      }
      clearTimeout(showFlash._timer);
      showFlash._timer = setTimeout(() => {
        el.className = "flash";
      }, 2800);
    }

    function setButtonsDisabled(disabled) {
      state.loading = disabled;
      for (const id of ["runAllBtn", "runScanBtn", "runPricesBtn", "runBacktestBtn", "refreshBtn"]) {
        document.getElementById(id).disabled = disabled;
      }
    }

    function formatCount(value) {
      if (value === null || value === undefined || value === "") return "-";
      return String(value);
    }

    function formatTableCell(key, value, compactMode) {
      const text = value === null || value === undefined ? "" : String(value);
      if (!compactMode) return text;

      if ((key.endsWith("_time") || key === "scan_time") && text.includes(" ")) {
        const [datePart, timePart] = text.split(" ", 2);
        return `<span class="datetime-cell"><span class="date-line">${datePart}</span><span class="time-line">${timePart || ""}</span></span>`;
      }

      if (["A_high", "B_low", "C_low", "C_close", "latest_price", "match_count"].includes(key)) {
        return `<span class="number-cell">${text}</span>`;
      }

      return text;
    }

    function renderTable(containerId, rows, columns, emptyText, options = {}) {
      const wrap = document.getElementById(containerId);
      if (!rows || rows.length === 0) {
        wrap.innerHTML = `<div class="empty">${emptyText}</div>`;
        return;
      }
      const compactMode = Boolean(options.compactMode);
      const cols = columns.filter((key) => rows.some((row) => row[key] !== undefined && row[key] !== ""));
      const thead = cols.map((key) => `<th>${key}</th>`).join("");
      const tbody = rows.map((row) => {
        const tds = cols.map((key) => `<td>${formatTableCell(key, row[key], compactMode)}</td>`).join("");
        return `<tr>${tds}</tr>`;
      }).join("");
      wrap.innerHTML = `<table class="${compactMode ? "fit-table" : ""}"><thead><tr>${thead}</tr></thead><tbody>${tbody}</tbody></table>`;
    }

    function renderMetrics(summary) {
      document.getElementById("metricPrices").textContent = summary.price_rows_text || formatCount(summary.price_rows);
      document.getElementById("metricPricesNote").textContent =
        summary.price_note || `最新价异常 ${summary.price_error_rows} 个，最近刷新 ${summary.prices_updated_at || "未知"}`;

      document.getElementById("metricEvents").textContent = summary.event_rows_text || formatCount(summary.event_rows);
      document.getElementById("metricEventsNote").textContent =
        summary.event_note || summary.latest_event_summary || "今天还没有历史命中事件";

      document.getElementById("metricMatches").textContent = summary.match_rows_text || formatCount(summary.match_rows);
      document.getElementById("metricMatchesNote").textContent =
        summary.match_note || (summary.match_rows > 0 ? "当前最后三根已命中" : "当前最后三根没有命中");

      document.getElementById("metricWatch").textContent = summary.watch_status_text || (summary.watch_running ? "运行中" : "未运行");
      document.getElementById("metricWatch").className =
        `value ${summary.watch_status_level || (summary.watch_running ? "status-good" : "status-bad")}`.trim();
      document.getElementById("metricWatchNote").textContent =
        summary.watch_note || "未发现夜盘监控任务";

      document.getElementById("metricAkshareApi").textContent = summary.akshare_api_status || "未知";
      document.getElementById("metricAkshareApi").className =
        `value ${summary.akshare_api_level || (summary.akshare_api_ok ? "status-good" : "status-bad")}`.trim();
      document.getElementById("metricAkshareApiNote").textContent =
        summary.akshare_api_note || "最近一次 AkShare 探测尚未完成";

      const akshareTickClass =
        summary.akshare_tick_delay_level === "good" ? "status-good" :
        summary.akshare_tick_delay_level === "warn" ? "status-warn" :
        summary.akshare_tick_delay_level === "bad" ? "status-bad" : "";
      document.getElementById("metricAkshareTick").textContent = summary.akshare_tick_delay_text || "-";
      document.getElementById("metricAkshareTick").className = `value ${akshareTickClass}`.trim();
      document.getElementById("metricAkshareTickNote").textContent =
        summary.akshare_tick_delay_note || "缺少可用于估算 AkShare 延迟的时间";

      document.getElementById("metricAlltickApi").textContent = summary.alltick_api_status || "未知";
      document.getElementById("metricAlltickApi").className =
        `value ${summary.alltick_api_level || (summary.alltick_api_ok ? "status-good" : "status-bad")}`.trim();
      document.getElementById("metricAlltickApiNote").textContent =
        summary.alltick_api_note || "最近一次 AllTick 探测尚未完成";

      const alltickTickClass =
        summary.alltick_tick_delay_level === "good" ? "status-good" :
        summary.alltick_tick_delay_level === "warn" ? "status-warn" :
        summary.alltick_tick_delay_level === "bad" ? "status-bad" : "";
      document.getElementById("metricAlltickTick").textContent = summary.alltick_tick_delay_text || "-";
      document.getElementById("metricAlltickTick").className = `value ${alltickTickClass}`.trim();
      document.getElementById("metricAlltickTickNote").textContent =
        summary.alltick_tick_delay_note || "缺少可用于估算 AllTick 延迟的时间";
    }

    function renderData(payload) {
      const publicMode = Boolean(payload.public_mode);
      renderMetrics(payload.summary);

      document.getElementById("eventsMeta").textContent = publicMode
        ? "公开版已隐藏历史命中记录"
        : `共 ${payload.summary.event_rows} 条，最近扫描 ${payload.summary.scan_updated_at || "未知"}`;
      document.getElementById("pricesMeta").textContent = publicMode
        ? "公开版已隐藏最新价明细"
        : `共 ${payload.summary.price_rows} 条，最近刷新 ${payload.summary.prices_updated_at || "未知"}`;
      document.getElementById("matchesMeta").textContent = publicMode
        ? "公开版已隐藏最后三根命中"
        : `共 ${payload.summary.match_rows} 条`;
      document.getElementById("snapshotErrorsMeta").textContent = publicMode
        ? "公开版已隐藏异常明细"
        : `共 ${payload.snapshot_errors.length} 条`;
      document.getElementById("priceErrorsMeta").textContent = publicMode
        ? "公开版已隐藏异常明细"
        : `共 ${payload.price_errors.length} 条`;
      document.getElementById("watchMeta").textContent =
        payload.watch.command || (publicMode ? "公开版已隐藏夜盘任务" : "等待夜盘任务启动");

      const backtestSummary = payload.backtest && payload.backtest.summary ? payload.backtest.summary : {};
      document.getElementById("backtestMeta").textContent = publicMode
        ? "公开版已隐藏回测结果"
        : backtestSummary.updated_at
          ? `目标日 ${backtestSummary.trade_date || "-"}，更新 ${backtestSummary.updated_at}`
          : `目标日 ${backtestSummary.trade_date || "-"}`;
      document.getElementById("backtestSummaryNote").innerHTML = publicMode
        ? [
            backtestSummary.public_note || "公开版已隐藏三交易日前历史明细。",
            "页面结构与入口已保留，方便上传 GitHub 时演示前端布局。",
            "如需恢复真实回测结果，请在私有环境重新接入你的历史 CSV 与脚本输出。",
          ].map((line) => `<div>${line}</div>`).join("")
        : [
            backtestSummary.available
              ? `这里直接按“历史命中”的样子展示三交易日前的信号明细。`
              : "还没有历史明细，点击上方“回测三天前历史”即可生成。",
            backtestSummary.available
              ? `当前共找到 ${backtestSummary.total_trades} 条信号，页面展示最近 ${backtestSummary.rows_shown || 0} 条。`
              : `目标日期：${backtestSummary.trade_date || "-"}`,
            backtestSummary.available
              ? `成功取到分钟线的品种 ${backtestSummary.symbols_ok} 个，拉取失败 ${backtestSummary.symbols_failed} 个。`
              : `目标日期：${backtestSummary.trade_date || "-"}`,
          ].map((line) => `<div>${line}</div>`).join("");

      renderTable(
        "eventsTableWrap",
        payload.events,
        ["连续代码", "交易所", "品种", "A_time", "B_time", "C_time", "A_high", "B_low", "C_low", "C_close"],
        publicMode ? "公开版已隐藏今天命中事件。" : "今天还没有命中事件。",
        { compactMode: true }
      );
      renderTable(
        "pricesTableWrap",
        payload.prices,
        ["交易所", "品种", "连续代码", "名称", "时间", "开盘", "最高", "最低", "最新价", "持仓量", "成交量"],
        publicMode ? "公开版已隐藏全部期货最新价明细。" : "还没有最新价数据。"
      );
      renderTable(
        "matchesTableWrap",
        payload.matches,
        ["连续代码", "交易所", "品种", "latest_bar_time", "latest_price", "match_count", "A_time", "B_time", "C_time", "B_low", "C_low"],
        publicMode ? "公开版已隐藏最后三根 K 线命中明细。" : "当前最后三根 K 线没有命中。",
        { compactMode: true }
      );
      renderTable(
        "snapshotErrorsWrap",
        payload.snapshot_errors,
        ["连续代码", "交易所", "品种", "error", "scan_time"],
        publicMode ? "公开版已隐藏分钟线异常品种明细。" : "分钟线没有异常。"
      );
      renderTable(
        "priceErrorsWrap",
        payload.price_errors,
        ["交易所", "品种", "连续代码", "错误"],
        publicMode ? "公开版已隐藏最新价异常品种明细。" : "最新价没有异常。"
      );
      renderTable(
        "backtestSummaryWrap",
        payload.backtest.rows,
        [
          "连续代码",
          "交易所",
          "品种",
          "A_time",
          "B_time",
          "C_time",
          "A_high",
          "B_low",
          "C_low",
          "C_close",
        ],
        publicMode ? "公开版已隐藏三交易日前历史明细。" : "还没有历史明细。",
        { compactMode: true }
      );

      document.getElementById("watchLog").textContent =
        payload.watch.log_tail || (publicMode ? "公开版不展示夜盘任务日志。" : "日志为空。");
    }

    async function loadData(silent = false) {
      try {
        const res = await fetch(apiUrl("/api/data"));
        const payload = await res.json();
        if (!res.ok) throw new Error(payload.error || "读取数据失败");
        renderData(payload);
        setLoadError("");
        if (!silent) showFlash("页面数据已刷新", "success");
      } catch (error) {
        setLoadError(`自动刷新失败：${error.message || "读取数据失败"}。页面先显示最近一次可用数据。`);
        showFlash(error.message || "读取数据失败", "error");
      }
    }

    async function postAction(path, successText) {
      if (state.loading) return;
      setButtonsDisabled(true);
      try {
        const res = await fetch(apiUrl(path), { method: "POST" });
        const payload = await res.json();
        if (!res.ok || payload.ok === false) {
          throw new Error(payload.error || "操作失败");
        }
        await loadData(true);
        if (payload.public_mode && payload.message) {
          showFlash(payload.message, "info");
          return;
        }
        showFlash(successText, "success");
      } catch (error) {
        showFlash(error.message || "操作失败", "error");
      } finally {
        setButtonsDisabled(false);
      }
    }

    function initCollapsiblePanels() {
      document.querySelectorAll(".collapse-toggle[data-collapse-target]").forEach((button) => {
        if (button.dataset.bound === "1") return;
        const card = document.getElementById(button.dataset.collapseTarget);
        if (!card) return;
        const syncButton = () => {
          const collapsed = card.classList.contains("is-collapsed");
          button.textContent = collapsed ? "展开面板" : "折叠面板";
          button.setAttribute("aria-expanded", String(!collapsed));
        };
        button.addEventListener("click", () => {
          card.classList.toggle("is-collapsed");
          syncButton();
        });
        button.dataset.bound = "1";
        syncButton();
      });
    }

    initCollapsiblePanels();
    document.getElementById("refreshBtn").addEventListener("click", () => loadData());
    document.getElementById("runScanBtn").addEventListener("click", () => postAction("/api/run-scan", "今天历史已重跑"));
    document.getElementById("runPricesBtn").addEventListener("click", () => postAction("/api/run-prices", "全部最新价已更新"));
    document.getElementById("runBacktestBtn").addEventListener("click", () => postAction("/api/run-backtest-3d", "三天前历史回测已完成"));
    document.getElementById("runAllBtn").addEventListener("click", () => postAction("/api/run-all", "全部数据已刷新"));

    if (initialData && initialData.summary) {
      renderData(initialData);
    }
    loadData(true);
    setInterval(() => loadData(true), 20000);
  </script>
</body>
</html>
"""


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def read_file_mtime(path: Path) -> str:
    if not path.exists():
        return ""
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")


def format_pct_text(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return "-"
    return f"{float(value):.{digits}f}%"


def build_public_dashboard_data() -> dict[str, object]:
    trade_date = backtest_trade_date()
    return {
        "public_mode": True,
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "public_mode": True,
            "price_rows": 0,
            "price_rows_text": "-",
            "price_error_rows": 0,
            "snapshot_rows": 0,
            "snapshot_error_rows": 0,
            "match_rows": 0,
            "match_rows_text": "-",
            "event_rows": 0,
            "event_rows_text": "-",
            "prices_updated_at": "公开版已隐藏",
            "scan_updated_at": "公开版已隐藏",
            "price_note": "公开版已隐藏最新价覆盖与明细。",
            "event_note": "公开版已隐藏今天命中事件。",
            "match_note": "公开版已隐藏最后三根命中结果。",
            "watch_running": False,
            "watch_status_text": "公开版",
            "watch_status_level": "",
            "watch_note": "公开版已隐藏夜盘任务状态与调度结果。",
            "latest_event_summary": "公开版已隐藏真实命中摘要。",
            "akshare_api_ok": False,
            "akshare_api_status": "公开版",
            "akshare_api_level": "",
            "akshare_api_note": "公开版不探测真实 AkShare 接口状态。",
            "akshare_api_request_ms": None,
            "akshare_tick_delay_seconds": None,
            "akshare_tick_delay_text": "-",
            "akshare_tick_delay_note": "公开版不展示真实 AkShare Tick 延迟。",
            "akshare_tick_delay_level": "",
            "alltick_api_ok": False,
            "alltick_api_status": "公开版",
            "alltick_api_level": "",
            "alltick_api_note": "公开版不探测真实 AllTick 接口状态。",
            "alltick_api_request_ms": None,
            "alltick_tick_delay_seconds": None,
            "alltick_tick_delay_text": "-",
            "alltick_tick_delay_note": "公开版不展示真实 AllTick Tick 延迟。",
            "alltick_tick_delay_level": "",
        },
        "prices": [],
        "price_errors": [],
        "snapshot_errors": [],
        "matches": [],
        "events": [],
        "backtest": {
            "summary": {
                "available": False,
                "trade_date": trade_date,
                "updated_at": "",
                "hold_bars": BACKTEST_HOLD_BARS,
                "rows_shown": 0,
                "symbols_ok": 0,
                "symbols_failed": 0,
                "positive_symbols": 0,
                "total_trades": 0,
                "win_rate_pct": 0.0,
                "win_rate_pct_text": "0.00%",
                "avg_net_return_pct": 0.0,
                "avg_net_return_pct_text": "0.0000%",
                "total_net_return_pct": 0.0,
                "total_net_return_pct_text": "0.0000%",
                "public_note": "公开版已隐藏三交易日前历史明细与回测结果。",
            },
            "rows": [],
            "failed": [],
        },
        "watch": {
            "pid": None,
            "running": False,
            "command": "公开版已隐藏夜盘任务入口",
            "dir": str(BASE_DIR),
            "log_tail": "公开版不展示夜盘任务日志。",
            "log_updated_at": "",
        },
    }


def tail_lines(path: Path, line_count: int = 120) -> str:
    if not path.exists():
        return ""
    lines: deque[str] = deque(maxlen=line_count)
    with path.open("r", encoding="utf-8-sig", errors="replace") as f:
        for line in f:
            lines.append(line.rstrip("\n"))
    return "\n".join(lines)


def parse_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def is_pid_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def get_process_command(pid: int | None) -> str:
    if not pid or not is_pid_running(pid):
        return ""
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "args="],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def filter_error_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if str(row.get("error", "")).strip()]


def is_excluded_price_row(row: dict[str, str]) -> bool:
    code = str(row.get("代码", "")).strip().upper()
    continuous_code = str(row.get("连续代码", "")).strip().upper()
    if code in EXCLUDED_PRICE_CODES:
        return True
    return any(continuous_code == f"{item}0" for item in EXCLUDED_PRICE_CODES)


def filter_excluded_price_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in rows if not is_excluded_price_row(row)]


def shift_trade_date(trade_date: str, days_back: int) -> str:
    current = datetime.strptime(trade_date, "%Y%m%d")
    for _ in range(days_back):
        current -= timedelta(days=1)
        while current.weekday() > 4:
            current -= timedelta(days=1)
    return current.strftime("%Y%m%d")


def stable_unique_texts(values: list[str]) -> list[str]:
    seen: set[str] = set()
    unique_values: list[str] = []
    for value in values:
        text = value.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        unique_values.append(text)
    return unique_values


def watch_pid_path(watch_dir: Path) -> Path:
    return watch_dir / WATCH_PID_NAME


def watch_log_path(watch_dir: Path) -> Path:
    return watch_dir / WATCH_LOG_NAME


def list_known_watch_dirs() -> list[Path]:
    dirs: list[Path] = []
    if LIVE_WATCH_DIR.exists():
        dirs.append(LIVE_WATCH_DIR)
    legacy_dirs = sorted(
        [
            path
            for path in BASE_DIR.glob("night_watch_*")
            if path.is_dir() and path.name not in {"night_watch_test", LIVE_WATCH_DIR.name}
        ],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    dirs.extend(legacy_dirs)
    return dirs


def build_watch_context(watch_dir: Path) -> dict[str, object]:
    pid_path = watch_pid_path(watch_dir)
    log_path = watch_log_path(watch_dir)
    pid = parse_pid(pid_path)
    running = is_pid_running(pid)
    command = get_process_command(pid) if running else ""
    return {
        "dir": watch_dir,
        "pid_path": pid_path,
        "log_path": log_path,
        "pid": pid,
        "running": running,
        "command": command,
    }


def resolve_watch_context() -> dict[str, object]:
    contexts = [build_watch_context(path) for path in list_known_watch_dirs()]
    for context in contexts:
        if context["running"]:
            return context
    if LIVE_WATCH_DIR.exists():
        live_context = build_watch_context(LIVE_WATCH_DIR)
        live_snapshot_path = LIVE_WATCH_DIR / "futures_abc_snapshot.csv"
        live_rows = read_csv_rows(live_snapshot_path)
        if live_rows and not all(str(row.get("error", "")).strip() for row in live_rows):
            return live_context
    for context in contexts:
        if Path(context["dir"]) != LIVE_WATCH_DIR:
            return context
    if (BASE_DIR / "futures_abc_snapshot.csv").exists():
        return build_watch_context(BASE_DIR)
    if LIVE_WATCH_DIR.exists() or not contexts:
        return build_watch_context(LIVE_WATCH_DIR)
    return contexts[0]


def watch_output_path(filename: str, watch_context: dict[str, object]) -> Path:
    watch_dir = Path(watch_context["dir"])
    candidate = watch_dir / filename
    if candidate.exists():
        return candidate
    return BASE_DIR / filename


def load_json_file(path: Path) -> object | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_alltick_tokens() -> list[str]:
    tokens: list[str] = []
    env_tokens = os.environ.get("ALLTICK_TOKENS", "")
    if env_tokens.strip():
        tokens.extend(item.strip() for item in env_tokens.split(","))

    config_payload = load_json_file(ALLTICK_CONFIG_PATH)
    if isinstance(config_payload, dict):
        config_tokens = config_payload.get("tokens", [])
        if isinstance(config_tokens, list):
            tokens.extend(str(item).strip() for item in config_tokens)
    elif isinstance(config_payload, list):
        tokens.extend(str(item).strip() for item in config_payload)

    return stable_unique_texts(tokens)


def mask_token(token: str) -> str:
    if len(token) <= 10:
        return token
    return f"{token[:4]}...{token[-6:]}"


def normalize_quote_time(value: object) -> str:
    text = str(value).strip()
    if text.isdigit() and len(text) == 6:
        return f"{text[0:2]}:{text[2:4]}:{text[4:6]}"
    return text


def parse_datetime_text(value: object) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_price_row_datetime(row: dict[str, str]) -> datetime | None:
    date_text = str(row.get("日期", "")).strip()
    time_text = normalize_quote_time(row.get("时间", ""))
    if not date_text or not time_text:
        return None
    return parse_datetime_text(f"{date_text} {time_text}")


def format_duration(seconds: int | None) -> str:
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds}秒"
    minutes, remain = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}分{remain}秒"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}小时{minutes}分"
    days, hours = divmod(hours, 24)
    return f"{days}天{hours}小时"


def tick_delay_level(seconds: int | None) -> str:
    if seconds is None:
        return "bad"
    if seconds <= 60:
        return "good"
    if seconds <= 900:
        return "warn"
    return "bad"


def collect_domestic_codes(*groups: list[dict[str, str]]) -> set[str]:
    codes: set[str] = set()
    for rows in groups:
        for row in rows:
            code = str(row.get("代码", "")).strip().upper()
            if code:
                codes.add(code)
            continuous_code = str(row.get("连续代码", "")).strip().upper()
            if continuous_code.endswith("0"):
                codes.add(continuous_code[:-1])
    return codes


def choose_akshare_probe_symbol(prices: list[dict[str, str]], snapshot: list[dict[str, str]]) -> str:
    candidates: list[str] = []
    for rows in (prices, snapshot):
        for row in rows:
            symbol = str(row.get("连续代码", "")).strip().upper()
            if symbol:
                candidates.append(symbol)
    if not candidates:
        return "AG0"

    preferred_symbols = ["AG0", "AU0", "RB0", "CU0", "AL0", "IF0", "SC0"]
    symbol_set = set(candidates)
    for symbol in preferred_symbols:
        if symbol in symbol_set:
            return symbol
    return candidates[0]


def choose_alltick_probe_target(
    prices: list[dict[str, str]],
    snapshot: list[dict[str, str]],
    watch_snapshot: list[dict[str, str]],
) -> dict[str, object]:
    domestic_codes = collect_domestic_codes(prices, snapshot, watch_snapshot)
    for mapping in ALLTICK_SYMBOL_MAP:
        if mapping["domestic_code"] in domestic_codes:
            return mapping
    return ALLTICK_SYMBOL_MAP[0]


def parse_alltick_tick_datetime(value: object) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromtimestamp(int(text) / 1000)
    except Exception:
        return None


def describe_next_auto_start(now: datetime) -> str:
    def candidate_windows(base_date: datetime) -> list[tuple[datetime, datetime, str]]:
        windows: list[tuple[datetime, datetime, str]] = []
        for session in AUTO_START_SESSION_OPENS:
            open_at = base_date.replace(
                hour=session["hour"],
                minute=session["minute"],
                second=0,
                microsecond=0,
            )
            if session["hour"] == 21 and open_at.weekday() not in {0, 1, 2, 3}:
                continue
            if session["hour"] != 21 and open_at.weekday() > 4:
                continue
            preopen_at = open_at - timedelta(minutes=AUTO_START_PREOPEN_MINUTES)
            windows.append((preopen_at, open_at, str(session["label"])))
        return windows

    future_windows: list[tuple[datetime, datetime, str]] = []
    for offset in range(0, 5):
        future_windows.extend(candidate_windows(now + timedelta(days=offset)))
    future_windows.sort(key=lambda item: item[0])

    for preopen_at, open_at, label in future_windows:
        if preopen_at >= now:
            return f"自动预启动：下次 {preopen_at.strftime('%Y-%m-%d %H:%M')}（{label}）"
        if preopen_at <= now <= open_at + timedelta(minutes=10):
            return f"自动预启动窗口：{label} {preopen_at.strftime('%H:%M')} ~ {(open_at + timedelta(minutes=10)).strftime('%H:%M')}"
    return "自动预启动：未命中近期交易窗口"


def build_watch_command(output_dir: Path, trade_date: str) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(BASE_DIR / "scan_futures_abc_pattern.py"),
        "--trade-date",
        trade_date,
        "--history-date",
        "ALL",
        "--watch",
        "--interval",
        str(WATCH_INTERVAL_SECONDS),
        "--skip-existing-on-start",
        "--output-dir",
        str(output_dir),
    ]
    if WATCH_SYMBOLS:
        command.extend(["--symbols", ",".join(WATCH_SYMBOLS)])
    return command


def start_watch_process(output_dir: Path | None = None, trade_date: str | None = None) -> dict[str, object]:
    watch_context = resolve_watch_context()
    if watch_context["running"]:
        return {
            "ok": True,
            "started": False,
            "note": "监控进程已经在运行",
            "pid": watch_context["pid"],
            "command": watch_context["command"],
        }

    watch_dir = output_dir or LIVE_WATCH_DIR
    watch_dir.mkdir(parents=True, exist_ok=True)
    log_path = watch_log_path(watch_dir)
    pid_path = watch_pid_path(watch_dir)
    command = build_watch_command(output_dir=watch_dir, trade_date=trade_date or today_trade_date())

    with log_path.open("a", encoding="utf-8") as log_file:
        process = subprocess.Popen(  # noqa: S603
            command,
            cwd=BASE_DIR,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=log_file,
            start_new_session=True,
            text=True,
        )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    return {
        "ok": True,
        "started": True,
        "note": "已拉起夜盘监控进程",
        "pid": process.pid,
        "command": " ".join(command),
    }


def maybe_autostart_watch() -> None:
    watch_context = resolve_watch_context()
    if watch_context["running"]:
        return

    now = datetime.now()
    trigger_key = ""
    for session in AUTO_START_SESSION_OPENS:
        open_at = now.replace(hour=session["hour"], minute=session["minute"], second=0, microsecond=0)
        if session["hour"] == 21 and open_at.weekday() not in {0, 1, 2, 3}:
            continue
        if session["hour"] != 21 and open_at.weekday() > 4:
            continue
        preopen_at = open_at - timedelta(minutes=AUTO_START_PREOPEN_MINUTES)
        if preopen_at <= now <= open_at + timedelta(minutes=10):
            trigger_key = f"{preopen_at:%Y%m%d%H%M}-{session['label']}"
            break

    if not trigger_key:
        return

    now_monotonic = time.monotonic()
    with AUTO_START_LOCK:
        last_key = str(AUTO_START_STATE.get("last_attempt_key", ""))
        last_attempt = float(AUTO_START_STATE.get("last_attempt_monotonic", 0.0) or 0.0)
        if trigger_key == last_key and now_monotonic - last_attempt < 180:
            return
        AUTO_START_STATE["last_attempt_key"] = trigger_key
        AUTO_START_STATE["last_attempt_monotonic"] = now_monotonic

    start_watch_process(output_dir=LIVE_WATCH_DIR, trade_date=today_trade_date())


def auto_start_loop() -> None:
    while True:
        try:
            maybe_autostart_watch()
        except Exception:
            pass
        time.sleep(AUTO_START_CHECK_INTERVAL_SECONDS)


def probe_akshare_source(prices: list[dict[str, str]], snapshot: list[dict[str, str]]) -> dict[str, object]:
    now_monotonic = time.monotonic()
    with AKSHARE_PROBE_CACHE_LOCK:
        cached_at = float(AKSHARE_PROBE_CACHE.get("checked_at_monotonic", 0.0) or 0.0)
        if now_monotonic - cached_at < PROBE_CACHE_TTL_SECONDS:
            return dict(AKSHARE_PROBE_CACHE["result"])

    symbol = choose_akshare_probe_symbol(prices=prices, snapshot=snapshot)
    started = time.perf_counter()
    try:
        response = requests.get(
            "https://hq.sinajs.cn",
            params={"list": f"nf_{symbol}"},
            headers=PROBE_HEADERS,
            timeout=PROBE_TIMEOUT_SECONDS,
        )
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        response.raise_for_status()
        response.encoding = "gbk"
        parts = response.text.split('"')
        payload = parts[1] if len(parts) >= 2 else ""
        fields = [item.strip() for item in payload.split(",")] if payload else []
        if len(fields) < 9 or not fields[0]:
            raise ValueError("empty quote payload")

        quote_time = normalize_quote_time(fields[1])
        current_price = fields[8]
        result = {
            "ok": True,
            "status": "通畅",
            "note": (
                f"{symbol} {fields[0]} 请求 {elapsed_ms}ms"
                + (f"，接口时间 {quote_time}" if quote_time else "")
                + (f"，最新价 {current_price}" if current_price else "")
            ),
            "request_ms": elapsed_ms,
            "symbol": symbol,
            "quote_name": fields[0],
            "quote_time": quote_time,
            "current_price": current_price,
        }
    except Exception as exc:  # noqa: BLE001
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        result = {
            "ok": False,
            "status": "异常",
            "note": f"{symbol} 探测失败：{type(exc).__name__}: {exc}",
            "request_ms": elapsed_ms,
            "symbol": symbol,
            "quote_name": "",
            "quote_time": "",
            "current_price": "",
        }

    with AKSHARE_PROBE_CACHE_LOCK:
        AKSHARE_PROBE_CACHE["checked_at_monotonic"] = now_monotonic
        AKSHARE_PROBE_CACHE["result"] = result
    return dict(result)


def probe_alltick_source(
    prices: list[dict[str, str]],
    snapshot: list[dict[str, str]],
    watch_snapshot: list[dict[str, str]],
) -> dict[str, object]:
    now_monotonic = time.monotonic()
    with ALLTICK_PROBE_CACHE_LOCK:
        cached_at = float(ALLTICK_PROBE_CACHE.get("checked_at_monotonic", 0.0) or 0.0)
        if now_monotonic - cached_at < ALLTICK_CACHE_TTL_SECONDS:
            return dict(ALLTICK_PROBE_CACHE["result"])

    tokens = load_alltick_tokens()
    target = choose_alltick_probe_target(prices=prices, snapshot=snapshot, watch_snapshot=watch_snapshot)
    if not tokens:
        result = {
            "ok": False,
            "status": "未配置",
            "note": f"未发现 {ALLTICK_CONFIG_PATH.name} 或 ALLTICK_TOKENS 环境变量",
            "request_ms": None,
            "representative": f"{target['label']}/{target['alltick_codes'][0]}",
            "tick_datetime": "",
            "current_price": "",
            "tick_delay_seconds": None,
            "tick_delay_text": "-",
            "tick_delay_note": "AllTick token 未配置，无法估算延迟。",
            "tick_delay_level": "bad",
        }
    else:
        result = None
        last_limit_result: dict[str, object] | None = None
        last_error_result: dict[str, object] | None = None

        for alltick_code in target["alltick_codes"]:
            representative = f"{target['label']}/{alltick_code}"
            code_invalid = False

            for index, token in enumerate(tokens):
                started = time.perf_counter()
                try:
                    query = {
                        "trace": f"dashboard-{int(time.time())}-{index}",
                        "data": {"symbol_list": [{"code": alltick_code}]},
                    }
                    response = requests.get(
                        ALLTICK_BASE_URL,
                        params={"token": token, "query": json.dumps(query, separators=(",", ":"))},
                        timeout=PROBE_TIMEOUT_SECONDS,
                    )
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    payload = response.json()

                    if response.status_code == 429 or payload.get("error_msg") == "Too many requests":
                        last_limit_result = {
                            "ok": False,
                            "status": "限流",
                            "note": f"{representative} 触发限流，请放慢轮询频率",
                            "request_ms": elapsed_ms,
                            "representative": representative,
                            "tick_datetime": "",
                            "current_price": "",
                            "tick_delay_seconds": None,
                            "tick_delay_text": "-",
                            "tick_delay_note": "AllTick 当前返回限流，暂时无法刷新 tick 时间。",
                            "tick_delay_level": "bad",
                        }
                        continue

                    if payload.get("ret") in {401, 403}:
                        last_error_result = {
                            "ok": False,
                            "status": "鉴权失败",
                            "note": f"{representative} 的 AllTick token 不可用，已尝试下一个 token",
                            "request_ms": elapsed_ms,
                            "representative": representative,
                            "tick_datetime": "",
                            "current_price": "",
                            "tick_delay_seconds": None,
                            "tick_delay_text": "-",
                            "tick_delay_note": "AllTick 鉴权失败，无法估算 tick 延迟。",
                            "tick_delay_level": "bad",
                        }
                        continue

                    tick_list = payload.get("data", {}).get("tick_list", [])
                    if response.status_code == 200 and payload.get("ret") == 200 and tick_list:
                        tick = tick_list[0]
                        tick_datetime = parse_alltick_tick_datetime(tick.get("tick_time"))
                        delay_seconds = None
                        delay_text = "-"
                        delay_note = "AllTick 未返回有效 tick_time。"
                        delay_level = "bad"
                        if tick_datetime is not None:
                            delay_seconds = max(0, int((datetime.now() - tick_datetime).total_seconds()))
                            delay_text = format_duration(delay_seconds)
                            delay_level = tick_delay_level(delay_seconds)
                            delay_note = (
                                f"按 {target['label']}/{tick.get('code', '')} 的 tick_time 估算："
                                f"{tick_datetime.strftime('%Y-%m-%d %H:%M:%S')}。"
                            )

                        result = {
                            "ok": True,
                            "status": "通畅",
                            "note": (
                                f"{target['label']}/{tick.get('code', alltick_code)} 请求 {elapsed_ms}ms"
                                + (f"，Tick时间 {tick_datetime.strftime('%H:%M:%S')}" if tick_datetime else "")
                                + (f"，最新价 {tick.get('price', '')}" if tick.get("price") else "")
                            ),
                            "request_ms": elapsed_ms,
                            "representative": f"{target['label']}/{tick.get('code', alltick_code)}",
                            "tick_datetime": tick_datetime.strftime("%Y-%m-%d %H:%M:%S") if tick_datetime else "",
                            "current_price": str(tick.get("price", "")),
                            "tick_delay_seconds": delay_seconds,
                            "tick_delay_text": delay_text,
                            "tick_delay_note": delay_note,
                            "tick_delay_level": delay_level,
                        }
                        break

                    error_text = payload.get("msg") or payload.get("error_msg") or response.text[:120]
                    last_error_result = {
                        "ok": False,
                        "status": "异常",
                        "note": f"{representative} 返回异常：{error_text}",
                        "request_ms": elapsed_ms,
                        "representative": representative,
                        "tick_datetime": "",
                        "current_price": "",
                        "tick_delay_seconds": None,
                        "tick_delay_text": "-",
                        "tick_delay_note": "AllTick 请求未返回有效 tick 时间。",
                        "tick_delay_level": "bad",
                    }
                    if "code invalid" in str(error_text).lower():
                        code_invalid = True
                        break
                except Exception as exc:  # noqa: BLE001
                    elapsed_ms = int((time.perf_counter() - started) * 1000)
                    last_error_result = {
                        "ok": False,
                        "status": "异常",
                        "note": f"{representative} 探测失败：{type(exc).__name__}: {exc}",
                        "request_ms": elapsed_ms,
                        "representative": representative,
                        "tick_datetime": "",
                        "current_price": "",
                        "tick_delay_seconds": None,
                        "tick_delay_text": "-",
                        "tick_delay_note": "AllTick 请求失败，无法估算 tick 延迟。",
                        "tick_delay_level": "bad",
                    }
                    continue

            if result is not None and result.get("ok"):
                break
            if code_invalid:
                continue

        if result is None:
            result = last_error_result or last_limit_result
        if result is None:
            result = {
                "ok": False,
                "status": "异常",
                "note": f"AllTick token 不可用，已尝试 {len(tokens)} 个 token",
                "request_ms": None,
                "representative": f"{target['label']}/{target['alltick_codes'][0]}",
                "tick_datetime": "",
                "current_price": "",
                "tick_delay_seconds": None,
                "tick_delay_text": "-",
                "tick_delay_note": "AllTick token 不可用，无法估算 tick 延迟。",
                "tick_delay_level": "bad",
            }

    with ALLTICK_PROBE_CACHE_LOCK:
        ALLTICK_PROBE_CACHE["checked_at_monotonic"] = now_monotonic
        ALLTICK_PROBE_CACHE["result"] = result
    return dict(result)


def compute_akshare_tick_delay_summary(
    prices: list[dict[str, str]],
    snapshot: list[dict[str, str]],
) -> dict[str, object]:
    now = datetime.now()

    price_datetimes = [
        parsed
        for row in prices
        if (parsed := parse_price_row_datetime(row)) is not None and parsed <= now + timedelta(minutes=5)
    ]
    if price_datetimes:
        latest_tick_at = max(price_datetimes)
        delay_seconds = max(0, int((now - latest_tick_at).total_seconds()))
        return {
            "delay_seconds": delay_seconds,
            "delay_text": format_duration(delay_seconds),
            "delay_level": tick_delay_level(delay_seconds),
            "note": (
                f"按 AkShare 最新价时间估算：{latest_tick_at.strftime('%Y-%m-%d %H:%M:%S')}。"
                " 休市或未刷新时延迟会明显变大。"
            ),
        }

    snapshot_datetimes = [
        parsed
        for row in snapshot
        if (parsed := parse_datetime_text(row.get("latest_bar_time", ""))) is not None
        and parsed <= now + timedelta(minutes=5)
    ]
    if snapshot_datetimes:
        latest_tick_at = max(snapshot_datetimes)
        delay_seconds = max(0, int((now - latest_tick_at).total_seconds()))
        return {
            "delay_seconds": delay_seconds,
            "delay_text": format_duration(delay_seconds),
            "delay_level": tick_delay_level(delay_seconds),
            "note": (
                f"按 AkShare 分钟线最新时间估算：{latest_tick_at.strftime('%Y-%m-%d %H:%M:%S')}。"
                " 当前没有可用的最新价时间，已自动回退到分钟线。"
            ),
        }

    return {
        "delay_seconds": None,
        "delay_text": "-",
        "delay_level": "bad",
        "note": "没有找到可用于估算 tick 延迟的行情时间。",
    }


def load_dashboard_data() -> dict[str, object]:
    if PUBLIC_GITHUB_MODE:
        return build_public_dashboard_data()

    maybe_autostart_watch()

    prices = read_csv_rows(PRICES_CSV)
    price_errors = read_csv_rows(PRICE_ERRORS_CSV)
    prices = filter_excluded_price_rows(prices)
    price_errors = filter_excluded_price_rows(price_errors)
    watch_context = resolve_watch_context()
    snapshot_path = watch_output_path("futures_abc_snapshot.csv", watch_context)
    matches_path = watch_output_path("futures_abc_matches.csv", watch_context)
    events_path = watch_output_path("futures_abc_match_events.csv", watch_context)

    snapshot = read_csv_rows(snapshot_path)
    matches = read_csv_rows(matches_path)
    events = read_csv_rows(events_path)
    snapshot_errors = filter_error_rows(snapshot)

    prices_preview = prices[:120]
    events_preview = list(reversed(events))[:50]
    matches_preview = matches[:50]
    snapshot_errors_preview = snapshot_errors[:50]
    price_errors_preview = price_errors[:50]

    pid = watch_context["pid"]
    watch_running = bool(watch_context["running"])
    watch_command = str(watch_context["command"])
    watch_note = "监控进程存活" if watch_running else "未检测到有效监控进程"
    if pid and watch_running and "sleep " in watch_command:
        watch_note = "已挂起，等待自动启动时间"
    auto_start_note = describe_next_auto_start(datetime.now())
    watch_note = f"{watch_note}；{auto_start_note}"

    latest_event = events[-1] if events else None
    latest_event_summary = ""
    if latest_event:
        latest_event_summary = (
            f"{latest_event.get('连续代码', '')} "
            f"{latest_event.get('A_time', '')[-8:]} → "
            f"{latest_event.get('C_time', '')[-8:]}"
        ).strip()

    akshare_probe = probe_akshare_source(prices=prices, snapshot=snapshot)
    akshare_tick_delay = compute_akshare_tick_delay_summary(prices=prices, snapshot=snapshot)
    alltick_probe = probe_alltick_source(prices=prices, snapshot=snapshot, watch_snapshot=snapshot)
    backtest_report = load_backtest_report(backtest_trade_date())

    return {
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "summary": {
            "price_rows": len(prices),
            "price_error_rows": len(price_errors),
            "snapshot_rows": len(snapshot),
            "snapshot_error_rows": len(snapshot_errors),
            "match_rows": len(matches),
            "event_rows": len(events),
            "prices_updated_at": read_file_mtime(PRICES_CSV),
            "scan_updated_at": read_file_mtime(snapshot_path),
            "watch_running": watch_running,
            "watch_note": watch_note,
            "latest_event_summary": latest_event_summary,
            "akshare_api_ok": akshare_probe["ok"],
            "akshare_api_status": akshare_probe["status"],
            "akshare_api_note": akshare_probe["note"],
            "akshare_api_request_ms": akshare_probe["request_ms"],
            "akshare_tick_delay_seconds": akshare_tick_delay["delay_seconds"],
            "akshare_tick_delay_text": akshare_tick_delay["delay_text"],
            "akshare_tick_delay_note": akshare_tick_delay["note"],
            "akshare_tick_delay_level": akshare_tick_delay["delay_level"],
            "alltick_api_ok": alltick_probe["ok"],
            "alltick_api_status": alltick_probe["status"],
            "alltick_api_note": alltick_probe["note"],
            "alltick_api_request_ms": alltick_probe["request_ms"],
            "alltick_tick_delay_seconds": alltick_probe["tick_delay_seconds"],
            "alltick_tick_delay_text": alltick_probe["tick_delay_text"],
            "alltick_tick_delay_note": alltick_probe["tick_delay_note"],
            "alltick_tick_delay_level": alltick_probe["tick_delay_level"],
        },
        "prices": prices_preview,
        "price_errors": price_errors_preview,
        "snapshot_errors": snapshot_errors_preview,
        "matches": matches_preview,
        "events": events_preview,
        "backtest": backtest_report,
        "watch": {
            "pid": pid,
            "running": watch_running,
            "command": watch_command,
            "dir": str(watch_context["dir"]),
            "log_tail": tail_lines(Path(watch_context["log_path"]), line_count=100),
            "log_updated_at": read_file_mtime(Path(watch_context["log_path"])),
        },
    }


def run_command(args: list[str], timeout: int = 240) -> dict[str, object]:
    proc = subprocess.run(
        args,
        cwd=BASE_DIR,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-12000:],
        "stderr": proc.stderr[-12000:],
    }


def today_trade_date() -> str:
    trade_day = datetime.now()
    while trade_day.weekday() > 4:
        trade_day -= timedelta(days=1)
    return trade_day.strftime("%Y%m%d")


def backtest_trade_date(days_back: int = BACKTEST_DAYS_BACK) -> str:
    return shift_trade_date(today_trade_date(), days_back)


def load_backtest_report(trade_date: str) -> dict[str, object]:
    summary_path = BACKTEST_DIR / f"futures_abc_backtest_summary_{trade_date}.csv"
    trades_path = BACKTEST_DIR / f"futures_abc_backtest_trades_{trade_date}.csv"
    failed_path = BACKTEST_DIR / f"futures_abc_backtest_failed_{trade_date}.csv"

    summary_rows = read_csv_rows(summary_path)
    trade_rows = read_csv_rows(trades_path)
    failed_rows = read_csv_rows(failed_path)

    if not summary_rows:
        return {
            "summary": {
                "available": False,
                "trade_date": trade_date,
                "updated_at": read_file_mtime(summary_path),
                "hold_bars": BACKTEST_HOLD_BARS,
                "rows_shown": 0,
                "symbols_ok": 0,
                "symbols_failed": len(failed_rows),
                "positive_symbols": 0,
                "total_trades": 0,
                "win_rate_pct": 0.0,
                "win_rate_pct_text": "0.00%",
                "avg_net_return_pct": 0.0,
                "avg_net_return_pct_text": "0.0000%",
                "total_net_return_pct": 0.0,
                "total_net_return_pct_text": "0.0000%",
            },
            "rows": [],
            "failed": failed_rows[:20],
        }

    normalized_rows: list[dict[str, object]] = []
    for row in summary_rows:
        trades = int(str(row.get("trades", "0") or 0))
        wins = int(str(row.get("wins", "0") or 0))
        win_rate_pct = float(str(row.get("win_rate_pct", "0") or 0))
        avg_net_return_pct = float(str(row.get("avg_net_return_pct", "0") or 0))
        total_net_return_pct = float(str(row.get("total_net_return_pct", "0") or 0))
        normalized_rows.append(
            {
                "symbol": str(row.get("symbol", "")).strip(),
                "trades": trades,
                "wins": wins,
                "win_rate_pct": win_rate_pct,
                "avg_net_return_pct": avg_net_return_pct,
                "total_net_return_pct": total_net_return_pct,
            }
        )

    normalized_rows.sort(key=lambda item: float(item["total_net_return_pct"]), reverse=True)
    total_trades = sum(int(item["trades"]) for item in normalized_rows)
    total_wins = sum(int(item["wins"]) for item in normalized_rows)
    total_net_return_pct = sum(float(item["total_net_return_pct"]) for item in normalized_rows)
    avg_net_return_pct = total_net_return_pct / total_trades if total_trades else 0.0
    updated_at = max(
        filter(
            None,
            [
                read_file_mtime(summary_path),
                read_file_mtime(trades_path),
                read_file_mtime(failed_path),
            ],
        ),
        default="",
    )
    trade_rows.sort(key=lambda item: str(item.get("c_time", item.get("signal_time", ""))), reverse=True)
    display_rows = []
    for row in trade_rows[:120]:
        display_rows.append(
            {
                "连续代码": str(row.get("symbol", "")).strip(),
                "交易所": str(row.get("exchange", "")).strip(),
                "品种": str(row.get("name", "")).strip(),
                "A_time": str(row.get("a_time", "")).strip(),
                "B_time": str(row.get("b_time", "")).strip(),
                "C_time": str(row.get("c_time", "")).strip(),
                "A_high": str(row.get("a_high", "")).strip(),
                "B_low": str(row.get("b_low", "")).strip(),
                "C_low": str(row.get("c_low", "")).strip(),
                "C_close": str(row.get("c_close", "")).strip(),
            }
        )

    display_rows = display_rows[:40]

    return {
        "summary": {
            "available": True,
            "trade_date": trade_date,
            "updated_at": updated_at,
            "hold_bars": BACKTEST_HOLD_BARS,
            "rows_shown": len(display_rows),
            "symbols_ok": len(normalized_rows),
            "symbols_failed": len(failed_rows),
            "positive_symbols": sum(1 for item in normalized_rows if float(item["total_net_return_pct"]) > 0),
            "total_trades": total_trades,
            "win_rate_pct": (total_wins / total_trades * 100.0) if total_trades else 0.0,
            "win_rate_pct_text": format_pct_text((total_wins / total_trades * 100.0) if total_trades else 0.0, digits=2),
            "avg_net_return_pct": avg_net_return_pct,
            "avg_net_return_pct_text": format_pct_text(avg_net_return_pct, digits=4),
            "total_net_return_pct": total_net_return_pct,
            "total_net_return_pct_text": format_pct_text(total_net_return_pct, digits=4),
        },
        "rows": display_rows,
        "failed": failed_rows[:20],
    }


def run_scan_today() -> dict[str, object]:
    LIVE_WATCH_DIR.mkdir(parents=True, exist_ok=True)
    command = [
        sys.executable,
        str(BASE_DIR / "scan_futures_abc_pattern.py"),
        "--trade-date",
        today_trade_date(),
        "--history-date",
        "ALL",
        "--output-dir",
        str(LIVE_WATCH_DIR),
    ]
    if WATCH_SYMBOLS:
        command.extend(["--symbols", ",".join(WATCH_SYMBOLS)])
    return run_command(command, timeout=600)


def run_prices_today() -> dict[str, object]:
    return run_command(
        [
            sys.executable,
            str(BASE_DIR / "akshare_futures_latest_all.py"),
            "--trade-date",
            today_trade_date(),
            "--output",
            str(PRICES_CSV),
        ],
        timeout=600,
    )


def run_backtest_three_days_ago() -> dict[str, object]:
    BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    target_trade_date = backtest_trade_date()
    result = run_command(
        [
            sys.executable,
            str(BASE_DIR / "futures_abc_backtest_batch.py"),
            "--trade-date",
            target_trade_date,
            "--hold-bars",
            str(BACKTEST_HOLD_BARS),
            "--output-dir",
            str(BACKTEST_DIR),
        ],
        timeout=1800,
    )
    result["trade_date"] = target_trade_date
    return result


def build_dashboard_html() -> str:
    initial_data = load_dashboard_data()
    initial_json = json.dumps(initial_data, ensure_ascii=False).replace("</", "<\\/")
    return (
        HTML.replace("__STOCK_PANEL_URL__", STOCK_PANEL_URL)
        .replace("__INITIAL_DATA__", initial_json)
    )


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "FuturesDashboard/1.0"

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(build_dashboard_html())
            return
        if parsed.path == "/api/data":
            self._send_json(load_dashboard_data())
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if PUBLIC_GITHUB_MODE and parsed.path in {
            "/api/run-scan",
            "/api/run-prices",
            "/api/run-backtest-3d",
            "/api/run-all",
        }:
            self._send_json(
                {
                    "ok": True,
                    "public_mode": True,
                    "message": "公开版保留了按钮入口，但不会执行真实扫描、拉价或回测任务。",
                    "stdout": "",
                    "stderr": "",
                },
                status=HTTPStatus.OK,
            )
            return
        if parsed.path == "/api/run-scan":
            result = run_scan_today()
            self._send_json(result, status=HTTPStatus.OK if result["ok"] else HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/run-prices":
            result = run_prices_today()
            self._send_json(result, status=HTTPStatus.OK if result["ok"] else HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/run-backtest-3d":
            result = run_backtest_three_days_ago()
            self._send_json(result, status=HTTPStatus.OK if result["ok"] else HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        if parsed.path == "/api/run-all":
            prices = run_prices_today()
            if not prices["ok"]:
                self._send_json(prices, status=HTTPStatus.INTERNAL_SERVER_ERROR)
                return
            scan = run_scan_today()
            payload = {
                "ok": scan["ok"],
                "prices": prices,
                "scan": scan,
                "stdout": (prices.get("stdout", "") + "\n" + scan.get("stdout", "")).strip(),
                "stderr": (prices.get("stderr", "") + "\n" + scan.get("stderr", "")).strip(),
            }
            self._send_json(payload, status=HTTPStatus.OK if scan["ok"] else HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._send_json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return

    def _send_html(self, content: str) -> None:
        encoded = content.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, payload: dict[str, object], status: HTTPStatus = HTTPStatus.OK) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def main() -> None:
    host = "127.0.0.1"
    port = 8765
    if len(sys.argv) >= 2:
        host = sys.argv[1]
    if len(sys.argv) >= 3:
        port = int(sys.argv[2])

    auto_thread = threading.Thread(target=auto_start_loop, name="watch-auto-start", daemon=True)
    auto_thread.start()

    server = ThreadingHTTPServer((host, port), DashboardHandler)
    print(f"Dashboard running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
