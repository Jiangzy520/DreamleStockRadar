from __future__ import annotations

import base64
import json
import os
import re
import ssl
import subprocess
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.request import Request, urlopen

from flask import Flask, jsonify, render_template_string, request


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
MAIN_WEBAPP_ROOT = PROJECT_ROOT / "extra_signal_services" / "mrj_quant_push_site_current"
if MAIN_WEBAPP_ROOT.exists() and str(MAIN_WEBAPP_ROOT) not in sys.path:
    sys.path.insert(0, str(MAIN_WEBAPP_ROOT))

from webapp.services.notifications import (
    DEFAULT_NOTIFICATION_CHANNEL_META as NOTIFICATION_CHANNEL_META,
    DEFAULT_NOTIFICATION_EVENT_META as NOTIFICATION_EVENT_META,
    NotificationService,
)

BASE_DIR = Path(os.getenv("PUSH_DASHBOARD_BASE_DIR", str(SCRIPT_DIR)))
VNTRADER_DIR = Path(os.getenv("PUSH_DASHBOARD_VNTRADER_DIR", str(PROJECT_ROOT / "vntrader")))
BRIDGE_CONFIG_PATH = BASE_DIR / "push_xtp_bridge.config.json"
FUTURES_BRIDGE_CONFIG_PATH = BASE_DIR / "push_ctp_bridge.config.json"
FUTURES_CONNECT_PATH = BASE_DIR / "connect_tq.json"
NOTIFICATION_CONFIG_PATH = BASE_DIR / "state" / "notification_channels.local.json"
PUSH_SNAPSHOT_URL = "http://127.0.0.1:8790/api/realtime/signals?limit=200"
LOCAL_EASTMONEY_URL = "http://127.0.0.1:8790/api/realtime/signals?limit=0"
LOCAL_PYTDX_URL = "http://127.0.0.1:8768/api/realtime/pytdx-backup/status"
REMOTE_ALLTICK_URL = "http://127.0.0.1:8768/api/realtime/signals?limit=0"
REMOTE_BASIC_AUTH_USER = "demo"
REMOTE_BASIC_AUTH_PASSWORD = "demo"
PUBLIC_GITHUB_MODE = os.getenv("PUBLIC_GITHUB_MODE", "1").strip().lower() not in {"0", "false", "no", "off"}
SERVICE_NAME = "push-xtp-bridge.service"
FUTURES_SERVICE_NAME = "push-ctp-bridge.service"
WEEKDAY_LABELS = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
NOTIFICATION_SERVICE = NotificationService(
    config_path=NOTIFICATION_CONFIG_PATH,
    event_meta=NOTIFICATION_EVENT_META,
    channel_meta=NOTIFICATION_CHANNEL_META,
)

ACCOUNT_RE = re.compile(r"^\[账户\]\s+(?P<account>\S+)\s+balance=(?P<balance>[\d.]+)\s+available=(?P<available>[\d.]+)$")
POSITION_RE = re.compile(r"^\[持仓\]\s+(?P<vt_positionid>\S+)\s+volume=(?P<volume>[\d.]+)\s+yd=(?P<yd>[\d.]+)$")

ORDER_PREFIXES = {
    "[下单]": "下单",
    "[演练]": "演练",
    "[委托]": "委托",
    "[成交]": "成交",
}

SOURCE_ORDER = ("alltick", "eastmoney", "tdx")
SOURCE_SPECS = (
    {"id": "alltick", "label": "AllTick", "url": REMOTE_ALLTICK_URL, "kind": "ssh_signals"},
    {"id": "eastmoney", "label": "东方财富", "url": LOCAL_EASTMONEY_URL, "kind": "auth_signals"},
    {"id": "tdx", "label": "通达信", "url": LOCAL_PYTDX_URL, "kind": "auth_tdx"},
)


FUTURES_ACCOUNT_RE = re.compile(r"^\[FUTURES_ACCOUNT\]\s+(?P<account>\S+)\s+balance=(?P<balance>[-\d.]+)\s+available=(?P<available>[-\d.]+)$")
FUTURES_POSITION_RE = re.compile(r"^\[FUTURES_POSITION\]\s+(?P<vt_positionid>\S+)\s+volume=(?P<volume>[-\d.]+)\s+yd=(?P<yd>[-\d.]+)$")
FUTURES_READY_RE = re.compile(r"^\[FUTURES_READY\]\s+active_front=(?P<front>\S+)\s+accounts=(?P<accounts>\d+)\s+contracts=(?P<contracts>\d+)$")
FUTURES_ORDER_PREFIXES = {
    "[FUTURES_ORDER]": "order",
    "[FUTURES_TRADE]": "trade",
    "[FUTURES_ORDER_EVENT]": "order_event",
    "[FUTURES_TRADE_EVENT]": "trade_event",
}

app = Flask(__name__)


HTML_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Push-XTP 仪表盘</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #101a2b;
      --panel-2: #13203a;
      --text: #e7edf7;
      --muted: #8fa1be;
      --accent: #31c48d;
      --warn: #f5b942;
      --danger: #ef6b73;
      --line: #223251;
      --mono: "Cascadia Code", "SFMono-Regular", Consolas, monospace;
      --sans: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: linear-gradient(180deg, #09111d 0%, #0e1627 100%);
      color: var(--text);
      font-family: var(--sans);
    }
    .wrap {
      width: min(1450px, calc(100vw - 24px));
      margin: 0 auto;
      padding: 18px 0 40px;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 18px;
    }
    h1 {
      margin: 0;
      font-size: 30px;
      line-height: 1.1;
    }
    .sub {
      margin-top: 6px;
      color: var(--muted);
      font-size: 14px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(12, 1fr);
      gap: 14px;
    }
    .card {
      background: linear-gradient(180deg, rgba(19, 32, 58, 0.95), rgba(16, 26, 43, 0.95));
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18);
    }
    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-5 { grid-column: span 5; }
    .span-6 { grid-column: span 6; }
    .span-7 { grid-column: span 7; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    .label {
      color: var(--muted);
      font-size: 13px;
      margin-bottom: 8px;
    }
    .value {
      font-size: 26px;
      font-weight: 700;
    }
    .value.small {
      font-size: 22px;
    }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .danger { color: var(--danger); }
    .kv {
      display: grid;
      grid-template-columns: 140px 1fr;
      gap: 10px 12px;
      font-size: 14px;
    }
    .kv div:nth-child(odd) { color: var(--muted); }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      padding: 10px 8px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th { color: var(--muted); font-weight: 600; }
    code, pre {
      font-family: var(--mono);
      font-size: 12px;
    }
    pre {
      margin: 0;
      white-space: pre-wrap;
      word-break: break-word;
      color: #d7e0ee;
      max-height: 380px;
      overflow: auto;
    }
    .pill {
      display: inline-block;
      padding: 4px 10px;
      border-radius: 999px;
      background: rgba(49, 196, 141, 0.12);
      color: var(--accent);
      border: 1px solid rgba(49, 196, 141, 0.25);
      font-size: 12px;
      font-weight: 600;
    }
    .hero-actions {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .hero-link-card {
      display: block;
      min-width: 220px;
      padding: 12px 14px;
      border-radius: 14px;
      border: 1px solid rgba(95, 139, 255, 0.22);
      background: linear-gradient(180deg, rgba(17, 30, 53, 0.96), rgba(12, 22, 39, 0.96));
      color: var(--text);
      text-decoration: none;
      box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
      transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
    }
    .hero-link-card:hover {
      transform: translateY(-1px);
      border-color: rgba(95, 139, 255, 0.42);
      box-shadow: 0 12px 28px rgba(0, 0, 0, 0.24);
    }
    .hero-link-label {
      color: #7ea5ff;
      font-size: 12px;
      font-weight: 700;
      letter-spacing: 0.02em;
      text-transform: uppercase;
    }
    .hero-link-title {
      margin-top: 6px;
      font-size: 18px;
      font-weight: 700;
      line-height: 1.2;
    }
    .hero-link-sub {
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
      line-height: 1.5;
    }
    .public-doc-card {
      margin-bottom: 16px;
      padding: 20px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(19, 32, 58, 0.96), rgba(13, 23, 39, 0.96));
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.16);
    }
    .public-doc-title {
      font-size: 22px;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .public-doc-note {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.75;
    }
    .public-doc-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .public-doc-item {
      padding: 16px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(14, 24, 40, 0.9);
    }
    .public-doc-item strong {
      display: block;
      font-size: 16px;
      margin-bottom: 6px;
    }
    .public-doc-item span {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .public-doc-tip {
      margin-top: 14px;
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid rgba(49, 196, 141, 0.24);
      background: rgba(49, 196, 141, 0.08);
      color: #d6f7e6;
      font-size: 13px;
      line-height: 1.7;
    }
    .empty {
      color: var(--muted);
      font-size: 14px;
      padding: 12px 0 4px;
    }
    .source-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-top: 10px;
    }
    .source-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 12px;
      border-radius: 999px;
      background: rgba(18, 34, 58, 0.88);
      border: 1px solid var(--line);
      color: var(--text);
      font-size: 12px;
    }
    .source-pill.ok {
      color: var(--accent);
      border-color: rgba(49, 196, 141, 0.28);
      background: rgba(49, 196, 141, 0.08);
    }
    .source-pill.warn {
      color: var(--warn);
      border-color: rgba(245, 185, 66, 0.28);
      background: rgba(245, 185, 66, 0.08);
    }
    .overlap-toolbar {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin: 8px 0 14px;
    }
    .overlap-toolbar-meta {
      color: var(--muted);
      font-size: 13px;
    }
    .overlap-toggle-btn {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 8px 14px;
      background: rgba(18, 34, 58, 0.88);
      color: var(--text);
      font-size: 13px;
      cursor: pointer;
    }
    .overlap-list {
      display: grid;
      gap: 14px;
    }
    .overlap-item {
      border: 1px solid var(--line);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(18, 32, 56, 0.96), rgba(14, 24, 40, 0.96));
      padding: 16px;
    }
    .overlap-card-button {
      width: 100%;
      padding: 0;
      border: 0;
      background: transparent;
      color: inherit;
      text-align: left;
      cursor: pointer;
    }
    .overlap-top {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: flex-start;
    }
    .overlap-head-left {
      min-width: 0;
    }
    .overlap-code {
      font-size: 18px;
      font-weight: 700;
      margin-right: 10px;
    }
    .overlap-name {
      color: var(--muted);
      font-size: 16px;
    }
    .overlap-badges {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      justify-content: flex-end;
    }
    .overlap-summary {
      color: var(--muted);
      font-size: 13px;
      margin-top: 8px;
    }
    .overlap-chevron {
      color: var(--muted);
      font-size: 18px;
      transition: transform 0.18s ease;
    }
    .overlap-item.open .overlap-chevron {
      transform: rotate(180deg);
    }
    .overlap-body {
      margin-top: 12px;
    }
    .overlap-item.collapsed .overlap-body {
      display: none;
    }
    .badge {
      display: inline-flex;
      align-items: center;
      padding: 4px 10px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(15, 27, 47, 0.9);
      font-size: 12px;
      font-weight: 700;
    }
    .badge.match-2 {
      color: var(--warn);
      border-color: rgba(245, 185, 66, 0.28);
      background: rgba(245, 185, 66, 0.09);
    }
    .badge.match-3 {
      color: var(--accent);
      border-color: rgba(49, 196, 141, 0.28);
      background: rgba(49, 196, 141, 0.09);
    }
    .badge.source-alltick {
      color: var(--cyan);
      border-color: rgba(85, 214, 255, 0.26);
      background: rgba(85, 214, 255, 0.08);
    }
    .badge.source-eastmoney {
      color: #5f8bff;
      border-color: rgba(95, 139, 255, 0.26);
      background: rgba(95, 139, 255, 0.08);
    }
    .badge.source-tdx {
      color: #ffd37d;
      border-color: rgba(255, 211, 125, 0.26);
      background: rgba(255, 211, 125, 0.08);
    }
    .overlap-lines {
      display: grid;
      gap: 10px;
    }
    .overlap-line {
      border: 1px solid rgba(40, 58, 89, 0.9);
      border-radius: 14px;
      background: rgba(13, 24, 41, 0.95);
      padding: 12px;
    }
    .overlap-line-top {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 6px;
      font-weight: 700;
    }
    .overlap-line-time {
      color: var(--cyan);
      white-space: nowrap;
    }
    .overlap-line-detail {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.7;
    }
    @media (max-width: 1100px) {
      .span-3, .span-4, .span-5, .span-6, .span-7, .span-8 { grid-column: span 12; }
      .hero { display: block; }
      .hero-actions {
        margin-top: 14px;
        justify-content: flex-start;
      }
      .hero-link-card {
        min-width: 0;
        width: 100%;
      }
      .public-doc-grid {
        grid-template-columns: 1fr;
      }
      .overlap-top, .overlap-line-top { display: block; }
      .overlap-badges { margin-top: 10px; justify-content: flex-start; }
    }
  </style>
</head>
<body class="public-github">
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>Push-XTP 仪表盘</h1>
        <div class="sub">展示股票桥接模块的结构、入口、信息卡片与功能区域。</div>
      </div>
      <div class="hero-actions">
        <a class="hero-link-card" href="/push" data-local-href="/push" data-remote-href="/push">
          <div class="hero-link-label">Signals</div>
          <div class="hero-link-title">返回股票信号页</div>
          <div class="hero-link-sub">回到本地股票实时信号中心与完整信号表</div>
        </a>
        <a class="hero-link-card" href="/futures" data-local-href="/futures" data-remote-href="/futures">
          <div class="hero-link-label">Signals</div>
          <div class="hero-link-title">返回期货信号页</div>
          <div class="hero-link-sub">回到本地期货信号面板与行情提醒页面</div>
        </a>
        <a class="hero-link-card" href="/bridge/futures" data-local-href="/bridge/futures" data-remote-href="/bridge/futures">
          <div class="hero-link-label">Futures</div>
          <div class="hero-link-title">期货自动交易</div>
          <div class="hero-link-sub">查看期货桥接页面结构、信息卡片与模块说明</div>
        </a>
        <a class="hero-link-card" href="/notifications" data-local-href="/notifications" data-remote-href="/notifications">
          <div class="hero-link-label">Notify</div>
          <div class="hero-link-title">通知推送</div>
          <div class="hero-link-sub">配置飞书、钉钉、企业微信 Webhook，并接收四类信号/交易事件</div>
        </a>
        <div class="pill" id="generatedAt">加载中...</div>
      </div>
    </div>
    <section class="public-doc-card">
      <div class="public-doc-title">页面说明</div>
      <div class="public-doc-note">
        这份桥接页用于展示股票桥接模块的布局、服务状态卡片、模块层级和三源入口说明。
      </div>
      <div class="public-doc-grid">
        <div class="public-doc-item">
          <strong>AllTick 版本</strong>
          <span>保留来源名称与结构说明，方便展示多源接入后的页面组织方式。</span>
        </div>
        <div class="public-doc-item">
          <strong>通达信版本</strong>
          <span>保留版本入口与模块位置，用于说明来源切换和模块关系。</span>
        </div>
        <div class="public-doc-item">
          <strong>东方财富版本</strong>
          <span>保留界面入口和来源说明，方便展示页面风格与来源标识方式。</span>
        </div>
      </div>
      <div class="public-doc-tip">
        当前页面重点展示股票桥接模块的结构、入口与信息卡片布局。
      </div>
    </section>

    <div class="grid sensitive-section">
      <section class="card span-3">
        <div class="label">服务器 CPU</div>
        <div class="value" id="serverCpu">-</div>
        <div class="sub" id="serverCpuMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">服务器内存</div>
        <div class="value" id="serverMemory">-</div>
        <div class="sub" id="serverMemoryMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">系统盘占用</div>
        <div class="value" id="serverDisk">-</div>
        <div class="sub" id="serverDiskMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">服务器运行时间</div>
        <div class="value ok" id="serverUptime">-</div>
        <div class="sub" id="serverUptimeMeta"></div>
      </section>

      <section class="card span-12">
        <div class="label">服务器资源详情</div>
        <div class="kv" id="serverSummary"></div>
      </section>
      <section class="card span-3">
        <div class="label">桥接服务</div>
        <div class="value" id="serviceState">-</div>
        <div class="sub" id="serviceMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">执行模式</div>
        <div class="value" id="orderMode">-</div>
        <div class="sub" id="orderMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">信号池</div>
        <div class="value" id="signalCount">-</div>
        <div class="sub" id="signalMeta"></div>
      </section>

      <section class="card span-3">
        <div class="label">状态去重</div>
        <div class="value" id="processedCount">-</div>
        <div class="sub" id="processedMeta"></div>
      </section>

      <section class="card span-12">
        <div class="label">明日交易健康状态</div>
        <div class="value" id="tomorrowTradeStatus">-</div>
        <div class="sub" id="tomorrowTradeMeta"></div>
        <div class="kv" id="tomorrowTradeChecks" style="margin-top: 12px;"></div>
      </section>

      <section class="card span-4">
        <div class="label">重合股票</div>
        <div class="value" id="overlapCount">-</div>
        <div class="sub" id="overlapMeta"></div>
      </section>

      <section class="card span-4">
        <div class="label">3端重合</div>
        <div class="value ok" id="tripleCount">-</div>
        <div class="sub">AllTick / 东方财富 / 通达信 同时命中</div>
      </section>

      <section class="card span-4">
        <div class="label">2端重合</div>
        <div class="value warn" id="doubleCount">-</div>
        <div class="sub">任意两个来源同时命中</div>
      </section>

      <section class="card span-5">
        <div class="label">最新信号</div>
        <div class="kv" id="latestSignal"></div>
      </section>

      <section class="card span-7">
        <div class="label">桥接配置摘要</div>
        <div class="kv" id="bridgeSummary"></div>
      </section>

      <section class="card span-12">
        <div class="label">重合来源状态</div>
        <div class="source-pills" id="overlapSourceStatus"></div>
      </section>

      <section class="card span-12">
        <div class="label">2端 / 3端 重合推送</div>
        <div class="overlap-toolbar">
          <div class="overlap-toolbar-meta" id="overlapDisplayMeta"></div>
          <button class="overlap-toggle-btn" id="overlapListToggle" type="button" style="display:none;" onclick="toggleOverlapList()"></button>
        </div>
        <div class="overlap-list" id="overlapList"></div>
        <div class="empty" id="overlapEmpty" style="display:none;">当前没有可展示的 2 端或 3 端重合推送。</div>
      </section>

      <section class="card span-4">
        <div class="label">资金摘要</div>
        <div class="kv" id="accountSummary"></div>
      </section>

      <section class="card span-8">
        <div class="label">持仓摘要</div>
        <table>
          <thead>
            <tr>
              <th>持仓标识</th>
              <th>数量</th>
              <th>昨仓</th>
              <th>最近记录时间</th>
            </tr>
          </thead>
          <tbody id="positionRows"></tbody>
        </table>
        <div class="empty" id="positionEmpty" style="display:none;">当前日志里还没有可展示的持仓记录。</div>
      </section>

      <section class="card span-6">
        <div class="label">最近下单记录</div>
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>类型</th>
              <th>详情</th>
            </tr>
          </thead>
          <tbody id="orderRows"></tbody>
        </table>
        <div class="empty" id="orderEmpty" style="display:none;">最近日志里还没有新的下单或演练记录。</div>
      </section>

      <section class="card span-6">
        <div class="label">最近跳过原因统计</div>
        <table>
          <thead>
            <tr>
              <th>原因</th>
              <th>次数</th>
            </tr>
          </thead>
          <tbody id="skipRows"></tbody>
        </table>
        <div class="empty" id="skipEmpty" style="display:none;">最近日志里还没有跳过记录。</div>
      </section>

      <section class="card span-12">
        <div class="label" id="timelineTitle">信号时间线</div>
        <table>
          <thead>
            <tr>
              <th>时间</th>
              <th>代码</th>
              <th>名称</th>
              <th>信号价</th>
              <th>关键ID</th>
            </tr>
          </thead>
          <tbody id="timelineRows"></tbody>
        </table>
      </section>

      <section class="card span-7">
        <div class="label">最近信号明细</div>
        <table>
          <thead>
            <tr>
              <th>代码</th>
              <th>名称</th>
              <th>交易日</th>
              <th>信号时间</th>
              <th>信号价</th>
            </tr>
          </thead>
          <tbody id="signalRows"></tbody>
        </table>
      </section>

      <section class="card span-5">
        <div class="label">最近日志</div>
        <pre id="logTail"></pre>
      </section>

      <section class="card span-12">
        <div class="label">原始 JSON 详情</div>
        <pre id="rawJson"></pre>
      </section>
    </div>
  </div>

  <script>
    const OVERLAP_PAGE_SIZE = 3;
    const overlapState = {
      showAll: false,
      openKeys: new Set(),
      latestPayload: {},
    };
    const futuresState = { open: false };

    function esc(value) {
      return String(value ?? "-")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
    }

    function toggleOverlapList() {
      overlapState.showAll = !overlapState.showAll;
      renderOverlap(overlapState.latestPayload || {});
    }

    function toggleOverlapItem(symbol) {
      if (overlapState.openKeys.has(symbol)) {
        overlapState.openKeys.delete(symbol);
      } else {
        overlapState.openKeys.add(symbol);
      }
      renderOverlap(overlapState.latestPayload || {});
    }

    function toggleFuturesPanel() {
      futuresState.open = !futuresState.open;
      document.getElementById("futuresDetailPanel").style.display = futuresState.open ? "block" : "none";
      document.getElementById("futuresPanelToggle").textContent = futuresState.open ? "收起详情" : "展开详情";
    }

    function fillTableRows(tbodyId, rowsHtml, emptyId) {
      const tbody = document.getElementById(tbodyId);
      const empty = document.getElementById(emptyId);
      tbody.innerHTML = rowsHtml;
      if (empty) {
        empty.style.display = rowsHtml ? "none" : "block";
      }
    }

    function syncEnvironmentLinks() {
      const host = String(window.location.hostname || "").toLowerCase();
      const isLocalHost = host === "127.0.0.1" || host === "localhost";
      document.querySelectorAll("a[data-local-href][data-remote-href]").forEach((link) => {
        link.href = isLocalHost ? link.dataset.localHref : link.dataset.remoteHref;
      });
    }

    function renderHtmlTable(targetId, headers, rows, emptyText = "暂无数据") {
      const el = document.getElementById(targetId);
      if (!rows.length) {
        el.innerHTML = `<div class="empty">${esc(emptyText)}</div>`;
        return;
      }
      const thead = "<thead><tr>" + headers.map((item) => `<th>${esc(item)}</th>`).join("") + "</tr></thead>";
      const tbody = "<tbody>" + rows.map((row) => "<tr>" + row.map((cell) => `<td>${cell ?? ""}</td>`).join("") + "</tr>").join("") + "</tbody>";
      el.innerHTML = `<table>${thead}${tbody}</table>`;
    }

    function metricClass(level) {
      if (level === "danger") return "value danger";
      if (level === "warn") return "value warn";
      return "value ok";
    }

    function formatMoney(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return "-";
      }
      return num.toLocaleString("zh-CN", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      });
    }

    function formatSignedMoney(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return "-";
      }
      const absText = Math.abs(num).toLocaleString("zh-CN", {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      });
      return `${num > 0 ? "+" : num < 0 ? "-" : ""}${absText}`;
    }

    function formatWinRate(winningCount, closedCount) {
      const wins = Number(winningCount);
      const total = Number(closedCount);
      if (!Number.isFinite(wins) || !Number.isFinite(total) || total <= 0) {
        return "-";
      }
      return `${((wins / total) * 100).toFixed(1)}%`;
    }

    function formatPnlCell(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return "-";
      }
      const cls = num >= 0 ? "ok" : "danger";
      return `<span class="${cls}">${esc(formatSignedMoney(num))}</span>`;
    }

    function renderFuturesPairRows(targetId, rows, emptyText = "暂无已配对成交") {
      renderHtmlTable(
        targetId,
        ["开仓成交", "品种 / 合约", "开仓信息", "平仓信息", "持有", "配对盈亏", "状态"],
        rows.map((row) => [
          esc(row.open_timestamp || "-"),
          [
            esc(row.product || "-"),
            `<div class="sub"><code>${esc(row.exchange || "-")} | ${esc(row.tq_symbol || "-")} | ${esc(row.continuous_symbol || "-")}</code></div>`,
          ].join(""),
          [
            `<div>手数 ${esc(row.volume ?? "-")} | 开仓 ${esc(row.open_price ?? "-")}</div>`,
            `<div class="sub">参考 ${esc(row.reference_price || "-")} | 保证金 ${esc(row.margin_estimate || "-")}</div>`,
            `<div class="sub">形态 ${esc(row.pattern_time || "-")} | 信号 ${esc(row.signal_time || "-")}</div>`,
          ].join(""),
          row.close_timestamp
            ? [
                `<div>${esc(row.close_timestamp)}</div>`,
                `<div class="sub">平仓 ${esc(row.close_price ?? "-")} | 方法 ${esc(row.pair_method || "-")}</div>`,
              ].join("")
            : `<span class="warn">持仓中</span>`,
          esc(row.hold_text || "-"),
          formatPnlCell(row.realized_pnl),
          esc(row.status || "-"),
        ]),
        emptyText,
      );
    }

    function renderFuturesHistoryOpenRows(targetId, rows, emptyText = "暂无历史未配对开仓") {
      renderHtmlTable(
        targetId,
        ["开仓成交", "品种 / 合约", "开仓信息", "持有", "说明"],
        rows.map((row) => [
          esc(row.open_timestamp || "-"),
          [
            esc(row.product || "-"),
            `<div class="sub"><code>${esc(row.exchange || "-")} | ${esc(row.tq_symbol || "-")} | ${esc(row.continuous_symbol || "-")}</code></div>`,
          ].join(""),
          [
            `<div>手数 ${esc(row.volume ?? "-")} | 开仓 ${esc(row.open_price ?? "-")}</div>`,
            `<div class="sub">参考 ${esc(row.reference_price || "-")} | 保证金 ${esc(row.margin_estimate || "-")}</div>`,
            `<div class="sub">形态 ${esc(row.pattern_time || "-")} | 信号 ${esc(row.signal_time || "-")}</div>`,
          ].join(""),
          esc(row.hold_text || "-"),
          '<span class="warn">历史未配对</span>',
        ]),
        emptyText,
      );
    }

    function formatPct(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return "-";
      }
      return `${(num * 100).toFixed(num * 100 >= 10 ? 0 : 1)}%`;
    }

    function renderOverlap(overlap) {
      overlapState.latestPayload = overlap || {};
      const items = overlap.items || [];
      const sources = overlap.sources || [];
      const visibleItems = overlapState.showAll ? items : items.slice(0, OVERLAP_PAGE_SIZE);

      document.getElementById("overlapCount").textContent = overlap.overlap_count ?? 0;
      document.getElementById("tripleCount").textContent = overlap.triple_count ?? 0;
      document.getElementById("doubleCount").textContent = overlap.double_count ?? 0;
      document.getElementById("overlapMeta").textContent =
        `已加载 ${overlap.loaded_source_count ?? 0}/${overlap.expected_source_count ?? 0} 路来源，最新刷新 ${overlap.updated_at || "-"}`;

      document.getElementById("overlapSourceStatus").innerHTML = sources.map((source) => {
        const klass = source.ok ? "ok" : "warn";
        const tail = source.ok
          ? `${source.signal_total_count ?? 0} 条信号`
          : (source.error || "读取失败");
        return `<div class="source-pill ${klass}">${esc(source.label)} 路 ${esc(tail)}</div>`;
      }).join("");

      document.getElementById("overlapDisplayMeta").textContent =
        items.length > OVERLAP_PAGE_SIZE
          ? `默认只显示 ${OVERLAP_PAGE_SIZE} 个卡片，当前 ${visibleItems.length}/${items.length}`
          : `当前共 ${items.length} 个重合标的，点击卡片可展开详情`;

      const listToggle = document.getElementById("overlapListToggle");
      if (items.length > OVERLAP_PAGE_SIZE) {
        listToggle.style.display = "inline-flex";
        listToggle.textContent = overlapState.showAll ? "收起其余卡片" : `展开全部 ${items.length} 个`;
      } else {
        listToggle.style.display = "none";
        listToggle.textContent = "";
      }

      const overlapHtml = visibleItems.map((item) => {
        const matchClass = item.source_count >= 3 ? "match-3" : "match-2";
        const matchLabel = item.source_count >= 3 ? "3端重合" : "2端重合";
        const isOpen = overlapState.openKeys.has(item.symbol);
        const openClass = isOpen ? "open" : "collapsed";
        const toggleLabel = isOpen ? "收起" : "展开";
        const sourceBadges = (item.sources || []).map((source) =>
          `<span class="badge source-${esc(source.source_id)}">${esc(source.label)}</span>`
        ).join("");

        const lines = (item.entries || []).map((entry) => {
          const leftText = `左1 ${esc(entry.l1_time || "-")} @ ${esc(entry.l1_price || "-")} | 左2 ${esc(entry.l2_time || "-")} @ ${esc(entry.l2_price || "-")}`;
          const rightText = `右1 ${esc(entry.r1_time || "-")} @ ${esc(entry.r1_price || "-")} | 右2 ${esc(entry.r2_time || "-")} @ ${esc(entry.r2_price || "-")}`;
          return `
            <div class="overlap-line">
              <div class="overlap-line-top">
                <span>${esc(entry.source_label)}</span>
                <span class="overlap-line-time">${esc(entry.signal_stamp || "-")} @ ${esc(entry.signal_price || "-")}</span>
              </div>
              <div class="overlap-line-detail">${leftText}<br>${rightText}</div>
            </div>
          `;
        }).join("");

        return `
          <div class="overlap-item ${openClass}">
            <button class="overlap-card-button" type="button" onclick='toggleOverlapItem(${JSON.stringify(item.symbol)})'>
              <div class="overlap-top">
                <div class="overlap-head-left">
                  <span class="overlap-code"><code>${esc(item.symbol)}</code></span>
                  <span class="overlap-name">${esc(item.name || item.symbol)}</span>
                  <div class="overlap-summary">最新 ${esc(item.latest_stamp || "-")} | ${esc(item.source_count)} 路来源 | ${esc(toggleLabel)}详情</div>
                </div>
                <div class="overlap-badges">
                  <span class="badge ${matchClass}">${matchLabel}</span>
                  ${sourceBadges}
                  <span class="overlap-chevron">▾</span>
                </div>
              </div>
            </button>
            <div class="overlap-body">
              <div class="overlap-lines">${lines}</div>
            </div>
          </div>
        `;
      }).join("");

      document.getElementById("overlapList").innerHTML = overlapHtml;
      document.getElementById("overlapEmpty").style.display = overlapHtml ? "none" : "block";
    }

    function renderFuturesPanel(data) {
      const service = data.service || {};
      const bridge = data.bridge || {};
      const account = data.account || {};
      const connect = data.connect || {};
      const config = data.config || {};
      const orderConfig = config.order || {};
      const remote = data.remote_snapshot || {};
      const summary = remote.summary || {};
      const profit = data.profit_summary || {};
      const accountBalance = Number(account.balance ?? 0);
      const strategyCapPct = Number(orderConfig.strategy_cap_pct ?? 0);
      const singleSymbolCapPct = Number(orderConfig.single_symbol_cap_pct ?? 0);
      const strategyCapAmount =
        Number.isFinite(accountBalance) && Number.isFinite(strategyCapPct)
          ? accountBalance * strategyCapPct
          : NaN;
      const singleSymbolCapAmount =
        Number.isFinite(strategyCapAmount) && Number.isFinite(singleSymbolCapPct)
          ? strategyCapAmount * singleSymbolCapPct
          : NaN;
      const sizingModeLabel = ({
        capital_ratio: "按资金比例自动算手",
        dynamic: "动态自动算手",
        fixed: "固定手数",
      })[orderConfig.sizing_mode] || orderConfig.sizing_mode || "固定手数";

      document.getElementById("futuresServiceState").textContent = service.active_state || "-";
      document.getElementById("futuresServiceState").className = "value " + (service.active_state === "active" ? "ok" : "danger");
      document.getElementById("futuresServiceMeta").textContent = `SubState: ${service.sub_state || "-"} | PID: ${service.main_pid || "-"}`;

      document.getElementById("futuresOrderMode").textContent = bridge.order_enabled ? "自动模拟" : "Dry-run";
      document.getElementById("futuresOrderMode").className = "value " + (bridge.order_enabled ? "warn" : "ok");
      document.getElementById("futuresReadyMeta").textContent = `${bridge.ready ? "TqKq已就绪" : "TqKq待命"} | ${bridge.active_front_label || connect.app_id || "-"}`;

      document.getElementById("futuresAccountBalance").textContent = account.balance ?? "-";
      document.getElementById("futuresAccountBalance").className = "value ok";
      document.getElementById("futuresAccountMeta").textContent = `可用 ${account.available ?? "-"} | 账户 ${account.account || connect.username || "-"}`;

      document.getElementById("futuresSignalCount").textContent = summary.event_rows ?? 0;
      document.getElementById("futuresSignalCount").className = "value";
      document.getElementById("futuresSignalMeta").textContent = summary.latest_event_summary || summary.watch_note || "-";

      document.getElementById("futuresProfitValue").textContent = formatSignedMoney(profit.realized_total ?? 0);
      document.getElementById("futuresProfitValue").className = metricClass(profit.level);
      document.getElementById("futuresProfitMeta").textContent =
        `今日 ${formatSignedMoney(profit.realized_today ?? 0)} | 已平 ${profit.closed_count ?? 0} 笔 | 胜率 ${formatWinRate(profit.winning_count ?? 0, profit.closed_count ?? 0)}`;

      document.getElementById("futuresPanelMeta").textContent =
        `${bridge.ready ? "执行链路已就绪" : "执行链路待命"} | 最近就绪 ${bridge.last_ready_at || "-"} | 最新刷新 ${data.generated_at || "-"}`;

      document.getElementById("futuresQuickSummary").innerHTML = [
        ["执行模式", connect.environment || "TqKq 模拟"],
        ["账户", connect.username || account.account || "-"],
        ["白名单", (bridge.allowed_symbols || []).join(", ") || "-"],
        ["资金模式", sizingModeLabel],
        ["总策略资金上限", `${formatPct(strategyCapPct)} | ${formatMoney(strategyCapAmount)}`],
        ["单票资金上限", `${formatPct(singleSymbolCapPct)} | ${formatMoney(singleSymbolCapAmount)}`],
        ["轮询 / 更新", `${bridge.poll_interval_seconds ?? "-"}s / ${bridge.update_timeout_seconds ?? "-"}s`],
        ["每日上限", bridge.daily_order_limit ?? "-"],
        ["最大持仓", bridge.max_positions ?? "-"],
        ["止损 / 止盈", `${bridge.stop_loss_pct ?? "-"} / ${bridge.take_profit_pct ?? "-"}`],
        ["最近错误", (data.state || {}).last_error || "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("futuresBridgeSummary").innerHTML = [
        ["自动下单", bridge.order_enabled ? "开启" : "关闭"],
        ["下单方式", sizingModeLabel],
        ["固定手数", bridge.fixed_volume ?? "-"],
        ["单次最大手数", orderConfig.max_volume_per_order ?? "-"],
        ["默认保证金率", formatPct(orderConfig.default_margin_rate)],
        ["单品种上限", bridge.max_signals_per_symbol_per_day ?? "-"],
        ["持仓秒数", bridge.max_hold_seconds ?? "-"],
        ["解析策略", bridge.resolver_selection || "-"],
        ["映射缓存TTL", bridge.resolver_cache_ttl_seconds ?? "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("futuresConnectSummary").innerHTML = [
        ["账号", connect.username || "-"],
        ["模式", connect.environment || "-"],
        ["应用标识", connect.app_id || "-"],
        ["交易链路", connect.trade_front || "-"],
        ["行情链路", connect.market_front || "-"],
        ["账户余额", account.balance ?? "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      renderHtmlTable(
        "futuresResolvedTable",
        ["连续代码", "实际合约", "方式", "持仓量", "成交量", "解析时间"],
        (data.resolved_contracts || []).map((row) => [
          `<code>${esc(row.continuous_symbol || "-")}</code>`,
          `<code>${esc(row.tq_symbol || "-")}</code>`,
          esc(row.method || "-"),
          esc(row.open_interest ?? "-"),
          esc(row.volume ?? "-"),
          esc(row.resolved_at || "-"),
        ])
      );

      renderHtmlTable(
        "futuresPositionTable",
        ["合约", "连续代码", "策略 / 券商", "可平", "开仓价", "计划平仓", "待平仓单"],
        (data.positions || []).map((row) => [
          `<code>${esc(row.vt_symbol || "-")}</code>`,
          esc(row.continuous_symbol || "-"),
          esc(row.volume ?? "-"),
          esc(row.broker_volume ?? "-"),
          esc(row.closable_volume ?? "-"),
          esc(row.entry_price ?? "-"),
          esc(row.planned_exit_at || "-"),
          esc(row.pending_exit_orderid || "-"),
        ])
      );

      renderHtmlTable(
        "futuresSignalTable",
        ["连续代码", "品种", "交易所", "信号时间", "参考价"],
        (data.recent_signals || []).map((row) => [
          `<code>${esc(row.continuous_symbol || "-")}</code>`,
          esc(row.product || "-"),
          esc(row.exchange || "-"),
          esc(row.signal_time || "-"),
          esc(row.reference_price || "-"),
        ])
      );

      const pairingSummary = data.pairing_summary || {};
      document.getElementById("futuresPairMeta").textContent =
        `按 ${pairingSummary.pair_method || "FIFO"} 配对 | 已平 ${pairingSummary.closed_count ?? 0} | 历史未配对开仓 ${pairingSummary.history_open_count ?? pairingSummary.open_count ?? 0}` +
        `${(pairingSummary.unmatched_close_count || 0) > 0 ? ` | 未匹配平仓 ${pairingSummary.unmatched_close_count}` : ""}`;
      renderFuturesPairRows("futuresOrderTable", data.paired_trade_records || []);
      document.getElementById("futuresHistoryOpenMeta").textContent =
        `历史未配对开仓 ${pairingSummary.history_open_count ?? pairingSummary.open_count ?? 0} 条 | 仅历史回放未配对，不代表真实当前持仓`;
      renderFuturesHistoryOpenRows("futuresHistoryOpenTable", data.history_open_records || []);

      renderHtmlTable(
        "futuresSkipTable",
        ["原因", "次数"],
        (data.skip_reason_counts || []).map((row) => [
          esc(row.reason || "-"),
          esc(row.count ?? 0),
        ])
      );

      document.getElementById("futuresLogTail").textContent = (data.logs || []).join("\\n") || "暂无日志";
    }

    syncEnvironmentLinks();

    async function loadData() {
      const response = await fetch("api/data", { cache: "no-store" });
      const data = await response.json();

      const service = data.service || {};
      const server = data.server || {};
      const bridge = data.bridge || {};
      const snapshot = data.snapshot || {};
      const latest = snapshot.latest_signal || {};
      const account = data.account || {};
      const tomorrowTradeHealth = data.tomorrow_trade_health || {};
      const positions = data.positions || [];
      const orders = data.recent_order_records || [];
      const skips = data.skip_reason_counts || [];
      const timeline = data.signal_timeline || [];
      const overlap = data.overlap || {};

      document.getElementById("generatedAt").textContent = data.generated_at || "-";
      document.getElementById("serverCpu").textContent =
        server.cpu_percent != null ? `${server.cpu_percent}%` : "-";
      document.getElementById("serverCpu").className = metricClass(server.cpu_level);
      document.getElementById("serverCpuMeta").textContent =
        `${server.cpu_count ?? "-"} 核 | 1m 负载 ${server.load_1m ?? "-"} | 负载率 ${server.load_pct_1m ?? "-"}%`;

      document.getElementById("serverMemory").textContent =
        server.memory_used_pct != null ? `${server.memory_used_pct}%` : "-";
      document.getElementById("serverMemory").className = metricClass(server.memory_level);
      document.getElementById("serverMemoryMeta").textContent =
        `${server.memory_used_gb ?? "-"} / ${server.memory_total_gb ?? "-"} GB | 可用 ${server.memory_available_gb ?? "-"} GB`;

      document.getElementById("serverDisk").textContent =
        server.disk_used_pct != null ? `${server.disk_used_pct}%` : "-";
      document.getElementById("serverDisk").className = metricClass(server.disk_level);
      document.getElementById("serverDiskMeta").textContent =
        `${server.disk_used_gb ?? "-"} / ${server.disk_total_gb ?? "-"} GB | 剩余 ${server.disk_free_gb ?? "-"} GB`;

      document.getElementById("serverUptime").textContent = server.uptime_text || "-";
      document.getElementById("serverUptime").className = "value ok";
      document.getElementById("serverUptimeMeta").textContent =
        `更新时间 ${server.updated_at || "-"} | Swap ${server.swap_used_gb ?? "-"} / ${server.swap_total_gb ?? "-"} GB`;

      document.getElementById("serverSummary").innerHTML = [
        ["CPU实时占用", server.cpu_percent != null ? `${server.cpu_percent}%` : "-"],
        ["CPU核心数", server.cpu_count ?? "-"],
        ["1m / 5m / 15m 负载", `${server.load_1m ?? "-"} / ${server.load_5m ?? "-"} / ${server.load_15m ?? "-"}`],
        ["1分钟负载率", server.load_pct_1m != null ? `${server.load_pct_1m}%` : "-"],
        ["内存占用", server.memory_used_pct != null ? `${server.memory_used_pct}%` : "-"],
        ["内存用量", `${server.memory_used_gb ?? "-"} / ${server.memory_total_gb ?? "-"} GB`],
        ["可用内存", server.memory_available_gb != null ? `${server.memory_available_gb} GB` : "-"],
        ["Swap占用", server.swap_total_gb ? `${server.swap_used_gb ?? "-"} / ${server.swap_total_gb ?? "-"} GB (${server.swap_used_pct ?? "-"}%)` : "未配置"],
        ["系统盘占用", server.disk_used_pct != null ? `${server.disk_used_pct}%` : "-"],
        ["系统盘用量", `${server.disk_used_gb ?? "-"} / ${server.disk_total_gb ?? "-"} GB`],
        ["系统盘剩余", server.disk_free_gb != null ? `${server.disk_free_gb} GB` : "-"],
        ["系统运行时间", server.uptime_text || "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("serviceState").textContent = service.active_state || "-";
      document.getElementById("serviceState").className = "value " + (service.active_state === "active" ? "ok" : "danger");
      document.getElementById("serviceMeta").textContent =
        `SubState: ${service.sub_state || "-"} | PID: ${service.main_pid || "-"} | 内存: ${service.memory_mb || "-"} MB`;

      document.getElementById("orderMode").textContent = bridge.order_enabled ? "真实下单" : "演练模式";
      document.getElementById("orderMode").className = "value " + (bridge.order_enabled ? "warn" : "ok");
      document.getElementById("orderMeta").textContent =
        `client_id=${bridge.client_id || "-"} | 每次 ${bridge.fixed_volume || "-"} 股 | 每日上限 ${bridge.daily_limit || "-"}`;

      document.getElementById("signalCount").textContent = snapshot.signal_total_count ?? "-";
      document.getElementById("signalMeta").textContent =
        `自选 ${snapshot.watchlist_count ?? "-"} | 最新Tick ${snapshot.latest_tick_symbol || "-"} @ ${snapshot.latest_tick_price || "-"}`;

      document.getElementById("processedCount").textContent = bridge.processed_count ?? "-";
      document.getElementById("processedMeta").textContent =
        `bootstrap=${bridge.bootstrap_complete ? "true" : "false"} | 今日已记单 ${bridge.today_order_total ?? 0}`;

      document.getElementById("tomorrowTradeStatus").textContent = tomorrowTradeHealth.status || "-";
      document.getElementById("tomorrowTradeStatus").className = metricClass(tomorrowTradeHealth.level || "warn");
      document.getElementById("tomorrowTradeMeta").textContent =
        `${tomorrowTradeHealth.date || "-"} ${tomorrowTradeHealth.weekday || ""} | ${tomorrowTradeHealth.summary || "-"}`;
      document.getElementById("tomorrowTradeChecks").innerHTML = [
        ...(tomorrowTradeHealth.checks || []).map((item) => [
          item.name,
          `${item.ok ? "OK" : "需处理"} | ${item.detail || "-"}`
        ]),
        ...(tomorrowTradeHealth.last_error ? [["最近错误", tomorrowTradeHealth.last_error]] : []),
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("latestSignal").innerHTML = [
        ["代码", latest.symbol || "-"],
        ["名称", latest.name || "-"],
        ["交易日", latest.trading_day || "-"],
        ["信号时间", latest.signal_time || "-"],
        ["信号价", latest.signal_price || "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("bridgeSummary").innerHTML = [
        ["推送源", bridge.snapshot_url || "-"],
        ["日志级别", bridge.log_level || "-"],
        ["运行记录", bridge.log_file || "-"],
        ["模块状态", bridge.state_file || "-"],
        ["轮询间隔", `${bridge.poll_interval_seconds ?? "-"} 秒`],
        ["交易时段", (bridge.trading_sessions || []).join(" , ") || "-"],
        ["过滤规则", (bridge.filter_flags || []).join(" / ") || "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      document.getElementById("accountSummary").innerHTML = [
        ["账户", account.account || "-"],
        ["总资产", account.balance ?? "-"],
        ["可用资金", account.available ?? "-"],
        ["冻结估算", account.frozen ?? "-"],
        ["持仓数量", account.position_count ?? 0],
        ["最后更新", account.timestamp || "-"],
      ].map(([k, v]) => `<div>${esc(k)}</div><div>${esc(v)}</div>`).join("");

      renderOverlap(overlap);

      fillTableRows("positionRows", positions.map((item) => `
        <tr>
          <td><code>${esc(item.vt_positionid || "-")}</code></td>
          <td>${esc(item.volume ?? "-")}</td>
          <td>${esc(item.yd_volume ?? "-")}</td>
          <td>${esc(item.timestamp || "-")}</td>
        </tr>
      `).join(""), "positionEmpty");

      fillTableRows("orderRows", orders.map((item) => `
        <tr>
          <td>${esc(item.timestamp || "-")}</td>
          <td>${esc(item.kind || "-")}</td>
          <td>${esc(item.detail || "-")}</td>
        </tr>
      `).join(""), "orderEmpty");

      fillTableRows("skipRows", skips.map((item) => `
        <tr>
          <td>${esc(item.reason || "-")}</td>
          <td>${esc(item.count ?? 0)}</td>
        </tr>
      `).join(""), "skipEmpty");

      document.getElementById("timelineTitle").textContent =
        `信号时间线（${data.timeline_day || "-"}）`;
      document.getElementById("timelineRows").innerHTML = timeline.map((item) => `
        <tr>
          <td>${esc(item.signal_time || "-")}</td>
          <td><code>${esc(item.symbol || "-")}</code></td>
          <td>${esc(item.name || "-")}</td>
          <td>${esc(item.signal_price || "-")}</td>
          <td><code>${esc(item.key || "-")}</code></td>
        </tr>
      `).join("");

      document.getElementById("signalRows").innerHTML = (snapshot.signals || []).map((item) => `
        <tr>
          <td><code>${esc(item.symbol || "-")}</code></td>
          <td>${esc(item.name || "-")}</td>
          <td>${esc(item.trading_day || "-")}</td>
          <td>${esc(item.signal_time || "-")}</td>
          <td>${esc(item.signal_price || "-")}</td>
        </tr>
      `).join("");

      document.getElementById("logTail").textContent = (data.logs || []).join("\\n");
      document.getElementById("rawJson").textContent = JSON.stringify(data, null, 2);
    }

    loadData();
    setInterval(loadData, 5000);
  </script>
</body>
</html>
"""


FUTURES_HTML_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Push-CTP 期货仪表盘</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #101a2b;
      --panel-2: #13203a;
      --text: #e7edf7;
      --muted: #8fa1be;
      --accent: #31c48d;
      --warn: #f5b942;
      --danger: #ef6b73;
      --line: #223251;
      --mono: "Cascadia Code", Consolas, monospace;
      --sans: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: linear-gradient(180deg, #09111d 0%, #0e1627 100%);
      color: var(--text);
      font-family: var(--sans);
    }
    .wrap { width: min(1320px, calc(100vw - 24px)); margin: 0 auto; padding: 18px 0 36px; }
    .hero { display: flex; justify-content: space-between; align-items: end; gap: 16px; margin-bottom: 16px; }
    h1 { margin: 0; font-size: 30px; line-height: 1.1; }
    .sub { color: var(--muted); margin-top: 6px; font-size: 14px; }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; }
    .card {
      background: linear-gradient(180deg, rgba(19, 32, 58, 0.95), rgba(16, 26, 43, 0.95));
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18);
    }
    .span-3 { grid-column: span 3; }
    .span-4 { grid-column: span 4; }
    .span-6 { grid-column: span 6; }
    .span-8 { grid-column: span 8; }
    .span-12 { grid-column: span 12; }
    .label { color: var(--muted); font-size: 13px; margin-bottom: 8px; }
    .value { font-size: 26px; font-weight: 700; }
    .ok { color: var(--accent); }
    .warn { color: var(--warn); }
    .danger { color: var(--danger); }
    .kv { display: grid; grid-template-columns: 150px 1fr; gap: 10px 12px; font-size: 14px; }
    .kv div:nth-child(odd) { color: var(--muted); }
    table { width: 100%; border-collapse: collapse; font-size: 14px; }
    th, td { padding: 10px 8px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }
    th { color: var(--muted); font-weight: 600; }
    code, pre { font-family: var(--mono); font-size: 12px; }
    pre { margin: 0; white-space: pre-wrap; word-break: break-word; max-height: 380px; overflow: auto; }
    .empty { color: var(--muted); font-size: 14px; padding: 8px 0; }
    .nav { display: flex; gap: 10px; flex-wrap: wrap; }
    .nav a {
      color: var(--text);
      text-decoration: none;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(18, 34, 58, 0.88);
    }
    .nav a:hover { border-color: rgba(49, 196, 141, 0.4); color: var(--accent); }
    .public-doc-card {
      margin-bottom: 16px;
      padding: 20px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, rgba(19, 32, 58, 0.96), rgba(13, 23, 39, 0.96));
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.16);
    }
    .public-doc-title {
      font-size: 22px;
      font-weight: 700;
      margin-bottom: 8px;
    }
    .public-doc-note {
      color: var(--muted);
      font-size: 14px;
      line-height: 1.75;
    }
    .public-doc-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }
    .public-doc-item {
      padding: 16px;
      border-radius: 14px;
      border: 1px solid var(--line);
      background: rgba(14, 24, 40, 0.9);
    }
    .public-doc-item strong {
      display: block;
      font-size: 16px;
      margin-bottom: 6px;
    }
    .public-doc-item span {
      color: var(--muted);
      font-size: 13px;
      line-height: 1.65;
    }
    .public-doc-tip {
      margin-top: 14px;
      padding: 14px 16px;
      border-radius: 14px;
      border: 1px solid rgba(49, 196, 141, 0.24);
      background: rgba(49, 196, 141, 0.08);
      color: #d6f7e6;
      font-size: 13px;
      line-height: 1.7;
    }
    @media (max-width: 1100px) {
      .span-3, .span-4, .span-6, .span-8, .span-12 { grid-column: span 12; }
      .public-doc-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body class="public-github">
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>Push-CTP 期货仪表盘</h1>
        <div class="sub" id="generatedAt">展示期货桥接模块的结构、入口关系、卡片区域与功能布局。</div>
      </div>
      <div class="nav">
        <a href="/push" data-local-href="/push" data-remote-href="/push">返回股票信号页</a>
        <a href="/futures" data-local-href="/futures" data-remote-href="/futures">返回期货信号页</a>
        <a href="/bridge/" data-local-href="/bridge/" data-remote-href="/bridge/">股票桥接</a>
        <a href="/bridge/futures" data-local-href="/bridge/futures" data-remote-href="/bridge/futures">期货桥接</a>
        <a href="/notifications" data-local-href="/notifications" data-remote-href="/notifications">通知推送</a>
      </div>
    </div>
    <section class="public-doc-card">
      <div class="public-doc-title">页面说明</div>
      <div class="public-doc-note">
        这份期货桥接页用于展示期货桥接模块的整体结构、入口关系、卡片布局和功能区域。
      </div>
      <div class="public-doc-grid">
        <div class="public-doc-item">
          <strong>桥接结构</strong>
          <span>保留股票桥接、期货桥接和信号页入口，方便展示系统模块关系。</span>
        </div>
        <div class="public-doc-item">
          <strong>模块布局保留</strong>
          <span>账户区、持仓区、成交区、历史区和日志区都保留在页面中，便于演示完整布局。</span>
        </div>
        <div class="public-doc-item">
          <strong>完整能力接入</strong>
          <span>如需启用完整桥接与交易流程，可在部署环境中接入账户、数据源与消息通道。</span>
        </div>
      </div>
      <div class="public-doc-tip">
        当前页面重点展示期货桥接模块的功能结构、卡片布局与入口关系。
      </div>
    </section>
    <div class="grid sensitive-section">
      <section class="card span-3">
        <div class="label">桥接服务</div>
        <div class="value" id="serviceState">-</div>
        <div class="sub" id="serviceDetail">-</div>
      </section>
      <section class="card span-3">
        <div class="label">桥接模式</div>
        <div class="value" id="orderMode">-</div>
        <div class="sub" id="readyState">-</div>
      </section>
      <section class="card span-3">
        <div class="label">CTP 资金</div>
        <div class="value" id="accountBalance">-</div>
        <div class="sub" id="accountAvailable">-</div>
      </section>
      <section class="card span-3">
        <div class="label">远端信号</div>
        <div class="value" id="eventRows">-</div>
        <div class="sub" id="watchNote">-</div>
      </section>
      <section class="card span-3">
        <div class="label">已实现盈亏</div>
        <div class="value" id="profitValue">-</div>
        <div class="sub" id="profitMeta">-</div>
      </section>
      <section class="card span-3">
        <div class="label">服务器 CPU</div>
        <div class="value" id="cpuValue">-</div>
        <div class="sub" id="loadValue">-</div>
      </section>
      <section class="card span-3">
        <div class="label">服务器内存</div>
        <div class="value" id="memValue">-</div>
        <div class="sub" id="swapValue">-</div>
      </section>
      <section class="card span-3">
        <div class="label">系统盘</div>
        <div class="value" id="diskValue">-</div>
        <div class="sub" id="uptimeValue">-</div>
      </section>
      <section class="card span-6">
        <div class="label">期货策略摘要</div>
        <div class="kv" id="bridgeSummary"></div>
      </section>
      <section class="card span-6">
        <div class="label">CTP 连接配置</div>
        <div class="kv" id="connectSummary"></div>
      </section>
      <section class="card span-12">
        <div class="label">最近信号</div>
        <div id="signalTable"></div>
      </section>
      <section class="card span-12">
        <div class="label">已平开平配对</div>
        <div class="sub" id="pairTableMeta">按成交顺序配对，避免把混仓均价盈亏挂到单笔开仓</div>
        <div id="pairTable"></div>
      </section>
      <section class="card span-6">
        <div class="label">真实当前持仓</div>
        <div id="positionTable"></div>
      </section>
      <section class="card span-6">
        <div class="label">历史未配对开仓</div>
        <div class="sub" id="historyOpenMeta">仅历史回放未配对，不代表真实当前持仓</div>
        <div id="historyOpenTable"></div>
      </section>
      <section class="card span-6">
        <div class="label">原始委托 / 成交事件</div>
        <div id="orderTable"></div>
      </section>
      <section class="card span-6">
        <div class="label">跳过原因统计</div>
        <div id="skipTable"></div>
      </section>
      <section class="card span-12">
        <div class="label">桥接日志</div>
        <pre id="logBlock">加载中...</pre>
      </section>
    </div>
  </div>
  <script>
    const emptyText = '<div class="empty">暂无数据</div>';

    function esc(value) {
      return String(value ?? '-')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function levelClass(level) {
      if (level === 'danger') return 'danger';
      if (level === 'warn') return 'warn';
      return 'ok';
    }

    function renderKv(targetId, rows) {
      const el = document.getElementById(targetId);
      if (!rows.length) {
        el.innerHTML = emptyText;
        return;
      }
      el.innerHTML = rows.map((row) => `<div>${esc(row[0])}</div><div>${esc(row[1])}</div>`).join('');
    }

    function renderTable(targetId, headers, rows) {
      const el = document.getElementById(targetId);
      if (!rows.length) {
        el.innerHTML = emptyText;
        return;
      }
      const thead = '<thead><tr>' + headers.map((item) => `<th>${esc(item)}</th>`).join('') + '</tr></thead>';
      const tbody = '<tbody>' + rows.map((row) => '<tr>' + row.map((cell) => `<td>${cell ?? ''}</td>`).join('') + '</tr>').join('') + '</tbody>';
      el.innerHTML = `<table>${thead}${tbody}</table>`;
    }

    function formatSignedMoney(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      const absText = Math.abs(num).toLocaleString('zh-CN', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 2,
      });
      return `${num > 0 ? '+' : num < 0 ? '-' : ''}${absText}`;
    }

    function formatWinRate(winningCount, closedCount) {
      const wins = Number(winningCount);
      const total = Number(closedCount);
      if (!Number.isFinite(wins) || !Number.isFinite(total) || total <= 0) {
        return '-';
      }
      return `${((wins / total) * 100).toFixed(1)}%`;
    }

    function formatPnlCell(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) {
        return '-';
      }
      const cls = num >= 0 ? 'ok' : 'danger';
      return `<span class="${cls}">${esc(formatSignedMoney(num))}</span>`;
    }

    async function loadData() {
      const response = await fetch('api/futures', { cache: 'no-store' });
      const data = await response.json();

      const service = data.service || {};
      const bridge = data.bridge || {};
      const server = data.server || {};
      const remote = data.remote_snapshot || {};
      const remoteSummary = remote.summary || {};
      const account = data.account || {};
      const connect = data.connect || {};
      const profit = data.profit_summary || {};
      const readyText = bridge.ready ? 'TqKq 已就绪' : 'TqKq 待命';

      document.getElementById('generatedAt').textContent = `最近更新：${data.generated_at || '-'}`;
      document.getElementById('serviceState').innerHTML = `<span class="${service.active_state === 'active' ? 'ok' : 'danger'}">${esc(service.active_state || '-')}</span>`;
      document.getElementById('serviceDetail').textContent = `SubState: ${service.sub_state || '-'} | PID: ${service.main_pid || '-'}`;
      document.getElementById('orderMode').innerHTML = `<span class="${bridge.order_enabled ? 'warn' : 'ok'}">${bridge.order_enabled ? '自动模拟' : 'Dry-run'}</span>`;
      document.getElementById('readyState').textContent = `${readyText} | ${bridge.active_front_label || connect.environment || '-'}`;
      document.getElementById('accountBalance').textContent = account.balance ?? '-';
      document.getElementById('accountAvailable').textContent = `可用 ${account.available ?? '-'} | 账户 ${account.account || connect.username || '-'}`;
      document.getElementById('eventRows').textContent = remoteSummary.event_rows ?? 0;
      document.getElementById('watchNote').textContent = remoteSummary.watch_note || remoteSummary.latest_event_summary || '-';
      document.getElementById('profitValue').textContent = formatSignedMoney(profit.realized_total ?? 0);
      document.getElementById('profitValue').className = `value ${profit.level === 'danger' ? 'danger' : profit.level === 'warn' ? 'warn' : 'ok'}`;
      document.getElementById('profitMeta').textContent =
        `今日 ${formatSignedMoney(profit.realized_today ?? 0)} | 已平 ${profit.closed_count ?? 0} 笔 | 胜率 ${formatWinRate(profit.winning_count ?? 0, profit.closed_count ?? 0)}`;
      document.getElementById('cpuValue').innerHTML = `<span class="${levelClass(server.cpu_level)}">${server.cpu_percent ?? '-'}%</span>`;
      document.getElementById('loadValue').textContent = `1m 负载 ${server.load_1m ?? '-'} / ${server.load_pct_1m ?? '-'}%`;
      document.getElementById('memValue').innerHTML = `<span class="${levelClass(server.memory_level)}">${server.memory_used_pct ?? '-'}%</span>`;
      document.getElementById('swapValue').textContent = `可用 ${server.memory_available_gb ?? '-'} GB / Swap ${server.swap_used_gb ?? 0} GB`;
      document.getElementById('diskValue').innerHTML = `<span class="${levelClass(server.disk_level)}">${server.disk_used_pct ?? '-'}%</span>`;
      document.getElementById('uptimeValue').textContent = `运行 ${server.uptime_text || '-'}`;

      renderKv('bridgeSummary', [
        ['轮询间隔', `${bridge.poll_interval_seconds ?? '-'} 秒`],
        ['白名单', (bridge.allowed_symbols || []).join(', ') || '-'],
        ['日内上限', bridge.daily_order_limit ?? '-'],
        ['单品种上限', bridge.max_signals_per_symbol_per_day ?? '-'],
        ['最大持仓数', bridge.max_positions ?? '-'],
        ['固定手数', bridge.fixed_volume ?? '-'],
        ['退出秒数', bridge.max_hold_seconds ?? '-'],
        ['止损 / 止盈', `${bridge.stop_loss_pct ?? '-'} / ${bridge.take_profit_pct ?? '-'}`],
      ]);

      renderKv('connectSummary', [
        ['投资者账号', connect.username || '-'],
        ['经纪商代码', connect.broker_id || '-'],
        ['产品名称', connect.app_id || '-'],
        ['柜台环境', connect.environment || '-'],
        ['交易前置', connect.trade_front || '-'],
        ['行情前置', connect.market_front || '-'],
      ]);

      renderTable('signalTable', ['连续代码', '品种', '交易所', '信号时间', '参考价'], (data.recent_signals || []).map((row) => [
        esc(row.continuous_symbol),
        esc(row.product),
        esc(row.exchange),
        esc(row.signal_time),
        esc(row.reference_price),
      ]));

      const pairingSummary = data.pairing_summary || {};
      document.getElementById('pairTableMeta').textContent =
        `按 ${pairingSummary.pair_method || 'FIFO'} 配对 | 已平 ${pairingSummary.closed_count ?? 0} | 历史未配对开仓 ${pairingSummary.history_open_count ?? pairingSummary.open_count ?? 0}` +
        `${(pairingSummary.unmatched_close_count || 0) > 0 ? ` | 未匹配平仓 ${pairingSummary.unmatched_close_count}` : ''}`;

      renderTable('pairTable', ['开仓成交', '品种 / 合约', '开仓信息', '平仓信息', '持有', '配对盈亏', '状态'], (data.paired_trade_records || []).map((row) => [
        esc(row.open_timestamp || '-'),
        [
          esc(row.product || '-'),
          `<div class="sub"><code>${esc(row.exchange || '-')} | ${esc(row.tq_symbol || '-')} | ${esc(row.continuous_symbol || '-')}</code></div>`,
        ].join(''),
        [
          `<div>手数 ${esc(row.volume ?? '-')} | 开仓 ${esc(row.open_price ?? '-')}</div>`,
          `<div class="sub">参考 ${esc(row.reference_price || '-')} | 保证金 ${esc(row.margin_estimate || '-')}</div>`,
          `<div class="sub">形态 ${esc(row.pattern_time || '-')} | 信号 ${esc(row.signal_time || '-')}</div>`,
        ].join(''),
        row.close_timestamp
          ? [
              `<div>${esc(row.close_timestamp)}</div>`,
              `<div class="sub">平仓 ${esc(row.close_price ?? '-')} | 方法 ${esc(row.pair_method || '-')}</div>`,
            ].join('')
          : '<span class="warn">持仓中</span>',
        esc(row.hold_text || '-'),
        formatPnlCell(row.realized_pnl),
        esc(row.status || '-'),
      ]));

      renderTable('positionTable', ['合约', '连续代码', '策略手数', '券商持仓', '可平', '开仓价', '计划平仓', '待平仓单'], (data.positions || []).map((row) => [
        `<code>${esc(row.vt_symbol)}</code>`,
        esc(row.continuous_symbol || '-'),
        esc(row.volume),
        esc(row.broker_volume ?? '-'),
        esc(row.closable_volume ?? '-'),
        esc(row.entry_price),
        esc(row.planned_exit_at || '-'),
        esc(row.pending_exit_orderid || '-'),
      ]));

      document.getElementById('historyOpenMeta').textContent =
        `历史未配对开仓 ${pairingSummary.history_open_count ?? pairingSummary.open_count ?? 0} 条 | 仅历史回放未配对，不代表真实当前持仓`;
      renderTable('historyOpenTable', ['开仓成交', '品种 / 合约', '开仓信息', '持有', '说明'], (data.history_open_records || []).map((row) => [
        esc(row.open_timestamp || '-'),
        [
          esc(row.product || '-'),
          `<div class="sub"><code>${esc(row.exchange || '-')} | ${esc(row.tq_symbol || '-')} | ${esc(row.continuous_symbol || '-')}</code></div>`,
        ].join(''),
        [
          `<div>手数 ${esc(row.volume ?? '-')} | 开仓 ${esc(row.open_price ?? '-')}</div>`,
          `<div class="sub">参考 ${esc(row.reference_price || '-')} | 保证金 ${esc(row.margin_estimate || '-')}</div>`,
          `<div class="sub">形态 ${esc(row.pattern_time || '-')} | 信号 ${esc(row.signal_time || '-')}</div>`,
        ].join(''),
        esc(row.hold_text || '-'),
        '<span class="warn">历史未配对</span>',
      ]));

      renderTable('orderTable', ['时间', '类型', '详情'], (data.recent_order_records || []).map((row) => [
        esc(row.timestamp),
        esc(row.kind),
        esc(row.detail),
      ]));

      renderTable('skipTable', ['原因', '次数'], (data.skip_reason_counts || []).map((row) => [
        esc(row.reason),
        esc(row.count),
      ]));

      document.getElementById('logBlock').textContent = (data.logs || []).join('\\n') || '暂无日志';
    }

    loadData();
    setInterval(loadData, 5000);
  </script>
</body>
</html>
"""


NOTIFICATIONS_HTML_TEMPLATE = """
<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>通知推送中心</title>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #101a2b;
      --panel-2: #13203a;
      --text: #e7edf7;
      --muted: #8fa1be;
      --accent: #31c48d;
      --cyan: #61dafb;
      --line: #223251;
      --warn: #f5b942;
      --danger: #ef6b73;
      --sans: "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      --mono: "Cascadia Code", "SFMono-Regular", Consolas, monospace;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: linear-gradient(180deg, #09111d 0%, #0e1627 100%);
      color: var(--text);
      font-family: var(--sans);
    }
    .wrap {
      width: min(1380px, calc(100vw - 24px));
      margin: 0 auto;
      padding: 18px 0 40px;
    }
    .hero {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      align-items: end;
      margin-bottom: 18px;
    }
    h1 { margin: 0; font-size: 30px; line-height: 1.1; }
    .sub { margin-top: 6px; color: var(--muted); font-size: 14px; }
    .nav {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .nav a {
      color: var(--text);
      text-decoration: none;
      padding: 8px 12px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(18, 34, 58, 0.88);
    }
    .nav a:hover { border-color: rgba(49, 196, 141, 0.4); color: var(--accent); }
    .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 14px; }
    .card {
      background: linear-gradient(180deg, rgba(19, 32, 58, 0.95), rgba(16, 26, 43, 0.95));
      border: 1px solid var(--line);
      border-radius: 16px;
      padding: 16px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.18);
    }
    .span-4 { grid-column: span 4; }
    .span-5 { grid-column: span 5; }
    .span-7 { grid-column: span 7; }
    .span-12 { grid-column: span 12; }
    .label { color: var(--muted); font-size: 13px; margin-bottom: 10px; }
    .title { font-size: 22px; font-weight: 700; margin-bottom: 6px; }
    .note { color: var(--muted); font-size: 13px; line-height: 1.75; }
    .field { margin-top: 12px; }
    .field label {
      display: block;
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }
    .field input, .field textarea, .field select {
      width: 100%;
      border: 1px solid var(--line);
      background: rgba(10, 18, 31, 0.92);
      color: var(--text);
      border-radius: 12px;
      padding: 10px 12px;
      font: inherit;
    }
    .field textarea { min-height: 116px; resize: vertical; }
    .switch-row {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 10px;
    }
    .channel-name { font-size: 20px; font-weight: 700; }
    .event-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px;
      margin-top: 12px;
    }
    .event-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: 12px;
      background: rgba(12, 21, 37, 0.82);
      font-size: 13px;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 14px;
    }
    button {
      border: 1px solid rgba(95, 139, 255, 0.24);
      background: linear-gradient(180deg, rgba(79, 130, 255, 0.92), rgba(47, 96, 214, 0.92));
      color: white;
      border-radius: 12px;
      padding: 10px 16px;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    button.secondary {
      background: rgba(18, 34, 58, 0.88);
      border-color: var(--line);
      color: var(--text);
    }
    button.warn {
      background: linear-gradient(180deg, rgba(245, 185, 66, 0.92), rgba(212, 145, 21, 0.92));
    }
    .status-box {
      margin-top: 12px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid rgba(49, 196, 141, 0.2);
      background: rgba(49, 196, 141, 0.08);
      color: #d6f7e6;
      font-size: 13px;
      line-height: 1.7;
      white-space: pre-wrap;
      word-break: break-word;
    }
    .mono {
      font-family: var(--mono);
      font-size: 12px;
    }
    .doc-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 14px;
    }
    .code-block {
      margin-top: 10px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid var(--line);
      background: rgba(9, 17, 29, 0.94);
      color: #d7e0ee;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: var(--mono);
      font-size: 12px;
      line-height: 1.7;
    }
    .doc-list {
      margin: 10px 0 0;
      padding-left: 18px;
      color: #d7e0ee;
      font-size: 13px;
      line-height: 1.8;
    }
    .doc-tip {
      margin-top: 14px;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid rgba(245, 185, 66, 0.18);
      background: rgba(245, 185, 66, 0.08);
      color: #ffe7ab;
      font-size: 13px;
      line-height: 1.8;
    }
    @media (max-width: 1100px) {
      .span-4, .span-5, .span-7, .span-12 { grid-column: span 12; }
      .doc-grid, .event-grid { grid-template-columns: 1fr; }
      .hero { display: block; }
      .nav { margin-top: 14px; justify-content: flex-start; }
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div>
        <h1>通知推送中心</h1>
        <div class="sub">统一配置飞书、钉钉、企业微信 Webhook，并通过一个接口接收股票信号、期货信号、股票交易信号、期货交易信号。</div>
      </div>
      <div class="nav">
        <a href="/push">股票信号页</a>
        <a href="/futures">期货信号页</a>
        <a href="/bridge/" data-local-href="/bridge/" data-remote-href="/bridge/">股票桥接</a>
        <a href="/bridge/futures" data-local-href="/bridge/futures" data-remote-href="/bridge/futures">期货桥接</a>
      </div>
    </div>

    <div class="grid">
      <section class="card span-12">
        <div class="title">使用文档</div>
        <div class="note">这部分用于给客户直接看，说明怎么填写推送地址、怎么测试，以及通知中心的使用方式。</div>
        <div class="doc-grid" style="margin-top: 12px;">
          <div>
            <div class="label">第一步：准备机器人 Webhook</div>
            <ul class="doc-list">
              <li>飞书：在群里创建自定义机器人，复制 Webhook 地址。</li>
              <li>钉钉：在群机器人里创建机器人，复制 Webhook 地址。</li>
              <li>企业微信：在群机器人里生成 Key，复制完整 Webhook 地址。</li>
              <li>把各平台地址分别填到下面对应输入框里，不需要写到账户配置文件里。</li>
            </ul>
          </div>
          <div>
            <div class="label">第二步：保存并测试</div>
            <ul class="doc-list">
              <li>勾选通道是否启用，再勾选它要接收的事件类型。</li>
              <li>点击 `保存推送配置` 后，当前部署环境会保存这组推送参数。</li>
              <li>然后在右侧 `测试发送` 区域选事件、填标题和内容，点击 `发送测试消息`。</li>
              <li>如果通道配置正确，飞书/钉钉/企业微信会立即收到测试文本消息。</li>
            </ul>
          </div>
          <div>
            <div class="label">第三步：接真实事件</div>
            <ul class="doc-list">
              <li>通知中心统一接收接口是：<code class="mono">POST /api/notifications/dispatch</code>。</li>
              <li>支持四类事件：股票信号、期货信号、股票交易信号、期货交易信号。</li>
              <li>外部脚本只要按下面的示例请求体 POST 进来，通知中心会按配置自动路由。</li>
              <li>当前页面和接口都已经就绪。</li>
            </ul>
          </div>
          <div>
            <div class="label">当前状态说明</div>
            <ul class="doc-list">
              <li>现在已经支持“保存配置 + 手动测试 + 统一接收接口路由”。</li>
              <li>你可以先用测试消息确认通道是否可达，再按需要接入股票和期货事件。</li>
              <li>统一接收接口适合后续扩展到更多信号与交易场景。</li>
              <li>如果后面要做自动推送，可把股票信号、期货信号和交易回报统一接入这里。</li>
            </ul>
          </div>
        </div>
      </section>

      <section class="card span-12">
        <div class="title">渠道配置</div>
        <div class="note">在这里填写飞书、钉钉、企业微信的推送地址，并为不同事件配置接收规则。</div>
        <div class="grid" style="margin-top: 16px;">
          <div class="card span-4">
            <div class="switch-row">
              <input id="feishuEnabled" type="checkbox">
              <div class="channel-name">飞书</div>
            </div>
            <div class="note">使用飞书群机器人 Webhook，接收选中的事件类型。</div>
            <div class="field">
              <label for="feishuWebhook">Webhook 地址</label>
              <input id="feishuWebhook" type="url" placeholder="https://open.feishu.cn/open-apis/bot/v2/hook/...">
            </div>
            <div class="field">
              <label>接收事件</label>
              <div class="event-grid" id="feishuEvents"></div>
            </div>
          </div>
          <div class="card span-4">
            <div class="switch-row">
              <input id="dingtalkEnabled" type="checkbox">
              <div class="channel-name">钉钉</div>
            </div>
            <div class="note">使用钉钉群机器人 Webhook，适合盘中提醒和成交通知。</div>
            <div class="field">
              <label for="dingtalkWebhook">Webhook 地址</label>
              <input id="dingtalkWebhook" type="url" placeholder="https://oapi.dingtalk.com/robot/send?access_token=...">
            </div>
            <div class="field">
              <label>接收事件</label>
              <div class="event-grid" id="dingtalkEvents"></div>
            </div>
          </div>
          <div class="card span-4">
            <div class="switch-row">
              <input id="wecomEnabled" type="checkbox">
              <div class="channel-name">企业微信</div>
            </div>
            <div class="note">使用企业微信群机器人 Webhook，适合交易和风控消息沉淀。</div>
            <div class="field">
              <label for="wecomWebhook">Webhook 地址</label>
              <input id="wecomWebhook" type="url" placeholder="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...">
            </div>
            <div class="field">
              <label>接收事件</label>
              <div class="event-grid" id="wecomEvents"></div>
            </div>
          </div>
        </div>
        <div class="toolbar">
          <button id="saveConfigBtn" type="button">保存推送配置</button>
          <button class="secondary" id="reloadConfigBtn" type="button">重新读取</button>
        </div>
        <div class="status-box" id="configStatus">等待读取当前通知配置...</div>
      </section>

      <section class="card span-5">
        <div class="title">测试发送</div>
        <div class="note">先保存配置，再用这里验证通道是否可达。</div>
        <div class="field">
          <label for="testEventType">测试事件类型</label>
          <select id="testEventType"></select>
        </div>
        <div class="field">
          <label for="testChannel">发送范围</label>
          <select id="testChannel">
            <option value="enabled">发送到所有已启用且勾选此事件的通道</option>
            <option value="feishu">仅飞书</option>
            <option value="dingtalk">仅钉钉</option>
            <option value="wecom">仅企业微信</option>
          </select>
        </div>
        <div class="field">
          <label for="testTitle">标题</label>
          <input id="testTitle" type="text" placeholder="例如：股票信号测试">
        </div>
        <div class="field">
          <label for="testMessage">消息内容</label>
          <textarea id="testMessage" placeholder="输入要发送的测试消息，支持多行。"></textarea>
        </div>
        <div class="toolbar">
          <button class="warn" id="sendTestBtn" type="button">发送测试消息</button>
        </div>
        <div class="status-box" id="testStatus">尚未发送测试消息。</div>
      </section>

      <section class="card span-7">
        <div class="title">统一接收接口</div>
        <div class="note">其他页面、策略脚本或交易桥接只需要把事件 POST 到这个接口，通知中心会按你的配置自动路由到飞书、钉钉、企业微信。</div>
        <div class="doc-grid" style="margin-top: 12px;">
          <div>
            <div class="label">可接收事件类型</div>
            <div class="code-block" id="eventDoc"></div>
          </div>
          <div>
            <div class="label">统一接口地址</div>
            <div class="code-block">POST /api/notifications/dispatch</div>
            <div class="label" style="margin-top: 12px;">示例请求体</div>
            <div class="code-block">{
  "event_type": "stock_signal",
  "title": "东方财富 命中新信号",
  "message": "300750.SZ 宁德时代",
  "lines": ["信号时间：09:45:12", "信号价格：188.50"],
  "payload": {"source": "eastmoney", "priority": 2}
}</div>
          </div>
        </div>
      </section>
    </div>
  </div>

  <script>
    const EVENT_META = {
      stock_signal: "股票信号",
      futures_signal: "期货信号",
      stock_trade_signal: "股票交易信号",
      futures_trade_signal: "期货交易信号",
    };
    const CHANNEL_META = {
      feishu: "飞书",
      dingtalk: "钉钉",
      wecom: "企业微信",
    };

    function syncEnvironmentLinks() {
      const host = String(window.location.hostname || "").toLowerCase();
      const isLocalHost = host === "127.0.0.1" || host === "localhost";
      document.querySelectorAll("a[data-local-href][data-remote-href]").forEach((link) => {
        link.href = isLocalHost ? link.dataset.localHref : link.dataset.remoteHref;
      });
    }

    function setText(id, text) {
      const el = document.getElementById(id);
      if (el) el.textContent = text;
    }

    function renderEventCheckboxes(channelId, selected) {
      const holder = document.getElementById(`${channelId}Events`);
      const selectedSet = new Set(selected || []);
      holder.innerHTML = Object.entries(EVENT_META).map(([eventId, label]) => `
        <label class="event-item">
          <input type="checkbox" data-channel="${channelId}" data-event="${eventId}" ${selectedSet.has(eventId) ? "checked" : ""}>
          <span>${label}</span>
        </label>
      `).join("");
    }

    function applyConfig(config) {
      for (const channelId of Object.keys(CHANNEL_META)) {
        const row = (config.channels || {})[channelId] || {};
        document.getElementById(`${channelId}Enabled`).checked = !!row.enabled;
        document.getElementById(`${channelId}Webhook`).value = row.webhook || "";
        renderEventCheckboxes(channelId, row.events || []);
      }
      setText("configStatus", `当前配置已加载。最近更新：${config.updated_at || "-"}`);
    }

    function collectConfig() {
      const channels = {};
      for (const channelId of Object.keys(CHANNEL_META)) {
        const events = Array.from(document.querySelectorAll(`input[data-channel="${channelId}"][data-event]:checked`)).map((item) => item.dataset.event);
        channels[channelId] = {
          enabled: document.getElementById(`${channelId}Enabled`).checked,
          webhook: document.getElementById(`${channelId}Webhook`).value.trim(),
          events,
        };
      }
      return { channels };
    }

    async function readJson(resp) {
      const data = await resp.json();
      if (!resp.ok || !data.ok) {
        throw new Error(data.error || `HTTP ${resp.status}`);
      }
      return data;
    }

    async function loadConfig() {
      setText("configStatus", "正在读取通知配置...");
      const resp = await fetch("/api/notifications/config", { cache: "no-store" });
      const data = await readJson(resp);
      applyConfig(data.data);
    }

    async function saveConfig() {
      setText("configStatus", "正在保存通知配置...");
      const resp = await fetch("/api/notifications/config", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(collectConfig()),
      });
      const data = await readJson(resp);
      applyConfig(data.data);
      setText("configStatus", data.message || "通知配置已保存。");
    }

    async function sendTest() {
      setText("testStatus", "正在发送测试消息...");
      const body = {
        event_type: document.getElementById("testEventType").value,
        channel: document.getElementById("testChannel").value,
        title: document.getElementById("testTitle").value.trim(),
        message: document.getElementById("testMessage").value.trim(),
      };
      const resp = await fetch("/api/notifications/test", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const data = await readJson(resp);
      setText("testStatus", `${data.message || "测试消息已处理。"}\n${JSON.stringify(data.data, null, 2)}`);
    }

    function initEventSelect() {
      const select = document.getElementById("testEventType");
      const eventDoc = document.getElementById("eventDoc");
      const lines = [];
      select.innerHTML = Object.entries(EVENT_META).map(([id, label]) => {
        lines.push(`${id}  =>  ${label}`);
        return `<option value="${id}">${label}</option>`;
      }).join("");
      eventDoc.textContent = lines.join("\\n");
    }

    syncEnvironmentLinks();
    document.getElementById("saveConfigBtn").addEventListener("click", saveConfig);
    document.getElementById("reloadConfigBtn").addEventListener("click", loadConfig);
    document.getElementById("sendTestBtn").addEventListener("click", sendTest);

    initEventSelect();
    loadConfig().catch((error) => {
      setText("configStatus", `读取通知配置失败：${error.message || error}`);
    });
  </script>
</body>
</html>
"""


def read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def read_lines(path: Path, limit: int = 5000) -> list[str]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-limit:]
    except Exception:
        return []


def tail_lines(path: Path, limit: int = 80) -> list[str]:
    return read_lines(path, limit)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def default_notification_config() -> dict[str, Any]:
    return NOTIFICATION_SERVICE.default_config()


def normalize_notification_events(values: Any) -> list[str]:
    return NOTIFICATION_SERVICE.normalize_events(values)


def normalize_notification_config(payload: dict[str, Any]) -> dict[str, Any]:
    return NOTIFICATION_SERVICE.normalize_config(payload)


def load_notification_config() -> dict[str, Any]:
    return NOTIFICATION_SERVICE.load_config()


def save_notification_config(config: dict[str, Any]) -> dict[str, Any]:
    return NOTIFICATION_SERVICE.save_config(config)


def notification_event_label(event_type: str) -> str:
    return NOTIFICATION_SERVICE.event_label(event_type)


def build_notification_text(
    event_type: str,
    title: str,
    message: str,
    lines: list[str] | None = None,
    payload: dict[str, Any] | None = None,
) -> str:
    return NOTIFICATION_SERVICE.build_text(event_type, title, message, lines, payload)


def post_json(url: str, body: dict[str, Any], timeout_seconds: float = 6.0) -> tuple[bool, str]:
    return NOTIFICATION_SERVICE._post_json(url, body, timeout_seconds)


def send_feishu_notification(webhook: str, content: str) -> dict[str, Any]:
    return NOTIFICATION_SERVICE._send_feishu(webhook, content)


def send_dingtalk_notification(webhook: str, content: str) -> dict[str, Any]:
    return NOTIFICATION_SERVICE._send_dingtalk(webhook, content)


def send_wecom_notification(webhook: str, content: str) -> dict[str, Any]:
    return NOTIFICATION_SERVICE._send_wecom(webhook, content)


def dispatch_notification_event(
    *,
    event_type: str,
    title: str,
    message: str,
    lines: list[str] | None = None,
    payload: dict[str, Any] | None = None,
    target_channel: str = "enabled",
) -> dict[str, Any]:
    return NOTIFICATION_SERVICE.dispatch_event(
        event_type=event_type,
        title=title,
        message=message,
        lines=lines,
        payload=payload,
        target_channel=target_channel,
    )


def mask_secret(config: dict[str, Any]) -> dict[str, Any]:
    masked = json.loads(json.dumps(config))
    remote = masked.get("remote", {})
    if remote.get("password"):
        remote["password"] = "***"
    notifications = masked.get("notifications", {})
    if isinstance(notifications, dict):
        dingtalk = notifications.get("dingtalk", {})
        if isinstance(dingtalk, dict) and dingtalk.get("webhook"):
            dingtalk["webhook"] = "***"
    if masked.get("password"):
        masked["password"] = "***"
    return masked


def parse_systemctl_properties(text: str) -> dict[str, str]:
    props: dict[str, str] = {}
    for line in text.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        props[key.strip()] = value.strip()
    return props


def service_status(service_name: str = SERVICE_NAME) -> dict[str, Any]:
    try:
        result = subprocess.run(
            [
                "systemctl",
                "show",
                service_name,
                "-p",
                "ActiveState",
                "-p",
                "SubState",
                "-p",
                "MainPID",
                "-p",
                "ExecMainStatus",
                "-p",
                "MemoryCurrent",
            ],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        )
        props = parse_systemctl_properties(result.stdout)
        raw_memory_current = props.get("MemoryCurrent", "0") or "0"
        memory_current = int(raw_memory_current) if str(raw_memory_current).isdigit() else 0
        return {
            "active_state": props.get("ActiveState", "unknown"),
            "sub_state": props.get("SubState", "unknown"),
            "main_pid": props.get("MainPID", "0"),
            "exec_main_status": props.get("ExecMainStatus", ""),
            "memory_mb": round(memory_current / 1024 / 1024, 1) if memory_current else 0,
        }
    except Exception as exc:
        return {
            "active_state": "error",
            "sub_state": str(exc),
            "main_pid": "0",
            "exec_main_status": "",
            "memory_mb": 0,
        }


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}d")
    if days or hours:
        parts.append(f"{hours}h")
    if days or hours or minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{secs}s")
    return " ".join(parts)


def read_proc_stat() -> tuple[int, int]:
    with open("/proc/stat", "r", encoding="utf-8") as fh:
        first = fh.readline().strip().split()
    values = [int(item) for item in first[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return idle, total


def cpu_usage_percent(sample_seconds: float = 0.15) -> float:
    idle1, total1 = read_proc_stat()
    time.sleep(sample_seconds)
    idle2, total2 = read_proc_stat()
    total_delta = max(1, total2 - total1)
    idle_delta = max(0, idle2 - idle1)
    usage = (1 - (idle_delta / total_delta)) * 100
    return round(max(0.0, min(100.0, usage)), 1)


def meminfo() -> dict[str, int]:
    info: dict[str, int] = {}
    with open("/proc/meminfo", "r", encoding="utf-8") as fh:
        for line in fh:
            if ":" not in line:
                continue
            key, rest = line.split(":", 1)
            value_text = rest.strip().split()[0]
            try:
                info[key] = int(value_text)
            except ValueError:
                continue
    return info


def usage_level(percent: float, warn: float = 75.0, danger: float = 90.0) -> str:
    if percent >= danger:
        return "danger"
    if percent >= warn:
        return "warn"
    return "ok"


def system_metrics() -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    try:
        load1, load5, load15 = os.getloadavg()
    except OSError:
        load1 = load5 = load15 = 0.0

    cpu_percent = cpu_usage_percent()
    load_pct_1m = round((load1 / cpu_count) * 100, 1) if cpu_count else 0.0

    info = meminfo()
    mem_total_kb = int(info.get("MemTotal", 0))
    mem_available_kb = int(info.get("MemAvailable", 0))
    mem_used_kb = max(0, mem_total_kb - mem_available_kb)
    mem_used_pct = round((mem_used_kb / mem_total_kb) * 100, 1) if mem_total_kb else 0.0

    swap_total_kb = int(info.get("SwapTotal", 0))
    swap_free_kb = int(info.get("SwapFree", 0))
    swap_used_kb = max(0, swap_total_kb - swap_free_kb)
    swap_used_pct = round((swap_used_kb / swap_total_kb) * 100, 1) if swap_total_kb else 0.0

    statvfs = os.statvfs("/")
    disk_total = statvfs.f_blocks * statvfs.f_frsize
    disk_free = statvfs.f_bavail * statvfs.f_frsize
    disk_used = max(0, disk_total - disk_free)
    disk_used_pct = round((disk_used / disk_total) * 100, 1) if disk_total else 0.0

    uptime_seconds = 0.0
    try:
        with open("/proc/uptime", "r", encoding="utf-8") as fh:
            uptime_seconds = float(fh.read().split()[0])
    except Exception:
        uptime_seconds = 0.0

    return {
        "cpu_count": cpu_count,
        "cpu_percent": cpu_percent,
        "cpu_level": usage_level(max(cpu_percent, load_pct_1m)),
        "load_1m": round(load1, 2),
        "load_5m": round(load5, 2),
        "load_15m": round(load15, 2),
        "load_pct_1m": load_pct_1m,
        "memory_total_gb": round(mem_total_kb / 1024 / 1024, 2) if mem_total_kb else 0.0,
        "memory_used_gb": round(mem_used_kb / 1024 / 1024, 2) if mem_used_kb else 0.0,
        "memory_available_gb": round(mem_available_kb / 1024 / 1024, 2) if mem_available_kb else 0.0,
        "memory_used_pct": mem_used_pct,
        "memory_level": usage_level(mem_used_pct),
        "swap_total_gb": round(swap_total_kb / 1024 / 1024, 2) if swap_total_kb else 0.0,
        "swap_used_gb": round(swap_used_kb / 1024 / 1024, 2) if swap_used_kb else 0.0,
        "swap_used_pct": swap_used_pct,
        "disk_total_gb": round(disk_total / 1024 / 1024 / 1024, 2) if disk_total else 0.0,
        "disk_used_gb": round(disk_used / 1024 / 1024 / 1024, 2) if disk_used else 0.0,
        "disk_free_gb": round(disk_free / 1024 / 1024 / 1024, 2) if disk_free else 0.0,
        "disk_used_pct": disk_used_pct,
        "disk_level": usage_level(disk_used_pct, warn=80.0, danger=92.0),
        "uptime_seconds": round(uptime_seconds, 1),
        "uptime_text": format_duration(uptime_seconds),
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def public_system_metrics() -> dict[str, Any]:
    return {
        "cpu_count": "-",
        "cpu_percent": None,
        "cpu_level": "ok",
        "load_1m": "-",
        "load_5m": "-",
        "load_15m": "-",
        "load_pct_1m": None,
        "memory_total_gb": "-",
        "memory_used_gb": "-",
        "memory_available_gb": "-",
        "memory_used_pct": None,
        "memory_level": "ok",
        "swap_total_gb": "-",
        "swap_used_gb": "-",
        "swap_used_pct": None,
        "disk_total_gb": "-",
        "disk_used_gb": "-",
        "disk_free_gb": "-",
        "disk_used_pct": None,
        "disk_level": "ok",
        "uptime_text": "展示模式",
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def fetch_snapshot() -> dict[str, Any]:
    try:
        with urlopen(PUSH_SNAPSHOT_URL, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
        return payload.get("data", {})
    except URLError as exc:
        return {"error": f"snapshot unavailable: {exc}"}
    except Exception as exc:
        return {"error": f"snapshot error: {exc}"}


def fetch_url_json(
    url: str,
    timeout: int = 6,
    *,
    headers: dict[str, str] | None = None,
    insecure_ssl: bool = False,
) -> dict[str, Any]:
    merged_headers = {"User-Agent": "Mozilla/5.0"}
    if headers:
        merged_headers.update(headers)
    request = Request(url, headers=merged_headers)
    context = ssl._create_unverified_context() if insecure_ssl and url.lower().startswith("https://") else None
    with urlopen(request, timeout=timeout, context=context) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def basic_auth_headers(username: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {token}"}


def fetch_remote_json_over_ssh(config_path: Path, url: str | None = None) -> dict[str, Any]:
    config = read_json(config_path)
    remote = config.get("remote", {})
    transport = clean_text(remote.get("transport")).lower() or "http"
    target_url = clean_text(url or remote.get("api_url"))
    if not target_url:
        raise RuntimeError("missing remote api_url")

    if transport != "http":
        raise RuntimeError("SSH transport disabled on 159 dashboard; use HTTP remote transport")

    http_username = clean_text(remote.get("basic_auth_username"))
    http_password = str(remote.get("basic_auth_password") or "")
    headers = basic_auth_headers(http_username, http_password) if http_username or http_password else None
    return fetch_url_json(
        target_url,
        headers=headers,
        timeout=float(remote.get("command_timeout_seconds") or 20),
        insecure_ssl=target_url.lower().startswith("https://"),
    )


def fetch_alltick_over_ssh() -> dict[str, Any]:
    config = read_json(BRIDGE_CONFIG_PATH)
    allocation = config.get("allocation", {})
    source_endpoints = allocation.get("source_endpoints", {}) if isinstance(allocation, dict) else {}
    alltick_url = clean_text(source_endpoints.get("alltick")) if isinstance(source_endpoints, dict) else ""
    return fetch_remote_json_over_ssh(BRIDGE_CONFIG_PATH, alltick_url or REMOTE_ALLTICK_URL)


def unwrap_payload(payload: dict[str, Any]) -> dict[str, Any]:
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    return payload


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def clean_price(value: Any) -> str:
    text = clean_text(value)
    if not text:
        return ""
    return text.split()[0]


def signal_stamp(row: dict[str, Any]) -> str:
    trading_day = clean_text(row.get("trading_day"))
    signal_time = clean_text(row.get("signal_time"))
    if signal_time and " " in signal_time:
        return signal_time
    if trading_day and signal_time:
        return f"{trading_day} {signal_time}"
    return signal_time or trading_day


def normalize_signal_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "key": clean_text(row.get("key")),
        "symbol": clean_text(row.get("symbol")),
        "name": clean_text(row.get("name")),
        "trading_day": clean_text(row.get("trading_day")),
        "signal_time": clean_text(row.get("signal_time")),
        "signal_price": clean_price(row.get("signal_price")),
        "r1_time": clean_text(row.get("r1_time")),
        "r1_price": clean_price(row.get("r1_price")),
        "r2_time": clean_text(row.get("r2_time")),
        "r2_price": clean_price(row.get("r2_price")),
        "l1_time": clean_text(row.get("l1_time")),
        "l1_price": clean_price(row.get("l1_price")),
        "l2_time": clean_text(row.get("l2_time")),
        "l2_price": clean_price(row.get("l2_price")),
    }


def normalize_signal_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    rows = [normalize_signal_row(row) for row in (payload.get("signals") or [])]
    rows = [row for row in rows if row["symbol"] and row["signal_time"]]
    rows.sort(key=signal_stamp)
    latest_signal = max(rows, key=signal_stamp) if rows else {}
    snapshot = dict(payload)
    snapshot["signals"] = rows
    snapshot["signal_total_count"] = payload.get("signal_total_count", len(rows))
    snapshot["latest_signal"] = payload.get("latest_signal") or latest_signal
    return snapshot


def normalize_tdx_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for raw_row in payload.get("signals") or []:
        symbol = clean_text(raw_row.get("code") or raw_row.get("symbol"))
        signal_time = clean_text(raw_row.get("buy_time") or raw_row.get("signal_time"))
        trading_day = clean_text(raw_row.get("session_date"))
        if not trading_day and signal_time and " " in signal_time:
            trading_day = signal_time.split(" ", 1)[0]
        rows.append(
            {
                "key": f"{symbol}|{trading_day}|{signal_time}",
                "symbol": symbol,
                "name": clean_text(raw_row.get("name")) or symbol,
                "trading_day": trading_day,
                "signal_time": signal_time,
                "signal_price": clean_price(raw_row.get("buy_price") or raw_row.get("signal_price")),
                "r1_time": clean_text(raw_row.get("right1_time") or raw_row.get("r1_time")),
                "r1_price": clean_price(raw_row.get("right1_price") or raw_row.get("r1_price")),
                "r2_time": clean_text(raw_row.get("right2_time") or raw_row.get("r2_time")),
                "r2_price": clean_price(raw_row.get("right2_price") or raw_row.get("r2_price")),
                "l1_time": clean_text(raw_row.get("left1_time") or raw_row.get("l1_time")),
                "l1_price": clean_price(raw_row.get("left1_price") or raw_row.get("l1_price")),
                "l2_time": clean_text(raw_row.get("left2_time") or raw_row.get("l2_time")),
                "l2_price": clean_price(raw_row.get("left2_price") or raw_row.get("l2_price")),
            }
        )

    rows = [row for row in rows if row["symbol"] and row["signal_time"]]
    rows.sort(key=signal_stamp)
    latest_signal = max(rows, key=signal_stamp) if rows else {}
    return {
        "updated_at": clean_text(payload.get("updated_at")),
        "watchlist_count": payload.get("watchlist_count", 0),
        "signal_total_count": len(rows),
        "match_count": payload.get("match_count", len(rows)),
        "market_status": clean_text(payload.get("market_status") or payload.get("cycle_status")),
        "signals": rows,
        "latest_signal": latest_signal,
    }


def fetch_source_snapshot(spec: dict[str, str]) -> dict[str, Any]:
    try:
        if spec["kind"] == "ssh_signals":
            raw_payload = unwrap_payload(fetch_alltick_over_ssh())
            snapshot = normalize_signal_snapshot(raw_payload)
        elif spec["kind"] == "auth_tdx":
            raw_payload = unwrap_payload(
                fetch_url_json(
                    spec["url"],
                    headers=basic_auth_headers(REMOTE_BASIC_AUTH_USER, REMOTE_BASIC_AUTH_PASSWORD),
                    insecure_ssl=True,
                )
            )
            snapshot = normalize_tdx_snapshot(raw_payload)
        elif spec["kind"] == "auth_signals":
            raw_payload = unwrap_payload(
                fetch_url_json(
                    spec["url"],
                    headers=basic_auth_headers(REMOTE_BASIC_AUTH_USER, REMOTE_BASIC_AUTH_PASSWORD),
                )
            )
            snapshot = normalize_signal_snapshot(raw_payload)
        else:
            raw_payload = unwrap_payload(fetch_url_json(spec["url"]))
            snapshot = normalize_signal_snapshot(raw_payload)
        return {
            "id": spec["id"],
            "label": spec["label"],
            "ok": True,
            "error": "",
            "snapshot": snapshot,
        }
    except URLError as exc:
        return {
            "id": spec["id"],
            "label": spec["label"],
            "ok": False,
            "error": str(exc.reason) if getattr(exc, "reason", None) else str(exc),
            "snapshot": {},
        }
    except Exception as exc:
        return {
            "id": spec["id"],
            "label": spec["label"],
            "ok": False,
            "error": str(exc),
            "snapshot": {},
        }


def fetch_sources() -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=len(SOURCE_SPECS)) as executor:
        future_map = {executor.submit(fetch_source_snapshot, spec): spec for spec in SOURCE_SPECS}
        for future in as_completed(future_map):
            result = future.result()
            results[result["id"]] = result

    for spec in SOURCE_SPECS:
        results.setdefault(
            spec["id"],
            {"id": spec["id"], "label": spec["label"], "ok": False, "error": "missing result", "snapshot": {}},
        )
    return results


def build_overlap_payload(sources: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source_order = {source_id: index for index, source_id in enumerate(SOURCE_ORDER)}
    grouped: dict[str, dict[str, Any]] = {}
    source_status_rows: list[dict[str, Any]] = []

    for spec in SOURCE_SPECS:
        source = sources[spec["id"]]
        snapshot = source.get("snapshot") or {}
        rows = snapshot.get("signals") or []

        source_status_rows.append(
            {
                "id": spec["id"],
                "label": spec["label"],
                "ok": bool(source.get("ok")),
                "error": source.get("error", ""),
                "signal_total_count": snapshot.get("signal_total_count", len(rows)),
                "updated_at": snapshot.get("updated_at", ""),
            }
        )

        latest_by_symbol: dict[str, dict[str, Any]] = {}
        for row in rows:
            symbol = clean_text(row.get("symbol")).upper()
            if not symbol:
                continue
            stamp = signal_stamp(row)
            prev = latest_by_symbol.get(symbol)
            if not prev or stamp > prev["stamp"]:
                latest_by_symbol[symbol] = {"stamp": stamp, "row": row}

        for symbol, value in latest_by_symbol.items():
            row = value["row"]
            item = grouped.setdefault(
                symbol,
                {"symbol": symbol, "name": clean_text(row.get("name")), "latest_stamp": "", "entries": []},
            )
            if not item["name"] and clean_text(row.get("name")):
                item["name"] = clean_text(row.get("name"))
            item["entries"].append(
                {
                    "source_id": spec["id"],
                    "source_label": spec["label"],
                    "signal_stamp": value["stamp"],
                    "signal_price": clean_price(row.get("signal_price")),
                    "l1_time": clean_text(row.get("l1_time")),
                    "l1_price": clean_price(row.get("l1_price")),
                    "l2_time": clean_text(row.get("l2_time")),
                    "l2_price": clean_price(row.get("l2_price")),
                    "r1_time": clean_text(row.get("r1_time")),
                    "r1_price": clean_price(row.get("r1_price")),
                    "r2_time": clean_text(row.get("r2_time")),
                    "r2_price": clean_price(row.get("r2_price")),
                }
            )
            if value["stamp"] > item["latest_stamp"]:
                item["latest_stamp"] = value["stamp"]
                if clean_text(row.get("name")):
                    item["name"] = clean_text(row.get("name"))

    overlap_items: list[dict[str, Any]] = []
    for item in grouped.values():
        if len(item["entries"]) < 2:
            continue
        entries = sorted(item["entries"], key=lambda row: source_order.get(row["source_id"], 99))
        overlap_items.append(
            {
                "symbol": item["symbol"],
                "name": item["name"] or item["symbol"],
                "latest_stamp": item["latest_stamp"],
                "source_count": len(entries),
                "sources": [{"source_id": row["source_id"], "label": row["source_label"]} for row in entries],
                "entries": entries,
            }
        )

    overlap_items.sort(key=lambda item: (-int(item["source_count"]), str(item["latest_stamp"]), str(item["symbol"])))
    loaded_source_count = sum(1 for source in source_status_rows if source["ok"])
    triple_count = sum(1 for item in overlap_items if item["source_count"] >= 3)
    double_count = sum(1 for item in overlap_items if item["source_count"] == 2)

    return {
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "expected_source_count": len(SOURCE_SPECS),
        "loaded_source_count": loaded_source_count,
        "overlap_count": len(overlap_items),
        "double_count": double_count,
        "triple_count": triple_count,
        "sources": source_status_rows,
        "items": overlap_items[:80],
    }


def parse_log_bundle(lines: list[str]) -> dict[str, Any]:
    skip_counter: Counter[str] = Counter()
    order_records: list[dict[str, Any]] = []
    position_map: dict[str, dict[str, Any]] = {}
    account_summary: dict[str, Any] = {}
    xtp_ready: dict[str, Any] = {}
    remote_connected: dict[str, Any] = {}
    last_error = ""

    for line in lines:
        parts = line.split(" | ", 2)
        if len(parts) != 3:
            continue
        timestamp, _level, message = parts

        if message.startswith("[XTP_READY]"):
            xtp_ready = {"timestamp": timestamp, "detail": message}
            continue

        if message.startswith("[REMOTE_CONNECTED]"):
            remote_connected = {
                "timestamp": timestamp,
                "host": clean_text(message[len("[REMOTE_CONNECTED]"):].strip()),
                "detail": message,
            }
            continue

        if "[ERROR]" in message or "鐧诲綍澶辫触" in message or "Traceback" in message:
            last_error = f"{timestamp} | {message}"

        account_match = ACCOUNT_RE.match(message)
        if account_match:
            balance = float(account_match.group("balance"))
            available = float(account_match.group("available"))
            account_summary = {
                "timestamp": timestamp,
                "account": account_match.group("account"),
                "balance": round(balance, 2),
                "available": round(available, 2),
                "frozen": round(balance - available, 2),
            }
            continue

        position_match = POSITION_RE.match(message)
        if position_match:
            volume = float(position_match.group("volume"))
            if volume <= 0:
                continue
            position_map[position_match.group("vt_positionid")] = {
                "vt_positionid": position_match.group("vt_positionid"),
                "volume": int(volume) if volume.is_integer() else volume,
                "yd_volume": int(float(position_match.group("yd"))) if float(position_match.group("yd")).is_integer() else float(position_match.group("yd")),
                "timestamp": timestamp,
            }
            continue

        if message.startswith("[璺宠繃]"):
            rest = message[len("[璺宠繃]"):].strip()
            reason = rest.rsplit(": ", 1)[-1] if ": " in rest else rest
            skip_counter[reason] += 1
            continue

        for prefix, label in ORDER_PREFIXES.items():
            if message.startswith(prefix):
                order_records.append(
                    {
                        "timestamp": timestamp,
                        "kind": label,
                        "detail": message[len(prefix):].strip(),
                    }
                )
                break

    positions = sorted(position_map.values(), key=lambda item: (-float(item["volume"]), item["vt_positionid"]))[:20]
    account_summary["position_count"] = len(position_map)

    return {
        "account": account_summary,
        "positions": positions,
        "recent_order_records": order_records[-20:][::-1],
        "skip_reason_counts": [{"reason": reason, "count": count} for reason, count in skip_counter.most_common(20)],
        "xtp_ready": xtp_ready,
        "remote_connected": remote_connected,
        "last_error": last_error,
    }


def merged_stock_account_summary(log_account: dict[str, Any], state_account: dict[str, Any]) -> dict[str, Any]:
    log_row = dict(log_account) if isinstance(log_account, dict) else {}
    state_row = dict(state_account) if isinstance(state_account, dict) else {}
    candidates: list[tuple[datetime, int, dict[str, Any]]] = []

    for priority, source_name, row in (
        (2, "state.runtime_account", state_row),
        (1, "log.account", log_row),
    ):
        if not row:
            continue
        account = clean_text(row.get("account"))
        balance = row.get("balance")
        available = row.get("available")
        if not account and balance in (None, "") and available in (None, ""):
            continue
        merged = dict(row)
        merged["source"] = source_name
        candidates.append((parse_datetime_text(merged.get("timestamp")) or datetime.min, priority, merged))

    if candidates:
        _, _, chosen = max(candidates, key=lambda item: (item[0], item[1]))
    else:
        chosen = state_row or log_row

    if chosen:
        chosen = dict(chosen)
        if "position_count" not in chosen or chosen.get("position_count") in ("", None):
            if log_row.get("position_count") not in ("", None):
                chosen["position_count"] = log_row.get("position_count")
            elif state_row.get("position_count") not in ("", None):
                chosen["position_count"] = state_row.get("position_count")
        chosen.setdefault("source", "state.runtime_account" if state_row else "log.account")
    return chosen


def inferred_remote_connected(source_status: list[dict[str, Any]]) -> dict[str, Any]:
    for item in source_status:
        if clean_text(item.get("id")) != "alltick":
            continue
        if not bool(item.get("ok")):
            return {}
        updated_at = clean_text(item.get("updated_at"))
        detail = "AllTick 实时探测可用"
        if updated_at:
            detail = f"AllTick 实时探测可用（{updated_at}）"
        return {
            "timestamp": updated_at,
            "host": "127.0.0.1",
            "detail": detail,
            "source": "alltick_snapshot",
        }
    return {}


def next_day_trade_health(
    *,
    config: dict[str, Any],
    service: dict[str, Any],
    account: dict[str, Any],
    runtime: dict[str, Any],
    source_status: list[dict[str, Any]],
) -> dict[str, Any]:
    tomorrow = datetime.now() + timedelta(days=1)
    weekday_label = WEEKDAY_LABELS[tomorrow.weekday()]
    allow_weekdays = config.get("filters", {}).get("allow_weekdays", [0, 1, 2, 3, 4])
    tomorrow_allowed = tomorrow.weekday() in allow_weekdays

    service_ok = (
        service.get("active_state") == "active"
        and service.get("sub_state") == "running"
        and str(service.get("exec_main_status", "")) in {"", "0"}
    )
    order_enabled = bool(config.get("order", {}).get("enabled", False))
    effective_remote = runtime.get("remote_connected", {}) if isinstance(runtime.get("remote_connected"), dict) else {}
    if not effective_remote:
        effective_remote = inferred_remote_connected(source_status)
    xtp_ready = bool(runtime.get("xtp_ready"))
    account_ready = bool(
        clean_text(account.get("account"))
        or account.get("balance") not in ("", None)
        or account.get("available") not in ("", None)
    )
    remote_ok = bool(effective_remote)
    source_ok_count = sum(1 for item in source_status if item.get("ok"))
    all_sources_ok = source_ok_count == len(SOURCE_SPECS)
    last_error = runtime.get("last_error") or ""
    last_error_ts = last_error.split(" | ", 1)[0] if " | " in last_error else ""
    ready_ts = runtime.get("xtp_ready", {}).get("timestamp", "")
    remote_ts = effective_remote.get("timestamp", "")

    # If the bridge has become ready and reconnected after an older error,
    # don't keep surfacing that stale failure as a current blocker.
    if last_error_ts and ready_ts and remote_ts and last_error_ts <= min(ready_ts, remote_ts):
        last_error = ""

    checks = [
        {
            "name": "明日为允许交易日",
            "ok": tomorrow_allowed,
            "detail": f"{tomorrow.strftime('%Y-%m-%d')} {weekday_label} | 仅按周几规则判断",
        },
        {
            "name": "执行服务运行",
            "ok": service_ok,
            "detail": f"{service.get('active_state', '-')}/{service.get('sub_state', '-')}",
        },
        {
            "name": "自动下单开关已开启",
            "ok": order_enabled,
            "detail": "order.enabled=true" if order_enabled else "order.enabled=false",
        },
        {
            "name": "XTP 交易链路已就绪",
            "ok": xtp_ready,
            "detail": runtime.get("xtp_ready", {}).get("timestamp") or "未检测到 [XTP_READY]",
        },
        {
            "name": "账户资金回报已获取",
            "ok": account_ready,
            "detail": account.get("account") or account.get("source") or "未检测到账户回报",
        },
        {
            "name": "117 信号源已连接",
            "ok": remote_ok,
            "detail": effective_remote.get("host") or effective_remote.get("detail") or "未检测到 [REMOTE_CONNECTED]",
        },
        {
            "name": "三路信号源可用",
            "ok": all_sources_ok,
            "detail": f"{source_ok_count}/{len(SOURCE_SPECS)} 路可用",
        },
    ]

    hard_fail = not all(
        [
            service_ok,
            order_enabled,
            xtp_ready,
            account_ready,
            remote_ok,
        ]
    )
    soft_fail = not tomorrow_allowed or not all_sources_ok

    if hard_fail:
        level = "danger"
        status = "不可交易"
        summary = "执行链路存在阻塞项，明天开盘前需要处理。"
    elif soft_fail:
        level = "warn"
        status = "观察中"
        summary = "核心执行链路已通，但仍有非阻塞项需要留意。"
    else:
        level = "ok"
        status = "可交易"
        summary = "明天按当前配置可自动接收 117 信号并执行买卖。"

    failed_checks = [item["name"] for item in checks if not item["ok"]]

    return {
        "date": tomorrow.strftime("%Y-%m-%d"),
        "weekday": weekday_label,
        "status": status,
        "level": level,
        "summary": summary,
        "failed_checks": failed_checks,
        "last_error": last_error,
        "checks": checks,
    }


def config_path(base_dir: Path, raw: str, default_name: str) -> Path:
    text = clean_text(raw)
    if not text:
        return base_dir / default_name
    path = Path(text)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def mask_connect_secret(config: dict[str, Any]) -> dict[str, Any]:
    masked = json.loads(json.dumps(config))
    if masked.get("瀵嗙爜"):
        masked["瀵嗙爜"] = "***"
    return masked


def first_present(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


def normalize_futures_signal_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "continuous_symbol": first_present(row, "连续代码", "杩炵画浠ｇ爜").upper(),
        "exchange": first_present(row, "交易所", "浜ゆ槗鎵€"),
        "product": first_present(row, "品种", "鍝佺"),
        "product_code": first_present(row, "代码", "浠ｇ爜").upper(),
        "signal_time": first_present(row, "C_time", "scan_time"),
        "reference_price": clean_price(first_present(row, "C_close")),
        "a_time": first_present(row, "A_time"),
        "b_time": first_present(row, "B_time"),
    }


def parse_futures_log_bundle(lines: list[str]) -> dict[str, Any]:
    skip_counter: Counter[str] = Counter()
    order_records: list[dict[str, Any]] = []
    account_summary: dict[str, Any] = {}
    ready_summary: dict[str, Any] = {}

    for line in lines:
        parts = line.split(" | ", 2)
        if len(parts) != 3:
            continue
        timestamp, _level, message = parts

        account_match = FUTURES_ACCOUNT_RE.match(message)
        if account_match:
            balance = float(account_match.group("balance"))
            available = float(account_match.group("available"))
            account_summary = {
                "timestamp": timestamp,
                "account": account_match.group("account"),
                "balance": round(balance, 2),
                "available": round(available, 2),
                "frozen": round(balance - available, 2),
            }
            continue

        ready_match = FUTURES_READY_RE.match(message)
        if ready_match:
            ready_summary = {
                "timestamp": timestamp,
                "front": ready_match.group("front"),
                "accounts": int(ready_match.group("accounts")),
                "contracts": int(ready_match.group("contracts")),
            }
            continue

        if message.startswith("[FUTURES_SKIP]"):
            rest = message[len("[FUTURES_SKIP]"):].strip()
            reason = rest.rsplit(": ", 1)[-1] if ": " in rest else rest
            skip_counter[reason] += 1
            continue

        for prefix, label in FUTURES_ORDER_PREFIXES.items():
            if message.startswith(prefix):
                order_records.append(
                    {
                        "timestamp": timestamp,
                        "kind": label,
                        "detail": message[len(prefix):].strip(),
                    }
                )
                break

    return {
        "account": account_summary,
        "ready": ready_summary,
        "recent_order_records": order_records[-30:][::-1],
        "skip_reason_counts": [{"reason": reason, "count": count} for reason, count in skip_counter.most_common(20)],
    }


def number_or_text(value: float) -> int | float:
    rounded = round(float(value), 4)
    if rounded.is_integer():
        return int(rounded)
    return rounded


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def parse_datetime_text(value: Any) -> datetime | None:
    text = clean_text(value)
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def build_futures_trade_pairs(state: dict[str, Any]) -> dict[str, Any]:
    raw_records = state.get("trade_records") or []
    if not isinstance(raw_records, list):
        raw_records = []

    normalized: list[dict[str, Any]] = []
    for row in raw_records:
        if not isinstance(row, dict):
            continue
        tq_symbol = clean_text(row.get("tq_symbol"))
        timestamp = clean_text(row.get("timestamp"))
        direction = clean_text(row.get("direction")).upper()
        offset = clean_text(row.get("offset")).upper()
        volume = safe_float(row.get("volume"))
        price = safe_float(row.get("price"))
        if not tq_symbol or not timestamp or not direction or not offset or volume <= 0 or price <= 0:
            continue
        normalized.append(
            {
                "trade_id": clean_text(row.get("trade_id")),
                "tq_symbol": tq_symbol,
                "direction": direction,
                "offset": offset,
                "volume": volume,
                "price": price,
                "timestamp": timestamp,
                "dt": parse_datetime_text(timestamp),
                "continuous_symbol": clean_text(row.get("continuous_symbol")),
                "product": clean_text(row.get("product")),
                "exchange": clean_text(row.get("exchange")),
                "signal_time": clean_text(row.get("signal_time")),
                "pattern_time": clean_text(row.get("pattern_time")),
                "scan_time": clean_text(row.get("scan_time")),
                "submitted_at": clean_text(row.get("submitted_at")),
                "reference_price": safe_float(row.get("reference_price")),
                "request_price": safe_float(row.get("request_price")),
                "per_lot_margin": safe_float(row.get("per_lot_margin")),
                "estimated_margin": safe_float(row.get("estimated_margin")),
                "multiplier": max(safe_float(row.get("multiplier"), 1.0), 1.0),
                "realized_pnl": safe_float(row.get("realized_pnl")),
            }
        )

    normalized.sort(
        key=lambda item: (
            item["dt"] or datetime.min,
            item["timestamp"],
            item["trade_id"],
        )
    )

    symbol_defaults: dict[str, dict[str, Any]] = {}
    for row in (state.get("strategy_positions", {}) or {}).values():
        if not isinstance(row, dict):
            continue
        tq_symbol = clean_text(row.get("tq_symbol") or row.get("vt_symbol"))
        if not tq_symbol:
            continue
        defaults = symbol_defaults.setdefault(
            tq_symbol,
            {
                "multiplier": 1.0,
                "per_lot_margin": 0.0,
                "continuous_symbol": "",
                "product": "",
                "exchange": "",
            },
        )
        if safe_float(row.get("multiplier")) > 1.0:
            defaults["multiplier"] = max(safe_float(defaults.get("multiplier"), 1.0), safe_float(row.get("multiplier"), 1.0))
        if safe_float(row.get("estimated_margin")) > 0 and safe_float(row.get("volume")) > 0:
            defaults["per_lot_margin"] = max(
                safe_float(defaults.get("per_lot_margin")),
                safe_float(row.get("estimated_margin")) / safe_float(row.get("volume")),
            )
        if clean_text(row.get("continuous_symbol")):
            defaults["continuous_symbol"] = clean_text(row.get("continuous_symbol"))
        if clean_text(row.get("product")):
            defaults["product"] = clean_text(row.get("product"))
        if clean_text(row.get("exchange")):
            defaults["exchange"] = clean_text(row.get("exchange"))

    for trade in normalized:
        tq_symbol = trade["tq_symbol"]
        defaults = symbol_defaults.setdefault(
            tq_symbol,
            {
                "multiplier": 1.0,
                "per_lot_margin": 0.0,
                "continuous_symbol": "",
                "product": "",
                "exchange": "",
            },
        )
        if safe_float(trade.get("multiplier")) > 1.0:
            defaults["multiplier"] = max(safe_float(defaults.get("multiplier"), 1.0), safe_float(trade.get("multiplier"), 1.0))
        if safe_float(trade.get("per_lot_margin")) > 0:
            defaults["per_lot_margin"] = safe_float(trade.get("per_lot_margin"))
        if trade.get("continuous_symbol"):
            defaults["continuous_symbol"] = trade["continuous_symbol"]
        if trade.get("product"):
            defaults["product"] = trade["product"]
        if trade.get("exchange"):
            defaults["exchange"] = trade["exchange"]

    open_lots: dict[str, list[dict[str, Any]]] = {}
    paired_rows: list[dict[str, Any]] = []
    unmatched_close_rows: list[dict[str, Any]] = []

    for trade in normalized:
        tq_symbol = trade["tq_symbol"]
        symbol_meta = symbol_defaults.get(tq_symbol, {})
        queue = open_lots.setdefault(tq_symbol, [])
        if trade["direction"] == "BUY" and trade["offset"] == "OPEN":
            queue.append(
                {
                    **trade,
                    "remaining_volume": trade["volume"],
                    "multiplier": max(safe_float(trade.get("multiplier"), 1.0), safe_float(symbol_meta.get("multiplier"), 1.0), 1.0),
                    "per_lot_margin": safe_float(trade.get("per_lot_margin")) or safe_float(symbol_meta.get("per_lot_margin")),
                    "continuous_symbol": trade.get("continuous_symbol") or clean_text(symbol_meta.get("continuous_symbol")),
                    "product": trade.get("product") or clean_text(symbol_meta.get("product")),
                    "exchange": trade.get("exchange") or clean_text(symbol_meta.get("exchange")),
                }
            )
            continue
        if trade["direction"] != "SELL" or trade["offset"] != "CLOSE":
            continue

        remaining = trade["volume"]
        while remaining > 1e-9 and queue:
            open_trade = queue[0]
            open_meta = symbol_defaults.get(tq_symbol, {})
            matched_volume = min(remaining, safe_float(open_trade.get("remaining_volume")))
            multiplier = max(
                safe_float(open_trade.get("multiplier"), 1.0),
                safe_float(open_meta.get("multiplier"), 1.0),
                safe_float(trade.get("multiplier"), 1.0),
                1.0,
            )
            open_dt = open_trade.get("dt")
            close_dt = trade.get("dt")
            hold_seconds: int | None = None
            if open_dt and close_dt:
                hold_seconds = max(int((close_dt - open_dt).total_seconds()), 0)

            per_lot_margin = safe_float(open_trade.get("per_lot_margin"))
            if per_lot_margin <= 0 and safe_float(open_trade.get("estimated_margin")) > 0 and safe_float(open_trade.get("volume")) > 0:
                per_lot_margin = safe_float(open_trade.get("estimated_margin")) / safe_float(open_trade.get("volume"))
            if per_lot_margin <= 0:
                per_lot_margin = safe_float(open_meta.get("per_lot_margin"))

            paired_rows.append(
                {
                    "open_trade_id": open_trade.get("trade_id", ""),
                    "close_trade_id": trade.get("trade_id", ""),
                    "tq_symbol": tq_symbol,
                    "continuous_symbol": open_trade.get("continuous_symbol") or trade.get("continuous_symbol") or "",
                    "product": open_trade.get("product") or trade.get("product") or "",
                    "exchange": open_trade.get("exchange") or trade.get("exchange") or "",
                    "volume": number_or_text(matched_volume),
                    "open_price": round(safe_float(open_trade.get("price")), 2),
                    "close_price": round(safe_float(trade.get("price")), 2),
                    "reference_price": round(safe_float(open_trade.get("reference_price")), 2)
                    if safe_float(open_trade.get("reference_price")) > 0
                    else "",
                    "request_price": round(safe_float(open_trade.get("request_price")), 2)
                    if safe_float(open_trade.get("request_price")) > 0
                    else "",
                    "margin_estimate": round(per_lot_margin * matched_volume, 2) if per_lot_margin > 0 else "",
                    "pattern_time": open_trade.get("pattern_time", ""),
                    "signal_time": open_trade.get("signal_time", ""),
                    "scan_time": open_trade.get("scan_time", ""),
                    "submitted_at": open_trade.get("submitted_at", ""),
                    "open_timestamp": open_trade.get("timestamp", ""),
                    "close_timestamp": trade.get("timestamp", ""),
                    "hold_seconds": hold_seconds,
                    "hold_text": format_duration(hold_seconds) if hold_seconds is not None else "-",
                    "pair_method": "FIFO",
                    "realized_pnl": round(
                        (safe_float(trade.get("price")) - safe_float(open_trade.get("price"))) * matched_volume * multiplier,
                        2,
                    ),
                    "status": "已平仓",
                }
            )

            open_trade["remaining_volume"] = safe_float(open_trade.get("remaining_volume")) - matched_volume
            remaining -= matched_volume
            if safe_float(open_trade.get("remaining_volume")) <= 1e-9:
                queue.pop(0)

        if remaining > 1e-9:
            unmatched_close_rows.append(
                {
                    "trade_id": trade.get("trade_id", ""),
                    "tq_symbol": tq_symbol,
                    "timestamp": trade.get("timestamp", ""),
                    "volume": number_or_text(remaining),
                    "price": round(safe_float(trade.get("price")), 2),
                }
            )

    open_rows: list[dict[str, Any]] = []
    now_dt = datetime.now()
    for queue in open_lots.values():
        for open_trade in queue:
            remaining_volume = safe_float(open_trade.get("remaining_volume"))
            if remaining_volume <= 1e-9:
                continue
            open_meta = symbol_defaults.get(clean_text(open_trade.get("tq_symbol")), {})
            open_dt = open_trade.get("dt")
            hold_seconds = max(int((now_dt - open_dt).total_seconds()), 0) if open_dt else None
            per_lot_margin = safe_float(open_trade.get("per_lot_margin"))
            if per_lot_margin <= 0 and safe_float(open_trade.get("estimated_margin")) > 0 and safe_float(open_trade.get("volume")) > 0:
                per_lot_margin = safe_float(open_trade.get("estimated_margin")) / safe_float(open_trade.get("volume"))
            if per_lot_margin <= 0:
                per_lot_margin = safe_float(open_meta.get("per_lot_margin"))
            open_rows.append(
                {
                    "open_trade_id": open_trade.get("trade_id", ""),
                    "close_trade_id": "",
                    "tq_symbol": open_trade.get("tq_symbol", ""),
                    "continuous_symbol": open_trade.get("continuous_symbol", "") or clean_text(open_meta.get("continuous_symbol")),
                    "product": open_trade.get("product", "") or clean_text(open_meta.get("product")),
                    "exchange": open_trade.get("exchange", "") or clean_text(open_meta.get("exchange")),
                    "volume": number_or_text(remaining_volume),
                    "open_price": round(safe_float(open_trade.get("price")), 2),
                    "close_price": "",
                    "reference_price": round(safe_float(open_trade.get("reference_price")), 2)
                    if safe_float(open_trade.get("reference_price")) > 0
                    else "",
                    "request_price": round(safe_float(open_trade.get("request_price")), 2)
                    if safe_float(open_trade.get("request_price")) > 0
                    else "",
                    "margin_estimate": round(per_lot_margin * remaining_volume, 2) if per_lot_margin > 0 else "",
                    "pattern_time": open_trade.get("pattern_time", ""),
                    "signal_time": open_trade.get("signal_time", ""),
                    "scan_time": open_trade.get("scan_time", ""),
                    "submitted_at": open_trade.get("submitted_at", ""),
                    "open_timestamp": open_trade.get("timestamp", ""),
                    "close_timestamp": "",
                    "hold_seconds": hold_seconds,
                    "hold_text": format_duration(hold_seconds) if hold_seconds is not None else "-",
                    "pair_method": "FIFO",
                    "realized_pnl": "",
                    "status": "持仓中",
                }
            )

    paired_rows.sort(
        key=lambda item: (
            parse_datetime_text(item.get("open_timestamp")) or datetime.min,
            parse_datetime_text(item.get("close_timestamp")) or datetime.min,
            clean_text(item.get("open_trade_id")),
            clean_text(item.get("close_trade_id")),
        ),
        reverse=True,
    )
    open_rows.sort(
        key=lambda item: (
            parse_datetime_text(item.get("open_timestamp")) or datetime.min,
            clean_text(item.get("open_trade_id")),
        ),
        reverse=True,
    )

    today_text = datetime.now().strftime("%Y-%m-%d")
    realized_total = 0.0
    realized_today = 0.0
    winning_count = 0
    losing_count = 0
    breakeven_count = 0
    last_close_at = ""
    for row in paired_rows:
        pnl = safe_float(row.get("realized_pnl"), 0.0)
        realized_total += pnl
        close_timestamp = clean_text(row.get("close_timestamp"))
        if close_timestamp.startswith(today_text):
            realized_today += pnl
        if pnl > 1e-9:
            winning_count += 1
        elif pnl < -1e-9:
            losing_count += 1
        else:
            breakeven_count += 1
        if close_timestamp and close_timestamp > last_close_at:
            last_close_at = close_timestamp

    if realized_today < -1e-9:
        profit_level = "danger"
    elif realized_today > 1e-9:
        profit_level = "ok"
    elif realized_total < -1e-9:
        profit_level = "danger"
    elif realized_total > 1e-9:
        profit_level = "ok"
    else:
        profit_level = "warn"

    return {
        "rows": paired_rows,
        "summary": {
            "closed_count": len(paired_rows),
            "open_count": len(open_rows),
            "history_open_count": len(open_rows),
            "unmatched_close_count": len(unmatched_close_rows),
            "pair_method": "FIFO",
            "note": "按真实成交顺序配对，避免把混仓均价盈亏挂到单笔开仓。",
        },
        "profit_summary": {
            "realized_total": round(realized_total, 2),
            "realized_today": round(realized_today, 2),
            "closed_count": len(paired_rows),
            "winning_count": winning_count,
            "losing_count": losing_count,
            "breakeven_count": breakeven_count,
            "last_close_at": last_close_at,
            "level": profit_level,
        },
        "history_open_rows": open_rows,
        "unmatched_close_rows": unmatched_close_rows[::-1],
    }


def build_futures_payload() -> dict[str, Any]:
    if PUBLIC_GITHUB_MODE:
        return build_public_futures_payload()
    config = read_json(FUTURES_BRIDGE_CONFIG_PATH)
    connect_config = read_json(FUTURES_CONNECT_PATH)

    state_path = config_path(BASE_DIR, str(config.get("state", {}).get("file") or ""), "state/push_ctp_bridge_state.json")
    log_path = config_path(BASE_DIR, str(config.get("logging", {}).get("file") or ""), "log/push_ctp_bridge.log")
    state = read_json(state_path)
    log_lines = read_lines(log_path, 5000)
    parsed_logs = parse_futures_log_bundle(log_lines)
    trade_pairs = build_futures_trade_pairs(state)

    remote_snapshot: dict[str, Any]
    try:
        remote_snapshot = unwrap_payload(fetch_remote_json_over_ssh(FUTURES_BRIDGE_CONFIG_PATH))
    except Exception as exc:
        remote_snapshot = {"error": str(exc)}

    recent_signals = [normalize_futures_signal_row(row) for row in (remote_snapshot.get("events") or [])]
    recent_signals = [row for row in recent_signals if row["continuous_symbol"] and row["signal_time"]]
    recent_signals.sort(key=lambda row: (row["signal_time"], row["continuous_symbol"]), reverse=True)

    strategy_positions = []
    for row in (state.get("strategy_positions", {}) or {}).values():
        if not isinstance(row, dict):
            continue
        strategy_positions.append(
            {
                "vt_symbol": clean_text(row.get("vt_symbol")),
                "volume": row.get("volume", 0),
                "entry_price": row.get("entry_price", 0),
                "entry_time": clean_text(row.get("entry_time")),
                "planned_exit_at": clean_text(row.get("planned_exit_at")),
                "pending_exit_orderid": clean_text(row.get("pending_exit_orderid")),
                "pending_exit_reason": clean_text(row.get("pending_exit_reason")),
                "continuous_symbol": clean_text(row.get("continuous_symbol")),
                "broker_volume": row.get("broker_volume", 0),
                "closable_volume": row.get("closable_volume", 0),
            }
        )
    strategy_positions.sort(key=lambda row: (row["entry_time"], row["vt_symbol"]), reverse=True)

    connect_summary = {
        "username": clean_text(connect_config.get("username")),
        "broker_id": "TqKq",
        "trade_front": "tqsdk trade websocket",
        "market_front": "tqsdk market websocket",
        "app_id": clean_text(connect_config.get("account_mode") or "TqKq"),
        "environment": "TqKq 模拟",
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "server": system_metrics(),
        "service": service_status(FUTURES_SERVICE_NAME),
        "bridge": {
            "order_enabled": bool(config.get("order", {}).get("enabled", False)),
            "poll_interval_seconds": config.get("remote", {}).get("poll_interval_seconds"),
            "daily_order_limit": config.get("order", {}).get("daily_order_limit"),
            "max_positions": config.get("order", {}).get("max_positions"),
            "max_signals_per_symbol_per_day": config.get("order", {}).get("max_signals_per_symbol_per_day"),
            "fixed_volume": config.get("order", {}).get("fixed_volume"),
            "max_hold_seconds": config.get("exit", {}).get("max_hold_seconds"),
            "stop_loss_pct": config.get("exit", {}).get("stop_loss_pct"),
            "take_profit_pct": config.get("exit", {}).get("take_profit_pct"),
            "allowed_symbols": config.get("filters", {}).get("allowed_continuous_symbols", []),
            "active_front_label": clean_text(state.get("active_front_label")) or clean_text(parsed_logs.get("ready", {}).get("front")),
            "last_ready_at": clean_text(state.get("last_ready_at")) or clean_text(parsed_logs.get("ready", {}).get("timestamp")),
            "ready": bool(clean_text(state.get("last_ready_at")) or parsed_logs.get("ready")),
        },
        "account": parsed_logs.get("account", {}),
        "positions": strategy_positions,
        "paired_trade_records": trade_pairs.get("rows", []),
        "history_open_records": trade_pairs.get("history_open_rows", []),
        "pairing_summary": trade_pairs.get("summary", {}),
        "profit_summary": trade_pairs.get("profit_summary", {}),
        "unmatched_close_records": trade_pairs.get("unmatched_close_rows", []),
        "recent_order_records": parsed_logs.get("recent_order_records", []),
        "skip_reason_counts": parsed_logs.get("skip_reason_counts", []),
        "logs": tail_lines(log_path, 100),
        "config": mask_secret(config),
        "connect": connect_summary,
        "connect_masked": mask_connect_secret(connect_config),
        "remote_snapshot": {
            "error": clean_text(remote_snapshot.get("error")),
            "summary": remote_snapshot.get("summary", {}) if isinstance(remote_snapshot.get("summary"), dict) else {},
        },
        "recent_signals": recent_signals,
        "state": {
            "bootstrap_complete": bool(state.get("bootstrap_complete", False)),
            "last_error": clean_text(state.get("last_error")),
            "state_file": str(state_path),
            "log_file": str(log_path),
        },
    }


def signal_timeline(snapshot: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    signals = snapshot.get("signals") or []
    if not signals:
        return "-", []

    latest_signal = snapshot.get("latest_signal") or {}
    timeline_day = str(latest_signal.get("trading_day") or signals[-1].get("trading_day") or "-")
    timeline_rows = [item for item in signals if str(item.get("trading_day")) == timeline_day]
    timeline_rows.sort(key=signal_stamp)
    return timeline_day, timeline_rows


def bridge_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    config = read_json(BRIDGE_CONFIG_PATH)
    state_path = Path(config.get("state", {}).get("file", str(VNTRADER_DIR / "push_xtp_bridge_state.json")))
    log_path = Path(config.get("logging", {}).get("file", str(VNTRADER_DIR / "log" / "push_xtp_bridge.log")))
    state = read_json(state_path)
    log_lines = read_lines(log_path, 5000)
    parsed_logs = parse_log_bundle(log_lines)
    account_summary = merged_stock_account_summary(
        parsed_logs.get("account", {}),
        state.get("runtime_account", {}) if isinstance(state.get("runtime_account"), dict) else {},
    )

    filter_flags: list[str] = []
    filters = config.get("filters", {})
    if filters.get("skip_st"):
        filter_flags.append("skip_ST")
    if filters.get("skip_star"):
        filter_flags.append("skip_STAR")
    if filters.get("skip_bj"):
        filter_flags.append("skip_BJ")

    timeline_day, timeline_rows = signal_timeline(snapshot)
    ordered_today = state.get("ordered_symbols_by_day", {}).get(timeline_day, {})

    summary = {
        "order_enabled": bool(config.get("order", {}).get("enabled", False)),
        "client_id": config.get("xtp", {}).get("client_id_override"),
        "fixed_volume": config.get("order", {}).get("fixed_volume"),
        "daily_limit": config.get("order", {}).get("daily_order_limit"),
        "snapshot_url": config.get("remote", {}).get("snapshot_url"),
        "poll_interval_seconds": config.get("remote", {}).get("poll_interval_seconds"),
        "log_level": config.get("logging", {}).get("level"),
        "log_file": str(log_path),
        "state_file": str(state_path),
        "trading_sessions": config.get("filters", {}).get("trading_sessions", []),
        "filter_flags": filter_flags,
        "processed_count": len(state.get("processed_keys", [])),
        "bootstrap_complete": bool(state.get("bootstrap_complete", False)),
        "today_order_total": sum(int(v) for v in ordered_today.values()),
    }

    return {
        "config_masked": mask_secret(config),
        "config_raw": config,
        "summary": summary,
        "logs": tail_lines(log_path, 80),
        "account": account_summary,
        "positions": parsed_logs["positions"],
        "recent_order_records": parsed_logs["recent_order_records"],
        "skip_reason_counts": parsed_logs["skip_reason_counts"],
        "timeline_day": timeline_day,
        "signal_timeline": timeline_rows,
        "runtime_health": {
            "xtp_ready": parsed_logs.get("xtp_ready", {}),
            "remote_connected": parsed_logs.get("remote_connected", {}),
            "last_error": parsed_logs.get("last_error", ""),
        },
    }


def build_public_overlap_payload(now_text: str) -> dict[str, Any]:
    sources = []
    for spec in SOURCE_SPECS:
        sources.append(
            {
                "source_id": spec["id"],
                "label": spec["label"],
                "ok": True,
                "signal_total_count": 0,
                "error": "",
                "updated_at": now_text,
            }
        )
    return {
        "overlap_count": 0,
        "triple_count": 0,
        "double_count": 0,
        "loaded_source_count": len(sources),
        "expected_source_count": len(sources),
        "updated_at": now_text,
        "sources": sources,
        "items": [],
    }


def build_public_dashboard_payload() -> dict[str, Any]:
    now = datetime.now()
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    weekday = WEEKDAY_LABELS[now.weekday()]
    overlap_payload = build_public_overlap_payload(now_text)

    return {
        "generated_at": now_text,
        "server": public_system_metrics(),
        "service": service_status(),
        "bridge": {
            "order_enabled": False,
            "client_id": "PUBLIC-DEMO",
            "fixed_volume": "公开占位",
            "daily_limit": "公开占位",
            "snapshot_url": "public-demo",
            "poll_interval_seconds": "-",
            "log_level": "public",
            "log_file": "public-hidden",
            "state_file": "public-hidden",
            "trading_sessions": ["展示模式"],
            "filter_flags": ["AllTick", "通达信", "东方财富"],
            "processed_count": 0,
            "bootstrap_complete": False,
            "today_order_total": 0,
        },
        "snapshot": {
            "signal_total_count": 0,
            "watchlist_count": 0,
            "latest_tick_symbol": "",
            "latest_tick_price": "",
            "latest_signal": {
                "symbol": "-",
                "name": "结构展示位",
                "trading_day": now.strftime("%Y-%m-%d"),
                "signal_time": "-",
                "signal_price": "-",
            },
            "signals": [],
        },
        "overlap": overlap_payload,
        "logs": ["这里展示股票桥接日志区域的位置与布局。"],
        "config": {},
        "account": {
            "account": "展示模式",
            "balance": "-",
            "available": "-",
            "frozen": "-",
            "position_count": 0,
            "timestamp": "-",
        },
        "tomorrow_trade_health": {
            "status": "展示模式",
            "level": "ok",
            "date": now.strftime("%Y-%m-%d"),
            "weekday": weekday,
            "summary": "当前展示的是健康检查区域的位置、卡片结构与说明布局。",
            "checks": [
                {"name": "三源入口", "ok": True, "detail": "AllTick / 通达信 / 东方财富结构保留"},
                {"name": "信号返回", "ok": True, "detail": "股票信号页和期货信号页入口可直接访问"},
                {"name": "交易记录", "ok": True, "detail": "页面保留记录区域与结构说明"},
            ],
            "last_error": "",
        },
        "positions": [],
        "recent_order_records": [],
        "skip_reason_counts": [],
        "timeline_day": now.strftime("%Y-%m-%d"),
        "signal_timeline": [],
    }


def build_public_futures_payload() -> dict[str, Any]:
    now = datetime.now()
    now_text = now.strftime("%Y-%m-%d %H:%M:%S")
    return {
        "generated_at": now_text,
        "server": public_system_metrics(),
        "service": service_status(FUTURES_SERVICE_NAME),
        "bridge": {
            "order_enabled": False,
            "ready": False,
            "active_front_label": "PUBLIC-DEMO",
            "last_ready_at": "-",
            "poll_interval_seconds": "-",
            "update_timeout_seconds": "-",
            "daily_order_limit": "-",
            "max_positions": "-",
            "max_signals_per_symbol_per_day": "-",
            "fixed_volume": "-",
            "max_hold_seconds": "-",
            "stop_loss_pct": "-",
            "take_profit_pct": "-",
            "allowed_symbols": [],
            "resolver_selection": "public-demo",
            "resolver_cache_ttl_seconds": "-",
        },
        "account": {
            "account": "展示模式",
            "balance": "-",
            "available": "-",
        },
        "connect": {
            "username": "PUBLIC-DEMO",
            "broker_id": "DEMO",
            "app_id": "TqKq",
            "environment": "展示模式",
            "trade_front": "public-hidden",
            "market_front": "public-hidden",
        },
        "positions": [],
        "resolved_contracts": [],
        "recent_signals": [],
        "paired_trade_records": [],
        "pairing_summary": {
            "pair_method": "FIFO",
            "closed_count": 0,
            "history_open_count": 0,
            "open_count": 0,
            "unmatched_close_count": 0,
        },
        "profit_summary": {
            "realized_total": 0,
            "realized_today": 0,
            "closed_count": 0,
            "winning_count": 0,
            "level": "ok",
        },
        "history_open_records": [],
        "unmatched_close_records": [],
        "recent_order_records": [],
        "skip_reason_counts": [],
        "logs": ["这里展示期货桥接日志区域的位置与布局。"],
        "remote_snapshot": {
            "error": "",
            "summary": {
                "event_rows": 0,
                "watch_note": "当前页面展示远端信号摘要区域的位置与说明样式",
                "latest_event_summary": "当前页面展示页面结构与卡片布局",
            },
        },
        "state": {
            "bootstrap_complete": False,
            "last_error": "",
            "state_file": "public-hidden",
            "log_file": "public-hidden",
        },
        "config": {
            "order": {
                "sizing_mode": "fixed",
                "strategy_cap_pct": 0,
                "single_symbol_cap_pct": 0,
                "default_margin_rate": 0,
                "max_volume_per_order": 0,
            }
        },
    }


def build_dashboard_payload() -> dict[str, Any]:
    if PUBLIC_GITHUB_MODE:
        return build_public_dashboard_payload()
    sources = fetch_sources()
    eastmoney_snapshot = sources["eastmoney"].get("snapshot") or {}
    overlap_payload = build_overlap_payload(sources)
    bridge = bridge_payload(eastmoney_snapshot)
    if not bridge["runtime_health"].get("remote_connected"):
        bridge["runtime_health"]["remote_connected"] = inferred_remote_connected(overlap_payload.get("sources", []))
    service = service_status()
    tomorrow_health = next_day_trade_health(
        config=bridge["config_raw"],
        service=service,
        account=bridge["account"],
        runtime=bridge["runtime_health"],
        source_status=overlap_payload.get("sources", []),
    )

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "server": system_metrics(),
        "service": service,
        "bridge": bridge["summary"],
        "snapshot": eastmoney_snapshot,
        "overlap": overlap_payload,
        "logs": bridge["logs"],
        "config": bridge["config_masked"],
        "account": bridge["account"],
        "tomorrow_trade_health": tomorrow_health,
        "positions": bridge["positions"],
        "recent_order_records": bridge["recent_order_records"],
        "skip_reason_counts": bridge["skip_reason_counts"],
        "timeline_day": bridge["timeline_day"],
        "signal_timeline": bridge["signal_timeline"],
    }


def build_futures_payload_v2() -> dict[str, Any]:
    if PUBLIC_GITHUB_MODE:
        return build_public_futures_payload()
    config = read_json(FUTURES_BRIDGE_CONFIG_PATH)
    connect_config = read_json(FUTURES_CONNECT_PATH)

    state_path = config_path(BASE_DIR, str(config.get("state", {}).get("file") or ""), "state/push_ctp_bridge_state.json")
    log_path = config_path(BASE_DIR, str(config.get("logging", {}).get("file") or ""), "log/push_ctp_bridge.log")
    state = read_json(state_path)
    log_lines = read_lines(log_path, 5000)
    parsed_logs = parse_futures_log_bundle(log_lines)
    trade_pairs = build_futures_trade_pairs(state)

    try:
        remote_snapshot = unwrap_payload(fetch_remote_json_over_ssh(FUTURES_BRIDGE_CONFIG_PATH))
    except Exception as exc:
        remote_snapshot = {"error": str(exc)}

    recent_signals = []
    for row in (remote_snapshot.get("events") or []):
        if not isinstance(row, dict):
            continue
        recent_signals.append(
            {
                "continuous_symbol": first_present(row, "连续代码", "杩炵画浠ｇ爜").upper(),
                "exchange": first_present(row, "交易所", "浜ゆ槗鎵€"),
                "product": first_present(row, "品种", "鍝佺"),
                "product_code": first_present(row, "代码", "浠ｇ爜").upper(),
                "signal_time": first_present(row, "C_time", "scan_time"),
                "reference_price": clean_price(first_present(row, "C_close")),
                "a_time": first_present(row, "A_time"),
                "b_time": first_present(row, "B_time"),
            }
        )
    recent_signals = [row for row in recent_signals if row["continuous_symbol"] and row["signal_time"]]
    recent_signals.sort(key=lambda row: (row["signal_time"], row["continuous_symbol"]), reverse=True)

    strategy_positions = []
    for row in (state.get("strategy_positions", {}) or {}).values():
        if not isinstance(row, dict):
            continue
        strategy_positions.append(
            {
                "vt_symbol": clean_text(row.get("vt_symbol") or row.get("tq_symbol")),
                "volume": row.get("volume", 0),
                "entry_price": row.get("entry_price", 0),
                "entry_time": clean_text(row.get("entry_time")),
                "planned_exit_at": clean_text(row.get("planned_exit_at")),
                "pending_exit_orderid": clean_text(row.get("pending_exit_orderid")),
                "pending_exit_reason": clean_text(row.get("pending_exit_reason")),
                "continuous_symbol": clean_text(row.get("continuous_symbol")),
                "broker_volume": row.get("broker_volume", 0),
                "closable_volume": row.get("closable_volume", 0),
                "multiplier": row.get("multiplier", 0),
            }
        )
    strategy_positions.sort(key=lambda row: (row["entry_time"], row["vt_symbol"]), reverse=True)

    resolved_contracts = []
    for continuous_symbol, row in (state.get("resolved_contracts", {}) or {}).items():
        if not isinstance(row, dict):
            continue
        resolved_contracts.append(
            {
                "continuous_symbol": continuous_symbol,
                "tq_symbol": clean_text(row.get("tq_symbol")),
                "method": clean_text(row.get("method")),
                "resolved_at": clean_text(row.get("resolved_at")),
                "open_interest": row.get("open_interest", 0),
                "volume": row.get("volume", 0),
            }
        )
    resolved_contracts.sort(key=lambda row: (row["continuous_symbol"], row["resolved_at"]))

    account_summary = state.get("runtime_account", {}) if isinstance(state.get("runtime_account"), dict) else {}
    if not account_summary:
        account_summary = parsed_logs.get("account", {})

    connect_summary = {
        "username": clean_text(connect_config.get("username")),
        "app_id": clean_text(connect_config.get("account_mode") or "TqKq"),
        "environment": "TqKq 模拟",
        "trade_front": "tqsdk trade websocket",
        "market_front": "tqsdk market websocket",
    }

    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "server": system_metrics(),
        "service": service_status(FUTURES_SERVICE_NAME),
        "bridge": {
            "order_enabled": bool(config.get("order", {}).get("enabled", False)),
            "ready": bool(clean_text(state.get("last_ready_at"))),
            "active_front_label": clean_text(state.get("active_front_label")) or clean_text(connect_summary.get("app_id")),
            "last_ready_at": clean_text(state.get("last_ready_at")),
            "poll_interval_seconds": config.get("remote", {}).get("poll_interval_seconds"),
            "update_timeout_seconds": config.get("tqsdk", {}).get("update_timeout_seconds"),
            "daily_order_limit": config.get("order", {}).get("daily_order_limit"),
            "max_positions": config.get("order", {}).get("max_positions"),
            "max_signals_per_symbol_per_day": config.get("order", {}).get("max_signals_per_symbol_per_day"),
            "fixed_volume": config.get("order", {}).get("fixed_volume"),
            "max_hold_seconds": config.get("exit", {}).get("max_hold_seconds"),
            "stop_loss_pct": config.get("exit", {}).get("stop_loss_pct"),
            "take_profit_pct": config.get("exit", {}).get("take_profit_pct"),
            "allowed_symbols": config.get("filters", {}).get("allowed_continuous_symbols", []),
            "resolver_selection": config.get("contract_resolver", {}).get("selection"),
            "resolver_cache_ttl_seconds": config.get("contract_resolver", {}).get("cache_ttl_seconds"),
        },
        "account": account_summary,
        "connect": connect_summary,
        "positions": strategy_positions,
        "resolved_contracts": resolved_contracts,
        "recent_signals": recent_signals,
        "paired_trade_records": trade_pairs.get("rows", []),
        "pairing_summary": trade_pairs.get("summary", {}),
        "profit_summary": trade_pairs.get("profit_summary", {}),
        "history_open_records": trade_pairs.get("history_open_rows", []),
        "unmatched_close_records": trade_pairs.get("unmatched_close_rows", []),
        "recent_order_records": parsed_logs.get("recent_order_records", []),
        "skip_reason_counts": parsed_logs.get("skip_reason_counts", []),
        "logs": tail_lines(log_path, 100),
        "remote_snapshot": {
            "error": clean_text(remote_snapshot.get("error")),
            "summary": remote_snapshot.get("summary", {}) if isinstance(remote_snapshot.get("summary"), dict) else {},
        },
        "state": {
            "bootstrap_complete": bool(state.get("bootstrap_complete", False)),
            "last_error": clean_text(state.get("last_error")),
            "state_file": str(state_path),
            "log_file": str(log_path),
        },
    }


@app.get("/notifications")
def notifications_index():
    return render_template_string(NOTIFICATIONS_HTML_TEMPLATE)


@app.get("/api/notifications/config")
def api_notifications_config_get():
    return jsonify({"ok": True, "data": load_notification_config()})


@app.post("/api/notifications/config")
def api_notifications_config_save():
    payload = request.get_json(silent=True) or {}
    config = save_notification_config(payload if isinstance(payload, dict) else {})
    return jsonify({"ok": True, "message": "通知配置已保存。", "data": config})


@app.post("/api/notifications/test")
def api_notifications_test():
    payload = request.get_json(silent=True) or {}
    event_type = str(payload.get("event_type") or "").strip()
    if event_type not in NOTIFICATION_EVENT_META:
        return jsonify({"ok": False, "error": "不支持的事件类型"}), 400
    channel = str(payload.get("channel") or "enabled").strip()
    result = dispatch_notification_event(
        event_type=event_type,
        title=str(payload.get("title") or f"{notification_event_label(event_type)} 测试").strip(),
        message=str(payload.get("message") or "这是一条来自通知推送中心的测试消息。").strip(),
        lines=[],
        payload={"mode": "test", "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
        target_channel=channel,
    )
    return jsonify(
        {
            "ok": True,
            "message": f"测试消息已处理，成功发送 {result['delivered_count']} 个通道。",
            "data": result,
        }
    )


@app.post("/api/notifications/dispatch")
def api_notifications_dispatch():
    payload = request.get_json(silent=True) or {}
    event_type = str(payload.get("event_type") or "").strip()
    if event_type not in NOTIFICATION_EVENT_META:
        return jsonify({"ok": False, "error": "不支持的事件类型"}), 400
    lines = payload.get("lines")
    if not isinstance(lines, list):
        lines = []
    result = dispatch_notification_event(
        event_type=event_type,
        title=str(payload.get("title") or notification_event_label(event_type)).strip(),
        message=str(payload.get("message") or payload.get("text") or "").strip(),
        lines=[str(line or "").strip() for line in lines if str(line or "").strip()],
        payload=payload.get("payload") if isinstance(payload.get("payload"), dict) else {},
        target_channel=str(payload.get("channel") or "enabled").strip(),
    )
    return jsonify({"ok": True, "message": "通知事件已接收。", "data": result})


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "service": SERVICE_NAME})


@app.get("/api/data")
def api_data():
    return jsonify(build_dashboard_payload())


@app.get("/api/futures")
def api_futures():
    return jsonify(build_futures_payload())


@app.get("/api/futures-v2")
def api_futures_v2():
    return jsonify(build_futures_payload_v2())


@app.get("/")
def index():
    return render_template_string(HTML_TEMPLATE)


@app.get("/futures")
def futures_index():
    return render_template_string(FUTURES_HTML_TEMPLATE)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8792, debug=False)

