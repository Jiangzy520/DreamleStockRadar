from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from vnpy.event import Event, EventEngine
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType, Status
from vnpy.trader.engine import MainEngine
from vnpy.trader.event import EVENT_ACCOUNT, EVENT_CONTRACT, EVENT_LOG, EVENT_ORDER, EVENT_POSITION, EVENT_TICK, EVENT_TRADE
from vnpy.trader.object import AccountData, ContractData, OrderData, OrderRequest, PositionData, SubscribeRequest, TickData, TradeData
from vnpy_xtp import XtpGateway


CHINA_TZ = ZoneInfo("Asia/Shanghai")
XTP_CLIENT_ID_KEY = "客户号"


def now_cn() -> datetime:
    return datetime.now(CHINA_TZ)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def round_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return round(value, 2)
    scaled = round(value / tick)
    rounded = scaled * tick
    precision = max(0, len(str(tick).split(".")[1]) if "." in str(tick) else 0)
    return round(rounded, precision)


def parse_signal_dt(raw: str) -> datetime | None:
    raw = str(raw or "").strip()
    if not raw:
        return None
    formats = ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S")
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=CHINA_TZ)
        except ValueError:
            continue
    return None


def parse_session(raw: str) -> tuple[dt_time, dt_time]:
    start_text, end_text = raw.split("-", 1)
    start = datetime.strptime(start_text.strip(), "%H:%M").time()
    end = datetime.strptime(end_text.strip(), "%H:%M").time()
    return start, end


def parse_clock(raw: str, default: str) -> dt_time:
    text = str(raw or "").strip() or default
    return datetime.strptime(text, "%H:%M").time()


def within_sessions(current: datetime, sessions: list[str]) -> bool:
    current_time = current.time()
    for item in sessions:
        start, end = parse_session(item)
        if start <= current_time <= end:
            return True
    return False


def parse_trading_day(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return None


def infer_exchange(symbol: str) -> Exchange | None:
    symbol = str(symbol or "").strip().upper()
    if symbol.endswith(".SZ") or symbol.endswith(".SZSE"):
        return Exchange.SZSE
    if symbol.endswith(".SH") or symbol.endswith(".SSE"):
        return Exchange.SSE
    if symbol.endswith(".BJ") or symbol.endswith(".BSE"):
        return Exchange.BSE
    return None


def symbol_code(symbol: str) -> str:
    return str(symbol or "").split(".", 1)[0].strip()


def to_vt_symbol(symbol: str) -> str | None:
    symbol = str(symbol or "").strip().upper()
    if not symbol:
        return None
    parts = symbol.split(".", 1)
    if len(parts) == 2 and parts[1] in {Exchange.SSE.value, Exchange.SZSE.value, Exchange.BSE.value}:
        return symbol
    exchange = infer_exchange(symbol)
    if exchange is None:
        return None
    return f"{symbol_code(symbol)}.{exchange.value}"


def format_dt(dt: datetime | None) -> str:
    if dt is None:
        return now_cn().strftime("%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CHINA_TZ)
    else:
        dt = dt.astimezone(CHINA_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, dict, tuple, set)):
        return len(value) == 0
    return False


def path_lookup(data: Any, path: str) -> Any:
    current = data
    for part in [item for item in str(path or "").split(".") if item]:
        if not isinstance(current, dict) or part not in current:
            return None
        current = current.get(part)
    return current


def first_present(data: dict[str, Any], aliases: list[str]) -> Any:
    for alias in aliases:
        value = path_lookup(data, alias)
        if not is_blank(value):
            return value
    return None


def signal_protocol_defaults() -> dict[str, Any]:
    return {
        "version": "v1",
        "require_ok_flag": True,
        "ok_paths": ["ok"],
        "data_paths": ["data"],
        "signals_paths": ["signals"],
        "metadata_paths": {
            "updated_at": ["updated_at"],
            "watchlist_count": ["watchlist_count"],
            "signal_total_count": ["signal_total_count"],
        },
        "field_aliases": {
            "key": ["key"],
            "symbol": ["symbol", "code"],
            "name": ["name"],
            "trading_day": ["trading_day", "session_date"],
            "signal_time": ["signal_time", "buy_time"],
            "signal_price": ["signal_price", "buy_price"],
        },
        "required_fields": ["symbol", "trading_day", "signal_time", "signal_price"],
    }


def normalize_signal_protocol(config: dict[str, Any] | None) -> dict[str, Any]:
    protocol = signal_protocol_defaults()
    raw = dict(config or {})
    protocol["version"] = str(raw.get("version") or protocol["version"])
    protocol["require_ok_flag"] = bool(raw.get("require_ok_flag", protocol["require_ok_flag"]))

    for key in ("ok_paths", "data_paths", "signals_paths", "required_fields"):
        value = raw.get(key)
        if isinstance(value, list) and value:
            protocol[key] = [str(item).strip() for item in value if str(item).strip()]

    raw_metadata = raw.get("metadata_paths")
    if isinstance(raw_metadata, dict):
        merged_metadata: dict[str, list[str]] = {}
        for field, default_aliases in protocol["metadata_paths"].items():
            value = raw_metadata.get(field)
            if isinstance(value, list) and value:
                merged_metadata[field] = [str(item).strip() for item in value if str(item).strip()]
            else:
                merged_metadata[field] = list(default_aliases)
        protocol["metadata_paths"] = merged_metadata

    raw_aliases = raw.get("field_aliases")
    if isinstance(raw_aliases, dict):
        merged_aliases: dict[str, list[str]] = {}
        for field, default_aliases in protocol["field_aliases"].items():
            value = raw_aliases.get(field)
            if isinstance(value, list) and value:
                merged_aliases[field] = [str(item).strip() for item in value if str(item).strip()]
            else:
                merged_aliases[field] = list(default_aliases)
        protocol["field_aliases"] = merged_aliases

    return protocol


def normalize_signal_row(row: dict[str, Any], protocol: dict[str, Any]) -> dict[str, Any] | None:
    aliases = protocol["field_aliases"]
    resolved = {field: first_present(row, alias_list) for field, alias_list in aliases.items()}

    symbol = str(resolved.get("symbol") or "").strip().upper()
    signal_time = str(resolved.get("signal_time") or "").strip()
    trading_day = str(resolved.get("trading_day") or "").strip()
    if not trading_day and signal_time and " " in signal_time:
        trading_day = signal_time.split(" ", 1)[0]

    signal_price_raw = str(resolved.get("signal_price") or "").strip()
    name = str(resolved.get("name") or symbol).strip()
    key = str(resolved.get("key") or "").strip()
    if not key and symbol and trading_day and signal_time:
        key = f"{symbol}|{trading_day}|{signal_time}"

    canonical = {
        "key": key,
        "symbol": symbol,
        "name": name,
        "trading_day": trading_day,
        "signal_time": signal_time,
        "signal_price": signal_price_raw,
    }
    for field in protocol["required_fields"]:
        if is_blank(canonical.get(field)):
            return None

    try:
        float(signal_price_raw)
    except (TypeError, ValueError):
        return None

    if not canonical["key"]:
        return None
    return canonical


def normalize_snapshot_payload(payload: dict[str, Any], protocol: dict[str, Any], logger: logging.Logger) -> dict[str, Any]:
    if protocol.get("require_ok_flag", True):
        ok_value = first_present(payload, protocol["ok_paths"])
        if ok_value is not None and not bool(ok_value):
            raise RuntimeError(f"remote payload not ok: {str(payload)[:200]}")

    data_root: dict[str, Any] = payload
    for path in protocol["data_paths"]:
        candidate = path_lookup(payload, path)
        if isinstance(candidate, dict):
            data_root = candidate
            break

    raw_rows: list[dict[str, Any]] = []
    for path in protocol["signals_paths"]:
        candidate = path_lookup(data_root, path)
        if isinstance(candidate, list):
            raw_rows = [row for row in candidate if isinstance(row, dict)]
            break
        candidate = path_lookup(payload, path)
        if isinstance(candidate, list):
            raw_rows = [row for row in candidate if isinstance(row, dict)]
            break

    normalized_rows: list[dict[str, Any]] = []
    rejected_rows = 0
    for row in raw_rows:
        normalized = normalize_signal_row(row, protocol)
        if normalized:
            normalized_rows.append(normalized)
        else:
            rejected_rows += 1

    if rejected_rows:
        logger.warning("[SIGNAL_PROTOCOL_REJECTED] version=%s rejected=%s accepted=%s", protocol["version"], rejected_rows, len(normalized_rows))

    metadata: dict[str, Any] = {}
    for field, aliases in protocol["metadata_paths"].items():
        value = first_present(data_root, aliases)
        if value is None:
            value = first_present(payload, aliases)
        metadata[field] = value

    signal_total_count = metadata.get("signal_total_count")
    try:
        signal_total_count = int(signal_total_count)
    except (TypeError, ValueError):
        signal_total_count = len(normalized_rows)

    snapshot = {
        "signals": normalized_rows,
        "updated_at": str(metadata.get("updated_at") or ""),
        "watchlist_count": metadata.get("watchlist_count"),
        "signal_total_count": signal_total_count,
        "protocol_version": protocol["version"],
    }
    return snapshot


@dataclass
class Signal:
    key: str
    symbol: str
    name: str
    trading_day: str
    signal_time: str
    signal_price: float
    raw: dict[str, Any]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Signal | None":
        key = str(data.get("key") or "").strip()
        symbol = str(data.get("symbol") or "").strip().upper()
        name = str(data.get("name") or "").strip()
        trading_day = str(data.get("trading_day") or "").strip()
        signal_time = str(data.get("signal_time") or "").strip()
        signal_price_raw = str(data.get("signal_price") or "").strip()
        if not (key and symbol and signal_time and signal_price_raw):
            return None
        try:
            signal_price = float(signal_price_raw)
        except ValueError:
            return None
        return cls(
            key=key,
            symbol=symbol,
            name=name,
            trading_day=trading_day,
            signal_time=signal_time,
            signal_price=signal_price,
            raw=dict(data),
        )


@dataclass
class ManagedOrder:
    vt_orderid: str
    symbol: str
    side: str
    attempt: int
    requested_volume: int
    requested_price: float
    created_at: float
    context: dict[str, Any]
    cancel_requested: bool = False


class BridgeState:
    def __init__(self, path: Path, max_processed: int, max_trade_records: int = 4000) -> None:
        self.path = path
        self.max_processed = max_processed
        self.max_trade_records = max_trade_records
        self.processed_keys: deque[str] = deque(maxlen=max_processed)
        self.ordered_symbols_by_day: dict[str, dict[str, int]] = {}
        self.strategy_positions: dict[str, dict[str, Any]] = {}
        self.trade_records: deque[dict[str, Any]] = deque(maxlen=max_trade_records)
        self.runtime_account: dict[str, Any] = {}
        self.close_summary_sent: dict[str, str] = {}
        self.bootstrap_complete: bool = False
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        for key in payload.get("processed_keys", []):
            if key:
                self.processed_keys.append(str(key))
        ordered = payload.get("ordered_symbols_by_day", {}) or {}
        if isinstance(ordered, dict):
            self.ordered_symbols_by_day = {
                str(day): {str(symbol): int(count) for symbol, count in (values or {}).items()}
                for day, values in ordered.items()
                if isinstance(values, dict)
            }
        positions = payload.get("strategy_positions", {}) or {}
        if isinstance(positions, dict):
            self.strategy_positions = {
                str(symbol): dict(values)
                for symbol, values in positions.items()
                if isinstance(values, dict)
            }
        for record in payload.get("trade_records", []) or []:
            if isinstance(record, dict):
                self.trade_records.append(dict(record))
        runtime_account = payload.get("runtime_account", {}) or {}
        if isinstance(runtime_account, dict):
            self.runtime_account = dict(runtime_account)
        close_summary_sent = payload.get("close_summary_sent", {}) or {}
        if isinstance(close_summary_sent, dict):
            self.close_summary_sent = {
                str(day): str(sent_at)
                for day, sent_at in close_summary_sent.items()
                if day and sent_at
            }
        self.bootstrap_complete = bool(payload.get("bootstrap_complete", False))

    def save(self) -> None:
        ensure_parent(self.path)
        payload = {
            "processed_keys": list(self.processed_keys),
            "ordered_symbols_by_day": self.ordered_symbols_by_day,
            "strategy_positions": self.strategy_positions,
            "trade_records": list(self.trade_records),
            "runtime_account": self.runtime_account,
            "close_summary_sent": self.close_summary_sent,
            "bootstrap_complete": self.bootstrap_complete,
            "updated_at": now_cn().strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def has_processed(self, key: str) -> bool:
        return key in self.processed_keys

    def mark_processed(self, key: str) -> None:
        if key in self.processed_keys:
            return
        self.processed_keys.append(key)

    def order_count(self, trading_day: str, symbol: str) -> int:
        return int(self.ordered_symbols_by_day.get(trading_day, {}).get(symbol, 0))

    def mark_order(self, trading_day: str, symbol: str) -> None:
        bucket = self.ordered_symbols_by_day.setdefault(trading_day, {})
        bucket[symbol] = int(bucket.get(symbol, 0)) + 1

    def get_strategy_position(self, symbol: str) -> dict[str, Any] | None:
        position = self.strategy_positions.get(str(symbol or "").upper())
        if not position:
            return None
        if float(position.get("volume", 0) or 0) <= 0:
            return None
        return dict(position)

    def get_all_strategy_positions(self) -> list[dict[str, Any]]:
        positions: list[dict[str, Any]] = []
        for position in self.strategy_positions.values():
            if float(position.get("volume", 0) or 0) > 0:
                positions.append(dict(position))
        positions.sort(key=lambda item: (str(item.get("entry_time") or ""), str(item.get("symbol") or "")))
        return positions

    def upsert_strategy_buy(
        self,
        symbol: str,
        name: str,
        volume: float,
        price: float,
        signal_key: str,
        trading_day: str,
        traded_at: str,
        priority: int = 1,
    ) -> None:
        symbol = str(symbol or "").upper()
        current = dict(self.strategy_positions.get(symbol, {}))
        old_volume = float(current.get("volume", 0) or 0)
        new_volume = old_volume + float(volume)
        if new_volume <= 0:
            return
        old_avg = float(current.get("avg_price", 0) or 0)
        if old_volume > 0 and old_avg > 0:
            avg_price = ((old_volume * old_avg) + (float(volume) * float(price))) / new_volume
        else:
            avg_price = float(price)
        entry_time = current.get("entry_time") or traded_at
        current_price = float(current.get("current_price", 0) or 0)
        if current_price <= 0:
            current_price = float(price)
        market_value = round(current_price * new_volume, 2)
        floating_pnl = round((current_price - avg_price) * new_volume, 2)
        floating_pnl_pct = round(((current_price - avg_price) / avg_price), 6) if avg_price > 0 else 0.0
        highest_price = max(float(current.get("highest_price", 0) or 0), float(price), avg_price, current_price)
        self.strategy_positions[symbol] = {
            "symbol": symbol,
            "name": name or current.get("name") or symbol,
            "volume": int(new_volume) if float(new_volume).is_integer() else new_volume,
            "avg_price": round(avg_price, 6),
            "entry_time": entry_time,
            "last_buy_time": traded_at,
            "last_update": traded_at,
            "entry_signal_key": signal_key or current.get("entry_signal_key", ""),
            "trading_day": trading_day or current.get("trading_day", ""),
            "priority": int(current.get("priority") or priority or 1),
            "current_price": round(current_price, 6),
            "market_value": market_value,
            "floating_pnl": floating_pnl,
            "floating_pnl_pct": floating_pnl_pct,
            "mark_time": current.get("mark_time") or traded_at,
            "highest_price": round(highest_price, 6),
            "highest_price_at": traded_at if highest_price == float(price) or not current.get("highest_price_at") else current.get("highest_price_at"),
            "partial_exit_done": bool(current.get("partial_exit_done", False)),
            "partial_exit_time": current.get("partial_exit_time", ""),
            "partial_exit_price": current.get("partial_exit_price", 0),
            "partial_exit_reason": current.get("partial_exit_reason", ""),
        }

    def apply_strategy_sell(self, symbol: str, volume: float, traded_at: str) -> None:
        symbol = str(symbol or "").upper()
        current = dict(self.strategy_positions.get(symbol, {}))
        if not current:
            return
        old_volume = float(current.get("volume", 0) or 0)
        new_volume = max(0.0, old_volume - float(volume))
        if new_volume <= 0:
            self.strategy_positions.pop(symbol, None)
            return
        current["volume"] = int(new_volume) if float(new_volume).is_integer() else new_volume
        current["last_sell_time"] = traded_at
        current["last_update"] = traded_at
        self.strategy_positions[symbol] = current

    def patch_strategy_position(self, symbol: str, **changes: Any) -> None:
        symbol = str(symbol or "").upper()
        current = dict(self.strategy_positions.get(symbol, {}))
        if not current:
            return
        current.update(changes)
        current["last_update"] = str(changes.get("last_update") or now_cn().strftime("%Y-%m-%d %H:%M:%S"))
        self.strategy_positions[symbol] = current

    def add_trade_record(self, record: dict[str, Any]) -> None:
        if not isinstance(record, dict):
            return
        self.trade_records.append(dict(record))

    def get_trade_records(self, trading_day: str | None = None) -> list[dict[str, Any]]:
        records = [dict(record) for record in self.trade_records]
        if trading_day:
            records = [record for record in records if str(record.get("trading_day") or "") == str(trading_day)]
        records.sort(key=lambda item: (str(item.get("traded_at") or ""), str(item.get("symbol") or "")))
        return records

    def has_close_summary_sent(self, trading_day: str) -> bool:
        return bool(self.close_summary_sent.get(str(trading_day) or ""))

    def mark_close_summary_sent(self, trading_day: str, sent_at: str) -> None:
        if not trading_day:
            return
        self.close_summary_sent[str(trading_day)] = str(sent_at)

class RemoteSignalClient:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.client: Any | None = None
        self.lock = threading.Lock()
        self.protocol = normalize_signal_protocol(config.get("signal_protocol", {}))

    def _transport(self) -> str:
        return str(self.config.get("transport") or "http").strip().lower()

    def _http_headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        username = str(self.config.get("basic_auth_username") or "").strip()
        password = str(self.config.get("basic_auth_password") or "").strip()
        if username or password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        return headers

    def _fetch_snapshot_http(self) -> dict[str, Any]:
        request = Request(
            str(self.config["snapshot_url"]),
            headers=self._http_headers(),
            method="GET",
        )
        with urlopen(request, timeout=float(self.config.get("command_timeout_seconds", 20))) as response:
            payload_text = response.read().decode("utf-8", "ignore")
        payload = json.loads(payload_text)
        return normalize_snapshot_payload(payload, self.protocol, self.logger)

    def close(self) -> None:
        with self.lock:
            if self.client:
                self.client.close()
                self.client = None

    def _connect(self) -> Any:
        raise RuntimeError("SSH transport disabled on 159 stock bridge; use HTTP remote transport")

    def fetch_snapshot(self) -> dict[str, Any]:
        if self._transport() != "http":
            raise RuntimeError("SSH transport disabled on 159 stock bridge; use HTTP remote transport")
        return self._fetch_snapshot_http()


class XtpTrader:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.state: BridgeState | None = None
        self.event_engine = EventEngine()
        self.main_engine = MainEngine(self.event_engine)
        self.main_engine.add_gateway(XtpGateway)
        self.gateway_name = str(self.config.get("gateway_name", "XTP"))
        self.last_account_event_at: float = 0.0
        self.connected_once = False
        self._last_account_log: tuple[float, float] | None = None
        self._position_cache: dict[str, float] = {}
        self.event_engine.register(EVENT_LOG, self._on_log)
        self.event_engine.register(EVENT_ACCOUNT, self._on_account)
        self.event_engine.register(EVENT_CONTRACT, self._on_contract)
        self.event_engine.register(EVENT_ORDER, self._on_order)
        self.event_engine.register(EVENT_TRADE, self._on_trade)
        self.event_engine.register(EVENT_POSITION, self._on_position)
        self.event_engine.register(EVENT_TICK, self._on_tick)

    def _on_log(self, event: Event) -> None:
        data = event.data
        msg = getattr(data, "msg", "")
        gateway_name = getattr(data, "gateway_name", "") or "System"
        self.logger.info("[VNPY:%s] %s", gateway_name, msg)

    def _on_account(self, event: Event) -> None:
        data: AccountData = event.data
        self.last_account_event_at = time.time()
        self.connected_once = True
        snapshot = (round(float(data.balance), 2), round(float(data.available), 2))
        if self.state is not None:
            self.state.runtime_account = {
                "timestamp": format_dt(now_cn()),
                "account": data.vt_accountid,
                "balance": round(float(data.balance), 2),
                "available": round(float(data.available), 2),
                "frozen": round(float(data.balance) - float(data.available), 2),
                "position_count": len(self.main_engine.get_all_positions()),
            }
        if snapshot == self._last_account_log:
            return
        self._last_account_log = snapshot
        self.logger.info("[ACCOUNT] %s balance=%.2f available=%.2f", data.vt_accountid, float(data.balance), float(data.available))

    def _on_contract(self, event: Event) -> None:
        data: ContractData = event.data
        contract_count = len(self.main_engine.get_all_contracts())
        if contract_count in {1, 1000, 3000, 5000}:
            self.logger.info("[CONTRACT] cached=%s sample=%s", contract_count, data.vt_symbol)

    def _on_order(self, event: Event) -> None:
        data: OrderData = event.data
        self.logger.info(
            "[ORDER] %s %s %s %.0f@%.3f status=%s traded=%.0f ref=%s",
            data.vt_orderid,
            data.vt_symbol,
            data.direction.value if data.direction else "",
            float(data.volume),
            float(data.price),
            data.status.value,
            float(data.traded),
            data.reference,
        )

    def _on_trade(self, event: Event) -> None:
        data: TradeData = event.data
        self.logger.info("[TRADE] %s %s %.0f@%.3f", data.vt_tradeid, data.vt_symbol, float(data.volume), float(data.price))

    def _on_position(self, event: Event) -> None:
        data: PositionData = event.data
        volume = float(data.volume)
        if volume <= 0:
            return
        cached = self._position_cache.get(data.vt_positionid)
        if cached == volume:
            return
        self._position_cache[data.vt_positionid] = volume
        self.logger.info("[POSITION] %s volume=%.0f yd=%.0f", data.vt_positionid, volume, float(data.yd_volume))

    def _on_tick(self, event: Event) -> None:
        _data: TickData = event.data
        return

    def _load_xtp_setting(self) -> dict[str, Any]:
        connect_path = Path(self.config["connect_file"])
        payload = json.loads(connect_path.read_text(encoding="utf-8"))
        client_id_override = self.config.get("client_id_override")
        if client_id_override:
            payload[XTP_CLIENT_ID_KEY] = int(client_id_override)
        return payload

    def connect(self) -> None:
        setting = self._load_xtp_setting()
        self.main_engine.connect(setting, self.gateway_name)
        self.logger.info("[XTP_CONNECT] client_id=%s", setting.get(XTP_CLIENT_ID_KEY))

    def wait_until_ready(self, timeout: float = 30.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            accounts = self.main_engine.get_all_accounts()
            contracts = self.main_engine.get_all_contracts()
            if accounts and contracts:
                self.logger.info("[XTP_READY] accounts=%s contracts=%s", len(accounts), len(contracts))
                return True
            time.sleep(1)
        self.logger.warning("[XTP_READY_TIMEOUT] timeout=%.0fs", timeout)
        return False

    def reconnect_if_needed(self, stale_seconds: float = 90.0) -> None:
        if not self.connected_once:
            return
        if time.time() - self.last_account_event_at <= stale_seconds:
            return
        self.logger.warning("[XTP_RECONNECT] stale account heartbeat > %.0fs", stale_seconds)
        self.connect()

    def first_account(self) -> AccountData | None:
        accounts = self.main_engine.get_all_accounts()
        return accounts[0] if accounts else None

    def has_position(self, symbol: str) -> bool:
        return self.position_stats(symbol)["volume"] > 0

    def position_stats(self, symbol: str) -> dict[str, float]:
        vt_symbol = to_vt_symbol(symbol)
        if not vt_symbol:
            return {"volume": 0.0, "sellable": 0.0, "yd_volume": 0.0}
        total_volume = 0.0
        yd_volume = 0.0
        for position in self.main_engine.get_all_positions():
            if position.vt_symbol != vt_symbol:
                continue
            total_volume += max(float(position.volume or 0), 0.0)
            yd_volume += max(float(getattr(position, "yd_volume", 0) or 0), 0.0)
        return {
            "volume": total_volume,
            "sellable": min(yd_volume, total_volume),
            "yd_volume": yd_volume,
        }

    def get_order(self, vt_orderid: str) -> OrderData | None:
        return self.main_engine.get_order(vt_orderid)

    def get_tick(self, symbol: str) -> TickData | None:
        vt_symbol = to_vt_symbol(symbol)
        if not vt_symbol:
            return None
        return self.main_engine.get_tick(vt_symbol)

    def get_all_active_orders(self) -> list[OrderData]:
        return self.main_engine.get_all_active_orders()

    def get_all_trades(self) -> list[TradeData]:
        return self.main_engine.get_all_trades()

    def get_contract(self, symbol: str) -> ContractData | None:
        vt_symbol = to_vt_symbol(symbol)
        if not vt_symbol:
            return None
        return self.main_engine.get_contract(vt_symbol)

    def subscribe_symbol(self, symbol: str) -> bool:
        exchange = infer_exchange(symbol)
        if exchange is None:
            return False
        req = SubscribeRequest(symbol=symbol_code(symbol), exchange=exchange)
        self.main_engine.subscribe(req, self.gateway_name)
        return True

    def cancel_order(self, vt_orderid: str) -> bool:
        order = self.main_engine.get_order(vt_orderid)
        if not order or not order.is_active():
            return False
        self.main_engine.cancel_order(order.create_cancel_request(), order.gateway_name)
        return True

    def send_order(self, symbol: str, direction: Direction, price: float, volume: int, reference: str) -> str:
        exchange = infer_exchange(symbol)
        if exchange is None:
            raise ValueError(f"unknown exchange for {symbol}")
        req = OrderRequest(
            symbol=symbol_code(symbol),
            exchange=exchange,
            direction=direction,
            type=OrderType.LIMIT,
            volume=volume,
            price=price,
            offset=Offset.NONE,
            reference=reference,
        )
        return self.main_engine.send_order(req, self.gateway_name)

    def send_buy_order(self, signal: Signal, price: float, volume: int, reference: str) -> str:
        return self.send_order(signal.symbol, Direction.LONG, price, volume, reference)

    def send_sell_order(self, symbol: str, price: float, volume: int, reference: str) -> str:
        return self.send_order(symbol, Direction.SHORT, price, volume, reference)

    def close(self) -> None:
        self.main_engine.close()


class DingtalkNotifier:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.logger = logger
        notifications_cfg = dict(config.get("notifications", {}) or {})
        dingtalk_cfg = dict(notifications_cfg.get("dingtalk", {}) or {})
        self.enabled = bool(dingtalk_cfg.get("enabled", False))
        self.webhook = str(dingtalk_cfg.get("webhook") or "").strip()
        self.timeout_seconds = float(dingtalk_cfg.get("timeout_seconds", 5))
        self.at_all = bool(dingtalk_cfg.get("at_all", False))
        self.title_prefix = str(dingtalk_cfg.get("title_prefix") or "XTPBridge")
        self.loop_error_cooldown_seconds = float(dingtalk_cfg.get("loop_error_cooldown_seconds", 180))
        self.rate_limit_backoff_seconds = float(dingtalk_cfg.get("rate_limit_backoff_seconds", 90))
        self._last_sent: dict[str, float] = {}
        self._rate_limited_until: float = 0.0

    def send(
        self,
        title: str,
        lines: list[str],
        *,
        rate_limit_key: str = "",
        min_interval_seconds: float = 0.0,
    ) -> bool:
        if not self.enabled or not self.webhook:
            return False

        if time.time() < self._rate_limited_until:
            return False

        key = rate_limit_key.strip()
        if key and min_interval_seconds > 0:
            last_sent = self._last_sent.get(key, 0.0)
            if time.time() - last_sent < min_interval_seconds:
                return False

        content_lines = [f"{self.title_prefix} | {title}"]
        content_lines.extend(str(line).strip() for line in lines if str(line).strip())
        payload = {
            "msgtype": "text",
            "text": {"content": "\n".join(content_lines)},
            "at": {"isAtAll": self.at_all},
        }
        request = Request(
            self.webhook,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
            data = json.loads(raw) if raw else {}
            errcode = int(data.get("errcode", 0) or 0)
            if errcode != 0:
                if errcode == 660026:
                    self._rate_limited_until = max(self._rate_limited_until, time.time() + self.rate_limit_backoff_seconds)
                self.logger.warning("[DINGTALK_FAILED] title=%s response=%s", title, raw)
                return False
            if key:
                self._last_sent[key] = time.time()
            self.logger.info("[DINGTALK_SENT] title=%s", title)
            return True
        except (HTTPError, URLError, TimeoutError, OSError, ValueError) as exc:
            self.logger.warning("[DINGTALK_ERROR] title=%s error=%s", title, exc)
            return False

class PushXtpBridge:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        state_cfg = config["state"]
        self.state = BridgeState(
            Path(state_cfg["file"]),
            int(state_cfg.get("max_processed_keys", 4000)),
            int(state_cfg.get("max_trade_records", 4000)),
        )
        self.remote = RemoteSignalClient(config["remote"], logger)
        self.trader = XtpTrader(config["xtp"], logger)
        self.trader.state = self.state
        self.notifier = DingtalkNotifier(config, logger)
        self.stop_event = threading.Event()
        self.managed_orders: dict[str, ManagedOrder] = {}
        self.seen_trade_ids: set[str] = set()
        self.subscribed_symbols: set[str] = set()
        self.last_overlap_error_at: float = 0.0
        self.last_premarket_check_day: str = ""

    def _priority_label(self, priority: int) -> str:
        priority = min(3, max(1, int(priority)))
        if priority >= 2:
            return f"{priority}端重合"
        return "单端信号"

    def _notify(self, title: str, lines: list[str], *, rate_limit_key: str = "", min_interval_seconds: float = 0.0) -> None:
        self.notifier.send(
            title,
            lines,
            rate_limit_key=rate_limit_key,
            min_interval_seconds=min_interval_seconds,
        )

    def _xtp_health(self) -> dict[str, Any]:
        account = self.trader.first_account()
        contract_count = len(self.trader.main_engine.get_all_contracts())
        heartbeat_age: float | None = None
        if self.trader.last_account_event_at > 0:
            heartbeat_age = max(0.0, time.time() - self.trader.last_account_event_at)
        ready = bool(account and contract_count > 0 and heartbeat_age is not None and heartbeat_age <= 180)
        return {
            "ready": ready,
            "account": account,
            "contract_count": contract_count,
            "heartbeat_age": heartbeat_age,
        }

    def _startup_retry_cfg(self) -> dict[str, Any]:
        notifications_cfg = dict(self.config.get("notifications", {}) or {})
        dingtalk_cfg = dict(notifications_cfg.get("dingtalk", {}) or {})
        startup_cfg = dict(dingtalk_cfg.get("startup_retry", {}) or {})
        startup_cfg.setdefault("enabled", True)
        startup_cfg.setdefault("attempts", 4)
        startup_cfg.setdefault("wait_timeout_seconds", 30)
        startup_cfg.setdefault("sleep_seconds", 6)
        return startup_cfg

    def _premarket_check_cfg(self) -> dict[str, Any]:
        notifications_cfg = dict(self.config.get("notifications", {}) or {})
        dingtalk_cfg = dict(notifications_cfg.get("dingtalk", {}) or {})
        premarket_cfg = dict(dingtalk_cfg.get("premarket_check", {}) or {})
        premarket_cfg.setdefault("enabled", True)
        premarket_cfg.setdefault("time", "09:20")
        premarket_cfg.setdefault("window_minutes", 15)
        return premarket_cfg

    def _close_summary_cfg(self) -> dict[str, Any]:
        notifications_cfg = dict(self.config.get("notifications", {}) or {})
        dingtalk_cfg = dict(notifications_cfg.get("dingtalk", {}) or {})
        close_cfg = dict(dingtalk_cfg.get("close_summary", {}) or {})
        close_cfg.setdefault("enabled", True)
        close_cfg.setdefault("time", "15:05")
        close_cfg.setdefault("window_minutes", 60)
        return close_cfg

    def _build_position_mark_patch(self, position: dict[str, Any], tick: TickData | None, marked_at: datetime) -> dict[str, Any] | None:
        if not tick:
            return None
        entry_price = float(position.get("avg_price", 0) or 0)
        volume = int(float(position.get("volume", 0) or 0))
        if entry_price <= 0 or volume <= 0:
            return None
        last_price = float(tick.last_price or tick.bid_price_1 or tick.ask_price_1 or 0)
        if last_price <= 0:
            return None
        highest_price = max(float(position.get("highest_price", 0) or 0), last_price, entry_price)
        return {
            "current_price": round(last_price, 6),
            "market_value": round(last_price * volume, 2),
            "floating_pnl": round((last_price - entry_price) * volume, 2),
            "floating_pnl_pct": round(((last_price - entry_price) / entry_price), 6) if entry_price > 0 else 0.0,
            "mark_time": format_dt(marked_at),
            "highest_price": round(highest_price, 6),
            "highest_price_at": format_dt(marked_at) if highest_price > float(position.get("highest_price", 0) or 0) else position.get("highest_price_at", ""),
            "last_update": format_dt(marked_at),
        }

    def _refresh_strategy_marks(self) -> bool:
        marked_at = now_cn()
        marks_changed = False
        for position in self.state.get_all_strategy_positions():
            symbol = str(position.get("symbol") or "").upper()
            if not symbol:
                continue
            self._subscribe_symbol(symbol)
            patch = self._build_position_mark_patch(position, self.trader.get_tick(symbol), marked_at)
            if not patch:
                continue
            previous_last = float(position.get("current_price", 0) or 0)
            previous_pnl = float(position.get("floating_pnl", 0) or 0)
            previous_value = float(position.get("market_value", 0) or 0)
            previous_high = float(position.get("highest_price", 0) or 0)
            missing_mark_fields = any(position.get(key) in (None, "") for key in ("current_price", "market_value", "floating_pnl", "mark_time"))
            if not missing_mark_fields:
                if (
                    abs(previous_last - float(patch["current_price"])) < 1e-6
                    and abs(previous_pnl - float(patch["floating_pnl"])) < 0.01
                    and abs(previous_value - float(patch["market_value"])) < 0.01
                    and abs(previous_high - float(patch["highest_price"])) < 1e-6
                ):
                    continue
            self.state.patch_strategy_position(symbol, **patch)
            marks_changed = True
        if marks_changed:
            self.state.save()
        return marks_changed

    def _ensure_xtp_ready_on_startup(self) -> dict[str, Any]:
        startup_cfg = self._startup_retry_cfg()
        attempts = max(1, int(startup_cfg.get("attempts", 4)))
        wait_timeout = max(5.0, float(startup_cfg.get("wait_timeout_seconds", 30)))
        sleep_seconds = max(1.0, float(startup_cfg.get("sleep_seconds", 6)))
        health = self._xtp_health()

        for attempt in range(1, attempts + 1):
            self.trader.connect()
            ready = self.trader.wait_until_ready(timeout=wait_timeout)
            health = self._xtp_health()
            if ready and health["ready"]:
                self.logger.info(
                    "[XTP_STARTUP_READY] attempt=%s contracts=%s heartbeat_age=%.1f",
                    attempt,
                    health["contract_count"],
                    float(health["heartbeat_age"] or 0.0),
                )
                return health
            self.logger.warning(
                "[XTP_STARTUP_RETRY] attempt=%s/%s ready=%s contracts=%s heartbeat_age=%s",
                attempt,
                attempts,
                health["ready"],
                health["contract_count"],
                f"{float(health['heartbeat_age']):.1f}s" if health["heartbeat_age"] is not None else "n/a",
            )
            if attempt < attempts:
                time.sleep(sleep_seconds)
        return health

    def _maybe_run_premarket_check(self) -> None:
        premarket_cfg = self._premarket_check_cfg()
        if not bool(premarket_cfg.get("enabled", True)):
            return

        now = now_cn()
        if now.weekday() >= 5:
            return

        check_time = parse_clock(str(premarket_cfg.get("time", "09:20")), "09:20")
        window_minutes = max(1, int(premarket_cfg.get("window_minutes", 15)))
        window_end = (datetime.combine(now.date(), check_time) + timedelta(minutes=window_minutes)).time()
        today_key = now.strftime("%Y-%m-%d")
        if self.last_premarket_check_day == today_key:
            return
        if now.time() < check_time or now.time() > window_end:
            return

        snapshot_ok = False
        snapshot_count = 0
        snapshot_error = ""
        try:
            snapshot = self.remote.fetch_snapshot()
            signals = snapshot.get("signals") or []
            snapshot_count = len(signals) if isinstance(signals, list) else 0
            snapshot_ok = True
        except Exception as exc:
            snapshot_error = str(exc)

        health = self._xtp_health()
        status_title = "开盘前自检通过" if health["ready"] and snapshot_ok else "开盘前自检异常"
        heartbeat_text = (
            f"{float(health['heartbeat_age']):.1f}s"
            if health["heartbeat_age"] is not None
            else "无"
        )
        lines = [
            f"时间: {format_dt(now)}",
            f"XTP就绪: {'是' if health['ready'] else '否'}",
            f"账户: {health['account'].vt_accountid if health['account'] else '未获取'}",
            f"合约数量: {health['contract_count']}",
            f"账户心跳: {heartbeat_text}",
            f"信号源: {'正常' if snapshot_ok else '异常'}",
            f"快照信号数: {snapshot_count}",
        ]
        if snapshot_error:
            lines.append(f"信号错误: {snapshot_error}")

        self._notify(status_title, lines, rate_limit_key=f"premarket_{today_key}", min_interval_seconds=600)
        self.last_premarket_check_day = today_key

        if not health["ready"]:
            self.logger.warning("[PREMARKET_XTP_NOT_READY] attempting reconnect")
            self.trader.connect()

    def _build_close_summary(self, trading_day: str) -> tuple[str, list[str]]:
        records = self.state.get_trade_records(trading_day)
        buy_records = [record for record in records if str(record.get("side") or "") == "buy"]
        sell_records = [record for record in records if str(record.get("side") or "") == "sell"]

        realized_profit = round(sum(max(0.0, float(record.get("realized_pnl", 0) or 0)) for record in sell_records), 2)
        realized_loss = round(sum(max(0.0, -float(record.get("realized_pnl", 0) or 0)) for record in sell_records), 2)
        realized_net = round(realized_profit - realized_loss, 2)
        win_count = sum(1 for record in sell_records if float(record.get("realized_pnl", 0) or 0) > 0)
        loss_count = sum(1 for record in sell_records if float(record.get("realized_pnl", 0) or 0) < 0)

        floating_profit = 0.0
        floating_loss = 0.0
        floating_details: list[dict[str, Any]] = []
        for position in self.state.get_all_strategy_positions():
            symbol = str(position.get("symbol") or "").upper()
            if not symbol:
                continue
            self._subscribe_symbol(symbol)
            tick = self.trader.get_tick(symbol)
            avg_price = float(position.get("avg_price", 0) or 0)
            volume = int(float(position.get("volume", 0) or 0))
            if avg_price <= 0 or volume <= 0:
                continue
            last_price = float(
                (tick.last_price or tick.bid_price_1 or tick.ask_price_1) if tick else avg_price
            )
            pnl = round((last_price - avg_price) * volume, 2)
            if pnl >= 0:
                floating_profit += pnl
            else:
                floating_loss += -pnl
            floating_details.append(
                {
                    "symbol": symbol,
                    "name": str(position.get("name") or symbol),
                    "pnl": pnl,
                    "volume": volume,
                    "last_price": last_price,
                    "avg_price": avg_price,
                }
            )

        floating_profit = round(floating_profit, 2)
        floating_loss = round(floating_loss, 2)
        floating_net = round(floating_profit - floating_loss, 2)
        total_net = round(realized_net + floating_net, 2)

        lines = [
            f"日期: {trading_day}",
            f"买入成交: {len(buy_records)} 笔",
            f"卖出成交: {len(sell_records)} 笔",
            f"已实现盈利: {realized_profit:.2f}",
            f"已实现亏损: {realized_loss:.2f}",
            f"已实现净值: {realized_net:.2f}",
            f"浮动盈利: {floating_profit:.2f}",
            f"浮动亏损: {floating_loss:.2f}",
            f"浮动净值: {floating_net:.2f}",
            f"总盈亏: {total_net:.2f}",
            f"当前持仓: {len(floating_details)} 只",
            f"平仓盈利笔数: {win_count}",
            f"平仓亏损笔数: {loss_count}",
        ]

        if sell_records:
            best_realized = max(sell_records, key=lambda item: float(item.get("realized_pnl", 0) or 0))
            worst_realized = min(sell_records, key=lambda item: float(item.get("realized_pnl", 0) or 0))
            lines.append(
                f"最佳平仓: {best_realized.get('symbol', '')} {best_realized.get('name', '')} {float(best_realized.get('realized_pnl', 0) or 0):.2f}"
            )
            lines.append(
                f"最差平仓: {worst_realized.get('symbol', '')} {worst_realized.get('name', '')} {float(worst_realized.get('realized_pnl', 0) or 0):.2f}"
            )

        if floating_details:
            best_floating = max(floating_details, key=lambda item: float(item.get("pnl", 0) or 0))
            worst_floating = min(floating_details, key=lambda item: float(item.get("pnl", 0) or 0))
            lines.append(
                f"最大浮盈: {best_floating.get('symbol', '')} {best_floating.get('name', '')} {float(best_floating.get('pnl', 0) or 0):.2f}"
            )
            lines.append(
                f"最大浮亏: {worst_floating.get('symbol', '')} {worst_floating.get('name', '')} {float(worst_floating.get('pnl', 0) or 0):.2f}"
            )

        title = "收盘总结"
        if total_net > 0:
            title = "收盘总结 盈利"
        elif total_net < 0:
            title = "收盘总结 亏损"
        return title, lines

    def _maybe_send_close_summary(self) -> None:
        close_cfg = self._close_summary_cfg()
        if not bool(close_cfg.get("enabled", True)):
            return

        now = now_cn()
        if now.weekday() >= 5:
            return

        close_time = parse_clock(str(close_cfg.get("time", "15:05")), "15:05")
        window_minutes = max(1, int(close_cfg.get("window_minutes", 60)))
        window_end = (datetime.combine(now.date(), close_time) + timedelta(minutes=window_minutes)).time()
        trading_day = now.strftime("%Y-%m-%d")
        if self.state.has_close_summary_sent(trading_day):
            return
        if now.time() < close_time or now.time() > window_end:
            return

        title, lines = self._build_close_summary(trading_day)
        self._notify(
            title,
            lines,
            rate_limit_key=f"close_summary_{trading_day}",
            min_interval_seconds=1800,
        )
        self.state.mark_close_summary_sent(trading_day, format_dt(now))
        self.state.save()

    def _buy_stage_markups(self) -> list[float]:
        buy_cfg = self.config.get("buy", {}) or {}
        raw_steps = buy_cfg.get("price_markup_bps_steps")
        if isinstance(raw_steps, list) and raw_steps:
            return [float(v) for v in raw_steps]
        base = float(self.config["order"].get("price_markup_bps", 20))
        return [base, max(base + 40, 60.0)]

    def _sell_stage_markdowns(self) -> list[float]:
        sell_cfg = self.config.get("sell", {}) or {}
        raw_steps = sell_cfg.get("price_markdown_bps_steps")
        if isinstance(raw_steps, list) and raw_steps:
            return [float(v) for v in raw_steps]
        return [10.0, 40.0]

    def _buy_stage_timeout(self) -> float:
        return float((self.config.get("buy", {}) or {}).get("stage_timeout_seconds", 8))

    def _sell_stage_timeout(self) -> float:
        return float((self.config.get("sell", {}) or {}).get("stage_timeout_seconds", 6))

    def _sell_cfg(self) -> dict[str, Any]:
        cfg = dict(self.config.get("sell", {}) or {})
        cfg.setdefault("enabled", True)
        cfg.setdefault("earliest_sell_time", "09:35")
        cfg.setdefault("single_weak_exit_time", "10:30")
        cfg.setdefault("forced_exit_time", "14:50")
        cfg.setdefault("stage_timeout_seconds", 6)
        cfg.setdefault("price_markdown_bps_steps", [10, 40])
        cfg.setdefault(
            "tiers",
            {
                "3": {
                    "stop_loss_pct": 0.045,
                    "take_profit_pct": 0.06,
                    "take_profit_ratio": 0.5,
                    "trail_drawdown_pct": 0.025,
                    "max_hold_trading_days": 3,
                },
                "2": {
                    "stop_loss_pct": 0.035,
                    "take_profit_pct": 0.04,
                    "take_profit_ratio": 0.5,
                    "trail_drawdown_pct": 0.02,
                    "max_hold_trading_days": 2,
                },
                "1": {
                    "stop_loss_pct": 0.025,
                    "take_profit_pct": 0.028,
                    "take_profit_ratio": 1.0,
                    "trail_drawdown_pct": 0.0,
                    "max_hold_trading_days": 1,
                    "weak_exit_if_not_green": True,
                },
            },
        )
        return cfg

    def _allocation_cfg(self) -> dict[str, Any]:
        cfg = dict(self.config.get("allocation", {}) or {})
        cfg.setdefault("enabled", True)
        cfg.setdefault("capital_base", "available_cash")
        cfg.setdefault("max_capital_base", 0)
        cfg.setdefault("overlap_api_url", "http://127.0.0.1:8792/api/data")
        cfg.setdefault("priority_weights", {"3": 4, "2": 2, "1": 1})
        cfg.setdefault("priority_budget_factors", {"3": 1.0, "2": 1.0, "1": 0.20})
        cfg.setdefault("min_source_count_to_trade", 2)
        cfg.setdefault("allow_single_source_trades", True)
        cfg.setdefault(
            "source_endpoints",
            {
                "alltick": "http://127.0.0.1:8768/api/realtime/signals?limit=0",
                "eastmoney": "http://127.0.0.1:8790/api/realtime/signals?limit=0",
                "pytdx": "http://127.0.0.1:8768/api/realtime/pytdx-backup/status",
            },
        )
        cfg.setdefault("basic_auth_username", "demo")
        cfg.setdefault("basic_auth_password", "demo")
        return cfg

    def _priority_weight_map(self) -> dict[int, float]:
        raw = (self._allocation_cfg().get("priority_weights") or {})
        result: dict[int, float] = {1: 1.0, 2: 2.0, 3: 4.0}
        if isinstance(raw, dict):
            for key, value in raw.items():
                try:
                    priority = min(3, max(1, int(key)))
                    weight = float(value)
                except (TypeError, ValueError):
                    continue
                if weight > 0:
                    result[priority] = weight
        return result

    def _priority_budget_factor_map(self) -> dict[int, float]:
        raw = (self._allocation_cfg().get("priority_budget_factors") or {})
        result: dict[int, float] = {1: 0.20, 2: 1.0, 3: 1.0}
        if isinstance(raw, dict):
            for key, value in raw.items():
                try:
                    priority = min(3, max(1, int(key)))
                    factor = float(value)
                except (TypeError, ValueError):
                    continue
                if factor >= 0:
                    result[priority] = factor
        return result

    def _source_endpoints(self) -> dict[str, str]:
        endpoints = self._allocation_cfg().get("source_endpoints") or {}
        if not isinstance(endpoints, dict):
            return {}
        normalized: dict[str, str] = {}
        for source_id, source_url in endpoints.items():
            source_key = str(source_id or "").strip().lower()
            source_value = str(source_url or "").strip()
            if source_key and source_value:
                normalized[source_key] = source_value
        return normalized

    def _min_source_count_to_trade(self) -> int:
        try:
            value = int(self._allocation_cfg().get("min_source_count_to_trade", 2))
        except (TypeError, ValueError):
            value = 2
        return min(3, max(1, value))

    def _allow_single_source_trades(self) -> bool:
        return bool(self._allocation_cfg().get("allow_single_source_trades", False))

    def _priority_fixed_volume_map(self) -> dict[int, int]:
        raw = (self.config.get("order", {}) or {}).get("priority_fixed_volumes") or {}
        result: dict[int, int] = {}
        if not isinstance(raw, dict):
            return result
        for key, value in raw.items():
            try:
                priority = min(3, max(1, int(key)))
                volume = int(value)
            except (TypeError, ValueError):
                continue
            if volume > 0:
                result[priority] = volume
        return result

    def _effective_allocation_cash(self, account: AccountData | None) -> float:
        raw_available = float(account.available) if account else 0.0
        try:
            max_capital_base = float(self._allocation_cfg().get("max_capital_base", 0) or 0)
        except (TypeError, ValueError):
            max_capital_base = 0.0
        if max_capital_base > 0:
            return max(0.0, min(raw_available, max_capital_base))
        return max(0.0, raw_available)

    def _position_priority(self, position: dict[str, Any]) -> int:
        try:
            return min(3, max(1, int(position.get("priority") or 1)))
        except (TypeError, ValueError):
            return 1

    def _tier_sell_rule(self, priority: int) -> dict[str, Any]:
        tiers = self._sell_cfg().get("tiers") or {}
        if not isinstance(tiers, dict):
            tiers = {}
        rule = dict(tiers.get(str(min(3, max(1, priority)))) or {})
        if priority == 3:
            rule.setdefault("stop_loss_pct", 0.045)
            rule.setdefault("take_profit_pct", 0.06)
            rule.setdefault("take_profit_ratio", 0.5)
            rule.setdefault("trail_drawdown_pct", 0.025)
            rule.setdefault("max_hold_trading_days", 3)
        elif priority == 2:
            rule.setdefault("stop_loss_pct", 0.035)
            rule.setdefault("take_profit_pct", 0.04)
            rule.setdefault("take_profit_ratio", 0.5)
            rule.setdefault("trail_drawdown_pct", 0.02)
            rule.setdefault("max_hold_trading_days", 2)
        else:
            rule.setdefault("stop_loss_pct", 0.025)
            rule.setdefault("take_profit_pct", 0.028)
            rule.setdefault("take_profit_ratio", 1.0)
            rule.setdefault("trail_drawdown_pct", 0.0)
            rule.setdefault("max_hold_trading_days", 1)
            rule.setdefault("weak_exit_if_not_green", True)
        return rule

    def _trading_days_since(self, trading_day: str, current: datetime | None = None) -> int:
        current = current or now_cn()
        start_dt = parse_trading_day(trading_day)
        if start_dt is None:
            entry_dt = parse_signal_dt(trading_day)
            start_dt = entry_dt.replace(tzinfo=None) if entry_dt else None
        if start_dt is None:
            return 0
        cursor = start_dt.date() + timedelta(days=1)
        end_date = current.date()
        count = 0
        while cursor <= end_date:
            if cursor.weekday() < 5:
                count += 1
            cursor += timedelta(days=1)
        return count

    def _lot_size_for(self, contract: ContractData | None) -> int:
        lot_size = int(self.config["order"].get("lot_size", 100))
        if contract and contract.min_volume:
            lot_size = max(lot_size, int(contract.min_volume))
        return lot_size

    def _sell_target_volume(self, total_volume: int, ratio: float, contract: ContractData | None) -> int:
        total_volume = int(total_volume)
        if total_volume <= 0:
            return 0
        if ratio >= 1:
            return total_volume
        lot_size = self._lot_size_for(contract)
        if total_volume <= lot_size:
            return total_volume
        target = int(total_volume * float(ratio))
        rounded = max(lot_size, (target // lot_size) * lot_size)
        if rounded >= total_volume:
            if total_volume >= lot_size * 2:
                return total_volume - lot_size
            return total_volume
        return rounded

    def _fetch_json_url(self, url: str, username: str = "", password: str = "") -> dict[str, Any]:
        headers: dict[str, str] = {}
        if username or password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        request = Request(url, headers=headers)
        with urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def _extract_overlap_items(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) and payload.get("ok") else payload
        overlap = data.get("overlap", {}) if isinstance(data, dict) else {}
        items = overlap.get("items", []) if isinstance(overlap, dict) else []
        return items if isinstance(items, list) else []

    def _extract_source_rows(self, source_id: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        data = payload.get("data") if isinstance(payload, dict) else {}
        rows = data.get("signals", []) if isinstance(data, dict) else []
        normalized_rows: list[dict[str, Any]] = []
        for row in rows if isinstance(rows, list) else []:
            if source_id == "pytdx":
                symbol = str(row.get("code") or row.get("symbol") or "").strip().upper()
                signal_time = str(row.get("buy_time") or row.get("signal_time") or "").strip()
            else:
                symbol = str(row.get("symbol") or row.get("code") or "").strip().upper()
                signal_time = str(row.get("signal_time") or row.get("buy_time") or "").strip()
            if symbol and signal_time:
                normalized_rows.append({"symbol": symbol, "signal_time": signal_time})
        return normalized_rows

    def _fetch_source_signal_map(self, base_snapshot: dict[str, Any]) -> dict[str, dict[str, Signal]]:
        allocation_cfg = self._allocation_cfg()
        username = str(allocation_cfg.get("basic_auth_username") or "").strip()
        password = str(allocation_cfg.get("basic_auth_password") or "").strip()
        source_signal_map: dict[str, dict[str, Signal]] = {}

        for source_id, source_url in self._source_endpoints().items():
            try:
                snapshot = normalize_snapshot_payload(
                    self._fetch_json_url(source_url, username=username, password=password),
                    self.remote.protocol,
                    self.logger,
                )
            except Exception as exc:
                if source_id == "alltick" and isinstance(base_snapshot, dict):
                    snapshot = base_snapshot
                else:
                    now_ts = time.time()
                    if now_ts - self.last_overlap_error_at >= 60:
                        self.logger.warning("[SOURCE_FETCH_FAILED] %s %s", source_id, exc)
                        self.last_overlap_error_at = now_ts
                    continue

            latest_by_symbol: dict[str, Signal] = {}
            for item in snapshot.get("signals") or []:
                signal = Signal.from_dict(item)
                if signal is None:
                    continue
                previous = latest_by_symbol.get(signal.symbol)
                if previous is None or signal.signal_time > previous.signal_time:
                    latest_by_symbol[signal.symbol] = signal
            source_signal_map[source_id] = latest_by_symbol

        return source_signal_map

    def _build_candidate_signals(self, base_snapshot: dict[str, Any]) -> tuple[list[Signal], dict[str, int]]:
        grouped: dict[str, dict[str, Signal]] = {}
        for source_id, rows in self._fetch_source_signal_map(base_snapshot).items():
            for symbol, signal in rows.items():
                grouped.setdefault(symbol, {})[source_id] = signal

        candidates: list[Signal] = []
        source_counts: dict[str, int] = {}
        min_source_count = self._min_source_count_to_trade()
        allow_single_source = self._allow_single_source_trades()

        for symbol, source_rows in grouped.items():
            representative = max(source_rows.values(), key=lambda item: item.signal_time)
            same_day_rows = {
                source_id: signal
                for source_id, signal in source_rows.items()
                if signal.trading_day == representative.trading_day
            }
            source_count = len(same_day_rows)
            source_counts[symbol] = min(3, source_count)
            if source_count < min_source_count and not (allow_single_source and source_count == 1):
                continue
            representative = max(same_day_rows.values(), key=lambda item: item.signal_time)
            source_ids = sorted(same_day_rows)
            merged_raw = dict(representative.raw)
            merged_raw["key"] = f"{representative.symbol}|{representative.trading_day}|{representative.signal_time}|src:{','.join(source_ids)}"
            merged_raw["source_count"] = source_count
            merged_raw["source_ids"] = source_ids
            merged_signal = Signal.from_dict(merged_raw)
            if merged_signal is not None:
                candidates.append(merged_signal)

        candidates.sort(key=lambda item: item.signal_time)
        return candidates, source_counts

    def _fetch_overlap_source_counts(self) -> dict[str, int]:
        allocation_cfg = self._allocation_cfg()
        url = str(allocation_cfg.get("overlap_api_url") or "").strip()
        try:
            items = self._extract_overlap_items(self._fetch_json_url(url)) if url else []
            source_counts: dict[str, int] = {}
            for item in items:
                symbol = str(item.get("symbol") or "").strip().upper()
                try:
                    source_count = int(item.get("source_count") or 0)
                except (TypeError, ValueError):
                    source_count = 0
                if symbol and source_count >= 2:
                    source_counts[symbol] = min(3, source_count)
            if source_counts:
                return source_counts
        except Exception as exc:
            now_ts = time.time()
            if now_ts - self.last_overlap_error_at >= 60:
                self.logger.warning("[OVERLAP_API_FALLBACK] %s", exc)
                self.last_overlap_error_at = now_ts

        username = str(allocation_cfg.get("basic_auth_username") or "").strip()
        password = str(allocation_cfg.get("basic_auth_password") or "").strip()
        endpoints = allocation_cfg.get("source_endpoints") or {}
        if not isinstance(endpoints, dict):
            return {}

        grouped: dict[str, set[str]] = {}
        try:
            for source_id, source_url in endpoints.items():
                source_id = str(source_id or "").strip()
                source_url = str(source_url or "").strip()
                if not source_id or not source_url:
                    continue
                rows = self._extract_source_rows(source_id, self._fetch_json_url(source_url, username=username, password=password))
                latest_by_symbol: dict[str, str] = {}
                for row in rows:
                    symbol = row["symbol"]
                    stamp = row["signal_time"]
                    previous = latest_by_symbol.get(symbol)
                    if previous is None or stamp > previous:
                        latest_by_symbol[symbol] = stamp
                for symbol in latest_by_symbol:
                    grouped.setdefault(symbol, set()).add(source_id)
            return {symbol: min(3, len(sources)) for symbol, sources in grouped.items() if len(sources) >= 2}
        except Exception as exc:
            now_ts = time.time()
            if now_ts - self.last_overlap_error_at >= 60:
                self.logger.warning("[OVERLAP_SOURCE_FETCH_FAILED] %s", exc)
                self.last_overlap_error_at = now_ts
            return {}

    def _signal_priority(self, signal: Signal, overlap_source_counts: dict[str, int]) -> int:
        try:
            source_count = int(signal.raw.get("source_count") or 0)
        except (TypeError, ValueError):
            source_count = 0
        if source_count > 0:
            return min(3, max(1, source_count))
        return min(3, max(1, int(overlap_source_counts.get(signal.symbol, 1))))

    def _subscribe_symbol(self, symbol: str) -> None:
        if not symbol or symbol in self.subscribed_symbols:
            return
        if self.trader.subscribe_symbol(symbol):
            self.subscribed_symbols.add(symbol)
            self.logger.info("[SUBSCRIBE] %s", symbol)

    def _should_skip_signal(self, signal: Signal) -> tuple[bool, str]:
        filters = self.config["filters"]
        order_cfg = self.config["order"]
        code = symbol_code(signal.symbol)
        name_upper = signal.name.upper()
        exchange = infer_exchange(signal.symbol)
        now = now_cn()

        if exchange is None:
            return True, "unknown exchange"
        if int(now.weekday()) not in [int(v) for v in filters.get("allow_weekdays", [0, 1, 2, 3, 4])]:
            return True, "weekday not allowed"
        if not within_sessions(now, list(filters.get("trading_sessions", ["09:30-11:30", "13:00-15:00"]))):
            return True, "outside trading session"
        if filters.get("skip_bj", True) and exchange == Exchange.BSE:
            return True, "skip BSE"
        if filters.get("skip_star", True) and code.startswith("688"):
            return True, "skip STAR"
        if filters.get("skip_st", True) and "ST" in name_upper:
            return True, "skip ST"
        signal_dt = parse_signal_dt(signal.signal_time)
        if signal_dt is None:
            return True, "bad signal time"
        if order_cfg.get("require_same_trading_day", True) and signal.trading_day != now.strftime("%Y-%m-%d"):
            return True, f"stale trading day {signal.trading_day}"
        max_age = int(order_cfg.get("max_signal_age_seconds", 300))
        age = (now - signal_dt).total_seconds()
        if age < -5:
            return True, "future signal"
        if age > max_age:
            return True, f"signal too old {int(age)}s"
        if self.state.order_count(signal.trading_day, signal.symbol) >= int(order_cfg.get("max_orders_per_symbol_per_day", 1)):
            return True, "symbol daily limit reached"
        if self.state.get_strategy_position(signal.symbol):
            return True, "strategy position exists"
        if bool(order_cfg.get("skip_if_has_position", True)) and self.trader.has_position(signal.symbol):
            return True, "broker position exists"
        return False, ""

    def _normalize_order_volume(self, volume: int, contract: ContractData | None, lot_size: int) -> int:
        normalized = int(volume or 0)
        if normalized <= 0:
            return 0
        max_volume = 0
        if contract is not None:
            for attr in ("max_limit_order_volume", "max_market_order_volume"):
                try:
                    candidate = int(getattr(contract, attr, 0) or 0)
                except (TypeError, ValueError):
                    continue
                if candidate > 0:
                    max_volume = candidate if max_volume <= 0 else min(max_volume, candidate)
        if max_volume > 0:
            normalized = min(normalized, max_volume)
        if lot_size > 1:
            normalized = (normalized // lot_size) * lot_size
        return max(0, normalized)

    def _calc_order_volume(
        self,
        signal: Signal,
        contract: ContractData | None,
        budget_cash: float | None = None,
        reference_price: float | None = None,
        priority: int = 1,
    ) -> int:
        order_cfg = self.config["order"]
        lot_size = int(order_cfg.get("lot_size", 100))
        if contract and contract.min_volume:
            lot_size = max(lot_size, int(contract.min_volume))
        priority_fixed_volume = self._priority_fixed_volume_map().get(min(3, max(1, int(priority))), 0)
        if priority_fixed_volume > 0:
            lots = max(1, math.ceil(priority_fixed_volume / lot_size))
            return self._normalize_order_volume(lots * lot_size, contract, lot_size)
        if budget_cash is not None and budget_cash > 0:
            price = float(reference_price or signal.signal_price or 0)
            if price <= 0:
                return 0
            raw_lots = int(float(budget_cash) // (price * lot_size))
            if raw_lots <= 0:
                return 0
            return self._normalize_order_volume(raw_lots * lot_size, contract, lot_size)
        fixed_volume = int(order_cfg.get("fixed_volume", 0))
        if fixed_volume > 0:
            lots = max(1, math.ceil(fixed_volume / lot_size))
            return self._normalize_order_volume(lots * lot_size, contract, lot_size)
        cash_per_order = float(order_cfg.get("cash_per_order", 0))
        if cash_per_order <= 0:
            return self._normalize_order_volume(lot_size, contract, lot_size)
        raw_lots = int(cash_per_order // (signal.signal_price * lot_size))
        if raw_lots <= 0:
            return 0
        return self._normalize_order_volume(raw_lots * lot_size, contract, lot_size)

    def _apply_order_volume_cap(self, volume: int, contract: ContractData | None) -> int:
        normalized = int(volume or 0)
        if normalized <= 0:
            return 0
        order_cfg = self.config["order"]
        lot_size = int(order_cfg.get("lot_size", 100))
        if contract and contract.min_volume:
            lot_size = max(lot_size, int(contract.min_volume))
        try:
            max_single_order_volume = int(order_cfg.get("max_single_order_volume", 0) or 0)
        except (TypeError, ValueError):
            max_single_order_volume = 0
        if max_single_order_volume > 0:
            normalized = min(normalized, max_single_order_volume)
        return self._normalize_order_volume(normalized, contract, lot_size)

    def _calc_buy_price(self, signal: Signal, contract: ContractData | None, markup_bps: float) -> float:
        tick = float(contract.pricetick) if contract and contract.pricetick else 0.01
        target = signal.signal_price * (1 + float(markup_bps) / 10000.0)
        return round_to_tick(target, tick)

    def _calc_sell_price(self, symbol: str, fallback_price: float, contract: ContractData | None, markdown_bps: float) -> float:
        tick_size = float(contract.pricetick) if contract and contract.pricetick else 0.01
        tick = self.trader.get_tick(symbol)
        candidates: list[float] = []
        if tick:
            last_price = float(tick.last_price or 0)
            bid_price_1 = float(tick.bid_price_1 or 0)
            if bid_price_1 > 0:
                candidates.append(bid_price_1)
            if last_price > 0:
                candidates.append(last_price * (1 - float(markdown_bps) / 10000.0))
        if not candidates:
            candidates.append(float(fallback_price) * (1 - float(markdown_bps) / 10000.0))
        target = min(candidates)
        if tick and float(tick.limit_down or 0) > 0:
            target = max(target, float(tick.limit_down))
        return round_to_tick(target, tick_size)

    def _daily_order_total(self, trading_day: str) -> int:
        return sum(int(v) for v in self.state.ordered_symbols_by_day.get(trading_day, {}).values())

    def _register_managed_order(
        self,
        vt_orderid: str,
        symbol: str,
        side: str,
        attempt: int,
        requested_volume: int,
        requested_price: float,
        context: dict[str, Any],
    ) -> None:
        self.managed_orders[vt_orderid] = ManagedOrder(
            vt_orderid=vt_orderid,
            symbol=symbol,
            side=side,
            attempt=attempt,
            requested_volume=requested_volume,
            requested_price=requested_price,
            created_at=time.time(),
            context=dict(context),
        )

    def _submit_buy_order(self, signal: Signal, volume: int, attempt: int, priority: int = 1) -> str:
        contract = self.trader.get_contract(signal.symbol)
        if contract is None:
            raise RuntimeError(f"contract not ready for {signal.symbol}")
        markup = self._buy_stage_markups()[attempt - 1]
        price = self._calc_buy_price(signal, contract, markup)
        reference = f"push_bridge|buy|{signal.key}|a{attempt}"
        vt_orderid = self.trader.send_buy_order(signal, price, volume, reference)
        if vt_orderid:
            self._register_managed_order(
                vt_orderid,
                signal.symbol,
                "buy",
                attempt,
                volume,
                price,
                {
                    "signal_key": signal.key,
                    "symbol": signal.symbol,
                    "name": signal.name,
                    "signal_price": signal.signal_price,
                    "signal_time": signal.signal_time,
                    "trading_day": signal.trading_day,
                    "priority": int(priority),
                },
            )
        return vt_orderid

    def _submit_sell_order(self, position: dict[str, Any], volume: int, reason: str, attempt: int, extra_context: dict[str, Any] | None = None) -> str:
        symbol = str(position.get("symbol") or "").upper()
        contract = self.trader.get_contract(symbol)
        if contract is None:
            raise RuntimeError(f"contract not ready for {symbol}")
        markdown = self._sell_stage_markdowns()[attempt - 1]
        price = self._calc_sell_price(symbol, float(position.get("avg_price", 0) or 0), contract, markdown)
        reference = f"push_bridge|sell|{symbol}|{reason}|a{attempt}"
        vt_orderid = self.trader.send_sell_order(symbol, price, volume, reference)
        if vt_orderid:
            self._register_managed_order(
                vt_orderid,
                symbol,
                "sell",
                attempt,
                volume,
                price,
                {
                    "symbol": symbol,
                    "name": position.get("name", symbol),
                    "reason": reason,
                    "entry_time": position.get("entry_time", ""),
                    "avg_price": float(position.get("avg_price", 0) or 0),
                    "priority": self._position_priority(position),
                    **(extra_context or {}),
                },
            )
        return vt_orderid

    def _reconcile_trades(self) -> None:
        trades = list(self.trader.get_all_trades())
        trades.sort(key=lambda item: item.datetime or datetime.min)
        changed = False
        for trade in trades:
            if trade.vt_tradeid in self.seen_trade_ids:
                continue
            self.seen_trade_ids.add(trade.vt_tradeid)
            managed = self.managed_orders.get(trade.vt_orderid)
            if not managed:
                continue
            traded_at = format_dt(trade.datetime)
            if managed.side == "buy":
                traded_day = traded_at[:10] if len(traded_at) >= 10 else str(managed.context.get("trading_day") or "")
                self.state.upsert_strategy_buy(
                    symbol=managed.symbol,
                    name=str(managed.context.get("name") or managed.symbol),
                    volume=float(trade.volume),
                    price=float(trade.price),
                    signal_key=str(managed.context.get("signal_key") or ""),
                    trading_day=str(managed.context.get("trading_day") or ""),
                    traded_at=traded_at,
                    priority=int(managed.context.get("priority") or 1),
                )
                self.state.add_trade_record(
                    {
                        "side": "buy",
                        "symbol": managed.symbol,
                        "name": str(managed.context.get("name") or managed.symbol),
                        "priority": int(managed.context.get("priority") or 1),
                        "volume": int(float(trade.volume)),
                        "price": round(float(trade.price), 6),
                        "traded_at": traded_at,
                        "trading_day": traded_day,
                        "signal_time": str(managed.context.get("signal_time") or ""),
                        "signal_price": round(float(managed.context.get("signal_price") or 0), 6),
                        "signal_key": str(managed.context.get("signal_key") or ""),
                        "realized_pnl": 0.0,
                    }
                )
                self._subscribe_symbol(managed.symbol)
                self.logger.info("[BUY_FILL] %s volume=%.0f price=%.3f tradeid=%s", managed.symbol, float(trade.volume), float(trade.price), trade.vt_tradeid)
                self._notify(
                    "买入成交",
                    [
                        f"标的: {managed.symbol} {managed.context.get('name', managed.symbol)}",
                        f"优先级: {self._priority_label(int(managed.context.get('priority') or 1))}",
                        f"成交数量: {int(float(trade.volume))}",
                        f"成交价格: {float(trade.price):.3f}",
                        f"信号时间: {managed.context.get('signal_time', '')}",
                        f"成交时间: {traded_at}",
                        f"成交编号: {trade.vt_tradeid}",
                    ],
                )
                changed = True
            elif managed.side == "sell":
                position_before_sell = self.state.get_strategy_position(managed.symbol) or {}
                avg_price = float(position_before_sell.get("avg_price", managed.context.get("avg_price", 0)) or 0)
                realized_pnl = round((float(trade.price) - avg_price) * float(trade.volume), 2) if avg_price > 0 else 0.0
                traded_day = traded_at[:10] if len(traded_at) >= 10 else ""
                self.state.apply_strategy_sell(managed.symbol, float(trade.volume), traded_at)
                self.state.add_trade_record(
                    {
                        "side": "sell",
                        "symbol": managed.symbol,
                        "name": str(managed.context.get("name") or managed.symbol),
                        "priority": int(managed.context.get("priority") or 1),
                        "volume": int(float(trade.volume)),
                        "price": round(float(trade.price), 6),
                        "traded_at": traded_at,
                        "trading_day": traded_day,
                        "entry_time": str(managed.context.get("entry_time") or position_before_sell.get("entry_time") or ""),
                        "entry_trading_day": str(position_before_sell.get("trading_day") or ""),
                        "avg_price": round(avg_price, 6),
                        "reason": str(managed.context.get("reason") or ""),
                        "realized_pnl": realized_pnl,
                    }
                )
                if managed.context.get("mark_partial_exit"):
                    self.state.patch_strategy_position(
                        managed.symbol,
                        partial_exit_done=True,
                        partial_exit_time=traded_at,
                        partial_exit_price=round(float(trade.price), 6),
                        partial_exit_reason=str(managed.context.get("reason") or ""),
                        last_update=traded_at,
                    )
                self.logger.info(
                    "[SELL_FILL] %s volume=%.0f price=%.3f tradeid=%s reason=%s",
                    managed.symbol,
                    float(trade.volume),
                    float(trade.price),
                    trade.vt_tradeid,
                    managed.context.get("reason", ""),
                )
                self._notify(
                    "卖出成交",
                    [
                        f"标的: {managed.symbol} {managed.context.get('name', managed.symbol)}",
                        f"优先级: {self._priority_label(int(managed.context.get('priority') or 1))}",
                        f"成交数量: {int(float(trade.volume))}",
                        f"成交价格: {float(trade.price):.3f}",
                        f"卖出原因: {managed.context.get('reason', '')}",
                        f"本次盈亏: {realized_pnl:.2f}",
                        f"成交时间: {traded_at}",
                        f"成交编号: {trade.vt_tradeid}",
                    ],
                )
                changed = True
        if changed:
            self.state.save()

    def _sync_strategy_positions_with_broker(self) -> None:
        changed = False
        now_text = format_dt(now_cn())
        for position in list(self.state.get_all_strategy_positions()):
            symbol = str(position.get("symbol") or "").upper()
            if not symbol:
                continue
            broker_stats = self.trader.position_stats(symbol)
            broker_volume = int(float(broker_stats.get("volume", 0) or 0))
            sellable_volume = int(float(broker_stats.get("sellable", 0) or 0))
            state_volume = int(float(position.get("volume", 0) or 0))

            if broker_volume <= 0 and not self._has_pending_sell(symbol):
                self.state.apply_strategy_sell(symbol, state_volume, now_text)
                self.logger.warning("[POSITION_STALE_DROP] %s state_volume=%s broker_volume=0", symbol, state_volume)
                changed = True
                continue

            patch: dict[str, Any] = {}
            if broker_volume > 0 and state_volume != broker_volume:
                patch["volume"] = broker_volume
                self.logger.info("[POSITION_SYNC] %s state_volume=%s broker_volume=%s sellable=%s", symbol, state_volume, broker_volume, sellable_volume)
            if int(float(position.get("broker_volume", 0) or 0)) != broker_volume:
                patch["broker_volume"] = broker_volume
            if int(float(position.get("sellable_volume", 0) or 0)) != sellable_volume:
                patch["sellable_volume"] = sellable_volume
            if patch:
                patch["last_update"] = now_text
                self.state.patch_strategy_position(symbol, **patch)
                changed = True

        if changed:
            self.state.save()

    def _reconcile_managed_orders(self) -> None:
        for vt_orderid, managed in list(self.managed_orders.items()):
            order = self.trader.get_order(vt_orderid)
            if not order:
                continue

            timeout = self._buy_stage_timeout() if managed.side == "buy" else self._sell_stage_timeout()
            if order.is_active():
                if not managed.cancel_requested and time.time() - managed.created_at >= timeout:
                    if self.trader.cancel_order(vt_orderid):
                        managed.cancel_requested = True
                        self.logger.info("[CANCEL_REQUEST] %s %s attempt=%s", managed.side.upper(), managed.symbol, managed.attempt)
                continue

            remaining = max(0, int(round(float(order.volume) - float(order.traded))))
            max_attempts = len(self._buy_stage_markups()) if managed.side == "buy" else len(self._sell_stage_markdowns())
            should_retry = order.status in {Status.CANCELLED, Status.REJECTED} and remaining > 0 and managed.attempt < max_attempts
            self.managed_orders.pop(vt_orderid, None)

            if not should_retry:
                continue

            if managed.side == "buy":
                signal = Signal(
                    key=str(managed.context.get("signal_key") or ""),
                    symbol=str(managed.context.get("symbol") or managed.symbol),
                    name=str(managed.context.get("name") or managed.symbol),
                    trading_day=str(managed.context.get("trading_day") or ""),
                    signal_time=str(managed.context.get("signal_time") or ""),
                    signal_price=float(managed.context.get("signal_price") or 0),
                    raw=dict(managed.context),
                )
                vt_retry = self._submit_buy_order(signal, remaining, managed.attempt + 1, priority=int(managed.context.get("priority") or 1))
                if vt_retry:
                    self.logger.info("[BUY_RETRY] %s remaining=%s attempt=%s orderid=%s", signal.symbol, remaining, managed.attempt + 1, vt_retry)
            else:
                position = self.state.get_strategy_position(managed.symbol)
                if not position:
                    continue
                broker_stats = self.trader.position_stats(managed.symbol)
                retry_volume = min(
                    remaining,
                    int(float(position.get("volume", 0) or 0)),
                    int(float(broker_stats.get("sellable", 0) or 0)),
                )
                if retry_volume <= 0:
                    self.logger.info("[SELL_RETRY_SKIP] %s remaining=%s sellable=0", managed.symbol, remaining)
                    continue
                vt_retry = self._submit_sell_order(position, retry_volume, str(managed.context.get("reason") or "retry"), managed.attempt + 1)
                if vt_retry:
                    self.logger.info("[SELL_RETRY] %s remaining=%s attempt=%s orderid=%s", managed.symbol, retry_volume, managed.attempt + 1, vt_retry)

    def _has_pending_sell(self, symbol: str) -> bool:
        return any(order.side == "sell" and order.symbol == symbol for order in self.managed_orders.values())

    def _evaluate_sell_rules(self) -> None:
        sell_cfg = self._sell_cfg()
        if not bool(sell_cfg.get("enabled", True)):
            return
        now = now_cn()
        if not within_sessions(now, list(self.config["filters"].get("trading_sessions", ["09:30-11:30", "13:00-15:00"]))):
            return
        earliest_sell_time = parse_clock(sell_cfg.get("earliest_sell_time", "09:35"), "09:35")
        weak_exit_time = parse_clock(sell_cfg.get("single_weak_exit_time", "10:30"), "10:30")
        forced_exit_time = parse_clock(sell_cfg.get("forced_exit_time", "14:50"), "14:50")

        for position in self.state.get_all_strategy_positions():
            symbol = str(position.get("symbol") or "").upper()
            if not symbol or self._has_pending_sell(symbol):
                continue

            self._subscribe_symbol(symbol)
            tick = self.trader.get_tick(symbol)
            if not tick:
                continue

            last_price = float(tick.last_price or tick.bid_price_1 or tick.ask_price_1 or 0)
            entry_price = float(position.get("avg_price", 0) or 0)
            if last_price <= 0 or entry_price <= 0:
                continue
            if now.time() < earliest_sell_time:
                continue

            entry_day = str(position.get("trading_day") or "")
            trading_days_held = self._trading_days_since(entry_day, now)
            if trading_days_held <= 0:
                continue

            priority = self._position_priority(position)
            tier_rule = self._tier_sell_rule(priority)
            contract = self.trader.get_contract(symbol)
            highest_price = max(float(position.get("highest_price", 0) or 0), last_price, entry_price)
            if highest_price > float(position.get("highest_price", 0) or 0):
                self.state.patch_strategy_position(symbol, highest_price=round(highest_price, 6), highest_price_at=format_dt(now), last_update=format_dt(now))
            partial_exit_done = bool(position.get("partial_exit_done", False))
            pnl_pct = (last_price - entry_price) / entry_price
            drawdown_pct = ((highest_price - last_price) / highest_price) if highest_price > 0 else 0.0
            trigger_reason = ""
            trigger_volume = 0
            extra_context: dict[str, Any] = {}
            stop_loss_pct = float(tier_rule.get("stop_loss_pct", 0) or 0)
            take_profit_pct = float(tier_rule.get("take_profit_pct", 0) or 0)
            take_profit_ratio = float(tier_rule.get("take_profit_ratio", 1.0) or 1.0)
            trail_drawdown_pct = float(tier_rule.get("trail_drawdown_pct", 0) or 0)
            max_hold_trading_days = int(tier_rule.get("max_hold_trading_days", 0) or 0)
            weak_exit_if_not_green = bool(tier_rule.get("weak_exit_if_not_green", False))

            broker_stats = self.trader.position_stats(symbol)
            broker_volume = int(float(broker_stats.get("volume", 0) or 0))
            sellable_volume = int(float(broker_stats.get("sellable", 0) or 0))
            if broker_volume <= 0:
                self.state.apply_strategy_sell(symbol, float(position.get("volume", 0) or 0), format_dt(now))
                self.state.save()
                self.logger.warning("[SELL_SKIP_STALE] %s dropped because broker volume is 0", symbol)
                continue

            current_volume = min(int(float(position.get("volume", 0) or 0)), broker_volume)
            if current_volume <= 0:
                continue

            if stop_loss_pct > 0 and pnl_pct <= -stop_loss_pct:
                trigger_reason = f"stop_loss_p{priority}"
                trigger_volume = current_volume
            elif priority == 1 and weak_exit_if_not_green and trading_days_held >= 1 and now.time() >= weak_exit_time and pnl_pct <= 0:
                trigger_reason = "weak_t1_exit"
                trigger_volume = current_volume
            elif not partial_exit_done and take_profit_pct > 0 and pnl_pct >= take_profit_pct:
                trigger_volume = self._sell_target_volume(current_volume, take_profit_ratio, contract)
                trigger_reason = "take_profit_partial" if trigger_volume < current_volume else "take_profit_full"
                if trigger_volume < current_volume:
                    extra_context["mark_partial_exit"] = True
            elif partial_exit_done and trail_drawdown_pct > 0 and drawdown_pct >= trail_drawdown_pct:
                trigger_reason = f"trail_exit_p{priority}"
                trigger_volume = current_volume
            elif max_hold_trading_days > 0 and trading_days_held >= max_hold_trading_days and now.time() >= forced_exit_time:
                trigger_reason = f"time_exit_t{max_hold_trading_days}"
                trigger_volume = current_volume

            if not trigger_reason:
                continue

            trigger_volume = min(trigger_volume, sellable_volume)
            if trigger_volume <= 0:
                self.logger.info("[SELL_SKIP] %s reason=%s sellable=0 broker_volume=%s", symbol, trigger_reason, broker_volume)
                continue
            vt_orderid = self._submit_sell_order(position, trigger_volume, trigger_reason, 1, extra_context=extra_context)
            if vt_orderid:
                self.logger.info(
                    "[SELL_TRIGGER] %s priority=%s held_days=%s volume=%s reason=%s last_price=%.3f entry=%.3f pnl=%.4f drawdown=%.4f orderid=%s",
                    symbol,
                    priority,
                    trading_days_held,
                    trigger_volume,
                    trigger_reason,
                    last_price,
                    entry_price,
                    pnl_pct,
                    drawdown_pct,
                    vt_orderid,
                )
                self._notify(
                    "卖出触发",
                    [
                        f"标的: {symbol} {position.get('name', symbol)}",
                        f"优先级: {self._priority_label(priority)}",
                        f"触发原因: {trigger_reason}",
                        f"触发数量: {trigger_volume}",
                        f"最新价格: {last_price:.3f}",
                        f"持仓成本: {entry_price:.3f}",
                        f"持有交易日: {trading_days_held}",
                        f"订单编号: {vt_orderid}",
                    ],
                    rate_limit_key=f"sell_trigger:{symbol}:{trigger_reason}",
                    min_interval_seconds=300,
                )

    def _handle_signal(self, signal: Signal, budget_cash: float | None = None, priority: int = 1) -> None:
        if self.state.has_processed(signal.key):
            return

        skip, reason = self._should_skip_signal(signal)
        if skip:
            self.state.mark_processed(signal.key)
            self.logger.info("[SKIP] %s %s: %s", signal.symbol, signal.signal_time, reason)
            self.state.save()
            return

        order_cfg = self.config["order"]
        daily_limit = int(order_cfg.get("daily_order_limit", 20))
        if self._daily_order_total(signal.trading_day) >= daily_limit:
            self.state.mark_processed(signal.key)
            self.logger.warning("[SKIP] %s: daily order limit reached %s", signal.symbol, daily_limit)
            self.state.save()
            return

        contract = self.trader.get_contract(signal.symbol)
        if contract is None:
            self._subscribe_symbol(signal.symbol)
            self.logger.warning("[SKIP] %s: contract not ready", signal.symbol)
            return

        stage_price = self._calc_buy_price(signal, contract, self._buy_stage_markups()[0])
        volume = self._calc_order_volume(signal, contract, budget_cash=budget_cash, reference_price=stage_price, priority=priority)
        capped_volume = self._apply_order_volume_cap(volume, contract)
        if capped_volume != volume:
            self.logger.info("[VOLUME_CAP] %s raw=%s capped=%s", signal.symbol, volume, capped_volume)
        volume = capped_volume
        if volume <= 0:
            self.state.mark_processed(signal.key)
            if budget_cash is not None and budget_cash > 0:
                self.logger.info("[SKIP] %s: allocation %.2f insufficient for one lot", signal.symbol, float(budget_cash))
            else:
                self.logger.info("[SKIP] %s: volume calculation returned zero", signal.symbol)
            self.state.save()
            return

        account = self.trader.first_account()
        if account:
            required_cash = stage_price * volume
            if bool(self.config["order"].get("require_available_cash", True)) and float(account.available) < required_cash:
                self.state.mark_processed(signal.key)
                self.logger.warning("[SKIP] %s: available cash %.2f < %.2f", signal.symbol, float(account.available), required_cash)
                self.state.save()
                return

        self._subscribe_symbol(signal.symbol)
        if bool(self.config["order"].get("enabled", True)):
            vt_orderid = self._submit_buy_order(signal, volume, 1, priority=priority)
            if not vt_orderid:
                self.logger.warning("[BUY_FAILED] %s: empty order id", signal.symbol)
                return
            self.logger.info(
                "[BUY_SUBMIT] %s %s priority=%s volume=%s price=%.3f signal_price=%.3f budget=%.2f orderid=%s",
                signal.symbol,
                signal.name,
                priority,
                volume,
                stage_price,
                signal.signal_price,
                float(budget_cash or 0),
                vt_orderid,
            )
            self._notify(
                "买入委托",
                [
                    f"标的: {signal.symbol} {signal.name}",
                    f"优先级: {self._priority_label(priority)}",
                    f"委托数量: {volume}",
                    f"委托价格: {stage_price:.3f}",
                    f"信号价格: {signal.signal_price:.3f}",
                    f"分配资金: {float(budget_cash or 0):.2f}",
                    f"信号时间: {signal.signal_time}",
                    f"订单编号: {vt_orderid}",
                ],
            )
        else:
            self.logger.info(
                "[DRY_RUN_BUY] %s %s priority=%s volume=%s price=%.3f signal_price=%.3f budget=%.2f",
                signal.symbol,
                signal.name,
                priority,
                volume,
                stage_price,
                signal.signal_price,
                float(budget_cash or 0),
            )

        self.state.mark_processed(signal.key)
        self.state.mark_order(signal.trading_day, signal.symbol)
        self.state.save()

    def _handle_snapshot(self, snapshot: dict[str, Any]) -> None:
        signals, overlap_source_counts = self._build_candidate_signals(snapshot)

        if not signals:
            self.logger.info(
                "[SNAPSHOT_EMPTY] watchlist=%s signal_total=%s eligible=%s",
                snapshot.get("watchlist_count"),
                snapshot.get("signal_total_count"),
                0,
            )
            return

        if not self.state.bootstrap_complete and not self.state.processed_keys:
            for signal in signals:
                self.state.mark_processed(signal.key)
            self.state.bootstrap_complete = True
            self.state.save()
            self.logger.info("[BOOTSTRAP] skipped current %s historical signals", len(signals))
            return

        self.state.bootstrap_complete = True
        order_cfg = self.config["order"]
        daily_limit = int(order_cfg.get("daily_order_limit", 20))
        remaining_slots = max(0, daily_limit - self._daily_order_total(now_cn().strftime("%Y-%m-%d")))

        if remaining_slots <= 0:
            for signal in signals:
                if self.state.has_processed(signal.key):
                    continue
                self.state.mark_processed(signal.key)
                self.logger.warning("[SKIP] %s: daily order limit reached %s", signal.symbol, daily_limit)
            self.state.save()
            return

        planned_symbol_counts: dict[str, int] = {}
        candidates_by_priority: dict[int, list[Signal]] = {3: [], 2: [], 1: []}

        for signal in signals:
            if self.state.has_processed(signal.key):
                continue
            skip, reason = self._should_skip_signal(signal)
            if skip:
                self.state.mark_processed(signal.key)
                self.logger.info("[SKIP] %s %s: %s", signal.symbol, signal.signal_time, reason)
                continue
            max_orders_per_symbol = int(order_cfg.get("max_orders_per_symbol_per_day", 1))
            planned_count = planned_symbol_counts.get(signal.symbol, 0)
            existing_count = self.state.order_count(signal.trading_day, signal.symbol)
            if existing_count + planned_count >= max_orders_per_symbol:
                self.state.mark_processed(signal.key)
                self.logger.info("[SKIP] %s %s: symbol daily limit already planned", signal.symbol, signal.signal_time)
                continue
            contract = self.trader.get_contract(signal.symbol)
            if contract is None:
                self._subscribe_symbol(signal.symbol)
                self.logger.warning("[WAIT_CONTRACT] %s: contract not ready", signal.symbol)
                continue
            priority = self._signal_priority(signal, overlap_source_counts)
            candidates_by_priority.setdefault(priority, []).append(signal)
            planned_symbol_counts[signal.symbol] = planned_count + 1

        selected: list[tuple[Signal, int]] = []
        skipped_by_limit: list[tuple[Signal, int]] = []
        for priority in (3, 2, 1):
            for signal in candidates_by_priority.get(priority, []):
                if len(selected) < remaining_slots:
                    selected.append((signal, priority))
                else:
                    skipped_by_limit.append((signal, priority))

        for signal, priority in skipped_by_limit:
            self.state.mark_processed(signal.key)
            self.logger.info("[SKIP] %s %s: daily limit reserved for higher priority tiers (p%s)", signal.symbol, signal.signal_time, priority)

        if not selected:
            self.state.save()
            return

        selected_priorities = sorted({priority for _, priority in selected}, reverse=True)
        priority_weights = self._priority_weight_map()
        priority_budget_factors = self._priority_budget_factor_map()
        total_weight = sum(priority_weights.get(priority, 1.0) for priority in selected_priorities)
        account = self.trader.first_account()
        raw_available_cash = float(account.available) if account else 0.0
        available_cash = self._effective_allocation_cash(account)
        if account and abs(raw_available_cash - available_cash) >= 0.01:
            self.logger.info(
                "[BUY_PLAN_CAP] raw_available=%.2f effective_available=%.2f",
                raw_available_cash,
                available_cash,
            )
        allocation_enabled = bool(self._allocation_cfg().get("enabled", True)) and available_cash > 0 and total_weight > 0

        budget_map: dict[str, float] = {}
        if allocation_enabled:
            for priority in selected_priorities:
                tier_rows = [signal for signal, signal_priority in selected if signal_priority == priority]
                if not tier_rows:
                    continue
                tier_budget = available_cash * priority_weights.get(priority, 1.0) / total_weight
                tier_budget *= priority_budget_factors.get(priority, 1.0)
                per_signal_budget = tier_budget / len(tier_rows)
                for signal in tier_rows:
                    budget_map[signal.key] = per_signal_budget
                self.logger.info(
                    "[BUY_PLAN] priority=%s signals=%s tier_budget=%.2f per_signal=%.2f available=%.2f factor=%.2f",
                    priority,
                    len(tier_rows),
                    tier_budget,
                    per_signal_budget,
                    available_cash,
                    priority_budget_factors.get(priority, 1.0),
                )

        for signal, priority in selected:
            self._handle_signal(signal, budget_cash=budget_map.get(signal.key), priority=priority)

        self.state.save()

    def run(self) -> None:
        health = self._ensure_xtp_ready_on_startup()
        self.seen_trade_ids = {trade.vt_tradeid for trade in self.trader.get_all_trades()}
        self._sync_strategy_positions_with_broker()
        for position in self.state.get_all_strategy_positions():
            self._subscribe_symbol(str(position.get("symbol") or ""))
        poll_interval = float(self.config["remote"].get("poll_interval_seconds", 3))
        account = health["account"] or self.trader.first_account()
        self._notify(
            "桥接启动",
            [
                f"时间: {format_dt(now_cn())}",
                f"订单开关: {'开启' if bool(self.config.get('order', {}).get('enabled', True)) else '关闭'}",
                f"账户: {account.vt_accountid if account else '未获取'}",
                f"可用资金: {float(account.available):.2f}" if account else "",
                f"XTP就绪: {'是' if health['ready'] else '否'}",
                f"合约数量: {health['contract_count']}",
            ],
            rate_limit_key="bridge_start",
            min_interval_seconds=30,
        )

        while not self.stop_event.is_set():
            try:
                self._maybe_run_premarket_check()
                self._maybe_send_close_summary()
                self.trader.reconnect_if_needed()
                self._reconcile_trades()
                self._sync_strategy_positions_with_broker()
                self._refresh_strategy_marks()
                snapshot = self.remote.fetch_snapshot()
                self._handle_snapshot(snapshot)
                self._reconcile_managed_orders()
                self._evaluate_sell_rules()
            except KeyboardInterrupt:
                self.logger.info("[STOP] interrupted by keyboard")
                break
            except Exception:
                self.logger.exception("[LOOP_ERROR] bridge loop failed")
                self._notify(
                    "桥接异常",
                    [
                        f"时间: {format_dt(now_cn())}",
                        "主循环发生异常，请检查服务器日志。",
                        "日志文件: push_xtp_bridge.log",
                    ],
                    rate_limit_key="loop_error",
                    min_interval_seconds=self.notifier.loop_error_cooldown_seconds,
                )
            self.stop_event.wait(poll_interval)

        self.shutdown()

    def shutdown(self) -> None:
        self.stop_event.set()
        self.state.save()
        self.remote.close()
        self.trader.close()
        self.logger.info("[STOPPED] bridge shutdown complete")


def build_logger(config: dict[str, Any]) -> logging.Logger:
    log_cfg = config["logging"]
    logger = logging.getLogger("push_xtp_bridge")
    logger.setLevel(getattr(logging, str(log_cfg.get("level", "INFO")).upper(), logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_path = Path(log_cfg["file"])
    ensure_parent(log_path)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def load_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload.setdefault(
        "signal_protocol",
        {
            "version": "v1",
            "require_ok_flag": True,
            "ok_paths": ["ok"],
            "data_paths": ["data"],
            "signals_paths": ["signals"],
            "metadata_paths": {
                "updated_at": ["updated_at"],
                "watchlist_count": ["watchlist_count"],
                "signal_total_count": ["signal_total_count"],
            },
            "field_aliases": {
                "key": ["key"],
                "symbol": ["symbol", "code"],
                "name": ["name"],
                "trading_day": ["trading_day", "session_date"],
                "signal_time": ["signal_time", "buy_time"],
                "signal_price": ["signal_price", "buy_price"],
            },
            "required_fields": ["symbol", "trading_day", "signal_time", "signal_price"],
        },
    )
    payload.setdefault("buy", {"stage_timeout_seconds": 8, "price_markup_bps_steps": [20, 70]})
    payload.setdefault(
        "allocation",
        {
            "enabled": True,
            "capital_base": "available_cash",
            "overlap_api_url": "http://127.0.0.1:8792/api/data",
            "priority_weights": {"3": 4, "2": 2, "1": 1},
            "priority_budget_factors": {"3": 1.0, "2": 1.0, "1": 0.20},
            "min_source_count_to_trade": 2,
            "allow_single_source_trades": True,
            "source_endpoints": {
                "alltick": "http://127.0.0.1:8768/api/realtime/signals?limit=0",
                "eastmoney": "http://127.0.0.1:8790/api/realtime/signals?limit=0",
                "pytdx": "http://127.0.0.1:8768/api/realtime/pytdx-backup/status",
            },
            "basic_auth_username": "demo",
            "basic_auth_password": "demo",
        },
    )
    payload.setdefault(
        "notifications",
        {
            "dingtalk": {
                "enabled": False,
                "webhook": "",
                "timeout_seconds": 5,
                "at_all": False,
                "title_prefix": "XTPBridge",
                "loop_error_cooldown_seconds": 180,
                "startup_retry": {
                    "enabled": True,
                    "attempts": 4,
                    "wait_timeout_seconds": 30,
                    "sleep_seconds": 6,
                },
                "premarket_check": {
                    "enabled": True,
                    "time": "09:20",
                    "window_minutes": 15,
                },
                "close_summary": {
                    "enabled": True,
                    "time": "15:05",
                    "window_minutes": 60,
                },
            }
        },
    )
    payload.setdefault(
        "sell",
        {
            "enabled": True,
            "earliest_sell_time": "09:35",
            "single_weak_exit_time": "10:30",
            "forced_exit_time": "14:50",
            "stage_timeout_seconds": 6,
            "price_markdown_bps_steps": [10, 40],
            "tiers": {
                "3": {
                    "stop_loss_pct": 0.045,
                    "take_profit_pct": 0.06,
                    "take_profit_ratio": 0.5,
                    "trail_drawdown_pct": 0.025,
                    "max_hold_trading_days": 3,
                },
                "2": {
                    "stop_loss_pct": 0.035,
                    "take_profit_pct": 0.04,
                    "take_profit_ratio": 0.5,
                    "trail_drawdown_pct": 0.02,
                    "max_hold_trading_days": 2,
                },
                "1": {
                    "stop_loss_pct": 0.025,
                    "take_profit_pct": 0.028,
                    "take_profit_ratio": 1.0,
                    "trail_drawdown_pct": 0.0,
                    "max_hold_trading_days": 1,
                    "weak_exit_if_not_green": True,
                },
            },
        },
    )
    return payload


def mask_secret(config: dict[str, Any]) -> dict[str, Any]:
    masked = json.loads(json.dumps(config))
    remote = masked.get("remote", {})
    if remote.get("password"):
        remote["password"] = "***"
    if remote.get("basic_auth_password"):
        remote["basic_auth_password"] = "***"
    allocation = masked.get("allocation", {})
    if allocation.get("basic_auth_password"):
        allocation["basic_auth_password"] = "***"
    notifications = masked.get("notifications", {})
    dingtalk = notifications.get("dingtalk", {}) if isinstance(notifications, dict) else {}
    if dingtalk.get("webhook"):
        dingtalk["webhook"] = "***"
    return masked


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push realtime signal bridge for XTP")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("push_xtp_bridge.config.json")),
        help="Path to config file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    logger = build_logger(config)
    logger.info("Loaded config: %s", config_path)
    logger.info("Config summary: %s", json.dumps(mask_secret(config), ensure_ascii=False))

    bridge = PushXtpBridge(config, logger)
    try:
        bridge.run()
    finally:
        if not bridge.stop_event.is_set():
            bridge.shutdown()


if __name__ == "__main__":
    main()
