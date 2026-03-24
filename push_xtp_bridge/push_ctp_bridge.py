from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from tqsdk import TqApi, TqAuth, TqKq


CHINA_TZ = ZoneInfo("Asia/Shanghai")

EXCHANGE_NAME_MAP = {
    "上海期货交易所": "SHFE",
    "上期所": "SHFE",
    "大连商品交易所": "DCE",
    "大商所": "DCE",
    "郑州商品交易所": "CZCE",
    "郑商所": "CZCE",
    "中国金融期货交易所": "CFFEX",
    "中金所": "CFFEX",
    "上海国际能源交易中心": "INE",
    "能源中心": "INE",
    "广州期货交易所": "GFEX",
    "广期所": "GFEX",
}

EXCHANGE_CODE_SET = {"SHFE", "DCE", "CZCE", "CFFEX", "INE", "GFEX"}


def now_cn() -> datetime:
    return datetime.now(CHINA_TZ)


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def resolve_path(base_dir: Path, raw: str) -> Path:
    path = Path(str(raw))
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def parse_session(raw: str) -> tuple[dt_time, dt_time]:
    start_text, end_text = raw.split("-", 1)
    start = datetime.strptime(start_text.strip(), "%H:%M").time()
    end = datetime.strptime(end_text.strip(), "%H:%M").time()
    return start, end


def within_sessions(current: datetime, sessions: list[str]) -> bool:
    current_time = current.time()
    for item in sessions:
        start, end = parse_session(item)
        if start <= end:
            if start <= current_time <= end:
                return True
        else:
            if current_time >= start or current_time <= end:
                return True
    return False


def parse_dt(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=CHINA_TZ)
        except ValueError:
            continue
    return None


def format_dt(value: datetime | None) -> str:
    value = value or now_cn()
    if value.tzinfo is None:
        value = value.replace(tzinfo=CHINA_TZ)
    else:
        value = value.astimezone(CHINA_TZ)
    return value.strftime("%Y-%m-%d %H:%M:%S")


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    if math.isnan(number) or math.isinf(number):
        return default
    return number


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def round_to_tick(value: float, tick: float) -> float:
    if tick <= 0:
        return round(value, 2)
    scaled = round(value / tick)
    rounded = scaled * tick
    precision = 0
    tick_text = f"{tick}"
    if "." in tick_text:
        precision = len(tick_text.split(".", 1)[1].rstrip("0"))
    return round(rounded, precision)


def clean_text(value: Any) -> str:
    return str(value or "").strip()


def exchange_from_text(raw: str) -> str:
    text = clean_text(raw)
    upper = text.upper()
    if upper in EXCHANGE_CODE_SET:
        return upper
    return EXCHANGE_NAME_MAP.get(text, "")


def normalize_tq_symbol(symbol: str, exchange_code: str) -> str:
    raw_symbol = clean_text(symbol).upper()
    exchange = clean_text(exchange_code).upper()
    if not raw_symbol or not exchange:
        return ""

    def _clear_exit_tracking(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload.pop("pending_exit_orderid", None)
        payload.pop("pending_exit_reason", None)
        payload.pop("pending_exit_requested_at", None)
        return payload
    if "." in raw_symbol:
        return raw_symbol

    letters: list[str] = []
    digits: list[str] = []
    for ch in raw_symbol:
        if ch.isalpha():
            letters.append(ch)
        elif ch.isdigit():
            digits.append(ch)
    product = "".join(letters)
    serial = "".join(digits)
    if not product or not serial:
        return ""

    if exchange == "CZCE" and len(serial) == 4:
        serial = serial[1:]
        return f"{exchange}.{product}{serial}"

    if exchange in {"SHFE", "DCE", "INE", "GFEX"}:
        return f"{exchange}.{product.lower()}{serial}"
    return f"{exchange}.{product}{serial}"


def mask_secret(config: dict[str, Any]) -> dict[str, Any]:
    masked = json.loads(json.dumps(config))
    remote = masked.get("remote", {})
    if remote.get("password"):
        remote["password"] = "***"
    if remote.get("basic_auth_password"):
        remote["basic_auth_password"] = "***"
    notifications = masked.get("notifications", {})
    dingtalk = notifications.get("dingtalk", {}) if isinstance(notifications, dict) else {}
    if dingtalk.get("webhook"):
        dingtalk["webhook"] = "***"
    tqsdk = masked.get("tqsdk", {})
    if tqsdk.get("password"):
        tqsdk["password"] = "***"
    return masked


def first_present(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = clean_text(row.get(key))
        if value:
            return value
    return ""


@dataclass
class FuturesSignal:
    key: str
    continuous_symbol: str
    exchange_code: str
    exchange_name: str
    product_name: str
    product_code: str
    trading_day: str
    signal_time: str
    reference_price: float
    raw: dict[str, Any]

    @classmethod
    def from_event_row(cls, row: dict[str, Any]) -> "FuturesSignal | None":
        continuous_symbol = first_present(row, "连续代码", "杩炵画浠ｇ爜").upper()
        # Use the upstream discovery time so fresh pushes are not treated as stale.
        signal_time = first_present(row, "scan_time", "C_time")
        exchange_name = first_present(row, "交易所", "浜ゆ槗鎵€")
        product_name = first_present(row, "品种", "鍝佺") or continuous_symbol
        product_code = first_present(row, "代码", "浠ｇ爜").upper()
        close_text = first_present(row, "C_close")
        if not (continuous_symbol and signal_time and close_text and product_code):
            return None
        reference_price = safe_float(close_text, default=float("nan"))
        if math.isnan(reference_price):
            return None
        signal_dt = parse_dt(signal_time)
        trading_day = signal_dt.strftime("%Y-%m-%d") if signal_dt else signal_time[:10]
        key = "|".join(
            [
                continuous_symbol,
                first_present(row, "A_time"),
                first_present(row, "B_time"),
                first_present(row, "C_time"),
            ]
        )
        return cls(
            key=key,
            continuous_symbol=continuous_symbol,
            exchange_code=exchange_from_text(exchange_name),
            exchange_name=exchange_name,
            product_name=product_name,
            product_code=product_code,
            trading_day=trading_day,
            signal_time=signal_time,
            reference_price=reference_price,
            raw=dict(row),
        )


class BridgeState:
    def __init__(self, path: Path, max_processed: int, max_trade_records: int) -> None:
        self.path = path
        self.max_processed = max_processed
        self.max_trade_records = max_trade_records
        self.processed_keys: deque[str] = deque(maxlen=max_processed)
        self.ordered_symbols_by_day: dict[str, dict[str, int]] = {}
        self.strategy_positions: dict[str, dict[str, Any]] = {}
        self.trade_records: deque[dict[str, Any]] = deque(maxlen=max_trade_records)
        self.pending_open_meta: dict[str, list[dict[str, Any]]] = {}
        self.bootstrap_complete: bool = False
        self.active_front_label: str = ""
        self.last_ready_at: str = ""
        self.last_error: str = ""
        self.runtime_account: dict[str, Any] = {}
        self.resolved_contracts: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return
        for key in payload.get("processed_keys", []) or []:
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
            self.strategy_positions = {str(key): dict(value) for key, value in positions.items() if isinstance(value, dict)}
        for record in payload.get("trade_records", []) or []:
            if isinstance(record, dict):
                self.trade_records.append(dict(record))
        pending_open_meta = payload.get("pending_open_meta", {}) or {}
        if isinstance(pending_open_meta, dict):
            self.pending_open_meta = {
                str(key): [dict(item) for item in values if isinstance(item, dict)]
                for key, values in pending_open_meta.items()
                if isinstance(values, list)
            }
        self.bootstrap_complete = bool(payload.get("bootstrap_complete", False))
        self.active_front_label = clean_text(payload.get("active_front_label"))
        self.last_ready_at = clean_text(payload.get("last_ready_at"))
        self.last_error = clean_text(payload.get("last_error"))
        runtime_account = payload.get("runtime_account", {}) or {}
        if isinstance(runtime_account, dict):
            self.runtime_account = dict(runtime_account)
        resolved_contracts = payload.get("resolved_contracts", {}) or {}
        if isinstance(resolved_contracts, dict):
            self.resolved_contracts = {str(key): dict(value) for key, value in resolved_contracts.items() if isinstance(value, dict)}

    def save(self) -> None:
        payload = {
            "processed_keys": list(self.processed_keys),
            "ordered_symbols_by_day": self.ordered_symbols_by_day,
            "strategy_positions": self.strategy_positions,
            "trade_records": list(self.trade_records),
            "pending_open_meta": self.pending_open_meta,
            "bootstrap_complete": self.bootstrap_complete,
            "active_front_label": self.active_front_label,
            "last_ready_at": self.last_ready_at,
            "last_error": self.last_error,
            "runtime_account": self.runtime_account,
            "resolved_contracts": self.resolved_contracts,
        }
        ensure_parent(self.path)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def has_processed(self, key: str) -> bool:
        return key in self.processed_keys

    def mark_processed(self, key: str) -> None:
        self.processed_keys.append(key)

    def order_count(self, trading_day: str, symbol: str) -> int:
        return int(self.ordered_symbols_by_day.get(trading_day, {}).get(symbol, 0))

    def mark_order(self, trading_day: str, symbol: str) -> None:
        bucket = self.ordered_symbols_by_day.setdefault(trading_day, {})
        bucket[symbol] = int(bucket.get(symbol, 0)) + 1

    def get_strategy_position(self, tq_symbol: str) -> dict[str, Any] | None:
        payload = self.strategy_positions.get(tq_symbol)
        return dict(payload) if payload else None

    def upsert_strategy_position(self, tq_symbol: str, payload: dict[str, Any]) -> None:
        self.strategy_positions[tq_symbol] = dict(payload)

    def remove_strategy_position(self, tq_symbol: str) -> None:
        self.strategy_positions.pop(tq_symbol, None)

    def add_trade_record(self, record: dict[str, Any]) -> None:
        self.trade_records.append(dict(record))

    def stash_pending_open_meta(self, tq_symbol: str, payload: dict[str, Any]) -> None:
        if not tq_symbol or not isinstance(payload, dict):
            return
        bucket = self.pending_open_meta.setdefault(str(tq_symbol), [])
        bucket.append(dict(payload))

    def pop_pending_open_meta(self, tq_symbol: str) -> dict[str, Any]:
        bucket = self.pending_open_meta.get(str(tq_symbol)) or []
        if not bucket:
            return {}
        item = dict(bucket.pop(0))
        if bucket:
            self.pending_open_meta[str(tq_symbol)] = bucket
        else:
            self.pending_open_meta.pop(str(tq_symbol), None)
        return item

    def set_runtime_account(self, payload: dict[str, Any]) -> None:
        self.runtime_account = dict(payload)

    def upsert_resolved_contract(self, continuous_symbol: str, payload: dict[str, Any]) -> None:
        self.resolved_contracts[continuous_symbol] = dict(payload)


class RemoteFuturesSource:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.client: Any | None = None

    def _transport(self) -> str:
        remote = self.config["remote"]
        return str(remote.get("transport") or "http").strip().lower()

    def _http_headers(self) -> dict[str, str]:
        remote = self.config["remote"]
        headers = {"Accept": "application/json"}
        username = str(remote.get("basic_auth_username") or "").strip()
        password = str(remote.get("basic_auth_password") or "").strip()
        if username or password:
            token = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {token}"
        return headers

    def _connect(self) -> Any:
        raise RuntimeError("SSH transport disabled on 159 futures bridge; use HTTP remote transport")

    def fetch_snapshot(self) -> dict[str, Any]:
        remote = self.config["remote"]
        if self._transport() != "http":
            raise RuntimeError("SSH transport disabled on 159 futures bridge; use HTTP remote transport")
        request = Request(
            str(remote["api_url"]),
            headers=self._http_headers(),
            method="GET",
        )
        with urlopen(request, timeout=int(remote.get("command_timeout_seconds", 20))) as response:
            payload_raw = response.read().decode("utf-8", errors="replace")
        payload = json.loads(payload_raw or "{}")
        if not isinstance(payload, dict):
            raise RuntimeError("futures payload is not a dict")
        return payload

    def close(self) -> None:
        if self.client is not None:
            self.client.close()
            self.client = None


class DingtalkNotifier:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.config = config.get("notifications", {}).get("dingtalk", {})
        self.logger = logger
        self.last_sent_at: dict[str, float] = {}
        self.rate_limit_backoff_seconds = float(self.config.get("rate_limit_backoff_seconds", 90))
        self.rate_limited_until = 0.0

    def send(self, title: str, lines: list[str], rate_limit_key: str = "", min_interval_seconds: int = 0) -> None:
        if not self.config.get("enabled"):
            return
        webhook = clean_text(self.config.get("webhook"))
        if not webhook:
            return
        if time.time() < self.rate_limited_until:
            return
        key = rate_limit_key or title
        if min_interval_seconds > 0:
            last_sent = self.last_sent_at.get(key, 0.0)
            if time.time() - last_sent < min_interval_seconds:
                return
        body = {
            "msgtype": "text",
            "text": {
                "content": "\n".join(
                    [f"{self.config.get('title_prefix', 'CTPBridge')} | {title}", *[line for line in lines if line]]
                )
            },
            "at": {"isAtAll": bool(self.config.get("at_all", False))},
        }
        request = Request(
            webhook,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8"},
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        )
        try:
            with urlopen(request, timeout=float(self.config.get("timeout_seconds", 5))) as response:
                payload = response.read().decode("utf-8", errors="replace")
            try:
                data = json.loads(payload or "{}")
            except ValueError:
                data = {}
            errcode = int(data.get("errcode", 0) or 0) if isinstance(data, dict) else 0
            if errcode != 0:
                if errcode == 660026:
                    self.rate_limited_until = max(self.rate_limited_until, time.time() + self.rate_limit_backoff_seconds)
                self.logger.warning("[DINGTALK_FAILED] title=%s payload=%s", title, payload)
                return
            self.last_sent_at[key] = time.time()
            self.logger.info("[DINGTALK_SENT] title=%s payload=%s", title, payload)
        except Exception as exc:
            self.logger.warning("[DINGTALK_ERROR] title=%s error=%s", title, exc)

    @property
    def loop_error_cooldown_seconds(self) -> int:
        return int(self.config.get("loop_error_cooldown_seconds", 180))


class TqTrader:
    def __init__(self, config: dict[str, Any], logger: logging.Logger, state: BridgeState) -> None:
        self.config = config
        self.logger = logger
        self.state = state
        self.api: TqApi | None = None
        self.trade_account: TqKq | None = None
        self.account: Any = None
        self.orders: Any = None
        self.trades: Any = None
        self.quotes: dict[str, Any] = {}
        self.last_connect_attempt = 0.0
        self.last_account_log_at = 0.0
        self.connect_settings_cache: dict[str, Any] = {}

    def _connect_file(self) -> Path:
        section = self.config.get("tqsdk", {})
        return resolve_path(Path(__file__).resolve().parent, str(section.get("connect_file", "./connect_tq.json")))

    def _load_connect_setting(self) -> dict[str, Any]:
        data = json.loads(self._connect_file().read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise RuntimeError("connect_tq.json must be an object")
        return data

    def close(self) -> None:
        if self.api is not None:
            try:
                self.api.close()
            except Exception:
                pass
        self.api = None
        self.trade_account = None
        self.account = None
        self.orders = None
        self.trades = None
        self.quotes = {}

    def connect(self) -> bool:
        if self.is_ready():
            return True
        reconnect_interval = int(self.config.get("tqsdk", {}).get("reconnect_interval_seconds", 30))
        if time.time() - self.last_connect_attempt < reconnect_interval:
            return False
        self.last_connect_attempt = time.time()

        try:
            connect = self._load_connect_setting()
            self.connect_settings_cache = connect
            username = clean_text(connect.get("username"))
            password = str(connect.get("password") or "")
            if not username or not password:
                raise RuntimeError("missing TqSdk username/password")

            self.close()
            self.trade_account = TqKq()
            self.api = TqApi(self.trade_account, auth=TqAuth(username, password))
            self.account = self.api.get_account(self.trade_account)
            self.orders = self.api.get_order(account=self.trade_account)
            self.trades = self.api.get_trade(account=self.trade_account)
            timeout_seconds = float(self.config.get("tqsdk", {}).get("connect_timeout_seconds", 10))
            self.api.wait_update(deadline=time.time() + timeout_seconds)
            self.sync_runtime_account(force=True)
            snapshot = self.account_snapshot()
            if not snapshot.get("account"):
                raise RuntimeError("Tq account snapshot unavailable")

            self.state.active_front_label = clean_text(connect.get("account_mode")) or "TqKq"
            self.state.last_ready_at = format_dt(now_cn())
            self.state.last_error = ""
            self.state.save()
            self.logger.info(
                "[FUTURES_READY] active_front=%s accounts=%s contracts=%s",
                self.state.active_front_label or "TqKq",
                1,
                len(self.state.resolved_contracts),
            )
            return True
        except Exception as exc:
            self.state.last_error = f"tq_connect_error:{exc}"
            self.state.save()
            self.logger.warning("[TQ_CONNECT_ERROR] %s", exc)
            self.close()
            return False

    def is_ready(self) -> bool:
        return self.api is not None and self.account is not None

    def pump(self, timeout_seconds: float = 0.3) -> bool:
        if self.api is None:
            return False
        try:
            changed = self.api.wait_update(deadline=time.time() + max(timeout_seconds, 0.05))
        except Exception as exc:
            self.state.last_error = f"tq_wait_update_error:{exc}"
            self.state.save()
            self.logger.warning("[TQ_WAIT_UPDATE_ERROR] %s", exc)
            self.close()
            return False
        self.sync_runtime_account(force=False)
        return changed

    def sync_runtime_account(self, force: bool = False) -> None:
        snapshot = self.account_snapshot()
        if not snapshot.get("account"):
            return
        previous = dict(self.state.runtime_account or {})
        changed = any(
            snapshot.get(key) != previous.get(key)
            for key in ("account", "balance", "available", "frozen")
        )
        self.state.set_runtime_account(snapshot)
        self.state.save()
        if force or changed or (time.time() - self.last_account_log_at > 60):
            self.last_account_log_at = time.time()
            self.logger.info(
                "[FUTURES_ACCOUNT] %s balance=%.2f available=%.2f",
                snapshot.get("account"),
                safe_float(snapshot.get("balance")),
                safe_float(snapshot.get("available")),
            )

    def account_snapshot(self) -> dict[str, Any]:
        if self.account is None:
            return {}
        account_id = clean_text(getattr(self.account, "user_id", "")) or clean_text(self.connect_settings_cache.get("username"))
        balance = safe_float(getattr(self.account, "balance", 0))
        available = safe_float(getattr(self.account, "available", 0))
        return {
            "timestamp": format_dt(now_cn()),
            "account": account_id,
            "balance": round(balance, 2),
            "available": round(available, 2),
            "frozen": round(balance - available, 2),
        }

    def ensure_quote(self, tq_symbol: str) -> Any:
        if tq_symbol not in self.quotes:
            if self.api is None:
                raise RuntimeError("TqApi not connected")
            self.quotes[tq_symbol] = self.api.get_quote(tq_symbol)
        return self.quotes[tq_symbol]

    def latest_quote(self, tq_symbol: str) -> Any | None:
        try:
            return self.ensure_quote(tq_symbol)
        except Exception:
            return None

    def query_contract_candidates(self, exchange_code: str, product_code: str) -> list[dict[str, Any]]:
        if self.api is None:
            return []
        exchange = clean_text(exchange_code).upper()
        product = clean_text(product_code).upper()
        if not exchange or not product:
            return []
        symbols = list(self.api.query_quotes(ins_class="FUTURE", exchange_id=exchange, expired=False))
        self.api.wait_update(deadline=time.time() + 1.0)
        candidates: list[dict[str, Any]] = []
        for tq_symbol in symbols:
            quote = self.ensure_quote(tq_symbol)
            quote_exchange = clean_text(getattr(quote, "exchange_id", "")).upper()
            quote_product = clean_text(getattr(quote, "product_id", "")).upper()
            if quote_exchange != exchange or quote_product != product:
                continue
            candidates.append(
                {
                    "tq_symbol": tq_symbol,
                    "exchange": quote_exchange,
                    "product_code": quote_product,
                    "delivery_year": safe_int(getattr(quote, "delivery_year", 0)),
                    "delivery_month": safe_int(getattr(quote, "delivery_month", 0)),
                    "open_interest": safe_float(getattr(quote, "open_interest", 0)),
                    "volume": safe_float(getattr(quote, "volume", 0)),
                    "last_price": safe_float(getattr(quote, "last_price", 0)),
                    "price_tick": safe_float(getattr(quote, "price_tick", 0)),
                    "volume_multiple": safe_float(getattr(quote, "volume_multiple", 1), 1.0),
                    "expired": bool(getattr(quote, "expired", False)),
                }
            )
        return [item for item in candidates if not item["expired"]]

    def all_orders(self) -> dict[str, Any]:
        if self.orders is None:
            return {}
        return dict(self.orders)

    def get_order(self, order_id: str) -> Any | None:
        if not order_id or self.api is None:
            return None
        try:
            return self.api.get_order(order_id, account=self.trade_account)
        except Exception:
            return None

    def cancel_order(self, order_id: str) -> None:
        if self.api is None or not order_id:
            return
        self.api.cancel_order(order_id, account=self.trade_account)

    def all_trades(self) -> dict[str, Any]:
        if self.trades is None:
            return {}
        return dict(self.trades)

    def get_position(self, tq_symbol: str) -> Any | None:
        if self.api is None:
            return None
        try:
            return self.api.get_position(tq_symbol, account=self.trade_account)
        except Exception:
            return None

    def send_order(self, tq_symbol: str, direction: str, offset: str, volume: int, limit_price: float) -> str:
        if self.api is None:
            raise RuntimeError("TqApi not connected")
        order = self.api.insert_order(
            symbol=tq_symbol,
            direction=direction,
            offset=offset,
            volume=int(volume),
            limit_price=float(limit_price),
            account=self.trade_account,
        )
        self.api.wait_update(deadline=time.time() + 1.0)
        return clean_text(getattr(order, "order_id", ""))


class PushCtpBridge:
    def __init__(self, config: dict[str, Any], logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.base_dir = Path(__file__).resolve().parent
        state_cfg = config.get("state", {})
        self.state = BridgeState(
            resolve_path(self.base_dir, str(state_cfg.get("file", "./state/push_ctp_bridge_state.json"))),
            int(state_cfg.get("max_processed_keys", 4000)),
            int(state_cfg.get("max_trade_records", 4000)),
        )
        self.remote = RemoteFuturesSource(config)
        self.notifier = DingtalkNotifier(config, logger)
        self.trader = TqTrader(config, logger, self.state)
        self.stop_requested = False
        self.seen_trade_ids: set[str] = set()
        self.order_status_cache: dict[str, str] = {}
        self.last_snapshot: dict[str, Any] = {}
        self.last_snapshot_poll_at: float = 0.0

    def shutdown(self) -> None:
        self.state.save()
        self.remote.close()
        self.trader.close()
        self.logger.info("[STOPPED] push_ctp_bridge stopped")

    def _allowed_symbols(self) -> set[str]:
        values = self.config.get("filters", {}).get("allowed_continuous_symbols", [])
        return {clean_text(item).upper() for item in values if clean_text(item)}

    def _sessions(self) -> list[str]:
        return [str(item) for item in self.config.get("filters", {}).get("trading_sessions", [])]

    def _signal_age_limit(self) -> int:
        return int(self.config.get("signal", {}).get("max_signal_age_seconds", 300))

    def _order_cfg(self) -> dict[str, Any]:
        return self.config.get("order", {})

    def _order_percent(self, key: str, default: float) -> float:
        raw = safe_float(self._order_cfg().get(key, default), default)
        if raw > 1:
            raw = raw / 100.0
        return max(0.0, min(raw, 1.0))

    def _strategy_capital_limit(self) -> float:
        snapshot = self.state.runtime_account or self.trader.account_snapshot()
        base = safe_float(snapshot.get("balance", 0))
        return max(base * self._order_percent("strategy_cap_pct", 0.5), 0.0)

    def _single_symbol_cap_limit(self) -> float:
        return self._strategy_capital_limit() * self._order_percent("single_symbol_cap_pct", 0.2)

    def _default_margin_rate(self) -> float:
        raw = safe_float(self._order_cfg().get("default_margin_rate", 0.12), 0.12)
        if raw > 1:
            raw = raw / 100.0
        return max(raw, 0.01)

    def _position_margin_snapshot(self, tq_symbol: str) -> float:
        position = self.trader.get_position(tq_symbol)
        if position is None:
            return 0.0
        for attr in ("margin_long", "margin_short", "margin"):
            margin = safe_float(getattr(position, attr, 0), 0.0)
            if margin > 0:
                return margin
        return 0.0

    def _estimate_margin_per_lot(self, tq_symbol: str, reference_price: float) -> float:
        quote = self.trader.latest_quote(tq_symbol)
        if quote is not None:
            for attr in ("margin_long", "margin_short", "margin"):
                margin = safe_float(getattr(quote, attr, 0), 0.0)
                if margin > 0:
                    return margin
        multiplier = safe_float(getattr(quote, "volume_multiple", 1), 1.0) if quote is not None else 1.0
        if quote is not None and safe_float(getattr(quote, "ask_price1", 0), 0.0) > 0:
            base_price = safe_float(getattr(quote, "ask_price1", 0), 0.0)
        elif quote is not None and safe_float(getattr(quote, "last_price", 0), 0.0) > 0:
            base_price = safe_float(getattr(quote, "last_price", 0), 0.0)
        else:
            base_price = safe_float(reference_price, 0.0)
        if base_price <= 0:
            return 0.0
        return base_price * max(multiplier, 1.0) * self._default_margin_rate()

    def _broker_long_position_stats(self, tq_symbol: str) -> dict[str, float]:
        position = self.trader.get_position(tq_symbol)
        if position is None:
            return {"volume": 0.0, "closable": 0.0}

        long_volume = safe_float(getattr(position, "pos_long", 0), 0.0)
        if long_volume <= 0:
            long_volume = max(safe_float(getattr(position, "pos", 0), 0.0), 0.0)

        frozen = safe_float(getattr(position, "pos_long_frozen", 0), 0.0)
        if frozen <= 0:
            frozen = safe_float(getattr(position, "pos_long_today_frozen", 0), 0.0) + safe_float(getattr(position, "pos_long_his_frozen", 0), 0.0)

        closable = max(long_volume - frozen, 0.0)
        return {"volume": long_volume, "closable": closable}

    def _sync_strategy_positions_with_broker(self) -> None:
        changed = False
        for tq_symbol, payload in list(self.state.strategy_positions.items()):
            broker_stats = self._broker_long_position_stats(tq_symbol)
            broker_volume = safe_float(broker_stats.get("volume", 0), 0.0)
            closable_volume = safe_float(broker_stats.get("closable", 0), 0.0)
            state_volume = safe_float(payload.get("volume", 0), 0.0)

            if broker_volume <= 0:
                self.state.remove_strategy_position(tq_symbol)
                self.logger.warning("[FUTURES_POSITION_DROP] %s state_volume=%s broker_volume=0", tq_symbol, state_volume)
                changed = True
                continue

            patch: dict[str, Any] = {}
            if abs(state_volume - broker_volume) > 1e-6:
                patch["volume"] = round(broker_volume, 4)
                self.logger.info("[FUTURES_POSITION_SYNC] %s state_volume=%s broker_volume=%s closable=%s", tq_symbol, state_volume, broker_volume, closable_volume)
            if abs(safe_float(payload.get("broker_volume", 0), 0.0) - broker_volume) > 1e-6:
                patch["broker_volume"] = round(broker_volume, 4)
            if abs(safe_float(payload.get("closable_volume", 0), 0.0) - closable_volume) > 1e-6:
                patch["closable_volume"] = round(closable_volume, 4)
            if patch:
                self.state.upsert_strategy_position(tq_symbol, {**payload, **patch})
                changed = True

        if changed:
            self.state.save()

    def _strategy_margin_used(self) -> float:
        total = 0.0
        for tq_symbol, position in list((self.state.strategy_positions or {}).items()):
            volume = safe_float(position.get("volume", 0), 0.0)
            if volume <= 0:
                continue
            margin = self._position_margin_snapshot(tq_symbol)
            if margin <= 0:
                margin = safe_float(position.get("estimated_margin", 0), 0.0)
            if margin <= 0:
                entry_price = safe_float(position.get("entry_price", 0), 0.0)
                multiplier = safe_float(position.get("multiplier", 1), 1.0)
                margin = entry_price * volume * max(multiplier, 1.0) * self._default_margin_rate()
            total += max(margin, 0.0)
        return total

    def _symbol_margin_used(self, continuous_symbol: str) -> float:
        total = 0.0
        for tq_symbol, position in list((self.state.strategy_positions or {}).items()):
            if clean_text(position.get("continuous_symbol")).upper() != clean_text(continuous_symbol).upper():
                continue
            volume = safe_float(position.get("volume", 0), 0.0)
            if volume <= 0:
                continue
            margin = self._position_margin_snapshot(tq_symbol)
            if margin <= 0:
                margin = safe_float(position.get("estimated_margin", 0), 0.0)
            if margin <= 0:
                entry_price = safe_float(position.get("entry_price", 0), 0.0)
                multiplier = safe_float(position.get("multiplier", 1), 1.0)
                margin = entry_price * volume * max(multiplier, 1.0) * self._default_margin_rate()
            total += max(margin, 0.0)
        return total

    def _order_volume_for_signal(self, signal: FuturesSignal, tq_symbol: str) -> tuple[int, float]:
        sizing_mode = clean_text(self._order_cfg().get("sizing_mode")).lower()
        if sizing_mode not in {"capital_ratio", "dynamic"}:
            return max(int(self._order_cfg().get("fixed_volume", 1)), 1), 0.0

        per_lot_margin = self._estimate_margin_per_lot(tq_symbol, signal.reference_price)
        if per_lot_margin <= 0:
            return 0, 0.0

        strategy_limit = self._strategy_capital_limit()
        symbol_limit = self._single_symbol_cap_limit()
        remaining_strategy = max(strategy_limit - self._strategy_margin_used(), 0.0)
        remaining_symbol = max(symbol_limit - self._symbol_margin_used(signal.continuous_symbol), 0.0)
        allocatable = min(remaining_strategy, remaining_symbol)
        if allocatable <= 0:
            return 0, per_lot_margin

        volume = int(allocatable // per_lot_margin)
        max_volume = int(self._order_cfg().get("max_volume_per_order", 0))
        if max_volume > 0:
            volume = min(volume, max_volume)
        return max(volume, 0), per_lot_margin

    def _exit_cfg(self) -> dict[str, Any]:
        return self.config.get("exit", {})

    def _resolver_cfg(self) -> dict[str, Any]:
        return self.config.get("contract_resolver", {})

    def _connect_summary(self) -> dict[str, Any]:
        return self.trader.connect_settings_cache or {}

    def _is_market_open(self, current: datetime) -> bool:
        allow_weekdays = {int(value) for value in self.config.get("filters", {}).get("allow_weekdays", [0, 1, 2, 3, 4])}
        if current.weekday() not in allow_weekdays:
            return False
        return within_sessions(current, self._sessions())

    def _build_signal_rows(self, snapshot: dict[str, Any]) -> list[FuturesSignal]:
        rows: list[FuturesSignal] = []
        for raw in snapshot.get("events", []) or []:
            if not isinstance(raw, dict):
                continue
            signal = FuturesSignal.from_event_row(raw)
            if signal is not None:
                rows.append(signal)
        rows.sort(key=lambda item: item.signal_time)
        return rows

    def _bootstrap(self, signals: list[FuturesSignal]) -> None:
        if self.state.bootstrap_complete:
            return
        if bool(self.config.get("signal", {}).get("bootstrap_skip_existing", True)):
            for signal in signals:
                self.state.mark_processed(signal.key)
            self.logger.info("[BOOTSTRAP] seeded=%s historical futures events", len(signals))
        self.state.bootstrap_complete = True
        self.state.save()

    def _notify(self, title: str, lines: list[str], rate_limit_key: str = "", min_interval_seconds: int = 0) -> None:
        self.notifier.send(title, lines, rate_limit_key=rate_limit_key, min_interval_seconds=min_interval_seconds)

    def _manual_mapping(self, continuous_symbol: str) -> str:
        mapping = self.config.get("contract_mapping", {}).get(continuous_symbol, {})
        tq_symbol = clean_text(mapping.get("tq_symbol"))
        if tq_symbol:
            return tq_symbol
        raw_symbol = clean_text(mapping.get("symbol"))
        exchange = clean_text(mapping.get("exchange"))
        if raw_symbol and exchange:
            return normalize_tq_symbol(raw_symbol, exchange)
        return ""

    def _resolved_contract(self, signal: FuturesSignal) -> dict[str, Any] | None:
        cached = self.state.resolved_contracts.get(signal.continuous_symbol)
        cache_ttl_seconds = int(self._resolver_cfg().get("cache_ttl_seconds", 1800))
        if cached:
            resolved_at = parse_dt(clean_text(cached.get("resolved_at")))
            if resolved_at and (now_cn() - resolved_at).total_seconds() <= cache_ttl_seconds:
                return dict(cached)

        manual = self._manual_mapping(signal.continuous_symbol)
        if manual and self.trader.is_ready():
            quote = self.trader.latest_quote(manual)
            if quote is not None and not bool(getattr(quote, "expired", False)):
                payload = {
                    "tq_symbol": manual,
                    "exchange": clean_text(getattr(quote, "exchange_id", "")),
                    "product_code": clean_text(getattr(quote, "product_id", "")).upper(),
                    "delivery_year": safe_int(getattr(quote, "delivery_year", 0)),
                    "delivery_month": safe_int(getattr(quote, "delivery_month", 0)),
                    "open_interest": safe_float(getattr(quote, "open_interest", 0)),
                    "volume": safe_float(getattr(quote, "volume", 0)),
                    "resolved_at": format_dt(now_cn()),
                    "method": "manual",
                }
                self.state.upsert_resolved_contract(signal.continuous_symbol, payload)
                self.state.save()
                return payload

        if not self.trader.is_ready():
            return None

        candidates = self.trader.query_contract_candidates(signal.exchange_code, signal.product_code)
        if not candidates:
            self.state.last_error = f"contract_not_found:{signal.continuous_symbol}:{signal.exchange_code}:{signal.product_code}"
            self.state.save()
            return None

        selection = clean_text(self._resolver_cfg().get("selection")).lower() or "open_interest"
        if selection == "nearest":
            candidates.sort(
                key=lambda item: (
                    item["delivery_year"] or 0,
                    item["delivery_month"] or 0,
                    -item["open_interest"],
                    -item["volume"],
                    item["tq_symbol"],
                )
            )
        else:
            candidates.sort(
                key=lambda item: (
                    -item["open_interest"],
                    -item["volume"],
                    item["delivery_year"] or 0,
                    item["delivery_month"] or 0,
                    item["tq_symbol"],
                )
            )
        chosen = dict(candidates[0])
        chosen["resolved_at"] = format_dt(now_cn())
        chosen["method"] = f"dynamic_{selection}"
        self.state.upsert_resolved_contract(signal.continuous_symbol, chosen)
        self.state.save()
        self.logger.info(
            "[FUTURES_MAPPING] %s -> %s oi=%.0f vol=%.0f delivery=%s-%02d method=%s",
            signal.continuous_symbol,
            chosen["tq_symbol"],
            safe_float(chosen.get("open_interest")),
            safe_float(chosen.get("volume")),
            safe_int(chosen.get("delivery_year")),
            safe_int(chosen.get("delivery_month")),
            chosen["method"],
        )
        return chosen

    def _reference_order_price(self, tq_symbol: str, reference_price: float, direction: str) -> float:
        quote = self.trader.latest_quote(tq_symbol)
        tick = safe_float(getattr(quote, "price_tick", 0), 0.0) if quote is not None else 0.0
        offset_ticks = int(self._order_cfg().get("price_offset_ticks", 1))

        if direction == "BUY":
            if quote is not None and safe_float(getattr(quote, "ask_price1", 0)) > 0:
                base = safe_float(getattr(quote, "ask_price1", 0))
            elif quote is not None and safe_float(getattr(quote, "last_price", 0)) > 0:
                base = safe_float(getattr(quote, "last_price", 0))
            else:
                base = float(reference_price)
            return round_to_tick(base + tick * offset_ticks, tick or 0.1)

        if quote is not None and safe_float(getattr(quote, "bid_price1", 0)) > 0:
            base = safe_float(getattr(quote, "bid_price1", 0))
        elif quote is not None and safe_float(getattr(quote, "last_price", 0)) > 0:
            base = safe_float(getattr(quote, "last_price", 0))
        else:
            base = float(reference_price)
        return round_to_tick(max(base - tick * offset_ticks, tick or 0.1), tick or 0.1)

    def _handle_signal(self, signal: FuturesSignal) -> None:
        current = now_cn()
        if not self._is_market_open(current):
            self.logger.info("[FUTURES_SKIP] %s: market closed", signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            return

        signal_dt = parse_dt(signal.signal_time)
        if signal_dt and (current - signal_dt).total_seconds() > self._signal_age_limit():
            self.logger.info("[FUTURES_SKIP] %s: signal too old %s", signal.continuous_symbol, signal.signal_time)
            self.state.mark_processed(signal.key)
            return

        allowed = self._allowed_symbols()
        if allowed and signal.continuous_symbol not in allowed:
            self.logger.info("[FUTURES_SKIP] %s: not in whitelist", signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            return

        daily_count = self.state.order_count(signal.trading_day, signal.continuous_symbol)
        daily_limit = int(self._order_cfg().get("max_signals_per_symbol_per_day", 3))
        if daily_limit > 0 and daily_count >= daily_limit:
            self.logger.info("[FUTURES_SKIP] %s: symbol daily limit reached", signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            return

        total_today = sum(int(value) for value in self.state.ordered_symbols_by_day.get(signal.trading_day, {}).values())
        daily_order_limit = int(self._order_cfg().get("daily_order_limit", 10))
        if daily_order_limit > 0 and total_today >= daily_order_limit:
            self.logger.info("[FUTURES_SKIP] %s: global daily limit reached", signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            return

        max_positions = int(self._order_cfg().get("max_positions", 3))
        if max_positions > 0 and len(self.state.strategy_positions) >= max_positions:
            self.logger.info("[FUTURES_SKIP] %s: max positions reached", signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            return

        if not self.trader.is_ready():
            self.logger.info("[FUTURES_DEFER] %s: tq not ready, keep pending", signal.continuous_symbol)
            return

        resolved = self._resolved_contract(signal)
        if resolved is None:
            self.logger.info("[FUTURES_DEFER] %s: contract not resolved yet", signal.continuous_symbol)
            return

        tq_symbol = clean_text(resolved.get("tq_symbol"))
        if self._order_cfg().get("skip_if_has_position", True) and self.state.get_strategy_position(tq_symbol):
            self.logger.info("[FUTURES_SKIP] %s: strategy position exists", tq_symbol)
            self.state.mark_processed(signal.key)
            return

        price = self._reference_order_price(tq_symbol, signal.reference_price, "BUY")
        volume, per_lot_margin = self._order_volume_for_signal(signal, tq_symbol)
        if volume <= 0:
            self.logger.info(
                "[FUTURES_SKIP] %s: capital limit reached strategy_limit=%.2f strategy_used=%.2f symbol_limit=%.2f symbol_used=%.2f per_lot_margin=%.2f",
                signal.continuous_symbol,
                self._strategy_capital_limit(),
                self._strategy_margin_used(),
                self._single_symbol_cap_limit(),
                self._symbol_margin_used(signal.continuous_symbol),
                per_lot_margin,
            )
            self.state.mark_processed(signal.key)
            return

        if not bool(self._order_cfg().get("enabled", False)):
            self.logger.info(
                "[FUTURES_ORDER] dry-run open %s -> %s volume=%s price=%.4f per_lot_margin=%.2f signal=%s",
                signal.continuous_symbol,
                tq_symbol,
                volume,
                price,
                per_lot_margin,
                signal.signal_time,
            )
            self._notify(
                "期货演练开仓",
                [
                    f"连续代码: {signal.continuous_symbol}",
                    f"下单合约: {tq_symbol}",
                    f"价格: {price}",
                    f"手数: {volume}",
                    f"时间: {signal.signal_time}",
                ],
            )
            self.state.mark_order(signal.trading_day, signal.continuous_symbol)
            self.state.mark_processed(signal.key)
            self.state.save()
            return

        order_id = self.trader.send_order(
            tq_symbol=tq_symbol,
            direction="BUY",
            offset="OPEN",
            volume=volume,
            limit_price=price,
        )
        self.logger.info(
            "[FUTURES_ORDER] open %s -> %s volume=%s price=%.4f per_lot_margin=%.2f orderid=%s signal=%s",
            signal.continuous_symbol,
            tq_symbol,
            volume,
            price,
            per_lot_margin,
            order_id or "-",
            signal.signal_time,
        )
        self._notify(
            "期货开仓委托",
            [
                f"连续代码: {signal.continuous_symbol}",
                f"下单合约: {tq_symbol}",
                f"价格: {price}",
                f"手数: {volume}",
                f"委托号: {order_id or '-'}",
                f"时间: {signal.signal_time}",
            ],
            rate_limit_key=f"futures_open_order:{tq_symbol}",
            min_interval_seconds=60,
        )
        self.state.stash_pending_open_meta(
            tq_symbol,
            {
                "order_id": order_id or "",
                "continuous_symbol": signal.continuous_symbol,
                "exchange_name": signal.exchange_name,
                "product_name": signal.product_name,
                "signal_time": signal.signal_time,
                "pattern_time": first_present(signal.raw, "C_time"),
                "scan_time": first_present(signal.raw, "scan_time", signal.signal_time),
                "reference_price": signal.reference_price,
                "request_price": price,
                "per_lot_margin": round(per_lot_margin, 2),
                "submitted_at": format_dt(now_cn()),
            },
        )
        self.state.mark_order(signal.trading_day, signal.continuous_symbol)
        self.state.mark_processed(signal.key)
        self.state.save()

    def _reconcile_orders(self) -> None:
        for order_id, order in self.trader.all_orders().items():
            status_key = "|".join(
                [
                    clean_text(getattr(order, "status", "")),
                    clean_text(getattr(order, "volume_left", "")),
                    clean_text(getattr(order, "last_msg", "")),
                ]
            )
            if self.order_status_cache.get(order_id) != status_key:
                self.order_status_cache[order_id] = status_key
                self.logger.info(
                    "[FUTURES_ORDER_EVENT] %s status=%s left=%s msg=%s",
                    order_id,
                    clean_text(getattr(order, "status", "")),
                    safe_float(getattr(order, "volume_left", 0)),
                    clean_text(getattr(order, "last_msg", "")) or "-",
                )

            for tq_symbol, payload in list(self.state.strategy_positions.items()):
                if payload.get("pending_exit_orderid") != order_id:
                    continue
                if bool(getattr(order, "is_dead", False)) and safe_float(getattr(order, "volume_left", 0)) > 0:
                    self.logger.info(
                        "[FUTURES_EXIT_PENDING] %s order=%s status=%s left=%s",
                        tq_symbol,
                        order_id,
                        clean_text(getattr(order, "status", "")),
                        safe_float(getattr(order, "volume_left", 0)),
                    )

    def _continuous_symbol_for_tq_symbol(self, tq_symbol: str) -> str:
        for continuous_symbol, row in (self.state.resolved_contracts or {}).items():
            if not isinstance(row, dict):
                continue
            if clean_text(row.get("tq_symbol")) == tq_symbol:
                return continuous_symbol
        return ""

    def _reconcile_trades(self) -> None:
        for trade_id, trade in self.trader.all_trades().items():
            if trade_id in self.seen_trade_ids:
                continue
            self.seen_trade_ids.add(trade_id)

            tq_symbol = f"{clean_text(getattr(trade, 'exchange_id', ''))}.{clean_text(getattr(trade, 'instrument_id', ''))}"
            direction = clean_text(getattr(trade, "direction", ""))
            offset = clean_text(getattr(trade, "offset", ""))
            volume = safe_float(getattr(trade, "volume", 0))
            price = safe_float(getattr(trade, "price", 0))
            quote = self.trader.latest_quote(tq_symbol)
            multiplier = safe_float(getattr(quote, "volume_multiple", 1), 1.0) if quote is not None else 1.0

            self.logger.info(
                "[FUTURES_TRADE_EVENT] %s %s %s volume=%s price=%.4f",
                trade_id,
                tq_symbol,
                direction,
                volume,
                price,
            )

            existing = self.state.get_strategy_position(tq_symbol) or {}
            pending_open_meta = self.state.pop_pending_open_meta(tq_symbol) if direction == "BUY" and offset == "OPEN" else {}
            record = {
                "trade_id": trade_id,
                "tq_symbol": tq_symbol,
                "direction": direction,
                "offset": offset,
                "volume": volume,
                "price": price,
                "timestamp": format_dt(now_cn()),
            }

            if direction == "BUY" and offset == "OPEN":
                if pending_open_meta:
                    record.update(
                        {
                            "continuous_symbol": clean_text(pending_open_meta.get("continuous_symbol")),
                            "product": clean_text(pending_open_meta.get("product_name")),
                            "exchange": clean_text(pending_open_meta.get("exchange_name")),
                            "signal_time": clean_text(pending_open_meta.get("signal_time")),
                            "pattern_time": clean_text(pending_open_meta.get("pattern_time")),
                            "scan_time": clean_text(pending_open_meta.get("scan_time")),
                            "reference_price": safe_float(pending_open_meta.get("reference_price"), 0.0),
                            "request_price": safe_float(pending_open_meta.get("request_price"), 0.0),
                            "per_lot_margin": safe_float(pending_open_meta.get("per_lot_margin"), 0.0),
                            "submitted_at": clean_text(pending_open_meta.get("submitted_at")),
                        }
                    )
                new_volume = volume + safe_float(existing.get("volume", 0))
                old_cost = safe_float(existing.get("entry_price", 0)) * safe_float(existing.get("volume", 0))
                new_cost = price * volume
                avg_price = (old_cost + new_cost) / new_volume if new_volume > 0 else price
                live_margin = self._position_margin_snapshot(tq_symbol)
                if live_margin <= 0:
                    live_margin = self._estimate_margin_per_lot(tq_symbol, avg_price) * new_volume
                position_payload = {
                    "vt_symbol": tq_symbol,
                    "tq_symbol": tq_symbol,
                    "volume": new_volume,
                    "entry_price": round(avg_price, 4),
                    "entry_trade_id": trade_id,
                    "entry_time": existing.get("entry_time") or format_dt(now_cn()),
                    "planned_exit_at": format_dt(now_cn() + timedelta(seconds=int(self._exit_cfg().get("max_hold_seconds", 90)))),
                    "pending_exit_orderid": "",
                    "pending_exit_reason": "",
                    "pending_exit_requested_at": "",
                    "continuous_symbol": clean_text(pending_open_meta.get("continuous_symbol")) or existing.get("continuous_symbol") or self._continuous_symbol_for_tq_symbol(tq_symbol),
                    "exchange": clean_text(pending_open_meta.get("exchange_name")) or clean_text(existing.get("exchange")),
                    "product": clean_text(pending_open_meta.get("product_name")) or clean_text(existing.get("product")),
                    "signal_time": clean_text(pending_open_meta.get("signal_time")) or clean_text(existing.get("signal_time")),
                    "pattern_time": clean_text(pending_open_meta.get("pattern_time")) or clean_text(existing.get("pattern_time")),
                    "scan_time": clean_text(pending_open_meta.get("scan_time")) or clean_text(existing.get("scan_time")),
                    "reference_price": safe_float(pending_open_meta.get("reference_price"), safe_float(existing.get("reference_price"), 0.0)),
                    "request_price": safe_float(pending_open_meta.get("request_price"), safe_float(existing.get("request_price"), 0.0)),
                    "multiplier": multiplier,
                    "estimated_margin": round(live_margin, 2),
                }
                self.state.upsert_strategy_position(tq_symbol, position_payload)
                self.logger.info(
                    "[FUTURES_TRADE] open %s volume=%s price=%.4f exit_at=%s",
                    tq_symbol,
                    new_volume,
                    avg_price,
                    position_payload["planned_exit_at"],
                )
                self._notify(
                    "期货开仓成交",
                    [
                        f"合约: {tq_symbol}",
                        f"价格: {avg_price:.4f}",
                        f"手数: {new_volume}",
                        f"计划平仓: {position_payload['planned_exit_at']}",
                    ],
                    rate_limit_key=f"futures_open_trade:{tq_symbol}",
                    min_interval_seconds=60,
                )
            elif direction == "SELL":
                current_volume = safe_float(existing.get("volume", volume))
                remaining_volume = max(current_volume - volume, 0.0)
                entry_price = safe_float(existing.get("entry_price", 0))
                realized_pnl = (price - entry_price) * volume * multiplier if entry_price > 0 else 0.0
                record["realized_pnl"] = round(realized_pnl, 4)
                if remaining_volume > 0:
                    existing["volume"] = remaining_volume
                    live_margin = self._position_margin_snapshot(tq_symbol)
                    if live_margin > 0:
                        existing["estimated_margin"] = round(live_margin, 2)
                    elif current_volume > 0 and safe_float(existing.get("estimated_margin", 0), 0.0) > 0:
                        existing["estimated_margin"] = round(
                            safe_float(existing.get("estimated_margin", 0), 0.0) * (remaining_volume / current_volume),
                            2,
                        )
                    self.state.upsert_strategy_position(tq_symbol, existing)
                else:
                    self.state.remove_strategy_position(tq_symbol)
                self.logger.info(
                    "[FUTURES_TRADE] close %s volume=%s price=%.4f pnl=%.4f",
                    tq_symbol,
                    volume,
                    price,
                    realized_pnl,
                )
                self._notify(
                    "期货平仓成交",
                    [
                        f"合约: {tq_symbol}",
                        f"价格: {price:.4f}",
                        f"手数: {volume}",
                        f"本次盈亏: {realized_pnl:.4f}",
                    ],
                    rate_limit_key=f"futures_close_trade:{tq_symbol}",
                    min_interval_seconds=60,
                )

            self.state.add_trade_record(record)
            self.state.save()

    def _evaluate_exits(self) -> None:
        if not bool(self._exit_cfg().get("enabled", True)) or not self.trader.is_ready():
            return

        current = now_cn()
        for tq_symbol, position in list(self.state.strategy_positions.items()):
            broker_stats = self._broker_long_position_stats(tq_symbol)
            broker_volume = safe_float(broker_stats.get("volume", 0), 0.0)
            closable_volume = safe_float(broker_stats.get("closable", 0), 0.0)
            if broker_volume <= 0:
                self.state.remove_strategy_position(tq_symbol)
                self.state.save()
                self.logger.warning("[FUTURES_EXIT_SKIP] %s dropped because broker volume is 0", tq_symbol)
                continue

            volume = min(safe_float(position.get("volume", 0), 0.0), broker_volume)
            if volume <= 0:
                continue

            pending_exit_orderid = clean_text(position.get("pending_exit_orderid"))
            pending_requested_at = parse_dt(clean_text(position.get("pending_exit_requested_at")))
            pending_retry_seconds = int(self._exit_cfg().get("pending_exit_retry_seconds", 20))

            if pending_exit_orderid:
                order = self.trader.get_order(pending_exit_orderid)
                elapsed = (current - pending_requested_at).total_seconds() if pending_requested_at else 0.0
                if order is not None and not bool(getattr(order, "is_dead", False)):
                    if pending_requested_at and elapsed >= pending_retry_seconds:
                        self.trader.cancel_order(pending_exit_orderid)
                        self.logger.info("[FUTURES_CANCEL] %s order=%s reason=retry_timeout", tq_symbol, pending_exit_orderid)
                    continue
                if elapsed < pending_retry_seconds:
                    continue
                if closable_volume <= 0:
                    self.logger.info(
                        "[FUTURES_EXIT_WAIT] %s order=%s reason=pending_reconcile broker_volume=%s closable=%s",
                        tq_symbol,
                        pending_exit_orderid,
                        broker_volume,
                        closable_volume,
                    )
                    continue
                self._clear_exit_tracking(position)
                self.state.upsert_strategy_position(tq_symbol, position)
                self.state.save()

            quote = self.trader.latest_quote(tq_symbol)
            mark_price = safe_float(getattr(quote, "last_price", 0)) if quote is not None else 0.0
            entry_price = safe_float(position.get("entry_price", 0))
            planned_exit_at = parse_dt(clean_text(position.get("planned_exit_at")))
            stop_loss_pct = safe_float(self._exit_cfg().get("stop_loss_pct", 0))
            take_profit_pct = safe_float(self._exit_cfg().get("take_profit_pct", 0))

            should_exit = False
            exit_reason = ""
            if planned_exit_at and current >= planned_exit_at:
                should_exit = True
                exit_reason = "time_exit"
            elif mark_price > 0 and entry_price > 0 and stop_loss_pct > 0 and mark_price <= entry_price * (1 - stop_loss_pct):
                should_exit = True
                exit_reason = "stop_loss"
            elif mark_price > 0 and entry_price > 0 and take_profit_pct > 0 and mark_price >= entry_price * (1 + take_profit_pct):
                should_exit = True
                exit_reason = "take_profit"

            if not should_exit:
                continue

            volume = min(volume, closable_volume if closable_volume > 0 else broker_volume)
            if volume <= 0:
                self.logger.info("[FUTURES_EXIT_SKIP] %s reason=%s closable=0 broker_volume=%s", tq_symbol, exit_reason, broker_volume)
                continue
            price = self._reference_order_price(tq_symbol, mark_price or entry_price, "SELL")
            if not bool(self._order_cfg().get("enabled", False)):
                self.logger.info("[FUTURES_ORDER] dry-run close %s reason=%s volume=%s price=%.4f", tq_symbol, exit_reason, volume, price)
                continue

            order_id = self.trader.send_order(
                tq_symbol=tq_symbol,
                direction="SELL",
                offset="CLOSE",
                volume=int(volume),
                limit_price=price,
            )
            position["pending_exit_orderid"] = order_id or "-"
            position["pending_exit_reason"] = exit_reason
            position["pending_exit_requested_at"] = format_dt(current)
            self.state.upsert_strategy_position(tq_symbol, position)
            self.state.save()
            self.logger.info("[FUTURES_ORDER] close %s reason=%s volume=%s price=%.4f orderid=%s", tq_symbol, exit_reason, volume, price, order_id or "-")
            self._notify(
                "期货触发平仓",
                [
                    f"合约: {tq_symbol}",
                    f"原因: {exit_reason}",
                    f"价格: {price}",
                    f"手数: {volume}",
                    f"委托号: {order_id or '-'}",
                ],
                rate_limit_key=f"futures_exit:{tq_symbol}:{exit_reason}",
                min_interval_seconds=300,
            )

    def _handle_snapshot(self, snapshot: dict[str, Any]) -> None:
        signals = self._build_signal_rows(snapshot)
        self.last_snapshot = snapshot
        self._bootstrap(signals)
        for signal in signals:
            if self.state.has_processed(signal.key):
                continue
            self._handle_signal(signal)
        self.state.save()

    def run(self) -> None:
        startup_ready = self.trader.connect()
        if startup_ready:
            self._notify(
                "期货桥接启动",
                [
                    f"时间: {format_dt(now_cn())}",
                    f"下单开关: {'开启' if bool(self._order_cfg().get('enabled', False)) else '关闭'}",
                    f"白名单数量: {len(self._allowed_symbols())}",
                    f"执行模式: {clean_text(self._connect_summary().get('account_mode')) or 'TqKq'}",
                ],
                rate_limit_key="futures_startup",
                min_interval_seconds=30,
            )
        else:
            self.logger.warning("[TQ_STANDBY] startup connect unavailable, bridge will stay alive and retry")
            self._notify(
                "期货桥接待命",
                [
                    f"时间: {format_dt(now_cn())}",
                    "TqSdk 当前未就绪，服务保持常驻并按重连间隔自动重试。",
                    f"最近错误: {self.state.last_error or '-'}",
                ],
                rate_limit_key="futures_startup_waiting",
                min_interval_seconds=600,
            )

        poll_interval = float(self.config.get("remote", {}).get("poll_interval_seconds", 5))
        update_timeout = float(self.config.get("tqsdk", {}).get("update_timeout_seconds", 0.3))

        while not self.stop_requested:
            try:
                if not self.trader.is_ready():
                    if self.trader.connect():
                        self._notify(
                            "期货TqKq已就绪",
                            [
                                f"时间: {format_dt(now_cn())}",
                                f"执行模式: {clean_text(self._connect_summary().get('account_mode')) or 'TqKq'}",
                                "期货模拟交易已恢复连接，开始接收并执行 117 期货信号。",
                            ],
                            rate_limit_key="futures_tq_ready",
                            min_interval_seconds=60,
                        )
                    else:
                        time.sleep(min(poll_interval, 5))
                        continue

                self.trader.pump(update_timeout)
                self._reconcile_orders()
                self._reconcile_trades()
                self._sync_strategy_positions_with_broker()
                self._evaluate_exits()

                if time.time() - self.last_snapshot_poll_at >= poll_interval:
                    snapshot = self.remote.fetch_snapshot()
                    self._handle_snapshot(snapshot)
                    self.last_snapshot_poll_at = time.time()
            except KeyboardInterrupt:
                break
            except Exception:
                self.logger.exception("[LOOP_ERROR] futures bridge loop failed")
                self._notify(
                    "期货桥接异常",
                    [
                        f"时间: {format_dt(now_cn())}",
                        "主循环发生异常，请检查 push_ctp_bridge.log",
                    ],
                    rate_limit_key="futures_loop_error",
                    min_interval_seconds=self.notifier.loop_error_cooldown_seconds,
                )
            time.sleep(0.5)

        self.shutdown()


def build_logger(config: dict[str, Any], base_dir: Path) -> logging.Logger:
    logger = logging.getLogger("push_ctp_bridge")
    logger.setLevel(getattr(logging, clean_text(config.get("logging", {}).get("level", "INFO")).upper(), logging.INFO))
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    log_path = resolve_path(base_dir, str(config.get("logging", {}).get("file", "./log/push_ctp_bridge.log")))
    ensure_parent(log_path)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger


def load_config(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Push futures signal bridge for TqSdk/TqKq")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).with_name("push_ctp_bridge.config.json")),
        help="Path to config file",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config_path = Path(args.config).resolve()
    config = load_config(config_path)
    base_dir = config_path.parent
    logger = build_logger(config, base_dir)
    logger.info("[STARTING] config=%s masked=%s", config_path, json.dumps(mask_secret(config), ensure_ascii=False))
    bridge = PushCtpBridge(config, logger)
    bridge.run()


if __name__ == "__main__":
    main()
