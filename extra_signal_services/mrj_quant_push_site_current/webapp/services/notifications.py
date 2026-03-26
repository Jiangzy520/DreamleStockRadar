from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


DEFAULT_NOTIFICATION_EVENT_META = {
    "stock_signal": "股票信号",
    "futures_signal": "期货信号",
    "stock_trade_signal": "股票交易信号",
    "futures_trade_signal": "期货交易信号",
}

DEFAULT_NOTIFICATION_CHANNEL_META = {
    "feishu": "飞书",
    "dingtalk": "钉钉",
    "wecom": "企业微信",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


@dataclass(slots=True)
class NotificationService:
    config_path: Path
    event_meta: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_NOTIFICATION_EVENT_META))
    channel_meta: dict[str, str] = field(default_factory=lambda: dict(DEFAULT_NOTIFICATION_CHANNEL_META))

    def default_config(self) -> dict[str, Any]:
        return {
            "updated_at": "",
            "channels": {
                channel_id: {
                    "enabled": False,
                    "webhook": "",
                    "events": list(self.event_meta.keys()),
                }
                for channel_id in self.channel_meta
            },
        }

    def normalize_events(self, values: Any) -> list[str]:
        if not isinstance(values, list):
            return []
        seen: set[str] = set()
        normalized: list[str] = []
        for value in values:
            text = str(value or "").strip()
            if text in self.event_meta and text not in seen:
                seen.add(text)
                normalized.append(text)
        return normalized

    def normalize_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        base = self.default_config()
        channels = payload.get("channels", {}) if isinstance(payload, dict) else {}
        for channel_id in self.channel_meta:
            row = channels.get(channel_id, {}) if isinstance(channels, dict) else {}
            base["channels"][channel_id] = {
                "enabled": bool(row.get("enabled", False)),
                "webhook": str(row.get("webhook") or "").strip(),
                "events": self.normalize_events(row.get("events")) or list(self.event_meta.keys()),
            }
        base["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return base

    def load_config(self) -> dict[str, Any]:
        raw = _read_json(self.config_path)
        if not raw:
            return self.default_config()
        return self.normalize_config(raw)

    def save_config(self, config: dict[str, Any]) -> dict[str, Any]:
        normalized = self.normalize_config(config)
        _write_json(self.config_path, normalized)
        return normalized

    def event_label(self, event_type: str) -> str:
        return self.event_meta.get(event_type, event_type or "未命名事件")

    def build_text(
        self,
        event_type: str,
        title: str,
        message: str,
        lines: list[str] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> str:
        text_lines = [title or self.event_label(event_type), f"事件类型：{self.event_label(event_type)}"]
        if message:
            text_lines.append(message)
        for line in lines or []:
            clean = str(line or "").strip()
            if clean:
                text_lines.append(clean)
        if isinstance(payload, dict):
            for key, value in payload.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    text_lines.append(f"{key}: {value}")
        return "\n".join(text_lines)

    def _post_json(self, url: str, body: dict[str, Any], timeout_seconds: float = 6.0) -> tuple[bool, str]:
        req = Request(
            url,
            data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8", errors="replace")
            return True, raw
        except Exception as exc:
            return False, str(exc)

    def _send_feishu(self, webhook: str, content: str) -> dict[str, Any]:
        ok, raw = self._post_json(webhook, {"msg_type": "text", "content": {"text": content}})
        if not ok:
            return {"ok": False, "raw": raw}
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        status_code = int(data.get("StatusCode", data.get("code", 0)) or 0) if isinstance(data, dict) else 0
        return {"ok": status_code == 0, "raw": raw}

    def _send_dingtalk(self, webhook: str, content: str) -> dict[str, Any]:
        ok, raw = self._post_json(webhook, {"msgtype": "text", "text": {"content": content}})
        if not ok:
            return {"ok": False, "raw": raw}
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        errcode = int(data.get("errcode", 0) or 0) if isinstance(data, dict) else 0
        return {"ok": errcode == 0, "raw": raw}

    def _send_wecom(self, webhook: str, content: str) -> dict[str, Any]:
        ok, raw = self._post_json(webhook, {"msgtype": "text", "text": {"content": content}})
        if not ok:
            return {"ok": False, "raw": raw}
        try:
            data = json.loads(raw or "{}")
        except Exception:
            data = {}
        errcode = int(data.get("errcode", 0) or 0) if isinstance(data, dict) else 0
        return {"ok": errcode == 0, "raw": raw}

    def dispatch_event(
        self,
        *,
        event_type: str,
        title: str,
        message: str,
        lines: list[str] | None = None,
        payload: dict[str, Any] | None = None,
        target_channel: str = "enabled",
    ) -> dict[str, Any]:
        config = self.load_config()
        text = self.build_text(event_type, title, message, lines, payload)
        results: list[dict[str, Any]] = []
        delivered_count = 0

        for channel_id, label in self.channel_meta.items():
            channel_cfg = (config.get("channels") or {}).get(channel_id, {})
            enabled = bool(channel_cfg.get("enabled", False))
            webhook = str(channel_cfg.get("webhook") or "").strip()
            events = self.normalize_events(channel_cfg.get("events"))

            if target_channel not in {"enabled", "", channel_id}:
                continue
            if target_channel in {"enabled", ""} and (not enabled or event_type not in events):
                continue
            if not webhook:
                results.append({"channel": channel_id, "label": label, "ok": False, "reason": "未配置 webhook"})
                continue

            if channel_id == "feishu":
                result = self._send_feishu(webhook, text)
            elif channel_id == "dingtalk":
                result = self._send_dingtalk(webhook, text)
            else:
                result = self._send_wecom(webhook, text)

            results.append(
                {
                    "channel": channel_id,
                    "label": label,
                    "ok": bool(result.get("ok")),
                    "raw": result.get("raw", ""),
                }
            )
            if result.get("ok"):
                delivered_count += 1

        return {
            "event_type": event_type,
            "event_label": self.event_label(event_type),
            "delivered_count": delivered_count,
            "results": results,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
