from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path
from types import ModuleType
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[4]
DASHBOARD_APP_PATH = REPO_ROOT / "push_xtp_bridge" / "dashboard_app.py"


@lru_cache(maxsize=1)
def _load_dashboard_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("stock_futures_public_dashboard_app", DASHBOARD_APP_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载桥接面板模块: {DASHBOARD_APP_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def build_stock_bridge_payload() -> dict[str, Any]:
    return _load_dashboard_module().build_dashboard_payload()


def build_futures_bridge_payload() -> dict[str, Any]:
    return _load_dashboard_module().build_futures_payload()


def build_futures_bridge_payload_v2() -> dict[str, Any]:
    return _load_dashboard_module().build_futures_payload_v2()


def get_stock_bridge_template() -> str:
    return _load_dashboard_module().HTML_TEMPLATE


def get_futures_bridge_template() -> str:
    return _load_dashboard_module().FUTURES_HTML_TEMPLATE


def get_notifications_template() -> str:
    return _load_dashboard_module().NOTIFICATIONS_HTML_TEMPLATE
