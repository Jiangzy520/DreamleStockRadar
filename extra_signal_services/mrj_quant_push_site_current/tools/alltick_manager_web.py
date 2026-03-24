#!/usr/bin/env python3
from __future__ import annotations

import argparse
import atexit
import csv
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
import webbrowser
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import urlopen


PROJECT_ROOT = Path(__file__).resolve().parent.parent
ASSET_PATH = PROJECT_ROOT / "tools" / "alltick_manager_web_assets" / "index.html"
DATA_DIR = PROJECT_ROOT / ".guanlan" / "alltick_manager"
API_FILE = DATA_DIR / "apis.txt"
STOCK_FILE = DATA_DIR / "watchlist.csv"
SETTINGS_FILE = DATA_DIR / "settings.json"
API_ASSIGNMENT_FILE = DATA_DIR / "api_assignments.csv"
STOCK_ASSIGNMENT_FILE = DATA_DIR / "stock_assignments.csv"
WATCHLIST_EXPORT_FILE = DATA_DIR / "watchlist.txt"
RUNTIME_FILE = DATA_DIR / "runtime.json"
LOG_FILE = DATA_DIR / "server.log"

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8765
DEFAULT_SETTINGS = {
    "distribution_mode": "balanced",
    "max_stocks_per_api": 2,
}


@dataclass(frozen=True)
class StockItem:
    code: str
    symbol: str
    name: str


def infer_suffix(code: str) -> str:
    if code.startswith(("5", "6", "9")):
        return "SH"
    if code.startswith(("0", "2", "3")):
        return "SZ"
    if code.startswith(("4", "8")):
        return "BJ"
    raise ValueError(f"Unsupported stock code: {code}")


def normalize_symbol(raw: str) -> str:
    text = raw.strip().upper()
    if not text:
        raise ValueError("Empty symbol")

    if "." in text:
        code, suffix = text.split(".", 1)
        if len(code) == 6 and code.isdigit() and suffix in {"SH", "SZ", "BJ"}:
            return f"{code}.{suffix}"

    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) != 6:
        raise ValueError(f"Unsupported symbol format: {raw}")
    return f"{digits}.{infer_suffix(digits)}"


def parse_api_text(text: str) -> list[str]:
    values: list[str] = []
    for part in re.split(r"[\s,]+", text):
        item = part.strip()
        if item:
            values.append(item)
    return values


def parse_stock_line(raw: str) -> StockItem | None:
    text = raw.strip()
    if not text or text.startswith("#"):
        return None

    lowered = text.lower().replace(" ", "")
    if lowered in {"code,name", "code,symbol,name", "symbol,name"}:
        return None

    if "," in text or "\t" in text:
        parts = [part.strip() for part in re.split(r"[,\t]", text) if part.strip()]
    else:
        parts = text.split(None, 1)

    if not parts:
        return None

    symbol = normalize_symbol(parts[0])
    code = symbol.split(".", 1)[0]
    name = parts[1].strip() if len(parts) > 1 else code
    return StockItem(code=code, symbol=symbol, name=name)


def choose_bootstrap_file(pattern: str) -> Path | None:
    matches = sorted(PROJECT_ROOT.glob(pattern), key=lambda item: (item.stat().st_mtime, item.name), reverse=True)
    return matches[0] if matches else None


def api_health_url(host: str, port: int) -> str:
    return f"http://{host}:{port}/api/health"


def manager_url(host: str, port: int) -> str:
    return f"http://{host}:{port}/"


def is_url_alive(url: str, timeout: float = 1.0) -> bool:
    try:
        with urlopen(url, timeout=timeout) as response:
            return response.status == 200
    except URLError:
        return False
    except Exception:
        return False


def find_free_port(host: str, start: int = DEFAULT_PORT, attempts: int = 50) -> int:
    for port in range(start, start + attempts):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if sock.connect_ex((host, port)) != 0:
                return port
    raise RuntimeError("No free port found for AllTick manager")


class AllTickManagerStore:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self._bootstrap()

    def _bootstrap(self) -> None:
        if not API_FILE.exists():
            source = choose_bootstrap_file("alltick_apis_*.txt")
            api_values = source.read_text(encoding="utf-8", errors="ignore").splitlines() if source else []
            self._write_api_lines(api_values)

        if not STOCK_FILE.exists():
            source = None
            for candidate in (
                PROJECT_ROOT / "图片2_股票去重清单_复核版.csv",
                PROJECT_ROOT / "图片2_股票去重清单.csv",
            ):
                if candidate.exists():
                    source = candidate
                    break
            if source is None:
                source = choose_bootstrap_file("图片2_股票去重清单*.csv")
            if source is not None:
                stocks = self._read_stock_csv(source)
                self._write_stocks(stocks)
            else:
                self._write_stocks([])

        if not SETTINGS_FILE.exists():
            self._write_settings(DEFAULT_SETTINGS)

        self.snapshot(write_exports=True)

    def _write_api_lines(self, values: list[str]) -> None:
        ordered: list[str] = []
        seen: set[str] = set()
        for raw in values:
            item = raw.strip()
            if not item or item in seen:
                continue
            seen.add(item)
            ordered.append(item)
        API_FILE.write_text("\n".join(ordered) + ("\n" if ordered else ""), encoding="utf-8")

    def _read_stock_csv(self, path: Path) -> list[StockItem]:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            items: list[StockItem] = []
            for row in reader:
                raw = row.get("symbol") or row.get("code") or ""
                name = (row.get("name") or "").strip()
                if not raw.strip():
                    continue
                symbol = normalize_symbol(raw)
                code = symbol.split(".", 1)[0]
                items.append(StockItem(code=code, symbol=symbol, name=name or code))
            return items

    def _write_stocks(self, stocks: list[StockItem]) -> None:
        with STOCK_FILE.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["code", "symbol", "name"])
            writer.writeheader()
            for item in stocks:
                writer.writerow(
                    {
                        "code": item.code,
                        "symbol": item.symbol,
                        "name": item.name,
                    }
                )

    def _read_settings(self) -> dict[str, Any]:
        try:
            data = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
        except Exception:
            data = {}
        settings = DEFAULT_SETTINGS.copy()
        settings.update(data if isinstance(data, dict) else {})
        try:
            settings["max_stocks_per_api"] = max(int(settings.get("max_stocks_per_api", 2)), 1)
        except Exception:
            settings["max_stocks_per_api"] = 2
        if settings.get("distribution_mode") not in {"balanced", "sequential"}:
            settings["distribution_mode"] = "balanced"
        return settings

    def _write_settings(self, settings: dict[str, Any]) -> None:
        merged = DEFAULT_SETTINGS.copy()
        merged.update(settings)
        SETTINGS_FILE.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")

    def load_apis(self) -> list[str]:
        if not API_FILE.exists():
            return []
        return [line.strip() for line in API_FILE.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]

    def load_stocks(self) -> list[StockItem]:
        if not STOCK_FILE.exists():
            return []
        return self._read_stock_csv(STOCK_FILE)

    def add_apis(self, text: str) -> dict[str, Any]:
        with self.lock:
            current = self.load_apis()
            seen = set(current)
            added = 0
            for item in parse_api_text(text):
                if item in seen:
                    continue
                current.append(item)
                seen.add(item)
                added += 1
            self._write_api_lines(current)
            state = self.snapshot(write_exports=True)
            return {"message": f"新增 API {added} 个", "state": state}

    def remove_api(self, api: str) -> dict[str, Any]:
        with self.lock:
            current = [item for item in self.load_apis() if item != api]
            self._write_api_lines(current)
            state = self.snapshot(write_exports=True)
            return {"message": f"已删除 API: {api}", "state": state}

    def add_stocks(self, text: str) -> dict[str, Any]:
        with self.lock:
            current = self.load_stocks()
            seen = {item.symbol for item in current}
            added = 0
            skipped = 0
            for raw in text.splitlines():
                try:
                    item = parse_stock_line(raw)
                except ValueError:
                    skipped += 1
                    continue
                if item is None:
                    continue
                if item.symbol in seen:
                    skipped += 1
                    continue
                current.append(item)
                seen.add(item.symbol)
                added += 1
            self._write_stocks(current)
            state = self.snapshot(write_exports=True)
            message = f"新增自选股 {added} 只"
            if skipped:
                message += f"，跳过 {skipped} 行"
            return {"message": message, "state": state}

    def remove_stock(self, symbol: str) -> dict[str, Any]:
        with self.lock:
            normalized = normalize_symbol(symbol)
            current = [item for item in self.load_stocks() if item.symbol != normalized]
            self._write_stocks(current)
            state = self.snapshot(write_exports=True)
            return {"message": f"已删除自选股: {normalized}", "state": state}

    def update_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            settings = self._read_settings()
            settings["distribution_mode"] = payload.get("distribution_mode", settings["distribution_mode"])
            settings["max_stocks_per_api"] = payload.get("max_stocks_per_api", settings["max_stocks_per_api"])
            self._write_settings(settings)
            state = self.snapshot(write_exports=True)
            return {"message": "分配规则已更新", "state": state}

    def distribute(self, apis: list[str], stocks: list[StockItem], settings: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        if not apis:
            return [], [], [
                {
                    "stock_seq": idx,
                    "code": stock.code,
                    "symbol": stock.symbol,
                    "name": stock.name,
                    "api": "",
                    "api_seq": "",
                    "status": "unassigned",
                }
                for idx, stock in enumerate(stocks, start=1)
            ]

        max_per_api = max(int(settings["max_stocks_per_api"]), 1)
        buckets: list[list[StockItem]] = [[] for _ in apis]
        unassigned: list[StockItem] = []

        if settings["distribution_mode"] == "sequential":
            bucket_index = 0
            for stock in stocks:
                while bucket_index < len(apis) and len(buckets[bucket_index]) >= max_per_api:
                    bucket_index += 1
                if bucket_index >= len(apis):
                    unassigned.append(stock)
                    continue
                buckets[bucket_index].append(stock)
        else:
            cursor = 0
            for stock in stocks:
                assigned = False
                for _ in range(len(apis)):
                    bucket_index = cursor % len(apis)
                    cursor += 1
                    if len(buckets[bucket_index]) >= max_per_api:
                        continue
                    buckets[bucket_index].append(stock)
                    assigned = True
                    break
                if not assigned:
                    unassigned.append(stock)

        api_rows: list[dict[str, Any]] = []
        stock_rows: list[dict[str, Any]] = []
        stock_seq = 1
        for api_seq, (api, bucket) in enumerate(zip(apis, buckets), start=1):
            stocks_text = " | ".join(f"{item.symbol} {item.name}" for item in bucket)
            api_rows.append(
                {
                    "api_seq": api_seq,
                    "api": api,
                    "stock_count": len(bucket),
                    "stocks": stocks_text,
                    "status": "used" if bucket else "idle",
                }
            )
            for slot, item in enumerate(bucket, start=1):
                stock_rows.append(
                    {
                        "stock_seq": stock_seq,
                        "code": item.code,
                        "symbol": item.symbol,
                        "name": item.name,
                        "api": api,
                        "api_seq": api_seq,
                        "status": f"slot_{slot}_of_{len(bucket)}",
                    }
                )
                stock_seq += 1

        for item in unassigned:
            stock_rows.append(
                {
                    "stock_seq": stock_seq,
                    "code": item.code,
                    "symbol": item.symbol,
                    "name": item.name,
                    "api": "",
                    "api_seq": "",
                    "status": "unassigned",
                }
            )
            stock_seq += 1

        unassigned_rows = [row for row in stock_rows if row["status"] == "unassigned"]
        return api_rows, stock_rows, unassigned_rows

    def _write_exports(self, api_rows: list[dict[str, Any]], stock_rows: list[dict[str, Any]], stocks: list[StockItem]) -> None:
        with API_ASSIGNMENT_FILE.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["api_seq", "api", "stock_count", "stocks", "status"])
            writer.writeheader()
            writer.writerows(api_rows)

        with STOCK_ASSIGNMENT_FILE.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=["stock_seq", "code", "symbol", "name", "api", "api_seq", "status"])
            writer.writeheader()
            writer.writerows(stock_rows)

        WATCHLIST_EXPORT_FILE.write_text(
            "\n".join(f"{item.symbol},{item.name}" for item in stocks) + ("\n" if stocks else ""),
            encoding="utf-8",
        )

    def snapshot(self, write_exports: bool = False) -> dict[str, Any]:
        with self.lock:
            apis = self.load_apis()
            stocks = self.load_stocks()
            settings = self._read_settings()
            api_rows, stock_rows, unassigned_rows = self.distribute(apis, stocks, settings)

            if write_exports:
                self._write_exports(api_rows, stock_rows, stocks)

            usage_by_api = {row["api"]: row["stock_count"] for row in api_rows}
            api_items = [
                {
                    "seq": idx,
                    "api": api,
                    "stock_count": usage_by_api.get(api, 0),
                }
                for idx, api in enumerate(apis, start=1)
            ]
            stock_items = [
                {
                    "seq": idx,
                    "code": item.code,
                    "symbol": item.symbol,
                    "name": item.name,
                }
                for idx, item in enumerate(stocks, start=1)
            ]

            used_api_count = sum(1 for row in api_rows if row["stock_count"] > 0)
            summary = {
                "api_count": len(apis),
                "stock_count": len(stocks),
                "used_api_count": used_api_count,
                "unused_api_count": max(len(apis) - used_api_count, 0),
                "assigned_stock_count": len(stocks) - len(unassigned_rows),
                "unassigned_stock_count": len(unassigned_rows),
                "max_stocks_per_api": settings["max_stocks_per_api"],
                "distribution_mode": settings["distribution_mode"],
                "data_dir": str(DATA_DIR),
                "exports": {
                    "api_assignments": str(API_ASSIGNMENT_FILE),
                    "stock_assignments": str(STOCK_ASSIGNMENT_FILE),
                    "watchlist": str(WATCHLIST_EXPORT_FILE),
                },
            }
            return {
                "summary": summary,
                "settings": settings,
                "apis": api_items,
                "stocks": stock_items,
                "assignments": {
                    "api_view": api_rows,
                    "stock_view": stock_rows,
                    "unassigned": unassigned_rows,
                },
            }


STORE = AllTickManagerStore()


class AllTickManagerHandler(BaseHTTPRequestHandler):
    server_version = "AllTickManager/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.serve_index()
            return
        if parsed.path == "/api/health":
            self.send_json({"ok": True})
            return
        if parsed.path == "/api/state":
            self.send_json(STORE.snapshot(write_exports=False))
            return
        if parsed.path.startswith("/api/export/"):
            self.serve_export(parsed.path.split("/", 3)[-1])
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        payload = self.read_json_body()
        try:
            if parsed.path == "/api/apis/add":
                self.send_json(STORE.add_apis(str(payload.get("text", ""))))
                return
            if parsed.path == "/api/apis/remove":
                self.send_json(STORE.remove_api(str(payload.get("api", ""))))
                return
            if parsed.path == "/api/stocks/add":
                self.send_json(STORE.add_stocks(str(payload.get("text", ""))))
                return
            if parsed.path == "/api/stocks/remove":
                self.send_json(STORE.remove_stock(str(payload.get("symbol", ""))))
                return
            if parsed.path == "/api/settings/update":
                self.send_json(STORE.update_settings(payload))
                return
        except ValueError as exc:
            self.send_json({"message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            return
        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def serve_index(self) -> None:
        if not ASSET_PATH.exists():
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Missing index.html asset")
            return
        body = ASSET_PATH.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def serve_export(self, name: str) -> None:
        mapping = {
            "api_assignments.csv": API_ASSIGNMENT_FILE,
            "stock_assignments.csv": STOCK_ASSIGNMENT_FILE,
            "watchlist.txt": WATCHLIST_EXPORT_FILE,
            "apis.txt": API_FILE,
            "watchlist.csv": STOCK_FILE,
        }
        path = mapping.get(name)
        if path is None or not path.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "Export not found")
            return

        content_type = "text/plain; charset=utf-8"
        if path.suffix == ".csv":
            content_type = "text/csv; charset=utf-8"
        body = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Disposition", f'attachment; filename="{path.name}"')
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except Exception:
            return {}
        return data if isinstance(data, dict) else {}

    def send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class AllTickManagerHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True


def write_runtime(host: str, port: int) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "pid": os.getpid(),
        "host": host,
        "port": port,
        "url": manager_url(host, port),
    }
    RUNTIME_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def remove_runtime() -> None:
    try:
        if RUNTIME_FILE.exists():
            RUNTIME_FILE.unlink()
    except Exception:
        pass


def read_runtime() -> dict[str, Any]:
    try:
        return json.loads(RUNTIME_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def wait_for_server(host: str, port: int, timeout: float = 10.0) -> bool:
    deadline = time.time() + timeout
    url = api_health_url(host, port)
    while time.time() < deadline:
        if is_url_alive(url, timeout=0.8):
            return True
        time.sleep(0.2)
    return False


def launch_manager(host: str) -> int:
    runtime = read_runtime()
    if runtime:
        try:
            runtime_port = int(runtime.get("port", 0))
        except Exception:
            runtime_port = 0
        runtime_host = str(runtime.get("host", host) or host)
        if runtime_port > 0 and is_url_alive(api_health_url(runtime_host, runtime_port)):
            webbrowser.open(runtime.get("url", manager_url(runtime_host, runtime_port)))
            return 0

    port = find_free_port(host)
    with LOG_FILE.open("a", encoding="utf-8") as log_handle:
        subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--serve", "--host", host, "--port", str(port)],
            cwd=str(PROJECT_ROOT),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    if not wait_for_server(host, port, timeout=12.0):
        print(f"启动失败，请查看日志: {LOG_FILE}")
        return 1

    webbrowser.open(manager_url(host, port))
    return 0


def run_server(host: str, port: int) -> int:
    STORE.snapshot(write_exports=True)
    server = AllTickManagerHTTPServer((host, port), AllTickManagerHandler)
    write_runtime(host, port)
    atexit.register(remove_runtime)
    print(f"AllTick manager running at {manager_url(host, port)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        remove_runtime()
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="AllTick 本地配对管理网页")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--serve", action="store_true", help="直接启动服务")
    parser.add_argument("--launch", action="store_true", help="后台确保服务运行并打开浏览器")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.launch:
        return launch_manager(args.host)
    return run_server(args.host, args.port)


if __name__ == "__main__":
    raise SystemExit(main())
