#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path

import ctypes


ROOT = Path("/home/jzy/.wine-tdx/drive_c/new_tdx64")
DLL_PATH = ROOT / "PYPlugins" / "TPythClient.dll"

MARKET_SUFFIX = {
    "0": "SZ",
    "1": "SH",
    "2": "BJ",
}

SUFFIX_MARKET = {
    "SZ": "0",
    "SH": "1",
    "BJ": "2",
}


def python_version_number() -> int:
    return int(f"{sys.version_info.major}{sys.version_info.minor}")


def parse_response(raw_ptr) -> dict:
    if not raw_ptr:
        raise RuntimeError("DLL returned empty pointer")
    raw = ctypes.cast(raw_ptr, ctypes.c_char_p).value
    if not raw:
        raise RuntimeError("DLL returned empty response")
    return json.loads(raw.decode("utf-8"))


class TdxClient:
    def __init__(self, run_mode: int = -1):
        if not DLL_PATH.exists():
            raise FileNotFoundError(f"Missing DLL: {DLL_PATH}")

        self.dll = ctypes.CDLL(str(DLL_PATH))
        self.dll.InitConnect.restype = ctypes.c_char_p
        self.dll.CloseConnect.restype = None
        self.dll.SetResToMain.restype = ctypes.c_char_p
        self.dll.GetUserBlockInStr.restype = ctypes.c_char_p
        self.dll.GetBlockStocksInStr.restype = ctypes.c_char_p

        self.run_mode = run_mode
        self.run_id = -1
        self.connected = False

    def connect(self) -> None:
        script_path = str(Path(__file__).resolve()).encode("utf-8")
        dll_path = str(DLL_PATH).encode("utf-8")
        resp = parse_response(
            self.dll.InitConnect(
                script_path,
                dll_path,
                self.run_mode,
                python_version_number(),
            )
        )
        if resp.get("ErrorId") not in {"0", "12"}:
            raise RuntimeError(f"InitConnect failed: {resp}")
        self.run_id = int(resp.get("run_id", "-1"))
        if self.run_id < 0:
            raise RuntimeError(f"Invalid run_id from InitConnect: {resp}")
        self.connected = True

    def close(self) -> None:
        if self.connected:
            self.dll.CloseConnect(self.run_id, self.run_mode)
            self.connected = False

    def get_user_blocks(self):
        return parse_response(self.dll.GetUserBlockInStr(self.run_id, 5000))

    def get_block_stocks(self, block_code: str):
        return parse_response(
            self.dll.GetBlockStocksInStr(
                self.run_id,
                block_code.encode("utf-8"),
                0,
                5000,
            )
        )

    def send_user_block(self, block_code: str, stocks: list[str], show: bool = False):
        stock_str = "|".join(encode_symbols(stocks))
        payload = f"XG,{block_code}||{stock_str}||{'1' if show else '0'}".encode("utf-8")
        return parse_response(self.dll.SetResToMain(self.run_id, self.run_mode, payload, 30000))


def blk_line_to_symbol(line: str) -> str | None:
    line = line.strip()
    if len(line) != 7 or not line.isdigit():
        return None
    code = line[:6]
    market = line[6]
    suffix = MARKET_SUFFIX.get(market)
    if not suffix:
        return None
    if code == "999999":
        return None
    return f"{code}.{suffix}"


def load_symbols_from_blk(path: Path) -> list[str]:
    symbols = []
    seen = set()
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        symbol = blk_line_to_symbol(raw)
        if symbol and symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    return symbols


def encode_symbols(symbols: list[str]) -> list[str]:
    encoded = []
    for symbol in symbols:
        if "#" in symbol:
            encoded.append(symbol)
            continue
        if "." not in symbol:
            raise ValueError(f"Invalid symbol format: {symbol}")
        code, suffix = symbol.split(".", 1)
        market = SUFFIX_MARKET.get(suffix.upper())
        if market is None:
            raise ValueError(f"Unsupported symbol suffix: {symbol}")
        if not code.isdigit() or len(code) != 6:
            raise ValueError(f"Invalid stock code: {symbol}")
        encoded.append(f"{market}#{code}")
    return encoded


def main() -> int:
    parser = argparse.ArgumentParser(description="通达信自选辅助工具")
    parser.add_argument("--run-mode", type=int, default=-1)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list-user-blocks")

    list_block = subparsers.add_parser("list-block")
    list_block.add_argument("block_code")

    send_block = subparsers.add_parser("send-block")
    send_block.add_argument("block_code")
    send_block.add_argument("--stock", action="append", dest="stocks", default=[])
    send_block.add_argument("--show", action="store_true")

    sync_block = subparsers.add_parser("sync-zxg-from-blk")
    sync_block.add_argument("blk_path")
    sync_block.add_argument("--show", action="store_true")

    args = parser.parse_args()

    client = TdxClient(run_mode=args.run_mode)
    try:
        client.connect()

        if args.command == "list-user-blocks":
            print(json.dumps(client.get_user_blocks(), ensure_ascii=False, indent=2))
            return 0

        if args.command == "list-block":
            print(json.dumps(client.get_block_stocks(args.block_code), ensure_ascii=False, indent=2))
            return 0

        if args.command == "send-block":
            print(
                json.dumps(
                    client.send_user_block(args.block_code, args.stocks, show=args.show),
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        if args.command == "sync-zxg-from-blk":
            blk_path = Path(args.blk_path)
            symbols = load_symbols_from_blk(blk_path)
            if not symbols:
                raise RuntimeError(f"No symbols parsed from {blk_path}")

            backup = client.get_block_stocks("ZXG")
            print(json.dumps({"backup": backup}, ensure_ascii=False, indent=2))

            clear_resp = client.send_user_block("ZXG", [], show=False)
            add_resp = client.send_user_block("ZXG", symbols, show=args.show)
            verify_resp = client.get_block_stocks("ZXG")

            print(
                json.dumps(
                    {
                        "clear": clear_resp,
                        "add": add_resp,
                        "verify_count": len(verify_resp.get("Value") or []),
                        "verify": verify_resp,
                    },
                    ensure_ascii=False,
                    indent=2,
                )
            )
            return 0

        raise RuntimeError(f"Unsupported command: {args.command}")
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
