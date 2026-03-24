#!/usr/bin/env python3
"""Minimal pytdx futures quote example with automatic host retries."""

from __future__ import annotations

import argparse

from pytdx.exhq import TdxExHq_API
from pytdx.util.best_ip import future_ip


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Query one futures quote via pytdx TdxExHq_API.")
    parser.add_argument("--market", type=int, default=47, help="Market id, for example 47.")
    parser.add_argument("--code", default="IFL0", help="Instrument code, for example IFL0.")
    parser.add_argument("--timeout", type=float, default=8.0, help="Socket timeout in seconds.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    for host in future_ip:
        api = TdxExHq_API(raise_exception=False)
        ip = host["ip"]
        port = host["port"]
        name = host.get("name", "")
        print(f"[TRY] {ip}:{port} {name}")

        try:
            connection = api.connect(ip, port, time_out=args.timeout)
            if not connection:
                print("  connect failed")
                continue

            quote = api.get_instrument_quote(args.market, args.code)
            if not quote:
                print("  no quote returned")
                continue

            df = api.to_df(quote)
            print(f"[OK] host={ip}:{port} code={args.code} market={args.market}")
            print(df.to_string(index=False))
            return
        except Exception as exc:  # noqa: BLE001
            print(f"  failed: {exc}")
        finally:
            try:
                api.disconnect()
            except Exception:  # noqa: BLE001
                pass

    raise SystemExit("No pytdx host returned a quote.")


if __name__ == "__main__":
    main()
