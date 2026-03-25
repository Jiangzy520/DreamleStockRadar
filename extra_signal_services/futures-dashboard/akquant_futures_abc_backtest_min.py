#!/usr/bin/env python3
"""Public placeholder for the private AKQuant futures backtest example."""

from __future__ import annotations

import json
import sys


def main() -> int:
    payload = {
        "public_mode": True,
        "status": "hidden",
        "message": "公开版已移除 AKQuant 期货回测示例中的真实策略实现，仅保留占位说明。",
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
