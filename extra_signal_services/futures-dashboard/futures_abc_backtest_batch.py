#!/usr/bin/env python3
"""Public placeholder for the private futures batch backtest script."""

from __future__ import annotations

import json
import sys


def main() -> int:
    payload = {
        "public_mode": True,
        "status": "hidden",
        "message": "公开版已移除期货批量回测实现，仅保留脚本名称用于说明项目结构。",
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
