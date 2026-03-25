#!/usr/bin/env python3
"""Public placeholder for the private futures scan worker.

The production strategy scanner has been removed from the public repository.
This file is kept only to preserve the project structure and script entry name.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    payload = {
        "public_mode": True,
        "status": "hidden",
        "message": "公开版已移除期货扫描策略实现，仅保留脚本入口名称和目录结构。",
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
