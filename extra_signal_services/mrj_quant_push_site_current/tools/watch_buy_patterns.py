#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Public placeholder for the private image signal watcher.

The production image-based watch and trigger logic has been removed from the
public repository. This file is preserved only to keep the tool layout intact.
"""

from __future__ import annotations

import json
import sys


def main() -> int:
    payload = {
        "public_mode": True,
        "status": "hidden",
        "message": "公开版已移除图片监控与买点触发逻辑，仅保留工具文件名和目录结构。",
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
