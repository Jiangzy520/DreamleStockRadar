#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
观澜量化 - 图片买点监控脚本

监控指定目录中的新截图，调用现有 AI vision 模型识别是否出现
“突破 / 回踩 / 拐点”类强势买点；若命中则立即输出结果、播放提示音，
并可选发送钉钉通知。

示例：
    python tools/watch_buy_patterns.py \
        --watch-dir "/home/jzy/桌面/量化/策略分析" \
        --once

    python tools/watch_buy_patterns.py \
        --watch-dir "/home/jzy/桌面/量化/策略分析" \
        --baseline-now
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from guanlan.core.constants import CONFIG_DIR
from guanlan.core.services.ai import AIClient, AIConfig
from guanlan.core.utils.logger import get_logger


logger = get_logger("buy_watch")


DEFAULT_SYSTEM_PROMPT = """你是一位专门识别中国股票 1 分钟分时强势买点的交易分析师。

你要识别的不是泛泛而谈的上涨形态，而是用户自定义的分时买点规则：
1. 买点类型只重点识别：突破、回踩、拐点。
2. 图中常见关键点位术语：左1、左2、右1、右2。
3. 图中左1通常是开盘后冲高的最高点，必须判断它是否达到 0 轴上方 2.5% 或 5% 涨幅阈值。
4. 图中右1通常是释放点、前一天的点或试拉升点。
5. 当右2或左2出现时，要判断是否进入“进场准备”状态。
6. 真正进场点不是盲目追涨，而是：
   - 突破右2后回踩不破再拐头向上；
   - 或突破右2后回调不破形成反抽；
   - 或突破左2时直接进场；
   - 若分时一和分时三同时成立，属于更强的加仓信号。
7. 量能必须检查：如果图上能看出右1和左1对应的最大成交量，右1最大成交量不能超过左1最大成交量；若无法看清，明确写未知。
8. 只分析 1 分钟强势分时图，必须优先判断图中是否具备白线、黄线或均价线、0 轴涨幅参考、成交量柱。
9. 以下情形一律降级：低流动性、没有量能配合的缓慢拉升、买点位置过高过晚、图形信息不完整。

负面情形：
1. 图上已经明显拉高后才追涨。
2. 没有量能确认。
3. 跌回均线或跌回平台内部。
4. 买点过晚，盈亏比明显变差。

你必须只输出一个 JSON 对象，不要输出 Markdown，不要输出代码块。
如果图上信息看不清，明确写“未知”，不要编造。
"""


DEFAULT_USER_PROMPT = """请严格按以下 JSON 字段输出，并基于图片判断这是不是我需要立刻关注的分时买点。输出必须使用中文：

{
  "品种名称": "字符串，未知就写未知",
  "周期": "字符串，优先识别1分钟，看不清就写未知",
  "形态归类": "分时一/分时三/分时一+三/其他",
  "买点类型": "突破/回踩/拐点/其他",
  "关键点位": {
    "左1": "如何识别、是否成立",
    "左2": "如何识别、是否成立",
    "右1": "如何识别、是否成立",
    "右2": "如何识别、是否成立"
  },
  "规则命中": ["命中的规则1", "命中的规则2"],
  "涨幅阈值": "左1是否达到0轴上方2.5%或5%，写清楚",
  "量能约束": "右1最大成交量是否不超过左1最大成交量，未知也要写",
  "当前是否进入进场准备": true,
  "最终进场触发条件是否成立": true,
  "进场触发": "突破右2回踩不破拐头/突破右2回调不破反抽/突破左2直上/其他",
  "入场确认条件": ["条件1", "条件2"],
  "止损位": "明确文字说明",
  "失效点": "明确文字说明",
  "是否强信号": true,
  "信号强度": "强/中/弱",
  "K线和成交量": "简洁说明量价关系",
  "指标线": ["图中能识别出的均线或关键线"],
  "原本想买的位置": "如果图片里有箭头/圈/标记，说明那个位置是否合理；没有就写未标注",
  "相似度判断": "高/中/低",
  "是否通知": true,
  "通知理由": "一句话说明为什么值得立即关注或为什么不值得",
  "摘要": "用1到2句话概括这个图的结论"
}

判断标准：
1. 只重点识别 1 分钟强势图中的突破、回踩、拐点买点。
2. 必须尽量判断左1/左2/右1/右2，而不是只说模糊的“平台突破”。
3. 如果左1涨幅没有达到你能识别出的阈值要求，必须降低评级。
4. 如果右1量能明显大于左1量能，必须降低评级或判定无效。
5. 必须明确判断当前是否进入进场准备，以及最终进场触发条件是否成立。
6. 入场确认条件必须能执行，不能空泛。
7. 止损位和失效点必须具体。
8. 如果图上已经涨太多、买点过晚、位置太高，则“是否通知”必须为 false。
9. 执行顺序必须尽量遵循：先周期，再左1涨幅，再关键点位，再量能关系，再突破，再回踩不破，最后看是否拐头向上。
10. 只有“是否强信号=true”且“买点类型”属于突破/回踩/拐点，且相似度判断为高或中，且“最终进场触发条件是否成立=true”，才允许发出立即关注信号。
"""


WATCH_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp"}
DEFAULT_TARGET_TYPES = {"突破", "回踩", "拐点"}


@dataclass
class AnalysisResult:
    image_path: str
    image_sha1: str
    analyzed_at: str
    品种名称: str = "未知"
    周期: str = "未知"
    形态归类: str = "其他"
    买点类型: str = "其他"
    关键点位: dict[str, str] | None = None
    规则命中: list[str] | None = None
    涨幅阈值: str = "未知"
    量能约束: str = "未知"
    当前是否进入进场准备: bool = False
    最终进场触发条件是否成立: bool = False
    进场触发: str = "未知"
    入场确认条件: list[str] | None = None
    止损位: str = "未知"
    失效点: str = "未知"
    是否强信号: bool = False
    信号强度: str = "弱"
    K线和成交量: str = "未知"
    指标线: list[str] | None = None
    原本想买的位置: str = "未标注"
    相似度判断: str = "低"
    是否通知: bool = False
    通知理由: str = ""
    摘要: str = ""
    raw_response: str = ""
    parse_error: str = ""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="监控截图目录并识别强势买点")
    parser.add_argument(
        "--watch-dir",
        default=str(CONFIG_DIR.parent / "策略分析"),
        help="要监控的图片目录",
    )
    parser.add_argument(
        "--ai-config",
        default=str(CONFIG_DIR / "config" / "ai.json"),
        help="AI 配置文件路径",
    )
    parser.add_argument(
        "--model",
        default="",
        help="指定使用的 vision 模型，不填则自动选择第一个已配置 API Key 的 vision 模型",
    )
    parser.add_argument(
        "--result-dir",
        default=str(CONFIG_DIR / "pattern_watch" / "results"),
        help="分析结果输出目录",
    )
    parser.add_argument(
        "--state-file",
        default=str(CONFIG_DIR / "pattern_watch" / "state.json"),
        help="已处理文件状态保存路径",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=5.0,
        help="目录轮询秒数",
    )
    parser.add_argument(
        "--min-age",
        type=float,
        default=1.5,
        help="文件最小静置秒数，避免还在写入时就分析",
    )
    parser.add_argument(
        "--prompt-file",
        default="",
        help="自定义提示词文件，存在时覆盖默认用户提示",
    )
    parser.add_argument(
        "--notify-types",
        default="突破,回踩,拐点",
        help="哪些买点类型触发通知，逗号分隔",
    )
    parser.add_argument(
        "--strong-only",
        action="store_true",
        default=True,
        help="只通知强信号（默认开启）",
    )
    parser.add_argument(
        "--no-strong-only",
        action="store_false",
        dest="strong_only",
        help="允许非强信号也通知",
    )
    parser.add_argument(
        "--sound",
        action="store_true",
        default=True,
        help="命中后播放声音（默认开启）",
    )
    parser.add_argument(
        "--no-sound",
        action="store_false",
        dest="sound",
        help="关闭声音提示",
    )
    parser.add_argument(
        "--sound-type",
        default="alarm",
        help="声音类型，默认 alarm",
    )
    parser.add_argument(
        "--dingtalk",
        action="store_true",
        default=False,
        help="命中后发送钉钉通知",
    )
    parser.add_argument(
        "--webhook",
        default="",
        help="钉钉 webhook，不填则尝试读取 GUI 配置",
    )
    parser.add_argument(
        "--secret",
        default="",
        help="钉钉 secret，不填则尝试读取 GUI 配置",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="只扫描一次后退出",
    )
    parser.add_argument(
        "--baseline-now",
        action="store_true",
        help="把当前目录已有文件直接标记为已处理，不做分析，适合首次启用",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=0,
        help="本次最多处理多少张图，0 表示不限",
    )
    return parser.parse_args()


def sanitize_name(value: str, limit: int = 40) -> str:
    text = re.sub(r"[^0-9A-Za-z_\-一-龥]+", "_", value).strip("_")
    return (text or "result")[:limit]


def load_prompt(prompt_file: str) -> str:
    if not prompt_file:
        return DEFAULT_USER_PROMPT

    path = Path(prompt_file)
    if not path.exists():
        raise FileNotFoundError(f"提示词文件不存在: {path}")
    return path.read_text(encoding="utf-8")


def extract_json_object(text: str) -> dict[str, Any]:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
        cleaned = re.sub(r"\s*```$", "", cleaned)

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end < 0 or end <= start:
        raise ValueError("AI 返回中未找到 JSON 对象")

    return json.loads(cleaned[start:end + 1])


def ensure_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        parts = re.split(r"[；;\n、,，]+", value)
        return [part.strip() for part in parts if part.strip()]
    return []


def ensure_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    return text in {"true", "1", "yes", "y", "是", "强", "通知"}


def sha1_of_file(path: Path) -> str:
    h = hashlib.sha1()
    h.update(path.read_bytes())
    return h.hexdigest()


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"processed": {}}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("状态文件损坏，将重建: {}", path)
        return {"processed": {}}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def pick_model(config: AIConfig, requested: str) -> str:
    if requested:
        cfg = config.get_model_config(requested)
        if not cfg.supports_vision:
            raise ValueError(f"模型不支持图片分析: {requested}")
        if not cfg.api_key:
            raise ValueError(f"模型未配置 API Key: {requested}")
        return requested

    for model_name in config.list_vision_models():
        model_cfg = config.get_model_config(model_name)
        if model_cfg.api_key:
            return model_name

    raise ValueError("没有找到已配置 API Key 的 vision 模型，请先在 .guanlan/config/ai.json 中填写")


def load_notification_config(args: argparse.Namespace) -> tuple[str, str]:
    if not args.dingtalk and not args.webhook.strip():
        return "", ""

    webhook = args.webhook.strip()
    secret = args.secret.strip()

    if webhook:
        return webhook, secret

    try:
        from guanlan.ui.common.config import cfg, load_config

        load_config()
        enabled = cfg.get(cfg.dingtalkActive)
        if not enabled:
            return "", ""

        return cfg.get(cfg.dingtalkWebhook).strip(), cfg.get(cfg.dingtalkSecret).strip()
    except Exception as exc:
        logger.warning("读取 GUI 钉钉配置失败: {}", exc)
        return "", ""


def configure_sound() -> None:
    try:
        from guanlan.ui.common.config import cfg, load_config
        from guanlan.core.services.sound import get_player

        load_config()
        if not cfg.get(cfg.enableSound):
            return

        volume = cfg.get(cfg.soundVolume) / 100
        get_player().set_volume(volume)
    except Exception as exc:
        logger.warning("读取声音配置失败，使用默认音量: {}", exc)


def should_notify(
    result: AnalysisResult,
    target_types: set[str],
    strong_only: bool,
) -> bool:
    buy_type = result.买点类型.strip()
    similarity = result.相似度判断.strip()
    pattern_type = result.形态归类.strip()
    threshold = result.涨幅阈值.strip()
    volume_rule = result.量能约束.strip()
    trigger = result.进场触发.strip()

    if not any(target in buy_type for target in target_types):
        return False
    if pattern_type == "其他":
        return False
    if not any(level in similarity for level in {"高", "中"}):
        return False
    if strong_only and not result.是否强信号:
        return False
    if not threshold or any(word in threshold for word in {"未知", "看不清", "未达到", "不足", "不满足"}):
        return False
    if not volume_rule or any(word in volume_rule for word in {"未知", "看不清", "不满足", "无效"}):
        return False
    if "超过" in volume_rule and "不超过" not in volume_rule:
        return False
    if not trigger or trigger in {"未知", "其他"}:
        return False
    if not result.最终进场触发条件是否成立:
        return False
    if result.是否通知:
        return True

    return result.是否强信号 and any(target in buy_type for target in target_types)


def build_markdown(result: AnalysisResult) -> str:
    conditions = result.入场确认条件 or ["未给出"]
    indicators = result.指标线 or ["未知"]
    matched_rules = result.规则命中 or ["未给出"]
    key_points = result.关键点位 or {}
    return "\n".join(
        [
            f"## 图片买点提醒",
            f"- 品种: {result.品种名称}",
            f"- 周期: {result.周期}",
            f"- 形态归类: {result.形态归类}",
            f"- 买点类型: {result.买点类型}",
            f"- 关键点位: 左1={key_points.get('左1', '未知')}；左2={key_points.get('左2', '未知')}；右1={key_points.get('右1', '未知')}；右2={key_points.get('右2', '未知')}",
            f"- 规则命中: {'；'.join(matched_rules)}",
            f"- 涨幅阈值: {result.涨幅阈值}",
            f"- 量能约束: {result.量能约束}",
            f"- 当前是否进入进场准备: {'是' if result.当前是否进入进场准备 else '否'}",
            f"- 最终进场触发条件是否成立: {'是' if result.最终进场触发条件是否成立 else '否'}",
            f"- 进场触发: {result.进场触发}",
            f"- 信号强度: {result.信号强度}",
            f"- 是否强信号: {'是' if result.是否强信号 else '否'}",
            f"- 相似度: {result.相似度判断}",
            f"- 入场确认条件: {'；'.join(conditions)}",
            f"- 止损位: {result.止损位}",
            f"- 失效点: {result.失效点}",
            f"- K线和成交量: {result.K线和成交量}",
            f"- 指标线: {'、'.join(indicators)}",
            f"- 原本想买的位置: {result.原本想买的位置}",
            f"- 通知理由: {result.通知理由 or result.摘要}",
            f"- 图片: {result.image_path}",
        ]
    )


def result_file_base(result_dir: Path, image_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    match = re.search(r"MsgID=(\d+)", image_path.name)
    suffix = match.group(1)[-6:] if match else sanitize_name(image_path.stem, limit=12)
    return result_dir / f"{stamp}_{suffix}"


def save_result_files(result_dir: Path, result: AnalysisResult, image_path: Path) -> tuple[Path, Path]:
    result_dir.mkdir(parents=True, exist_ok=True)
    base = result_file_base(result_dir, image_path)
    json_path = base.with_suffix(".json")
    md_path = base.with_suffix(".md")

    json_path.write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    md_path.write_text(build_markdown(result), encoding="utf-8")
    return json_path, md_path


def copy_alert_image(alert_dir: Path, image_path: Path) -> Path:
    alert_dir.mkdir(parents=True, exist_ok=True)
    target = alert_dir / image_path.name
    if not target.exists():
        shutil.copy2(image_path, target)
    return target


class DingTalkNotifier:
    def __init__(self, webhook: str, secret: str):
        self.webhook = webhook
        self.secret = secret
        self._bot = None

    def enabled(self) -> bool:
        return bool(self.webhook)

    def _get_bot(self):
        if self._bot is None:
            from dingtalkchatbot.chatbot import DingtalkChatbot

            self._bot = DingtalkChatbot(self.webhook, secret=self.secret or None)
        return self._bot

    def send(self, title: str, content: str) -> None:
        if not self.enabled():
            return
        self._get_bot().send_markdown(title=title, text=content)


async def analyze_image(
    client: AIClient,
    image_path: Path,
    *,
    model: str,
    user_prompt: str,
) -> AnalysisResult:
    raw = await client.chat_with_image(
        user_prompt,
        image_path,
        model=model,
        system_prompt=DEFAULT_SYSTEM_PROMPT,
    )

    file_sha1 = sha1_of_file(image_path)
    result = AnalysisResult(
        image_path=str(image_path),
        image_sha1=file_sha1,
        analyzed_at=datetime.now().isoformat(timespec="seconds"),
        raw_response=raw,
    )

    try:
        data = extract_json_object(raw)
        result.品种名称 = str(data.get("品种名称", "未知"))
        result.周期 = str(data.get("周期", "未知"))
        result.形态归类 = str(data.get("形态归类", "其他"))
        result.买点类型 = str(data.get("买点类型", "其他"))
        key_points = data.get("关键点位")
        result.关键点位 = key_points if isinstance(key_points, dict) else {}
        result.规则命中 = ensure_list(data.get("规则命中"))
        result.涨幅阈值 = str(data.get("涨幅阈值", "未知"))
        result.量能约束 = str(data.get("量能约束", "未知"))
        result.当前是否进入进场准备 = ensure_bool(data.get("当前是否进入进场准备"))
        result.最终进场触发条件是否成立 = ensure_bool(data.get("最终进场触发条件是否成立"))
        result.进场触发 = str(data.get("进场触发", "未知"))
        result.入场确认条件 = ensure_list(data.get("入场确认条件"))
        result.止损位 = str(data.get("止损位", "未知"))
        result.失效点 = str(data.get("失效点", "未知"))
        result.是否强信号 = ensure_bool(data.get("是否强信号"))
        result.信号强度 = str(data.get("信号强度", "弱"))
        result.K线和成交量 = str(data.get("K线和成交量", "未知"))
        result.指标线 = ensure_list(data.get("指标线"))
        result.原本想买的位置 = str(data.get("原本想买的位置", "未标注"))
        result.相似度判断 = str(data.get("相似度判断", "低"))
        result.是否通知 = ensure_bool(data.get("是否通知"))
        result.通知理由 = str(data.get("通知理由", ""))
        result.摘要 = str(data.get("摘要", ""))
    except Exception as exc:
        result.parse_error = str(exc)

    return result


def collect_candidates(
    watch_dir: Path,
    state: dict[str, Any],
    min_age: float,
) -> list[Path]:
    now = datetime.now().timestamp()
    processed = state.setdefault("processed", {})
    files: list[Path] = []

    for path in sorted(watch_dir.iterdir(), key=lambda p: p.stat().st_mtime):
        if not path.is_file() or path.suffix.lower() not in WATCH_SUFFIXES:
            continue
        if now - path.stat().st_mtime < min_age:
            continue

        key = str(path.resolve())
        marker = f"{path.stat().st_mtime}:{path.stat().st_size}"
        if processed.get(key) == marker:
            continue
        files.append(path)

    return files


async def process_once(
    args: argparse.Namespace,
    client: AIClient | None,
    model_name: str,
    user_prompt: str,
    notifier: DingTalkNotifier,
) -> int:
    watch_dir = Path(args.watch_dir).expanduser().resolve()
    result_dir = Path(args.result_dir).expanduser().resolve()
    state_file = Path(args.state_file).expanduser().resolve()
    alerts_dir = result_dir / "alerts"

    watch_dir.mkdir(parents=True, exist_ok=True)
    state = load_state(state_file)
    processed = state.setdefault("processed", {})

    if args.baseline_now:
        for path in watch_dir.iterdir():
            if path.is_file() and path.suffix.lower() in WATCH_SUFFIXES:
                processed[str(path.resolve())] = f"{path.stat().st_mtime}:{path.stat().st_size}"
        save_state(state_file, state)
        logger.info("已建立基线，当前目录文件全部标记为已处理: {}", watch_dir)
        return 0

    target_types = {
        item.strip() for item in args.notify_types.split(",")
        if item.strip()
    } or DEFAULT_TARGET_TYPES

    if client is None:
        raise RuntimeError("AI 客户端未初始化")

    candidates = collect_candidates(watch_dir, state, args.min_age)
    if args.max_files > 0:
        candidates = candidates[:args.max_files]

    if not candidates:
        logger.info("没有发现待分析图片: {}", watch_dir)
        return 0

    logger.info("发现 {} 张待分析图片", len(candidates))

    hit_count = 0
    for path in candidates:
        logger.info("开始分析: {}", path.name)
        try:
            result = await analyze_image(
                client,
                path,
                model=model_name,
                user_prompt=user_prompt,
            )

            result.是否通知 = should_notify(result, target_types, args.strong_only)
            json_path, md_path = save_result_files(result_dir, result, path)

            processed[str(path.resolve())] = f"{path.stat().st_mtime}:{path.stat().st_size}"
            save_state(state_file, state)

            logger.info(
                "分析完成 | 类型={} | 强信号={} | 相似度={} | 通知={}",
                result.买点类型,
                result.是否强信号,
                result.相似度判断,
                result.是否通知,
            )

            if result.parse_error:
                logger.warning("结果解析失败，已保存原始响应: {}", result.parse_error)

            if result.是否通知:
                hit_count += 1
                alert_image = copy_alert_image(alerts_dir, path)
                markdown = build_markdown(result)
                logger.warning("命中目标图形: {}", path.name)
                logger.warning("\n{}", markdown)

                if args.sound:
                    configure_sound()
                    from guanlan.core.services.sound import play as play_sound

                    play_sound(args.sound_type)

                if notifier.enabled():
                    title = f"图片买点提醒 - {result.品种名称} {result.买点类型}"
                    notifier.send(title, markdown)

                logger.info("结果文件: {} | {}", json_path, md_path)
                logger.info("提醒图片副本: {}", alert_image)

        except Exception as exc:
            logger.error("分析失败 {}: {}", path.name, exc)

    logger.info("本轮结束，命中 {} 张", hit_count)
    return hit_count


async def main_async() -> int:
    args = parse_args()

    webhook, secret = load_notification_config(args)
    notifier = DingTalkNotifier(webhook, secret)

    logger.info("监控目录: {}", Path(args.watch_dir).expanduser().resolve())
    logger.info("结果目录: {}", Path(args.result_dir).expanduser().resolve())
    if notifier.enabled():
        logger.info("钉钉通知: 已启用")
    else:
        logger.info("钉钉通知: 未启用")

    if args.baseline_now:
        await process_once(args, client=None, model_name="", user_prompt="", notifier=notifier)
        return 0

    config_path = Path(args.ai_config).expanduser().resolve()
    config = AIConfig(config_path)
    model_name = pick_model(config, args.model.strip())
    client = AIClient(config)
    user_prompt = load_prompt(args.prompt_file)

    logger.info("使用模型: {}", model_name)

    if args.once:
        await process_once(args, client, model_name, user_prompt, notifier)
        return 0

    while True:
        await process_once(args, client, model_name, user_prompt, notifier)
        await asyncio.sleep(args.poll_interval)


def main() -> int:
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("用户中断，退出监控")
        return 0
    except Exception as exc:
        logger.error("启动失败: {}", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
