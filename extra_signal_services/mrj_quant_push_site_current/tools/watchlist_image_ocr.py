from __future__ import annotations

import os
import re
import subprocess
import tempfile
from functools import lru_cache
from pathlib import Path
from typing import Any

from PIL import Image, ImageEnhance, ImageFilter, ImageOps


CODE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
CODE_LINE_RE = re.compile(r"(?<!\d)(\d{6})(?!\d)")
RESAMPLING = getattr(Image, "Resampling", Image)
TESSERACT_ENV_VAR = "TESSERACT_CMD"
TESSERACT_CANDIDATES = (
    Path(r"C:\Program Files\Tesseract-OCR\tesseract.exe"),
    Path(r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"),
)


def _unique_codes_from_text(text: str) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for match in CODE_RE.findall(text or ""):
        if match in seen:
            continue
        seen.add(match)
        result.append(match)
    return result


def _is_supported_code(code: str) -> bool:
    return bool(code) and len(code) == 6 and code[0] in {"0", "2", "3", "4", "5", "6", "8", "9"}


def _clean_name(text: str) -> str:
    value = str(text or "").strip().replace(" ", "")
    value = re.sub(r"[^\u4e00-\u9fffA-Za-z0-9*]+", "", value)
    value = re.sub(r"^(SH|SZ|BJ)+", "", value, flags=re.IGNORECASE)
    return value[:32]


def _extract_rows_from_text(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        matches = list(CODE_LINE_RE.finditer(line))
        if not matches:
            continue
        for idx, match in enumerate(matches):
            code = match.group(1)
            if not _is_supported_code(code) or code in seen:
                continue
            next_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(line)
            trailing = line[match.end():next_start]
            leading = line[:match.start()] if idx == 0 else ""
            name = _clean_name(trailing) or _clean_name(leading)
            rows.append({"code": code, "name": name or code})
            seen.add(code)
    return rows


@lru_cache(maxsize=1)
def _resolve_tesseract_cmd() -> str | None:
    explicit = str(os.getenv(TESSERACT_ENV_VAR, "")).strip()
    if explicit and Path(explicit).exists():
        return explicit
    for candidate in TESSERACT_CANDIDATES:
        if candidate.exists():
            return str(candidate)
    return None


@lru_cache(maxsize=1)
def _available_tesseract_langs() -> set[str]:
    cmd = _resolve_tesseract_cmd()
    if not cmd:
        return set()
    try:
        result = subprocess.run(
            [cmd, "--list-langs"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=3,
            check=False,
        )
    except Exception:
        return set()
    values = set()
    for line in (result.stdout or "").splitlines():
        item = line.strip()
        if not item or item.lower().startswith("list of available languages"):
            continue
        values.add(item)
    return values


def _preferred_languages() -> list[str]:
    langs = _available_tesseract_langs()
    ordered: list[str] = []
    if {"chi_sim", "eng"}.issubset(langs):
        ordered.append("chi_sim+eng")
    if "eng" in langs:
        ordered.append("eng")
    return ordered or ["eng"]


def _add_margin(image: Image.Image, padding: int = 16) -> Image.Image:
    return ImageOps.expand(image, border=padding, fill="white")


def _image_variants(image: Image.Image) -> list[Image.Image]:
    base_rgb = image.convert("RGB")
    base_gray = image.convert("L")
    enlarged = base_gray.resize((base_gray.width * 2, base_gray.height * 2), RESAMPLING.LANCZOS)
    sharpened = enlarged.filter(ImageFilter.SHARPEN)
    high_contrast = ImageEnhance.Contrast(sharpened).enhance(2.2)
    thresholded = high_contrast.point(lambda px: 255 if px > 175 else 0)
    thresholded_dark = ImageOps.invert(high_contrast).point(lambda px: 255 if px > 175 else 0)
    return [
        _add_margin(base_rgb),
        _add_margin(base_gray),
        _add_margin(enlarged),
        _add_margin(thresholded),
        _add_margin(thresholded_dark),
    ]


def _run_tesseract(image_path: Path, psm: int, whitelist: bool = False, lang: str = "eng") -> str:
    tesseract_cmd = _resolve_tesseract_cmd()
    if not tesseract_cmd:
        return ""
    cmd = [tesseract_cmd, str(image_path), "stdout", "--psm", str(psm), "-l", lang]
    if whitelist:
        cmd.extend(["-c", "tessedit_char_whitelist=0123456789 "])
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="ignore",
            timeout=3,
            check=False,
        )
    except Exception:
        return ""
    if result.returncode not in (0, 1):
        return ""
    return (result.stdout or "").strip()


def _ocr_text_from_path(image_path: Path) -> str:
    with tempfile.TemporaryDirectory(prefix="watchlist_ocr_src_") as tmpdir:
        tmp_path = Path(tmpdir) / f"source{image_path.suffix or '.png'}"
        with Image.open(image_path) as image:
            image.convert("RGB").save(tmp_path)
        best_text = ""
        best_score = -1
        runs = [(6, False), (11, False), (6, True)]
        for lang in _preferred_languages():
            for psm, whitelist in runs:
                text = _run_tesseract(tmp_path, psm=psm, whitelist=whitelist, lang=lang)
                if not text:
                    continue
                rows = _extract_rows_from_text(text)
                score = len(rows) * 100 + sum(1 for row in rows if row.get("name") and row["name"] != row["code"])
                if score > best_score:
                    best_text = text
                    best_score = score
                elif not best_text and text.strip():
                    best_text = text
                if rows:
                    return text
        return best_text


def _ocr_text_from_variants(image: Image.Image) -> str:
    best_text = ""
    best_score = -1
    runs = [(6, False), (11, False), (6, True)]
    with tempfile.TemporaryDirectory(prefix="watchlist_ocr_") as tmpdir:
        tmp_root = Path(tmpdir)
        for idx, variant in enumerate(_image_variants(image)):
            image_path = tmp_root / f"variant_{idx}.png"
            variant.save(image_path)
            for lang in _preferred_languages():
                for psm, whitelist in runs:
                    text = _run_tesseract(image_path, psm=psm, whitelist=whitelist, lang=lang)
                    if not text:
                        continue
                    rows = _extract_rows_from_text(text)
                    score = len(rows) * 100 + sum(1 for row in rows if row.get("name") and row["name"] != row["code"])
                    if score > best_score:
                        best_text = text
                        best_score = score
                    elif not best_text and text.strip():
                        best_text = text
                    if rows:
                        return text
    return best_text


def extract_codes_from_image(path: str | Path) -> dict[str, Any]:
    image_path = Path(path)
    if not _resolve_tesseract_cmd():
        raise RuntimeError("Tesseract OCR is not installed or not found")
    raw_text = _ocr_text_from_path(image_path)
    if not _extract_rows_from_text(raw_text):
        with Image.open(image_path) as image:
            raw_text = _ocr_text_from_variants(image)
    rows = _extract_rows_from_text(raw_text)
    codes = [row["code"] for row in rows]
    return {
        "file_name": image_path.name,
        "codes": codes,
        "rows": rows,
        "code_count": len(codes),
        "raw_text": raw_text,
    }
