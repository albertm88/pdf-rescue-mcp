from __future__ import annotations

import math
import re
import statistics
import unicodedata
from pathlib import Path

import fitz

from .models import (
    PageTextReport,
    PdfType,
    RecommendedAction,
    TextLayerInspection,
    ensure_path,
)

CID_PATTERN = re.compile(r"\(?cid[: ]?\d+\)?", re.IGNORECASE)
TOC_PATTERNS = [
    re.compile(r"\bcontents\b", re.IGNORECASE),
    re.compile(r"\btable\s+of\s+contents\b", re.IGNORECASE),
    re.compile(r"\bchapter\b", re.IGNORECASE),
    re.compile(r"\d+\s*[.．]\s*\S+"),
    re.compile(r"第\s*[一二三四五六七八九十百千万\d]+\s*[章节篇卷部]"),
    re.compile(r"目\s*录"),
]


class PdfAccessError(PermissionError):
    def __init__(self, message: str, *, page_count: int = 0) -> None:
        super().__init__(message)
        self.page_count = page_count


def _open_pdf(path: Path, password: str | None = None) -> tuple[fitz.Document, bool]:
    doc = fitz.open(path)
    encrypted = bool(doc.needs_pass)
    if not encrypted:
        return doc, False
    page_count = doc.page_count
    if not password:
        doc.close()
        raise PdfAccessError("PDF受密码保护，请提供密码后再处理。", page_count=page_count)
    if not doc.authenticate(password):
        doc.close()
        raise PdfAccessError("PDF密码不正确，无法读取页面。", page_count=page_count)
    return doc, True


def _clean_text(text: str) -> str:
    return "\n".join(line.strip() for line in text.replace("\x00", "").splitlines()).strip()


def _garble_score(text: str) -> float:
    if not text:
        return 0.0
    total = len(text)
    suspicious = 0
    suspicious += len(CID_PATTERN.findall(text)) * 5
    for char in text:
        code = ord(char)
        category = unicodedata.category(char)
        if char in {"\ufffd", "\u25a1", "\u25af", "\u25a0"}:
            suspicious += 3
        elif 0xE000 <= code <= 0xF8FF:
            suspicious += 2
        elif category.startswith("C") and char not in "\n\r\t":
            suspicious += 2

    non_space = [c for c in text if not c.isspace()]
    if non_space:
        meaningful = sum(
            1
            for c in non_space
            if "\u4e00" <= c <= "\u9fff" or c.isalpha() or c.isdigit()
        )
        meaningful_ratio = meaningful / len(non_space)
        if meaningful_ratio < 0.35:
            suspicious += int((0.35 - meaningful_ratio) * total)

    for match in re.finditer(r"(.)\1{8,}", text):
        suspicious += len(match.group(0))

    return min(1.0, suspicious / max(total, 1))


def _toc_like(text: str) -> bool:
    compact = re.sub(r"\s+", "", text)
    if not compact:
        return False
    hits = sum(1 for pattern in TOC_PATTERNS if pattern.search(text) or pattern.search(compact))
    dotted_lines = len(re.findall(r"\S\s*[.·．。]{2,}\s*\d+", text))
    return hits >= 2 or dotted_lines >= 3


def _page_image_coverage(page: fitz.Page, text_dict: dict) -> tuple[int, float]:
    page_area = max(page.rect.width * page.rect.height, 1.0)
    image_area = 0.0
    image_blocks = 0
    for block in text_dict.get("blocks", []):
        if block.get("type") == 1:
            image_blocks += 1
            bbox = fitz.Rect(block.get("bbox", (0, 0, 0, 0)))
            image_area += max(bbox.width, 0) * max(bbox.height, 0)
    return image_blocks, min(1.0, image_area / page_area)


def _page_report(page: fitz.Page, page_number: int) -> PageTextReport:
    text = _clean_text(page.get_text("text", sort=True))
    text_dict = page.get_text("dict")
    image_blocks, image_coverage = _page_image_coverage(page, text_dict)
    text_blocks = sum(1 for block in text_dict.get("blocks", []) if block.get("type") == 0)
    xobject_images = len(page.get_images(full=True))
    garble = _garble_score(text)
    text_chars = len(text)
    likely_scanned = image_coverage > 0.55 or (xobject_images > 0 and text_chars < 60)
    likely_has_usable_text = text_chars >= 30 and garble < 0.18

    warnings: list[str] = []
    if garble >= 0.18:
        warnings.append("文本层可能损坏或乱码")
    if likely_scanned and text_chars == 0:
        warnings.append("纯图像扫描页")
    if likely_scanned and 0 < text_chars < 80:
        warnings.append("扫描页存在稀疏文本层")

    return PageTextReport(
        page_number=page_number,
        width=round(page.rect.width, 2),
        height=round(page.rect.height, 2),
        text_chars=text_chars,
        text_blocks=text_blocks,
        image_blocks=image_blocks,
        xobject_images=xobject_images,
        image_coverage_ratio=round(image_coverage, 4),
        garble_score=round(garble, 4),
        likely_scanned=likely_scanned,
        likely_has_usable_text=likely_has_usable_text,
        warnings=warnings,
    )


def _ratio(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _inspection_page_indices(page_count: int, max_pages: int | None) -> list[int]:
    """抽样时覆盖开头、中段和结尾，避免只看目录就误判整本书。"""
    if page_count <= 0:
        return []
    if max_pages is None or max_pages >= page_count:
        return list(range(page_count))
    count = max(0, max_pages)
    if count == 0:
        return []
    if count == 1:
        return [0]

    selected = {0, page_count - 1}
    front_anchor_count = min(5, max(1, count // 4))
    selected.update(range(front_anchor_count))
    for position in range(1, count - 1):
        scaled_position = position * (page_count - 1) / (count - 1)
        selected.add(int(scaled_position + 0.5))
        if len(selected) >= count:
            break
    if len(selected) < count:
        for index in range(page_count):
            selected.add(index)
            if len(selected) >= count:
                break
    return sorted(selected)[:count]


def _classify(pages: list[PageTextReport], has_outline: bool, has_toc_like_pages: bool) -> tuple[PdfType, RecommendedAction, list[str]]:
    warnings: list[str] = []
    total = len(pages)
    text_pages = sum(1 for page in pages if page.text_chars >= 30 and page.garble_score < 0.18)
    scanned_pages = sum(1 for page in pages if page.likely_scanned)
    no_text_scanned = sum(1 for page in pages if page.likely_scanned and page.text_chars < 20)
    broken_pages = sum(1 for page in pages if page.text_chars >= 20 and page.garble_score >= 0.18)

    text_ratio = _ratio(text_pages, total)
    scanned_ratio = _ratio(scanned_pages, total)
    broken_ratio = _ratio(broken_pages, total)
    no_text_scanned_ratio = _ratio(no_text_scanned, total)

    if total == 0:
        return PdfType.EMPTY_OR_UNKNOWN, RecommendedAction.MANUAL_REVIEW, ["PDF为空"]

    if broken_ratio >= 0.2:
        warnings.append("多页文本层可疑")
        return PdfType.BROKEN_TEXT_LAYER, RecommendedAction.REBUILD_TEXT_LAYER, warnings

    if no_text_scanned_ratio >= 0.8 and text_ratio < 0.2:
        return PdfType.IMAGE_ONLY_SCANNED, RecommendedAction.FULL_BOOK_OCR, warnings

    if scanned_ratio >= 0.5 and text_ratio >= 0.5:
        return PdfType.SEARCHABLE_SCANNED, RecommendedAction.REUSE_TEXT_LAYER, warnings

    if scanned_ratio >= 0.2 and text_ratio >= 0.2:
        warnings.append("文档疑似混合了原生文本页和扫描页")
        return PdfType.MIXED, RecommendedAction.OCR_SCANNED_PAGES, warnings

    if text_ratio >= 0.8 and scanned_ratio < 0.2:
        return PdfType.BORN_DIGITAL_FULL_TEXT, RecommendedAction.EXTRACT_DIRECTLY, warnings

    if text_ratio >= 0.6 and (has_outline or has_toc_like_pages):
        return PdfType.BORN_DIGITAL_FULL_TEXT, RecommendedAction.EXTRACT_DIRECTLY, warnings

    if text_ratio < 0.2 and scanned_ratio < 0.2:
        warnings.append("可用文本或图像证据很少")
        return PdfType.EMPTY_OR_UNKNOWN, RecommendedAction.MANUAL_REVIEW, warnings

    return PdfType.MIXED, RecommendedAction.OCR_SCANNED_PAGES, warnings


def inspect_pdf_text_layer(
    path: str | Path,
    max_pages: int | None = None,
    password: str | None = None,
) -> TextLayerInspection:
    pdf_path = ensure_path(path)
    if not pdf_path.exists():
        raise FileNotFoundError(str(pdf_path))

    pages: list[PageTextReport] = []
    toc_like_pages = 0
    file_size_bytes = pdf_path.stat().st_size
    try:
        doc, encrypted = _open_pdf(pdf_path, password=password)
    except PdfAccessError as exc:
        return TextLayerInspection(
            path=str(pdf_path),
            page_count=exc.page_count,
            inspected_pages=0,
            pdf_type=PdfType.PASSWORD_PROTECTED,
            has_extractable_text=False,
            has_outline=False,
            has_toc_like_pages=False,
            text_layer_quality=0.0,
            garble_risk=1.0,
            coverage_score=0.0,
            scanned_page_ratio=0.0,
            text_page_ratio=0.0,
            recommended_action=RecommendedAction.PASSWORD_REQUIRED,
            pages=[],
            warnings=[str(exc)],
            encrypted=True,
            file_size_bytes=file_size_bytes,
        )
    except fitz.FileDataError:
        return TextLayerInspection(
            path=str(pdf_path),
            page_count=0,
            inspected_pages=0,
            pdf_type=PdfType.CORRUPTED,
            has_extractable_text=False,
            has_outline=False,
            has_toc_like_pages=False,
            text_layer_quality=0.0,
            garble_risk=1.0,
            coverage_score=0.0,
            scanned_page_ratio=0.0,
            text_page_ratio=0.0,
            recommended_action=RecommendedAction.REPAIR_REQUIRED,
            pages=[],
            warnings=["PDF结构损坏或无法读取，请先修复文件后再处理。"],
            encrypted=False,
            file_size_bytes=file_size_bytes,
        )

    with doc:
        page_count = doc.page_count
        has_outline = bool(doc.get_toc(simple=True))
        pdf_version = str((doc.metadata or {}).get("format") or "") or None
        inspect_indices = _inspection_page_indices(page_count, max_pages)
        for index in inspect_indices:
            page = doc.load_page(index)
            report = _page_report(page, index + 1)
            pages.append(report)
            if _toc_like(page.get_text("text", sort=True)):
                toc_like_pages += 1

    pdf_type, action, warnings = _classify(pages, has_outline, toc_like_pages > 0)

    if pages:
        text_pages = sum(1 for page in pages if page.likely_has_usable_text)
        scanned_pages = sum(1 for page in pages if page.likely_scanned)
        garble_values = [page.garble_score for page in pages if page.text_chars > 0]
        garble_risk = statistics.mean(garble_values) if garble_values else 0.0
        text_layer_quality = max(0.0, 1.0 - garble_risk)
        if pdf_type == PdfType.IMAGE_ONLY_SCANNED:
            text_layer_quality = 0.0
        coverage_score = text_pages / len(pages)
        scanned_ratio = scanned_pages / len(pages)
        text_ratio = text_pages / len(pages)
    else:
        garble_risk = math.nan
        text_layer_quality = 0.0
        coverage_score = 0.0
        scanned_ratio = 0.0
        text_ratio = 0.0

    if max_pages and max_pages < page_count:
        warnings.append(f"本次抽样检查了 {len(pages)} / {page_count} 页")

    return TextLayerInspection(
        path=str(pdf_path),
        page_count=page_count,
        inspected_pages=len(pages),
        pdf_type=pdf_type,
        has_extractable_text=text_ratio > 0.0,
        has_outline=has_outline,
        has_toc_like_pages=toc_like_pages > 0,
        text_layer_quality=round(text_layer_quality, 4),
        garble_risk=round(garble_risk if not math.isnan(garble_risk) else 1.0, 4),
        coverage_score=round(coverage_score, 4),
        scanned_page_ratio=round(scanned_ratio, 4),
        text_page_ratio=round(text_ratio, 4),
        recommended_action=action,
        pages=pages,
        warnings=warnings,
        encrypted=encrypted,
        file_size_bytes=file_size_bytes,
        pdf_version=pdf_version,
    )


def extract_direct_text_pages(path: str | Path, password: str | None = None) -> list[tuple[int, str]]:
    pdf_path = ensure_path(path)
    pages: list[tuple[int, str]] = []
    doc, _ = _open_pdf(pdf_path, password=password)
    with doc:
        for index in range(doc.page_count):
            text = _clean_text(doc.load_page(index).get_text("text", sort=True))
            pages.append((index + 1, text))
    return pages
