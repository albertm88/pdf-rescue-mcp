from __future__ import annotations

from pathlib import Path

import fitz
from PIL import Image, ImageDraw

from pdf_rescue_mcp.models import PdfType, RecommendedAction
from pdf_rescue_mcp.pdf_inspector import inspect_pdf_text_layer


def _make_text_pdf(path: Path) -> None:
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Chapter 1\nThis is a born digital PDF with extractable text.")
    doc.save(path)
    doc.close()


def _make_image_pdf(path: Path, tmp_path: Path) -> None:
    image_path = tmp_path / "scan.png"
    image = Image.new("RGB", (1200, 1600), "white")
    draw = ImageDraw.Draw(image)
    draw.text((120, 160), "Scanned page image", fill="black")
    image.save(image_path)

    doc = fitz.open()
    page = doc.new_page(width=600, height=800)
    page.insert_image(page.rect, filename=image_path)
    doc.save(path)
    doc.close()


def _make_text_then_scan_pdf(path: Path, tmp_path: Path) -> None:
    image_path = tmp_path / "body-scan.png"
    image = Image.new("RGB", (1200, 1600), "white")
    draw = ImageDraw.Draw(image)
    draw.text((120, 160), "正文扫描页", fill="black")
    image.save(image_path)

    doc = fitz.open()
    for index in range(2):
        page = doc.new_page()
        page.insert_text((72, 72), f"目录页 {index + 1} 可抽取文本")
    for _ in range(4):
        page = doc.new_page(width=600, height=800)
        page.insert_image(page.rect, filename=image_path)
    doc.save(path)
    doc.close()


def test_inspects_born_digital_pdf(tmp_path: Path) -> None:
    pdf_path = tmp_path / "text.pdf"
    _make_text_pdf(pdf_path)

    result = inspect_pdf_text_layer(pdf_path)

    assert result.pdf_type == PdfType.BORN_DIGITAL_FULL_TEXT
    assert result.recommended_action == RecommendedAction.EXTRACT_DIRECTLY
    assert result.has_extractable_text is True


def test_inspects_image_only_pdf(tmp_path: Path) -> None:
    pdf_path = tmp_path / "scan.pdf"
    _make_image_pdf(pdf_path, tmp_path)

    result = inspect_pdf_text_layer(pdf_path)

    assert result.pdf_type == PdfType.IMAGE_ONLY_SCANNED
    assert result.recommended_action == RecommendedAction.FULL_BOOK_OCR
    assert result.has_extractable_text is False


def test_full_inspection_detects_scanned_body_after_textual_front_matter(tmp_path: Path) -> None:
    pdf_path = tmp_path / "mixed.pdf"
    _make_text_then_scan_pdf(pdf_path, tmp_path)

    result = inspect_pdf_text_layer(pdf_path)

    assert result.inspected_pages == 6
    assert result.pdf_type == PdfType.MIXED
    assert result.recommended_action == RecommendedAction.OCR_SCANNED_PAGES


def test_limited_inspection_covers_first_middle_and_last_pages(tmp_path: Path) -> None:
    pdf_path = tmp_path / "mixed.pdf"
    _make_text_then_scan_pdf(pdf_path, tmp_path)

    result = inspect_pdf_text_layer(pdf_path, max_pages=3)

    assert [page.page_number for page in result.pages] == [1, 4, 6]
    assert any("抽样检查" in warning for warning in result.warnings)


def test_password_protected_pdf_is_diagnosed_and_opens_with_password(tmp_path: Path) -> None:
    pdf_path = tmp_path / "受保护.pdf"
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Protected PDF text layer remains extractable after authentication.")
    doc.save(
        pdf_path,
        encryption=fitz.PDF_ENCRYPT_AES_256,
        user_pw="测试密码",
        owner_pw="测试密码-所有者",
    )
    doc.close()

    protected = inspect_pdf_text_layer(pdf_path)
    opened = inspect_pdf_text_layer(pdf_path, password="测试密码")

    assert protected.pdf_type == PdfType.PASSWORD_PROTECTED
    assert protected.recommended_action == RecommendedAction.PASSWORD_REQUIRED
    assert protected.encrypted is True
    assert opened.pdf_type == PdfType.BORN_DIGITAL_FULL_TEXT
    assert opened.encrypted is True
    assert opened.has_extractable_text is True


def test_structurally_invalid_pdf_returns_repair_diagnosis(tmp_path: Path) -> None:
    pdf_path = tmp_path / "损坏.pdf"
    pdf_path.write_bytes(b"this is not a PDF")

    result = inspect_pdf_text_layer(pdf_path)

    assert result.pdf_type == PdfType.CORRUPTED
    assert result.recommended_action == RecommendedAction.REPAIR_REQUIRED
    assert any("修复" in warning for warning in result.warnings)
