from __future__ import annotations

from pathlib import Path

import pdf_rescue_mcp.planner as planner
from pdf_rescue_mcp.models import PdfType, RecommendedAction, RuntimeProfile, TextLayerInspection


def test_planning_uses_lightweight_runtime_probe(monkeypatch) -> None:
    inspection = TextLayerInspection(
        path="book.pdf",
        page_count=100,
        inspected_pages=20,
        pdf_type=PdfType.IMAGE_ONLY_SCANNED,
        has_extractable_text=False,
        has_outline=False,
        has_toc_like_pages=False,
        text_layer_quality=0.0,
        garble_risk=0.0,
        coverage_score=0.0,
        scanned_page_ratio=1.0,
        text_page_ratio=0.0,
        recommended_action=RecommendedAction.FULL_BOOK_OCR,
        pages=[],
    )
    calls: list[bool] = []

    def fake_runtime(*, deep_ocr_probe: bool = True) -> RuntimeProfile:
        calls.append(deep_ocr_probe)
        return RuntimeProfile(
            platform="Windows",
            python="3.12",
            cpu_count=8,
            memory_gb=32.0,
            gpu_available=False,
            recommended_mode="book-balanced",
            max_workers=1,
            max_dpi=500,
            tools={},
            python_packages={},
        )

    inspected_pages: list[int | None] = []

    def fake_inspection(_: Path, max_pages: int | None = None) -> TextLayerInspection:
        inspected_pages.append(max_pages)
        return inspection

    monkeypatch.setattr(planner, "inspect_pdf_text_layer", fake_inspection)
    monkeypatch.setattr(planner, "doctor_runtime", fake_runtime)
    monkeypatch.setattr(planner, "available_ocr_engine", lambda: "paddleocr")

    result = planner.plan_pdf_job(Path("book.pdf"))

    assert calls == [False]
    assert inspected_pages == [20]
    assert result["engine"] == "paddleocr"
    assert result["dpi"] == 220
    assert result["estimated_seconds"] == 4200
