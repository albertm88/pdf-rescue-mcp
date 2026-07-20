from __future__ import annotations

from pathlib import Path

from PIL import Image, ImageDraw

import pdf_rescue_mcp.ocr_engines as ocr_engines
from pdf_rescue_mcp.ocr_engines import (
    OcrLine,
    PaddleOcrAdapter,
    _cpu_thread_count,
    _coerce_bbox,
    _is_blank_page_image,
    _sort_lines_for_reading,
    text_from_ocr_blocks_in_reading_order,
)


class ArrayLikeBox:
    def tolist(self) -> list[list[int]]:
        return [[1, 2], [3, 4], [5, 6], [7, 8]]


def test_cpu_thread_count_stays_within_device_capacity() -> None:
    assert _cpu_thread_count(1) == 1
    assert _cpu_thread_count(2) == 2
    assert _cpu_thread_count(8) == 4


def test_configured_ocr_threads_are_limited_to_the_one_to_four_worker_contract(monkeypatch) -> None:
    monkeypatch.setenv("PDF_RESCUE_OCR_THREADS", "8")

    assert _cpu_thread_count() == 4


def test_gpu_inference_failure_falls_back_to_cpu(monkeypatch, tmp_path: Path) -> None:
    class FailingEngine:
        def predict(self, image_path: str):
            raise RuntimeError("模拟图形处理器推理失败")

    class WorkingEngine:
        def predict(self, image_path: str):
            return [
                {
                    "rec_texts": ["测试"],
                    "rec_scores": [0.99],
                    "dt_polys": [[
                        [0, 0],
                        [10, 0],
                        [10, 10],
                        [0, 10],
                    ]],
                }
            ]

    adapter = object.__new__(PaddleOcrAdapter)
    adapter.model_size = "small"
    adapter.device = "gpu"
    adapter.gpu_confirmed = True
    adapter.engine = FailingEngine()

    class CpuFallback:
        def __init__(self, model_size: str = "small", force_cpu: bool = False) -> None:
            self.engine = WorkingEngine()
            self.device = "cpu"
            self.gpu_confirmed = False

    monkeypatch.setattr(ocr_engines, "PaddleOcrAdapter", CpuFallback)

    lines = adapter.recognize(tmp_path / "page.png")

    assert [line.text for line in lines] == ["测试"]
    assert adapter.device == "cpu"
    assert adapter.gpu_confirmed is False


def test_paddle_initialization_failure_falls_back_to_tesseract(monkeypatch) -> None:
    sentinel = object()

    class FailingPaddle:
        def __init__(self, model_size: str = "small") -> None:
            raise RuntimeError("模拟飞桨后端失败")

    monkeypatch.setattr(ocr_engines, "available_ocr_engine", lambda: "paddleocr")
    monkeypatch.setattr(ocr_engines, "_tesseract_available", lambda: True)
    monkeypatch.setattr(ocr_engines, "PaddleOcrAdapter", FailingPaddle)
    monkeypatch.setattr(ocr_engines, "TesseractAdapter", lambda: sentinel)

    engine, adapter = ocr_engines.create_ocr_adapter()

    assert engine == "tesseract"
    assert adapter is sentinel


def test_coerce_bbox_keeps_plain_list() -> None:
    box = [[1, 2], [3, 4], [5, 6], [7, 8]]

    assert _coerce_bbox(box) == box


def test_coerce_bbox_accepts_array_like_box() -> None:
    assert _coerce_bbox(ArrayLikeBox()) == [[1, 2], [3, 4], [5, 6], [7, 8]]


def test_sparse_index_text_is_not_treated_as_blank(tmp_path: Path) -> None:
    image_path = tmp_path / "sparse-index.png"
    image = Image.new("L", (1200, 1600), 255)
    draw = ImageDraw.Draw(image)
    for index in range(8):
        draw.text((80, 80 + index * 30), f"Y  milkvetch                         {213 + index}", fill=0)
    image.save(image_path)

    assert _is_blank_page_image(image_path) is False


def test_sort_lines_for_two_column_reading_order() -> None:
    lines: list[OcrLine] = []
    for row in range(12):
        y = row * 20
        lines.extend(
            [
                OcrLine(
                    text=f"左{row}",
                    confidence=0.9,
                    bbox=[[10, y], [110, y], [110, y + 10], [10, y + 10]],
                ),
                OcrLine(
                    text=f"右{row}",
                    confidence=0.9,
                    bbox=[[400, y], [500, y], [500, y + 10], [400, y + 10]],
                ),
            ]
        )

    sorted_lines = _sort_lines_for_reading(lines)

    assert [line.text for line in sorted_lines] == [
        *[f"左{row}" for row in range(12)],
        *[f"右{row}" for row in range(12)],
    ]


def test_sort_lines_uses_line_starts_when_column_line_widths_overlap() -> None:
    blocks: list[dict] = []
    for row in range(12):
        y = row * 30
        blocks.extend(
            [
                {
                    "text": f"左栏{row}",
                    "confidence": 0.95,
                    "bbox": [[100, y], [680, y], [680, y + 20], [100, y + 20]],
                },
                {
                    "text": f"右栏{row}",
                    "confidence": 0.95,
                    "bbox": [[800, y], [1450, y], [1450, y + 20], [800, y + 20]],
                },
            ]
        )

    text = text_from_ocr_blocks_in_reading_order(blocks)

    assert text.splitlines() == [
        *[f"左栏{row}" for row in range(12)],
        *[f"右栏{row}" for row in range(12)],
    ]


def test_sort_lines_does_not_split_single_column_indentation() -> None:
    lines = [
        OcrLine(
            text=f"第{row}行",
            confidence=0.95,
            bbox=[
                [100 if row % 2 else 240, row * 25],
                [900, row * 25],
                [900, row * 25 + 18],
                [100 if row % 2 else 240, row * 25 + 18],
            ],
        )
        for row in range(24)
    ]

    assert _sort_lines_for_reading(lines) == lines


def test_sort_lines_keeps_right_aligned_left_column_fragments() -> None:
    blocks: list[dict] = []
    for row in range(20):
        y = row * 30
        blocks.extend(
            [
                {
                    "text": f"左栏{row}",
                    "confidence": 0.95,
                    "bbox": [[150, y], [760, y], [760, y + 20], [150, y + 20]],
                },
                {
                    "text": f"右栏{row}",
                    "confidence": 0.95,
                    "bbox": [[840, y], [1480, y], [1480, y + 20], [840, y + 20]],
                },
            ]
        )
    blocks.extend(
        [
            {
                "text": "左栏靠右署名",
                "confidence": 0.95,
                "bbox": [[620, 610], [760, 610], [760, 630], [620, 630]],
            },
            {
                "text": "左栏靠右续题",
                "confidence": 0.95,
                "bbox": [[600, 640], [760, 640], [760, 660], [600, 660]],
            },
        ]
    )

    lines = text_from_ocr_blocks_in_reading_order(blocks).splitlines()

    assert lines.index("左栏靠右署名") < lines.index("右栏0")
    assert lines.index("左栏靠右续题") < lines.index("右栏0")
