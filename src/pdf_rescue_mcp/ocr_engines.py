from __future__ import annotations

import importlib.util
import contextlib
import os
import re
import shutil
import sys
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fitz
from PIL import Image

from .models import PageRecord
from .paths import temporary_directory


_GPU_DLL_HANDLES: list[Any] = []
_GPU_DLL_DIRECTORIES: set[str] = set()


def prepare_paddle_gpu_dlls() -> list[str]:
    """Expose bundled NVIDIA DLL folders before Paddle loads them on Windows."""
    if sys.platform != "win32" or not hasattr(os, "add_dll_directory"):
        return []
    registered: list[str] = []
    for site_packages in (Path(path) for path in sys.path if path):
        nvidia_root = site_packages / "nvidia"
        if not nvidia_root.is_dir():
            continue
        for bin_dir in nvidia_root.glob("*/bin"):
            path = str(bin_dir.resolve())
            if path in _GPU_DLL_DIRECTORIES or not any(bin_dir.glob("*.dll")):
                continue
            try:
                handle = os.add_dll_directory(path)
            except OSError:
                continue
            _GPU_DLL_HANDLES.append(handle)
            _GPU_DLL_DIRECTORIES.add(path)
            registered.append(path)
    return registered


@contextlib.contextmanager
def _quiet_native_output():
    stdout_fd = os.dup(1)
    stderr_fd = os.dup(2)
    with open(os.devnull, "w", encoding="utf-8") as devnull:
        try:
            os.dup2(devnull.fileno(), 1)
            os.dup2(devnull.fileno(), 2)
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    yield
        finally:
            os.dup2(stdout_fd, 1)
            os.dup2(stderr_fd, 2)
            os.close(stdout_fd)
            os.close(stderr_fd)


@dataclass
class OcrLine:
    text: str
    confidence: float
    bbox: list[Any] | None = None


def _cpu_thread_count(logical_processors: int | None = None) -> int:
    configured = os.environ.get("PDF_RESCUE_OCR_THREADS")
    if configured and logical_processors is None:
        try:
            requested = int(configured)
        except ValueError:
            requested = 0
        if requested > 0:
            return max(1, min(4, requested))
    count = logical_processors if logical_processors is not None else os.cpu_count()
    count = count or 1
    # 线程数优化：
    # - PaddleOCR 检测(CNN)和识别(RNN)流水线在物理核心数附近最优
    # - 超过物理核心数(8)后HyperThreading收益递减，反而因争抢变慢
    # - 保留2个逻辑核给系统+MCP服务
    # - AMD Ryzen 7 5800H: 8物理核/16逻辑核 -> 最优6-8线程
    # Batch workers deliberately stay within the 1–4 thread scheduling contract.
    physical_cores = count // 2 if count >= 8 else count  # 粗略估算物理核数
    # Do not reserve both cores on a two-core machine: that would make a
    # nominally dual-core device run a single OCR thread for no benefit.
    reserved = 2 if count >= 4 else 0
    optimal = min(physical_cores, 4)  # 上限4线程，避免过度争抢
    return max(1, min(count - reserved, optimal))


def _coerce_bbox(value: Any) -> list[Any] | None:
    if hasattr(value, "tolist"):
        value = value.tolist()
    if isinstance(value, tuple):
        value = list(value)
    return value if isinstance(value, list) else None


def _bbox_points(bbox: list[Any] | None) -> list[tuple[float, float]]:
    if not bbox:
        return []
    if all(isinstance(item, (int, float)) for item in bbox) and len(bbox) >= 4:
        return [(float(bbox[0]), float(bbox[1])), (float(bbox[2]), float(bbox[3]))]
    points: list[tuple[float, float]] = []
    for item in bbox:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            try:
                points.append((float(item[0]), float(item[1])))
            except (TypeError, ValueError):
                continue
    return points


def _line_position(line: OcrLine) -> tuple[float, float, float] | None:
    points = _bbox_points(line.bbox)
    if not points:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return min(xs), min(ys), (min(xs) + max(xs)) / 2


def _line_geometry(line: OcrLine) -> tuple[float, float, float, float] | None:
    points = _bbox_points(line.bbox)
    if not points:
        return None
    xs = [point[0] for point in points]
    ys = [point[1] for point in points]
    return min(xs), min(ys), max(xs), max(ys)


def _sort_lines_for_reading(lines: list[OcrLine]) -> list[OcrLine]:
    positioned = [(line, geometry) for line in lines if (geometry := _line_geometry(line))]
    if len(positioned) < 20:
        return lines

    layout_positioned = [
        (line, geometry)
        for line, geometry in positioned
        if len(re.sub(r"\s+", "", line.text)) >= 8 and geometry[2] - geometry[0] >= 80
    ]
    if len(layout_positioned) < 16:
        layout_positioned = positioned

    starts = sorted(geometry[0] for _, geometry in layout_positioned)
    minimum_cluster_size = max(5, round(len(starts) * 0.15))
    content_left = min(geometry[0] for _, geometry in layout_positioned)
    content_right = max(geometry[2] for _, geometry in layout_positioned)
    content_width = content_right - content_left
    gap_candidates = [
        (starts[index + 1] - starts[index], index)
        for index in range(minimum_cluster_size - 1, len(starts) - minimum_cluster_size)
    ]
    if not gap_candidates:
        return lines

    minimum_gap = max(90.0, content_width * 0.1)
    candidate_gutters: dict[tuple[float, int], float] = {}
    for candidate in gap_candidates:
        gap, index = candidate
        if gap < minimum_gap:
            continue
        split_start = (starts[index] + starts[index + 1]) / 2
        left_edges = sorted(
            geometry[2] for _, geometry in layout_positioned if geometry[0] <= split_start
        )
        right_starts = sorted(
            geometry[0] for _, geometry in layout_positioned if geometry[0] > split_start
        )
        if not left_edges or not right_starts:
            continue
        left_edge = left_edges[round((len(left_edges) - 1) * 0.9)]
        right_start = right_starts[0]
        gutter = right_start - left_edge
        gutter_midpoint = (left_edge + right_start) / 2
        relative_gutter_midpoint = (gutter_midpoint - content_left) / max(content_width, 1.0)
        if gutter >= max(15.0, content_width * 0.01) and 0.25 <= relative_gutter_midpoint <= 0.75:
            candidate_gutters[candidate] = gutter
    eligible_gaps = list(candidate_gutters)
    if not eligible_gaps:
        return lines

    def gap_score(candidate: tuple[float, int]) -> float:
        gap, index = candidate
        left_size = index + 1
        right_size = len(starts) - left_size
        balance = min(left_size, right_size) / max(left_size, right_size)
        gutter = candidate_gutters[candidate]
        return (gutter + gap * 0.2) * balance

    largest_gap, split_index = max(eligible_gaps, key=gap_score)
    left_count = split_index + 1
    right_count = len(starts) - left_count
    if (
        min(left_count, right_count) < 5
        or largest_gap < minimum_gap
    ):
        return lines

    split_x = (starts[split_index] + starts[split_index + 1]) / 2
    left: list[tuple[OcrLine, tuple[float, float, float, float]]] = []
    right: list[tuple[OcrLine, tuple[float, float, float, float]]] = []
    for line, geometry in positioned:
        target = left if geometry[0] <= split_x else right
        target.append((line, geometry))

    layout_left = [(line, geometry) for line, geometry in layout_positioned if geometry[0] <= split_x]
    layout_right = [(line, geometry) for line, geometry in layout_positioned if geometry[0] > split_x]
    left_start = sorted(geometry[0] for _, geometry in layout_left)[len(layout_left) // 2]
    right_start = sorted(geometry[0] for _, geometry in layout_right)[len(layout_right) // 2]
    left_y_min = min(geometry[1] for _, geometry in layout_left)
    left_y_max = max(geometry[3] for _, geometry in layout_left)
    right_y_min = min(geometry[1] for _, geometry in layout_right)
    right_y_max = max(geometry[3] for _, geometry in layout_right)
    vertical_overlap = max(0.0, min(left_y_max, right_y_max) - max(left_y_min, right_y_min))
    shorter_vertical_span = min(left_y_max - left_y_min, right_y_max - right_y_min)
    overlap_ratio = vertical_overlap / shorter_vertical_span if shorter_vertical_span > 0 else 0.0
    if right_start - left_start < content_width * 0.35 or overlap_ratio < 0.35:
        return lines

    sorted_positioned = [
        line
        for column in (left, right)
        for line, _ in sorted(column, key=lambda item: (item[1][1], item[1][0]))
    ]
    positioned_ids = {id(line) for line, _ in positioned}
    trailing = [line for line in lines if id(line) not in positioned_ids]
    return sorted_positioned + trailing


def text_from_ocr_blocks_in_reading_order(blocks: list[dict[str, Any]]) -> str:
    lines = [
        OcrLine(
            text=str(block.get("text") or block.get("文本") or "").strip(),
            confidence=float(block.get("confidence") or block.get("置信度") or 0.0),
            bbox=_coerce_bbox(block.get("bbox") or block.get("位置")),
        )
        for block in blocks
        if str(block.get("text") or block.get("文本") or "").strip()
    ]
    return "\n".join(line.text for line in _sort_lines_for_reading(lines)).strip()


def available_ocr_engine() -> str | None:
    if (
        importlib.util.find_spec("paddleocr") is not None
        and importlib.util.find_spec("paddle") is not None
    ):
        return "paddleocr"
    if _tesseract_available():
        return "tesseract"
    return None


def _tesseract_available() -> bool:
    return importlib.util.find_spec("pytesseract") is not None and bool(shutil.which("tesseract"))


def render_pdf_page(
    pdf_path: Path,
    page_index: int,
    output_path: Path,
    dpi: int = 300,
    rotation: int = 0,
    password: str | None = None,
) -> None:
    zoom = dpi / 72
    matrix = fitz.Matrix(zoom, zoom)
    if rotation % 360:
        matrix = matrix.prerotate(rotation % 360)
    with fitz.open(pdf_path) as doc:
        if doc.needs_pass:
            if not password or not doc.authenticate(password):
                raise PermissionError("PDF需要正确密码才能渲染页面。")
        page = doc.load_page(page_index)
        pix = page.get_pixmap(matrix=matrix, alpha=False)
        pix.save(output_path)


def _is_blank_page_image(image_path: Path) -> bool:
    image = Image.open(image_path).convert("L")
    width, height = image.size
    total = width * height
    pixels = image.tobytes()
    dark_ratio = sum(1 for pixel in pixels if pixel < 210) / total
    very_dark_ratio = sum(1 for pixel in pixels if pixel < 120) / total
    if dark_ratio >= 0.005 or very_dark_ratio >= 0.003:
        return False

    # 扫描空白页常只剩一条底边污迹；真正的稀疏索引页仍会形成成片的横、竖笔画。
    row_counts = [0] * height
    column_counts = [0] * width
    for index, pixel in enumerate(pixels):
        if pixel < 210:
            row_counts[index // width] += 1
            column_counts[index % width] += 1
    active_rows = sum(count >= 8 for count in row_counts)
    active_columns = sum(count >= 8 for count in column_counts)
    return active_rows < 20 and active_columns < 8


def _paddle_gpu_ready(paddle: Any) -> bool:
    try:
        compiled = bool(paddle.device.is_compiled_with_cuda()) or bool(
            getattr(paddle.device, "is_compiled_with_rocm", lambda: False)()
        )
        if not compiled or paddle.device.cuda.device_count() < 1:
            return False
        paddle.set_device("gpu:0")
        probe = paddle.to_tensor([1.0], dtype="float32")
        result = (probe + probe).numpy().tolist()
        return bool(result and abs(float(result[0]) - 2.0) < 1e-6)
    except Exception:
        try:
            paddle.set_device("cpu")
        except Exception:
            pass
        return False


class PaddleOcrAdapter:
    def __init__(self, model_size: str = "small", force_cpu: bool = False) -> None:
        os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        prepare_paddle_gpu_dlls()
        with _quiet_native_output():
            import paddle
            from paddleocr import PaddleOCR

        if model_size == "medium":
            detection_model = "PP-OCRv6_medium_det"
            recognition_model = "PP-OCRv6_medium_rec"
        else:
            detection_model = "PP-OCRv6_small_det"
            recognition_model = "PP-OCRv6_small_rec"

        self.model_size = model_size
        self.device = "cpu"
        self.gpu_confirmed = False
        self.gpu_fallback_reason: str | None = None
        use_gpu = not force_cpu and _paddle_gpu_ready(paddle)

        def build_engine(target_device: str):
            paddle.set_device("gpu:0" if target_device == "gpu" else "cpu")
            try:
                with _quiet_native_output():
                    return PaddleOCR(
                        lang="ch",
                        device=target_device,
                        engine="paddle_static",
                        enable_mkldnn=False,
                        cpu_threads=_cpu_thread_count(),
                        text_detection_model_name=detection_model,
                        text_recognition_model_name=recognition_model,
                        use_doc_orientation_classify=False,
                        use_doc_unwarping=False,
                        use_textline_orientation=False,
                    )
            except TypeError:
                legacy_kwargs: dict[str, Any] = {
                    "lang": "ch",
                    "use_angle_cls": True,
                    "show_log": False,
                }
                if target_device == "gpu":
                    legacy_kwargs["use_gpu"] = True
                try:
                    with _quiet_native_output():
                        return PaddleOCR(**legacy_kwargs)
                except TypeError:
                    legacy_kwargs.pop("use_gpu", None)
                    with _quiet_native_output():
                        return PaddleOCR(**legacy_kwargs)

        try:
            self.engine = build_engine("gpu" if use_gpu else "cpu")
        except Exception as exc:
            if not use_gpu:
                raise
            self.gpu_fallback_reason = f"图形处理器模型初始化失败，已回退处理器：{type(exc).__name__}。"
            self.engine = build_engine("cpu")

        active_device = str(getattr(paddle, "get_device", lambda: "cpu")())
        self.device = "gpu" if active_device.startswith("gpu") else "cpu"
        self.gpu_confirmed = self.device == "gpu"

    def recognize(self, image_path: Path) -> list[OcrLine]:
        try:
            return self._recognize_current(image_path)
        except Exception as exc:
            if self.device != "gpu":
                raise
            self.gpu_fallback_reason = f"图形处理器首次推理失败，已回退处理器：{type(exc).__name__}。"
            fallback = PaddleOcrAdapter(model_size=self.model_size, force_cpu=True)
            self.engine = fallback.engine
            self.device = fallback.device
            self.gpu_confirmed = False
            return self._recognize_current(image_path)

    def _recognize_current(self, image_path: Path) -> list[OcrLine]:
        if hasattr(self.engine, "predict"):
            return self._recognize_predict(image_path)
        if hasattr(self.engine, "ocr"):
            return self._recognize_legacy(image_path)
        raise RuntimeError("Unsupported PaddleOCR API")

    def _recognize_legacy(self, image_path: Path) -> list[OcrLine]:
        with _quiet_native_output():
            result = self.engine.ocr(str(image_path))
        lines: list[OcrLine] = []
        if not result:
            return lines
        page_result = result[0] if isinstance(result, list) and result and isinstance(result[0], list) else result
        for item in page_result:
            try:
                bbox, payload = item
                text, score = payload
                lines.append(OcrLine(text=str(text), confidence=float(score), bbox=_coerce_bbox(bbox)))
            except Exception:
                continue
        return _sort_lines_for_reading(lines)

    def _recognize_predict(self, image_path: Path) -> list[OcrLine]:
        with _quiet_native_output():
            results = self.engine.predict(str(image_path))
        lines: list[OcrLine] = []
        for result in results if isinstance(results, list) else [results]:
            data: dict[str, Any]
            if hasattr(result, "json"):
                data = result.json
            elif isinstance(result, dict):
                data = result
            else:
                data = getattr(result, "res", {})
            if isinstance(data, dict) and "res" in data and isinstance(data["res"], dict):
                data = data["res"]
            texts = data.get("rec_texts") or data.get("texts") or []
            scores = data.get("rec_scores") or data.get("scores") or []
            boxes = data.get("dt_polys") or data.get("rec_boxes") or data.get("boxes") or []
            for index, text in enumerate(texts):
                score = float(scores[index]) if index < len(scores) else 0.0
                bbox = _coerce_bbox(boxes[index]) if index < len(boxes) else None
                lines.append(OcrLine(text=str(text), confidence=score, bbox=bbox))
        return _sort_lines_for_reading(lines)


class TesseractAdapter:
    device = "cpu"
    gpu_confirmed = False

    def recognize(self, image_path: Path) -> list[OcrLine]:
        import pytesseract

        image = Image.open(image_path)
        data = pytesseract.image_to_data(
            image,
            lang="chi_sim+eng",
            output_type=pytesseract.Output.DICT,
            config="--psm 6",
        )
        lines: list[OcrLine] = []
        count = len(data.get("text", []))
        for index in range(count):
            text = str(data["text"][index]).strip()
            if not text:
                continue
            try:
                conf = float(data["conf"][index])
            except ValueError:
                conf = 0.0
            bbox = [
                float(data["left"][index]),
                float(data["top"][index]),
                float(data["left"][index] + data["width"][index]),
                float(data["top"][index] + data["height"][index]),
            ]
            lines.append(OcrLine(text=text, confidence=max(conf, 0.0) / 100.0, bbox=bbox))
        return _sort_lines_for_reading(lines)


def create_ocr_adapter(model_size: str = "small") -> tuple[str, PaddleOcrAdapter | TesseractAdapter]:
    engine_name = available_ocr_engine()
    if not engine_name:
        raise RuntimeError("没有可用OCR引擎。请安装飞桨OCR或备用OCR扩展。")

    if engine_name == "paddleocr":
        try:
            return engine_name, PaddleOcrAdapter(model_size=model_size)
        except Exception as exc:
            if _tesseract_available():
                return "tesseract", TesseractAdapter()
            raise RuntimeError("飞桨OCR初始化失败，且没有可用备用OCR。") from exc
    return engine_name, TesseractAdapter()


def ocr_pdf_page(
    pdf_path: Path,
    page_number: int,
    adapter: PaddleOcrAdapter | TesseractAdapter | None = None,
    engine_name: str | None = None,
    dpi: int = 300,
    rotation: int = 0,
    password: str | None = None,
) -> tuple[str, PageRecord]:
    if adapter is None or engine_name is None:
        engine_name, adapter = create_ocr_adapter()
    ocr_device = getattr(adapter, "device", "cpu")
    gpu_confirmed = bool(getattr(adapter, "gpu_confirmed", False))
    runtime_warning = getattr(adapter, "gpu_fallback_reason", None)

    with temporary_directory("ocr", prefix="page") as tmp:
        image_path = tmp / f"page-{page_number:05d}.png"
        render_kwargs: dict[str, Any] = {}
        if rotation % 360:
            render_kwargs["rotation"] = rotation
        render_pdf_page(pdf_path, page_number - 1, image_path, dpi=dpi, password=password, **render_kwargs)
        if _is_blank_page_image(image_path):
            return (
                engine_name,
                PageRecord(
                    page_number=page_number,
                    text="",
                    confidence=1.0,
                    source="blank_page",
                    warnings=([runtime_warning] if runtime_warning else []) + ["疑似空白页"],
                    blocks=[],
                    ocr_device=ocr_device,
                    gpu_confirmed=gpu_confirmed,
                ),
            )
        lines = adapter.recognize(image_path)

    text = "\n".join(line.text for line in lines).strip()
    confidence = sum(line.confidence for line in lines) / len(lines) if lines else 0.0
    return (
        engine_name,
        PageRecord(
            page_number=page_number,
            text=text,
            confidence=round(confidence, 4),
            source=engine_name,
            warnings=([runtime_warning] if runtime_warning else [])
            + ([] if text else ["OCR没有返回文本"]),
            blocks=[
                {"text": line.text, "confidence": line.confidence, "bbox": line.bbox}
                for line in lines
            ],
            ocr_device=getattr(adapter, "device", ocr_device),
            gpu_confirmed=bool(getattr(adapter, "gpu_confirmed", gpu_confirmed)),
        ),
    )


def ocr_pdf_pages(
    pdf_path: Path,
    dpi: int = 300,
    max_pages: int | None = None,
    password: str | None = None,
) -> tuple[str, list[PageRecord]]:
    engine_name, adapter = create_ocr_adapter()
    pages: list[PageRecord] = []
    with fitz.open(pdf_path) as doc:
        count = min(doc.page_count, max_pages) if max_pages else doc.page_count

    for page_number in range(1, count + 1):
        _, page = ocr_pdf_page(
            pdf_path,
            page_number,
            adapter=adapter,
            engine_name=engine_name,
            dpi=dpi,
            password=password,
        )
        pages.append(page)
    return engine_name, pages
