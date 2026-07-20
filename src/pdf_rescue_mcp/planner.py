from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import PdfType
from .ocr_engines import available_ocr_engine
from .pdf_inspector import inspect_pdf_text_layer
from .runtime import doctor_runtime


def _normalize_quality(target_quality: str) -> str:
    value = target_quality.lower().replace("_", "-")
    if value in {"fast", "book-fast"}:
        return "book-fast"
    if value in {"balanced", "auto", "book-balanced"}:
        return "book-balanced"
    if value in {"quality", "high", "book-quality"}:
        return "book-quality"
    if value in {"forensic", "archival", "book-forensic", "book-archival"}:
        return "book-forensic"
    return "book-balanced"


def _dpi_for(mode: str, max_dpi: int) -> int:
    requested = {
        "book-fast": 180,
        "book-balanced": 220,
        "book-quality": 300,
        "book-forensic": 300,
    }[mode]
    return min(requested, max_dpi)


def _estimate_seconds(
    page_count: int,
    mode: str,
    engine: str,
    direct_text: bool,
    gpu_available: bool = False,
) -> int:
    if direct_text:
        return max(1, round(page_count * 0.05))
    if engine == "paddleocr":
        base = 24.0 if gpu_available else 42.0
    else:
        base = {"tesseract": 40.0, "none": 0.0}.get(engine, 3.0)
    multiplier = {
        "book-fast": 0.75,
        "book-balanced": 1.0,
        "book-quality": 1.8,
        "book-forensic": 3.0,
    }[mode]
    return round(page_count * base * multiplier)


def plan_pdf_job(
    path: str | Path,
    target_quality: str = "balanced",
    max_seconds: int | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    # 规划只需判断文本层路线，固定抽样避免整本大扫描件阻塞交互。
    if password is None:
        inspection = inspect_pdf_text_layer(path, max_pages=20)
    else:
        inspection = inspect_pdf_text_layer(path, max_pages=20, password=password)
    # Planning is part of the interactive MCP path.  Do not wait for a stalled
    # driver or five external ``--version`` probes here; the monitoring layer
    # performs the complete health check independently.
    runtime = doctor_runtime(deep_ocr_probe=False, probe_external=False)
    mode = _normalize_quality(target_quality)
    dpi = _dpi_for(mode, runtime.max_dpi)
    direct_text = inspection.pdf_type == PdfType.BORN_DIGITAL_FULL_TEXT
    reusable_text = inspection.pdf_type == PdfType.SEARCHABLE_SCANNED and mode in {
        "book-fast",
        "book-balanced",
    }

    if inspection.pdf_type == PdfType.PASSWORD_PROTECTED:
        engine = "none"
        route = "password_required"
    elif inspection.pdf_type == PdfType.CORRUPTED:
        engine = "none"
        route = "repair_required"
    elif direct_text:
        engine = "pdf_text_layer"
        route = "direct_text_extract"
    elif reusable_text:
        engine = "existing_ocr_text_layer"
        route = "reuse_text_layer_with_audit"
    else:
        engine = available_ocr_engine() or "none"
        route = "ocr_required"

    estimated_seconds = _estimate_seconds(
        inspection.page_count,
        mode,
        engine,
        direct_text or reusable_text,
        gpu_available=runtime.gpu_available,
    )
    warnings = list(inspection.warnings)
    if engine == "none" and route == "ocr_required":
        warnings.append("需要OCR识别，但未安装可用OCR运行包")
    if max_seconds is not None and estimated_seconds > max_seconds:
        warnings.append("预计耗时超过限制；建议先用快速模式或抽样页测试。")

    if engine == "paddleocr":
        model_hint = "均衡模式用小型中文模型，高质量或取证级用中型中文模型"
    elif engine == "tesseract":
        model_hint = "备用OCR更适合简单页面，不适合作为复杂中文书主力"
    elif engine == "pdf_text_layer":
        model_hint = "不需要OCR模型"
    elif route == "password_required":
        model_hint = "请提供PDF密码后再检查或提取"
    elif route == "repair_required":
        model_hint = "请先修复PDF结构，再重新检查或提取"
    else:
        model_hint = "请安装飞桨OCR来处理中文扫描书"

    return {
        "path": str(Path(path).expanduser().resolve()),
        "pdf_type": inspection.pdf_type.value,
        "recommended_action": inspection.recommended_action.value,
        "route": route,
        "mode": mode,
        "engine": engine,
        "model_hint": model_hint,
        "dpi": dpi,
        "max_workers": runtime.max_workers,
        "estimated_seconds": estimated_seconds,
        "page_count": inspection.page_count,
        "text_layer_quality": inspection.text_layer_quality,
        "garble_risk": inspection.garble_risk,
        "runtime_recommended_mode": runtime.recommended_mode,
        "gpu_available": runtime.gpu_available,
        "gpu": runtime.gpu.model_dump(mode="json"),
        "warnings": warnings,
    }
