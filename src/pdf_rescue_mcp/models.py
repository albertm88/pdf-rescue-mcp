from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class PdfType(str, Enum):
    BORN_DIGITAL_FULL_TEXT = "born_digital_full_text"
    SEARCHABLE_SCANNED = "searchable_scanned_pdf"
    MIXED = "mixed_pdf"
    BROKEN_TEXT_LAYER = "broken_text_layer_pdf"
    IMAGE_ONLY_SCANNED = "image_only_scanned_pdf"
    EMPTY_OR_UNKNOWN = "empty_or_unknown_pdf"
    PASSWORD_PROTECTED = "password_protected_pdf"
    CORRUPTED = "corrupted_pdf"


class RecommendedAction(str, Enum):
    EXTRACT_DIRECTLY = "extract_directly_no_ocr"
    REUSE_TEXT_LAYER = "reuse_text_layer_with_quality_audit"
    OCR_SCANNED_PAGES = "ocr_scanned_pages_only"
    REBUILD_TEXT_LAYER = "strip_or_ignore_text_layer_and_rebuild_ocr"
    FULL_BOOK_OCR = "run_full_book_ocr"
    MANUAL_REVIEW = "manual_review_required"
    PASSWORD_REQUIRED = "password_required"
    REPAIR_REQUIRED = "repair_required"


class ToolStatus(BaseModel):
    name: str
    available: bool
    version: str | None = None
    path: str | None = None
    notes: list[str] = Field(default_factory=list)


class GpuStatus(BaseModel):
    hardware_available: bool = False
    hardware_name: str | None = None
    hardware_memory_gb: float | None = None
    driver_version: str | None = None
    compute_capability: str | None = None
    device_count: int = 0
    backend: str | None = None
    runtime_available: bool = False
    confirmed: bool = False
    device: str = "cpu"
    reason: str | None = None


class RuntimeReadiness(BaseModel):
    platform_supported: bool
    python_supported: bool
    runtime_dirs_writable: bool
    uv_available: bool
    recommended_runner: str
    notes: list[str] = Field(default_factory=list)


class RuntimeProfile(BaseModel):
    platform: str
    python: str
    cpu_count: int
    memory_gb: float | None = None
    gpu_available: bool = False
    gpu: GpuStatus = Field(default_factory=GpuStatus)
    recommended_mode: str
    max_workers: int
    max_dpi: int
    tools: dict[str, ToolStatus]
    python_packages: dict[str, bool]
    readiness: RuntimeReadiness = Field(
        default_factory=lambda: RuntimeReadiness(
            platform_supported=True,
            python_supported=True,
            runtime_dirs_writable=True,
            uv_available=False,
            recommended_runner="python",
        )
    )
    notes: list[str] = Field(default_factory=list)


class PageTextReport(BaseModel):
    page_number: int
    width: float
    height: float
    text_chars: int
    text_blocks: int
    image_blocks: int
    xobject_images: int
    image_coverage_ratio: float
    garble_score: float
    likely_scanned: bool
    likely_has_usable_text: bool
    warnings: list[str] = Field(default_factory=list)


class TextLayerInspection(BaseModel):
    path: str
    page_count: int
    inspected_pages: int
    pdf_type: PdfType
    has_extractable_text: bool
    has_outline: bool
    has_toc_like_pages: bool
    text_layer_quality: float
    garble_risk: float
    coverage_score: float
    scanned_page_ratio: float
    text_page_ratio: float
    recommended_action: RecommendedAction
    pages: list[PageTextReport]
    warnings: list[str] = Field(default_factory=list)
    encrypted: bool = False
    file_size_bytes: int | None = None
    pdf_version: str | None = None


class ChunkRecord(BaseModel):
    chunk_id: str
    book_title: str | None = None
    chapter: str | None = None
    page_start: int
    page_end: int
    text: str
    confidence: float
    source_page: int
    source_path: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class PageRecord(BaseModel):
    page_number: int
    printed_page: str | None = None
    text: str
    confidence: float
    source: str
    warnings: list[str] = Field(default_factory=list)
    blocks: list[dict[str, Any]] = Field(default_factory=list)
    ocr_device: str | None = None
    gpu_confirmed: bool = False


class ExtractionResult(BaseModel):
    status: str
    job_dir: str
    pdf_type: PdfType
    engine: str
    manifest_path: str
    markdown_path: str | None = None
    chunks_path: str | None = None
    pages_path: str | None = None
    quality_path: str | None = None
    audit_path: str | None = None
    ocr_device: str | None = None
    gpu_confirmed: bool = False
    warnings: list[str] = Field(default_factory=list)
    next_steps: list[str] = Field(default_factory=list)


def ensure_path(value: str | Path) -> Path:
    return Path(value).expanduser().resolve()
