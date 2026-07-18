from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from pdf_rescue_mcp.library_pipeline import (
    batch_extract_library,
    build_knowledge_base_index,
    scan_pdf_library,
)
from pdf_rescue_mcp.models import PdfType


def _touch_pdf(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"%PDF-1.4\n%%EOF\n")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _append_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def test_scan_pdf_library_skips_outputs_and_project_dirs(tmp_path: Path) -> None:
    _touch_pdf(tmp_path / "01-主系列" / "第一本.pdf")
    _touch_pdf(tmp_path / "_mcp实战输出" / "不应扫描.pdf")
    _touch_pdf(tmp_path / "pdf-rescue-mcp" / "不应扫描.pdf")

    result = scan_pdf_library(tmp_path, inspect_pages=0)

    assert result["概要"]["发现PDF数量"] == 1
    assert result["概要"]["本次列入数量"] == 1
    assert result["书籍"][0]["文件名"] == "第一本.pdf"
    assert Path(result["输出文件"]["书库清单JSON"]).exists()
    assert Path(result["输出文件"]["书库清单CSV"]).exists()
    assert Path(result["输出文件"]["书库清单Markdown"]).exists()


def test_scan_pdf_library_accepts_uppercase_pdf_extension(tmp_path: Path) -> None:
    _touch_pdf(tmp_path / "大写扩展名.PDF")

    result = scan_pdf_library(tmp_path, inspect_pages=0)

    assert result["概要"]["发现PDF数量"] == 1
    assert result["书籍"][0]["文件名"] == "大写扩展名.PDF"


def test_batch_extract_library_respects_max_books(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _touch_pdf(tmp_path / "第一本.pdf")
    _touch_pdf(tmp_path / "第二本.pdf")
    calls: list[Path] = []

    def fake_extract_book_text(
        pdf_path: Path,
        *,
        output_dir: Path,
        mode: str,
        max_pages: int | None,
        resume: bool,
    ) -> SimpleNamespace:
        calls.append(pdf_path)
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(
            job_dir=str(output_dir),
            status="ok",
            pdf_type=PdfType.IMAGE_ONLY_SCANNED,
            engine="paddleocr",
            quality_path=str(Path(output_dir) / "数据" / "质量.json"),
            audit_path=str(Path(output_dir) / "审计" / "审计.html"),
        )

    def fake_inspect_pdf_text_layer(pdf_path: Path, max_pages: int) -> SimpleNamespace:
        return SimpleNamespace(page_count=2)

    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.extract_book_text", fake_extract_book_text)
    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.inspect_pdf_text_layer", fake_inspect_pdf_text_layer)

    result = batch_extract_library(tmp_path, max_books=1, max_pages_per_book=2)

    assert result["概要"]["本次处理书数"] == 1
    assert len(calls) == 1
    assert calls[0].name == "第一本.pdf"
    assert (tmp_path / "pdf_rescue_output" / "library_batch" / "批量状态.json").exists()


def test_batch_extract_library_skips_empty_pdf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _touch_pdf(tmp_path / "空白.pdf")
    calls: list[Path] = []

    def fake_extract_book_text(*args, **kwargs) -> None:
        calls.append(args[0])

    def fake_inspect_pdf_text_layer(pdf_path: Path, max_pages: int) -> SimpleNamespace:
        return SimpleNamespace(page_count=0, pdf_type=PdfType.EMPTY_OR_UNKNOWN)

    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.extract_book_text", fake_extract_book_text)
    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.inspect_pdf_text_layer", fake_inspect_pdf_text_layer)

    result = batch_extract_library(tmp_path, max_books=1)

    assert calls == []
    assert result["结果"][0]["状态"] == "已跳过"
    assert result["结果"][0]["原因"] == "PDF为空或无法读取页面"


def test_batch_extract_library_skips_completed_sample_for_same_page_request(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "样本书.pdf"
    _touch_pdf(pdf_path)
    job_dir = tmp_path / "_mcp实战输出" / "书库批处理" / "样本书-救援结果"
    _write_json(
        job_dir / "状态.json",
        {"状态": "完成", "目标页数": 2, "已处理页数": 2, "PDF总页数": 10, "是否抽样": True},
    )
    calls: list[Path] = []

    def fake_extract_book_text(*args, **kwargs) -> None:
        calls.append(args[0])

    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.extract_book_text", fake_extract_book_text)

    result = batch_extract_library(tmp_path, max_books=1, max_pages_per_book=2)

    assert calls == []
    assert result["结果"][0]["状态"] == "已跳过"
    assert result["结果"][0]["原因"] == "已有满足页数要求的完成结果"


def test_batch_extract_library_does_not_skip_sample_when_full_book_requested(
    tmp_path: Path,
    monkeypatch,
) -> None:
    pdf_path = tmp_path / "样本书.pdf"
    _touch_pdf(pdf_path)
    job_dir = tmp_path / "_mcp实战输出" / "书库批处理" / "样本书-救援结果"
    _write_json(
        job_dir / "状态.json",
        {"状态": "完成", "目标页数": 2, "已处理页数": 2, "PDF总页数": 10, "是否抽样": True},
    )
    calls: list[Path] = []

    def fake_extract_book_text(
        pdf_path: Path,
        *,
        output_dir: Path,
        mode: str,
        max_pages: int | None,
        resume: bool,
    ) -> SimpleNamespace:
        calls.append(pdf_path)
        return SimpleNamespace(
            job_dir=str(output_dir),
            status="ok",
            pdf_type=PdfType.IMAGE_ONLY_SCANNED,
            engine="paddleocr",
            quality_path=str(Path(output_dir) / "数据" / "质量.json"),
            audit_path=str(Path(output_dir) / "审计" / "审计.html"),
        )

    def fake_inspect_pdf_text_layer(pdf_path: Path, max_pages: int) -> SimpleNamespace:
        return SimpleNamespace(page_count=10)

    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.extract_book_text", fake_extract_book_text)
    monkeypatch.setattr("pdf_rescue_mcp.library_pipeline.inspect_pdf_text_layer", fake_inspect_pdf_text_layer)

    result = batch_extract_library(tmp_path, max_books=1, max_pages_per_book=None)

    assert calls == [pdf_path]
    assert result["概要"]["本次处理书数"] == 1
    assert result["结果"][0]["状态"] == "完成"


def test_build_knowledge_base_index_deduplicates_chunks(tmp_path: Path) -> None:
    duplicate = {
        "片段编号": "书甲-p0001-001",
        "书名": "书甲",
        "起始页": 1,
        "结束页": 1,
        "来源页": 1,
        "来源路径": "books/书甲.pdf",
        "文本": "同一段知识内容",
        "置信度": 0.98,
    }
    unique = {
        "片段编号": "书乙-p0002-001",
        "书名": "书乙",
        "起始页": 2,
        "结束页": 2,
        "来源页": 2,
        "来源路径": "books/书乙.pdf",
        "文本": "另一段知识内容",
        "置信度": 0.96,
    }
    job_a = tmp_path / "任务甲"
    job_b = tmp_path / "任务乙"
    _write_json(
        job_a / "状态.json",
        {"状态": "完成", "来源PDF": "books/书甲.pdf", "低置信页数": 1, "失败页数": 0},
    )
    _write_json(
        job_b / "状态.json",
        {"状态": "完成", "来源PDF": "books/书乙.pdf", "低置信页数": 0, "失败页数": 0},
    )
    _append_jsonl(job_a / "数据" / "片段.jsonl", [duplicate])
    _append_jsonl(
        job_a / "数据" / "页面.jsonl",
        [
            {
                "页码": 1,
                "文本": "同一段知识内容",
                "置信度": 0.86,
                "来源": "飞桨OCR",
                "警告": ["页面平均置信度低于 0.90"],
            }
        ],
    )
    _append_jsonl(job_b / "数据" / "片段.jsonl", [duplicate, unique])
    _append_jsonl(
        job_b / "数据" / "页面.jsonl",
        [
            {
                "页码": 2,
                "文本": "另一段知识内容",
                "置信度": 0.96,
                "来源": "飞桨OCR",
                "警告": [],
            }
        ],
    )

    result = build_knowledge_base_index(tmp_path)

    chunks_path = Path(result["输出文件"]["知识库片段"])
    index_path = Path(result["输出文件"]["知识库索引"])
    chunks = [json.loads(line) for line in chunks_path.read_text(encoding="utf-8").splitlines()]
    assert result["概要"]["任务数"] == 2
    assert result["概要"]["片段数"] == 2
    assert result["概要"]["低置信页数"] == 1
    assert len(chunks) == 2
    assert {chunk["文本"] for chunk in chunks} == {"同一段知识内容", "另一段知识内容"}
    assert sum(1 for chunk in chunks if chunk["文本"] == "同一段知识内容") == 1
    assert all(chunk["任务名称"] in {"任务甲", "任务乙"} for chunk in chunks)
    assert result["概要"]["质量问题页数"] == 1
    assert result["任务"][0]["质量巡检"]["状态"] == "已巡检"
    assert "问题页数" in result["任务"][0]["质量巡检"]
    assert "分栏重排页数" in result["任务"][0]["质量巡检"]
    assert "书内页码移除页数" in result["任务"][0]["质量巡检"]
    assert index_path.exists()


def test_build_knowledge_base_index_prefers_most_complete_job_per_pdf(tmp_path: Path) -> None:
    partial_job = tmp_path / "旧版两页"
    complete_job = tmp_path / "新版十页"
    source_pdf = "books/同一本书.pdf"
    _write_json(
        partial_job / "状态.json",
        {"状态": "完成", "来源PDF": source_pdf, "目标页数": 2, "已处理页数": 2},
    )
    _write_json(
        complete_job / "状态.json",
        {"状态": "完成", "来源PDF": source_pdf, "目标页数": 10, "已处理页数": 10},
    )
    _append_jsonl(
        partial_job / "数据" / "片段.jsonl",
        [{"来源路径": source_pdf, "来源页": 1, "文本": "旧版片段"}],
    )
    _append_jsonl(
        complete_job / "数据" / "片段.jsonl",
        [{"来源路径": source_pdf, "来源页": 1, "文本": "新版片段"}],
    )

    result = build_knowledge_base_index(tmp_path)

    chunks_path = Path(result["输出文件"]["知识库片段"])
    chunks = [json.loads(line) for line in chunks_path.read_text(encoding="utf-8").splitlines()]
    assert result["概要"]["候选任务数"] == 2
    assert result["概要"]["任务数"] == 1
    assert result["概要"]["跳过任务数"] == 1
    assert chunks[0]["文本"] == "新版片段"
    assert result["跳过任务"][0]["原因"] == "同一PDF已有更完整的提取结果"


def test_build_knowledge_base_index_skips_sample_jobs_by_default(tmp_path: Path) -> None:
    sample_job = tmp_path / "抽样任务"
    source_pdf = "books/抽样书.pdf"
    _write_json(
        sample_job / "状态.json",
        {"状态": "完成", "来源PDF": source_pdf, "目标页数": 2, "已处理页数": 2},
    )
    _write_json(sample_job / "数据" / "质量.json", {"检查结果": {"总页数": 100}})
    _append_jsonl(
        sample_job / "数据" / "片段.jsonl",
        [{"来源路径": source_pdf, "来源页": 1, "文本": "抽样片段"}],
    )

    default_result = build_knowledge_base_index(tmp_path)
    included_result = build_knowledge_base_index(tmp_path, include_samples=True)

    assert default_result["概要"]["任务数"] == 0
    assert default_result["概要"]["跳过任务数"] == 1
    assert default_result["跳过任务"][0]["原因"] == "抽样任务默认不纳入正式知识库"
    assert included_result["概要"]["任务数"] == 1
    assert included_result["概要"]["片段数"] == 1
