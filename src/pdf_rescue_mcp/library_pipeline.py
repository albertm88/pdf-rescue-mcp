from __future__ import annotations

import csv
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

from .book_pipeline import audit_job_quality, extract_book_text
from .models import ensure_path
from .pdf_inspector import inspect_pdf_text_layer
from .zh import zh_data

SKIP_DIR_NAMES = {
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "pdf-rescue-mcp",
    "_mcp实战输出",
    "pdf_rescue_output",
}


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_name(path: Path) -> str:
    name = re.sub(r"[^\w\u4e00-\u9fff.-]+", "-", path.stem, flags=re.UNICODE).strip("-")
    return name or "未命名书籍"


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _iter_pdf_paths(root: Path) -> Iterable[Path]:
    candidates = (path for path in root.rglob("*") if path.is_file() and path.suffix.lower() == ".pdf")
    for path in sorted(candidates, key=lambda item: str(item)):
        if any(part in SKIP_DIR_NAMES for part in path.relative_to(root).parts[:-1]):
            continue
        yield path


def _job_dir_for_pdf(pdf_path: Path, library_root: Path, output_root: Path) -> Path:
    relative_parent = pdf_path.parent.relative_to(library_root)
    return output_root / relative_parent / f"{_safe_name(pdf_path)}-rescue-result"


def _existing_job_dir_for_pdf(pdf_path: Path, library_root: Path, output_root: Path) -> Path:
    current = _job_dir_for_pdf(pdf_path, library_root, output_root)
    if current.exists():
        return current
    legacy_root = library_root / "_mcp实战输出" / "书库批处理"
    legacy = legacy_root / pdf_path.parent.relative_to(library_root) / f"{_safe_name(pdf_path)}-救援结果"
    return legacy if legacy.exists() else current


def _default_library_output_root(library_root: Path) -> Path:
    configured_root = os.environ.get("PDF_RESCUE_OUTPUT_ROOT")
    root = ensure_path(configured_root) if configured_root else library_root / "pdf_rescue_output"
    return root / "library_batch"


def _read_status(job_dir: Path) -> dict | None:
    status_path = job_dir / "状态.json"
    if not status_path.exists():
        return None
    return json.loads(status_path.read_text(encoding="utf-8"))


def _write_catalog_files(output_root: Path, records: list[dict], payload: dict) -> dict:
    catalog_json = output_root / "书库清单.json"
    catalog_csv = output_root / "书库清单.csv"
    catalog_md = output_root / "书库清单.md"
    _write_json(catalog_json, payload)
    output_root.mkdir(parents=True, exist_ok=True)
    with catalog_csv.open("w", encoding="utf-8-sig", newline="") as handle:
        fieldnames = [
            "序号",
            "文件名",
            "相对路径",
            "文件大小MB",
            "PDF类型",
            "总页数",
            "建议动作",
            "任务状态",
            "建议输出目录",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow({key: record.get(key, "") for key in fieldnames})

    lines = ["# 书库清单", "", f"生成时间：{payload['生成时间']}", ""]
    lines.append("| 序号 | 文件名 | PDF类型 | 总页数 | 任务状态 |")
    lines.append("| --- | --- | --- | --- | --- |")
    for record in records:
        lines.append(
            f"| {record['序号']} | {record['文件名']} | {record.get('PDF类型', '')} | "
            f"{record.get('总页数', '')} | {record.get('任务状态', '')} |"
        )
    catalog_md.write_text("\n".join(lines), encoding="utf-8")
    return {
        "书库清单JSON": str(catalog_json),
        "书库清单CSV": str(catalog_csv),
        "书库清单Markdown": str(catalog_md),
    }


def scan_pdf_library(
    root: str | Path,
    output_dir: str | Path | None = None,
    max_files: int | None = None,
    inspect_pages: int = 3,
) -> dict:
    library_root = ensure_path(root)
    if not library_root.exists():
        raise FileNotFoundError(str(library_root))
    output_root = ensure_path(output_dir) if output_dir else _default_library_output_root(library_root)
    records: list[dict] = []
    pdf_paths = list(_iter_pdf_paths(library_root))
    selected_paths = pdf_paths[:max_files] if max_files and max_files > 0 else pdf_paths

    for index, pdf_path in enumerate(selected_paths, start=1):
        job_dir = _existing_job_dir_for_pdf(pdf_path, library_root, output_root)
        status = _read_status(job_dir)
        record: dict = {
            "序号": index,
            "PDF路径": str(pdf_path),
            "相对路径": str(pdf_path.relative_to(library_root)),
            "文件名": pdf_path.name,
            "文件大小MB": round(pdf_path.stat().st_size / 1024 / 1024, 2),
            "建议输出目录": str(job_dir),
            "任务状态": status.get("状态", "未开始") if status else "未开始",
            "已处理页数": status.get("已处理页数") if status else 0,
            "低置信页数": status.get("低置信页数") if status else 0,
            "失败页数": status.get("失败页数") if status else 0,
        }
        if inspect_pages > 0:
            try:
                inspection = inspect_pdf_text_layer(pdf_path, max_pages=inspect_pages)
                record.update(
                    {
                        "PDF类型": zh_data(inspection.pdf_type),
                        "总页数": inspection.page_count,
                        "已检查页数": inspection.inspected_pages,
                        "建议动作": zh_data(inspection.recommended_action),
                        "扫描页比例": inspection.scanned_page_ratio,
                        "文本层质量": inspection.text_layer_quality,
                        "警告": zh_data(inspection.warnings),
                    }
                )
            except Exception as exc:
                record.update(
                    {
                        "PDF类型": "检查失败",
                        "总页数": None,
                        "建议动作": "需要人工复核",
                        "警告": [f"检查失败：{type(exc).__name__}"],
                    }
                )
        records.append(record)

    summary = {
        "根目录": str(library_root),
        "输出根目录": str(output_root),
        "发现PDF数量": len(pdf_paths),
        "本次列入数量": len(records),
        "未开始数量": sum(1 for item in records if item.get("任务状态") == "未开始"),
        "已完成数量": sum(1 for item in records if item.get("任务状态") == "完成"),
        "检查页数": inspect_pages,
        "生成时间": _now(),
    }
    payload = {"概要": summary, "书籍": records}
    payload["输出文件"] = _write_catalog_files(output_root, records, {**summary, "书籍": records})
    return payload


def batch_extract_library(
    root: str | Path,
    output_dir: str | Path | None = None,
    mode: str = "book-balanced",
    max_books: int | None = 1,
    max_pages_per_book: int | None = None,
    resume: bool = True,
) -> dict:
    library_root = ensure_path(root)
    output_root = ensure_path(output_dir) if output_dir else _default_library_output_root(library_root)
    pdf_paths = list(_iter_pdf_paths(library_root))
    results: list[dict] = []
    processed = 0
    limit = None if max_books is not None and max_books <= 0 else max_books

    for pdf_path in pdf_paths:
        job_dir = _existing_job_dir_for_pdf(pdf_path, library_root, output_root)
        status = _read_status(job_dir)
        if status and _completed_job_covers_request(job_dir, status, max_pages_per_book):
            results.append(
                {
                    "PDF路径": str(pdf_path),
                    "任务目录": str(job_dir),
                    "状态": "已跳过",
                    "原因": "已有满足页数要求的完成结果",
                }
            )
            continue
        if limit is not None and processed >= limit:
            break
        try:
            inspection = inspect_pdf_text_layer(pdf_path, max_pages=1)
        except Exception as exc:
            results.append(
                {
                    "PDF路径": str(pdf_path),
                    "任务目录": str(job_dir),
                    "状态": "已跳过",
                    "原因": f"PDF检查失败：{type(exc).__name__}",
                }
            )
            continue
        if inspection.page_count <= 0:
            results.append(
                {
                    "PDF路径": str(pdf_path),
                    "任务目录": str(job_dir),
                    "状态": "已跳过",
                    "原因": "PDF为空或无法读取页面",
                    "PDF类型": zh_data(inspection.pdf_type),
                }
            )
            continue
        result = extract_book_text(
            pdf_path,
            output_dir=job_dir,
            mode=mode,
            max_pages=max_pages_per_book,
            resume=resume,
        )
        results.append(
            {
                "PDF路径": str(pdf_path),
                "任务目录": result.job_dir,
                "状态": zh_data(result.status),
                "PDF类型": zh_data(result.pdf_type),
                "引擎": zh_data(result.engine),
                "质量报告": result.quality_path,
                "审计报告": result.audit_path,
            }
        )
        processed += 1

    summary = {
        "根目录": str(library_root),
        "输出根目录": str(output_root),
        "模式": zh_data(mode),
        "本次处理书数": processed,
        "结果记录数": len(results),
        "单书页数上限": max_pages_per_book,
        "更新时间": _now(),
    }
    payload = {"概要": summary, "结果": results}
    _write_json(output_root / "批量状态.json", payload)
    return payload


def _chunk_identity(record: dict) -> tuple[str, str, str]:
    source_path = str(record.get("来源路径") or record.get("source_path") or "")
    source_page = str(record.get("来源页") or record.get("source_page") or "")
    text = str(record.get("文本") or record.get("text") or "")
    return source_path, source_page, text


def _int_value(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _load_status_for_index(job_dir: Path) -> dict:
    status_path = job_dir / "状态.json"
    if not status_path.exists():
        return {}
    try:
        return json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"状态": "状态读取失败"}


def _load_quality_for_index(job_dir: Path) -> dict:
    quality_path = job_dir / "数据" / "质量.json"
    if not quality_path.exists():
        return {}
    try:
        return json.loads(quality_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _quality_total_pages(quality: dict) -> int:
    direct_total = _int_value(quality.get("PDF总页数") or quality.get("总页数"))
    if direct_total:
        return direct_total
    inspection = quality.get("检查结果")
    if isinstance(inspection, dict):
        return _int_value(inspection.get("总页数"))
    return 0


def _audit_summary_for_index(job_dir: Path) -> dict:
    try:
        audit = audit_job_quality(job_dir, max_issues=20, use_latest_rules=True)
    except Exception as exc:
        return {
            "状态": "巡检失败",
            "错误": f"{type(exc).__name__}: {exc}",
        }
    return {
        "状态": "已巡检",
        "页面来源": audit.get("页面来源"),
        "已巡检页数": audit.get("已巡检页数", 0),
        "尚未巡检页数": audit.get("尚未巡检页数", 0),
        "问题页数": audit.get("问题页数", 0),
        "低置信页数": audit.get("低置信页数", 0),
        "密集索引保护低置信页数": audit.get("密集索引保护低置信页数", 0),
        "无文本页数": audit.get("无文本页数", 0),
        "可自动刷新页数": audit.get("可自动刷新页数", 0),
        "分栏重排页数": audit.get("分栏重排页数", 0),
        "书内页码移除页数": audit.get("书内页码移除页数", 0),
        "页边噪声清理页数": audit.get("页边噪声清理页数", 0),
        "图表噪声清理页数": audit.get("图表噪声清理页数", 0),
        "图版乱码清理页数": audit.get("图版乱码清理页数", 0),
        "分裂标题残留页数": audit.get("分裂标题残留页数", 0),
        "图表噪声残留页数": audit.get("图表噪声残留页数", 0),
        "问题页样例": audit.get("问题页", [])[:5],
        "建议": audit.get("建议", []),
    }


def _completed_job_covers_request(
    job_dir: Path,
    status: dict,
    max_pages_per_book: int | None,
) -> bool:
    if status.get("状态") != "完成":
        return False
    processed_pages = _int_value(status.get("已处理页数") or status.get("目标页数"))
    if max_pages_per_book is not None and max_pages_per_book > 0:
        return processed_pages >= max_pages_per_book

    total_pages = _int_value(status.get("PDF总页数")) or _quality_total_pages(
        _load_quality_for_index(job_dir)
    )
    if status.get("是否抽样"):
        return False
    if total_pages and processed_pages < total_pages:
        return False
    return True


def _index_candidate(source_chunks_path: Path) -> dict:
    job_dir = source_chunks_path.parents[1]
    status = _load_status_for_index(job_dir)
    quality = _load_quality_for_index(job_dir)
    total_pages = _int_value(status.get("PDF总页数")) or _quality_total_pages(quality)
    target_pages = _int_value(status.get("目标页数"))
    is_sample = bool(status.get("是否抽样") or quality.get("是否抽样"))
    if total_pages and target_pages and target_pages < total_pages:
        is_sample = True
    return {
        "片段文件": source_chunks_path,
        "任务目录": job_dir,
        "状态": status,
        "状态文本": str(status.get("状态", "未知")),
        "来源PDF": str(status.get("来源PDF") or ""),
        "目标页数": target_pages,
        "已处理页数": _int_value(status.get("已处理页数")),
        "PDF总页数": total_pages,
        "是否抽样": is_sample,
        "片段文件大小": source_chunks_path.stat().st_size,
    }


def _candidate_score(candidate: dict) -> tuple[int, int, int, str]:
    return (
        candidate["已处理页数"],
        candidate["目标页数"],
        candidate["片段文件大小"],
        str(candidate["任务目录"]),
    )


def _select_index_candidates(
    root: Path,
    kb_dir: Path,
    include_incomplete: bool,
    include_samples: bool,
    keep_all_versions: bool,
) -> tuple[list[dict], list[dict], int]:
    candidates: list[dict] = []
    skipped: list[dict] = []
    all_chunk_paths = sorted(root.rglob("数据/片段.jsonl"), key=lambda item: str(item))

    for source_chunks_path in all_chunk_paths:
        if kb_dir in source_chunks_path.parents:
            continue
        candidate = _index_candidate(source_chunks_path)
        if not include_incomplete and candidate["状态文本"] != "完成":
            skipped.append(
                {
                    "任务目录": str(candidate["任务目录"]),
                    "状态": candidate["状态文本"],
                    "来源PDF": candidate["来源PDF"] or None,
                    "原因": "未完成或状态未知，默认不纳入知识库",
                }
            )
            continue
        if not include_samples and candidate["是否抽样"]:
            skipped.append(
                {
                    "任务目录": str(candidate["任务目录"]),
                    "状态": candidate["状态文本"],
                    "来源PDF": candidate["来源PDF"] or None,
                    "原因": "抽样任务默认不纳入正式知识库",
                }
            )
            continue
        candidates.append(candidate)

    if keep_all_versions:
        return candidates, skipped, len(all_chunk_paths)

    best_by_source: dict[str, dict] = {}
    for candidate in candidates:
        source_key = candidate["来源PDF"] or str(candidate["任务目录"])
        current = best_by_source.get(source_key)
        if current is None:
            best_by_source[source_key] = candidate
            continue
        if _candidate_score(candidate) > _candidate_score(current):
            skipped.append(
                {
                    "任务目录": str(current["任务目录"]),
                    "状态": current["状态文本"],
                    "来源PDF": current["来源PDF"] or None,
                    "原因": "同一PDF已有更完整的提取结果",
                }
            )
            best_by_source[source_key] = candidate
        else:
            skipped.append(
                {
                    "任务目录": str(candidate["任务目录"]),
                    "状态": candidate["状态文本"],
                    "来源PDF": candidate["来源PDF"] or None,
                    "原因": "同一PDF已有更完整的提取结果",
                }
            )

    selected = sorted(best_by_source.values(), key=lambda item: str(item["片段文件"]))
    return selected, skipped, len(all_chunk_paths)


def build_knowledge_base_index(
    output_root: str | Path,
    output_dir: str | Path | None = None,
    include_incomplete: bool = False,
    include_samples: bool = False,
    keep_all_versions: bool = False,
) -> dict:
    root = ensure_path(output_root)
    if not root.exists():
        raise FileNotFoundError(str(root))
    kb_dir = ensure_path(output_dir) if output_dir else root / "知识库"
    kb_dir.mkdir(parents=True, exist_ok=True)
    chunks_path = kb_dir / "知识库片段.jsonl"
    index_path = kb_dir / "知识库索引.json"

    seen: set[tuple[str, str, str]] = set()
    job_records: list[dict] = []
    chunk_count = 0
    total_chars = 0
    low_confidence_pages = 0
    failed_pages = 0
    quality_issue_pages = 0
    auto_refresh_pages = 0
    guarded_dense_low_confidence_pages = 0
    malformed_chunk_lines = 0
    selected_candidates, skipped_jobs, candidate_count = _select_index_candidates(
        root,
        kb_dir,
        include_incomplete=include_incomplete,
        include_samples=include_samples,
        keep_all_versions=keep_all_versions,
    )

    with chunks_path.open("w", encoding="utf-8") as output:
        for candidate in selected_candidates:
            source_chunks_path = candidate["片段文件"]
            job_dir = candidate["任务目录"]
            status = candidate["状态"]
            job_chunk_count = 0
            job_chars = 0
            with source_chunks_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        malformed_chunk_lines += 1
                        continue
                    identity = _chunk_identity(record)
                    if identity in seen:
                        continue
                    seen.add(identity)
                    record["任务目录"] = str(job_dir)
                    record["任务名称"] = job_dir.name
                    text = str(record.get("文本") or record.get("text") or "")
                    job_chunk_count += 1
                    chunk_count += 1
                    job_chars += len(text)
                    total_chars += len(text)
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")

            low_confidence_pages += int(status.get("低置信页数") or 0)
            failed_pages += int(status.get("失败页数") or 0)
            audit_summary = _audit_summary_for_index(job_dir)
            quality_issue_pages += _int_value(audit_summary.get("问题页数"))
            auto_refresh_pages += _int_value(audit_summary.get("可自动刷新页数"))
            guarded_dense_low_confidence_pages += _int_value(
                audit_summary.get("密集索引保护低置信页数")
            )
            job_records.append(
                {
                    "任务目录": str(job_dir),
                    "状态": status.get("状态", "未知"),
                    "来源PDF": status.get("来源PDF"),
                    "目标页数": status.get("目标页数"),
                    "已处理页数": status.get("已处理页数"),
                    "PDF总页数": candidate["PDF总页数"] or None,
                    "是否抽样": candidate["是否抽样"],
                    "低置信页数": status.get("低置信页数", 0),
                    "失败页数": status.get("失败页数", 0),
                    "片段数": job_chunk_count,
                    "字数": job_chars,
                    "质量巡检": audit_summary,
                }
            )

    payload = {
        "概要": {
            "输出根目录": str(root),
            "知识库目录": str(kb_dir),
            "候选任务数": candidate_count,
            "任务数": len(job_records),
            "跳过任务数": len(skipped_jobs),
            "片段数": chunk_count,
            "总字数": total_chars,
            "低置信页数": low_confidence_pages,
            "密集索引保护低置信页数": guarded_dense_low_confidence_pages,
            "失败页数": failed_pages,
            "质量问题页数": quality_issue_pages,
            "可自动刷新页数": auto_refresh_pages,
            "异常片段行数": malformed_chunk_lines,
            "包含抽样任务": include_samples,
            "生成时间": _now(),
        },
        "任务": job_records,
        "跳过任务": skipped_jobs,
        "输出文件": {
            "知识库片段": str(chunks_path),
            "知识库索引": str(index_path),
        },
    }
    _write_json(index_path, payload)
    return payload
