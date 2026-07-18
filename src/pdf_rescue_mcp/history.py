from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

HISTORY_FILE_NAME = "处理历史.jsonl"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def append_history_event(
    job_dir: str | Path,
    event: str,
    *,
    run_id: str | None = None,
    values: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(job_dir).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    record: dict[str, Any] = {
        "记录时间": _now(),
        "事件": event,
        "运行编号": run_id or uuid4().hex[:16],
        "任务目录": str(root),
    }
    if values:
        record.update(values)
    history_path = root / HISTORY_FILE_NAME
    with history_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
    return record


def _read_history_file(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return records
    for line in lines:
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def _load_status_snapshot(job_dir: Path) -> dict[str, Any] | None:
    status_path = job_dir / "状态.json"
    if not status_path.exists():
        return None
    try:
        value = json.loads(status_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _snapshot_record(job_dir: Path, status: dict[str, Any]) -> dict[str, Any]:
    quality: dict[str, Any] = {}
    quality_path = job_dir / "数据" / "质量.json"
    if quality_path.exists():
        try:
            loaded = json.loads(quality_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                quality = loaded
        except (OSError, UnicodeError, json.JSONDecodeError):
            pass
    return {
        "记录时间": status.get("更新时间") or _now(),
        "事件": "历史补录",
        "运行编号": None,
        "任务目录": str(job_dir),
        "来源PDF": status.get("来源PDF"),
        "状态": status.get("状态", "未知"),
        "模式": status.get("模式"),
        "引擎": status.get("引擎") or quality.get("引擎"),
        "目标页数": status.get("目标页数") or quality.get("目标页数"),
        "已处理页数": status.get("已处理页数") or quality.get("已输出页数"),
        "有文本页数": status.get("有文本页数") or quality.get("有文本页数"),
        "空白页数": status.get("空白页数") or quality.get("空白页数"),
        "低置信页数": status.get("低置信页数") or quality.get("低置信页数"),
        "失败页数": status.get("失败页数") or quality.get("失败页数"),
        "质量报告": str(quality_path) if quality_path.exists() else None,
    }


def _redact_record(record: dict[str, Any], include_sensitive: bool) -> dict[str, Any]:
    result = dict(record)
    if include_sensitive:
        return result
    for key in ("来源PDF", "任务目录", "质量报告", "审计报告"):
        value = result.get(key)
        if value:
            result[key] = Path(str(value)).name
    return result


def collect_processing_history(
    root: str | Path,
    *,
    max_records: int = 500,
    source_pdf: str | None = None,
    status: str | None = None,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    library_root = Path(root).expanduser().resolve()
    if not library_root.exists():
        raise FileNotFoundError(str(library_root))
    records: list[dict[str, Any]] = []
    history_jobs: set[Path] = set()
    for history_path in library_root.rglob(HISTORY_FILE_NAME):
        job_dir = history_path.parent.resolve()
        history_jobs.add(job_dir)
        records.extend(_read_history_file(history_path))

    for status_path in library_root.rglob("状态.json"):
        job_dir = status_path.parent.resolve()
        if job_dir in history_jobs:
            continue
        snapshot = _load_status_snapshot(job_dir)
        if snapshot:
            records.append(_snapshot_record(job_dir, snapshot))

    def matches(record: dict[str, Any]) -> bool:
        if source_pdf:
            source = str(record.get("来源PDF") or "")
            if source_pdf.lower() not in source.lower() and Path(source).name.lower() != Path(source_pdf).name.lower():
                return False
        if status and str(record.get("状态") or "") != status:
            return False
        return True

    records = [record for record in records if matches(record)]
    records.sort(key=lambda record: str(record.get("记录时间") or ""), reverse=True)
    if max_records > 0:
        records = records[:max_records]
    safe_records = [_redact_record(record, include_sensitive) for record in records]
    return {
        "根目录": str(library_root) if include_sensitive else library_root.name,
        "记录数": len(safe_records),
        "是否脱敏": not include_sensitive,
        "记录": safe_records,
        "生成时间": _now(),
    }


def _history_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# PDF处理历史",
        "",
        f"生成时间：{report['生成时间']}",
        f"记录数：{report['记录数']}",
        f"是否脱敏：{'是' if report['是否脱敏'] else '否'}",
        "",
        "| 时间 | 事件 | 来源文件 | 状态 | 引擎 | 已处理页数 | 低置信页 | 失败页 |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: |",
    ]
    for record in report["记录"]:
        lines.append(
            "| {记录时间} | {事件} | {来源PDF} | {状态} | {引擎} | {已处理页数} | {低置信页数} | {失败页数} |".format(
                记录时间=record.get("记录时间", ""),
                事件=record.get("事件", ""),
                来源PDF=record.get("来源PDF", ""),
                状态=record.get("状态", ""),
                引擎=record.get("引擎", ""),
                已处理页数=record.get("已处理页数", ""),
                低置信页数=record.get("低置信页数", ""),
                失败页数=record.get("失败页数", ""),
            )
        )
    return "\n".join(lines) + "\n"


def _history_html(report: dict[str, Any]) -> str:
    rows = []
    for record in report["记录"]:
        rows.append(
            "<tr>"
            + "".join(
                f"<td>{html.escape(str(record.get(key) or ''))}</td>"
                for key in ("记录时间", "事件", "来源PDF", "状态", "引擎", "已处理页数", "低置信页数", "失败页数")
            )
            + "</tr>"
        )
    style = (
        'body{font-family:system-ui,"Microsoft YaHei",sans-serif;margin:2rem;color:#222}'
        "table{border-collapse:collapse;width:100%}"
        "th,td{border:1px solid #ccc;padding:.45rem;text-align:left}"
        "th{background:#f2f2f2}"
    )
    return (
        '<!doctype html><html lang="zh-CN"><meta charset="utf-8">'
        f"<title>PDF处理历史</title><style>{style}</style>"
        "<h1>PDF处理历史</h1>"
        f"<p>生成时间：{html.escape(str(report['生成时间']))}　记录数：{report['记录数']}　"
        f"是否脱敏：{'是' if report['是否脱敏'] else '否'}</p>"
        "<table><thead><tr><th>时间</th><th>事件</th><th>来源文件</th><th>状态</th>"
        "<th>引擎</th><th>已处理页数</th><th>低置信页</th><th>失败页</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></html>"
    )


def share_processing_history(
    root: str | Path,
    *,
    output_dir: str | Path | None = None,
    max_records: int = 500,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    report = collect_processing_history(
        root,
        max_records=max_records,
        include_sensitive=include_sensitive,
    )
    output_root = Path(output_dir).expanduser().resolve() if output_dir else Path(root).expanduser().resolve() / "处理历史分享"
    output_root.mkdir(parents=True, exist_ok=True)
    json_path = output_root / "处理历史.json"
    markdown_path = output_root / "处理历史.md"
    html_path = output_root / "处理历史.html"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    markdown_path.write_text(_history_markdown(report), encoding="utf-8")
    html_path.write_text(_history_html(report), encoding="utf-8")
    return {
        "状态": "已生成",
        "记录数": report["记录数"],
        "是否脱敏": report["是否脱敏"],
        "输出文件": {
            "JSON": str(json_path),
            "Markdown": str(markdown_path),
            "HTML": str(html_path),
        },
        "报告": report,
    }
