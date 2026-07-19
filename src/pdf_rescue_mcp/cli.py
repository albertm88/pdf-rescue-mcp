from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console

from .book_pipeline import (
    audit_job_quality,
    export_page_image_evidence,
    extract_book_text,
    get_page_evidence,
    read_job_status,
    resume_job,
)
from .library_pipeline import batch_extract_library, scan_pdf_library
from .pdf_inspector import inspect_pdf_text_layer
from .planner import plan_pdf_job
from .runtime import doctor_runtime
from .stdio_encoding import configure_utf8_stdio
from .zh import zh_data


def _translate_click_help(text: str) -> str:
    """Keep CLI help stable and readable when Click/Typer upgrades."""
    replacements = {
        "Show this message and exit.": "显示此帮助并退出。",
        "[required]": "[必填]",
        "default: book-balanced": "默认值：书籍均衡",
        "default: resume": "默认值：继续",
        "default: no-resume": "默认值：不继续",
        "default:": "默认值：",
    }
    for original, translated in replacements.items():
        text = text.replace(original, translated)
    return text


def _write_chinese_section(formatter, heading: str, rows: list[tuple[str, str]]) -> None:
    if not rows:
        return
    formatter.write_paragraph()
    formatter.write(f"{'':>{formatter.current_indent}}{heading}：\n")
    formatter.indent()
    try:
        formatter.write_dl(rows)
    finally:
        formatter.dedent()


class _ChineseHelpMixin:
    """Small Click-format boundary; command behavior remains ordinary Typer."""

    def get_help_option_names(self, ctx):
        names = super().get_help_option_names(ctx)
        if "--帮助" not in names:
            names.append("--帮助")
        return names

    def format_usage(self, ctx, formatter) -> None:
        pieces = " ".join(self.collect_usage_pieces(ctx)).replace("OPTIONS", "选项")
        formatter.write_usage(ctx.command_path, pieces, prefix="用法：")

    def format_options(self, ctx, formatter) -> None:
        arguments: list[tuple[str, str]] = []
        options: list[tuple[str, str]] = []
        for parameter in self.get_params(ctx):
            record = parameter.get_help_record(ctx)
            if record is None:
                continue
            translated = (record[0], _translate_click_help(record[1]))
            if parameter.param_type_name == "argument":
                arguments.append(translated)
            elif parameter.param_type_name == "option":
                options.append(translated)
        _write_chinese_section(formatter, "参数", arguments)
        _write_chinese_section(formatter, "选项", options)


class ChineseTyperCommand(_ChineseHelpMixin, typer.core.TyperCommand):
    pass


class ChineseTyperGroup(_ChineseHelpMixin, typer.core.TyperGroup):
    def format_options(self, ctx, formatter) -> None:
        super().format_options(ctx, formatter)
        self.format_commands(ctx, formatter)

    def format_commands(self, ctx, formatter) -> None:
        rows: list[tuple[str, str]] = []
        for subcommand in self.list_commands(ctx):
            command = self.get_command(ctx, subcommand)
            if command is not None and not command.hidden:
                rows.append((subcommand, command.get_short_help_str(formatter.width - 6 - len(subcommand))))
        _write_chinese_section(formatter, "命令", rows)


app = typer.Typer(
    add_completion=False,
    rich_markup_mode=None,
    help="PDF 书籍救援工具",
    cls=ChineseTyperGroup,
)

configure_utf8_stdio()
console = Console()


@app.command("体检", cls=ChineseTyperCommand)
def doctor(
    as_json: bool = typer.Option(False, "--json", help="输出完整 JSON 数据"),
):
    """检查运行环境和 OCR 依赖"""
    result = doctor_runtime()
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    profile = result
    console.print(f"CPU 核心: {profile.cpu_count} | 内存: {profile.memory_gb} GB")
    for note in profile.notes:
        console.print(f"- {zh_data(note)}")


@app.command("检查", cls=ChineseTyperCommand)
def inspect(
    path: Path,
    max_pages: Optional[int] = typer.Option(None, "--max-pages", help="检查前 N 页"),
    password: Optional[str] = typer.Option(None, "--password", help="PDF 密码"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """检查 PDF 类型和文本层"""
    result = inspect_pdf_text_layer(str(path), max_pages=max_pages, password=password)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"PDF 类型: {zh_data(result.pdf_type)}")
    console.print(f"建议操作: {zh_data(result.recommended_action)}")
    console.print(f"总页数: {result.page_count}")


@app.command("规划", cls=ChineseTyperCommand)
def plan(
    path: Path,
    target_quality: str = typer.Option("balanced", "--quality", help="目标质量"),
    max_seconds: Optional[int] = typer.Option(None, "--max-seconds", help="最大耗时"),
    password: Optional[str] = typer.Option(None, "--password", help="PDF 密码"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """规划 PDF 处理路线"""
    result = plan_pdf_job(str(path), target_quality=target_quality, max_seconds=max_seconds, password=password)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"处理路线: {zh_data(result.get('route', ''))}")
    console.print(f"引擎: {zh_data(result.get('engine', ''))}")


@app.command("提取", cls=ChineseTyperCommand)
def extract(
    path: Path,
    output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="输出目录"),
    mode: str = typer.Option("book-balanced", "--mode", help="识别模式"),
    max_pages: Optional[int] = typer.Option(None, "--max-pages", help="处理前 N 页"),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="断点续传"),
    password: Optional[str] = typer.Option(None, "--password", help="PDF 密码"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """提取书籍文本"""
    result = extract_book_text(
        path=str(path),
        output_dir=str(output_dir) if output_dir else None,
        mode=mode,
        max_pages=max_pages,
        resume=resume,
        password=password or os.environ.get("PDF_RESCUE_PASSWORD"),
    )
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"状态: {zh_data(result.status)}")
    console.print(f"任务目录: {result.job_dir}")
    console.print(f"引擎: {zh_data(result.engine)}")
    for step in result.next_steps:
        console.print(f"- {zh_data(step)}")


@app.command("状态", cls=ChineseTyperCommand)
def status(
    job_dir: Path,
    stalled_after_seconds: int = typer.Option(600, "--stalled-after", help="卡死判定秒数"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """查看任务状态"""
    result = read_job_status(str(job_dir), stalled_after_seconds=stalled_after_seconds)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    state = result.get("状态", {})
    console.print(f"状态: {state.get('状态', '未知')}")
    console.print(f"进度: {state.get('已处理页数', 0)} / {state.get('目标页数', 0)}")


@app.command("恢复", cls=ChineseTyperCommand)
def resume_cmd(
    job_dir: Path,
    mode: Optional[str] = typer.Option(None, "--mode", help="覆盖识别模式"),
    stalled_after_seconds: int = typer.Option(600, "--stalled-after", help="卡死判定秒数"),
    force: bool = typer.Option(False, "--force", help="强制恢复"),
    password: Optional[str] = typer.Option(None, "--password", help="PDF 密码"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """恢复中断的任务"""
    result = resume_job(
        job_dir=str(job_dir),
        mode=mode,
        stalled_after_seconds=stalled_after_seconds,
        force=force,
        password=password,
    )
    if as_json:
        console.print(json.dumps({"结果": str(result)}, ensure_ascii=False, indent=2))
        return
    console.print(f"结果: {result}")


@app.command("质检", cls=ChineseTyperCommand)
def audit_quality(
    job_dir: Path,
    max_issues: int = typer.Option(80, "--max-issues", help="最大问题数"),
    use_latest_rules: bool = typer.Option(True, "--latest-rules/--current-only", help="使用最新规则"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """巡检提取质量"""
    result = audit_job_quality(str(job_dir), max_issues=max_issues, use_latest_rules=use_latest_rules)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"已巡检: {result.get('已巡检页数', 0)} 页")


@app.command("页面证据", cls=ChineseTyperCommand)
def page_evidence_cmd(
    job_dir: Path,
    page_number: int = typer.Argument(..., help="页码"),
    include_blocks: bool = typer.Option(False, "--include-blocks", help="包含识别块"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """查看页面识别证据"""
    result = get_page_evidence(str(job_dir), page_number=page_number, include_blocks=include_blocks)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"页码: {result.get('页码')}")
    console.print(f"置信度: {result.get('置信度')}")
    text = str(result.get("文本") or "")
    console.print(text[:1000])


@app.command("页面图像证据", cls=ChineseTyperCommand)
def page_image_evidence_cmd(
    job_dir: Path,
    page_number: int = typer.Argument(..., help="页码"),
    dpi: int = typer.Option(160, "--dpi", help="渲染 DPI"),
    password: Optional[str] = typer.Option(None, "--password", help="PDF 密码"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """导出页面图像证据"""
    result = export_page_image_evidence(str(job_dir), page_number=page_number, dpi=dpi, password=password)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"图像路径: {result.get('图像路径')}")


@app.command("书库扫描", cls=ChineseTyperCommand)
def scan_library_cmd(
    root: Path,
    output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="输出目录"),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="最大文件数"),
    inspect_pages: int = typer.Option(3, "--inspect-pages", help="检查页数"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """扫描书库"""
    result = scan_pdf_library(str(root), output_dir=str(output_dir) if output_dir else None, max_files=max_files, inspect_pages=inspect_pages)
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"发现 PDF: {result.get('summary', {}).get('发现PDF数量', 0)}")


@app.command("书库提取", cls=ChineseTyperCommand)
def extract_library_cmd(
    root: Path,
    output_dir: Optional[Path] = typer.Option(None, "--output-dir", help="输出目录"),
    mode: str = typer.Option("book-balanced", "--mode", help="识别模式"),
    max_books: Optional[int] = typer.Option(1, "--max-books", help="最大处理书数"),
    max_pages_per_book: Optional[int] = typer.Option(None, "--max-pages-per-book", help="每本书最大页数"),
    resume: bool = typer.Option(True, "--resume/--no-resume", help="断点续传"),
    as_json: bool = typer.Option(False, "--json", help="输出 JSON"),
):
    """批量提取书库"""
    result = batch_extract_library(
        root=str(root),
        output_dir=str(output_dir) if output_dir else None,
        mode=mode,
        max_books=max_books,
        max_pages_per_book=max_pages_per_book,
        resume=resume,
    )
    if as_json:
        console.print(json.dumps(zh_data(result), ensure_ascii=False, indent=2))
        return
    console.print(f"本次处理书数: {result.get('summary', {}).get('本次处理书数', 0)}")


if __name__ == "__main__":
    app()
