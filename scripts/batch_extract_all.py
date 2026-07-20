"""启动百科全书书库的可恢复、资源感知批量任务。

书本完成数和页数进度统一从 ``_BatchManager.status()`` 读取；完成判定必须
覆盖源 PDF 总页数，旧的抽样状态不会被误计为完成。批量管理器会按 CPU、内存
和每个 OCR worker 的实时占用动态决定并发数。
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pdf_rescue_mcp.server import _batch_manager, _task_manager
from pdf_rescue_mcp.ocr_engines import available_ocr_engine

ROOT = r"D:\BaiduNetdiskDownload\dabao"
OUTPUT = r"D:\农业百科全书-转文字"
MODE = "book-fast"


def _text(value: object, suffix: str = "") -> str:
    return f"{value}{suffix}" if value is not None else "未知"


def format_batch_monitor_report(status: dict[str, object]) -> str:
    """Render the stable operator report from ``get_batch_status`` data.

    The MCP response remains structured for every client.  This terminal
    renderer gives the same contract a readable, copyable form without taking
    another OCR/resource sample.
    """
    fixed = status.get("固定监测格式")
    report = fixed if isinstance(fixed, dict) else {}
    task_list = report.get("当前worker任务列表")
    if not isinstance(task_list, list):
        task_list = status.get("当前worker任务列表")
    if not isinstance(task_list, list):
        task_list = []

    progress = report.get("进度")
    if isinstance(progress, dict):
        progress_text = progress.get("文本") or progress.get("百分比") or "未知"
    else:
        progress_text = status.get("处理进度文本") or progress or "未知"

    lines = [
        "--- 批量OCR固定监测格式 ---",
        f"书籍: {report.get('书籍') or status.get('书籍名') or status.get('当前书籍') or '无'}",
        f"已用时间: {report.get('已用时间') or status.get('运行时间') or '未知'}",
        f"剩余时间: {report.get('剩余时间') or status.get('剩余时间') or '未知'}",
        f"处理速度: {report.get('处理速度') or status.get('处理速度文本') or '未知'}",
        f"剩余书本数量: {report.get('剩余书本数量', status.get('剩余书本数量', '未知'))}",
        f"进度: {progress_text}",
        f"OCR线程预算: {_text(report.get('OCR线程预算', status.get('OCR线程预算')))}",
        f"进程线程数: {_text(report.get('进程线程数', status.get('进程线程数')))}",
        f"CPU整机占比: {_text(report.get('CPU整机占比', status.get('CPU整机占比')), '%')}",
        f"CPU等效核心: {_text(report.get('CPU等效核心', status.get('CPU等效核心')), '核')}",
        f"RSS内存: {_text(report.get('RSS内存', status.get('RSS内存')), 'MB')}",
        "当前worker任务列表:",
    ]
    if not task_list:
        lines.append("  （暂无任务）")
    for task in task_list:
        if not isinstance(task, dict):
            continue
        marker = task.get("标记") or "?"
        book = task.get("书籍") or task.get("书名") or "未知书籍"
        lines.append(f"  {marker} {book}")
        if marker == "-":
            progress_item = task.get("进度")
            if isinstance(progress_item, dict):
                task_progress = progress_item.get("文本") or "未知"
            else:
                task_progress = task.get("进度文本") or "未知"
            lines.append(
                "    "
                f"已用: {task.get('已用时间') or '未知'} | "
                f"剩余: {task.get('剩余时间') or '未知'} | "
                f"速度: {task.get('近期实际处理速度') or task.get('处理速度') or '未知'}"
            )
            lines.append(
                "    "
                f"PID: {_text(task.get('实际Worker PID'))} | "
                f"进度: {task_progress} | "
                f"OCR线程预算: {_text(task.get('OCR线程预算'))} | "
                f"进程线程数: {_text(task.get('进程线程数'))} | "
                f"CPU整机占比: {_text(task.get('CPU整机占比'), '%')} | "
                f"CPU等效核心: {_text(task.get('CPU等效核心'), '核')} | "
                f"RSS内存: {_text(task.get('RSS内存MB'), 'MB')}"
            )
    return "\n".join(lines)


def main() -> None:
    # Do not let a controller launched without the optional OCR extra mark
    # resumable books as failed.  The controller must fail before it attaches
    # or starts any worker; the correct portable command is shown explicitly.
    if not available_ocr_engine():
        raise SystemExit(
            "OCR runtime unavailable. Start with: uv run --locked --extra ocr "
            "python scripts/batch_extract_all.py"
        )
    print("=== 中国农业百科全书批量提取 ===")
    print(f"根目录: {ROOT}")
    print(f"输出: {OUTPUT}")
    print(f"模式: {MODE}")
    print("并发策略: 根据CPU线程、可用内存和每个worker实时占用动态调整")
    print()

    # A controller restart must first reattach durable task supervision and the
    # persisted batch.  Starting a fresh batch here would reset the book list
    # and could compete with OCR workers that survived the old controller.
    _batch_manager.restore_pending()
    _task_manager.restore_pending(allow_takeover=_batch_manager.allows_task_recovery)
    if _batch_manager.status().get("运行中"):
        started = {"状态": "已从持久状态恢复"}
    else:
        started = _batch_manager.start_batch(
            root=ROOT,
            output_dir=OUTPUT,
            mode=MODE,
            max_books=None,
            max_pages_per_book=None,
            resume=True,
            max_workers=None,
        )
    print(f"启动结果: {started}")

    while True:
        status = _batch_manager.status()
        completed = status.get("书本完成数", status.get("已完成", 0))
        total = status.get("书本总数", status.get("总书数", 0))
        print(format_batch_monitor_report(status), flush=True)
        if not status.get("运行中"):
            print("=== 批量任务结束 ===")
            print(f"完成: {completed}/{total} | 失败: {status.get('书本失败数', status.get('失败', 0))}")
            break
        time.sleep(30)


if __name__ == "__main__":
    main()
