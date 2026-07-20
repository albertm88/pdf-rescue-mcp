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

ROOT = r"D:\BaiduNetdiskDownload\dabao"
OUTPUT = r"D:\农业百科全书-转文字"
MODE = "book-fast"


def main() -> None:
    print("=== 中国农业百科全书批量提取 ===")
    print(f"根目录: {ROOT}")
    print(f"输出: {OUTPUT}")
    print(f"模式: {MODE}")
    print("并发策略: 根据CPU线程、可用内存和每个worker实时占用动态调整")
    print()

    # A controller restart must first reattach durable task supervision and the
    # persisted batch.  Starting a fresh batch here would reset the book list
    # and could compete with OCR workers that survived the old controller.
    _task_manager.restore_pending()
    _batch_manager.restore_pending()
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
        current = status.get("书籍名") or status.get("当前书籍") or "无"
        page_text = status.get("处理进度文本") or "未知"
        worker_plan = status.get("worker调度") or {}
        worker_summary = status.get("worker资源汇总") or {}
        target_workers = worker_plan.get("target_workers", 1)
        active_workers = worker_plan.get("active_workers", 0)
        registered_workers = status.get("worker数", active_workers)
        rss_mb = worker_summary.get("总运行内存占用MB")
        process_threads = worker_summary.get("总进程线程数")
        active_cpu_threads = worker_summary.get("总活跃CPU线程数")
        cpu_percent = worker_summary.get("总CPU占整机比例")
        resource_text = (
            f"RSS: {rss_mb if rss_mb is not None else '未知'}MB | "
            f"进程线程: {process_threads if process_threads is not None else '未知'} | "
            f"活跃CPU线程: {active_cpu_threads if active_cpu_threads is not None else '未知'} | "
            f"CPU: {cpu_percent if cpu_percent is not None else '未知'}%"
        )
        print(
            f"书本进度: {completed}/{total} | 当前: {current} | "
            f"页进度: {page_text} | worker: {registered_workers}/{target_workers} | {resource_text} | "
            f"预计剩余: {status.get('剩余时间') or '未知'}",
            flush=True,
        )
        if not status.get("运行中"):
            print("=== 批量任务结束 ===")
            print(f"完成: {completed}/{total} | 失败: {status.get('书本失败数', status.get('失败', 0))}")
            break
        time.sleep(30)


if __name__ == "__main__":
    main()
