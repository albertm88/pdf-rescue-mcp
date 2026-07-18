"""
批量提取启动脚本：使用 _TaskManager 子进程架构连续处理所有书籍。
保留书库的相对目录结构（01-主系列-已齐等）。
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pdf_rescue_mcp.server import _task_manager
from pdf_rescue_mcp.library_pipeline import scan_pdf_library, _job_dir_for_pdf, _read_status
from pdf_rescue_mcp.book_pipeline import _safe_name

ROOT = r"D:\BaiduNetdiskDownload\dabao"
OUTPUT = r"D:\农业百科全书-转文字"
MODE = "book-fast"  # 快速模式，约 8-15 秒/页


def main():
    print("=== 中国农业百科全书批量提取 ===")
    print(f"根目录: {ROOT}")
    print(f"输出: {OUTPUT}")
    print(f"模式: {MODE}")
    print()
    
    root_path = Path(ROOT)
    output_path = Path(OUTPUT)
    
    # 扫描书库
    print("正在扫描书库...")
    scan = scan_pdf_library(ROOT, output_dir=OUTPUT, inspect_pages=3)
    books = scan.get("书籍", [])
    print(f"发现 {len(books)} 本书")
    
    # 过滤出需要 OCR 的 PDF（纯扫描 + 混合类型）
    need_ocr = [
        b for b in books 
        if b.get("PDF类型") in ("纯扫描PDF", "混合PDF") 
        or "OCR" in str(b.get("建议动作", ""))
    ]
    print(f"需要 OCR: {len(need_ocr)} 本")
    print()
    
    # 逐本启动提取
    for i, book in enumerate(need_ocr):
        pdf_path = Path(book["PDF路径"])
        book_name = book["文件名"].replace(".pdf", "")
        total_pages = book.get("总页数", 0)
        
        # 使用 library_pipeline 的逻辑计算正确的任务目录（保留相对路径）
        job_dir = _job_dir_for_pdf(pdf_path, root_path, output_path)
        
        # 检查是否已完成
        status = _read_status(job_dir)
        if status and status.get("状态") == "完成":
            processed = status.get("已处理页数", 0)
            target = status.get("目标页数", 0)
            if processed >= target and target > 0:
                print(f"[{i+1}/{len(need_ocr)}] 跳过已完成: {book_name} ({processed}/{target} 页)")
                continue
        
        print(f"[{i+1}/{len(need_ocr)}] 启动: {book_name} ({total_pages} 页)")
        print(f"  任务目录: {job_dir}")
        
        # start_extraction 会在 output_dir 下创建 *-rescue-result
        # 所以传入 job_dir.parent，让它创建 job_dir.name（已经是 *-rescue-result）
        actual_output_dir = job_dir.parent
        returned_job_dir, already_running = _task_manager.start_extraction(
            path=str(pdf_path),
            output_dir=str(actual_output_dir),
            mode=MODE,
            max_pages=None,
            resume=True,
            password=None,
        )
        
        if already_running:
            print(f"  已在运行")
        else:
            pid = _task_manager.get_task_info(returned_job_dir).get("进程ID")
            print(f"  子进程 PID: {pid}")
        
        # 等待这本书完成（每 30 秒检查一次）
        print(f"  处理中...", end="", flush=True)
        last_progress = -1
        while True:
            time.sleep(30)
            info = _task_manager.get_task_info(str(job_dir))
            if not info or not info.get("存活"):
                # 进程结束，检查状态
                status = _read_status(job_dir)
                if status:
                    state = status.get("状态", "未知")
                    processed = status.get("已处理页数", 0)
                    total = status.get("目标页数", 0)
                    print(f"\n  {state}: {processed}/{total} 页")
                else:
                    print(f"\n  进程已结束（无状态文件）")
                break
            
            # 显示进度
            status = _read_status(job_dir)
            if status:
                processed = status.get("已处理页数", 0)
                total = status.get("目标页数", 0)
                if processed != last_progress:
                    pct = round(processed / total * 100, 1) if total else 0
                    print(f" {pct}%", end="", flush=True)
                    last_progress = processed
        
        print()
    
    print("\n=== 所有书籍提取完成 ===")


if __name__ == "__main__":
    main()
