from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import os
import re
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Callable, Literal

from mcp.server.fastmcp import Context, FastMCP
from pydantic import Field

from .book_pipeline import _safe_name
from .book_pipeline import audit_job_quality as run_audit_job_quality
from .book_pipeline import add_term_glossary_replacement as add_glossary_replacement
from .book_pipeline import extract_book_text as run_extract_book_text
from .book_pipeline import export_page_image_evidence as run_export_page_image_evidence
from .book_pipeline import get_page_evidence as read_page_evidence
from .book_pipeline import get_term_glossary as read_term_glossary
from .book_pipeline import read_job_status as run_read_job_status
from .book_pipeline import resume_job as run_resume_job
from .library_pipeline import batch_extract_library as run_batch_extract_library
from .library_pipeline import scan_pdf_library as run_scan_pdf_library
from .pdf_inspector import inspect_pdf_text_layer as run_inspect_pdf_text_layer
from .planner import plan_pdf_job as run_plan_pdf_job
from .paths import configure_file_logging, project_relative_path, timestamped_log_path
from .runtime import doctor_runtime as run_doctor_runtime
from .stdio_encoding import configure_utf8_stdio
from .history import collect_processing_history, share_processing_history
from .zh import zh_data


MCP_INSTRUCTIONS = """
这是一个本地 PDF 救援服务。默认优先调用 `rescue_pdf`，不要先要求用户在“检查、
规划、OCR、前台、后台、质检”等内部操作间做选择。

对一个明确的 PDF 请求，直接把用户的原话放进 `request`，传入 `path` 或已有的
`job_dir`，让该工具选择工作流：它会先诊断和规划，再自动选择直接提取、OCR 提取或
可恢复的后台任务。只有缺少不可推断的事实（例如 PDF 路径、密码、所需证据页码）时
才向用户追问。长时间 OCR 任务启动后，仍使用同一个 `rescue_pdf` 查询状态、巡检、
恢复或查看证据。

其余工具是高级接口，仅在需要批量书库、术语词表或精确控制某一步时调用。不要读取
README 后把内部工具清单转交给用户选择。
""".strip()


mcp = FastMCP("中文PDF书籍救援MCP", instructions=MCP_INSTRUCTIONS)


def _make_progress_callback(ctx: Context | None) -> Callable[[int, int, float, str | None], None] | None:
    """构造同步进度回调，内部线程安全地调度异步 ctx.report_progress。"""
    if ctx is None:
        return None
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        return None

    def _callback(processed: int, total: int, pct: float, message: str | None) -> None:
        try:
            asyncio.run_coroutine_threadsafe(
                ctx.report_progress(processed, total, message),
                loop,
            )
        except Exception:
            pass

    return _callback


class _TaskManager:
    """三层机制任务管理器：子进程提取 + 监控层检测 + 优化层调整。

    业务层：子进程（subprocess.Popen）跑 pdf_rescue_mcp.cli 提取，每页写 状态.json。
    监控层：watcher 线程每 10 秒检测子进程存活（poll）+ 状态文件更新频率；
            单页超时（默认 180 秒）判定卡死。
    优化层：卡死时 terminate() 强杀子进程，降级为 book-fast 重启一次；
            连续失败标记失败。
    """

    WATCH_INTERVAL = 10
    PAGE_TIMEOUT = 180
    MAX_AUTO_RESTART = 1
    RESTART_DOWNGRADE_MODE = "book-fast"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._watcher: threading.Thread | None = None
        self._stopping = False

    # ── 业务层：启动子进程提取 ──

    def _launch_subprocess(
        self, source: Path, job_dir: Path, mode: str, max_pages: int | None, resume: bool
    ) -> subprocess.Popen:
        child_env = os.environ.copy()
        child_env.update({"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8", "PYTHONLEGACYWINDOWSSTDIO": "0"})
        source_root = str(Path(__file__).resolve().parents[1])
        existing = child_env.get("PYTHONPATH")
        child_env["PYTHONPATH"] = f"{source_root}{os.pathsep}{existing}" if existing else source_root
        # 确保子进程使用项目 .venv（和 MCP 服务器相同的环境）
        venv_scripts = Path(source_root) / ".venv" / "Scripts"
        if venv_scripts.exists():
            child_env["PATH"] = f"{venv_scripts}{os.pathsep}{child_env.get('PATH', '')}"
            child_env["VIRTUAL_ENV"] = str(Path(source_root) / ".venv")
        log_path = timestamped_log_path(f"extract-{_safe_name(source)}")
        # CLI 会在 output-dir 下创建 *-rescue-result 子目录，所以传入 job_dir 的父目录
        cli_output_dir = job_dir.parent
        # 直接使用 sys.executable（MCP 服务器的 Python），确保环境一致
        command = [
            sys.executable, "-u", "-m", "pdf_rescue_mcp.cli", "提取",
            str(source), "--output-dir", str(cli_output_dir), "--mode", mode, "--json",
            "--resume" if resume else "--no-resume",
        ]
        if max_pages is not None:
            command.extend(["--max-pages", str(max_pages)])
        with log_path.open("a", encoding="utf-8") as log_handle:
            popen_kwargs: dict[str, Any] = {
                "stdin": subprocess.DEVNULL, "stdout": log_handle, "stderr": subprocess.STDOUT,
                "start_new_session": True, "env": child_env, "cwd": source_root,
            }
            if sys.platform == "win32":
                popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            return subprocess.Popen(command, **popen_kwargs)

    def start_extraction(self, path: str, output_dir: str | None, mode: str,
                         max_pages: int | None, resume: bool, password: str | None,
                         ) -> tuple[str, bool]:
        source = Path(path).expanduser().resolve()
        safe_name = f"{_safe_name(source)}-rescue-result"
        base_dir = (
            Path(output_dir).expanduser().resolve()
            if output_dir
            else (Path(os.environ.get("PDF_RESCUE_OUTPUT_ROOT", "")).expanduser().resolve()
                  if os.environ.get("PDF_RESCUE_OUTPUT_ROOT")
                  else source.parent / "pdf_rescue_output")
        )
        job_dir = base_dir / safe_name
        job_dir.mkdir(parents=True, exist_ok=True)
        job_key = str(job_dir.resolve())
        with self._lock:
            info = self._tasks.get(job_key)
            if info and info.get("process") and info["process"].poll() is None:
                return str(job_dir), True
            proc = self._launch_subprocess(source, job_dir, mode, max_pages, resume)
            self._tasks[job_key] = {
                "process": proc, "started_at": time.time(),
                "restart_count": 0, "mode": mode,
            }
        self._ensure_watcher()
        return str(job_dir), False

    # ── 监控层：watcher 线程 ──

    def _ensure_watcher(self) -> None:
        if self._watcher is None or not self._watcher.is_alive():
            self._watcher = threading.Thread(target=self._watcher_loop, daemon=True, name="task-watcher")
            self._watcher.start()

    def _watcher_loop(self) -> None:
        while not self._stopping:
            time.sleep(self.WATCH_INTERVAL)
            with self._lock:
                for job_key, info in list(self._tasks.items()):
                    proc = info.get("process")
                    if proc is None:
                        continue
                    rc = proc.poll()
                    if rc is not None:
                        if rc != 0:
                            self._handle_exit_code(job_key, info, rc)
                        else:
                            info["process"] = None  # 正常结束
                        continue
                    self._check_stalled(job_key, info)

    def _handle_exit_code(self, job_key: str, info: dict, rc: int) -> None:
        info["process"] = None
        status_path = Path(job_key) / "状态.json"
        try:
            if status_path.exists():
                payload = json.loads(status_path.read_text(encoding="utf-8"))
                if payload.get("状态") == "进行中":
                    payload["状态"] = "失败"
                    payload["失败原因"] = f"子进程退出码 {rc}"
                    payload["失败时间"] = datetime.now().isoformat(timespec="seconds")
                    status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _check_stalled(self, job_key: str, info: dict) -> None:
        status_path = Path(job_key) / "状态.json"
        if not status_path.exists():
            return
        try:
            age = (datetime.now() - datetime.fromtimestamp(status_path.stat().st_mtime)).total_seconds()
        except Exception:
            return
        if age < self.PAGE_TIMEOUT:
            return
        restart_count = int(info.get("restart_count", 0))
        if restart_count >= self.MAX_AUTO_RESTART:
            self._mark_stalled(job_key, info, age)
            return
        self._auto_restart(job_key, info, age)

    # ── 优化层：强杀 + 降级重启 ──

    def _mark_stalled(self, job_key: str, info: dict, age: float) -> None:
        proc = info.get("process")
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
        info["process"] = None
        status_path = Path(job_key) / "状态.json"
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            payload["状态"] = "卡死"
            payload["失败原因"] = f"状态文件 {int(age)} 秒未更新，已 kill 子进程，用完重启次数"
            payload["失败时间"] = datetime.now().isoformat(timespec="seconds")
            status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

    def _auto_restart(self, job_key: str, info: dict, age: float) -> None:
        old_proc = info.get("process")
        if old_proc and old_proc.poll() is None:
            try:
                old_proc.terminate()
            except Exception:
                pass
        info["restart_count"] = int(info.get("restart_count", 0)) + 1
        status_path = Path(job_key) / "状态.json"
        source_pdf = None
        try:
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            source_pdf = payload.get("来源PDF")
            payload["上次卡死"] = f"状态文件 {int(age)} 秒未更新"
            payload["自动重启次数"] = info["restart_count"]
            payload["重启模式"] = self.RESTART_DOWNGRADE_MODE
            status_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass
        if not source_pdf:
            return
        source = Path(source_pdf)
        if not source.exists():
            return
        info["process"] = self._launch_subprocess(
            source, Path(job_key), self.RESTART_DOWNGRADE_MODE, None, True
        )

    # ── 查询接口 ──

    def is_running(self, job_dir: str) -> bool:
        with self._lock:
            info = self._tasks.get(str(Path(job_dir).resolve()))
            proc = info.get("process") if info else None
            return proc is not None and proc.poll() is None

    def get_task_info(self, job_dir: str) -> dict[str, Any] | None:
        with self._lock:
            info = self._tasks.get(str(Path(job_dir).resolve()))
            if not info:
                return None
            proc = info.get("process")
            return {
                "存活": proc is not None and proc.poll() is None,
                "进程ID": proc.pid if proc else None,
                "重启次数": int(info.get("restart_count", 0)),
                "启动时间": info.get("started_at"),
                "模式": info.get("mode"),
            }


_task_manager = _TaskManager()


class _BatchManager:
    """批量任务管理器：在后台线程中逐本启动提取，MCP 服务器保持响应。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._books: list[dict[str, Any]] = []
        self._current_index = -1
        self._current_job_dir: str | None = None
        self._completed: list[str] = []
        self._failed: list[dict[str, str]] = []
        self._started_at: float | None = None
        self._mode = "book-fast"
        self._output_dir: str | None = None
        self._root: str | None = None

    def start_batch(self, root: str, output_dir: str | None, mode: str,
                    max_books: int | None, resume: bool) -> dict[str, Any]:
        """启动批量提取，立即返回，后台逐本处理。"""
        with self._lock:
            if self._running:
                return {"状态": "已在运行", "当前进度": self.status()}

            self._root = root
            self._output_dir = output_dir
            self._mode = mode
            self._started_at = time.time()
            self._completed = []
            self._failed = []
            self._current_index = -1
            self._current_job_dir = None

            # 扫描书库（快速模式：不逐本检查文本层，直接全部需要OCR）
            from .library_pipeline import scan_pdf_library, _job_dir_for_pdf, _read_status
            scan = scan_pdf_library(root, output_dir=output_dir, inspect_pages=0)
            books = scan.get("书籍", [])
            need_ocr = [
                b for b in books
                if b.get("PDF类型") in ("纯扫描PDF", "混合PDF", "未知")
                or "OCR" in str(b.get("建议动作", ""))
                or b.get("PDF类型") is None
            ]
            if max_books is not None:
                need_ocr = need_ocr[:max_books]

            self._books = need_ocr
            self._running = True
            self._thread = threading.Thread(target=self._batch_loop, daemon=True, name="batch-extractor")
            self._thread.start()

            return {
                "状态": "已启动",
                "总书数": len(self._books),
                "模式": mode,
                "输出目录": output_dir,
            }

    def _batch_loop(self) -> None:
        """后台线程：逐本启动提取并等待完成。"""
        from .library_pipeline import _job_dir_for_pdf, _read_status
        root_path = Path(self._root) if self._root else None
        output_path = Path(self._output_dir) if self._output_dir else None

        for i, book in enumerate(self._books):
            with self._lock:
                self._current_index = i
            pdf_path = Path(book["PDF路径"])
            book_name = book["文件名"].replace(".pdf", "")
            total_pages = book.get("总页数", 0)

            try:
                job_dir = _job_dir_for_pdf(pdf_path, root_path, output_path) if root_path and output_path else None

                # 检查是否已完成
                status = _read_status(job_dir) if job_dir else None
                if status and status.get("状态") == "完成":
                    processed = status.get("已处理页数", 0)
                    target = status.get("目标页数", 0)
                    if processed >= target and target > 0:
                        with self._lock:
                            self._completed.append(book_name)
                        continue

                # 启动子进程
                actual_output_dir = str(job_dir.parent) if job_dir else None
                returned_job_dir, already_running = _task_manager.start_extraction(
                    path=str(pdf_path),
                    output_dir=actual_output_dir,
                    mode=self._mode,
                    max_pages=None,
                    resume=True,
                    password=None,
                )

                with self._lock:
                    self._current_job_dir = returned_job_dir

                # 等待这本书完成
                while True:
                    time.sleep(30)
                    if not self._running:
                        return
                    info = _task_manager.get_task_info(returned_job_dir)
                    if not info or not info.get("存活"):
                        s = _read_status(Path(returned_job_dir))
                        if s and s.get("状态") == "完成":
                            with self._lock:
                                self._completed.append(book_name)
                        else:
                            with self._lock:
                                self._failed.append({"书名": book_name, "原因": str(s.get("状态", "未知")) if s else "无状态文件"})
                        break
            except Exception as e:
                with self._lock:
                    self._failed.append({"书名": book_name, "原因": str(e)})

        with self._lock:
            self._running = False
            self._current_job_dir = None

    def status(self) -> dict[str, Any]:
        """返回批量任务状态。"""
        with self._lock:
            current_book = None
            current_progress = None
            current_job_dir = None
            if 0 <= self._current_index < len(self._books):
                current_book = self._books[self._current_index]["文件名"].replace(".pdf", "")
                current_job_dir = self._current_job_dir
                if self._current_job_dir:
                    status_path = Path(self._current_job_dir) / "状态.json"
                    if status_path.exists():
                        try:
                            s = json.loads(status_path.read_text(encoding="utf-8"))
                            processed = s.get("已处理页数", 0)
                            total = s.get("目标页数", 0) or s.get("PDF总页数", 0)
                            avg = s.get("平均每页秒")
                            current_progress = {
                                "已处理": processed,
                                "总数": total,
                                "百分比": f"{processed / total * 100:.1f}%" if total > 0 else "0%",
                                "状态": s.get("状态", "未知"),
                                "平均秒每页": round(avg, 1) if avg else None,
                                "本书预计剩余秒": int(avg * (total - processed)) if avg and total > processed else None,
                            }
                        except Exception:
                            pass

            elapsed = None
            elapsed_str = None
            eta_str = None
            if self._started_at:
                elapsed = int(time.time() - self._started_at)
                mins, secs = divmod(elapsed, 60)
                hours, mins = divmod(mins, 60)
                elapsed_str = f"{hours}小时{mins}分{secs}秒" if hours > 0 else f"{mins}分{secs}秒"

            done = len(self._completed) + len(self._failed)
            total_books = len(self._books)
            if self._running and done > 0 and elapsed and elapsed > 10:
                # 估算剩余时间
                avg_per_book = elapsed / done
                remaining_books = total_books - done
                eta_seconds = int(avg_per_book * remaining_books)
                eta_mins, eta_secs = divmod(eta_seconds, 60)
                eta_hours, eta_mins = divmod(eta_mins, 60)
                eta_str = f"约{eta_hours}小时{eta_mins}分" if eta_hours > 0 else f"约{eta_mins}分{eta_secs}秒"

            return {
                "运行中": self._running,
                "总书数": total_books,
                "已完成": len(self._completed),
                "失败": len(self._failed),
                "待处理": total_books - done,
                "整体进度": f"{done}/{total_books} ({done / total_books * 100:.1f}%)" if total_books > 0 else "0/0",
                "当前书籍": current_book,
                "当前书籍任务目录": current_job_dir,
                "当前书籍进度": current_progress,
                "已运行时间": elapsed_str,
                "预计剩余时间": eta_str,
                "失败列表": self._failed[-5:] if self._failed else [],
                "查询提示": "用 get_job_status 传入当前书籍任务目录查看详细进度（每1-5秒更新）",
            }

    def stop(self) -> dict[str, Any]:
        """停止批量任务（当前书籍会继续完成）。"""
        with self._lock:
            self._running = False
        return {"状态": "已发送停止信号，当前书籍将继续完成"}


_batch_manager = _BatchManager()


def _infer_workflow(
    workflow: str,
    request: str | None,
    path: str | None,
    job_dir: str | None,
    page_number: int | None,
) -> str:
    """Map a user's intent to a lifecycle step without exposing tool routing to the user."""
    if workflow != "auto":
        return workflow

    text = (request or "").lower().replace(" ", "")
    if page_number is not None or re.search(r"第?\s*\d+\s*页", text) or any(
        word in text for word in ("页面证据", "页截图", "页图像", "页图片")
    ):
        return "evidence"
    if any(word in text for word in ("恢复", "继续处理", "断点", "中断")):
        return "resume"
    if any(word in text for word in ("质检", "质量", "巡检", "低置信")):
        return "audit"
    if job_dir or any(word in text for word in ("进度", "状态", "处理到哪", "完成了吗")):
        return "status"
    if path and any(
        word in text
        for word in ("提取", "转成", "转换", "救援", "开始处理", "开始ocr", "开始识别", "识别全文", "导出文本")
    ):
        return "extract"
    if any(
        word in text
        for word in ("诊断", "检查", "分析", "文本层", "能不能识别", "是否扫描", "是否需要ocr", "需要ocr吗")
    ):
        return "diagnose"
    if path:
        return "extract"
    raise ValueError("请提供 PDF 路径，或提供已有任务目录以查询、巡检或恢复任务。")


def _page_number_from_request(request: str | None) -> int | None:
    if not request:
        return None
    match = re.search(r"第?\s*(\d+)\s*页", request)
    return int(match.group(1)) if match else None


def _workflow_response(
    status: str,
    executed: list[str],
    result: dict[str, Any],
    next_step: str,
) -> dict[str, Any]:
    """Return one compact, model-readable response for the primary workflow tool."""
    return {
        "状态": status,
        "已执行": executed,
        "结果": zh_data(result),
        "下一步": next_step,
    }


@mcp.tool(
    name="rescue_pdf",
    title="PDF一键救援",
    description=(
        "首选入口：直接处理用户关于单个PDF的请求，不要让用户选择内部工具。传入PDF路径和用户原话，"
        "会自动诊断、规划，并按文档类型和预计耗时完成前台提取或启动可恢复后台任务；也可用同一入口"
        "查询进度、质检、恢复任务和查看页面证据。前台提取时每页实时推送进度（页数、百分比、剩余时间）。"
    ),
)
async def rescue_pdf(
    path: Annotated[
        str | None,
        Field(description="待处理的单个 PDF 路径。诊断或提取时必填；查询已有任务时可不填。"),
    ] = None,
    request: Annotated[
        str | None,
        Field(description="用户的原始诉求，尽量原样传入。服务用它自动选择流程；不要先问用户选择操作。"),
    ] = None,
    job_dir: Annotated[
        str | None,
        Field(description="已有救援任务目录。查询进度、质检、恢复或查看页面证据时使用。"),
    ] = None,
    workflow: Annotated[
        Literal["auto", "diagnose", "extract", "status", "audit", "resume", "evidence"],
        Field(description="默认 auto，根据 request 自动判断。仅在调用方已明确知道生命周期步骤时指定。"),
    ] = "auto",
    execution: Annotated[
        Literal["auto", "foreground", "background"],
        Field(description="提取执行方式。默认 auto：短任务前台完成，长 OCR 任务自动后台运行。"),
    ] = "auto",
    mode: Annotated[
        Literal["book-fast", "book-balanced", "book-quality", "book-forensic"],
        Field(description="识别质量。默认 book-balanced；除非用户要求速度、最高质量或取证级，不必询问。"),
    ] = "book-balanced",
    output_dir: Annotated[
        str | None,
        Field(description="结果目录；未提供时自动创建在 PDF 同级的 pdf_rescue_output 下。"),
    ] = None,
    max_pages: Annotated[
        int | None,
        Field(description="仅处理前 N 页；用户要求试跑、样本或明确页数时填写。"),
    ] = None,
    page_number: Annotated[
        int | None,
        Field(description="查看页面证据时的 1 起始页码；也会尝试从 request 中提取“第 N 页”。"),
    ] = None,
    evidence_format: Annotated[
        Literal["auto", "text", "image"],
        Field(description="页面证据形式。默认 auto；用户提到图片、截图或渲染页时自动导出图片。"),
    ] = "auto",
    password: Annotated[
        str | None,
        Field(description="仅用于本次密码保护 PDF 调用；不会写入任务记录或后台命令。"),
    ] = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """Run the common PDF lifecycle through one task-oriented MCP tool."""
    selected = _infer_workflow(workflow, request, path, job_dir, page_number)

    if selected == "diagnose":
        if not path:
            raise ValueError("诊断需要 PDF 路径。")
        plan = run_plan_pdf_job(path, target_quality=mode, password=password)
        return _workflow_response(
            "已完成诊断和规划",
            ["诊断PDF", "规划处理任务"],
            plan,
            "如需开始救援，继续使用 rescue_pdf 并保留相同 path；默认会自动执行。",
        )

    if selected == "status":
        if not job_dir:
            raise ValueError("查询进度需要已有任务目录。")
        status = run_read_job_status(job_dir)
        return _workflow_response(
            "已读取任务状态",
            ["查看任务状态"],
            status,
            "任务仍在进行时可稍后用同一入口再次查询；疑似中断时可直接请求恢复。",
        )

    if selected == "audit":
        if not job_dir:
            raise ValueError("质量巡检需要已有任务目录。")
        audit = run_audit_job_quality(job_dir)
        return _workflow_response(
            "已完成质量巡检",
            ["巡检任务质量"],
            audit,
            "若结果提示低置信页，可用同一入口并提供页码查看页面证据。",
        )

    if selected == "resume":
        if not job_dir:
            raise ValueError("恢复任务需要已有任务目录。")
        resumed = run_resume_job(job_dir, mode=mode, password=password)
        return _workflow_response(
            "已尝试恢复任务",
            ["恢复任务"],
            resumed,
            "恢复后用同一入口查询进度和巡检质量。",
        )

    if selected == "evidence":
        if not job_dir:
            raise ValueError("查看页面证据需要已有任务目录。")
        actual_page_number = page_number or _page_number_from_request(request)
        if actual_page_number is None:
            raise ValueError("请提供需要核对的页码，例如 page_number=23。")
        wants_image = evidence_format == "image" or (
            evidence_format == "auto" and bool(request) and any(
                word in request for word in ("图片", "图像", "截图", "渲染")
            )
        )
        if wants_image:
            evidence = run_export_page_image_evidence(job_dir, actual_page_number, password=password)
            executed = ["导出页面图像证据"]
        else:
            evidence = read_page_evidence(job_dir, actual_page_number, include_blocks=False)
            executed = ["查看页面证据"]
        return _workflow_response(
            "已返回页面证据",
            executed,
            evidence,
            "如需核对其他页面，继续用同一入口并提供页码。",
        )

    if selected != "extract" or not path:
        raise ValueError("提取需要 PDF 路径。")

    plan = run_plan_pdf_job(path, target_quality=mode, password=password)
    route = str(plan.get("route", ""))
    if route == "password_required":
        return _workflow_response(
            "等待PDF密码",
            ["诊断PDF", "规划处理任务"],
            plan,
            "请在下一次 rescue_pdf 调用中提供 password；密码不会写入记录。",
        )
    if route == "repair_required":
        return _workflow_response(
            "PDF结构需要修复",
            ["诊断PDF", "规划处理任务"],
            plan,
            "当前文件无法安全提取；请先获得可打开的 PDF 副本后再调用。",
        )

    estimated_seconds = int(plan.get("estimated_seconds", 0) or 0)
    route_needs_ocr = route == "ocr_required"
    run_in_background = execution == "background" or (
        execution == "auto" and password is None and (route_needs_ocr or estimated_seconds > 60)
    )
    if run_in_background:
        job_dir, already = _task_manager.start_extraction(
            path,
            output_dir=output_dir,
            mode=mode,
            max_pages=max_pages,
            resume=True,
            password=password,
        )
        if ctx is not None:
            await ctx.info(
                f"已启动后台提取：{path}（预计 {estimated_seconds} 秒，约 {estimated_seconds // 60} 分钟）"
            )
        return _workflow_response(
            "已启动后台救援任务" if not already else "任务已在运行",
            ["诊断PDF", "规划处理任务", "后台提取书籍"],
            {
                "任务目录": job_dir,
                "已在运行": already,
                "线程模式": "进程内守护线程（复用OCR模型）",
            },
            "使用同一 rescue_pdf 并传入结果中的任务目录即可查询进度、巡检或恢复。",
        )

    callback = _make_progress_callback(ctx)
    if ctx is not None and not run_in_background:
        await ctx.info(
            f"开始前台提取：{path}（模式 {mode}，预计 {estimated_seconds} 秒）"
        )
    extracted = await asyncio.to_thread(
        run_extract_book_text,
        path,
        output_dir=output_dir,
        mode=mode,
        max_pages=max_pages,
        resume=True,
        password=password,
        progress_callback=callback,
    )
    return _workflow_response(
        "已完成PDF救援",
        ["诊断PDF", "规划处理任务", "提取书籍文本"],
        extracted,
        "可用同一 rescue_pdf 并传入任务目录进行质量巡检或查看页面证据。",
    )


@mcp.tool(
    name="run_health_check",
    title="运行体检",
    description="检查当前设备、内存、显卡和OCR相关依赖，给出推荐处理模式。快速模式不加载OCR模型，立即返回。",
)
def doctor_runtime(
    deep_probe: Annotated[bool, Field(description="是否深度探测（加载飞桨验证GPU，较慢）。默认false快速返回。")] = False,
) -> dict[str, Any]:
    return zh_data(run_doctor_runtime(deep_ocr_probe=deep_probe))


@mcp.tool(
    name="inspect_pdf_text_layer",
    title="检查PDF文本层",
    description="判断PDF是否已经包含完整可用文本层，识别扫描、混合、损坏和密码保护PDF。",
)
def inspect_pdf_text_layer(
    path: str,
    max_pages: int | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    return zh_data(run_inspect_pdf_text_layer(path, max_pages=max_pages, password=password))


@mcp.tool(
    name="diagnose_pdf",
    title="诊断PDF",
    description="检查PDF类型、乱码风险、扫描页比例和建议处理动作。",
)
def diagnose_pdf(
    path: str,
    max_pages: int | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    return inspect_pdf_text_layer(path, max_pages=max_pages, password=password)


@mcp.tool(
    name="plan_pdf_job",
    title="规划处理任务",
    description="根据当前设备和PDF状态规划速度与品质平衡的处理路线；密码不会写入记录。",
)
def plan_pdf_job(
    path: str,
    target_quality: str = "balanced",
    max_seconds: int | None = None,
    password: str | None = None,
) -> dict[str, Any]:
    return zh_data(
        run_plan_pdf_job(
            path,
            target_quality=target_quality,
            max_seconds=max_seconds,
            password=password,
        )
    )


@mcp.tool(
    name="extract_book_text",
    title="提取书籍文本",
    description="提取PDF为可校验的正文、分段文本、页面记录和质量审计。提取在进程内守护线程运行（复用OCR模型，不重复加载），工具立即返回任务目录，不阻塞。用 get_job_status 轮询页数、百分比和剩余时间；健康检测线程自动标记崩溃任务为失败。",
)
async def extract_book_text(
    path: str,
    output_dir: str | None = None,
    mode: str = "book-balanced",
    max_pages: int | None = None,
    resume: bool = True,
    password: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    plan = run_plan_pdf_job(path, target_quality=mode, password=password)
    estimated_seconds = int(plan.get("estimated_seconds", 0) or 0)
    route = str(plan.get("route", ""))
    if route in ("password_required", "repair_required"):
        return _workflow_response(
            "等待密码或修复" if route == "password_required" else "PDF结构需要修复",
            ["诊断PDF", "规划处理任务"],
            plan,
            "请提供密码或可打开的PDF副本后再调用。",
        )
    job_dir, already = _task_manager.start_extraction(
        path,
        output_dir=output_dir,
        mode=mode,
        max_pages=max_pages,
        resume=resume,
        password=password,
    )
    if ctx is not None:
        await ctx.info(
            f"已启动后台提取：{path}（预计 {estimated_seconds} 秒，约 {estimated_seconds // 60} 分钟）"
        )
    return {
        "状态": "已在运行" if already else "已启动后台提取",
        "说明": "提取在进程内守护线程运行，复用OCR模型，不阻塞。用 get_job_status 轮询进度。",
        "规划": plan,
        "任务目录": job_dir,
        "线程健康检测": "已启用（每15秒检测线程存活和状态更新，崩溃自动标记失败）",
        "后续步骤": [
            "用 get_job_status 传入任务目录查看实时进度（页数、百分比、剩余时间）",
            "用 audit_job_quality 巡检已处理页质量",
            "用 resume_job 恢复中断的任务",
        ],
    }


@mcp.tool(
    name="extract_book_background",
    title="后台提取书籍",
    description="启动可恢复的整本书后台提取任务，立即返回任务目录和进程信息，适合长时间扫描书籍。",
)
def start_book_extraction_background(
    path: str,
    output_dir: str | None = None,
    mode: str = "book-balanced",
    max_pages: int | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    source = Path(path).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(str(source))
    if not source.is_file():
        raise ValueError("来源路径必须是PDF文件")

    job_dir = (
        Path(output_dir).expanduser().resolve()
        if output_dir
        else (Path(os.environ["PDF_RESCUE_OUTPUT_ROOT"]).expanduser().resolve()
              if os.environ.get("PDF_RESCUE_OUTPUT_ROOT") else source.parent / "pdf_rescue_output")
        / f"{_safe_name(source)}-rescue-result"
    )
    job_dir.mkdir(parents=True, exist_ok=True)
    status_path = job_dir / "状态.json"
    existing_pid = _find_background_process(source, job_dir)
    if existing_pid:
        return {
            "状态": "已在运行",
            "任务目录": str(job_dir),
            "进程ID": existing_pid,
            "说明": "已有活跃任务，已避免重复启动。",
        }
    if status_path.exists():
        try:
            status_payload = run_read_job_status(job_dir)
            status = status_payload["状态"]
            if status.get("状态") == "进行中" and not status_payload["状态新鲜度"]["疑似中断"]:
                return {
                    "状态": "已在运行",
                    "任务目录": str(job_dir),
                    "进程ID": existing_pid,
                    "说明": "已有活跃任务，已避免重复启动。",
                }
        except (OSError, ValueError, KeyError, TypeError):
            pass

    log_path = timestamped_log_path(f"background-{_safe_name(source)}")
    child_env = os.environ.copy()
    child_env.update(
        {
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONLEGACYWINDOWSSTDIO": "0",
        }
    )
    source_root = str(Path(__file__).resolve().parents[1])
    existing_python_path = child_env.get("PYTHONPATH")
    child_env["PYTHONPATH"] = (
        source_root
        if not existing_python_path
        else os.pathsep.join((source_root, existing_python_path))
    )
    command = [
        sys.executable,
        "-u",
        "-m",
        "pdf_rescue_mcp.cli",
        "提取",
        str(source),
        "--output-dir",
        str(job_dir),
        "--mode",
        mode,
        "--json",
        "--resume" if resume else "--no-resume",
    ]
    if max_pages is not None:
        command.extend(["--max-pages", str(max_pages)])

    with log_path.open("a", encoding="utf-8") as log_handle:
        popen_kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": log_handle,
            "stderr": subprocess.STDOUT,
            "start_new_session": True,
            "env": child_env,
        }
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        process = subprocess.Popen(
            command,
            **popen_kwargs,
        )
    return {
        "状态": "已启动",
        "任务目录": str(job_dir),
        "进程ID": process.pid,
        "日志路径": project_relative_path(log_path),
        "启动方式": "后台 CLI 任务",
        "说明": "后台任务已启动；请用查看任务状态和巡检任务质量继续跟踪。",
    }


def _find_background_process(source: Path, job_dir: Path) -> int | None:
    """尽量返回同一来源和任务目录对应的后台进程号。"""
    source_text = str(source).lower()
    job_text = str(job_dir).lower()
    try:
        import psutil

        for process in psutil.process_iter(["pid", "cmdline"]):
            try:
                command_line = " ".join(process.info.get("cmdline") or []).lower()
            except (OSError, psutil.Error):
                continue
            if "pdf_rescue_mcp.cli" in command_line and source_text in command_line and job_text in command_line:
                return int(process.info["pid"])
    except (ImportError, OSError):
        return None
    return None


@mcp.tool(
    name="get_page_evidence",
    title="查看页面证据",
    description="读取指定页的识别文本、置信度、识别块和警告。",
)
def get_page_evidence(job_dir: str, page_number: int, include_blocks: bool = False) -> dict[str, Any]:
    return zh_data(read_page_evidence(job_dir, page_number, include_blocks=include_blocks))


@mcp.tool(
    name="get_term_glossary",
    title="查看专业术语词表",
    description="返回当前可审计的书名限定词表，供智能体依据页面证据补充明确错字。",
)
def get_term_glossary() -> dict[str, Any]:
    return zh_data(read_term_glossary())


@mcp.tool(
    name="update_term_glossary",
    title="更新专业术语词表",
    description="依据已核对的页面证据，向书名限定词表加入一条明确错字替换；不支持不确定或跨行替换。",
)
def add_term_glossary_replacement(
    rule_name: str,
    title_keywords: list[str],
    wrong: str,
    right: str,
) -> dict[str, Any]:
    return zh_data(add_glossary_replacement(rule_name, title_keywords, wrong, right))


@mcp.tool(
    name="export_page_image_evidence",
    title="导出页面图像证据",
    description="把指定页渲染成图片，保存到审计目录，并返回图像路径、页面记录和来源PDF；支持密码保护PDF。",
)
def export_page_image_evidence(
    job_dir: str,
    page_number: int,
    dpi: int = 160,
    password: str | None = None,
) -> dict[str, Any]:
    return zh_data(run_export_page_image_evidence(job_dir, page_number, dpi=dpi, password=password))


@mcp.tool(
    name="get_job_status",
    title="查看任务状态",
    description="读取书籍提取任务的进度、低置信页、失败页和质量报告位置。返回包含人类可读的进度摘要（页数、百分比、剩余时间）和三层监控信息（线程健康、状态文件新鲜度、自动重启次数）。",
)
def get_job_status(job_dir: str, stalled_after_seconds: int = 600) -> dict[str, Any]:
    payload = run_read_job_status(job_dir, stalled_after_seconds=stalled_after_seconds)
    status = payload.get("状态", {}) or {}
    target = int(status.get("目标页数") or 0)
    processed = int(status.get("已处理页数") or 0)
    pct = round(processed / target * 100, 1) if target else 0.0
    eta = status.get("预计剩余秒")
    avg = status.get("平均每页秒")
    runtime = payload.get("状态新鲜度", {}).get("运行判断", "未知")
    eta_text = f"{int(eta) // 60}分{int(eta) % 60}秒" if isinstance(eta, (int, float)) else "未知"
    avg_text = f"{round(float(avg), 1)}秒/页" if avg else "未知"
    # 三层监控：子进程存活 + 状态文件新鲜度 + 重启次数
    task_info = _task_manager.get_task_info(job_dir) or {}
    process_alive = task_info.get("存活", False)
    pid = task_info.get("进程ID")
    restart_count = task_info.get("重启次数", 0)
    health = f"子进程运行中（PID {pid}）" if process_alive else ("子进程已结束" if status.get("状态") == "完成" else "子进程未运行/已退出")
    if restart_count > 0:
        health += f" | 已自动重启 {restart_count} 次"
    summary = (
        f"进度：第 {processed}/{target} 页（{pct}%）| "
        f"速度：{avg_text} | 预计剩余：{eta_text} | 运行判断：{runtime} | 线程：{health}"
    )
    result = zh_data(payload)
    result["进度摘要"] = summary
    result["线程健康"] = {
        "存活": process_alive,
        "进程ID": pid,
        "重启次数": restart_count,
        "说明": health,
    }
    result["监控配置"] = {
        "监控间隔秒": _TaskManager.WATCH_INTERVAL,
        "单页超时秒": _TaskManager.PAGE_TIMEOUT,
        "最大自动重启": _TaskManager.MAX_AUTO_RESTART,
        "降级模式": _TaskManager.RESTART_DOWNGRADE_MODE,
    }
    return result


@mcp.tool(
    name="get_processing_history",
    title="查看处理历史",
    description="汇总指定目录下的PDF处理历史、状态、页数和质量指标；默认隐藏本地完整路径。",
)
def get_processing_history(
    root: str,
    max_records: int = 100,
    source_pdf: str | None = None,
    status: str | None = None,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    return zh_data(
        collect_processing_history(
            root,
            max_records=max_records,
            source_pdf=source_pdf,
            status=status,
            include_sensitive=include_sensitive,
        )
    )


@mcp.tool(
    name="share_processing_history",
    title="分享处理记录",
    description="生成可分享的处理历史JSON、Markdown和HTML文件，默认脱敏本地路径。",
)
def share_processing_records(
    root: str,
    output_dir: str | None = None,
    max_records: int = 500,
    include_sensitive: bool = False,
) -> dict[str, Any]:
    return zh_data(
        share_processing_history(
            root,
            output_dir=output_dir,
            max_records=max_records,
            include_sensitive=include_sensitive,
        )
    )


@mcp.tool(
    name="resume_job",
    title="恢复任务",
    description="检查任务是否疑似中断；确认中断后复用逐页缓存，从断点继续。恢复在进程内守护线程运行，立即返回任务目录，用查看任务状态跟踪进度和线程健康。",
)
async def resume_job(
    job_dir: str,
    mode: str | None = None,
    stalled_after_seconds: int = 600,
    force: bool = False,
    password: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    status_payload = run_read_job_status(job_dir, stalled_after_seconds=stalled_after_seconds)
    state = str(status_payload.get("状态", {}).get("状态") or "未知")
    if state == "完成":
        return zh_data({"动作": "无需恢复", "原因": "任务已经完成", "任务状态": status_payload})
    source_pdf = status_payload.get("状态", {}).get("来源PDF")
    target_pages = int(status_payload.get("状态", {}).get("目标页数") or 0)
    processed = int(status_payload.get("状态", {}).get("已处理页数") or 0)
    remaining = max(target_pages - processed, 0)
    selected_mode = mode or str(status_payload.get("状态", {}).get("模式") or "book-balanced")
    # 如果已在运行，直接返回
    if _task_manager.is_running(job_dir):
        return {
            "状态": "任务已在运行",
            "说明": "守护线程仍在提取中，无需重复恢复。",
            "任务状态": zh_data(status_payload),
            "后续步骤": ["用 get_job_status 查看实时进度和线程健康"],
        }
    if not source_pdf:
        raise ValueError("状态文件中缺少来源PDF，无法恢复任务")
    # 统一用守护线程恢复，立即返回
    actual_job_dir, already = _task_manager.start_extraction(
        source_pdf,
        output_dir=job_dir,
        mode=selected_mode,
        resume=True,
        password=password,
    )
    if ctx is not None:
        await ctx.info(f"已启动后台恢复：{job_dir}（剩余 {remaining} 页）")
    return {
        "状态": "已在运行" if already else "已启动后台恢复",
        "说明": f"剩余 {remaining} 页，守护线程复用缓存继续，不阻塞。",
        "任务状态": zh_data(status_payload),
        "任务目录": actual_job_dir,
        "线程健康检测": "已启用（崩溃自动标记失败）",
        "后续步骤": [
            "用 get_job_status 传入任务目录查看实时进度和线程健康",
            "用 audit_job_quality 巡检已处理页质量",
        ],
    }


@mcp.tool(
    name="audit_job_quality",
    title="巡检任务质量",
    description="巡检已输出页面或运行中逐页缓存，发现低置信页、缺页、分裂标题残留和图表噪声残留。",
)
def audit_job_quality(
    job_dir: str,
    max_issues: int = 80,
    use_latest_rules: bool = True,
) -> dict[str, Any]:
    return zh_data(run_audit_job_quality(job_dir, max_issues=max_issues, use_latest_rules=use_latest_rules))


@mcp.tool(
    name="scan_pdf_library",
    title="扫描书库",
    description="递归扫描目录中的PDF书籍，抽样检查文本层，生成书库清单和建议输出目录。",
)
def scan_pdf_library(
    root: str,
    output_dir: str | None = None,
    max_files: int | None = None,
    inspect_pages: int = 3,
) -> dict[str, Any]:
    return zh_data(
        run_scan_pdf_library(
            root,
            output_dir=output_dir,
            max_files=max_files,
            inspect_pages=inspect_pages,
        )
    )


@mcp.tool(
    name="batch_extract_library",
    title="批量提取书库",
    description="按目录顺序批量提取PDF书籍，在后台逐本处理，立即返回。用 get_batch_status 查看进度。支持断点续传。",
)
def batch_extract_library(
    root: str,
    output_dir: str | None = None,
    mode: str = "book-fast",
    max_books: int | None = None,
    max_pages_per_book: int | None = None,
    resume: bool = True,
) -> dict[str, Any]:
    return zh_data(
        _batch_manager.start_batch(
            root=root,
            output_dir=output_dir,
            mode=mode,
            max_books=max_books,
            resume=resume,
        )
    )


@mcp.tool(
    name="get_batch_status",
    title="查看批量任务状态",
    description="查看批量提取的整体进度：总书数、已完成、失败、当前书籍进度、运行时间。",
)
def get_batch_status() -> dict[str, Any]:
    return zh_data(_batch_manager.status())


@mcp.tool(
    name="stop_batch",
    title="停止批量任务",
    description="停止批量提取（当前正在处理的书籍会继续完成，不再启动下一本）。",
)
def stop_batch() -> dict[str, Any]:
    return zh_data(_batch_manager.stop())


def main() -> None:
    configure_utf8_stdio()
    configure_file_logging()
    mcp.run()


if __name__ == "__main__":
    main()
