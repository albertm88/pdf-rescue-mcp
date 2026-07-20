from __future__ import annotations

import asyncio
import hashlib
import json
import math
import subprocess
import sys
import os
import re
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Callable, Literal

from mcp.server.fastmcp import Context, FastMCP
from mcp.types import ToolAnnotations
from pydantic import Field

from .book_pipeline import _safe_name
from .book_pipeline import audit_job_quality as run_audit_job_quality
from .book_pipeline import add_term_glossary_replacement as add_glossary_replacement
from .book_pipeline import extract_book_text as run_extract_book_text
from .book_pipeline import export_page_image_evidence as run_export_page_image_evidence
from .book_pipeline import get_page_evidence as read_page_evidence
from .book_pipeline import get_term_glossary as read_term_glossary
from .book_pipeline import read_job_status as run_read_job_status
from .book_pipeline import _format_duration, _processing_metrics
from .capacity_benchmark import CapacityBenchmarkManager
from .library_pipeline import scan_pdf_library as run_scan_pdf_library
from .pdf_inspector import inspect_pdf_text_layer as run_inspect_pdf_text_layer
from .planner import plan_pdf_job as run_plan_pdf_job
from .paths import configure_file_logging, project_relative_path, timestamped_log_path
from .runtime import (
    collect_process_resource_usage,
    doctor_runtime as run_doctor_runtime,
)
from .stdio_encoding import configure_utf8_stdio
from .history import collect_processing_history, share_processing_history
from .iteration import build_iteration_plan
from .process_controller import ProcessController, WorkerHandle
from .resource_scheduler import ResourceScheduler, WorkerPlan, worker_threads_for_pages
from .supervisor import LocalSupervisor, SupervisedAttempt
from .task_store import Lease
from .throughput_tuning import ThroughputProfileStore
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
    """Supervise isolated OCR workers without making MCP share their process.

    The worker's heartbeat is authoritative.  A launcher PID is retained only for
    diagnostics because a Windows virtual-environment launcher may exit after it
    starts the real interpreter.  This keeps the MCP server responsive even while
    PaddleOCR is busy or wedged in native code.
    """

    WATCH_INTERVAL = 5
    HEARTBEAT_TIMEOUT = 90
    PROGRESS_TIMEOUT = 600
    STARTUP_TIMEOUT = 120
    STARTUP_PROGRESS_TIMEOUT = 600
    CANCEL_GRACE = 45
    MAX_AUTO_RESTART = 1
    TASK_STATE_FILE = "后台任务索引.json"
    TASK_METADATA_FILE = "后台任务.json"
    HEARTBEAT_FILE = "后台任务心跳.json"
    CANCEL_FILE = "停止请求.json"

    def __init__(
        self,
        *,
        enable_durable_supervision: bool = False,
        supervisor: LocalSupervisor | None = None,
        process_controller: ProcessController | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._tasks: dict[str, dict[str, Any]] = {}
        # Tasks whose durable lease still belongs to a recently stopped
        # controller.  They are deliberately kept out of `_tasks` until this
        # adapter wins the fencing lease, but remain in the durable index so a
        # partial takeover can never erase another live worker from recovery.
        self._pending_restores: dict[str, dict[str, Any]] = {}
        self._watcher: threading.Thread | None = None
        self._stopping = False
        self._enable_durable_supervision = enable_durable_supervision or supervisor is not None
        self._supervisor = supervisor
        self._process_controller = process_controller or ProcessController()
        self._runtime_state_path: Path | None = None

    @staticmethod
    def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temporary_path = handle.name
                handle.write(json.dumps(payload, ensure_ascii=False, indent=2))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, path)
            temporary_path = None
        finally:
            if temporary_path is not None:
                try:
                    Path(temporary_path).unlink(missing_ok=True)
                except OSError:
                    pass

    def _state_path(self) -> Path:
        if self._enable_durable_supervision:
            if self._runtime_state_path is None:
                from .runtime_paths import ensure_runtime_paths

                self._runtime_state_path = ensure_runtime_paths().state_dir / "mcp" / self.TASK_STATE_FILE
            return self._runtime_state_path
        from .paths import PROJECT_ROOT

        return PROJECT_ROOT / "tmp" / "mcp" / self.TASK_STATE_FILE

    def _get_supervisor(self) -> LocalSupervisor | None:
        if not self._enable_durable_supervision:
            return None
        if self._supervisor is None:
            self._supervisor = LocalSupervisor(process_controller=self._process_controller)
        return self._supervisor

    @staticmethod
    def _task_paths(job_dir: Path) -> tuple[Path, Path, Path]:
        return (
            job_dir / _TaskManager.TASK_METADATA_FILE,
            job_dir / _TaskManager.HEARTBEAT_FILE,
            job_dir / _TaskManager.CANCEL_FILE,
        )

    @staticmethod
    def _output_root(source: Path, output_dir: str | None) -> Path:
        if output_dir:
            return Path(output_dir).expanduser().resolve()
        configured_root = os.environ.get("PDF_RESCUE_OUTPUT_ROOT")
        return (
            Path(configured_root).expanduser().resolve()
            if configured_root
            else source.parent / "pdf_rescue_output"
        )

    def _save_state_locked(self) -> None:
        persisted = {**self._pending_restores, **self._tasks}
        serialized = {
            job_key: {
                key: value
                for key, value in info.items()
                if key not in {"process", "password", "supervision_context"}
            }
            for job_key, info in persisted.items()
        }
        try:
            self._atomic_json(self._state_path(), {"任务": serialized})
        except OSError:
            pass

    def _save_metadata_locked(self, info: dict[str, Any]) -> None:
        metadata_path = Path(info["metadata_path"])
        payload = {
            key: value
            for key, value in info.items()
            if key not in {"process", "password", "supervision_context"}
        }
        try:
            self._atomic_json(metadata_path, payload)
        except OSError:
            pass

    def _try_reattach_pending_locked(self) -> None:
        """Adopt only workers whose durable task lease is now available.

        A controller restart can happen before the old controller's lease
        naturally expires.  Retrying from the lightweight monitor preserves
        automatic recovery without a second MCP adapter cancelling or
        restarting a worker it does not own.
        """
        if not self._enable_durable_supervision:
            return
        supervisor = self._get_supervisor()
        if supervisor is None:
            return
        for job_key, info in list(self._pending_restores.items()):
            if self._is_terminal_state(self._status_state(Path(info["job_dir"]))):
                self._pending_restores.pop(job_key, None)
                continue
            context = supervisor.reattach(
                job_id=str(info.get("持久任务ID") or ""),
                attempt_id=(
                    str(info.get("处理尝试ID"))
                    if info.get("处理尝试ID")
                    else None
                ),
            )
            if context is None:
                continue
            info["supervision_context"] = context
            self._tasks[job_key] = info
            self._pending_restores.pop(job_key, None)

    def restore_pending(self, *, allow_takeover: bool = True) -> None:
        """Reattach owned monitors after an MCP restart without duplicating one."""
        try:
            payload = json.loads(self._state_path().read_text(encoding="utf-8"))
            tasks = payload.get("任务", {})
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return
        if not isinstance(tasks, dict):
            return
        with self._lock:
            for job_key, stored in tasks.items():
                if not isinstance(stored, dict):
                    continue
                # A controller takeover callback can race normal application
                # startup.  Re-reading the same durable task must remain
                # idempotent rather than turning an already-owned task into a
                # second pending lease candidate.
                if job_key in self._tasks or job_key in self._pending_restores:
                    continue
                info = dict(stored)
                info["process"] = None
                info.setdefault("job_dir", job_key)
                info.setdefault("metadata_path", str(Path(job_key) / self.TASK_METADATA_FILE))
                info.setdefault("heartbeat_path", str(Path(job_key) / self.HEARTBEAT_FILE))
                info.setdefault("cancel_path", str(Path(job_key) / self.CANCEL_FILE))
                if self._enable_durable_supervision:
                    if not allow_takeover:
                        continue
                    # A newly started MCP adapter is an observer until it wins
                    # the durable task lease.  Metadata alone is not authority
                    # to cancel, terminate, or restart a worker owned by a
                    # different VS Code/Trae/Codex/AnythingLLM adapter.
                    supervisor = self._get_supervisor()
                    context = (
                        supervisor.reattach(
                            job_id=str(info.get("持久任务ID") or ""),
                            attempt_id=(
                                str(info.get("处理尝试ID"))
                                if info.get("处理尝试ID")
                                else None
                            ),
                        )
                        if supervisor is not None
                        else None
                    )
                    if context is None:
                        # The old controller may have died only moments ago.
                        # Keep this as a passive retry candidate; it cannot
                        # supervise or mutate the worker until it owns the
                        # task lease.
                        if not self._is_terminal_state(self._status_state(Path(job_key))):
                            self._pending_restores[job_key] = info
                        continue
                    info["supervision_context"] = context
                self._tasks[job_key] = info
            if self._tasks or self._pending_restores:
                self._ensure_watcher_locked()

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any] | None:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            return payload if isinstance(payload, dict) else None
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _heartbeat(self, info: dict[str, Any]) -> dict[str, Any]:
        path = Path(info["heartbeat_path"])
        if not path.exists():
            return {
                "存在": False,
                "活跃": False,
                "距上次心跳秒数": None,
                "进程ID": None,
                "当前页": None,
                "最后完成页": None,
                "最后进度时间": None,
            }
        payload = self._read_json(path)
        if payload is None:
            return {
                "存在": True,
                "活跃": False,
                "距上次心跳秒数": None,
                "进程ID": None,
                "当前页": None,
                "最后完成页": None,
                "最后进度时间": None,
            }
        try:
            age = max(0, round(time.time() - path.stat().st_mtime))
        except OSError:
            age = None
        active = payload.get("状态") == "运行中" and age is not None and age < self.HEARTBEAT_TIMEOUT
        return {
            "存在": True,
            "活跃": active,
            "距上次心跳秒数": age,
            "进程ID": payload.get("进程ID"),
            "状态": payload.get("状态"),
            "当前页": payload.get("当前页"),
            "当前页开始时间": payload.get("当前页开始时间"),
            "最后完成页": payload.get("最后完成页"),
            "最后进度时间": payload.get("最后进度时间"),
        }

    @staticmethod
    def _progress_age_seconds(heartbeat: dict[str, Any]) -> int | None:
        value = heartbeat.get("最后进度时间") or heartbeat.get("当前页开始时间")
        if not value:
            return None
        try:
            updated = datetime.fromisoformat(str(value))
            now = datetime.now(updated.tzinfo) if updated.tzinfo else datetime.now()
            return max(0, round((now - updated).total_seconds()))
        except (TypeError, ValueError):
            return None

    def _startup_progress_is_stalled(self, info: dict[str, Any], heartbeat: dict[str, Any]) -> bool:
        """Detect a live worker that never reaches its first page boundary.

        The independent heartbeat proves only that Python is alive.  It does
        not prove that PDF inspection, model initialisation, or the first OCR
        page can make progress.  Keep this timeout separate from page-level
        stagnation so a long real page is not mistaken for a failed startup.
        """
        if not heartbeat.get("活跃"):
            return False
        if heartbeat.get("当前页") is not None or heartbeat.get("最后完成页") is not None:
            return False
        try:
            started_at = float(info.get("started_at") or 0)
        except (TypeError, ValueError):
            return False
        return started_at > 0 and time.time() - started_at >= self.STARTUP_PROGRESS_TIMEOUT

    def _progress_is_stalled(self, heartbeat: dict[str, Any]) -> bool:
        # Older workers did not emit page fields.  Preserve their heartbeat-only
        # compatibility instead of falsely interrupting them during an upgrade.
        if heartbeat.get("当前页") is None:
            return False
        age = self._progress_age_seconds(heartbeat)
        return age is not None and age >= self.PROGRESS_TIMEOUT

    @staticmethod
    def _pid_alive(pid: object) -> bool:
        try:
            import psutil

            return isinstance(pid, int) and pid > 0 and psutil.pid_exists(pid)
        except ImportError:
            return False

    @staticmethod
    def _launched_process_alive(info: dict[str, Any]) -> bool:
        process = info.get("process")
        poll = getattr(process, "poll", None)
        return bool(callable(poll) and poll() is None)

    @staticmethod
    def _status_state(job_dir: Path) -> str | None:
        payload = _TaskManager._read_json(job_dir / "状态.json")
        return str(payload.get("状态")) if payload and payload.get("状态") else None

    @staticmethod
    def _is_terminal_state(state: str | None) -> bool:
        return state in {"完成", "失败", "已取消", "未完成", "卡死"}

    def _is_live(self, info: dict[str, Any]) -> bool:
        if self._is_terminal_state(self._status_state(Path(info["job_dir"]))):
            return False
        heartbeat = self._heartbeat(info)
        if heartbeat["活跃"]:
            return True
        started_at = float(info.get("started_at", 0) or 0)
        # After an MCP restart there is no in-memory Popen handle.  Preserve the
        # startup grace window so a second supervisor cannot duplicate a worker
        # before its first heartbeat has been written.
        if (
            info.get("phase") == "启动中"
            and not info.get("cancel_requested_at")
            and time.time() - started_at < self.STARTUP_TIMEOUT
        ):
            return True
        return (
            time.time() - started_at < self.STARTUP_TIMEOUT
            and self._launched_process_alive(info)
        )

    def _initialise_status(self, info: dict[str, Any]) -> None:
        status_path = Path(info["job_dir"]) / "状态.json"
        payload = self._read_json(status_path) or {}
        # A resumed job can inherit ``无可用OCR引擎`` from an earlier failed
        # launcher.  The next worker may use a valid runtime, so that stale
        # value must not be rendered as a current failure throughout Paddle's
        # startup window.  Preserve page caches/counts, but reset only fields
        # that describe the new attempt's runtime readiness.
        payload.pop("失败原因", None)
        payload.pop("失败时间", None)
        payload.update(
            {
                "状态": "启动中",
                "来源PDF": info["source"],
                "模式": info["mode"],
                "引擎": "待worker确认",
                "OCR设备": None,
                "图形处理器已确认": False,
                "更新时间": datetime.now().isoformat(timespec="seconds"),
            }
        )
        self._atomic_json(status_path, payload)

    def _begin_supervision_locked(
        self,
        *,
        source: Path,
        base_dir: Path,
        mode: str,
        max_pages: int | None,
        resume: bool,
    ) -> SupervisedAttempt | None:
        supervisor = self._get_supervisor()
        if supervisor is None:
            return None
        return supervisor.begin(
            source_path=source,
            output_root=base_dir,
            mode=mode,
            max_pages=max_pages,
            resume=resume,
        )

    def _settle_supervision_locked(
        self,
        info: dict[str, Any],
        *,
        phase: str,
        retry: bool = False,
        reason: str | None = None,
    ) -> None:
        context = info.get("supervision_context")
        supervisor = self._get_supervisor()
        if not isinstance(context, SupervisedAttempt) or supervisor is None:
            return
        supervisor.settle(
            context,
            outcome=supervisor.outcome_for_status(phase),
            retry=retry,
            result={"job_dir": info.get("job_dir"), "phase": phase} if not retry else None,
            error={"reason": reason or phase} if phase != "完成" else None,
        )

    def _recover_orphaned_supervision_locked(self, info: dict[str, Any]) -> bool:
        """Release a stale durable attempt before an explicit resume.

        This path is reached only after local liveness checks have found no
        active worker.  A valid fencing lease still wins inside the supervisor,
        so a concurrent adapter cannot be stolen merely because this MCP host
        restarted.
        """
        supervisor = self._get_supervisor()
        job_id = info.get("持久任务ID")
        if supervisor is None or not isinstance(job_id, str) or not job_id:
            return False
        try:
            return supervisor.recover_orphan(
                job_id,
                reason="本地任务已终止，按显式恢复请求重新入队",
            )
        except Exception:
            return False

    def _renew_supervision_locked(self, info: dict[str, Any]) -> bool:
        context = info.get("supervision_context")
        supervisor = self._get_supervisor()
        if not isinstance(context, SupervisedAttempt) or supervisor is None:
            return True
        return supervisor.renew(context)

    def _capture_worker_handle_locked(
        self,
        info: dict[str, Any],
        *,
        pid: object,
        command: list[str] | tuple[str, ...],
        field: str = "worker_handle",
    ) -> None:
        if not isinstance(pid, int) or pid <= 0:
            return
        try:
            context = info.get("supervision_context")
            identity = self._process_controller.capture_identity(pid)
            handle = WorkerHandle(
                identity=identity,
                command=tuple(command),
                attempt_id=context.attempt_id if isinstance(context, SupervisedAttempt) else None,
            )
            info[field] = handle.to_dict()
            supervisor = self._get_supervisor()
            if isinstance(context, SupervisedAttempt) and supervisor is not None:
                supervisor.bind_worker(context, handle)
        except Exception:
            # A Windows launcher may disappear before psutil can observe it.  The
            # worker heartbeat will provide a second opportunity to capture the
            # real interpreter PID without sacrificing a successful launch.
            return

    def _launch_worker_locked(self, info: dict[str, Any]) -> None:
        source = Path(info["source"])
        job_dir = Path(info["job_dir"])
        heartbeat_path = Path(info["heartbeat_path"])
        cancel_path = Path(info["cancel_path"])
        try:
            heartbeat_path.unlink(missing_ok=True)
            cancel_path.unlink(missing_ok=True)
        except OSError:
            pass

        self._initialise_status(info)
        child_env = os.environ.copy()
        child_env.update(
            {
                "PYTHONUTF8": "1",
                "PYTHONIOENCODING": "utf-8",
                "PYTHONLEGACYWINDOWSSTDIO": "0",
                "PDF_RESCUE_HEARTBEAT_PATH": str(heartbeat_path),
                "PDF_RESCUE_CANCEL_PATH": str(cancel_path),
            }
        )
        if info.get("ocr_threads") is not None:
            child_env["PDF_RESCUE_OCR_THREADS"] = str(info["ocr_threads"])
        # Capacity profiles exclude a fixed number of cold-start pages from the
        # rolling OCR throughput metric.  It is an environment knob because the
        # child process owns the Paddle adapter and must never be hot-reconfigured.
        if info.get("ocr_profile_warmup_pages") is not None:
            child_env["PDF_RESCUE_OCR_PROFILE_WARMUP_PAGES"] = str(
                info["ocr_profile_warmup_pages"]
            )
        context = info.get("supervision_context")
        supervisor = self._get_supervisor()
        if isinstance(context, SupervisedAttempt) and supervisor is not None:
            child_env.update(supervisor.child_environment(context))
        if info.get("password"):
            child_env["PDF_RESCUE_PASSWORD"] = str(info["password"])
        project_root = Path(__file__).resolve().parents[2]
        source_root = project_root / "src"
        existing_python_path = child_env.get("PYTHONPATH")
        child_env["PYTHONPATH"] = (
            str(source_root)
            if not existing_python_path
            else os.pathsep.join((str(source_root), existing_python_path))
        )
        log_path = timestamped_log_path(f"extract-{_safe_name(source)}")
        command = [
            sys.executable,
            "-u",
            "-m",
            "pdf_rescue_mcp.cli",
            "提取",
            str(source),
            "--output-dir",
            str(job_dir.parent),
            "--mode",
            str(info["mode"]),
            "--json",
            "--resume" if info.get("resume", True) else "--no-resume",
        ]
        if info.get("max_pages") is not None:
            command.extend(["--max-pages", str(info["max_pages"])])

        # ``sys.platform`` is stable in production but is also used by launchers
        # and tests to select the target OS.  Keep group/session flags scoped to
        # that target while process termination remains on the injected controller.
        platform_is_windows = sys.platform.lower().startswith("win")
        launch_controller = (
            self._process_controller
            if self._process_controller.is_windows == platform_is_windows
            else ProcessController(platform_name=sys.platform)
        )
        popen_kwargs: dict[str, Any] = {
            "stdin": subprocess.DEVNULL,
            "stdout": None,
            "stderr": subprocess.STDOUT,
            "env": child_env,
            "cwd": str(project_root),
        }
        popen_kwargs.update(launch_controller.build_popen_kwargs())
        with log_path.open("a", encoding="utf-8") as log_handle:
            popen_kwargs["stdout"] = log_handle
            process = subprocess.Popen(command, **popen_kwargs)
        info["process"] = process
        info["launcher_pid"] = getattr(process, "pid", None)
        self._capture_worker_handle_locked(
            info,
            pid=info["launcher_pid"],
            command=command,
            field="launcher_handle",
        )
        info["log_path"] = str(log_path)
        info["phase"] = "启动中"
        info["cancel_requested_at"] = None
        self._save_metadata_locked(info)
        self._save_state_locked()

    def start_extraction(
        self,
        path: str,
        output_dir: str | None,
        mode: str,
        max_pages: int | None,
        resume: bool,
        password: str | None,
        ocr_threads: int | None = None,
        ocr_profile_warmup_pages: int | None = None,
    ) -> tuple[str, bool]:
        source = Path(path).expanduser().resolve()
        base_dir = self._output_root(source, output_dir)
        job_dir = (base_dir / f"{_safe_name(source)}-rescue-result").resolve()
        job_dir.mkdir(parents=True, exist_ok=True)
        job_key = str(job_dir)
        metadata_path, heartbeat_path, cancel_path = self._task_paths(job_dir)
        with self._lock:
            info = self._tasks.get(job_key)
            if info and self._is_live(info):
                return job_key, True
            pending_info = self._pending_restores.get(job_key)
            if pending_info and (
                self._is_live(pending_info)
                or self._pid_alive(self._heartbeat(pending_info).get("进程ID"))
            ):
                # A different adapter still owns the durable lease.  Reuse its
                # deterministic job path and let the passive reattach loop
                # take over only after that lease has expired.
                return job_key, True
            current_state = self._status_state(job_dir)
            if (
                info
                and not self._is_terminal_state(current_state)
                and self._pid_alive(self._heartbeat(info).get("进程ID"))
            ):
                # The old worker has no fresh heartbeat. Let the monitor stop it before retrying.
                self._request_cancel_locked(info)
                return job_key, True

            recovery_info = info
            stored = self._read_json(metadata_path)
            if stored:
                stored_info = dict(stored)
                stored_info.update(
                    {
                        "job_dir": job_key,
                        "metadata_path": str(metadata_path),
                        "heartbeat_path": str(heartbeat_path),
                        "cancel_path": str(cancel_path),
                        "process": None,
                    }
                )
                stored_state = self._status_state(job_dir)
                if self._is_live(stored_info) or (
                    not self._is_terminal_state(stored_state)
                    and self._pid_alive(self._heartbeat(stored_info).get("进程ID"))
                ):
                    self._tasks[job_key] = stored_info
                    self._ensure_watcher_locked()
                    return job_key, True
                recovery_info = stored_info

            if isinstance(recovery_info, dict):
                self._recover_orphaned_supervision_locked(recovery_info)

            supervision_context = self._begin_supervision_locked(
                source=source,
                base_dir=base_dir,
                mode=mode,
                max_pages=max_pages,
                resume=resume,
            )
            if self._enable_durable_supervision and supervision_context is None:
                # Another local MCP adapter already holds a lease for this exact
                # source/output/mode tuple.  Returning its deterministic job path
                # keeps all client integrations non-blocking and avoids duplicate OCR.
                return job_key, True

            info = {
                "job_dir": job_key,
                "source": str(source),
                "mode": mode,
                "max_pages": max_pages,
                "resume": resume,
                "password": password,
                "ocr_threads": ocr_threads,
                "ocr_profile_warmup_pages": ocr_profile_warmup_pages,
                "started_at": time.time(),
                "restart_count": 0,
                "phase": "启动中",
                "launcher_pid": None,
                "worker_pid": None,
                "metadata_path": str(metadata_path),
                "heartbeat_path": str(heartbeat_path),
                "cancel_path": str(cancel_path),
                "cancel_requested_at": None,
                "process": None,
                "supervision_context": supervision_context,
            }
            if supervision_context is not None:
                info.update(
                    {
                        "持久任务ID": supervision_context.job_id,
                        "处理尝试ID": supervision_context.attempt_id,
                        "任务数据库": str(self._get_supervisor().database_path),
                    }
                )
            self._pending_restores.pop(job_key, None)
            self._tasks[job_key] = info
            try:
                self._launch_worker_locked(info)
            except Exception as exc:
                self._settle_supervision_locked(
                    info,
                    phase="失败",
                    reason=f"launch_failed:{type(exc).__name__}",
                )
                self._tasks.pop(job_key, None)
                self._save_state_locked()
                raise
            self._ensure_watcher_locked()
        return job_key, False

    def _ensure_watcher_locked(self) -> None:
        if self._watcher is None or not self._watcher.is_alive():
            self._watcher = threading.Thread(target=self._watcher_loop, daemon=True, name="ocr-task-monitor")
            self._watcher.start()

    def _watcher_loop(self) -> None:
        while not self._stopping:
            time.sleep(self.WATCH_INTERVAL)
            with self._lock:
                self._try_reattach_pending_locked()
                for job_key, info in list(self._tasks.items()):
                    self._watch_task_locked(job_key, info)

    def _finish_task_locked(self, job_key: str, info: dict[str, Any], phase: str) -> None:
        info["phase"] = phase
        self._settle_supervision_locked(info, phase=phase)
        self._save_metadata_locked(info)
        self._tasks.pop(job_key, None)
        self._save_state_locked()

    def _watch_task_locked(self, job_key: str, info: dict[str, Any]) -> None:
        job_dir = Path(info["job_dir"])
        state = self._status_state(job_dir)
        if self._is_terminal_state(state):
            self._finish_task_locked(job_key, info, state or "已结束")
            return

        heartbeat = self._heartbeat(info)
        if heartbeat["活跃"]:
            info["worker_pid"] = heartbeat.get("进程ID")
            self._capture_worker_handle_locked(
                info,
                pid=heartbeat.get("进程ID"),
                command=("pdf_rescue_mcp.worker",),
            )
            if not self._renew_supervision_locked(info):
                info["phase"] = "监管租约失效"
                self._save_metadata_locked(info)
                self._save_state_locked()
                return
            if self._startup_progress_is_stalled(info, heartbeat):
                self._recover_stalled_locked(job_key, info, heartbeat)
                return
            if self._progress_is_stalled(heartbeat):
                self._recover_stalled_locked(job_key, info, heartbeat)
                return
            if info.get("phase") != "运行中":
                info["phase"] = "运行中"
                self._save_metadata_locked(info)
                self._save_state_locked()
            return

        started_at = float(info.get("started_at", 0) or 0)
        if (
            time.time() - started_at < self.STARTUP_TIMEOUT
            and not info.get("cancel_requested_at")
            and (info.get("phase") == "启动中" or self._launched_process_alive(info))
        ):
            return
        self._recover_stalled_locked(job_key, info, heartbeat)

    def _request_cancel_locked(
        self,
        info: dict[str, Any],
        *,
        reason: str = "监控检测到工作进程无响应",
    ) -> None:
        if info.get("cancel_requested_at"):
            return
        context = info.get("supervision_context")
        supervisor = self._get_supervisor()
        if isinstance(context, SupervisedAttempt) and supervisor is not None:
            try:
                supervisor.request_cancel(context, reason=reason)
            except Exception:
                pass
        elif supervisor is not None and isinstance(info.get("持久任务ID"), str):
            try:
                supervisor.store.request_cancel(info["持久任务ID"], reason=reason)
            except Exception:
                pass
        try:
            self._atomic_json(
                Path(info["cancel_path"]),
                {"请求时间": datetime.now().isoformat(timespec="seconds"), "原因": reason},
            )
        except OSError:
            pass
        info["phase"] = "请求停止中"
        info["cancel_requested_at"] = time.time()
        self._save_metadata_locked(info)
        self._save_state_locked()

    def request_cancel(self, job_dir: str, *, reason: str = "用户请求停止") -> dict[str, Any]:
        """Persist a cooperative stop request without blocking an MCP adapter."""
        job_key = str(Path(job_dir).expanduser().resolve())
        with self._lock:
            info = self._tasks.get(job_key)
            if info is None:
                metadata_path, heartbeat_path, cancel_path = self._task_paths(Path(job_key))
                stored = self._read_json(metadata_path)
                if stored is None:
                    raise FileNotFoundError(str(metadata_path))
                info = dict(stored)
                info.update(
                    {
                        "job_dir": job_key,
                        "metadata_path": str(metadata_path),
                        "heartbeat_path": str(heartbeat_path),
                        "cancel_path": str(cancel_path),
                        "process": None,
                    }
                )
                self._tasks[job_key] = info
            if self._is_terminal_state(self._status_state(Path(job_key))):
                return {"状态": "任务已结束", "任务目录": job_key}
            self._request_cancel_locked(info, reason=reason)
            return {
                "状态": "已请求安全停止",
                "任务目录": job_key,
                "说明": "工作进程会在当前页边界停止；若无响应，监管层会按超时策略终止进程树。",
            }

    @staticmethod
    def _terminate_process_tree(pid: object) -> None:
        if not isinstance(pid, int) or pid <= 0:
            return
        try:
            import psutil

            process = psutil.Process(pid)
            descendants = process.children(recursive=True)
            for item in descendants + [process]:
                try:
                    item.terminate()
                except (psutil.Error, OSError):
                    pass
            _, still_running = psutil.wait_procs(descendants + [process], timeout=5)
            for item in still_running:
                try:
                    item.kill()
                except (psutil.Error, OSError):
                    pass
        except (ImportError, OSError):
            pass

    def _terminate_owned_process_locked(self, info: dict[str, Any], fallback_pid: object) -> None:
        for field in ("worker_handle", "launcher_handle"):
            handle_data = info.get(field)
            if not isinstance(handle_data, dict):
                continue
            try:
                handle = WorkerHandle.from_dict(handle_data)
                if isinstance(fallback_pid, int) and handle.pid != fallback_pid:
                    continue
                result = self._process_controller.terminate_tree(
                    handle, grace_seconds=5, kill_wait_seconds=5
                )
                if result.identity_matched:
                    return
            except Exception:
                continue
        self._terminate_process_tree(fallback_pid)

    def _mark_failed_locked(self, job_key: str, info: dict[str, Any], reason: str) -> None:
        status_path = Path(info["job_dir"]) / "状态.json"
        payload = self._read_json(status_path) or {"来源PDF": info["source"]}
        payload.update(
            {
                "状态": "失败",
                "失败原因": reason,
                "失败时间": datetime.now().isoformat(timespec="seconds"),
                "更新时间": datetime.now().isoformat(timespec="seconds"),
            }
        )
        try:
            self._atomic_json(status_path, payload)
        except OSError:
            pass
        self._settle_supervision_locked(info, phase="失败", reason=reason)
        info["supervision_context"] = None
        self._finish_task_locked(job_key, info, "失败")

    def _recover_stalled_locked(
        self,
        job_key: str,
        info: dict[str, Any],
        heartbeat: dict[str, Any],
    ) -> None:
        if not info.get("cancel_requested_at"):
            self._request_cancel_locked(info)
            return
        if time.time() - float(info["cancel_requested_at"]) < self.CANCEL_GRACE:
            return

        worker_pid = heartbeat.get("进程ID") or info.get("worker_pid")
        if self._pid_alive(worker_pid):
            self._terminate_owned_process_locked(info, worker_pid)
        if self._launched_process_alive(info):
            self._terminate_owned_process_locked(info, info.get("launcher_pid"))
        if self._pid_alive(worker_pid) or self._launched_process_alive(info):
            return

        restart_count = int(info.get("restart_count", 0))
        if restart_count >= self.MAX_AUTO_RESTART:
            progress_age = self._progress_age_seconds(heartbeat)
            if progress_age is not None:
                reason = f"工作进程仍有心跳但第 {heartbeat.get('当前页')} 页已无前进 {progress_age} 秒，且自动恢复次数已用尽"
            else:
                age = heartbeat.get("距上次心跳秒数")
                reason = f"工作进程心跳已停止 {age} 秒，且自动恢复次数已用尽"
            self._mark_failed_locked(job_key, info, reason)
            return

        self._settle_supervision_locked(
            info,
            phase="卡死",
            retry=True,
            reason="worker_heartbeat_or_page_progress_stalled",
        )
        if not isinstance(info.get("supervision_context"), SupervisedAttempt):
            supervisor = self._get_supervisor()
            durable_job_id = info.get("持久任务ID")
            if supervisor is not None and isinstance(durable_job_id, str):
                try:
                    supervisor.recover_orphan(
                        durable_job_id,
                        reason="restored_supervisor_confirmed_worker_stalled",
                    )
                except Exception:
                    # The subsequent lease claim is the final ownership guard.
                    pass
        info["restart_count"] = restart_count + 1
        info["started_at"] = time.time()
        info["process"] = None
        info["worker_handle"] = None
        info["launcher_handle"] = None
        try:
            context = self._begin_supervision_locked(
                source=Path(info["source"]),
                base_dir=Path(info["job_dir"]).parent,
                mode=str(info["mode"]),
                max_pages=info.get("max_pages"),
                resume=bool(info.get("resume", True)),
            )
            if self._enable_durable_supervision and context is None:
                self._mark_failed_locked(job_key, info, "自动恢复未取得持久任务租约")
                return
            info["supervision_context"] = context
            if context is not None:
                info.update(
                    {
                        "持久任务ID": context.job_id,
                        "处理尝试ID": context.attempt_id,
                        "任务数据库": str(self._get_supervisor().database_path),
                    }
                )
        except Exception as exc:
            self._mark_failed_locked(job_key, info, f"自动恢复准备失败：{type(exc).__name__}: {exc}")
            return
        try:
            self._launch_worker_locked(info)
        except Exception as exc:
            self._mark_failed_locked(job_key, info, f"自动恢复启动失败：{type(exc).__name__}: {exc}")

    def is_running(self, job_dir: str) -> bool:
        return bool((self.get_task_info(job_dir) or {}).get("存活"))

    def get_task_info(self, job_dir: str) -> dict[str, Any] | None:
        job_key = str(Path(job_dir).resolve())
        with self._lock:
            info = self._tasks.get(job_key)
            if info is None:
                metadata_path, heartbeat_path, cancel_path = self._task_paths(Path(job_key))
                stored = self._read_json(metadata_path)
                if stored is None:
                    return None
                info = dict(stored)
                info.update(
                    {
                        "job_dir": job_key,
                        "metadata_path": str(metadata_path),
                        "heartbeat_path": str(heartbeat_path),
                        "cancel_path": str(cancel_path),
                        "process": None,
                    }
                )
            heartbeat = self._heartbeat(info)
            # ``存活`` is an admission/ownership signal, not merely a
            # heartbeat signal.  A newly spawned worker can take tens of
            # seconds to initialise Paddle and write its first heartbeat;
            # reporting it as dead during that startup grace makes the batch
            # scheduler launch duplicate books every polling interval.
            live = self._is_live(info)
            progress_age = self._progress_age_seconds(heartbeat)
            return {
                "存活": live,
                "工作进程ID": heartbeat.get("进程ID") or info.get("worker_pid"),
                "启动进程ID": info.get("launcher_pid"),
                "心跳": heartbeat,
                "重启次数": int(info.get("restart_count", 0)),
                "启动时间": info.get("started_at"),
                "模式": info.get("mode"),
                "监控阶段": info.get("phase"),
                "日志路径": info.get("log_path"),
                "持久任务ID": info.get("持久任务ID"),
                "处理尝试ID": info.get("处理尝试ID"),
                "任务数据库": info.get("任务数据库"),
                "页级前进已停滞": self._progress_is_stalled(heartbeat),
                "距上次页级前进秒数": progress_age,
            }

    def get_supervision_snapshot(self, job_dir: str) -> dict[str, Any] | None:
        """Return durable audit events for internal status and iteration reads."""
        job_key = str(Path(job_dir).expanduser().resolve())
        with self._lock:
            info = self._tasks.get(job_key)
            if info is None:
                metadata_path, _heartbeat_path, _cancel_path = self._task_paths(Path(job_key))
                info = self._read_json(metadata_path)
            if not isinstance(info, dict):
                return None
            job_id = info.get("持久任务ID")
            supervisor = self._get_supervisor()
            if not isinstance(job_id, str) or supervisor is None:
                return None
            try:
                return supervisor.task_snapshot(job_id, event_limit=200)
            except Exception:
                return None


_task_manager = _TaskManager(enable_durable_supervision=True)
_capacity_benchmark_manager = CapacityBenchmarkManager(
    task_starter=_task_manager.start_extraction,
    task_info_reader=_task_manager.get_task_info,
    task_canceller=lambda job_dir: _task_manager.request_cancel(
        job_dir,
        reason="容量基准已取消或检测到生产 OCR",
    ),
)


def _build_task_metrics(status: dict[str, Any], task_info: dict[str, Any]) -> dict[str, Any]:
    """合并业务层页级指标与监控层资源快照。

    优化层的重启次数/监控阶段仍由 ``工作进程健康`` 单独返回；这里仅把资源
    快照放入统一指标，避免业务层直接依赖 psutil 或进程控制。
    """
    raw_metrics = status.get("任务指标")
    metrics = dict(raw_metrics) if isinstance(raw_metrics, dict) else _processing_metrics(status)
    heartbeat = task_info.get("心跳") or {}
    worker_pid = task_info.get("工作进程ID") or heartbeat.get("进程ID")
    metrics["资源占用率"] = collect_process_resource_usage(worker_pid)
    return metrics


def _finite_number(value: object) -> float | None:
    """Return a finite metric value without turning a missing sample into zero."""
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _looks_like_resume_cache_replay(pages: float, seconds: float) -> bool:
    """Reject implausibly fast bulk progress caused by rebuilding page cache.

    A resumed worker may emit hundreds of already-OCRed cache records before
    it begins a single new OCR page.  Those records are valid recovery work,
    but they must not be presented as live OCR throughput.
    """
    return pages >= 8.0 and seconds > 0.0 and pages * 60.0 / seconds >= 10.0


def _nonnegative_integer(value: object) -> int | None:
    number = _finite_number(value)
    if number is None or number < 0 or not number.is_integer():
        return None
    return int(number)


def _iso_timestamp(value: object) -> float | None:
    """Parse a persisted ISO timestamp without inventing a time sample."""
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text).timestamp()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _worker_resource_summary(
    worker_resources: list[dict[str, object]],
    *,
    logical_cpu_count: int,
    total_memory_mb: float | None,
) -> dict[str, object]:
    """Aggregate existing worker snapshots without taking a second sample.

    A missing PID or metric remains visible as an incomplete observation.  This
    is important during the worker startup handshake: reporting an unknown
    worker as ``0`` would make the scheduler and an MCP client reach opposite
    conclusions about available capacity.
    """
    worker_count = len(worker_resources)
    pid_count = 0
    usable_count = 0
    complete_count = 0
    thread_sample_count = 0
    usages: list[dict[str, object]] = []
    configured_thread_budgets: list[int] = []

    for worker in worker_resources:
        usage = worker.get("资源占用率")
        snapshot = usage if isinstance(usage, dict) else {}
        usages.append(snapshot)
        if _nonnegative_integer(worker.get("进程ID")) is not None:
            pid_count += 1
        if snapshot.get("状态") == "可用":
            usable_count += 1
        if isinstance(snapshot.get("线程CPU占用率"), dict):
            thread_sample_count += 1
        if (
            snapshot.get("状态") == "可用"
            and _finite_number(snapshot.get("CPU等效核心数")) is not None
            and _finite_number(snapshot.get("内存MB")) is not None
            and _nonnegative_integer(snapshot.get("进程线程数")) is not None
        ):
            complete_count += 1
        budget = _nonnegative_integer(worker.get("线程预算"))
        if budget is not None:
            configured_thread_budgets.append(budget)

    def _total_number(key: str, digits: int = 1) -> float | None:
        values = [value for usage in usages if (value := _finite_number(usage.get(key))) is not None]
        return round(sum(values), digits) if values else None

    def _total_integer(key: str) -> int | None:
        values = [value for usage in usages if (value := _nonnegative_integer(usage.get(key))) is not None]
        return sum(values) if values else None

    cpu_core_values: list[float] = []
    for usage in usages:
        core_equivalents = _finite_number(usage.get("CPU等效核心数"))
        if core_equivalents is None:
            cpu_percent = _finite_number(usage.get("CPU占用率"))
            if cpu_percent is not None:
                core_equivalents = cpu_percent / 100.0 * max(1, logical_cpu_count)
        if core_equivalents is not None:
            cpu_core_values.append(max(0.0, core_equivalents))

    total_cpu_cores = round(sum(cpu_core_values), 2) if cpu_core_values else None
    total_cpu_percent = (
        round(min(100.0, total_cpu_cores / max(1, logical_cpu_count) * 100.0), 1)
        if total_cpu_cores is not None
        else None
    )
    total_rss_mb = _total_number("内存MB")
    memory_capacity_mb = _finite_number(total_memory_mb)
    total_memory_percent = (
        round(total_rss_mb / memory_capacity_mb * 100.0, 2)
        if total_rss_mb is not None and memory_capacity_mb is not None and memory_capacity_mb > 0
        else _total_number("内存占用率", digits=2)
    )

    return {
        "统计worker数": worker_count,
        "已取得PID的worker数": pid_count,
        "可采样worker数": usable_count,
        "不可采样worker数": worker_count - usable_count,
        "线程采样worker数": thread_sample_count,
        "采样完整": complete_count == worker_count,
        "总OCR线程预算": sum(configured_thread_budgets) if configured_thread_budgets else None,
        "总进程线程数": _total_integer("进程线程数"),
        "总活跃CPU线程数": _total_integer("活跃CPU线程数"),
        "总饱和CPU线程数": _total_integer("饱和CPU线程数"),
        "总RSS内存MB": total_rss_mb,
        "总内存占整机比例": total_memory_percent,
        "总运行内存占用MB": total_rss_mb,
        "总运行内存占整机比例": total_memory_percent,
        "总CPU等效核心数": total_cpu_cores,
        "总CPU占整机比例": total_cpu_percent,
        "资源汇总说明": (
            "总CPU占整机比例按各 worker CPU 等效核心数之和除以逻辑 CPU 数计算，"
            "固定为 0–100%；总CPU等效核心数可大于 1，不能当作百分比。"
            "总RSS内存MB为已采样进程 RSS 之和；采样不完整时不会把未知值计为 0。"
        ),
    }


class _BatchManager:
    """批量任务管理器：后台调度独立书籍 worker，MCP 服务器保持响应。

    支持状态持久化、书本级完成计数，以及按 CPU/内存实时占用动态调整并发。
    """

    BATCH_STATE_FILE = "批量状态.json"
    STOP_REQUEST_FILE = "批量停止请求.json"
    PAGE_RATE_SAMPLE_WINDOW_SECONDS = 300
    PAGE_RATE_MAX_SAMPLES = 12
    OBSERVER_TAKEOVER_INTERVAL = 5.0

    def __init__(
        self,
        *,
        use_portable_runtime: bool = False,
        controller_supervisor: LocalSupervisor | None = None,
        on_observer_promoted: Callable[[], None] | None = None,
    ) -> None:
        self._lock = threading.RLock()
        self._state_write_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._phase = "空闲"
        self._initialization_error: str | None = None
        self._max_books: int | None = None
        self._run_token = 0
        self._observer_only = False
        self._books: list[dict[str, Any]] = []
        self._current_index = -1
        self._current_job_dir: str | None = None
        self._completed: list[str] = []
        self._failed: list[dict[str, str]] = []
        self._active_jobs: dict[str, dict[str, Any]] = {}
        # Supervision-layer page-rate samples.  They are deliberately kept
        # outside the OCR worker: querying progress must never delay a page.
        self._worker_progress_samples: dict[str, dict[str, Any]] = {}
        # A status call reads this immutable-per-cycle supervision snapshot;
        # it never sleeps to sample worker CPU or mutates page-rate baselines.
        self._worker_supervision: dict[str, dict[str, Any]] = {}
        self._started_at: float | None = None
        self._mode = "book-fast"
        self._max_pages_per_book: int | None = None
        self._requested_workers: int | None = None
        self._resource_scheduler = ResourceScheduler()
        self._throughput_profiles = ThroughputProfileStore()
        self._last_worker_plan: WorkerPlan | None = None
        self._last_worker_plan_snapshot: dict[str, Any] = {}
        self._resume = True
        self._output_dir: str | None = None
        self._root: str | None = None
        self._state_path: Path | None = None
        self._use_portable_runtime = use_portable_runtime
        # One durable controller lease protects the single persisted batch
        # ledger.  A new stdio MCP process that cannot acquire it is an
        # observer: it may read the ledger and worker heartbeats, but cannot
        # start a second scheduler or mutate recovery state.
        # Keep SQLite lazy: importing an MCP adapter for tools/list must not
        # create/open a runtime database before it actually controls a batch.
        self._controller_supervisor = controller_supervisor
        self._on_observer_promoted = on_observer_promoted
        self._controller_lease: Lease | None = None
        self._lease_renewer: threading.Thread | None = None
        self._lease_stop = threading.Event()
        self._observer_takeover_thread: threading.Thread | None = None
        self._observer_takeover_stop = threading.Event()
        # A non-owning MCP host only mirrors the controller's durable snapshot.
        # It never samples a worker or writes the ledger merely to answer a
        # status request.
        self._observed_controller: dict[str, Any] = {}
        self._observed_state_mtime_ns: int | None = None
        # A transient local SQLite failure must never make a restored batch
        # look like an unsupervised local task. Keep its last durable snapshot
        # visible as an observer and retry the lease in the background.
        self._controller_lease_error: str | None = None

    def _state_file_path(self) -> Path:
        """返回批量状态文件路径。"""
        if self._state_path:
            return self._state_path
        if self._use_portable_runtime:
            from .runtime_paths import ensure_runtime_paths

            self._state_path = ensure_runtime_paths().state_dir / "mcp" / self.BATCH_STATE_FILE
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            return self._state_path
        from .paths import PROJECT_ROOT
        self._state_path = PROJECT_ROOT / "tmp" / "mcp" / self.BATCH_STATE_FILE
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        return self._state_path

    def _apply_persisted_state_locked(self, state: dict[str, Any]) -> bool:
        """Load one atomic controller snapshot without claiming authority."""
        if not isinstance(state, dict):
            return False
        self._root = state.get("根目录")
        self._output_dir = state.get("输出目录")
        self._mode = state.get("模式", "book-fast")
        self._max_books = state.get("最大书数")
        self._max_pages_per_book = state.get("每本最大页数")
        self._resume = bool(state.get("断点续传", True))
        self._started_at = state.get("开始时间")
        self._completed = list(state.get("已完成", []))
        self._failed = list(state.get("失败", []))
        self._active_jobs = {
            str(item.get("任务目录")): dict(item)
            for item in state.get("活动任务", [])
            if isinstance(item, dict) and item.get("任务目录")
        }
        raw_samples = state.get("worker页速采样", {})
        self._worker_progress_samples = (
            {
                str(job_dir): dict(sample)
                for job_dir, sample in raw_samples.items()
                if isinstance(sample, dict)
            }
            if isinstance(raw_samples, dict)
            else {}
        )
        raw_supervision = state.get("worker监管快照", {})
        self._worker_supervision = (
            {
                str(job_dir): dict(snapshot)
                for job_dir, snapshot in raw_supervision.items()
                if isinstance(snapshot, dict)
            }
            if isinstance(raw_supervision, dict)
            else {}
        )
        self._requested_workers = state.get("最大并发worker")
        self._resource_scheduler = ResourceScheduler(max_workers=self._requested_workers)
        raw_worker_plan = state.get("worker调度快照", {})
        self._last_worker_plan_snapshot = (
            dict(raw_worker_plan) if isinstance(raw_worker_plan, dict) else {}
        )
        # A persisted dict is authoritative for observers.  Only a controller
        # builds the richer in-memory plan object during its supervision loop.
        self._last_worker_plan = None
        self._books = list(state.get("书籍列表", []))
        self._current_index = state.get("当前索引", -1)
        self._current_job_dir = state.get("当前任务目录")
        self._initialization_error = state.get("初始化错误")
        self._running = bool(state.get("运行中"))
        self._phase = str(state.get("批处理阶段") or ("运行中" if self._running else "空闲"))
        observed_controller = state.get("控制器")
        self._observed_controller = (
            dict(observed_controller) if isinstance(observed_controller, dict) else {}
        )
        return True

    def _refresh_observer_state_locked(self) -> None:
        """Refresh an observer from the sole controller's durable snapshot.

        This intentionally performs only a local stat/read of an atomically
        replaced JSON file.  It is not a second scheduler, worker monitor, CPU
        sampler, or state writer, so repeated status calls from any LLM host
        cannot contend with OCR.
        """
        if not self._observer_only:
            return
        state_path = self._state_file_path()
        try:
            mtime_ns = state_path.stat().st_mtime_ns
        except OSError:
            return
        if self._observed_state_mtime_ns == mtime_ns:
            return
        state = _TaskManager._read_json(state_path)
        if state is None:
            return
        if self._apply_persisted_state_locked(state):
            self._observed_state_mtime_ns = mtime_ns

    def _stop_request_path(self) -> Path:
        return self._state_file_path().with_name(self.STOP_REQUEST_FILE)

    def _consume_stop_request_locked(self) -> bool:
        """Apply a command written by an observer without granting it control."""
        if not self._owns_controller_lease_locked():
            return False
        request_path = self._stop_request_path()
        if not request_path.exists():
            return False
        try:
            request_path.unlink(missing_ok=True)
        except OSError:
            return False
        self._running = False
        self._phase = "停止中"
        self._save_state()
        return True

    def _controller_resource_key(self) -> str:
        """Return a stable lease scope for the one persisted batch ledger."""
        state_path = str(self._state_file_path().resolve())
        digest = hashlib.sha256(state_path.encode("utf-8", errors="surrogatepass")).hexdigest()
        return f"batch-ledger-v1:{digest}"

    def _get_controller_supervisor(self) -> LocalSupervisor:
        if self._controller_supervisor is None:
            self._controller_supervisor = LocalSupervisor()
        return self._controller_supervisor

    @property
    def allows_task_recovery(self) -> bool:
        """Whether this adapter may attach task watchdogs during startup."""
        with self._lock:
            return not (self._running and self._observer_only)

    def _owns_controller_lease_locked(self) -> bool:
        lease = self._controller_lease
        if lease is None or self._observer_only:
            return False
        try:
            owns_lease = self._get_controller_supervisor().owns_controller_lease(lease)
        except Exception:
            owns_lease = False
        if not owns_lease:
            self._lose_controller_lease_locked()
        return owns_lease

    def _lose_controller_lease_locked(self) -> None:
        """Fence an old scheduler before it can write or launch another worker."""
        self._controller_lease = None
        self._observer_only = True
        self._run_token += 1
        self._lease_stop.set()
        if self._running:
            self._ensure_observer_takeover_locked()

    def _record_controller_lease_error_locked(self, exc: Exception) -> None:
        """Retain a running ledger as read-only if its lease backend is unavailable."""
        self._controller_lease = None
        self._observer_only = True
        self._controller_lease_error = f"{type(exc).__name__}: {exc}"[:240]
        self._lease_stop.set()
        if self._running:
            self._ensure_observer_takeover_locked()

    def _acquire_controller_lease_locked(self) -> bool:
        try:
            return self._acquire_controller_lease_once_locked()
        except Exception as exc:
            # A different MCP host may still own the live workers. Failing
            # closed as an observer avoids a duplicate scheduler while the
            # background observer loop retries after the backend recovers.
            self._record_controller_lease_error_locked(exc)
            return False

    def _acquire_controller_lease_once_locked(self) -> bool:
        supervisor = self._get_controller_supervisor()
        if self._controller_lease is not None:
            renewed = supervisor.renew_controller_lease(self._controller_lease)
            if renewed is not None:
                self._controller_lease = renewed
                self._observer_only = False
                self._controller_lease_error = None
                return True
            self._lose_controller_lease_locked()
        lease = supervisor.acquire_controller_lease(
            self._controller_resource_key()
        )
        if lease is None:
            self._observer_only = True
            self._controller_lease_error = None
            if self._running:
                self._ensure_observer_takeover_locked()
            return False
        self._controller_lease = lease
        self._observer_only = False
        self._controller_lease_error = None
        self._observer_takeover_stop.set()
        self._lease_stop.clear()
        self._ensure_lease_renewer_locked()
        return True

    def _ensure_lease_renewer_locked(self) -> None:
        if self._lease_renewer is not None and self._lease_renewer.is_alive():
            return
        self._lease_renewer = threading.Thread(
            target=self._lease_renewer_loop,
            daemon=True,
            name="batch-controller-lease-renewer",
        )
        self._lease_renewer.start()

    def _lease_renewer_loop(self) -> None:
        try:
            supervisor = self._get_controller_supervisor()
            interval = max(1.0, supervisor.CONTROLLER_LEASE_SECONDS / 3.0)
        except Exception as exc:
            with self._lock:
                self._record_controller_lease_error_locked(exc)
            return
        while not self._lease_stop.wait(interval):
            with self._lock:
                lease = self._controller_lease
                if lease is None:
                    return
                try:
                    renewed = supervisor.renew_controller_lease(lease)
                except Exception as exc:
                    self._record_controller_lease_error_locked(exc)
                    return
                if renewed is not None:
                    self._controller_lease = renewed
                    self._controller_lease_error = None
                    continue
                # Do not write a terminal state or touch a worker after losing
                # fencing.  Another adapter may already have taken over.
                self._lose_controller_lease_locked()
                return

    def _ensure_observer_takeover_locked(self) -> None:
        """Keep one passive lease watcher for automatic controller failover."""
        if (
            self._observer_takeover_thread is not None
            and self._observer_takeover_thread.is_alive()
        ):
            return
        self._observer_takeover_stop.clear()
        self._observer_takeover_thread = threading.Thread(
            target=self._observer_takeover_loop,
            daemon=True,
            name="batch-controller-observer",
        )
        self._observer_takeover_thread.start()

    def _observer_takeover_loop(self) -> None:
        """Promote one observer only after the prior controller lease expires."""
        while not self._observer_takeover_stop.wait(self.OBSERVER_TAKEOVER_INTERVAL):
            with self._lock:
                if not (self._running and self._observer_only):
                    return
                self._refresh_observer_state_locked()
                if not self._running or not self._acquire_controller_lease_locked():
                    continue
                # A successful fencing acquire makes any older scheduler run
                # token invalid before this adapter starts recovery work.
                self._run_token += 1
                run_token = self._run_token
                state = _TaskManager._read_json(self._state_file_path())
                if state is not None:
                    self._apply_persisted_state_locked(state)
                if not self._running or not self._root:
                    self._release_controller_lease_locked()
                    return
                phase = self._phase

            # Reattach task monitors after, never before, this adapter owns the
            # batch lease.  The task manager's pending restore queue handles
            # any worker lease that has not expired yet.
            if self._on_observer_promoted is not None:
                try:
                    self._on_observer_promoted()
                except Exception:
                    pass

            with self._lock:
                if not self._is_current_controller_run_locked(run_token):
                    return
                if phase == "准备中":
                    self._start_preparation_locked(run_token)
                    return
                if phase == "启动失败":
                    self._running = False
                    self._save_state()
                    self._release_controller_lease_locked()
                    return

            ledger_changed = self._reconcile_book_ledger()
            if self._requeue_missing_engine_failures() or ledger_changed:
                self._save_state()
            with self._lock:
                if self._is_current_controller_run_locked(run_token):
                    self._phase = "运行中"
                    self._start_batch_loop_locked(run_token)
            return

    def _release_controller_lease_locked(self) -> None:
        lease = self._controller_lease
        self._controller_lease = None
        self._lease_stop.set()
        self._observer_takeover_stop.set()
        if lease is not None:
            try:
                self._get_controller_supervisor().release_controller_lease(lease)
            except Exception:
                pass

    def _is_current_controller_run_locked(self, run_token: int | None) -> bool:
        return (
            self._owns_controller_lease_locked()
            and (run_token is None or run_token == self._run_token)
        )

    def _save_state(self) -> None:
        """将当前批量状态持久化到磁盘。"""
        # One controller writes a snapshot at a time.  Atomic replace prevents
        # torn JSON; the write lock prevents a slow earlier snapshot from
        # overwriting a newer completion/admission snapshot in this process.
        try:
            with self._lock:
                if not self._owns_controller_lease_locked():
                    return
                with self._state_write_lock:
                    state = {
                        "运行中": self._running,
                        "批处理阶段": self._phase,
                        "初始化错误": self._initialization_error,
                        "最大书数": self._max_books,
                        "根目录": self._root,
                        "输出目录": self._output_dir,
                        "模式": self._mode,
                        "每本最大页数": self._max_pages_per_book,
                        "断点续传": self._resume,
                        "开始时间": self._started_at,
                        "当前索引": self._current_index,
                        "当前任务目录": self._current_job_dir,
                        "已完成": list(self._completed),
                        "失败": list(self._failed),
                        "活动任务": [dict(item) for item in self._active_jobs.values()],
                        "worker页速采样": dict(self._worker_progress_samples),
                        "worker监管快照": dict(self._worker_supervision),
                        "worker调度快照": dict(self._last_worker_plan_snapshot),
                        "最大并发worker": self._requested_workers,
                        "书籍列表": list(self._books),
                        "控制器": {
                            "角色": "控制器" if self._controller_lease else "未持有",
                            "所有者": (
                                self._get_controller_supervisor().owner_id
                                if self._controller_lease else None
                            ),
                        },
                    }
                    _TaskManager._atomic_json(self._state_file_path(), state)
        except Exception:
            pass

    def _start_preparation_locked(self, run_token: int) -> None:
        self._thread = threading.Thread(
            target=self._prepare_and_run_batch,
            args=(run_token,),
            daemon=True,
            name="batch-library-discovery",
        )
        self._thread.start()

    def _start_batch_loop_locked(self, run_token: int) -> None:
        self._thread = threading.Thread(
            target=self._batch_loop,
            args=(run_token,),
            daemon=True,
            name="batch-extractor",
        )
        self._thread.start()

    def _prepare_and_run_batch(self, run_token: int) -> None:
        """Discover PDFs outside the MCP request and the batch state lock."""
        try:
            from .library_pipeline import scan_pdf_library

            with self._lock:
                root = self._root
                output_dir = self._output_dir
                max_books = self._max_books
            if not root:
                raise ValueError("批量任务缺少书库根目录")
            scan = scan_pdf_library(root, output_dir=output_dir, inspect_pages=3)
            books = scan.get("书籍", [])
            need_ocr = [
                book
                for book in books
                if book.get("PDF类型") in ("纯扫描PDF", "混合PDF", "未知")
                or "OCR" in str(book.get("建议动作", ""))
                or book.get("PDF类型") is None
            ]
            if max_books is not None:
                need_ocr = need_ocr[:max_books]
        except Exception as exc:
            with self._lock:
                if not self._is_current_controller_run_locked(run_token):
                    return
                self._initialization_error = f"{type(exc).__name__}: {exc}"
                self._phase = "启动失败"
                self._running = False
                self._save_state()
                self._release_controller_lease_locked()
            return

        with self._lock:
            if not self._is_current_controller_run_locked(run_token):
                return
            self._consume_stop_request_locked()
            if not self._running:
                self._phase = "已停止"
                self._save_state()
                self._release_controller_lease_locked()
                return
            self._books = list(need_ocr)
            self._initialization_error = None
            self._phase = "运行中"
            self._save_state()
        self._batch_loop(run_token)

    def restore_pending(self) -> None:
        """Restore a batch as its sole controller, or as a read-only observer."""
        try:
            state_path = self._state_file_path()
            if not state_path.exists():
                return
            state = _TaskManager._read_json(state_path)
            if state is None or not state.get("运行中"):
                return
            with self._lock:
                self._apply_persisted_state_locked(state)
                # A restored running ledger has no local authority until the
                # durable controller lease is positively acquired.
                self._observer_only = True
                self._controller_lease_error = None
                try:
                    self._observed_state_mtime_ns = state_path.stat().st_mtime_ns
                except OSError:
                    self._observed_state_mtime_ns = None
                self._run_token += 1
                run_token = self._run_token
                if not self._root:
                    return
                if not self._acquire_controller_lease_locked():
                    return
                run_token = self._run_token

                if self._phase == "准备中":
                    self._start_preparation_locked(run_token)
                    return
                if self._phase == "启动失败":
                    self._running = False
                    self._release_controller_lease_locked()
                    return

            # An OCR runtime may be unavailable after a controller/runtime
            # update.  That is a recoverable infrastructure failure, not a
            # failed book: its atomic page cache remains the resume boundary.
            ledger_changed = self._reconcile_book_ledger()
            if self._requeue_missing_engine_failures() or ledger_changed:
                self._save_state()

            with self._lock:
                if self._is_current_controller_run_locked(run_token):
                    self._phase = "运行中"
                    self._start_batch_loop_locked(run_token)
        except Exception as exc:
            # Restoration is best effort.  A future adapter may take over only
            # after the durable lease expires, never concurrently.
            with self._lock:
                self._release_controller_lease_locked()
                self._record_controller_lease_error_locked(exc)

    def start_batch(
        self,
        root: str,
        output_dir: str | None,
        mode: str,
        max_books: int | None,
        max_pages_per_book: int | None,
        resume: bool,
        max_workers: int | None = None,
    ) -> dict[str, Any]:
        """启动批量提取，立即返回，后台逐本处理。"""
        with self._lock:
            if self._running:
                return {
                    "状态": "由其他控制器监管" if self._observer_only else "已在运行",
                    "当前进度": self.status(),
                    "后续只读调用": {
                        "工具": "get_batch_status",
                        "参数": {},
                        "只读": True,
                    },
                }

            self._run_token += 1
            run_token = self._run_token
            if not self._acquire_controller_lease_locked():
                self._running = True
                self._phase = "由其他控制器监管"
                return {
                    "状态": "由其他控制器监管",
                    "说明": "另一个本机 MCP 控制器持有批量监管租约；当前适配器仅观察，不会重复扫描或启动 worker。",
                    "后续只读调用": {
                        "工具": "get_batch_status",
                        "参数": {},
                        "只读": True,
                    },
                }

            run_token = self._run_token

            self._root = root
            self._output_dir = output_dir
            self._mode = mode
            self._max_books = max_books
            self._max_pages_per_book = max_pages_per_book
            self._resume = resume
            self._started_at = time.time()
            self._completed = []
            self._failed = []
            self._active_jobs = {}
            self._worker_progress_samples = {}
            self._worker_supervision = {}
            self._books = []
            self._current_index = -1
            self._current_job_dir = None
            self._requested_workers = max_workers
            self._resource_scheduler = ResourceScheduler(max_workers=max_workers)
            # Admission planning samples CPU and memory.  It belongs to the
            # background supervision loop, never to an MCP start request.
            self._last_worker_plan = None
            self._last_worker_plan_snapshot = {
                "状态": "等待监管调度采样",
                "reason": "书库发现完成后由后台监管层计算并发计划。",
                "cpu_count": None,
                "total_memory_gb": None,
            }
            self._running = True
            self._phase = "准备中"
            self._initialization_error = None
            try:
                self._stop_request_path().unlink(missing_ok=True)
            except OSError:
                pass
            self._save_state()
            self._start_preparation_locked(run_token)

            return {
                "状态": "准备中",
                "批处理阶段": "准备中",
                "总书数": None,
                "模式": mode,
                "输出目录": output_dir,
                "每本最大页数": max_pages_per_book,
                "断点续传": resume,
                "最大并发worker": max_workers,
                "并发策略": "按CPU线程、系统可用内存和worker实时占用动态调整；书库发现由监管层后台完成。",
                "后续只读调用": {
                    "工具": "get_batch_status",
                    "参数": {},
                    "只读": True,
                },
            }

    def _expected_book_pages(self, book: dict[str, Any], status: dict[str, Any] | None) -> int:
        source_total = 0
        for value in (book.get("总页数"), (status or {}).get("PDF总页数")):
            try:
                source_total = max(source_total, int(value or 0))
            except (TypeError, ValueError):
                continue
        if self._max_pages_per_book:
            return min(source_total, self._max_pages_per_book) if source_total else self._max_pages_per_book
        return source_total or int((status or {}).get("目标页数") or 0)

    def _is_book_complete(self, book: dict[str, Any], status: dict[str, Any] | None) -> bool:
        if not status or status.get("状态") != "完成":
            return False
        expected = self._expected_book_pages(book, status)
        try:
            processed = int(status.get("已处理页数") or 0)
            target = int(status.get("目标页数") or 0)
        except (TypeError, ValueError):
            return False
        if expected <= 0:
            return processed > 0 and processed >= target > 0
        return processed >= expected and target >= expected

    def _record_book_completion(self, book_name: str) -> None:
        if book_name not in self._completed:
            self._completed.append(book_name)
        self._failed = [item for item in self._failed if item.get("书名") != book_name]

    def _record_book_failure(self, book_name: str, reason: str) -> None:
        self._completed = [name for name in self._completed if name != book_name]
        if not any(item.get("书名") == book_name for item in self._failed):
            self._failed.append({"书名": book_name, "原因": reason})

    def _observe_worker_page_rate(
        self,
        *,
        job_key: str,
        worker_pid: int | None,
        heartbeat: dict[str, Any],
        status: dict[str, Any],
        metrics: dict[str, Any],
    ) -> tuple[dict[str, object], bool]:
        """Measure actual page completion speed from two supervision samples.

        OCR-engine timings and cumulative elapsed time are useful diagnostics,
        but neither necessarily represents current end-to-end page progress
        after cache reuse or a resumed attempt.  This monitoring-layer sample
        therefore uses only durable completed-page deltas and their timestamps.
        A PID/page/timestamp reset starts a new series instead of mixing runs.
        """
        completed_pages = _nonnegative_integer(heartbeat.get("最后完成页"))
        if completed_pages is None:
            completed_pages = _nonnegative_integer(metrics.get("已处理页数"))
        if completed_pages is None:
            return {
                "状态": "不可用",
                "文本": "暂无可确认的已完成页数",
                "页每分钟": None,
                "秒每页": None,
                "样本页数": 0,
                "样本时长秒": None,
                "速度口径": "监督层页级完成增量",
            }, False

        progress_at = _iso_timestamp(heartbeat.get("最后进度时间"))
        time_source = "工作进程心跳最后进度时间"
        if progress_at is None:
            progress_at = _iso_timestamp(status.get("更新时间"))
            time_source = "状态文件更新时间"

        observed_at = time.time()
        previous_raw = self._worker_progress_samples.get(job_key)
        previous = dict(previous_raw) if isinstance(previous_raw, dict) else {}
        previous_pid = _nonnegative_integer(previous.get("worker_pid"))
        previous_pages = _nonnegative_integer(previous.get("completed_pages"))
        previous_progress_at = _finite_number(previous.get("progress_at"))
        previous_observed_at = _finite_number(previous.get("observed_at"))

        reset_reason = None
        if previous:
            if worker_pid is not None and previous_pid is not None and worker_pid != previous_pid:
                reset_reason = "Worker PID 已变化"
            elif previous_pages is not None and completed_pages < previous_pages:
                reset_reason = "完成页数回退"
            elif (
                progress_at is not None
                and previous_progress_at is not None
                and progress_at < previous_progress_at
            ):
                reset_reason = "页级进度时间回退"

        samples: list[dict[str, float]] = []
        cache_replay_discarded = False
        if reset_reason is None:
            for item in previous.get("samples", []):
                if not isinstance(item, dict):
                    continue
                pages = _finite_number(item.get("pages"))
                seconds = _finite_number(item.get("seconds"))
                recorded_at = _finite_number(item.get("recorded_at"))
                if (
                    pages is not None
                    and pages > 0
                    and seconds is not None
                    and seconds > 0
                    and recorded_at is not None
                    and observed_at - recorded_at <= self.PAGE_RATE_SAMPLE_WINDOW_SECONDS
                ):
                    if _looks_like_resume_cache_replay(pages, seconds):
                        cache_replay_discarded = True
                    else:
                        samples.append(
                            {"pages": pages, "seconds": seconds, "recorded_at": recorded_at}
                        )

        advanced = False
        sample_source = time_source
        if previous_pages is not None and completed_pages > previous_pages:
            delta_pages = completed_pages - previous_pages
            if progress_at is not None and previous_progress_at is not None:
                delta_seconds = progress_at - previous_progress_at
            elif previous_observed_at is not None:
                # Older workers may not expose a page timestamp.  This is a
                # conservative polling-window fallback and is named as such.
                delta_seconds = observed_at - previous_observed_at
                sample_source = "状态轮询时间（兼容旧worker）"
            else:
                delta_seconds = None
            if delta_seconds is not None and delta_seconds > 0:
                if _looks_like_resume_cache_replay(float(delta_pages), float(delta_seconds)):
                    cache_replay_discarded = True
                else:
                    samples.append(
                        {
                            "pages": float(delta_pages),
                            "seconds": float(delta_seconds),
                            "recorded_at": observed_at,
                        }
                    )
                    advanced = True

        samples = samples[-self.PAGE_RATE_MAX_SAMPLES :]
        next_state = {
            "worker_pid": worker_pid,
            "completed_pages": completed_pages,
            "progress_at": progress_at,
            "observed_at": observed_at,
            "samples": samples,
        }
        changed = reset_reason is not None or previous != next_state
        # Do not write the batch state merely because a poll happened while a
        # difficult page was still running.  Persist a new baseline, a page
        # advance, or a reset so a controller restart retains the rate window.
        if previous_pages == completed_pages and reset_reason is None:
            next_state["observed_at"] = previous.get("observed_at", observed_at)
            changed = previous != next_state
        if changed:
            self._worker_progress_samples[job_key] = next_state

        sample_pages = sum(item["pages"] for item in samples)
        sample_seconds = sum(item["seconds"] for item in samples)
        if sample_pages > 0 and sample_seconds > 0:
            seconds_per_page = round(sample_seconds / sample_pages, 2)
            pages_per_minute = round(60.0 / seconds_per_page, 3)
            text = (
                f"{pages_per_minute:.3f}页/分钟（{seconds_per_page:.2f}秒/页，"
                f"近{int(sample_pages)}页/{_format_duration(sample_seconds)}）"
            )
            if not advanced:
                text += "；等待下一页完成"
            if cache_replay_discarded:
                text += "；已排除断点缓存回放"
            return {
                "状态": "已采样" if advanced else "等待下一页完成",
                "文本": text,
                "页每分钟": pages_per_minute,
                "秒每页": seconds_per_page,
                "样本页数": int(sample_pages),
                "样本时长秒": round(sample_seconds, 1),
                "速度口径": f"监督层页级完成增量（{sample_source}）",
                "重置原因": reset_reason,
                "已排除缓存回放": cache_replay_discarded,
            }, changed

        reset_text = f"；已重置：{reset_reason}" if reset_reason else ""
        if cache_replay_discarded:
            reset_text += "；已排除断点缓存回放"
        return {
            "状态": "采样中",
            "文本": f"采样中（已确认完成 {completed_pages} 页，等待下一页完成）{reset_text}",
            "页每分钟": None,
            "秒每页": None,
            "样本页数": 0,
            "样本时长秒": None,
            "速度口径": f"监督层页级完成增量（{time_source}）",
            "重置原因": reset_reason,
            "已排除缓存回放": cache_replay_discarded,
        }, changed

    def _next_book_index(self) -> int:
        """Return the first book index that is not already owned by a worker."""
        active_indices: list[int] = []
        for record in self._active_jobs.values():
            try:
                active_indices.append(int(record.get("索引")))
            except (TypeError, ValueError):
                continue
        if active_indices:
            return max(active_indices) + 1
        return max(0, self._current_index) if self._current_index >= 0 else 0

    def _refresh_worker_supervision(
        self,
        active_records: list[dict[str, Any]],
    ) -> tuple[list[int], dict[int, int], list[float], bool]:
        """Sample workers once in the supervision loop for both status and admission.

        The expensive per-process CPU window belongs here, never in
        ``get_batch_status``.  Every caller thereafter receives the last
        bounded snapshot together with its sample time instead of competing
        with scheduling or OCR for process inspection.
        """
        from .library_pipeline import _read_status

        worker_pids: list[int] = []
        worker_thread_budgets: dict[int, int] = {}
        throughput_samples: list[float] = []
        snapshots: dict[str, dict[str, Any]] = {}
        speed_samples_changed = False
        sampled_at = datetime.now().isoformat(timespec="seconds")
        for record in active_records:
            job_key = str(record.get("任务目录") or "")
            if not job_key:
                continue
            info = _task_manager.get_task_info(job_key) or {}
            heartbeat = info.get("心跳") if isinstance(info.get("心跳"), dict) else {}
            candidate_pid = _nonnegative_integer(info.get("工作进程ID"))
            pid = candidate_pid if bool(info.get("存活")) else None
            if pid is not None:
                worker_pids.append(pid)
                try:
                    worker_thread_budgets[pid] = max(1, int(record.get("线程预算") or 1))
                except (TypeError, ValueError):
                    pass
            status = _read_status(Path(job_key)) or {}
            raw_metrics = status.get("任务指标")
            metrics = dict(raw_metrics) if isinstance(raw_metrics, dict) else _processing_metrics(status)
            usage = collect_process_resource_usage(pid)
            metrics["资源占用率"] = usage
            with self._lock:
                page_rate, rate_changed = self._observe_worker_page_rate(
                    job_key=job_key,
                    worker_pid=pid,
                    heartbeat=heartbeat,
                    status=status,
                    metrics=metrics,
                )
            speed_samples_changed = speed_samples_changed or rate_changed
            short_ocr_rate = _finite_number(metrics.get("短窗OCR页每分钟"))
            if short_ocr_rate is not None and short_ocr_rate > 0:
                throughput_samples.append(short_ocr_rate)
            snapshots[job_key] = {
                "任务信息": info,
                "心跳": heartbeat,
                "状态": status,
                "任务指标": metrics,
                "资源占用率": usage,
                "页速": page_rate,
                "采样时间": sampled_at,
            }
        with self._lock:
            active_keys = {str(record.get("任务目录") or "") for record in active_records}
            self._worker_supervision = {
                key: value
                for key, value in self._worker_supervision.items()
                if key in active_keys
            }
            self._worker_supervision.update(snapshots)
        return worker_pids, worker_thread_budgets, throughput_samples, speed_samples_changed

    def _reconcile_book_ledger(self) -> bool:
        """Make completed/failed book counts agree with durable page status."""
        if not self._root or not self._output_dir or not self._books:
            return False
        from .library_pipeline import _job_dir_for_pdf, _read_status

        books_by_name = {
            Path(str(book.get("文件名") or book.get("PDF路径") or "")).stem: book
            for book in self._books
        }

        def is_complete(book_name: str) -> bool:
            book = books_by_name.get(book_name)
            if book is None:
                return False
            job_dir = _job_dir_for_pdf(
                Path(str(book.get("PDF路径") or "")), Path(self._root), Path(self._output_dir)
            )
            return self._is_book_complete(book, _read_status(job_dir))

        previous_completed = list(self._completed)
        previous_failed = list(self._failed)
        completed = [name for name in self._completed if is_complete(str(name))]
        failed: list[dict[str, Any]] = []
        for item in self._failed:
            if not isinstance(item, dict):
                continue
            book_name = str(item.get("书名") or "")
            if is_complete(book_name):
                if book_name not in completed:
                    completed.append(book_name)
            else:
                failed.append(item)
        self._completed = list(dict.fromkeys(completed))
        self._failed = failed
        return self._completed != previous_completed or self._failed != previous_failed

    def _requeue_missing_engine_failures(self) -> bool:
        """Restore cached books that only failed because OCR was unavailable."""
        if not self._root or not self._output_dir or not self._books:
            return False
        from .library_pipeline import _job_dir_for_pdf, _read_status

        remaining_failures: list[dict[str, Any]] = []
        recovered = False
        active_names = {str(record.get("书名") or "") for record in self._active_jobs.values()}
        for failure in self._failed:
            if not isinstance(failure, dict):
                continue
            book_name = str(failure.get("书名") or "")
            book_index = next(
                (
                    index
                    for index, book in enumerate(self._books)
                    if Path(str(book.get("文件名") or book.get("PDF路径") or "")).stem == book_name
                ),
                None,
            )
            if book_index is None or book_name in active_names:
                remaining_failures.append(failure)
                continue
            book = self._books[book_index]
            pdf_path = Path(str(book.get("PDF路径") or ""))
            job_dir = _job_dir_for_pdf(pdf_path, Path(self._root), Path(self._output_dir))
            status = _read_status(job_dir)
            cache_dir = job_dir / "缓存" / "页面OCR"
            engine_missing = (status or {}).get("引擎") == "无可用OCR引擎"
            if not engine_missing:
                remaining_failures.append(failure)
                continue
            if not any(cache_dir.glob("*.json")):
                # No page cache exists yet, so this is a clean pending launch.
                # Do not revive it as an active worker and bypass the current
                # resource plan; the normal admission loop will pick it up.
                recovered = True
                continue
            expected_pages = self._expected_book_pages(book, status)
            thread_budget = worker_threads_for_pages(
                expected_pages,
                capacity_threads=2,
                available_thread_slots=2,
            )
            self._active_jobs[str(job_dir)] = {
                "索引": book_index,
                "书名": book_name,
                "任务目录": str(job_dir),
                "来源PDF": str(pdf_path),
                "线程预算": thread_budget,
                "工作量页数": expected_pages,
                "线程预算依据": "OCR 运行时缺失后从页级缓存安全恢复；默认最多 2 线程。",
            }
            active_names.add(book_name)
            recovered = True
        if recovered:
            self._failed = remaining_failures
            self._current_job_dir = next(iter(self._active_jobs), self._current_job_dir)
        return recovered

    def _resume_thread_budget(self, record: dict[str, Any]) -> int:
        """Apply the current safe thread policy when a stopped worker restarts.

        A persisted record may originate from an older page-count-only policy.
        Do not resurrect its 3/4-thread allocation unless a matching, activated
        throughput profile explicitly recommends a high-thread worker.
        """
        try:
            recorded = max(1, int(record.get("线程预算") or 1))
        except (TypeError, ValueError):
            recorded = 1
        active_tuning = self._throughput_profiles.active_recommendation(mode=self._mode) or {}
        try:
            tuned = max(1, int(active_tuning.get("threads_per_worker") or 0))
        except (TypeError, ValueError):
            tuned = 0
        if tuned >= 3:
            return min(4, tuned)
        if recorded > 2:
            record["线程预算"] = 2
            record["线程预算依据"] = "恢复时按默认 2 线程策略降配；未激活已验证的高线程吞吐档案。"
            return 2
        return recorded

    def _resume_cancelled_active_jobs(self, run_token: int | None = None) -> bool:
        """Restart interrupted books before admitting a new book worker."""
        if not self._running or not self._resume:
            return False
        from .library_pipeline import _read_status

        resumed = False
        for job_key, record in list(self._active_jobs.items()):
            if run_token is not None:
                with self._lock:
                    if not self._is_current_controller_run_locked(run_token):
                        return False
            status = _read_status(Path(job_key))
            state = (status or {}).get("状态")
            engine_missing = (status or {}).get("引擎") == "无可用OCR引擎"
            if state != "已取消" and not (state == "未完成" and engine_missing):
                continue
            info = _task_manager.get_task_info(job_key) or {}
            if info.get("存活"):
                continue
            try:
                if run_token is not None:
                    with self._lock:
                        if not self._is_current_controller_run_locked(run_token):
                            return False
                resume_threads = self._resume_thread_budget(record)
                resumed_job_dir, _already_running = _task_manager.start_extraction(
                    path=str(record.get("来源PDF") or ""),
                    output_dir=str(Path(job_key).parent),
                    mode=self._mode,
                    max_pages=self._max_pages_per_book,
                    resume=True,
                    password=None,
                    ocr_threads=resume_threads,
                )
            except Exception:
                continue
            if resumed_job_dir != job_key:
                resumed_record = dict(record)
                resumed_record["任务目录"] = resumed_job_dir
                with self._lock:
                    self._active_jobs.pop(job_key, None)
                    self._active_jobs[resumed_job_dir] = resumed_record
                    sample = self._worker_progress_samples.pop(job_key, None)
                    if isinstance(sample, dict):
                        self._worker_progress_samples[resumed_job_dir] = sample
                    supervision = self._worker_supervision.pop(job_key, None)
                    if isinstance(supervision, dict):
                        self._worker_supervision[resumed_job_dir] = supervision
                    self._current_job_dir = resumed_job_dir
            resumed = True
        return resumed

    def _batch_loop(self, run_token: int | None = None) -> None:
        """后台调度书籍任务，并按资源余量动态分配独立 OCR workers。"""
        from .library_pipeline import _job_dir_for_pdf, _read_status

        root_path = Path(self._root) if self._root else None
        output_path = Path(self._output_dir) if self._output_dir else None
        # On a restored batch, ``_current_index`` is the most recently
        # registered book, while active records may include it and earlier
        # books.  Rescheduling that index consumes a thread slot for an
        # already-live worker and can incorrectly downgrade the genuinely new
        # worker to one thread.  With active records, continue strictly after
        # their largest index; without them retain the existing resume point.
        next_index = self._next_book_index()

        while True:
            with self._lock:
                if run_token is not None and not self._is_current_controller_run_locked(run_token):
                    return
                self._consume_stop_request_locked()
            # Restored jobs are resumed first.  The next pass samples their
            # new PIDs before deciding whether a further book can be admitted.
            if self._resume_cancelled_active_jobs(run_token):
                self._save_state()
                time.sleep(5)
                continue

            with self._lock:
                active_snapshot = list(self._active_jobs.values())
            # Resource and page-rate sampling happens once in this supervision
            # cycle.  Status readers consume its cache and never sleep while
            # measuring worker CPU.
            (
                worker_pids,
                worker_thread_budgets,
                throughput_samples,
                speed_samples_changed,
            ) = self._refresh_worker_supervision(active_snapshot)
            aggregate_throughput = round(sum(throughput_samples), 3) if throughput_samples else None
            active_tuning = self._throughput_profiles.active_recommendation(mode=self._mode)
            plan = self._resource_scheduler.plan(
                worker_pids,
                worker_thread_budgets=worker_thread_budgets,
                preferred_workers=(active_tuning or {}).get("workers"),
                preferred_threads_per_worker=(active_tuning or {}).get("threads_per_worker"),
                throughput_pages_per_minute=aggregate_throughput,
                tuning_profile_id=(active_tuning or {}).get("配置ID"),
            )
            with self._lock:
                self._last_worker_plan = plan
                self._last_worker_plan_snapshot = plan.to_dict()
            if speed_samples_changed:
                self._save_state()

            # 只在批量任务未收到停止请求时补充新的书籍 worker。
            available_new_worker_slots = plan.available_thread_slots
            while (
                self._running
                and (run_token is None or self._is_current_controller_run_locked(run_token))
                and len(active_snapshot) < plan.target_workers
                and next_index < len(self._books)
            ):
                if available_new_worker_slots < 1:
                    break
                index = next_index
                next_index += 1
                book = self._books[index]
                pdf_path = Path(book["PDF路径"])
                book_name = book["文件名"].replace(".pdf", "")
                if book_name in self._completed or any(item.get("书名") == book_name for item in self._failed):
                    continue
                try:
                    job_dir = _job_dir_for_pdf(pdf_path, root_path, output_path) if root_path and output_path else None
                    status = _read_status(job_dir) if job_dir else None
                    if self._is_book_complete(book, status):
                        with self._lock:
                            self._record_book_completion(book_name)
                        self._save_state()
                        continue

                    expected_pages = self._expected_book_pages(book, status)
                    worker_threads = worker_threads_for_pages(
                        expected_pages,
                        capacity_threads=plan.threads_per_worker,
                        available_thread_slots=available_new_worker_slots,
                        allow_high_thread_budget=bool(
                            active_tuning
                            and int((active_tuning or {}).get("threads_per_worker") or 0) >= 3
                        ),
                    )
                    actual_output_dir = str(job_dir.parent) if job_dir else None
                    returned_job_dir, _already_running = _task_manager.start_extraction(
                        path=str(pdf_path),
                        output_dir=actual_output_dir,
                        mode=self._mode,
                        max_pages=self._max_pages_per_book,
                        resume=self._resume,
                        password=None,
                        ocr_threads=worker_threads,
                    )
                    record = {
                        "索引": index,
                        "书名": book_name,
                        "任务目录": returned_job_dir,
                        "来源PDF": str(pdf_path),
                        "线程预算": worker_threads,
                        "工作量页数": expected_pages,
                        "线程预算依据": (
                            f"{expected_pages or '未知'} 页工作量默认最多 2 线程；"
                            f"仅在已激活吞吐基准明确证明高线程有收益时提升，"
                            f"再受当前可用线程槽限制为 {worker_threads}。"
                        ),
                    }
                    available_new_worker_slots = max(0, available_new_worker_slots - worker_threads)
                    with self._lock:
                        self._active_jobs[returned_job_dir] = record
                        self._current_index = index
                        self._current_job_dir = returned_job_dir
                        active_snapshot = list(self._active_jobs.values())
                    self._save_state()
                except Exception as exc:
                    with self._lock:
                        self._record_book_failure(book_name, f"{type(exc).__name__}: {exc}")
                    self._save_state()

            # 回收已结束的任务；页数不足的“完成”状态不会被计入完成书本。
            reaped_finished_worker = False
            for job_key, record in list(self._active_jobs.items()):
                info = _task_manager.get_task_info(job_key) or {}
                status = _read_status(Path(job_key))
                if info.get("存活"):
                    continue
                book_name = str(record.get("书名") or "未知书籍")
                book_index = int(record.get("索引") or -1)
                book = self._books[book_index] if 0 <= book_index < len(self._books) else {}
                if self._running and self._resume and (status or {}).get("状态") == "已取消":
                    # A batch launch is explicit resume intent.  A prior MCP
                    # process may have been stopped after asking the worker to
                    # cancel, leaving a terminal status plus an orphaned
                    # durable attempt.  Retry the same book in place instead
                    # of misclassifying it as failed and moving to later books.
                    try:
                        with self._lock:
                            if not self._is_current_controller_run_locked(run_token):
                                return
                        resume_threads = self._resume_thread_budget(record)
                        resumed_job_dir, _already_running = _task_manager.start_extraction(
                            path=str(record.get("来源PDF") or book.get("PDF路径")),
                            output_dir=str(Path(job_key).parent),
                            mode=self._mode,
                            max_pages=self._max_pages_per_book,
                            resume=True,
                            password=None,
                            ocr_threads=resume_threads,
                        )
                        if resumed_job_dir != job_key:
                            resumed_record = dict(record)
                            resumed_record["任务目录"] = resumed_job_dir
                            with self._lock:
                                self._active_jobs.pop(job_key, None)
                                self._active_jobs[resumed_job_dir] = resumed_record
                                sample = self._worker_progress_samples.pop(job_key, None)
                                if isinstance(sample, dict):
                                    self._worker_progress_samples[resumed_job_dir] = sample
                                self._current_job_dir = resumed_job_dir
                        self._save_state()
                        continue
                    except Exception:
                        # Fall through to the normal failed-book record, which
                        # retains a visible reason for a genuine resume error.
                        pass
                with self._lock:
                    self._active_jobs.pop(job_key, None)
                    self._worker_progress_samples.pop(job_key, None)
                    self._worker_supervision.pop(job_key, None)
                    if self._is_book_complete(book, status):
                        self._record_book_completion(book_name)
                    else:
                        state = str((status or {}).get("状态") or "无状态文件")
                        processed = (status or {}).get("已处理页数", 0)
                        expected = self._expected_book_pages(book, status)
                        self._record_book_failure(book_name, f"{state}（{processed}/{expected or '未知'}页）")
                self._save_state()
                reaped_finished_worker = True

            with self._lock:
                active_count = len(self._active_jobs)
                all_scheduled = next_index >= len(self._books)
                should_stop = not self._running
                self._current_job_dir = next(iter(self._active_jobs), None)
            if active_count == 0 and (all_scheduled or should_stop):
                break
            # A finished worker has just released capacity.  Re-plan and admit
            # the next book in the same scheduling turn rather than idling for
            # an additional fixed polling interval.
            if reaped_finished_worker and not should_stop:
                continue
            time.sleep(5)

        with self._lock:
            if run_token is not None and not self._is_current_controller_run_locked(run_token):
                return
            self._running = False
            self._active_jobs = {}
            self._current_job_dir = None
            self._phase = "已停止" if should_stop else "已完成"
            self._save_state()
            self._release_controller_lease_locked()

    def status(self) -> dict[str, Any]:
        """Return the last supervision snapshot without affecting OCR scheduling."""

        with self._lock:
            self._refresh_observer_state_locked()
            current_book = None
            current_progress = None
            current_metrics: dict[str, Any] = {}
            current_status: dict[str, Any] | None = None
            current_job_dir = None
            active_records = list(self._active_jobs.values())
            supervision_snapshot = {
                key: dict(value)
                for key, value in self._worker_supervision.items()
                if isinstance(value, dict)
            }
            if active_records:
                current_book = str(active_records[0].get("书名"))
                current_job_dir = str(active_records[0].get("任务目录"))
            elif 0 <= self._current_index < len(self._books):
                current_book = Path(self._books[self._current_index]["文件名"]).stem
                current_job_dir = self._current_job_dir
            if current_job_dir:
                cached = supervision_snapshot.get(str(current_job_dir), {})
                cached_status = cached.get("状态") if isinstance(cached, dict) else None
                current_status = dict(cached_status) if isinstance(cached_status, dict) else None

            elapsed = None
            elapsed_str = None
            eta_str = None
            eta_seconds = None
            if self._started_at:
                elapsed = int(time.time() - self._started_at)
                elapsed_str = _format_duration(elapsed)

            total_books = len(self._books)
            completed_books = len(self._completed)
            failed_books = len(self._failed)
            active_book_count = len(active_records)
            done = completed_books + failed_books
            pending_books = max(0, total_books - done - active_book_count)
            if self._running and done > 0 and elapsed and elapsed > 10:
                # 估算剩余时间
                avg_per_book = elapsed / done
                remaining_books = total_books - done
                eta_seconds = int(avg_per_book * remaining_books)
                eta_str = f"约{_format_duration(eta_seconds)}"

            worker_resources: list[dict[str, object]] = []
            worker_task_rows_by_index: dict[int, dict[str, object]] = {}
            throughput_samples: list[float] = []
            for worker_index, record in enumerate(active_records, start=1):
                job_key = str(record.get("任务目录"))
                observed = supervision_snapshot.get(job_key, {})
                info_value = observed.get("任务信息") if isinstance(observed, dict) else None
                info = dict(info_value) if isinstance(info_value, dict) else {}
                heartbeat = info.get("心跳") if isinstance(info.get("心跳"), dict) else {}
                heartbeat_pid = _nonnegative_integer(heartbeat.get("进程ID"))
                candidate_pid = _nonnegative_integer(info.get("工作进程ID"))
                heartbeat_active = bool(heartbeat.get("活跃"))
                # The launcher is never an OCR worker PID.  Only a fresh
                # worker heartbeat can confirm the actual child PID.
                pid = heartbeat_pid if heartbeat_active else None
                status_value = observed.get("状态") if isinstance(observed, dict) else None
                status = dict(status_value) if isinstance(status_value, dict) else {}
                metrics_value = observed.get("任务指标") if isinstance(observed, dict) else None
                metrics = dict(metrics_value) if isinstance(metrics_value, dict) else _processing_metrics(status)
                usage_value = observed.get("资源占用率") if isinstance(observed, dict) else None
                usage = (
                    dict(usage_value)
                    if isinstance(usage_value, dict)
                    else {
                        "状态": "等待控制器监管采样",
                        "说明": "状态接口不会自行探测进程；等待监管层下一次快照。",
                    }
                )
                metrics["资源占用率"] = usage
                page_rate_value = observed.get("页速") if isinstance(observed, dict) else None
                page_rate = (
                    dict(page_rate_value)
                    if isinstance(page_rate_value, dict)
                    else {
                        "状态": "等待监管采样",
                        "文本": "等待监管层采样",
                        "页每分钟": None,
                        "秒每页": None,
                        "样本页数": 0,
                        "样本时长秒": None,
                        "速度口径": "监管层尚未产生页级样本",
                    }
                )
                short_ocr_rate = _finite_number(metrics.get("短窗OCR页每分钟"))
                if short_ocr_rate is not None and short_ocr_rate > 0:
                    throughput_samples.append(short_ocr_rate)
                try:
                    thread_budget = max(1, int(record.get("线程预算") or metrics.get("OCR线程预算") or 1))
                except (TypeError, ValueError):
                    thread_budget = None
                book_name = str(record.get("书名") or metrics.get("书籍名") or "未知书籍")
                processed_pages = _nonnegative_integer(metrics.get("已处理页数")) or 0
                total_pages = _nonnegative_integer(metrics.get("总处理页数")) or 0
                progress_text = str(metrics.get("处理进度文本") or "未知")
                progress = {
                    "已处理页数": processed_pages,
                    "总处理页数": total_pages,
                    "百分比": metrics.get("处理进度"),
                    "文本": f"{processed_pages}/{total_pages}（{progress_text}）" if total_pages else progress_text,
                }
                measured_rate = _finite_number(page_rate.get("页每分钟"))
                eta_rate = measured_rate if measured_rate and measured_rate > 0 else short_ocr_rate
                remaining_pages = max(0, total_pages - processed_pages)
                worker_eta_seconds = (
                    int(remaining_pages * 60.0 / eta_rate)
                    if eta_rate is not None and eta_rate > 0 and total_pages > 0
                    else None
                )
                worker_eta_text = (
                    f"约{_format_duration(worker_eta_seconds)}"
                    if worker_eta_seconds is not None
                    else metrics.get("剩余时间")
                )
                pid_confirmation = (
                    "心跳已确认" if pid is not None else
                    "尚未由活动心跳确认" if candidate_pid is not None else "等待worker心跳"
                )
                worker_resource = {
                    "worker序号": worker_index,
                    "书名": book_name,
                    "书籍": book_name,
                    "任务目录": job_key,
                    "监管采样时间": observed.get("采样时间") if isinstance(observed, dict) else None,
                    "已用时间": metrics.get("运行时间"),
                    "已用时间秒": metrics.get("运行时间秒"),
                    "剩余时间": worker_eta_text,
                    "剩余时间秒": worker_eta_seconds,
                    "剩余时间口径": (
                        "监督层实际页速" if measured_rate and measured_rate > 0
                        else "OCR引擎短窗页速" if eta_rate is not None and eta_rate > 0
                        else "OCR worker 未提供有效页速"
                    ),
                    "进度": progress,
                    "处理速度": page_rate["文本"],
                    "近期实际处理速度": page_rate["文本"],
                    "近期实际处理页每分钟": page_rate["页每分钟"],
                    "近期实际处理秒每页": page_rate["秒每页"],
                    "近期实际速度状态": page_rate["状态"],
                    "近期实际速度样本页数": page_rate["样本页数"],
                    "近期实际速度样本时长秒": page_rate["样本时长秒"],
                    "近期实际速度口径": page_rate["速度口径"],
                    "本次运行累计平均速度": metrics.get("处理速度文本"),
                    "本次运行累计平均秒每页": metrics.get("处理速度"),
                    "OCR引擎短窗速度": (
                        f"{short_ocr_rate:.3f}页/分钟"
                        if short_ocr_rate is not None and short_ocr_rate > 0
                        else "暂无样本"
                    ),
                    "OCR吞吐页每分钟": short_ocr_rate,
                    "进程ID": pid,
                    "实际Worker PID": pid,
                    "Worker PID确认状态": pid_confirmation,
                    "线程预算": thread_budget,
                    "OCR线程预算": thread_budget,
                    "工作量页数": record.get("工作量页数"),
                    "线程预算依据": record.get("线程预算依据"),
                    # Direct fields keep clients from having to infer the
                    # distinction between configured OCR concurrency,
                    # observed CPU-active threads, and all OS threads.
                    "进程线程数": usage.get("进程线程数"),
                    "活跃CPU线程数": usage.get("活跃CPU线程数"),
                    "饱和CPU线程数": usage.get("饱和CPU线程数"),
                    "线程CPU占用率": usage.get("线程CPU占用率"),
                    "CPU占整机比例": usage.get("CPU占用率"),
                    "CPU整机占比": usage.get("CPU占用率"),
                    "CPU等效核心数": usage.get("CPU等效核心数"),
                    "CPU等效核心": usage.get("CPU等效核心数"),
                    "内存MB": usage.get("内存MB"),
                    "RSS内存MB": usage.get("运行内存占用MB"),
                    "内存占整机比例": usage.get("内存占用率"),
                    "运行内存占用MB": usage.get("运行内存占用MB"),
                    "运行内存占整机比例": usage.get("运行内存占整机比例"),
                    "资源占用率": usage,
                }
                worker_resources.append(worker_resource)
                task_marker = "-" if bool(info.get("存活")) and heartbeat_active else "!"
                task_row = {
                    "标记": task_marker,
                    "显示": f"{task_marker} {book_name}",
                    "状态": "正在执行" if task_marker == "-" else "监测确认中或需恢复",
                    **worker_resource,
                }
                record_index = _nonnegative_integer(record.get("索引"))
                if record_index is not None:
                    worker_task_rows_by_index[record_index] = task_row
                if job_key == current_job_dir:
                    current_metrics = metrics
                    current_book = book_name

            # A terminal/restored batch can have a remembered current book
            # without an active record.  Preserve the established single-book
            # fields in that state without issuing a second worker probe.
            if not current_metrics and current_status:
                observed = supervision_snapshot.get(str(current_job_dir or ""), {})
                cached_metrics = observed.get("任务指标") if isinstance(observed, dict) else None
                current_metrics = (
                    dict(cached_metrics)
                    if isinstance(cached_metrics, dict)
                    else _processing_metrics(current_status)
                )
            if current_metrics:
                processed = _nonnegative_integer(current_metrics.get("已处理页数")) or 0
                total = _nonnegative_integer(current_metrics.get("总处理页数")) or 0
                current_progress = {
                    "已处理": processed,
                    "总数": total,
                    "百分比": current_metrics.get("处理进度文本", "0.0%"),
                    "文本": (
                        f"{processed}/{total}（{current_metrics.get('处理进度文本', '0.0%')}）"
                        if total
                        else current_metrics.get("处理进度文本", "0.0%")
                    ),
                    "状态": (current_status or {}).get("状态", "未知"),
                    "平均秒每页": current_metrics.get("处理速度"),
                    "本书预计剩余秒": current_metrics.get("剩余时间秒"),
                    "资源占用率": current_metrics.get("资源占用率"),
                }

            completed_names = {str(name) for name in self._completed}
            failed_by_name = {
                str(item.get("书名")): item
                for item in self._failed
                if isinstance(item, dict) and item.get("书名")
            }
            task_list: list[dict[str, object]] = []
            included_active_indices: set[int] = set()
            inactive_fields: dict[str, object] = {
                "已用时间": None,
                "剩余时间": None,
                "进度": None,
                "实际Worker PID": None,
                "OCR线程预算": None,
                "进程线程数": None,
                "CPU整机占比": None,
                "CPU等效核心": None,
                "RSS内存MB": None,
            }
            for book_index, book in enumerate(self._books):
                book_name = Path(str(book.get("文件名") or book.get("PDF路径") or "未知书籍")).stem
                active_row = worker_task_rows_by_index.get(book_index)
                if active_row is not None:
                    task_list.append(active_row)
                    included_active_indices.add(book_index)
                elif book_name in completed_names:
                    task_list.append(
                        {
                            "标记": "v",
                            "显示": f"v {book_name}",
                            "状态": "已完成",
                            "书籍": book_name,
                            "处理速度": "无（已完成，worker已退出）",
                            "说明": "已完成书籍不保留已退出worker的PID、CPU或RSS，避免把历史资源误报为实时值。",
                            **inactive_fields,
                        }
                    )
                elif book_name in failed_by_name:
                    failed = failed_by_name[book_name]
                    task_list.append(
                        {
                            "标记": "x",
                            "显示": f"x {book_name}",
                            "状态": "失败",
                            "书籍": book_name,
                            "处理速度": "无（失败）",
                            "失败原因": failed.get("原因"),
                            **inactive_fields,
                        }
                    )
                else:
                    task_list.append(
                        {
                            "标记": "○",
                            "显示": f"○ {book_name}",
                            "状态": "待处理",
                            "书籍": book_name,
                            "处理速度": "未开始",
                            **inactive_fields,
                        }
                    )
            for record_index, active_row in worker_task_rows_by_index.items():
                if record_index not in included_active_indices:
                    task_list.append(active_row)

            active_tuning = self._throughput_profiles.active_recommendation(mode=self._mode)
            aggregate_throughput = round(sum(throughput_samples), 3) if throughput_samples else None
            worker_plan_snapshot = (
                self._last_worker_plan.to_dict()
                if self._last_worker_plan is not None
                else dict(self._last_worker_plan_snapshot)
            )
            if not worker_plan_snapshot:
                worker_plan_snapshot = {
                    "状态": "等待监管调度采样",
                    "reason": "状态接口不会自行重算资源调度。",
                    "cpu_count": None,
                    "total_memory_gb": None,
                }
            logical_cpu_count = _nonnegative_integer(worker_plan_snapshot.get("cpu_count"))
            if logical_cpu_count is None:
                inferred_cpu_counts: list[int] = []
                for worker in worker_resources:
                    usage = worker.get("资源占用率")
                    snapshot = usage if isinstance(usage, dict) else {}
                    cores = _finite_number(snapshot.get("CPU等效核心数"))
                    percent = _finite_number(snapshot.get("CPU占用率"))
                    if cores is not None and percent is not None and percent > 0:
                        inferred_cpu_counts.append(max(1, round(cores * 100.0 / percent)))
                logical_cpu_count = inferred_cpu_counts[0] if inferred_cpu_counts else 1
            total_memory_gb = _finite_number(worker_plan_snapshot.get("total_memory_gb"))
            worker_resource_summary = _worker_resource_summary(
                worker_resources,
                logical_cpu_count=logical_cpu_count,
                total_memory_mb=total_memory_gb * 1024.0 if total_memory_gb is not None else None,
            )
            resource_usage = current_metrics.get("资源占用率") or {
                "状态": "等待控制器监管采样",
                "说明": "状态接口不会自行探测进程；等待监管层下一次快照。",
            }
            remaining_book_count = pending_books + active_book_count
            current_worker = next(
                (
                    worker
                    for worker in worker_resources
                    if str(worker.get("任务目录")) == str(current_job_dir)
                ),
                None,
            )
            fixed_monitoring = {
                "书籍": current_metrics.get("书籍名") or current_book,
                # Batch elapsed/ETA are intentionally separate from the
                # per-worker values in ``当前worker任务列表``.
                "已用时间": elapsed_str,
                "剩余时间": eta_str or "未知",
                "处理速度": (
                    current_worker.get("近期实际处理速度")
                    if current_worker is not None
                    else current_metrics.get("处理速度文本") or "未知"
                ),
                "当前worker任务列表": task_list,
                "剩余书本数量": remaining_book_count,
                "实际Worker PID": [worker.get("实际Worker PID") for worker in worker_resources],
                "进度": current_progress,
                "OCR线程预算": worker_resource_summary.get("总OCR线程预算"),
                "进程线程数": worker_resource_summary.get("总进程线程数"),
                "CPU整机占比": worker_resource_summary.get("总CPU占整机比例"),
                "CPU等效核心": worker_resource_summary.get("总CPU等效核心数"),
                "RSS内存": worker_resource_summary.get("总RSS内存MB"),
                "说明": (
                    "当前worker任务列表中：v=已完成，-=正在执行，!=监测确认中/需恢复，"
                    "x=失败，○=待处理。每个活跃worker的处理速度以监督层连续页级完成增量计算；"
                    "累计平均和OCR引擎短窗速度仅作辅助诊断。"
                ),
            }
            if self._observer_only:
                agent_action = {
                    "operation": "observe",
                    "requires_agent_action": False,
                    "automatic_handling": "由持有监管租约的本机控制器继续调度、恢复和写入账本。",
                    "next_poll_after_seconds": 30,
                }
            elif self._phase == "准备中":
                agent_action = {
                    "operation": "observe",
                    "requires_agent_action": False,
                    "automatic_handling": "监管层正在后台发现书库；发现完成后会自动进入调度。",
                    "next_poll_after_seconds": 15,
                }
            elif self._phase == "启动失败":
                agent_action = {
                    "operation": "review_initialization_error",
                    "requires_agent_action": True,
                    "automatic_handling": "未启动任何 OCR worker，避免创建半完成批次。",
                    "next_poll_after_seconds": None,
                }
            elif self._running:
                agent_action = {
                    "operation": "observe",
                    "requires_agent_action": False,
                    "automatic_handling": "监管层持续按资源和页级进度自动补位、恢复和调度。",
                    "next_poll_after_seconds": 30,
                }
            else:
                agent_action = {
                    "operation": "none",
                    "requires_agent_action": False,
                    "automatic_handling": "批次已到达稳定终态。",
                    "next_poll_after_seconds": None,
                }

            return {
                "运行中": self._running,
                "批处理阶段": self._phase,
                "初始化错误": self._initialization_error,
                "控制器角色": "观察者" if self._observer_only else "控制器" if self._controller_lease else "本地状态",
                "控制器所有者": (
                    self._observed_controller.get("所有者")
                    if self._observer_only
                    else None if self._controller_lease is None
                    else self._get_controller_supervisor().owner_id
                ),
                "被观察控制器": dict(self._observed_controller) if self._observer_only else None,
                "控制器租约状态": (
                    "持有"
                    if self._controller_lease is not None and not self._observer_only
                    else "观察中，后台重试"
                    if self._observer_only and self._controller_lease_error
                    else "由其他控制器持有"
                    if self._observer_only
                    else "未初始化"
                ),
                "控制器租约错误": self._controller_lease_error,
                "代理动作": agent_action,
                "总书数": total_books,
                "书本总数": total_books,
                "已完成": completed_books,
                "书本完成数": completed_books,
                "失败": failed_books,
                "书本失败数": failed_books,
                "待处理": pending_books,
                "书本待处理数": pending_books,
                "进行中书本数": active_book_count,
                # Remaining means books that can still make progress in this
                # batch: active + queued.  Failed books stay explicit rather
                # than being silently counted as either complete or pending.
                "剩余书本数量": remaining_book_count,
                # ``worker数`` is the number of active batch records.  The
                # adjacent PID/sample counts expose startup and lost-heartbeat
                # gaps instead of silently treating them as idle processes.
                "worker数": active_book_count,
                "已取得PID的worker数": worker_resource_summary["已取得PID的worker数"],
                "可采样worker数": worker_resource_summary["可采样worker数"],
                "不可采样worker数": worker_resource_summary["不可采样worker数"],
                "worker总占用内存MB": worker_resource_summary["总RSS内存MB"],
                "worker总内存占整机比例": worker_resource_summary["总内存占整机比例"],
                "worker总运行内存占用MB": worker_resource_summary["总运行内存占用MB"],
                "worker总运行内存占整机比例": worker_resource_summary["总运行内存占整机比例"],
                "worker总进程线程数": worker_resource_summary["总进程线程数"],
                "worker总CPU等效核心数": worker_resource_summary["总CPU等效核心数"],
                "worker总CPU占整机比例": worker_resource_summary["总CPU占整机比例"],
                "实际Worker PID": fixed_monitoring["实际Worker PID"],
                "OCR线程预算": worker_resource_summary["总OCR线程预算"],
                "进程线程数": worker_resource_summary["总进程线程数"],
                "CPU整机占比": worker_resource_summary["总CPU占整机比例"],
                "CPU等效核心": worker_resource_summary["总CPU等效核心数"],
                "RSS内存": worker_resource_summary["总RSS内存MB"],
                "整体进度": (
                    f"{done}/{total_books} ({done / total_books * 100:.1f}%)"
                    if total_books > 0
                    else "书库发现中" if self._phase == "准备中" else "0/0"
                ),
                "当前书籍": current_book,
                "当前书籍任务目录": current_job_dir,
                "当前书籍进度": current_progress,
                # 统一页级指标默认指向当前书籍；批量书数进度仍由“整体进度”给出。
                "书籍名": current_metrics.get("书籍名") or current_book,
                "总处理页数": current_metrics.get("总处理页数"),
                "已处理页数": current_metrics.get("已处理页数"),
                "处理进度": current_metrics.get("处理进度"),
                "处理进度文本": current_metrics.get("处理进度文本"),
                "处理速度": current_metrics.get("处理速度"),
                "处理速度文本": current_metrics.get("处理速度文本"),
                "运行时间": elapsed_str,
                "运行时间秒": elapsed,
                "剩余时间": eta_str or "未知",
                "剩余时间秒": eta_seconds,
                "资源占用率": resource_usage,
                "当前书籍指标": current_metrics or None,
                "活动书籍": [
                    {
                        "书名": record.get("书名"),
                        "任务目录": record.get("任务目录"),
                        "来源PDF": record.get("来源PDF"),
                        "线程预算": record.get("线程预算"),
                        "工作量页数": record.get("工作量页数"),
                        "线程预算依据": record.get("线程预算依据"),
                    }
                    for record in active_records
                ],
                "当前worker任务列表": task_list,
                "固定监测格式": fixed_monitoring,
                "worker资源": worker_resources,
                "worker资源汇总": worker_resource_summary,
                "worker调度": worker_plan_snapshot,
                "OCR吞吐页每分钟": aggregate_throughput,
                "吞吐调优策略": active_tuning or {
                    "状态": "未激活",
                    "说明": "可先运行 OCR 容量基准测试，验证2/4/6/8线程和多worker组合后再显式激活。",
                },
                "每本最大页数": self._max_pages_per_book,
                "最大并发worker": self._requested_workers,
                "断点续传": self._resume,
                "已运行时间": elapsed_str,
                "预计剩余时间": eta_str,
                "失败列表": self._failed[-5:] if self._failed else [],
                "查询提示": "用 get_batch_status 查看固定监测格式和每个worker页速；用 get_job_status 传入书籍任务目录查看单书详细进度。",
            }

    def stop(self) -> dict[str, Any]:
        """停止批量任务（当前书籍会继续完成）。"""
        with self._lock:
            if self._observer_only:
                try:
                    _TaskManager._atomic_json(
                        self._stop_request_path(),
                        {
                            "请求时间": datetime.now().isoformat(timespec="seconds"),
                            "请求者": self._get_controller_supervisor().owner_id,
                        },
                    )
                except OSError:
                    return {
                        "状态": "无法转交停止请求",
                        "说明": "当前适配器是观察者，且无法写入本机监管命令文件。",
                    }
                return {
                    "状态": "已转交停止请求",
                    "说明": "持有批量监管租约的控制器会在下一次监管周期停止新增书籍；当前 worker 保留页级断点。",
                }
            self._running = False
            self._phase = "停止中"
            self._save_state()
        return {"状态": "已发送停止信号，当前书籍将继续完成"}


_batch_manager = _BatchManager(
    use_portable_runtime=True,
    on_observer_promoted=lambda: _task_manager.restore_pending(allow_takeover=True),
)


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

    raw_text = (request or "").lower()
    text = raw_text.replace(" ", "")
    if page_number is not None or re.search(r"第?\s*\d+\s*页", text) or re.search(
        r"\b(?:page|p)\s*\d+\b", raw_text
    ) or any(
        word in text for word in ("页面证据", "页截图", "页图像", "页图片")
    ) or any(
        phrase in raw_text for phrase in ("page evidence", "show page", "screenshot", "render page")
    ):
        return "evidence"
    if any(word in text for word in ("恢复", "继续处理", "断点", "中断")) or any(
        phrase in raw_text for phrase in ("resume", "continue", "restart", "recover")
    ):
        return "resume"
    if any(word in text for word in ("质检", "质量", "巡检", "低置信")) or any(
        phrase in raw_text for phrase in ("audit", "quality", "review")
    ):
        return "audit"
    if job_dir or any(word in text for word in ("进度", "状态", "处理到哪", "完成了吗")) or any(
        phrase in raw_text for phrase in ("progress", "status", "how far", "completed")
    ):
        return "status"
    extract_requested = any(
        word in text
        for word in ("提取", "转成", "转换", "救援", "开始处理", "开始ocr", "开始识别", "识别全文", "导出文本")
    ) or any(
        phrase in raw_text
        for phrase in ("extract", "convert", "rescue", "start processing", "start ocr", "full text", "export text")
    )
    diagnose_requested = any(
        word in text
        for word in ("诊断", "检查", "分析", "文本层", "能不能识别", "是否扫描", "是否需要ocr", "需要ocr吗")
    ) or any(
        phrase in raw_text
        for phrase in ("diagnose", "inspect", "check", "analyze", "analyse", "text layer", "need ocr", "scanned")
    )
    if path and extract_requested:
        return "extract"
    if diagnose_requested:
        return "diagnose"
    if path:
        return "extract"
    raise ValueError("请提供 PDF 路径，或提供已有任务目录以查询、巡检或恢复任务。")


def _page_number_from_request(request: str | None) -> int | None:
    if not request:
        return None
    match = re.search(r"第?\s*(\d+)\s*页", request)
    if match is None:
        match = re.search(r"\b(?:page|p)\s*(\d+)\b", request, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def _workflow_response(
    status: str,
    executed: list[str],
    result: dict[str, Any],
    next_step: str,
) -> dict[str, Any]:
    """Return one compact, model-readable response for the primary workflow tool."""
    raw_next_call = result.get("后续只读调用") if isinstance(result, dict) else None
    next_call = (
        {
            "tool": raw_next_call.get("tool") or raw_next_call.get("工具"),
            "arguments": raw_next_call.get("arguments") or raw_next_call.get("参数") or {},
            "read_only": bool(raw_next_call.get("read_only", raw_next_call.get("只读", False))),
        }
        if isinstance(raw_next_call, dict)
        else None
    )
    return {
        "contract_version": "1.0",
        "状态": status,
        "已执行": executed,
        "结果": zh_data(result),
        "下一步": next_step,
        # Keep this machine envelope outside zh_data: parameter names such as
        # ``path`` and ``job_dir`` must remain valid JSON-RPC argument names,
        # not translated display labels.
        "next_call": next_call,
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
        Field(description="待处理的 PDF 路径或包含 PDF 的目录。目录会作为批量后台任务处理；查询已有任务时可不填。"),
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

    # The business facade accepts a directory as a batch target as well as a
    # PDF.  Hosts therefore do not need to discover and route themselves to a
    # second launch tool; the same status call remains valid afterwards.
    if path and Path(path).expanduser().is_dir():
        if selected == "status":
            batch_status = _batch_manager.status()
            return _workflow_response(
                "已读取批量任务状态",
                ["查看批量监管快照"],
                {
                    **batch_status,
                    "后续只读调用": {
                        "工具": "rescue_pdf",
                        "参数": {"path": path, "workflow": "status"},
                        "只读": True,
                    },
                },
                "监管层会持续自动调度；仅在结果明确要求用户输入时再追问。",
            )
        if selected == "extract":
            started = _batch_manager.start_batch(
                root=path,
                output_dir=output_dir,
                mode=mode,
                max_books=None,
                max_pages_per_book=max_pages,
                resume=True,
            )
            return _workflow_response(
                "已提交批量后台任务",
                ["提交书库发现", "后台调度 OCR worker"],
                {
                    **started,
                    "后续只读调用": {
                        "工具": "rescue_pdf",
                        "参数": {"path": path, "workflow": "status"},
                        "只读": True,
                    },
                },
                "书库发现和 OCR 都在监管层后台运行；无需等待扫描完成。",
            )
        raise ValueError("目录路径当前支持提取或查询批量状态；页面证据、恢复和质检需要具体任务目录。")

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
        # The primary facade must expose the same business + supervision
        # snapshot as the dedicated status tool.  Returning only the business
        # JSON forced hosts to guess whether they should make a second call for
        # heartbeat, PID, resource, or auto-recovery evidence.
        status = get_job_status(job_dir)
        return _workflow_response(
            "已读取任务状态",
            ["查看任务状态"],
            status,
            "监管层会自动处理可恢复的中断；仅在结果明确要求密码、文件修复或人工确认时再向用户追问。",
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
        # Reuse the public resume path so this primary entry starts an isolated
        # worker and returns immediately instead of synchronously OCR'ing the
        # remaining pages in the MCP process.
        resumed = await resume_job(job_dir, mode=mode, password=password, ctx=ctx)
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

    if route == "ocr_required" and str(plan.get("engine") or "") == "none":
        # Do not create a background job that the worker is already known to
        # be unable to execute.  This is a blocked precondition, not an OCR
        # failure, so every MCP host can surface one stable remediation without
        # asking an agent to inspect a doomed task directory.
        return _workflow_response(
            "OCR运行环境不可用",
            ["诊断PDF", "规划处理任务"],
            {
                **plan,
                "operation": "blocked",
                "error_code": "ocr_runtime_unavailable",
                "terminal": False,
                "requires_user_action": True,
            },
            "当前环境未发现可用 OCR 引擎；安装或修复 OCR 运行环境后，使用同一 rescue_pdf 请求重试。",
        )

    estimated_seconds = int(plan.get("estimated_seconds", 0) or 0)
    route_needs_ocr = route == "ocr_required"
    # Native OCR can block inside a model/runtime call indefinitely.  An explicit
    # ``foreground`` preference is honoured for short direct-text work only; an
    # OCR route is *always* isolated so no MCP client loses its transport session.
    run_in_background = route_needs_ocr or execution == "background" or (
        execution == "auto" and estimated_seconds > 60
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
                "执行隔离": "独立 OCR 工作进程；MCP 仅负责调度和监测",
                "后续只读调用": {
                    "工具": "rescue_pdf",
                    "参数": {"job_dir": job_dir, "workflow": "status"},
                    "只读": True,
                },
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
    description="提取PDF为可校验的正文、分段文本、页面记录和质量审计。OCR 在独立工作进程中运行，工具立即返回，不会阻塞 MCP。用 get_job_status 查看进度和工作进程心跳。",
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
        "说明": "OCR 运行在独立工作进程中；MCP 保持可响应。用 get_job_status 轮询进度和心跳。",
        "规划": plan,
        "任务目录": job_dir,
        "工作进程监测": "已启用（每5秒检查心跳；异常时先安全停止，再确认退出后断点恢复一次）",
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
    job_dir, already = _task_manager.start_extraction(
        str(source),
        output_dir=output_dir,
        mode=mode,
        max_pages=max_pages,
        resume=resume,
        password=None,
    )
    task_info = _task_manager.get_task_info(job_dir) or {}
    return {
        "状态": "已在运行" if already else "已启动",
        "任务目录": job_dir,
        "启动进程ID": task_info.get("启动进程ID"),
        "日志路径": project_relative_path(Path(task_info["日志路径"])) if task_info.get("日志路径") else None,
        "启动方式": "独立 OCR 工作进程",
        "说明": "MCP 不执行 OCR；它只启动和监测独立工作进程。以工作进程心跳而非包装器 PID 判定存活。",
    }


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
    description="读取书籍提取任务的运行时间、剩余时间、书籍名、总处理页数、处理进度、处理速度、CPU/内存资源占用率，以及低置信页、失败页和质量报告位置。监测以独立工作进程的心跳为准，不会把 Windows 包装器 PID 当作 OCR 进程。",
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
def get_job_status(job_dir: str, stalled_after_seconds: int = 600) -> dict[str, Any]:
    payload = run_read_job_status(job_dir, stalled_after_seconds=stalled_after_seconds)
    status = payload.get("状态", {}) or {}
    # 监控优先级：工作进程心跳 > 页面状态文件 > 启动器 PID（仅作诊断）。
    task_info = _task_manager.get_task_info(job_dir) or {}
    metrics = _build_task_metrics(status, task_info)
    target = int(metrics.get("总处理页数") or 0)
    processed = int(metrics.get("已处理页数") or 0)
    pct = float(metrics.get("处理进度") or 0.0)
    runtime = payload.get("状态新鲜度", {}).get("运行判断", "未知")
    eta_text = str(metrics.get("剩余时间") or "未知")
    avg_text = str(metrics.get("处理速度文本") or "未知")
    worker_alive = task_info.get("存活", False)
    worker_pid = task_info.get("工作进程ID")
    launcher_pid = task_info.get("启动进程ID")
    restart_count = task_info.get("重启次数", 0)
    heartbeat = task_info.get("心跳") or payload.get("工作进程心跳") or {}
    if worker_alive:
        health = f"工作进程心跳正常（PID {worker_pid}）"
    elif status.get("状态") == "完成":
        health = "任务已完成"
    elif heartbeat.get("存在"):
        health = f"工作进程心跳未活跃（距上次 {heartbeat.get('距上次心跳秒数')} 秒）"
    else:
        health = "尚未收到工作进程心跳"
    if restart_count > 0:
        health += f" | 已自动重启 {restart_count} 次"
    summary = (
        f"进度：第 {processed}/{target} 页（{pct}%）| "
        f"速度：{avg_text} | 已运行：{metrics.get('运行时间', '未知')} | "
        f"预计剩余：{eta_text} | 运行判断：{runtime} | 监测：{health}"
    )
    result = zh_data(payload)
    result["进度摘要"] = summary
    result["任务指标"] = metrics
    # 同时提供扁平字段，方便MCP客户端不解析嵌套对象也能直接展示。
    for key in (
        "运行时间",
        "运行时间秒",
        "剩余时间",
        "剩余时间秒",
        "书籍名",
        "总处理页数",
        "已处理页数",
        "处理进度",
        "处理进度文本",
        "处理速度",
        "处理速度文本",
        "资源占用率",
    ):
        result[key] = metrics.get(key)
    result["工作进程健康"] = {
        "存活": worker_alive,
        "工作进程ID": worker_pid,
        "启动进程ID": launcher_pid,
        "心跳": heartbeat,
        "重启次数": restart_count,
        "监控阶段": task_info.get("监控阶段"),
        "页级前进已停滞": task_info.get("页级前进已停滞", False),
        "距上次页级前进秒数": task_info.get("距上次页级前进秒数"),
        "当前页": heartbeat.get("当前页"),
        "最后完成页": heartbeat.get("最后完成页"),
        "资源占用率": metrics.get("资源占用率"),
        "说明": health,
    }
    result["监控配置"] = {
        "监控间隔秒": _TaskManager.WATCH_INTERVAL,
        "心跳超时秒": _TaskManager.HEARTBEAT_TIMEOUT,
        "页级前进超时秒": _TaskManager.PROGRESS_TIMEOUT,
        "安全停止等待秒": _TaskManager.CANCEL_GRACE,
        "最大自动重启": _TaskManager.MAX_AUTO_RESTART,
        "自动恢复方式": "先请求停止并确认旧工作进程退出，再按原模式断点恢复",
    }
    state_name = str(status.get("状态") or "未知")
    if state_name == "完成":
        result["代理动作"] = {
            "operation": "none",
            "requires_agent_action": False,
            "automatic_handling": "任务已完成；监管层不再占用 OCR worker。",
            "next_poll_after_seconds": None,
        }
    elif worker_alive:
        result["代理动作"] = {
            "operation": "observe",
            "requires_agent_action": False,
            "automatic_handling": "工作进程和页级心跳由监管层持续监测；卡页会按恢复策略处理。",
            "next_poll_after_seconds": 30,
        }
    elif state_name in {"启动中", "进行中", "已取消", "卡死", "未完成"}:
        result["代理动作"] = {
            "operation": "automatic_recovery",
            "requires_agent_action": False,
            "automatic_handling": "监管层正在确认旧进程、执行断点恢复或等待租约接管。",
            "next_poll_after_seconds": 30,
        }
    else:
        result["代理动作"] = {
            "operation": "review_failure",
            "requires_agent_action": True,
            "automatic_handling": "自动恢复已无法安全继续；请根据失败原因提供密码、可打开的 PDF 或运行环境。",
            "next_poll_after_seconds": None,
        }
    result["后续只读调用"] = {
        "工具": "rescue_pdf",
        "参数": {"job_dir": job_dir, "workflow": "status"},
        "只读": True,
    }
    result["contract_version"] = "1.0"
    result["next_call"] = {
        "tool": "rescue_pdf",
        "arguments": {"job_dir": job_dir, "workflow": "status"},
        "read_only": True,
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
    description="检查任务是否疑似中断；确认中断后复用逐页缓存，从断点继续。恢复会启动独立 OCR 工作进程，立即返回，不阻塞 MCP。",
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
    from .book_pipeline import _mode_from_status

    selected_mode = mode or _mode_from_status(status_payload.get("状态", {}).get("模式"))
    # 如果已在运行，直接返回
    if _task_manager.is_running(job_dir):
        return {
            "状态": "任务已在运行",
            "说明": "独立工作进程仍在提取中，无需重复恢复。",
            "任务状态": zh_data(status_payload),
            "后续步骤": ["用 get_job_status 查看实时进度和线程健康"],
        }
    if not source_pdf:
        raise ValueError("状态文件中缺少来源PDF，无法恢复任务")
    # 使用已有任务目录的父目录，避免生成嵌套的 *-rescue-result 目录。
    actual_job_dir, already = _task_manager.start_extraction(
        source_pdf,
        output_dir=str(Path(job_dir).resolve().parent),
        mode=selected_mode,
        resume=True,
        password=password,
    )
    if ctx is not None:
        await ctx.info(f"已启动后台恢复：{job_dir}（剩余 {remaining} 页）")
    return {
        "状态": "已在运行" if already else "已启动后台恢复",
        "说明": f"剩余 {remaining} 页，独立工作进程复用缓存继续，MCP 不会被占用。",
        "任务状态": zh_data(status_payload),
        "任务目录": actual_job_dir,
        "工作进程监测": "已启用（心跳异常时先安全停止，再确认退出后断点恢复）",
        "后续步骤": [
            "用 get_job_status 传入任务目录查看实时进度和线程健康",
            "用 audit_job_quality 巡检已处理页质量",
        ],
    }


@mcp.tool(
    name="cancel_job",
    title="安全停止任务",
    description="请求正在运行的书籍任务在当前页边界安全停止。请求会持久化；OCR 无响应时监管层会按跨平台进程树策略清理，MCP 不会被阻塞。",
)
def cancel_job(job_dir: str, reason: str = "用户请求停止") -> dict[str, Any]:
    return zh_data(_task_manager.request_cancel(job_dir, reason=reason))


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
    name="get_iteration_plan",
    title="查看迭代改善计划",
    description="基于当前任务状态、质量巡检和监管事件生成版本化改善建议。该工具只提出可审计建议，绝不会自动改写OCR规则、更新代码或重跑任务。",
)
def get_iteration_plan(
    job_dir: str,
    max_issues: int = 80,
    strategy_version: str = "1.0.0",
) -> dict[str, Any]:
    task_status = run_read_job_status(job_dir)
    quality_audit = run_audit_job_quality(job_dir, max_issues=max_issues)
    snapshot = _task_manager.get_supervision_snapshot(job_dir) or {}
    task_events = snapshot.get("events") if isinstance(snapshot, dict) else None
    plan = build_iteration_plan(
        task_status=task_status,
        quality_audit=quality_audit,
        task_events=task_events,
        strategy_version=strategy_version,
    )
    return zh_data(plan)


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
    description="按目录批量提取PDF书籍，在后台调度独立OCR worker，立即返回。用 get_batch_status 查看书本完成数、页数进度和资源调度。并发数会结合CPU线程、系统内存和每个worker的实际占用动态调整；支持断点续传。",
)
def batch_extract_library(
    root: str,
    output_dir: str | None = None,
    mode: str = "book-fast",
    max_books: int | None = None,
    max_pages_per_book: int | None = None,
    resume: bool = True,
    max_workers: int | None = None,
) -> dict[str, Any]:
    batch_kwargs: dict[str, Any] = {
        "root": root,
        "output_dir": output_dir,
        "mode": mode,
        "max_books": max_books,
        "max_pages_per_book": max_pages_per_book,
        "resume": resume,
    }
    if max_workers is not None:
        batch_kwargs["max_workers"] = max_workers
    response = zh_data(_batch_manager.start_batch(**batch_kwargs))
    response["contract_version"] = "1.0"
    response["next_call"] = {
        "tool": "get_batch_status",
        "arguments": {},
        "read_only": True,
    }
    return response


@mcp.tool(
    name="get_batch_status",
    title="查看批量任务状态",
    description=(
        "查看批量提取的整体进度，返回书本完成数/总数、当前书籍运行时间、剩余时间、"
        "总处理页数、处理进度、处理速度，以及固定监测格式中的逐 worker 任务列表（v=完成，-=执行中）。"
        "每个活动 worker 都包含监督层实际页速、真实心跳确认 PID、OCR线程预算、进程线程、"
        "CPU整机占比、CPU等效核心和RSS内存。总 CPU 比例按整机逻辑 CPU 归一化，不把多核累计值误报为百分比。"
    ),
    annotations=ToolAnnotations(
        readOnlyHint=True,
        destructiveHint=False,
        idempotentHint=True,
        openWorldHint=False,
    ),
)
def get_batch_status() -> dict[str, Any]:
    response = zh_data(_batch_manager.status())
    response["contract_version"] = "1.0"
    response["next_call"] = {
        "tool": "get_batch_status",
        "arguments": {},
        "read_only": True,
    }
    return response


@mcp.tool(
    name="stop_batch",
    title="停止批量任务",
    description="停止批量提取（当前正在处理的书籍会继续完成，不再启动下一本）。",
)
def stop_batch() -> dict[str, Any]:
    return zh_data(_batch_manager.stop())


@mcp.tool(
    name="plan_ocr_capacity_profile",
    title="规划 OCR 容量基准",
    description=(
        "规划 1/2/3/4 线程单 worker 与多 worker 的隔离 OCR 吞吐基准。"
        "只创建配置和私有样本计划；发现任意生产 OCR 正在运行时会标记为延期，不会占用当前任务资源。"
    ),
)
def plan_ocr_capacity_profile(
    source_pdf: str,
    mode: str = "book-fast",
    sample_pages: int = 8,
    warmup_pages: int = 2,
    max_workers: int | None = None,
    candidate_threads: list[int] | None = None,
) -> dict[str, Any]:
    return zh_data(
        _capacity_benchmark_manager.plan(
            source_pdf=source_pdf,
            mode=mode,
            sample_pages=sample_pages,
            warmup_pages=warmup_pages,
            max_workers=max_workers,
            candidate_threads=tuple(candidate_threads or (1, 2, 3, 4)),
        )
    )


@mcp.tool(
    name="start_ocr_capacity_profile",
    title="启动 OCR 容量基准",
    description=(
        "在机器没有任何生产 OCR 时后台运行已规划的容量基准。"
        "每个候选使用独立、不重叠的私有 PDF 页样本；调用立即返回，OCR 不会阻塞 MCP。"
    ),
)
def start_ocr_capacity_profile(profile_id: str) -> dict[str, Any]:
    return zh_data(_capacity_benchmark_manager.start(profile_id))


@mcp.tool(
    name="get_ocr_capacity_profile",
    title="查看 OCR 容量基准",
    description="读取容量基准的候选、逐 worker 线程资源样本、页吞吐、质量门禁和建议；不改变任何运行中任务。",
)
def get_ocr_capacity_profile(profile_id: str) -> dict[str, Any]:
    return zh_data(_capacity_benchmark_manager.status(profile_id))


@mcp.tool(
    name="activate_ocr_capacity_profile",
    title="激活 OCR 容量策略",
    description=(
        "显式激活已完成基准给出的建议，供之后启动的批处理 worker 使用。"
        "不会热改、重启或中断已经运行的 OCR worker。"
    ),
)
def activate_ocr_capacity_profile(profile_id: str) -> dict[str, Any]:
    return zh_data(_capacity_benchmark_manager.activate(profile_id))


_MCP_TRANSPORT_ALIASES = {
    "stdio": "stdio",
    "http": "streamable-http",
    "streamable-http": "streamable-http",
    "streamable_http": "streamable-http",
}
_LOOPBACK_HTTP_HOSTS = {"127.0.0.1", "localhost", "::1", "[::1]"}


def _configured_mcp_transport() -> Literal["stdio", "streamable-http"]:
    """Read the public transport switch without changing the safe stdio default.

    A local Streamable HTTP endpoint is useful for MCP hosts that cannot launch a
    child process.  It intentionally remains loopback-only: this service can
    access local PDFs and does not configure an authentication provider.
    """
    raw_value = os.environ.get("PDF_RESCUE_MCP_TRANSPORT", "stdio")
    selected = _MCP_TRANSPORT_ALIASES.get(raw_value.strip().lower())
    if selected is None:
        allowed = ", ".join(sorted(_MCP_TRANSPORT_ALIASES))
        raise ValueError(
            "PDF_RESCUE_MCP_TRANSPORT 仅支持 "
            f"{allowed}；默认值为 stdio。"
        )
    return selected  # type: ignore[return-value]


def _configure_local_http_endpoint() -> None:
    """Apply local-only HTTP settings before FastMCP starts its ASGI server."""
    host = os.environ.get("PDF_RESCUE_MCP_HOST", "127.0.0.1").strip()
    if host.lower() not in _LOOPBACK_HTTP_HOSTS:
        raise ValueError(
            "Streamable HTTP 仅允许绑定本机回环地址（127.0.0.1、localhost 或 ::1）。"
            "如需跨机器访问，请在受认证保护的 MCP 网关后部署。"
        )
    raw_port = os.environ.get("PDF_RESCUE_MCP_PORT", "8000").strip()
    try:
        port = int(raw_port)
    except ValueError as exc:
        raise ValueError("PDF_RESCUE_MCP_PORT 必须是 1 到 65535 的整数。") from exc
    if not 1 <= port <= 65535:
        raise ValueError("PDF_RESCUE_MCP_PORT 必须是 1 到 65535 的整数。")
    mcp.settings.host = host
    mcp.settings.port = port


def main() -> None:
    configure_utf8_stdio()
    configure_file_logging()
    _batch_manager.restore_pending()
    # A second stdio adapter that only opens a status view must stay passive
    # while another controller owns the durable batch ledger.  Standalone jobs
    # still recover normally when no active batch controller exists.
    _task_manager.restore_pending(allow_takeover=_batch_manager.allows_task_recovery)
    transport = _configured_mcp_transport()
    if transport == "streamable-http":
        _configure_local_http_endpoint()
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
