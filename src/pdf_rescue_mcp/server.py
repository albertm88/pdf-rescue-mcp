from __future__ import annotations

import asyncio
import json
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
from .supervisor import LocalSupervisor, SupervisedAttempt
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
        serialized = {
            job_key: {
                key: value
                for key, value in info.items()
                if key not in {"process", "password", "supervision_context"}
            }
            for job_key, info in self._tasks.items()
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

    def restore_pending(self) -> None:
        """Reattach the monitor after an MCP restart; do not start work during import."""
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
                info = dict(stored)
                info["process"] = None
                info.setdefault("job_dir", job_key)
                info.setdefault("metadata_path", str(Path(job_key) / self.TASK_METADATA_FILE))
                info.setdefault("heartbeat_path", str(Path(job_key) / self.HEARTBEAT_FILE))
                info.setdefault("cancel_path", str(Path(job_key) / self.CANCEL_FILE))
                self._tasks[job_key] = info
            if self._tasks:
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
        payload.update(
            {
                "状态": "启动中",
                "来源PDF": info["source"],
                "模式": info["mode"],
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
            if info and self._pid_alive(self._heartbeat(info).get("进程ID")):
                # The old worker has no fresh heartbeat. Let the monitor stop it before retrying.
                self._request_cancel_locked(info)
                return job_key, True

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
                if self._is_live(stored_info) or self._pid_alive(self._heartbeat(stored_info).get("进程ID")):
                    self._tasks[job_key] = stored_info
                    self._ensure_watcher_locked()
                    return job_key, True

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
            state = self._status_state(Path(job_key))
            live = bool(heartbeat["活跃"] and not self._is_terminal_state(state))
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


class _BatchManager:
    """批量任务管理器：在后台线程中逐本启动提取，MCP 服务器保持响应。

    支持状态持久化：批量配置和进度写入文件，MCP 服务器重启后自动恢复。
    """

    BATCH_STATE_FILE = "批量状态.json"

    def __init__(self, *, use_portable_runtime: bool = False) -> None:
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._running = False
        self._books: list[dict[str, Any]] = []
        self._current_index = -1
        self._current_job_dir: str | None = None
        self._completed: list[str] = []
        self._failed: list[dict[str, str]] = []
        self._started_at: float | None = None
        self._mode = "book-fast"
        self._max_pages_per_book: int | None = None
        self._resume = True
        self._output_dir: str | None = None
        self._root: str | None = None
        self._state_path: Path | None = None
        self._use_portable_runtime = use_portable_runtime

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

    def _save_state(self) -> None:
        """将当前批量状态持久化到磁盘。"""
        try:
            state = {
                "运行中": self._running,
                "根目录": self._root,
                "输出目录": self._output_dir,
                "模式": self._mode,
                "每本最大页数": self._max_pages_per_book,
                "断点续传": self._resume,
                "开始时间": self._started_at,
                "当前索引": self._current_index,
                "当前任务目录": self._current_job_dir,
                "已完成": self._completed,
                "失败": self._failed,
                "书籍列表": self._books,
            }
            _TaskManager._atomic_json(self._state_file_path(), state)
        except Exception:
            pass

    def restore_pending(self) -> None:
        """MCP 启动完成后恢复未完成的批量任务，不在模块导入时启动后台工作。"""
        try:
            state_path = self._state_file_path()
            if not state_path.exists():
                return
            state = _TaskManager._read_json(state_path)
            if state is None:
                return
            if not state.get("运行中"):
                return
            self._root = state.get("根目录")
            self._output_dir = state.get("输出目录")
            self._mode = state.get("模式", "book-fast")
            self._max_pages_per_book = state.get("每本最大页数")
            self._resume = bool(state.get("断点续传", True))
            self._started_at = state.get("开始时间")
            self._completed = state.get("已完成", [])
            self._failed = state.get("失败", [])
            self._books = state.get("书籍列表", [])
            self._current_index = state.get("当前索引", -1)
            self._current_job_dir = state.get("当前任务目录")

            if not self._root or not self._books:
                return

            # 检查当前书籍是否仍在运行（子进程可能独立存活）
            if self._current_job_dir:
                from .library_pipeline import _read_status
                s = _read_status(Path(self._current_job_dir)) if Path(self._current_job_dir).exists() else None
                if s and s.get("状态") == "进行中":
                    # 当前书籍的子进程仍在运行，从当前索引继续
                    pass
                elif s and s.get("状态") == "完成":
                    # 当前书籍已完成，移到下一本
                    book_name = self._books[self._current_index]["文件名"].replace(".pdf", "") if 0 <= self._current_index < len(self._books) else None
                    if book_name and book_name not in self._completed:
                        self._completed.append(book_name)
                    self._current_index += 1
                else:
                    # 当前书籍未完成，从当前索引重试
                    pass

            self._running = True
            self._thread = threading.Thread(target=self._batch_loop, daemon=True, name="batch-extractor")
            self._thread.start()
        except Exception:
            # 恢复失败不影响正常使用
            pass

    def start_batch(
        self,
        root: str,
        output_dir: str | None,
        mode: str,
        max_books: int | None,
        max_pages_per_book: int | None,
        resume: bool,
    ) -> dict[str, Any]:
        """启动批量提取，立即返回，后台逐本处理。"""
        with self._lock:
            if self._running:
                return {"状态": "已在运行", "当前进度": self.status()}

            self._root = root
            self._output_dir = output_dir
            self._mode = mode
            self._max_pages_per_book = max_pages_per_book
            self._resume = resume
            self._started_at = time.time()
            self._completed = []
            self._failed = []
            self._current_index = -1
            self._current_job_dir = None

            # 扫描书库（快速模式：不逐本检查文本层，直接全部需要OCR）
            from .library_pipeline import scan_pdf_library
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
            self._save_state()
            self._thread = threading.Thread(target=self._batch_loop, daemon=True, name="batch-extractor")
            self._thread.start()

            return {
                "状态": "已启动",
                "总书数": len(self._books),
                "模式": mode,
                "输出目录": output_dir,
                "每本最大页数": max_pages_per_book,
                "断点续传": resume,
            }

    def _batch_loop(self) -> None:
        """后台线程：逐本启动提取并等待完成。"""
        from .library_pipeline import _job_dir_for_pdf, _read_status
        root_path = Path(self._root) if self._root else None
        output_path = Path(self._output_dir) if self._output_dir else None

        # 恢复时跳过已完成的书籍
        start_index = max(0, self._current_index) if self._current_index >= 0 else 0

        for i in range(start_index, len(self._books)):
            with self._lock:
                self._current_index = i
            book = self._books[i]
            pdf_path = Path(book["PDF路径"])
            book_name = book["文件名"].replace(".pdf", "")

            # 跳过已完成的
            if book_name in self._completed:
                continue

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
                        self._save_state()
                        continue

                # 启动子进程
                actual_output_dir = str(job_dir.parent) if job_dir else None
                returned_job_dir, already_running = _task_manager.start_extraction(
                    path=str(pdf_path),
                    output_dir=actual_output_dir,
                    mode=self._mode,
                    max_pages=self._max_pages_per_book,
                    resume=self._resume,
                    password=None,
                )

                with self._lock:
                    self._current_job_dir = returned_job_dir
                self._save_state()

                # 等待这本书完成
                while True:
                    time.sleep(30)
                    if not self._running:
                        self._save_state()
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
                        self._save_state()
                        break
            except Exception as e:
                with self._lock:
                    self._failed.append({"书名": book_name, "原因": str(e)})
                self._save_state()

        with self._lock:
            self._running = False
            self._current_job_dir = None
        self._save_state()

    def status(self) -> dict[str, Any]:
        """返回批量任务状态。"""
        with self._lock:
            current_book = None
            current_progress = None
            current_metrics: dict[str, Any] = {}
            current_job_dir = None
            if 0 <= self._current_index < len(self._books):
                current_book = Path(self._books[self._current_index]["文件名"]).stem
                current_job_dir = self._current_job_dir
                if self._current_job_dir:
                    status_path = Path(self._current_job_dir) / "状态.json"
                    if status_path.exists():
                        try:
                            s = json.loads(status_path.read_text(encoding="utf-8"))
                            task_info = _task_manager.get_task_info(self._current_job_dir) or {}
                            current_metrics = _build_task_metrics(s, task_info)
                            processed = current_metrics.get("已处理页数", 0)
                            total = current_metrics.get("总处理页数", 0)
                            current_progress = {
                                "已处理": processed,
                                "总数": total,
                                "百分比": current_metrics.get("处理进度文本", "0.0%"),
                                "状态": s.get("状态", "未知"),
                                "平均秒每页": current_metrics.get("处理速度"),
                                "本书预计剩余秒": current_metrics.get("剩余时间秒"),
                                "资源占用率": current_metrics.get("资源占用率"),
                            }
                        except Exception:
                            pass

            elapsed = None
            elapsed_str = None
            eta_str = None
            eta_seconds = None
            if self._started_at:
                elapsed = int(time.time() - self._started_at)
                elapsed_str = _format_duration(elapsed)

            done = len(self._completed) + len(self._failed)
            total_books = len(self._books)
            if self._running and done > 0 and elapsed and elapsed > 10:
                # 估算剩余时间
                avg_per_book = elapsed / done
                remaining_books = total_books - done
                eta_seconds = int(avg_per_book * remaining_books)
                eta_str = f"约{_format_duration(eta_seconds)}"

            resource_usage = current_metrics.get("资源占用率") or collect_process_resource_usage(None)

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
                "每本最大页数": self._max_pages_per_book,
                "断点续传": self._resume,
                "已运行时间": elapsed_str,
                "预计剩余时间": eta_str,
                "失败列表": self._failed[-5:] if self._failed else [],
                "查询提示": "用 get_job_status 传入当前书籍任务目录查看详细进度（每1-5秒更新）",
            }

    def stop(self) -> dict[str, Any]:
        """停止批量任务（当前书籍会继续完成）。"""
        with self._lock:
            self._running = False
        self._save_state()
        return {"状态": "已发送停止信号，当前书籍将继续完成"}


_batch_manager = _BatchManager(use_portable_runtime=True)


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
            max_pages_per_book=max_pages_per_book,
            resume=resume,
        )
    )


@mcp.tool(
    name="get_batch_status",
    title="查看批量任务状态",
    description="查看批量提取的整体进度，并返回当前书籍的运行时间、剩余时间、书籍名、总处理页数、处理进度、处理速度和CPU/内存资源占用率。",
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
    _task_manager.restore_pending()
    _batch_manager.restore_pending()
    transport = _configured_mcp_transport()
    if transport == "streamable-http":
        _configure_local_http_endpoint()
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
