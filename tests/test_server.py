from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta
from pathlib import Path

import pdf_rescue_mcp.server as server
import pdf_rescue_mcp.paths as paths


class FakeProcess:
    pid = 24680


def test_get_job_status_exposes_metrics_and_worker_resources(tmp_path: Path, monkeypatch) -> None:
    job_dir = tmp_path / "书籍-rescue-result"
    job_dir.mkdir()
    (job_dir / "状态.json").write_text(
        json.dumps(
            {
                "状态": "进行中",
                "来源PDF": str(tmp_path / "农业化学卷.pdf"),
                "目标页数": 100,
                "已处理页数": 25,
                "更新时间": "2026-07-18T16:00:00",
                "开始时间": "2026-07-18T15:00:00",
                "已耗时秒": 3600,
                "平均每页秒": 144.0,
                "预计剩余秒": 10800,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    class FakeTaskManager:
        def get_task_info(self, _job_dir: str) -> dict:
            return {
                "存活": True,
                "工作进程ID": 97531,
                "启动进程ID": 24680,
                "重启次数": 0,
                "监控阶段": "运行中",
                "心跳": {"存在": True, "活跃": True, "进程ID": 97531},
            }

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda pid: {"状态": "可用", "进程ID": pid, "CPU占用率": 31.5, "内存占用率": 4.2},
    )

    result = server.get_job_status(str(job_dir))

    assert result["书籍名"] == "农业化学卷"
    assert result["总处理页数"] == 100
    assert result["处理进度"] == 25.0
    assert result["处理速度"] == 144.0
    assert result["剩余时间秒"] == 10800
    assert result["资源占用率"]["CPU占用率"] == 31.5
    assert result["任务指标"]["资源占用率"]["进程ID"] == 97531


def test_batch_status_exposes_current_book_metrics(tmp_path: Path, monkeypatch) -> None:
    job_dir = tmp_path / "输出" / "书-rescue-result"
    job_dir.mkdir(parents=True)
    (job_dir / "状态.json").write_text(
        json.dumps(
            {
                "状态": "进行中",
                "来源PDF": str(tmp_path / "书.pdf"),
                "目标页数": 20,
                "已处理页数": 5,
                "开始时间": "2026-07-18T15:00:00",
                "已耗时秒": 300,
                "平均每页秒": 60.0,
                "预计剩余秒": 900,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    manager = server._BatchManager()
    manager._books = [{"文件名": "书.pdf", "PDF路径": str(tmp_path / "书.pdf")}]
    manager._current_index = 0
    manager._current_job_dir = str(job_dir)
    manager._started_at = 0
    monkeypatch.setattr(server, "_task_manager", type("TaskManager", (), {
        "get_task_info": lambda _self, _job_dir: {
            "存活": True,
            "工作进程ID": 97531,
            "心跳": {"存在": True, "活跃": True, "进程ID": 97531},
        }
    })())
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda pid: {"状态": "可用", "进程ID": pid, "CPU占用率": 12.0, "内存占用率": 2.0},
    )

    result = manager.status()

    assert result["书籍名"] == "书"
    assert result["总处理页数"] == 20
    assert result["处理进度"] == 25.0
    assert result["处理速度"] == 60.0
    assert result["当前书籍指标"]["资源占用率"]["CPU占用率"] == 12.0


def test_mcp_transport_defaults_to_stdio(monkeypatch) -> None:
    monkeypatch.delenv("PDF_RESCUE_MCP_TRANSPORT", raising=False)

    assert server._configured_mcp_transport() == "stdio"


def test_mcp_transport_accepts_streamable_http_aliases(monkeypatch) -> None:
    monkeypatch.setenv("PDF_RESCUE_MCP_TRANSPORT", "http")

    assert server._configured_mcp_transport() == "streamable-http"


def test_mcp_transport_rejects_unknown_value(monkeypatch) -> None:
    monkeypatch.setenv("PDF_RESCUE_MCP_TRANSPORT", "sse")

    try:
        server._configured_mcp_transport()
    except ValueError as exc:
        assert "仅支持" in str(exc)
    else:
        raise AssertionError("invalid MCP transport must be rejected")


def test_streamable_http_endpoint_is_loopback_only(monkeypatch) -> None:
    monkeypatch.setenv("PDF_RESCUE_MCP_HOST", "0.0.0.0")

    try:
        server._configure_local_http_endpoint()
    except ValueError as exc:
        assert "回环" in str(exc)
    else:
        raise AssertionError("non-loopback HTTP binding must be rejected")


def test_streamable_http_endpoint_sets_explicit_loopback_port(monkeypatch) -> None:
    original_host = server.mcp.settings.host
    original_port = server.mcp.settings.port
    monkeypatch.setenv("PDF_RESCUE_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("PDF_RESCUE_MCP_PORT", "8765")
    try:
        server._configure_local_http_endpoint()

        assert server.mcp.settings.host == "127.0.0.1"
        assert server.mcp.settings.port == 8765
    finally:
        server.mcp.settings.host = original_host
        server.mcp.settings.port = original_port


def test_background_book_extraction_returns_task_information(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    monkeypatch.setattr(server, "_task_manager", manager)
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)

    result = server.start_book_extraction_background(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-balanced",
        max_pages=12,
        resume=True,
    )

    assert result["状态"] == "已启动"
    assert result["启动进程ID"] == 24680
    assert (tmp_path / result["日志路径"]).is_file()
    assert result["日志路径"].startswith("logs/")
    command, kwargs = calls[0]
    assert command[0] == server.sys.executable
    assert command[1:5] == ["-u", "-m", "pdf_rescue_mcp.cli", "提取"]
    assert "--resume" in command
    assert "--max-pages" in command
    assert kwargs["stdin"] is server.subprocess.DEVNULL
    assert kwargs["stderr"] is server.subprocess.STDOUT
    assert "PDF_RESCUE_HEARTBEAT_PATH" in kwargs["env"]
    assert "PDF_RESCUE_CANCEL_PATH" in kwargs["env"]

    heartbeat_path = Path(kwargs["env"]["PDF_RESCUE_HEARTBEAT_PATH"])
    heartbeat_path.write_text('{"状态":"运行中","进程ID":97531}', encoding="utf-8")
    health = manager.get_task_info(result["任务目录"])
    assert health and health["存活"] is True
    assert health["工作进程ID"] == 97531
    assert health["启动进程ID"] == 24680
    manager._stopping = True


def test_background_book_extraction_avoids_windows_flags_on_linux(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    monkeypatch.setattr(server, "_task_manager", manager)
    source = tmp_path / "book.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(server.sys, "platform", "linux")

    server.start_book_extraction_background(str(source), output_dir=str(tmp_path / "output"))

    assert "creationflags" not in calls[0][1]
    manager._stopping = True


def test_background_task_passes_password_only_through_child_environment(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    monkeypatch.setattr(server, "_task_manager", manager)
    source = tmp_path / "locked.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)

    job_dir, already = manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "output"),
        mode="book-balanced",
        max_pages=None,
        resume=True,
        password="only-for-this-call",
    )

    metadata = json.loads((Path(job_dir) / manager.TASK_METADATA_FILE).read_text(encoding="utf-8"))
    task_state = json.loads(manager._state_path().read_text(encoding="utf-8"))
    command, popen_kwargs = calls[0]

    assert already is False
    assert popen_kwargs["env"]["PDF_RESCUE_PASSWORD"] == "only-for-this-call"
    assert "only-for-this-call" not in command
    assert "only-for-this-call" not in json.dumps(metadata, ensure_ascii=False)
    assert "only-for-this-call" not in json.dumps(task_state, ensure_ascii=False)
    manager._stopping = True


def test_stalled_worker_is_asked_to_stop_before_a_restart(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    job_dir = tmp_path / "任务"
    job_dir.mkdir()
    (job_dir / "状态.json").write_text('{"状态":"进行中"}', encoding="utf-8")
    metadata_path, heartbeat_path, cancel_path = manager._task_paths(job_dir)
    info = {
        "job_dir": str(job_dir),
        "source": str(tmp_path / "书.pdf"),
        "mode": "book-fast",
        "started_at": 0,
        "restart_count": 0,
        "phase": "运行中",
        "launcher_pid": None,
        "worker_pid": None,
        "metadata_path": str(metadata_path),
        "heartbeat_path": str(heartbeat_path),
        "cancel_path": str(cancel_path),
        "cancel_requested_at": None,
        "process": None,
    }

    with manager._lock:
        manager._tasks[str(job_dir)] = info
        manager._watch_task_locked(str(job_dir), info)

    assert cancel_path.exists()
    assert info["phase"] == "请求停止中"
    assert info["restart_count"] == 0
    manager._stopping = True


def test_live_heartbeat_with_no_page_progress_requests_a_safe_stop(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    manager.PROGRESS_TIMEOUT = 1
    job_dir = tmp_path / "任务"
    job_dir.mkdir()
    (job_dir / "状态.json").write_text('{"状态":"进行中"}', encoding="utf-8")
    metadata_path, heartbeat_path, cancel_path = manager._task_paths(job_dir)
    stale_time = (datetime.now() - timedelta(seconds=10)).isoformat(timespec="seconds")
    heartbeat_path.write_text(
        json.dumps(
            {
                "状态": "运行中",
                "进程ID": None,
                "当前页": 17,
                "当前页开始时间": stale_time,
                "最后完成页": 16,
                "最后进度时间": stale_time,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    info = {
        "job_dir": str(job_dir),
        "source": str(tmp_path / "书.pdf"),
        "mode": "book-fast",
        "started_at": time.time(),
        "restart_count": 0,
        "phase": "运行中",
        "launcher_pid": None,
        "worker_pid": None,
        "metadata_path": str(metadata_path),
        "heartbeat_path": str(heartbeat_path),
        "cancel_path": str(cancel_path),
        "cancel_requested_at": None,
        "process": None,
    }

    with manager._lock:
        manager._tasks[str(job_dir)] = info
        manager._watch_task_locked(str(job_dir), info)

    assert cancel_path.exists()
    assert info["phase"] == "请求停止中"
    assert manager.get_task_info(str(job_dir))["页级前进已停滞"] is True
    manager._stopping = True


def test_durable_supervisor_passes_only_non_secret_worker_identity(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv("PDF_RESCUE_RUNTIME_ROOT", str(tmp_path / "runtime-layout"))
    supervisor = server.LocalSupervisor(
        database_path=tmp_path / "runtime" / "tasks.sqlite3",
        owner_id="server-test",
    )
    manager = server._TaskManager(enable_durable_supervision=True, supervisor=supervisor)
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    job_dir, already = manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-balanced",
        max_pages=None,
        resume=True,
        password="runtime-only",
    )

    assert already is False
    environment = calls[0][1]["env"]
    assert environment["PDF_RESCUE_TASK_DATABASE"] == str(supervisor.database_path)
    assert environment["PDF_RESCUE_TASK_ATTEMPT_ID"]
    assert "runtime-only" not in json.dumps(
        json.loads((Path(job_dir) / manager.TASK_METADATA_FILE).read_text(encoding="utf-8")),
        ensure_ascii=False,
    )
    task_id = manager.get_task_info(job_dir)["持久任务ID"]
    assert supervisor.store.get_task(task_id).state == "running"

    (Path(job_dir) / "状态.json").write_text('{"状态":"完成"}', encoding="utf-8")
    with manager._lock:
        manager._watch_task_locked(job_dir, manager._tasks[job_dir])
    assert supervisor.store.get_task(task_id).state == "completed"
    manager._stopping = True


def _plan(route: str, estimated_seconds: int = 5) -> dict[str, object]:
    return {
        "route": route,
        "estimated_seconds": estimated_seconds,
        "page_count": 12,
        "mode": "book-balanced",
        "warnings": [],
    }


def test_primary_tool_extracts_a_clear_request_without_asking_for_tool_choice(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("direct_text_extract"))

    def fake_extract(*args, **kwargs):
        calls.append(kwargs)
        return {"status": "ok", "job_dir": "D:/out/book-rescue-result"}

    monkeypatch.setattr(server, "run_extract_book_text", fake_extract)

    result = asyncio.run(
        server.rescue_pdf(
            path="D:/books/book.pdf",
            request="请分析这本书并提取成可核对的文本",
        )
    )

    assert result["状态"] == "已完成PDF救援"
    assert result["已执行"] == ["诊断PDF", "规划处理任务", "提取书籍文本"]
    assert calls == [{
        "output_dir": None,
        "mode": "book-balanced",
        "max_pages": None,
        "resume": True,
        "password": None,
        "progress_callback": None,
    }]


def test_primary_tool_starts_long_ocr_in_background(monkeypatch) -> None:
    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("ocr_required", 180))
    launches: list[dict[str, object]] = []

    class FakeTaskManager:
        def start_extraction(self, *args, **kwargs):
            launches.append(kwargs)
            return "D:/out/book-rescue-result", False

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())

    result = asyncio.run(server.rescue_pdf(path="D:/books/scan.pdf", request="把这个扫描PDF救援成文本"))

    assert result["状态"] == "已启动后台救援任务"
    assert result["已执行"][-1] == "后台提取书籍"
    assert launches == [{"output_dir": None, "mode": "book-balanced", "max_pages": None, "resume": True, "password": None}]


def test_primary_tool_never_runs_ocr_in_foreground_even_when_requested(monkeypatch) -> None:
    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("ocr_required", 1))
    launches: list[dict[str, object]] = []

    class FakeTaskManager:
        def start_extraction(self, *args, **kwargs):
            launches.append(kwargs)
            return "D:/out/book-rescue-result", False

    def synchronous_ocr_must_not_run(*_args, **_kwargs):
        raise AssertionError("OCR must be isolated from the MCP process")

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())
    monkeypatch.setattr(server, "run_extract_book_text", synchronous_ocr_must_not_run)

    result = asyncio.run(
        server.rescue_pdf(
            path="D:/books/scan.pdf",
            request="提取扫描PDF",
            execution="foreground",
        )
    )

    assert result["状态"] == "已启动后台救援任务"
    assert len(launches) == 1


def test_iteration_tool_combines_business_quality_and_supervision_evidence(monkeypatch) -> None:
    monkeypatch.setattr(
        server,
        "run_read_job_status",
        lambda _job_dir: {
            "状态": {"状态": "完成", "目标页数": 2, "已处理页数": 2},
            "任务指标": {"资源占用率": {"CPU占用率": 12.0, "内存占用率": 20.0}},
        },
    )
    monkeypatch.setattr(
        server,
        "run_audit_job_quality",
        lambda _job_dir, **_kwargs: {
            "状态": "完成",
            "目标页数": 2,
            "已巡检页数": 2,
            "尚未巡检页数": 0,
            "低置信页数": 0,
            "无文本页数": 0,
            "可自动刷新页数": 0,
            "图表噪声残留页数": 0,
            "分裂标题残留页数": 0,
            "图文混排标注页数": 0,
        },
    )
    monkeypatch.setattr(
        server,
        "_task_manager",
        type(
            "TaskManager",
            (),
            {
                "get_supervision_snapshot": lambda _self, _job_dir: {
                    "events": [{"event_type": "page_completed", "created_at": 123.0}]
                }
            },
        )(),
    )

    result = server.get_iteration_plan("D:/out/book-rescue-result")

    assert result["governance"]["advisory_only"] is True
    assert result["governance"]["can_auto_apply"] is False
    assert result["evidence_summary"]["events"]["by_type"] == {"page_completed": 1}


def test_batch_tool_preserves_page_limit_and_resume_policy(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeBatchManager:
        def start_batch(self, **kwargs):
            calls.append(kwargs)
            return {"状态": "已启动"}

    monkeypatch.setattr(server, "_batch_manager", FakeBatchManager())

    result = server.batch_extract_library(
        "D:/books",
        output_dir="D:/out",
        mode="book-fast",
        max_books=3,
        max_pages_per_book=12,
        resume=False,
    )

    assert result["状态"] == "已启动"
    assert calls == [{
        "root": "D:/books",
        "output_dir": "D:/out",
        "mode": "book-fast",
        "max_books": 3,
        "max_pages_per_book": 12,
        "resume": False,
    }]


def test_primary_tool_starts_password_protected_ocr_in_background(monkeypatch) -> None:
    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("ocr_required", 180))
    launches: list[dict[str, object]] = []

    class FakeTaskManager:
        def start_extraction(self, *args, **kwargs):
            launches.append(kwargs)
            return "D:/out/locked-rescue-result", False

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())

    result = asyncio.run(
        server.rescue_pdf(
            path="D:/books/locked-scan.pdf",
            request="把这个加密扫描PDF救援成文本",
            password="only-for-this-call",
        )
    )

    assert result["状态"] == "已启动后台救援任务"
    assert launches == [{
        "output_dir": None,
        "mode": "book-balanced",
        "max_pages": None,
        "resume": True,
        "password": "only-for-this-call",
    }]


def test_primary_tool_resume_reuses_background_task_manager(monkeypatch) -> None:
    launches: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class FakeTaskManager:
        def is_running(self, _job_dir: str) -> bool:
            return False

        def start_extraction(self, *args, **kwargs):
            launches.append((args, kwargs))
            return "D:/out/book-rescue-result", False

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())
    monkeypatch.setattr(
        server,
        "run_read_job_status",
        lambda _job_dir, **_kwargs: {
            "状态": {
                "状态": "未完成",
                "来源PDF": "D:/books/book.pdf",
                "目标页数": 100,
                "已处理页数": 35,
                "模式": "book-balanced",
            }
        },
    )

    def synchronous_resume_must_not_run(*_args, **_kwargs):
        raise AssertionError("rescue_pdf resume must not run OCR synchronously")

    monkeypatch.setattr(server, "run_resume_job", synchronous_resume_must_not_run, raising=False)

    result = asyncio.run(
        server.rescue_pdf(
            job_dir="D:/out/book-rescue-result",
            workflow="resume",
            password="only-for-this-call",
        )
    )

    assert result["状态"] == "已尝试恢复任务"
    assert launches == [
        (
            ("D:/books/book.pdf",),
            {
                "output_dir": str(Path("D:/out/book-rescue-result").resolve().parent),
                "mode": "book-balanced",
                "resume": True,
                "password": "only-for-this-call",
            },
        )
    ]


def test_primary_tool_routes_existing_job_to_status(monkeypatch) -> None:
    monkeypatch.setattr(
        server,
        "run_read_job_status",
        lambda job_dir: {"status": {"status": "running"}, "job_dir": job_dir},
    )

    result = asyncio.run(server.rescue_pdf(job_dir="D:/out/book-rescue-result", request="现在处理进度怎么样"))

    assert result["状态"] == "已读取任务状态"
    assert result["已执行"] == ["查看任务状态"]


def test_primary_tool_diagnoses_before_extracting_when_user_only_asks_about_ocr(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_plan(*args, **kwargs):
        calls.append(kwargs)
        return _plan("ocr_required", 180)

    monkeypatch.setattr(server, "run_plan_pdf_job", fake_plan)
    result = asyncio.run(server.rescue_pdf(path="D:/books/scan.pdf", request="检查这个PDF是否需要OCR"))

    assert result["状态"] == "已完成诊断和规划"
    assert result["已执行"] == ["诊断PDF", "规划处理任务"]
    assert calls == [{"target_quality": "book-balanced", "password": None}]
