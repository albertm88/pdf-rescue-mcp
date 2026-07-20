from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pdf_rescue_mcp.server as server
import pdf_rescue_mcp.paths as paths
import pdf_rescue_mcp.library_pipeline as library_pipeline
from pdf_rescue_mcp.supervisor import LocalSupervisor


class FakeProcess:
    pid = 24680


def _isolated_batch_manager(tmp_path: Path, owner_id: str) -> server._BatchManager:
    manager = server._BatchManager(
        controller_supervisor=LocalSupervisor(
            database_path=tmp_path / "runtime" / "tasks.sqlite3",
            owner_id=owner_id,
        )
    )
    manager._state_path = tmp_path / "state" / "批量状态.json"
    return manager


def test_batch_start_returns_before_library_discovery_finishes(tmp_path: Path, monkeypatch) -> None:
    manager = _isolated_batch_manager(tmp_path, "controller-one")
    discovery_started = threading.Event()
    release_discovery = threading.Event()

    def blocking_scan(*_args, **_kwargs):
        discovery_started.set()
        assert release_discovery.wait(5)
        return {"书籍": []}

    monkeypatch.setattr(library_pipeline, "scan_pdf_library", blocking_scan)

    started_at = time.monotonic()
    result = manager.start_batch(
        root=str(tmp_path),
        output_dir=str(tmp_path / "out"),
        mode="book-fast",
        max_books=None,
        max_pages_per_book=None,
        resume=True,
    )

    assert time.monotonic() - started_at < 0.5
    assert result["状态"] == "准备中"
    assert discovery_started.wait(2)
    assert manager.status()["批处理阶段"] == "准备中"

    release_discovery.set()
    assert manager._thread is not None
    manager._thread.join(5)
    assert manager.status()["批处理阶段"] == "已完成"


def test_second_batch_adapter_observes_without_starting_a_competing_controller(
    tmp_path: Path,
    monkeypatch,
) -> None:
    first = _isolated_batch_manager(tmp_path, "controller-one")
    second = _isolated_batch_manager(tmp_path, "controller-two")
    discovery_started = threading.Event()
    release_discovery = threading.Event()

    def blocking_scan(*_args, **_kwargs):
        discovery_started.set()
        assert release_discovery.wait(5)
        return {"书籍": []}

    monkeypatch.setattr(library_pipeline, "scan_pdf_library", blocking_scan)
    first.start_batch(
        root=str(tmp_path),
        output_dir=str(tmp_path / "out"),
        mode="book-fast",
        max_books=None,
        max_pages_per_book=None,
        resume=True,
    )
    assert discovery_started.wait(2)

    second.restore_pending()

    observed = second.status()
    assert second._thread is None
    assert observed["控制器角色"] == "观察者"
    assert observed["代理动作"]["requires_agent_action"] is False

    release_discovery.set()
    assert first._thread is not None
    first._thread.join(5)


def test_batch_observer_refreshes_the_controller_snapshot_without_becoming_a_scheduler(
    tmp_path: Path,
    monkeypatch,
) -> None:
    first = _isolated_batch_manager(tmp_path, "controller-one")
    second = _isolated_batch_manager(tmp_path, "controller-two")
    book_a = {"文件名": "book-a.pdf", "PDF路径": str(tmp_path / "book-a.pdf")}
    book_b = {"文件名": "book-b.pdf", "PDF路径": str(tmp_path / "book-b.pdf")}

    with first._lock:
        assert first._acquire_controller_lease_locked()
        first._running = True
        first._phase = "运行中"
        first._root = str(tmp_path)
        first._output_dir = str(tmp_path / "out")
        first._books = [book_a]
        first._completed = ["book-a"]
        first._save_state()

    second.restore_pending()
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("status must not sample")),
    )
    monkeypatch.setattr(
        server.ResourceScheduler,
        "plan",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("status must not plan")),
    )
    initial = second.status()
    assert second._thread is None
    assert initial["控制器角色"] == "观察者"
    assert initial["控制器所有者"] == "controller-one"
    assert initial["书本完成数"] == 1

    # The observer receives the new atomic snapshot, but never takes the
    # controller lease or writes a competing batch ledger.
    time.sleep(0.01)
    with first._lock:
        first._books.append(book_b)
        first._completed.append("book-b")
        first._save_state()
    persisted_after_controller_write = first._state_path.read_text(encoding="utf-8")

    refreshed = second.status()
    assert refreshed["书本总数"] == 2
    assert refreshed["书本完成数"] == 2
    assert second._controller_lease is None
    assert second._thread is None
    assert first._state_path.read_text(encoding="utf-8") == persisted_after_controller_write

    with first._lock:
        first._release_controller_lease_locked()


def test_batch_observer_promotes_after_controller_lease_is_released(
    tmp_path: Path,
    monkeypatch,
) -> None:
    first = _isolated_batch_manager(tmp_path, "controller-one")
    promoted: list[str] = []
    second = server._BatchManager(
        controller_supervisor=LocalSupervisor(
            database_path=tmp_path / "runtime" / "tasks.sqlite3",
            owner_id="controller-two",
        ),
        on_observer_promoted=lambda: promoted.append("promoted"),
    )
    second._state_path = first._state_path
    second.OBSERVER_TAKEOVER_INTERVAL = 0.01
    discovery_started = threading.Event()
    release_discovery = threading.Event()

    def blocking_scan(*_args, **_kwargs):
        discovery_started.set()
        assert release_discovery.wait(3)
        return {"书籍": []}

    monkeypatch.setattr(library_pipeline, "scan_pdf_library", blocking_scan)
    with first._lock:
        assert first._acquire_controller_lease_locked()
        first._running = True
        first._phase = "准备中"
        first._root = str(tmp_path)
        first._output_dir = str(tmp_path / "out")
        first._save_state()

    second.restore_pending()
    assert second._observer_only is True
    assert second._observer_takeover_thread is not None

    with first._lock:
        first._release_controller_lease_locked()

    assert discovery_started.wait(2)
    assert promoted == ["promoted"]
    assert second._observer_only is False
    assert second._controller_lease is not None

    release_discovery.set()
    assert second._thread is not None
    second._thread.join(3)


def test_batch_restore_with_unavailable_lease_backend_stays_read_only_observer(
    tmp_path: Path,
) -> None:
    first = _isolated_batch_manager(tmp_path, "controller-one")

    class UnavailableLeaseSupervisor:
        owner_id = "unavailable-controller"
        CONTROLLER_LEASE_SECONDS = 45

        def acquire_controller_lease(self, _resource_key: str):
            raise sqlite3.OperationalError("lease database unavailable")

    with first._lock:
        assert first._acquire_controller_lease_locked()
        first._running = True
        first._phase = "运行中"
        first._root = str(tmp_path)
        first._output_dir = str(tmp_path / "out")
        first._books = [{"文件名": "book-a.pdf", "PDF路径": str(tmp_path / "book-a.pdf")}]
        first._save_state()

    second = server._BatchManager(controller_supervisor=UnavailableLeaseSupervisor())
    second._state_path = first._state_path
    try:
        second.restore_pending()
        observed = second.status()

        assert second._observer_only is True
        assert second._controller_lease is None
        assert second._thread is None
        assert observed["控制器角色"] == "观察者"
        assert observed["控制器所有者"] == "controller-one"
        assert observed["控制器租约状态"] == "观察中，后台重试"
        assert observed["控制器租约错误"].startswith("OperationalError:")
        assert observed["代理动作"]["requires_agent_action"] is False
    finally:
        second._observer_takeover_stop.set()
        with first._lock:
            first._release_controller_lease_locked()


def test_batch_state_write_is_rejected_after_controller_fencing_is_lost(tmp_path: Path) -> None:
    first = _isolated_batch_manager(tmp_path, "controller-one")
    second = _isolated_batch_manager(tmp_path, "controller-two")
    with first._lock:
        assert first._acquire_controller_lease_locked()
        first._running = True
        first._root = str(tmp_path)
        first._output_dir = str(tmp_path / "out")
        first._completed = ["first-snapshot"]
        first._save_state()
        stale_lease = first._controller_lease

    assert stale_lease is not None
    assert first._get_controller_supervisor().release_controller_lease(stale_lease)
    with second._lock:
        assert second._acquire_controller_lease_locked()
        second._running = True
        second._root = str(tmp_path)
        second._output_dir = str(tmp_path / "out")
        second._completed = ["new-controller-snapshot"]
        second._save_state()

    with first._lock:
        first._completed = ["stale-controller-write"]
        first._save_state()
        assert first._observer_only is True

    persisted = json.loads(first._state_path.read_text(encoding="utf-8"))
    assert persisted["已完成"] == ["new-controller-snapshot"]
    first._observer_takeover_stop.set()
    with second._lock:
        second._release_controller_lease_locked()


def test_resume_cache_replay_is_not_reported_as_live_ocr_throughput() -> None:
    assert server._looks_like_resume_cache_replay(153.0, 14.0) is True
    assert server._looks_like_resume_cache_replay(1.0, 55.0) is False
    assert server._looks_like_resume_cache_replay(8.0, 180.0) is False


def test_failed_book_is_removed_from_completed_ledger() -> None:
    manager = server._BatchManager()
    manager._completed = ["已完成书", "需纠正书"]

    manager._record_book_failure("需纠正书", "未完成（0/100页）")

    assert manager._completed == ["已完成书"]
    assert manager._failed == [{"书名": "需纠正书", "原因": "未完成（0/100页）"}]


def test_restored_batch_continues_after_existing_active_workers() -> None:
    manager = server._BatchManager()
    manager._current_index = 5
    manager._active_jobs = {
        "chemistry": {"索引": 4},
        "history": {"索引": 5},
    }

    assert manager._next_book_index() == 6


def test_batch_without_active_workers_keeps_saved_resume_index() -> None:
    manager = server._BatchManager()
    manager._current_index = 5

    assert manager._next_book_index() == 5


def test_batch_restarts_cancelled_active_job_before_admitting_new_book(
    tmp_path: Path,
    monkeypatch,
) -> None:
    job_dir = tmp_path / "书-rescue-result"
    job_dir.mkdir()
    (job_dir / "状态.json").write_text(json.dumps({"状态": "已取消"}), encoding="utf-8")
    source_pdf = tmp_path / "书.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")

    manager = server._BatchManager()
    manager._running = True
    manager._resume = True
    manager._mode = "book-fast"
    manager._active_jobs = {
        str(job_dir): {
            "索引": 5,
            "书名": "书",
            "任务目录": str(job_dir),
            "来源PDF": str(source_pdf),
            "线程预算": 4,
        }
    }
    calls: list[dict] = []

    class FakeTaskManager:
        def get_task_info(self, _job_dir: str) -> dict:
            return {"存活": False}

        def start_extraction(self, **kwargs):
            calls.append(kwargs)
            return str(job_dir), False

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())

    assert manager._resume_cancelled_active_jobs() is True
    assert calls == [{
        "path": str(source_pdf),
        "output_dir": str(job_dir.parent),
        "mode": "book-fast",
        "max_pages": None,
        "resume": True,
        "password": None,
        "ocr_threads": 2,
    }]


def test_resumed_worker_keeps_high_thread_budget_only_for_active_profile(monkeypatch) -> None:
    manager = server._BatchManager()
    record = {"线程预算": 4}
    monkeypatch.setattr(
        manager._throughput_profiles,
        "active_recommendation",
        lambda *, mode: {"threads_per_worker": 4},
    )

    assert manager._resume_thread_budget(record) == 4
    assert record["线程预算"] == 4


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
    manager._active_jobs = {
        str(job_dir): {
            "索引": 0,
            "书名": "书",
            "任务目录": str(job_dir),
            "来源PDF": str(tmp_path / "书.pdf"),
            "线程预算": 2,
        }
    }
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
        lambda pid: {
            "状态": "可用",
            "进程ID": pid,
            "CPU占用率": 12.0,
            "CPU等效核心数": 1.92,
            "线程CPU占用率": {"10": 100.0, "11": 92.0},
            "进程线程数": 27,
            "活跃CPU线程数": 2,
            "饱和CPU线程数": 2,
            "内存MB": 512.0,
            "内存占用率": 2.0,
            "运行内存占用MB": 512.0,
            "运行内存占整机比例": 2.0,
        },
    )

    manager._refresh_worker_supervision(list(manager._active_jobs.values()))
    result = manager.status()

    assert result["书籍名"] == "书"
    assert result["书本总数"] == 1
    assert result["书本完成数"] == 0
    assert result["书本失败数"] == 0
    assert result["书本待处理数"] == 0
    assert result["进行中书本数"] == 1
    assert result["总处理页数"] == 20
    assert result["处理进度"] == 25.0
    assert result["处理速度"] == 60.0
    assert result["当前书籍指标"]["资源占用率"]["CPU占用率"] == 12.0
    assert result["worker数"] == 1
    assert result["已取得PID的worker数"] == 1
    assert result["可采样worker数"] == 1
    assert result["worker总占用内存MB"] == 512.0
    assert result["worker总运行内存占用MB"] == 512.0
    assert result["worker总进程线程数"] == 27
    assert result["worker资源"][0]["进程线程数"] == 27
    assert result["worker资源"][0]["线程CPU占用率"] == {"10": 100.0, "11": 92.0}
    assert result["worker资源"][0]["运行内存占用MB"] == 512.0
    assert result["worker资源汇总"]["总OCR线程预算"] == 2
    assert result["worker资源汇总"]["总活跃CPU线程数"] == 2
    assert result["worker资源汇总"]["总饱和CPU线程数"] == 2
    assert result["worker资源汇总"]["总运行内存占用MB"] == 512.0
    assert result["worker资源汇总"]["采样完整"] is True


def test_batch_status_reports_actual_per_worker_page_rate(tmp_path: Path, monkeypatch) -> None:
    job_dir = tmp_path / "输出" / "农业化学卷-rescue-result"
    job_dir.mkdir(parents=True)
    status_path = job_dir / "状态.json"

    def write_status(processed: int) -> None:
        status_path.write_text(
            json.dumps(
                {
                    "状态": "进行中",
                    "来源PDF": str(tmp_path / "农业化学卷.pdf"),
                    "目标页数": 100,
                    "已处理页数": processed,
                    "开始时间": "2026-07-20T09:00:00",
                    "已耗时秒": 600,
                    "平均每页秒": 120.0,
                    "预计剩余秒": 9000,
                    "短窗OCR页每分钟": 3.0,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    write_status(5)
    heartbeat = {"completed": 5, "at": "2026-07-20T10:00:00"}

    class FakeTaskManager:
        def get_task_info(self, _job_dir: str) -> dict:
            return {
                "存活": True,
                "工作进程ID": 97531,
                "启动进程ID": 24680,
                "心跳": {
                    "存在": True,
                    "活跃": True,
                    "进程ID": 97531,
                    "最后完成页": heartbeat["completed"],
                    "最后进度时间": heartbeat["at"],
                },
            }

    manager = server._BatchManager()
    manager._books = [{"文件名": "农业化学卷.pdf", "PDF路径": str(tmp_path / "农业化学卷.pdf")}]
    manager._current_index = 0
    manager._current_job_dir = str(job_dir)
    manager._active_jobs = {
        str(job_dir): {
            "索引": 0,
            "书名": "农业化学卷",
            "任务目录": str(job_dir),
            "来源PDF": str(tmp_path / "农业化学卷.pdf"),
            "线程预算": 4,
        }
    }
    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda pid: {
            "状态": "可用",
            "进程ID": pid,
            "CPU占用率": 51.9,
            "CPU等效核心数": 8.31,
            "线程CPU占用率": {"10": 100.0},
            "进程线程数": 27,
            "活跃CPU线程数": 2,
            "饱和CPU线程数": 1,
            "内存MB": 512.0,
            "内存占用率": 3.5,
            "运行内存占用MB": 512.0,
            "运行内存占整机比例": 3.5,
        },
    )

    manager._refresh_worker_supervision(list(manager._active_jobs.values()))
    first = manager.status()
    assert first["worker资源"][0]["近期实际速度状态"] == "采样中"

    write_status(7)
    heartbeat.update({"completed": 7, "at": "2026-07-20T10:02:00"})
    manager._refresh_worker_supervision(list(manager._active_jobs.values()))
    second = manager.status()
    worker = second["worker资源"][0]

    assert worker["近期实际处理页每分钟"] == 1.0
    assert worker["近期实际处理秒每页"] == 60.0
    assert worker["处理速度"].startswith("1.000页/分钟")
    assert worker["OCR吞吐页每分钟"] == 3.0
    assert worker["实际Worker PID"] == 97531
    assert worker["实际Worker PID"] != 24680
    assert worker["OCR线程预算"] == 4
    assert worker["进程线程数"] == 27
    assert worker["CPU整机占比"] == 51.9
    assert worker["CPU等效核心"] == 8.31
    assert worker["RSS内存MB"] == 512.0

    samples_before = dict(manager._worker_progress_samples)
    supervision_before = dict(manager._worker_supervision)
    writes: list[object] = []
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda _pid: (_ for _ in ()).throw(AssertionError("status must use the supervision cache")),
    )
    monkeypatch.setattr(
        server._TaskManager,
        "_atomic_json",
        lambda *_args, **_kwargs: writes.append("write"),
    )

    cached = manager.status()

    assert cached["worker资源"][0]["近期实际处理页每分钟"] == 1.0
    assert manager._worker_progress_samples == samples_before
    assert manager._worker_supervision == supervision_before
    assert writes == []
    fixed = second["固定监测格式"]
    assert fixed["当前worker任务列表"][0]["标记"] == "-"
    assert fixed["实际Worker PID"] == [97531]
    assert fixed["CPU整机占比"] == 51.9
    assert fixed["RSS内存"] == 512.0


def test_batch_status_task_markers_and_remaining_book_count(tmp_path: Path, monkeypatch) -> None:
    books = ["已完成书", "运行书A", "运行书B", "失败书", "待处理书"]
    job_dirs: dict[str, Path] = {}
    for book_name in ("运行书A", "运行书B"):
        job_dir = tmp_path / f"{book_name}-rescue-result"
        job_dir.mkdir()
        (job_dir / "状态.json").write_text(
            json.dumps(
                {
                    "状态": "进行中",
                    "来源PDF": str(tmp_path / f"{book_name}.pdf"),
                    "目标页数": 20,
                    "已处理页数": 5,
                    "开始时间": "2026-07-20T10:00:00",
                    "更新时间": "2026-07-20T10:01:00",
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        job_dirs[book_name] = job_dir

    class FakeTaskManager:
        def get_task_info(self, job_dir: str) -> dict:
            pid = 1001 if "运行书A" in job_dir else 1002
            return {
                "存活": True,
                "工作进程ID": pid,
                "心跳": {
                    "存在": True,
                    "活跃": True,
                    "进程ID": pid,
                    "最后完成页": 5,
                    "最后进度时间": "2026-07-20T10:01:00",
                },
            }

    manager = server._BatchManager()
    manager._books = [
        {"文件名": f"{book_name}.pdf", "PDF路径": str(tmp_path / f"{book_name}.pdf")}
        for book_name in books
    ]
    manager._completed = ["已完成书"]
    manager._failed = [{"书名": "失败书", "原因": "测试失败"}]
    manager._active_jobs = {
        str(job_dirs["运行书A"]): {
            "索引": 1,
            "书名": "运行书A",
            "任务目录": str(job_dirs["运行书A"]),
            "线程预算": 2,
        },
        str(job_dirs["运行书B"]): {
            "索引": 2,
            "书名": "运行书B",
            "任务目录": str(job_dirs["运行书B"]),
            "线程预算": 3,
        },
    }
    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())
    monkeypatch.setattr(
        server,
        "collect_process_resource_usage",
        lambda pid: {
            "状态": "可用",
            "进程ID": pid,
            "CPU占用率": 10.0,
            "CPU等效核心数": 1.6,
            "线程CPU占用率": {},
            "进程线程数": 12,
            "活跃CPU线程数": 1,
            "饱和CPU线程数": 0,
            "内存MB": 256.0,
            "内存占用率": 1.5,
            "运行内存占用MB": 256.0,
            "运行内存占整机比例": 1.5,
        },
    )

    manager._refresh_worker_supervision(list(manager._active_jobs.values()))
    result = manager.status()
    markers = {item["书籍"]: item["标记"] for item in result["当前worker任务列表"]}

    assert result["书本完成数"] == 1
    assert result["书本失败数"] == 1
    assert result["书本待处理数"] == 1
    assert result["进行中书本数"] == 2
    assert result["剩余书本数量"] == 3
    assert markers == {
        "已完成书": "v",
        "运行书A": "-",
        "运行书B": "-",
        "失败书": "x",
        "待处理书": "○",
    }
    completed = next(item for item in result["当前worker任务列表"] if item["书籍"] == "已完成书")
    assert completed["实际Worker PID"] is None
    assert completed["RSS内存MB"] is None


def test_worker_resource_summary_preserves_missing_worker_samples() -> None:
    summary = server._worker_resource_summary(
        [
            {
                "进程ID": 101,
                "线程预算": 4,
                "资源占用率": {
                    "状态": "可用",
                    "CPU等效核心数": 1.5,
                    "内存MB": 800.0,
                    "进程线程数": 31,
                    "活跃CPU线程数": 4,
                    "饱和CPU线程数": 2,
                    "线程CPU占用率": {"1": 100.0},
                },
            },
            {
                "进程ID": 102,
                "线程预算": 2,
                "资源占用率": {
                    "状态": "可用",
                    "CPU等效核心数": 0.5,
                    "内存MB": 400.0,
                    "进程线程数": 19,
                    "活跃CPU线程数": 1,
                    "饱和CPU线程数": 0,
                    "线程CPU占用率": {"2": 50.0},
                },
            },
            {
                "进程ID": None,
                "线程预算": 2,
                "资源占用率": {"状态": "不可用", "进程线程数": None},
            },
        ],
        logical_cpu_count=16,
        total_memory_mb=16 * 1024,
    )

    assert summary["统计worker数"] == 3
    assert summary["已取得PID的worker数"] == 2
    assert summary["可采样worker数"] == 2
    assert summary["不可采样worker数"] == 1
    assert summary["线程采样worker数"] == 2
    assert summary["采样完整"] is False
    assert summary["总OCR线程预算"] == 8
    assert summary["总进程线程数"] == 50
    assert summary["总活跃CPU线程数"] == 5
    assert summary["总饱和CPU线程数"] == 2
    assert summary["总RSS内存MB"] == 1200.0
    assert summary["总内存占整机比例"] == 7.32
    assert summary["总CPU等效核心数"] == 2.0
    assert summary["总CPU占整机比例"] == 12.5


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


def test_background_task_is_live_during_startup_grace_before_first_heartbeat(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")

    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())

    job_dir, already_running = manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )
    health = manager.get_task_info(job_dir)

    assert already_running is False
    assert health and health["存活"] is True
    assert health["心跳"]["活跃"] is False
    assert health["监控阶段"] == "启动中"
    manager._stopping = True


def test_live_heartbeat_without_first_page_is_recovered_after_startup_timeout(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    job_dir = tmp_path / "输出" / "启动卡死书-rescue-result"
    job_dir.mkdir(parents=True)
    status_path = job_dir / "状态.json"
    heartbeat_path = job_dir / "后台任务心跳.json"
    cancel_path = job_dir / "停止请求.json"
    status_path.write_text('{"状态":"启动中"}', encoding="utf-8")
    heartbeat_path.write_text(
        json.dumps(
            {
                "状态": "运行中",
                "进程ID": 97531,
                "当前页": None,
                "最后完成页": None,
                "最后进度时间": None,
            }
        ),
        encoding="utf-8",
    )
    info = {
        "job_dir": str(job_dir),
        "source": str(tmp_path / "启动卡死书.pdf"),
        "started_at": time.time() - manager.STARTUP_PROGRESS_TIMEOUT - 1,
        "phase": "启动中",
        "heartbeat_path": str(heartbeat_path),
        "cancel_path": str(cancel_path),
        "metadata_path": str(job_dir / "后台任务.json"),
        "launcher_pid": None,
        "worker_pid": None,
        "process": None,
    }
    recovered: list[dict[str, object]] = []
    monkeypatch.setattr(
        manager,
        "_recover_stalled_locked",
        lambda _job_key, _info, heartbeat: recovered.append(heartbeat),
    )

    manager._watch_task_locked(str(job_dir), info)

    assert len(recovered) == 1
    assert recovered[0]["当前页"] is None
    assert recovered[0]["最后完成页"] is None
    manager._stopping = True


def test_new_worker_attempt_clears_stale_missing_engine_status(tmp_path: Path, monkeypatch) -> None:
    """A good retry must not report an old runtime failure during warm-up."""

    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    result_dir = tmp_path / "输出" / "扫描书-rescue-result"
    result_dir.mkdir(parents=True)
    (result_dir / "状态.json").write_text(
        json.dumps(
            {
                "状态": "未完成",
                "引擎": "无可用OCR引擎",
                "失败原因": "previous runtime missing",
                "失败时间": "2026-07-20T13:00:00",
                "已处理页数": 12,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())

    job_dir, already_running = manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )

    status = json.loads((Path(job_dir) / "状态.json").read_text(encoding="utf-8"))
    assert already_running is False
    assert status["状态"] == "启动中"
    assert status["引擎"] == "待worker确认"
    assert status["已处理页数"] == 12
    assert "失败原因" not in status
    assert "失败时间" not in status
    manager._stopping = True


def test_cancelled_task_restarts_despite_reused_heartbeat_pid(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    first_manager = server._TaskManager()
    job_dir, first_already_running = first_manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )
    result_dir = Path(job_dir)
    (result_dir / "状态.json").write_text('{"状态":"已取消"}', encoding="utf-8")
    (result_dir / "后台任务心跳.json").write_text(
        '{"状态":"已取消","进程ID":97531}', encoding="utf-8"
    )
    monkeypatch.setattr(
        server._TaskManager,
        "_pid_alive",
        staticmethod(lambda _pid: True),
    )

    restarted_manager = server._TaskManager()
    restarted_job_dir, restarted_already_running = restarted_manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )

    assert first_already_running is False
    assert restarted_already_running is False
    assert restarted_job_dir == job_dir
    assert len(calls) == 2
    first_manager._stopping = True
    restarted_manager._stopping = True


def test_durable_cancelled_orphan_is_requeued_for_explicit_resume(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv("PDF_RESCUE_RUNTIME_ROOT", str(tmp_path / "runtime-layout"))
    database_path = tmp_path / "runtime" / "tasks.sqlite3"
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    first_supervisor = server.LocalSupervisor(database_path=database_path, owner_id="old-owner")
    first_manager = server._TaskManager(enable_durable_supervision=True, supervisor=first_supervisor)
    job_dir, _ = first_manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )
    first_info = first_manager._tasks[job_dir]
    first_context = first_info["supervision_context"]
    first_supervisor.store.release_lease(
        first_context.lease.resource_key,
        owner_id=first_supervisor.owner_id,
        token=first_context.lease.token,
    )
    result_dir = Path(job_dir)
    (result_dir / "状态.json").write_text('{"状态":"已取消"}', encoding="utf-8")

    resumed_supervisor = server.LocalSupervisor(database_path=database_path, owner_id="new-owner")
    resumed_manager = server._TaskManager(enable_durable_supervision=True, supervisor=resumed_supervisor)
    resumed_job_dir, already_running = resumed_manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )

    task_id = first_info["持久任务ID"]
    assert resumed_job_dir == job_dir
    assert already_running is False
    assert len(calls) == 2
    assert resumed_supervisor.store.get_task(task_id).state == "running"
    assert resumed_supervisor.store.get_task(task_id).latest_attempt_number == 2
    first_manager._stopping = True
    resumed_manager._stopping = True


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


def test_background_task_passes_profile_thread_and_warmup_environment(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    manager = server._TaskManager()
    source = tmp_path / "capacity-sample.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    manager.start_extraction(
        str(source),
        output_dir=str(tmp_path / "output"),
        mode="book-fast",
        max_pages=10,
        resume=False,
        password=None,
        ocr_threads=4,
        ocr_profile_warmup_pages=2,
    )

    environment = calls[0][1]["env"]
    assert environment["PDF_RESCUE_OCR_THREADS"] == "4"
    assert environment["PDF_RESCUE_OCR_PROFILE_WARMUP_PAGES"] == "2"
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


def test_restored_monitor_retries_after_old_task_lease_expires(tmp_path: Path, monkeypatch) -> None:
    """A controller restart must eventually regain supervision without touching OCR."""
    monkeypatch.setenv("PDF_RESCUE_RUNTIME_ROOT", str(tmp_path / "runtime-layout"))
    database_path = tmp_path / "runtime" / "tasks.sqlite3"
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    monkeypatch.setattr(server.subprocess, "Popen", lambda *_args, **_kwargs: FakeProcess())

    first_supervisor = LocalSupervisor(database_path=database_path, owner_id="old-owner")
    first = server._TaskManager(enable_durable_supervision=True, supervisor=first_supervisor)
    job_dir, already_running = first.start_extraction(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-fast",
        max_pages=None,
        resume=True,
        password=None,
    )
    assert already_running is False

    second_supervisor = LocalSupervisor(database_path=database_path, owner_id="new-owner")
    second = server._TaskManager(enable_durable_supervision=True, supervisor=second_supervisor)
    second.restore_pending()
    assert job_dir not in second._tasks
    assert job_dir in second._pending_restores

    old_context = first._tasks[job_dir]["supervision_context"]
    assert first_supervisor.store.release_lease(
        old_context.lease.resource_key,
        owner_id=first_supervisor.owner_id,
        token=old_context.lease.token,
    )
    with second._lock:
        second._try_reattach_pending_locked()

    assert job_dir in second._tasks
    assert job_dir not in second._pending_restores
    first._stopping = True
    second._stopping = True


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


def test_primary_tool_does_not_launch_a_known_unavailable_ocr_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        server,
        "run_plan_pdf_job",
        lambda *_args, **_kwargs: {**_plan("ocr_required", 0), "engine": "none"},
    )

    class FakeTaskManager:
        def start_extraction(self, *_args, **_kwargs):
            raise AssertionError("a known unavailable OCR runtime must not create a worker")

    monkeypatch.setattr(server, "_task_manager", FakeTaskManager())

    result = asyncio.run(server.rescue_pdf(path="D:/books/scan.pdf", request="extract this scan"))

    assert result["状态"] == "OCR运行环境不可用"
    assert result["结果"]["error_code"] == "ocr_runtime_unavailable"
    assert result["结果"]["requires_user_action"] is True


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


def test_primary_tool_accepts_english_lifecycle_intent() -> None:
    assert server._infer_workflow("auto", "resume the task", None, "D:/out/job", None) == "resume"
    assert server._infer_workflow("auto", "show page 5", None, "D:/out/job", None) == "evidence"
    assert server._infer_workflow("auto", "audit quality", None, "D:/out/job", None) == "audit"
    assert server._infer_workflow(
        "auto", "check whether this PDF needs OCR", "D:/books/scan.pdf", None, None
    ) == "diagnose"


def test_primary_tool_starts_directory_as_a_background_batch(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    class FakeBatchManager:
        def start_batch(self, **kwargs):
            calls.append(kwargs)
            return {"状态": "准备中"}

    monkeypatch.setattr(server, "_batch_manager", FakeBatchManager())
    monkeypatch.setattr(
        server,
        "run_plan_pdf_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("directory must not synchronously plan a PDF")),
    )

    result = asyncio.run(server.rescue_pdf(path=str(tmp_path), request="extract all PDFs"))

    assert result["状态"] == "已提交批量后台任务"
    assert calls == [{
        "root": str(tmp_path),
        "output_dir": None,
        "mode": "book-balanced",
        "max_books": None,
        "max_pages_per_book": None,
        "resume": True,
    }]
    assert result["next_call"] == {
        "tool": "rescue_pdf",
        "arguments": {"path": str(tmp_path), "workflow": "status"},
        "read_only": True,
    }


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
    calls: list[str] = []
    monkeypatch.setattr(
        server,
        "get_job_status",
        lambda job_dir: (
            calls.append(job_dir)
            or {"工作进程健康": {"工作进程ID": 12345}, "任务目录": job_dir}
        ),
    )

    result = asyncio.run(server.rescue_pdf(job_dir="D:/out/book-rescue-result", request="现在处理进度怎么样"))

    assert result["状态"] == "已读取任务状态"
    assert result["已执行"] == ["查看任务状态"]
    assert calls == ["D:/out/book-rescue-result"]
    assert result["结果"]["工作进程健康"]["工作进程ID"] == 12345


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
