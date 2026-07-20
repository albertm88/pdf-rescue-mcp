from __future__ import annotations

import pdf_rescue_mcp.resource_scheduler as scheduler_module
from pdf_rescue_mcp.resource_scheduler import (
    ResourceScheduler,
    WorkerResource,
    sample_process_cpu_usage,
    worker_threads_for_pages,
)


def test_resource_scheduler_uses_cpu_and_memory_headroom(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 30.0)
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(lambda pid: WorkerResource(pid, 20.0, 800.0, 5.0, "可用")),
    )

    plan = ResourceScheduler(max_workers=4).plan([101])

    assert plan.target_workers == 4
    assert plan.hard_limit == 4
    assert plan.threads_per_worker == 2
    assert plan.workers[0].pid == 101


def test_resource_scheduler_does_not_expand_when_cpu_is_busy(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 92.0)
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(lambda pid: WorkerResource(pid, 96.0, 2400.0, 15.0, "可用")),
    )

    plan = ResourceScheduler(max_workers=4).plan([101])

    assert plan.target_workers == 1
    assert "CPU" in plan.reason


def test_scheduler_counts_available_memory_as_additional_worker_capacity(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 5.16 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 20.0)
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(lambda pid: WorkerResource(pid, 10.0, 800.0, 5.0, "可用")),
    )

    plan = ResourceScheduler(max_workers=4).plan(
        [101, 102],
        worker_thread_budgets={101: 4, 102: 4},
    )

    assert plan.additional_memory_slots == 1
    assert plan.hard_limit == 3
    assert plan.target_workers == 3
    assert plan.threads_per_worker == 2


def test_worker_threads_follow_page_band_without_exceeding_live_capacity() -> None:
    assert worker_threads_for_pages(20, capacity_threads=4, available_thread_slots=4) == 1
    assert worker_threads_for_pages(120, capacity_threads=4, available_thread_slots=4) == 2
    assert worker_threads_for_pages(300, capacity_threads=4, available_thread_slots=4) == 2
    assert worker_threads_for_pages(500, capacity_threads=4, available_thread_slots=4) == 2
    assert worker_threads_for_pages(
        300, capacity_threads=4, available_thread_slots=4, allow_high_thread_budget=True
    ) == 3
    assert worker_threads_for_pages(
        500, capacity_threads=4, available_thread_slots=4, allow_high_thread_budget=True
    ) == 4
    assert worker_threads_for_pages(500, capacity_threads=2, available_thread_slots=6) == 2
    assert worker_threads_for_pages(500, capacity_threads=4, available_thread_slots=1) == 1


def test_scheduler_keeps_one_baseline_worker_for_fresh_high_external_cpu(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 92.0)

    plan = ResourceScheduler(max_workers=4).plan()

    assert plan.hard_limit == 4
    assert plan.target_workers == 1
    assert plan.external_cpu_percent == 92.0
    assert "基线worker" in plan.reason


def test_resource_scheduler_does_not_treat_one_saturated_thread_as_global_limit(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 30.0)
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(
            lambda pid: WorkerResource(
                pid,
                40.0,
                800.0,
                5.0,
                "可用",
                {"ocr-thread": 99.0},
            )
        ),
    )

    plan = ResourceScheduler(max_workers=4).plan([101])

    assert plan.target_workers == 4
    assert plan.workers[0].thread_cpu_percent == {"ocr-thread": 99.0}
    assert plan.used_saturated_threads == 1
    assert plan.used_thread_slots == 1
    assert plan.worker_available_thread_slots == 13
    assert plan.system_available_thread_slots == 9
    assert plan.available_thread_slots == 9


def test_scheduler_allows_profiled_four_thread_worker_after_six_busy_threads(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 45.0)
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(
            lambda pid: WorkerResource(
                pid,
                37.5,
                1200.0,
                7.0,
                "可用",
                {str(index): 100.0 for index in range(6)},
                6.0,
            )
        ),
    )

    plan = ResourceScheduler(max_workers=4).plan(
        [101],
        worker_thread_budgets={101: 6},
        preferred_workers=2,
        preferred_threads_per_worker=4,
        throughput_pages_per_minute=18.5,
        tuning_profile_id="profile-1",
    )

    assert plan.target_workers == 2
    assert plan.next_worker_threads == 4
    assert plan.used_saturated_threads == 6
    assert plan.used_thread_slots == 6
    assert plan.worker_available_thread_slots == 8
    assert plan.system_available_thread_slots == 6
    assert plan.available_thread_slots == 6
    assert plan.throughput_pages_per_minute == 18.5
    assert plan.tuning_profile_id == "profile-1"


def test_scheduler_reopens_worker_admission_after_system_load_declines(monkeypatch) -> None:
    """A high external load may defer work, but cannot latch the batch at 2 workers."""

    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 12 * 1024**3})(),
    )
    cpu_samples = iter((90.0, 60.0))
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: next(cpu_samples))
    monkeypatch.setattr(
        ResourceScheduler,
        "_worker_resource",
        staticmethod(
            lambda pid: WorkerResource(
                pid,
                12.5,
                800.0,
                5.0,
                "可用",
                {"ocr-a": 100.0, "ocr-b": 100.0},
                2.0,
            )
        ),
    )

    scheduler = ResourceScheduler(max_workers=4)
    busy_plan = scheduler.plan([101, 102], worker_thread_budgets={101: 2, 102: 2})
    recovered_plan = scheduler.plan([101, 102], worker_thread_budgets={101: 2, 102: 2})

    assert busy_plan.target_workers == 2
    assert busy_plan.available_thread_slots == 0
    # External load is still 35%, but four total threads remain after the
    # system reserve; the next pass must automatically admit two 2-thread workers.
    assert recovered_plan.external_cpu_percent == 35.0
    assert recovered_plan.system_available_thread_slots == 4
    assert recovered_plan.target_workers == 4


def test_scheduler_does_not_force_first_worker_below_memory_reserve(monkeypatch) -> None:
    monkeypatch.setattr(scheduler_module.psutil, "cpu_count", lambda logical=True: 16 if logical else 8)
    monkeypatch.setattr(
        scheduler_module.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3, "available": 1 * 1024**3})(),
    )
    monkeypatch.setattr(scheduler_module.psutil, "cpu_percent", lambda interval=0.05: 10.0)

    plan = ResourceScheduler(max_workers=4).plan()

    assert plan.target_workers == 0
    assert plan.hard_limit == 0
    assert "内存" in plan.reason


def test_process_cpu_sample_normalizes_process_and_bounds_each_thread(monkeypatch) -> None:
    class CpuTimes:
        def __init__(self, user: float, system: float = 0.0) -> None:
            self.user = user
            self.system = system

    class ThreadTimes:
        def __init__(self, thread_id: int, user_time: float) -> None:
            self.id = thread_id
            self.user_time = user_time
            self.system_time = 0.0

    class FakeProcess:
        def __init__(self) -> None:
            self.cpu_calls = 0
            self.thread_calls = 0

        def cpu_times(self):
            self.cpu_calls += 1
            return CpuTimes(10.0 if self.cpu_calls == 1 else 10.4)

        def threads(self):
            self.thread_calls += 1
            if self.thread_calls == 1:
                return [ThreadTimes(11, 1.0), ThreadTimes(12, 2.0)]
            return [ThreadTimes(11, 1.2), ThreadTimes(12, 2.3)]

    ticks = iter((100.0, 100.2))
    monkeypatch.setattr(scheduler_module.time, "perf_counter", lambda: next(ticks))
    monkeypatch.setattr(scheduler_module.time, "sleep", lambda _seconds: None)

    sample = sample_process_cpu_usage(FakeProcess(), interval=0.2, logical_cpu_count=4)

    assert sample.cpu_core_equivalents == 2.0
    assert sample.cpu_percent == 50.0
    assert sample.thread_cpu_percent == {"11": 100.0, "12": 100.0}
    assert sample.sample_window_seconds == 0.2
