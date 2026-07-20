"""Resource-aware OCR worker planning.

The scheduler deliberately makes a conservative decision.  OCR workers are
separate processes and a single PaddleOCR process can consume several logical
cores and a large resident set.  We therefore use both system headroom and
per-worker samples before opening another book task.  A missing sample never
pretends that more capacity is available.
"""

from __future__ import annotations

import math
import os
import time
from dataclasses import asdict, dataclass, replace
from typing import Iterable, Mapping

import psutil


MIN_OCR_THREADS_PER_WORKER = 1
MAX_OCR_THREADS_PER_WORKER = 4


def _positive_float(value: object, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) and number > 0 else default


def _positive_int(value: object, default: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return number if number > 0 else default


def worker_threads_for_pages(
    page_count: int | None,
    *,
    capacity_threads: int,
    available_thread_slots: int | None = None,
    allow_high_thread_budget: bool = False,
) -> int:
    """Choose a 1–4 thread budget for a *new* worker.

    Page count expresses how worthwhile a longer-lived worker is; it never
    overrides live CPU capacity.  Existing PaddleOCR workers are intentionally
    not hot-reconfigured because their native runtime owns the thread pool.
    """
    try:
        pages = max(0, int(page_count or 0))
    except (TypeError, ValueError):
        pages = 0
    # Page count alone does not justify extra OCR threads.  On the current
    # 16-logical-thread host, measured 2-thread throughput is equivalent to
    # 4-thread throughput, so keep every normal worker at <=2 threads.  An
    # explicitly activated, hardware-matched throughput profile may opt into
    # the old 3/4-thread page bands.
    if pages >= 80:
        page_budget = 2
        if allow_high_thread_budget:
            if pages >= 400:
                page_budget = 4
            elif pages >= 200:
                page_budget = 3
    else:
        page_budget = 1

    capacity = max(MIN_OCR_THREADS_PER_WORKER, int(capacity_threads))
    if available_thread_slots is not None:
        capacity = min(capacity, max(MIN_OCR_THREADS_PER_WORKER, int(available_thread_slots)))
    return max(
        MIN_OCR_THREADS_PER_WORKER,
        min(MAX_OCR_THREADS_PER_WORKER, page_budget, capacity),
    )


def _cpu_seconds(value: object) -> float:
    """Read either process or thread CPU time without relying on psutil internals."""
    return float(getattr(value, "user", getattr(value, "user_time", 0.0))) + float(
        getattr(value, "system", getattr(value, "system_time", 0.0))
    )


@dataclass(frozen=True)
class ProcessCpuSample:
    """CPU observation expressed in user-facing, bounded units.

    ``cpu_percent`` is normalized to the total logical CPU capacity of the
    machine, so it is always in the 0–100 range.  Multi-threaded process CPU
    time is retained as ``cpu_core_equivalents`` instead of being misleadingly
    rendered as a percentage above 100.
    """

    cpu_percent: float | None
    cpu_core_equivalents: float | None
    thread_cpu_percent: dict[str, float] | None
    sample_window_seconds: float | None


def sample_process_cpu_usage(
    process: psutil.Process,
    *,
    interval: float = 0.2,
    logical_cpu_count: int | None = None,
) -> ProcessCpuSample:
    """Sample one process and its individual threads over a short window.

    A psutil process can legitimately report more than 100% when it consumes
    several logical cores.  This helper normalizes the process figure to the
    whole machine and clamps each individual OS thread to 100%, eliminating
    timer-resolution spikes from the thread display on Windows.
    """
    sample_interval = max(0.05, min(1.0, float(interval)))
    logical_count = max(
        1,
        int(logical_cpu_count or psutil.cpu_count(logical=True) or os.cpu_count() or 1),
    )
    before_process = _cpu_seconds(process.cpu_times())
    try:
        before_threads = {str(item.id): _cpu_seconds(item) for item in process.threads()}
    except (OSError, psutil.Error):
        before_threads = None

    started_at = time.perf_counter()
    time.sleep(sample_interval)
    elapsed = max(0.01, time.perf_counter() - started_at)
    after_process = _cpu_seconds(process.cpu_times())

    core_equivalents = max(0.0, after_process - before_process) / elapsed
    process_percent = min(100.0, core_equivalents / logical_count * 100.0)

    thread_percent: dict[str, float] | None = None
    if before_threads is not None:
        try:
            after_threads = {str(item.id): _cpu_seconds(item) for item in process.threads()}
            thread_percent = {
                thread_id: round(
                    min(100.0, max(0.0, after_threads[thread_id] - before_cpu) / elapsed * 100.0),
                    1,
                )
                for thread_id, before_cpu in sorted(before_threads.items(), key=lambda item: int(item[0]))
                if thread_id in after_threads
            }
        except (OSError, psutil.Error):
            thread_percent = None

    return ProcessCpuSample(
        cpu_percent=round(process_percent, 1),
        cpu_core_equivalents=round(core_equivalents, 2),
        thread_cpu_percent=thread_percent,
        sample_window_seconds=round(elapsed, 3),
    )


@dataclass(frozen=True)
class WorkerResource:
    """Live worker resource usage.

    ``cpu_percent`` means its share of all logical CPUs, never cumulative
    multi-core process percent.  See ``cpu_core_equivalents`` for the latter.
    """

    pid: int
    cpu_percent: float | None
    memory_mb: float | None
    memory_percent: float | None
    status: str
    thread_cpu_percent: dict[str, float] | None = None
    cpu_core_equivalents: float | None = None
    thread_sample_window_seconds: float | None = None
    configured_threads: int | None = None
    active_thread_count: int | None = None
    saturated_thread_count: int | None = None
    configured_thread_utilization_percent: float | None = None
    max_thread_cpu_percent: float | None = None


@dataclass(frozen=True)
class WorkerPlan:
    """A serializable concurrency decision and its evidence."""

    target_workers: int
    hard_limit: int
    active_workers: int
    cpu_count: int
    physical_core_count: int
    system_cpu_percent: float | None
    total_memory_gb: float
    available_memory_gb: float
    reserve_memory_gb: float
    memory_per_worker_gb: float
    threads_per_worker: int
    workers: tuple[WorkerResource, ...]
    reason: str
    next_worker_threads: int | None = None
    used_saturated_threads: int = 0
    used_thread_slots: int = 0
    available_thread_slots: int = 0
    worker_available_thread_slots: int = 0
    system_available_thread_slots: int | None = None
    additional_memory_slots: int = 0
    external_cpu_percent: float | None = None
    throughput_pages_per_minute: float | None = None
    tuning_profile_id: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["workers"] = [asdict(worker) for worker in self.workers]
        return payload


class ResourceScheduler:
    """Plan OCR concurrency from live CPU, memory and worker observations.

    ``max_workers`` is an operator cap.  When omitted, a small safe cap is
    derived from physical cores and the platform's available memory.  The
    scheduler can increase concurrency only when the current system and all
    observed workers have headroom; it never stops a running worker merely
    because a later sample is busy.
    """

    def __init__(
        self,
        *,
        max_workers: int | None = None,
        reserve_memory_gb: float | None = None,
        memory_per_worker_gb: float | None = None,
        cores_per_worker: int = 2,
        absolute_cap: int = 4,
        reserve_threads: int = 2,
    ) -> None:
        env_cap = os.environ.get("PDF_RESCUE_MAX_WORKERS")
        env_reserve = os.environ.get("PDF_RESCUE_RESERVE_MEMORY_GB")
        env_worker_memory = os.environ.get("PDF_RESCUE_MEMORY_PER_WORKER_GB")
        self.max_workers = _positive_int(max_workers or env_cap, absolute_cap)
        self.reserve_memory_gb = _positive_float(
            reserve_memory_gb if reserve_memory_gb is not None else env_reserve,
            2.0,
        )
        self.memory_per_worker_gb = _positive_float(
            memory_per_worker_gb if memory_per_worker_gb is not None else env_worker_memory,
            2.0,
        )
        self.cores_per_worker = max(1, int(cores_per_worker))
        self.absolute_cap = max(1, int(absolute_cap))
        self.reserve_threads = max(0, int(reserve_threads))

    @staticmethod
    def _with_thread_metrics(worker: WorkerResource, configured_threads: int | None) -> WorkerResource:
        """Summarize per-thread evidence without hiding the raw thread map."""
        thread_values = list((worker.thread_cpu_percent or {}).values())
        if not thread_values:
            return replace(worker, configured_threads=configured_threads)
        active = sum(value >= 5.0 for value in thread_values)
        saturated = sum(value >= 80.0 for value in thread_values)
        budget = configured_threads or len(thread_values)
        top_threads = sorted(thread_values, reverse=True)[: max(1, budget)]
        utilization = min(100.0, sum(top_threads) / max(1, budget))
        return replace(
            worker,
            configured_threads=configured_threads,
            active_thread_count=active,
            saturated_thread_count=saturated,
            configured_thread_utilization_percent=round(utilization, 1),
            max_thread_cpu_percent=round(max(thread_values), 1),
        )

    @staticmethod
    def _worker_resource(pid: int) -> WorkerResource:
        try:
            process = psutil.Process(int(pid))
            if not process.is_running():
                return WorkerResource(pid=int(pid), cpu_percent=None, memory_mb=None, memory_percent=None, status="已退出")
            sample = sample_process_cpu_usage(process)
            memory = process.memory_info()
            return WorkerResource(
                pid=int(pid),
                cpu_percent=sample.cpu_percent,
                memory_mb=round(memory.rss / (1024 * 1024), 1),
                memory_percent=round(float(process.memory_percent()), 2),
                status="可用",
                thread_cpu_percent=sample.thread_cpu_percent,
                cpu_core_equivalents=sample.cpu_core_equivalents,
                thread_sample_window_seconds=sample.sample_window_seconds,
            )
        except (OSError, ValueError, psutil.Error):
            return WorkerResource(pid=int(pid), cpu_percent=None, memory_mb=None, memory_percent=None, status="不可用")

    def plan(
        self,
        worker_pids: Iterable[int] = (),
        *,
        worker_thread_budgets: Mapping[int, int] | None = None,
        preferred_workers: int | None = None,
        preferred_threads_per_worker: int | None = None,
        throughput_pages_per_minute: float | None = None,
        tuning_profile_id: str | None = None,
    ) -> WorkerPlan:
        cpu_count = max(1, int(psutil.cpu_count(logical=True) or os.cpu_count() or 1))
        physical_count = max(1, int(psutil.cpu_count(logical=False) or max(1, cpu_count // 2)))
        memory = psutil.virtual_memory()
        total_memory_gb = round(memory.total / (1024**3), 2)
        available_bytes = getattr(memory, "available", memory.total)
        available_memory_gb = round(available_bytes / (1024**3), 2)
        try:
            system_cpu = round(float(psutil.cpu_percent(interval=0.05)), 1)
        except (OSError, psutil.Error):
            system_cpu = None

        pids = tuple(dict.fromkeys(int(pid) for pid in worker_pids if int(pid) > 0))
        thread_budgets = {
            int(pid): _positive_int(value, 1)
            for pid, value in (worker_thread_budgets or {}).items()
            if int(pid) > 0
        }
        workers = tuple(
            self._with_thread_metrics(self._worker_resource(pid), thread_budgets.get(pid))
            for pid in pids
        )
        active_workers = len(workers)
        # ``available_memory_gb`` is the memory left *after* current workers
        # have started.  It therefore describes how many **additional** workers
        # may be admitted, not the total worker count.  Treating it as a total
        # cap made an already-running 2-worker batch permanently unable to add
        # a third worker even when it still had one full worker's reserve.
        additional_memory_slots = int(
            max(0.0, available_memory_gb - self.reserve_memory_gb) // self.memory_per_worker_gb
        )
        worker_cap = min(self.max_workers, self.absolute_cap)
        admission_limit = min(worker_cap, active_workers + additional_memory_slots)
        hard_limit = max(active_workers, admission_limit)
        safe_default_threads = max(
            MIN_OCR_THREADS_PER_WORKER,
            min(MAX_OCR_THREADS_PER_WORKER, physical_count // max(1, hard_limit)),
        )
        requested_threads = _positive_int(preferred_threads_per_worker, safe_default_threads)
        requested_threads = max(
            MIN_OCR_THREADS_PER_WORKER,
            min(MAX_OCR_THREADS_PER_WORKER, physical_count, requested_threads),
        )
        requested_workers = (
            min(_positive_int(preferred_workers, hard_limit), hard_limit)
            if hard_limit > 0
            else 0
        )

        # Thread slots, not a generic process CPU threshold, are the primary
        # capacity signal.  A worker is charged for the larger of its configured
        # thread budget and observed active thread count; sampling an unexpected
        # extra active thread therefore cannot be hidden by a lower budget.  This
        # makes a 16-logical-thread host reason about actual occupied worker
        # threads rather than treating one 100%-busy thread as a blanket ban on
        # another worker.
        used_saturated_threads = sum(
            min(
                worker.configured_threads or worker.saturated_thread_count or 0,
                worker.saturated_thread_count or 0,
            )
            for worker in workers
        )
        used_thread_slots = sum(
            max(
                worker.configured_threads or 0,
                worker.active_thread_count or 0,
                worker.saturated_thread_count or 0,
            )
            if worker.thread_cpu_percent
            else (
                worker.configured_threads
                or int(
                    math.ceil(
                        worker.cpu_core_equivalents
                        if worker.cpu_core_equivalents is not None
                        else max(0.0, (worker.cpu_percent or 0.0) / 100.0 * cpu_count)
                    )
                )
            )
            for worker in workers
        )
        reserve_logical = min(max(0, cpu_count - 1), self.reserve_threads)
        worker_available_thread_slots = max(0, cpu_count - reserve_logical - used_thread_slots)

        # A worker's thread budget is not the entire machine load: it tells us
        # how much OCR capacity is already reserved, while the system sample
        # tells us how much capacity is actually left for *all* processes.
        # Both constraints must leave room for a new worker.  In particular,
        # do not turn ``external_cpu_percent >= 25`` into a permanent veto:
        # on a 16-thread host, a 60% system load still has four usable logical
        # threads after the two system-reserve threads are protected.
        system_available_thread_slots: int | None = None
        if system_cpu is not None:
            busy_logical_threads = cpu_count * system_cpu / 100.0
            system_available_thread_slots = max(
                0,
                int(math.floor(cpu_count - reserve_logical - busy_logical_threads)),
            )
        available_thread_slots = min(
            worker_available_thread_slots,
            system_available_thread_slots
            if system_available_thread_slots is not None
            else worker_available_thread_slots,
        )
        additional_by_thread = available_thread_slots // requested_threads
        thread_target = max(active_workers, active_workers + additional_by_thread)
        target = max(active_workers, min(hard_limit, requested_workers, max(1, thread_target)))
        reasons: list[str] = []

        worker_cores = sum(
            worker.cpu_core_equivalents
            if worker.cpu_core_equivalents is not None
            else max(0.0, (worker.cpu_percent or 0.0) / 100.0 * cpu_count)
            for worker in workers
        )
        external_cpu_percent = None
        if system_cpu is not None:
            system_cores = system_cpu / 100.0 * cpu_count
            external_cpu_percent = round(max(0.0, system_cores - worker_cores) / cpu_count * 100.0, 1)
        # Whole-machine CPU is a guardrail, but it is combined with the live
        # OCR-thread reservation above instead of being a fixed external-load
        # cutoff.  Every scheduler pass therefore reopens admission as soon as
        # the measured total headroom can hold the next worker's budget.
        if system_available_thread_slots is not None and available_thread_slots < requested_threads:
            external_detail = (
                f"，外部CPU负载约 {external_cpu_percent:.1f}%"
                if external_cpu_percent is not None
                else ""
            )
            if active_workers:
                reasons.append(
                    f"整机CPU仅剩 {system_available_thread_slots} 个可用线程槽"
                    f"{external_detail}，不足以启动 {requested_threads} 线程worker"
                )
            elif admission_limit > 0:
                # A fresh batch has no worker sample yet.  Keep one baseline
                # worker so later passes can observe actual per-thread usage.
                reasons.append(
                    f"整机CPU仅剩 {system_available_thread_slots} 个可用线程槽"
                    f"{external_detail}，仅启动1个基线worker"
                )
        if available_memory_gb <= self.reserve_memory_gb:
            target = min(target, active_workers)
            reasons.append("可用内存已接近保留水位")
        if used_saturated_threads:
            reasons.append(
                f"已观测到 {used_saturated_threads} 个饱和OCR线程、{used_thread_slots} 个已占用线程槽，"
                f"下一worker预算为 {requested_threads} 线程"
            )
        if used_thread_slots >= max(1, cpu_count - reserve_logical):
            reasons.append("worker CPU线程槽已接近容量，暂不扩容")
        if preferred_threads_per_worker is not None:
            reasons.append("使用已激活吞吐配置的线程预算")
        if active_workers >= hard_limit:
            reasons.append(f"当前worker数 {active_workers} 已达到容量上限 {hard_limit}")
        if not reasons:
            reasons.append("线程槽、内存和外部负载均有余量，允许按容量上限调度")

        # The budget applies only to newly launched workers.  Existing workers
        # remain untouched so Paddle adapter state is never hot-reconfigured.
        threads_per_worker = requested_threads
        return WorkerPlan(
            target_workers=target,
            hard_limit=hard_limit,
            active_workers=active_workers,
            cpu_count=cpu_count,
            physical_core_count=physical_count,
            system_cpu_percent=system_cpu,
            total_memory_gb=total_memory_gb,
            available_memory_gb=available_memory_gb,
            reserve_memory_gb=self.reserve_memory_gb,
            memory_per_worker_gb=self.memory_per_worker_gb,
            threads_per_worker=threads_per_worker,
            workers=workers,
            reason="；".join(reasons),
            next_worker_threads=threads_per_worker,
            used_saturated_threads=used_saturated_threads,
            used_thread_slots=used_thread_slots,
            available_thread_slots=available_thread_slots,
            worker_available_thread_slots=worker_available_thread_slots,
            system_available_thread_slots=system_available_thread_slots,
            additional_memory_slots=additional_memory_slots,
            external_cpu_percent=external_cpu_percent,
            throughput_pages_per_minute=throughput_pages_per_minute,
            tuning_profile_id=tuning_profile_id,
        )


def plan_ocr_workers(
    worker_pids: Iterable[int] = (),
    *,
    max_workers: int | None = None,
) -> WorkerPlan:
    """Convenience API for status tools and tests."""
    return ResourceScheduler(max_workers=max_workers).plan(worker_pids)
