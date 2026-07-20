"""Non-blocking, isolated OCR capacity profiling.

The benchmark runs only while the machine has no production OCR process.  It
creates private, non-overlapping PDF fixtures in the runtime cache, then uses
ordinary supervised OCR workers so the MCP process stays responsive.  Results
are evidence for the iteration layer; activation remains an explicit action.
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any

import fitz
import psutil

from .library_pipeline import _read_status
from .resource_scheduler import ResourceScheduler
from .runtime import collect_process_resource_usage
from .runtime_paths import ensure_runtime_paths
from .throughput_tuning import (
    CapacityCandidate,
    ThroughputProfileStore,
    build_capacity_candidates,
    hardware_fingerprint,
)


TaskStarter = Callable[..., tuple[str, bool]]
TaskInfoReader = Callable[[str], dict[str, Any] | None]
TaskCanceller = Callable[[str], object]


def _percentile(values: Iterable[float], percentile: float) -> float | None:
    samples = sorted(float(value) for value in values)
    if not samples:
        return None
    position = (len(samples) - 1) * max(0.0, min(1.0, percentile))
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return round(samples[low], 3)
    return round(samples[low] + (samples[high] - samples[low]) * (position - low), 3)


def find_live_ocr_processes(
    *,
    excluded_pids: set[int] | None = None,
    excluded_path_prefixes: Iterable[str] = (),
) -> list[int]:
    """Find independent extraction workers across MCP clients on this host."""
    excluded = excluded_pids or set()
    normalized_paths = tuple(str(path).lower() for path in excluded_path_prefixes if str(path))
    result: list[int] = []
    for process in psutil.process_iter(["pid", "cmdline"]):
        try:
            pid = int(process.info["pid"])
            if pid in excluded:
                continue
            command = " ".join(process.info.get("cmdline") or [])
            normalized_command = command.lower()
            if any(path in normalized_command for path in normalized_paths):
                continue
            if "pdf_rescue_mcp.cli" not in command:
                continue
            if "提取" in command or " extract" in command:
                result.append(pid)
        except (OSError, psutil.Error, TypeError, ValueError):
            continue
    return result


def _profile_root(profile_id: str) -> Path:
    return ensure_runtime_paths().cache_dir / "ocr-capacity-profiles" / profile_id


def _build_fixture_pages(
    page_count: int,
    worker_count: int,
    sample_pages: int,
    warmup_pages: int,
) -> list[list[int]]:
    """Split one common measurement set into disjoint worker fixtures.

    Every candidate measures the same representative source pages.  Only the
    warm-up pages differ by worker count, so a faster multi-worker score cannot
    be an artefact of sampling easier pages than a single-worker score.
    """
    required = sample_pages + worker_count * warmup_pages
    if page_count < required:
        raise ValueError(f"PDF页数不足：需要 {required} 页，实际 {page_count} 页")
    if sample_pages < worker_count:
        raise ValueError("测量页数必须不少于并发 worker 数")
    measurement_pages = [
        min(page_count - 1, int((index + 0.5) * page_count / sample_pages))
        for index in range(sample_pages)
    ]
    if len(set(measurement_pages)) != len(measurement_pages):
        measurement_pages = list(range(sample_pages))
    used = set(measurement_pages)
    remaining = [index for index in range(page_count) if index not in used]
    warmup_total = worker_count * warmup_pages
    warmup_pool = remaining[:warmup_total]
    return [
        warmup_pool[offset * warmup_pages : (offset + 1) * warmup_pages]
        + measurement_pages[offset::worker_count]
        for offset in range(worker_count)
    ]


def _write_fixture(source_pdf: Path, target_pdf: Path, page_indexes: list[int]) -> None:
    target_pdf.parent.mkdir(parents=True, exist_ok=True)
    source = fitz.open(source_pdf)
    fixture = fitz.open()
    try:
        for page_index in page_indexes:
            fixture.insert_pdf(source, from_page=page_index, to_page=page_index)
        fixture.save(target_pdf, garbage=4, deflate=True)
    finally:
        fixture.close()
        source.close()


class CapacityBenchmarkManager:
    """Runs profile candidates in a daemon thread without blocking MCP stdio."""

    SAMPLE_INTERVAL_SECONDS = 2.0
    STARTUP_GRACE_SECONDS = 120.0
    SAFE_STOP_WAIT_SECONDS = 60.0

    def __init__(
        self,
        *,
        task_starter: TaskStarter,
        task_info_reader: TaskInfoReader,
        task_canceller: TaskCanceller | None = None,
        profile_store: ThroughputProfileStore | None = None,
    ) -> None:
        self._task_starter = task_starter
        self._task_info_reader = task_info_reader
        self._task_canceller = task_canceller
        self._store = profile_store or ThroughputProfileStore()
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._running_profile_id: str | None = None
        self._run_id: str | None = None
        self._cancel = threading.Event()
        self._owned_pids: set[int] = set()
        self._owned_path_prefixes: set[str] = set()
        self._active_job_dirs: set[str] = set()

    def _register_owned_task(self, job_dir: str) -> None:
        """Remember launcher/worker PIDs as soon as the task manager returns."""
        info = self._task_info_reader(job_dir) or {}
        for value in info.values():
            if isinstance(value, int) and value > 0:
                self._owned_pids.add(value)

    def _request_active_workers_stop(self) -> None:
        if self._task_canceller is None:
            return
        with self._lock:
            job_dirs = tuple(self._active_job_dirs)
        for job_dir in job_dirs:
            try:
                self._task_canceller(job_dir)
            except Exception:
                # The task supervisor owns the eventual hard-stop escalation;
                # do not let a single already-finished worker block the profile.
                continue

    def _wait_for_workers_to_settle(self, job_dirs: Iterable[str]) -> None:
        deadline = time.monotonic() + self.SAFE_STOP_WAIT_SECONDS
        pending = set(job_dirs)
        while pending and time.monotonic() < deadline:
            for job_dir in tuple(pending):
                status = _read_status(Path(job_dir)) or {}
                terminal = str(status.get("状态") or "") in {"完成", "失败", "已取消", "未完成", "卡死"}
                # A false `存活` before the first heartbeat is not proof that
                # the child has exited.  Only a terminal status releases the
                # profile's safety wait.
                if terminal:
                    pending.discard(job_dir)
            if pending:
                time.sleep(0.5)

    def plan(
        self,
        *,
        source_pdf: str,
        mode: str,
        sample_pages: int,
        warmup_pages: int,
        max_workers: int | None,
        candidate_threads: Iterable[int] = (1, 2, 3, 4),
    ) -> dict[str, Any]:
        source = Path(source_pdf).expanduser().resolve()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(str(source))
        document = fitz.open(source)
        try:
            page_count = document.page_count
        finally:
            document.close()
        if page_count <= 0:
            raise ValueError("PDF没有可用于容量测试的页面")

        topology = hardware_fingerprint()
        memory = psutil.virtual_memory()
        available_memory_gb = memory.available / (1024**3)
        memory_worker_capacity = int(max(0.0, available_memory_gb - 2.0) // 2.0)
        # Production work always wins over an iteration benchmark.  Under a
        # busy OCR run its memory footprint can make the instantaneous headroom
        # look insufficient; returning "resources insufficient" first is
        # misleading and nudges an agent toward needless diagnosis.  Preserve a
        # deferred plan and re-check headroom when it is actually started.
        active = find_live_ocr_processes()
        if memory_worker_capacity < 1 and not active:
            return {
                "状态": "资源不足",
                "配置ID": None,
                "来源PDF": str(source),
                "PDF总页数": page_count,
                "机器拓扑": topology,
                "可用内存GB": round(available_memory_gb, 2),
                "说明": "可用内存未超过 2GB 保留水位，未创建也不会启动容量基准。",
            }
        planning_memory_capacity = max(1, memory_worker_capacity) if active else memory_worker_capacity
        worker_limit = max_workers or min(4, planning_memory_capacity)
        candidates = build_capacity_candidates(
            logical_cpu_count=int(topology["logical_cpu_count"]),
            physical_cpu_count=int(topology["physical_cpu_count"]),
            memory_worker_capacity=planning_memory_capacity,
            max_workers=worker_limit,
            candidate_threads=candidate_threads,
        )
        measured_pages = max(1, int(sample_pages))
        warmup_count = max(0, int(warmup_pages))
        runnable = [
            candidate
            for candidate in candidates
            if candidate.workers <= measured_pages
            and measured_pages + candidate.workers * warmup_count <= page_count
        ]
        if not runnable:
            raise ValueError("PDF页数不足，无法为任一候选生成不重叠的测试样本")
        profile = self._store.create_profile(
            source_pdf=str(source),
            mode=mode,
            candidates=runnable,
            sample_pages=measured_pages,
            warmup_pages=warmup_count,
        )
        state = "已延期" if active else "可运行"
        self._store.update(profile["配置ID"], 状态=state, 活跃OCR进程=active)
        return {
            "状态": state,
            "配置ID": profile["配置ID"],
            "来源PDF": str(source),
            "PDF总页数": page_count,
            "样本页数": measured_pages,
            "预热页数": warmup_count,
            "机器拓扑": topology,
            "可用内存GB": round(available_memory_gb, 2),
            "候选": [candidate.to_dict() for candidate in runnable],
            "活跃OCR进程": active,
            "说明": (
                "检测到生产OCR，基准测试已延期且不会与其争抢资源。"
                if active
                else "基准会在独立缓存目录使用不重叠页面；每个候选串行执行，候选内部才并发。"
            ),
        }

    def start(self, profile_id: str) -> dict[str, Any]:
        with self._lock:
            profile = self._store.get(profile_id)
            if profile is None:
                raise KeyError(profile_id)
            if self._thread is not None and self._thread.is_alive():
                return {"状态": "已有基准运行中", "配置ID": self._running_profile_id}
            if profile.get("试验"):
                return {
                    "状态": "需要重新规划",
                    "配置ID": profile_id,
                    "说明": "已有试验结果保持不可变；请重新规划生成新的配置ID后再运行。",
                }
            active = find_live_ocr_processes()
            if active:
                self._store.update(profile_id, 状态="已延期", 活跃OCR进程=active)
                return {
                    "状态": "已延期",
                    "配置ID": profile_id,
                    "活跃OCR进程": active,
                    "说明": "不会在任何生产OCR运行时启动基准测试。",
                }
            available_memory_gb = psutil.virtual_memory().available / (1024**3)
            if available_memory_gb <= 2.0:
                self._store.update(
                    profile_id,
                    状态="资源不足",
                    活跃OCR进程=[],
                    可用内存GB=round(available_memory_gb, 2),
                )
                return {
                    "状态": "资源不足",
                    "配置ID": profile_id,
                    "可用内存GB": round(available_memory_gb, 2),
                    "说明": "生产OCR已结束，但可用内存未超过 2GB 保留水位；不会启动基准任务。",
                }
            self._cancel.clear()
            self._owned_pids.clear()
            self._owned_path_prefixes.clear()
            self._running_profile_id = profile_id
            self._run_id = f"run-{time.time_ns()}"
            self._store.update(
                profile_id,
                状态="运行中",
                活跃OCR进程=[],
                当前运行ID=self._run_id,
            )
            self._thread = threading.Thread(
                target=self._run,
                args=(profile_id,),
                daemon=True,
                name="ocr-capacity-profile",
            )
            self._thread.start()
            return {"状态": "已启动", "配置ID": profile_id}

    def status(self, profile_id: str) -> dict[str, Any]:
        profile = self._store.get(profile_id)
        if profile is None:
            raise KeyError(profile_id)
        return profile

    def cancel(self, profile_id: str) -> dict[str, Any]:
        with self._lock:
            if profile_id != self._running_profile_id or self._thread is None or not self._thread.is_alive():
                return {"状态": "未在运行", "配置ID": profile_id}
            self._cancel.set()
            self._request_active_workers_stop()
            self._store.update(profile_id, 状态="正在取消")
            return {"状态": "已请求取消", "配置ID": profile_id}

    def activate(self, profile_id: str) -> dict[str, Any]:
        profile = self._store.activate(profile_id)
        return {
            "状态": "已激活",
            "配置ID": profile_id,
            "建议": (profile.get("建议") or {}).get("推荐"),
            "说明": "仅影响之后启动的worker；不会重配或中断正在运行的OCR。",
        }

    def _run(self, profile_id: str) -> None:
        try:
            profile = self._store.get(profile_id)
            if profile is None:
                return
            source = Path(str(profile["来源PDF"]))
            mode = str(profile.get("模式") or "book-fast")
            sample_pages = max(1, int(profile.get("样本页数") or 1))
            warmup_pages = max(0, int(profile.get("预热页数") or 0))
            for raw_candidate in profile.get("候选") or []:
                if self._cancel.is_set():
                    latest = self._store.get(profile_id) or {}
                    if latest.get("状态") != "已延期":
                        self._store.update(profile_id, 状态="已取消")
                    return
                candidate = CapacityCandidate(
                    workers=max(1, int(raw_candidate["workers"])),
                    threads_per_worker=max(1, int(raw_candidate["threads_per_worker"])),
                )
                unexpected = find_live_ocr_processes(excluded_pids=self._owned_pids)
                if unexpected:
                    self._store.update(
                        profile_id,
                        状态="已延期",
                        活跃OCR进程=unexpected,
                        中断原因="检测到新的生产OCR，未继续容量测试。",
                    )
                    return
                trial = self._run_candidate(
                    profile_id,
                    source=source,
                    mode=mode,
                    candidate=candidate,
                    sample_pages=sample_pages,
                    warmup_pages=warmup_pages,
                )
                self._store.append_trial(profile_id, trial)
                if self._cancel.is_set():
                    latest = self._store.get(profile_id) or {}
                    if latest.get("状态") != "已延期":
                        self._store.update(profile_id, 状态="已取消")
                    return
            self._store.update(profile_id, 状态="已完成", 活跃OCR进程=[])
        except Exception as exc:
            self._store.update(
                profile_id,
                状态="失败",
                错误类型=type(exc).__name__,
                错误信息=str(exc),
            )
        finally:
            with self._lock:
                self._running_profile_id = None
                self._run_id = None
                self._owned_pids.clear()
                self._owned_path_prefixes.clear()
                self._active_job_dirs.clear()

    def _run_candidate(
        self,
        profile_id: str,
        *,
        source: Path,
        mode: str,
        candidate: CapacityCandidate,
        sample_pages: int,
        warmup_pages: int,
    ) -> dict[str, Any]:
        document = fitz.open(source)
        try:
            page_groups = _build_fixture_pages(
                document.page_count,
                candidate.workers,
                sample_pages,
                warmup_pages,
            )
        finally:
            document.close()
        run_id = self._run_id or f"run-{time.time_ns()}"
        candidate_root = _profile_root(profile_id) / "runs" / run_id / candidate.key
        with self._lock:
            self._owned_path_prefixes.add(str(candidate_root).lower())
        jobs: list[dict[str, Any]] = []
        started_at = time.monotonic()
        for worker_index, page_indexes in enumerate(page_groups, start=1):
            unexpected = find_live_ocr_processes(
                excluded_pids=self._owned_pids,
                excluded_path_prefixes=self._owned_path_prefixes,
            )
            if unexpected:
                self._cancel.set()
                self._store.update(
                    profile_id,
                    状态="已延期",
                    活跃OCR进程=unexpected,
                    中断原因="启动候选 worker 前检测到生产 OCR",
                )
                self._request_active_workers_stop()
                break
            fixture_path = candidate_root / "fixtures" / f"worker-{worker_index}.pdf"
            _write_fixture(source, fixture_path, page_indexes)
            output_root = candidate_root / "outputs" / f"worker-{worker_index}"
            job_dir, _already_running = self._task_starter(
                path=str(fixture_path),
                output_dir=str(output_root),
                mode=mode,
                max_pages=len(page_indexes),
                resume=False,
                password=None,
                ocr_threads=candidate.threads_per_worker,
                ocr_profile_warmup_pages=warmup_pages,
            )
            self._register_owned_task(job_dir)
            jobs.append({"任务目录": job_dir, "页码": page_indexes, "启动时刻": time.monotonic()})
        with self._lock:
            self._active_job_dirs = {str(job["任务目录"]) for job in jobs}

        system_cpu_samples: list[float] = []
        available_memory_samples: list[float] = []
        rss_samples: list[float] = []
        saturated_thread_samples: list[float] = []
        thread_utilization_samples: list[float] = []
        external_cpu_samples: list[float] = []
        per_worker_samples: dict[str, list[dict[str, object]]] = {item["任务目录"]: [] for item in jobs}
        deadline = time.monotonic() + max(300.0, max(map(len, page_groups)) * 180.0)
        while time.monotonic() < deadline:
            if self._cancel.is_set():
                self._request_active_workers_stop()
                self._wait_for_workers_to_settle(str(job["任务目录"]) for job in jobs)
                break
            unexpected = find_live_ocr_processes(
                excluded_pids=self._owned_pids,
                excluded_path_prefixes=self._owned_path_prefixes,
            )
            if unexpected:
                self._cancel.set()
                self._request_active_workers_stop()
                self._store.update(
                    profile_id,
                    状态="已延期",
                    活跃OCR进程=unexpected,
                    中断原因="检测到生产 OCR，已请求基准 worker 安全停止",
                )
                self._wait_for_workers_to_settle(str(job["任务目录"]) for job in jobs)
                break
            live = False
            pids: list[int] = []
            budgets: dict[int, int] = {}
            for job in jobs:
                info = self._task_info_reader(str(job["任务目录"])) or {}
                pid = info.get("工作进程ID")
                if isinstance(pid, int) and pid > 0:
                    self._owned_pids.add(pid)
                    pids.append(pid)
                    budgets[pid] = candidate.threads_per_worker
                    usage = collect_process_resource_usage(pid)
                    per_worker_samples[str(job["任务目录"])].append(usage)
                    thread_cores = float(usage.get("线程CPU总和等效核心数") or 0.0)
                    thread_utilization_samples.append(
                        round(thread_cores / max(1, candidate.threads_per_worker) * 100.0, 2)
                    )
                status = _read_status(Path(str(job["任务目录"]))) or {}
                terminal = str(status.get("状态") or "") in {"完成", "失败", "已取消", "未完成", "卡死"}
                # TaskManager deliberately waits for the first heartbeat before
                # reporting `存活`.  Treat a newly launched, non-terminal task
                # as live through its startup grace period so profiling never
                # exits before Paddle has initialized.
                starting = time.monotonic() - float(job["启动时刻"]) < self.STARTUP_GRACE_SECONDS
                live = live or (not terminal and (bool(info.get("存活")) or starting))
            plan = ResourceScheduler().plan(pids, worker_thread_budgets=budgets)
            if plan.system_cpu_percent is not None:
                system_cpu_samples.append(plan.system_cpu_percent)
            if plan.external_cpu_percent is not None:
                external_cpu_samples.append(plan.external_cpu_percent)
            available_memory_samples.append(plan.available_memory_gb)
            rss_samples.append(sum(worker.memory_mb or 0.0 for worker in plan.workers))
            saturated_thread_samples.append(float(sum(worker.saturated_thread_count or 0 for worker in plan.workers)))
            if not live:
                break
            time.sleep(self.SAMPLE_INTERVAL_SECONDS)

        timed_out = time.monotonic() >= deadline
        if timed_out:
            self._request_active_workers_stop()
            self._wait_for_workers_to_settle(str(job["任务目录"]) for job in jobs)
        elapsed_seconds = max(0.001, time.monotonic() - started_at)
        statuses = [_read_status(Path(str(job["任务目录"]))) or {} for job in jobs]
        completed_pages = sum(
            max(
                0,
                min(
                    max(0, len(job["页码"]) - warmup_pages),
                    int(status.get("已处理页数") or 0) - warmup_pages,
                ),
            )
            for job, status in zip(jobs, statuses, strict=True)
        )
        failed_pages = sum(int(status.get("失败页数") or 0) for status in statuses)
        low_pages = sum(int(status.get("低置信页数") or 0) for status in statuses)
        steady_ppm = sum(
            max(0.0, float(status.get("短窗OCR页每分钟") or 0.0))
            for status in statuses
        )
        page_seconds = [
            float(status.get("短窗OCR中位秒每页"))
            for status in statuses
            if status.get("短窗OCR中位秒每页") is not None
        ]
        terminal_ok = all(status.get("状态") == "完成" for status in statuses)
        rejection_reasons: list[str] = []
        if self._cancel.is_set():
            rejection_reasons.append("基准已取消")
        if timed_out:
            rejection_reasons.append("候选超时，已请求 worker 安全停止")
        if not terminal_ok:
            rejection_reasons.append("至少一个worker未完成")
        if completed_pages < sample_pages:
            rejection_reasons.append("有效OCR样本页不足")
        if steady_ppm <= 0:
            rejection_reasons.append("未取得短窗OCR吞吐率")
        per_worker_summary: dict[str, dict[str, float | None]] = {}
        for job_dir, samples in per_worker_samples.items():
            per_worker_summary[job_dir] = {
                "rss_mb_p95": _percentile(
                    [float(sample.get("内存MB") or 0.0) for sample in samples], 0.95
                ),
                "active_thread_count_p95": _percentile(
                    [float(sample.get("活跃CPU线程数") or 0.0) for sample in samples], 0.95
                ),
                "saturated_thread_count_p95": _percentile(
                    [float(sample.get("饱和CPU线程数") or 0.0) for sample in samples], 0.95
                ),
                "thread_cpu_core_equivalents_p95": _percentile(
                    [float(sample.get("线程CPU总和等效核心数") or 0.0) for sample in samples], 0.95
                ),
            }
        result = {
            "候选": candidate.key,
            "workers": candidate.workers,
            "threads_per_worker": candidate.threads_per_worker,
            "configured_threads_total": candidate.configured_threads_total,
            "measured_pages": completed_pages,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "pages_per_minute": round(steady_ppm, 3),
            "end_to_end_pages_per_minute": round(completed_pages / elapsed_seconds * 60.0, 3),
            "p50_seconds_per_page": _percentile(page_seconds, 0.5),
            "failed_pages": failed_pages,
            "low_confidence_ratio": round(low_pages / max(1, completed_pages), 4),
            "system_cpu_percent_p95": _percentile(system_cpu_samples, 0.95),
            "available_memory_min_gb": round(min(available_memory_samples), 3)
            if available_memory_samples
            else None,
            "total_rss_mb_p95": _percentile(rss_samples, 0.95),
            "saturated_thread_count_p95": _percentile(saturated_thread_samples, 0.95),
            "thread_utilization_percent_p95": _percentile(thread_utilization_samples, 0.95),
            "external_cpu_percent_p95": _percentile(external_cpu_samples, 0.95),
            "per_worker_samples": per_worker_samples,
            "per_worker_summary": per_worker_summary,
            "rejection_reasons": rejection_reasons,
        }
        with self._lock:
            self._active_job_dirs.clear()
        return result
