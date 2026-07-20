"""Evidence-based OCR thread and worker tuning.

This module belongs to the iteration/update layer.  It stores results from
isolated OCR capacity profiles and produces an *advisory* configuration for
future workers.  It never changes an OCR adapter that is already running.
"""

from __future__ import annotations

import json
import os
import platform
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

import psutil

from .runtime_paths import ensure_runtime_paths


PROFILE_FILE_NAME = "ocr_capacity_profiles.json"
PROFILE_VERSION = 1


def _as_positive_int(value: object, default: int) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return default
    return result if result > 0 else default


def _as_nonnegative_float(value: object, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    return result if result >= 0 else default


@dataclass(frozen=True)
class CapacityCandidate:
    """One isolated OCR capacity configuration to measure."""

    workers: int
    threads_per_worker: int

    @property
    def configured_threads_total(self) -> int:
        return self.workers * self.threads_per_worker

    @property
    def key(self) -> str:
        return f"{self.workers}x{self.threads_per_worker}"

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["configured_threads_total"] = self.configured_threads_total
        data["key"] = self.key
        return data


def hardware_fingerprint() -> dict[str, object]:
    """Return the stable hardware context required for profile reuse."""
    logical = max(1, int(psutil.cpu_count(logical=True) or os.cpu_count() or 1))
    physical = max(1, int(psutil.cpu_count(logical=False) or max(1, logical // 2)))
    return {
        "platform": platform.system(),
        "machine": platform.machine(),
        "processor": platform.processor() or None,
        "logical_cpu_count": logical,
        "physical_cpu_count": physical,
    }


def build_capacity_candidates(
    *,
    logical_cpu_count: int,
    physical_cpu_count: int,
    memory_worker_capacity: int,
    max_workers: int = 4,
    candidate_threads: Iterable[int] = (1, 2, 3, 4),
    reserve_threads: int = 2,
) -> list[CapacityCandidate]:
    """Build a 1–4 CPU-thread-per-worker profile matrix.

    Single-worker trials establish the best CPU-thread budget.  Multi-worker
    trials are included up to the logical-thread budget left after an operating
    system reserve.  The profile is deliberately allowed to compare useful
    combinations (for example 3x4 on a 16-logical-thread host),
    while the measured per-thread saturation, RSS, external CPU and throughput
    decide whether those combinations are actually worth activating.
    """
    logical = max(1, int(logical_cpu_count))
    worker_limit = max(1, min(int(max_workers), int(memory_worker_capacity)))
    thread_limit = max(1, min(4, logical))
    configured_thread_limit = max(1, logical - max(0, int(reserve_threads)))
    thread_options = sorted(
        {
            _as_positive_int(value, 1)
            for value in candidate_threads
            if _as_positive_int(value, 1) <= thread_limit
        }
    )
    if not thread_options:
        thread_options = [min(2, thread_limit)]

    candidates: list[CapacityCandidate] = [
        CapacityCandidate(workers=1, threads_per_worker=threads)
        for threads in thread_options
    ]
    for workers in range(2, worker_limit + 1):
        for threads in thread_options:
            if workers * threads <= configured_thread_limit:
                candidates.append(CapacityCandidate(workers=workers, threads_per_worker=threads))

    return sorted(
        {candidate.key: candidate for candidate in candidates}.values(),
        key=lambda item: (item.workers != 1, item.workers, item.threads_per_worker),
    )


def _trial_number(trial: dict[str, Any], key: str, default: float = 0.0) -> float:
    return _as_nonnegative_float(trial.get(key), default)


def evaluate_capacity_trials(
    trials: Iterable[dict[str, Any]],
    *,
    reserve_memory_gb: float = 2.0,
    improvement_threshold: float = 0.05,
    quality_regression_tolerance: float = 0.03,
) -> dict[str, object]:
    """Rank completed profile trials using throughput, quality and efficiency.

    Overall CPU is a guardrail only.  The primary evidence is measured total
    pages/minute, configured worker/thread count, hot-thread utilization and
    worker RSS.  A multi-worker result must beat the best single-worker result
    by ``improvement_threshold``; otherwise the simpler single worker wins.
    """
    normalized: list[dict[str, Any]] = []
    for original in trials:
        trial = dict(original)
        workers = _as_positive_int(trial.get("workers"), 1)
        threads = _as_positive_int(trial.get("threads_per_worker"), 1)
        ppm = _trial_number(trial, "pages_per_minute")
        low_confidence_ratio = _trial_number(trial, "low_confidence_ratio")
        total_rss_mb_p95 = _trial_number(trial, "total_rss_mb_p95")
        thread_utilization_percent_p95 = _trial_number(trial, "thread_utilization_percent_p95")
        external_cpu_percent_p95 = _trial_number(trial, "external_cpu_percent_p95")
        failures = _as_positive_int(trial.get("failed_pages"), 0) if trial.get("failed_pages") else 0
        available_memory = trial.get("available_memory_min_gb")
        available_memory_value = (
            _as_nonnegative_float(available_memory) if available_memory is not None else None
        )
        system_cpu = trial.get("system_cpu_percent_p95")
        system_cpu_value = _as_nonnegative_float(system_cpu) if system_cpu is not None else None
        reasons: list[str] = list(trial.get("rejection_reasons") or [])
        if trial.get("quality_passed") is False:
            reasons.append("质量门禁未通过")
        if ppm <= 0:
            reasons.append("未取得有效吞吐率")
        if failures > 0:
            reasons.append("存在失败页")
        if available_memory_value is not None and available_memory_value <= reserve_memory_gb:
            reasons.append("可用内存触及保留水位")
        if system_cpu_value is not None and system_cpu_value > 92.0:
            reasons.append("整机CPU超过安全护栏")

        trial.update(
            {
                "workers": workers,
                "threads_per_worker": threads,
                "configured_threads_total": workers * threads,
                "pages_per_minute": round(ppm, 3),
                "low_confidence_ratio": round(low_confidence_ratio, 4),
                "total_rss_mb_p95": round(total_rss_mb_p95, 3),
                "thread_utilization_percent_p95": round(thread_utilization_percent_p95, 3),
                "external_cpu_percent_p95": round(external_cpu_percent_p95, 3),
                "pages_per_configured_thread_minute": round(
                    ppm / max(1, workers * threads), 3
                ),
                "failed_pages": failures,
                "available_memory_min_gb": available_memory_value,
                "system_cpu_percent_p95": system_cpu_value,
                "rejection_reasons": list(dict.fromkeys(reasons)),
                "valid": not reasons,
            }
        )
        normalized.append(trial)

    valid_trials = [trial for trial in normalized if trial["valid"]]
    single_trials = [trial for trial in valid_trials if trial["workers"] == 1]
    best_single = max(single_trials, key=lambda item: item["pages_per_minute"], default=None)
    if best_single is None:
        return {
            "状态": "证据不足",
            "advisory_only": True,
            "推荐": None,
            "基线": None,
            "试验": normalized,
            "说明": "没有通过质量和资源护栏的单worker基线，不能激活自动调度策略。",
        }

    baseline_ppm = float(best_single["pages_per_minute"])
    baseline_low_confidence_ratio = float(best_single.get("low_confidence_ratio") or 0.0)
    for trial in valid_trials:
        if float(trial.get("low_confidence_ratio") or 0.0) > (
            baseline_low_confidence_ratio + max(0.0, quality_regression_tolerance)
        ):
            trial["rejection_reasons"].append("低置信页比例相对单 worker 基线回退")
            trial["rejection_reasons"] = list(dict.fromkeys(trial["rejection_reasons"]))
            trial["valid"] = False
    valid_trials = [trial for trial in normalized if trial["valid"]]
    for trial in valid_trials:
        speedup = trial["pages_per_minute"] / baseline_ppm if baseline_ppm else 0.0
        efficiency = speedup / trial["workers"] if trial["workers"] else 0.0
        trial["相对最佳单worker加速比"] = round(speedup, 3)
        trial["并发效率"] = round(efficiency, 3)

    acceptable: list[dict[str, Any]] = []
    for trial in valid_trials:
        if trial["workers"] <= 1:
            acceptable.append(trial)
            continue
        if trial["pages_per_minute"] >= baseline_ppm * (1.0 + improvement_threshold):
            acceptable.append(trial)

    # First retain only candidates that are within the configured throughput
    # tolerance of the fastest safe configuration.  Inside that band, select
    # the lower-concurrency choice: it is easier to keep stable when another
    # program starts using the machine.  Do not approximate this comparison by
    # rounding ratios; a 5% tie must be an actual 5% tie.
    peak_ppm = max(float(item["pages_per_minute"]) for item in acceptable)
    near_peak = [
        item
        for item in acceptable
        if float(item["pages_per_minute"]) >= peak_ppm * (1.0 - improvement_threshold)
    ]
    winner = min(
        near_peak,
        key=lambda item: (
            int(item["configured_threads_total"]),
            int(item["workers"]),
            float(item.get("total_rss_mb_p95") or float("inf")),
            abs(float(item.get("thread_utilization_percent_p95") or 0.0) - 80.0),
            float(item.get("external_cpu_percent_p95") or float("inf")),
            -float(item["pages_per_minute"]),
        ),
    )
    return {
        "状态": "已生成建议",
        "advisory_only": True,
        "基线": best_single,
        "推荐": {
            "workers": winner["workers"],
            "threads_per_worker": winner["threads_per_worker"],
            "configured_threads_total": winner["configured_threads_total"],
            "pages_per_minute": winner["pages_per_minute"],
            "pages_per_configured_thread_minute": winner["pages_per_configured_thread_minute"],
            "thread_utilization_percent_p95": winner["thread_utilization_percent_p95"],
            "total_rss_mb_p95": winner["total_rss_mb_p95"],
            "external_cpu_percent_p95": winner["external_cpu_percent_p95"],
            "相对最佳单worker加速比": winner.get("相对最佳单worker加速比", 1.0),
            "并发效率": winner.get("并发效率", 1.0),
            "依据": "仅比较通过质量、内存和整机安全护栏的真实OCR试验；不修改运行中worker。",
        },
        "试验": normalized,
        "说明": "多worker必须比最佳单worker至少快5%，否则保留更简单的单worker配置。",
    }


class ThroughputProfileStore:
    """Portable, atomic persistence for profile plans and their activation."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self._lock = threading.RLock()

    def _path(self) -> Path:
        if self.path is None:
            self.path = ensure_runtime_paths().state_dir / "mcp" / PROFILE_FILE_NAME
        return self.path

    def _read(self) -> dict[str, Any]:
        try:
            payload = json.loads(self._path().read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        return {
            "版本": PROFILE_VERSION,
            "激活配置ID": payload.get("激活配置ID"),
            "配置": payload.get("配置", {}),
        }

    @contextmanager
    def _interprocess_lock(self):
        """Serialize read-modify-write operations across MCP clients/LLMs."""
        lock_path = self._path().with_suffix(self._path().suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as handle:
            handle.seek(0)
            if handle.read(1) == b"":
                handle.seek(0)
                handle.write(b"0")
                handle.flush()
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == "nt":
                    import msvcrt

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _write(self, payload: dict[str, Any]) -> None:
        path = self._path()
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid4().hex}.tmp")
        temp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp_path, path)

    def create_profile(
        self,
        *,
        source_pdf: str,
        mode: str,
        candidates: Iterable[CapacityCandidate],
        sample_pages: int,
        warmup_pages: int,
    ) -> dict[str, Any]:
        profile_id = f"profile-{int(time.time())}-{uuid4().hex[:8]}"
        profile = {
            "配置ID": profile_id,
            "状态": "已规划",
            "来源PDF": source_pdf,
            "模式": mode,
            "硬件指纹": hardware_fingerprint(),
            "候选": [candidate.to_dict() for candidate in candidates],
            "样本页数": max(1, int(sample_pages)),
            "预热页数": max(0, int(warmup_pages)),
            "试验": [],
            "建议": None,
            "创建时间": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "更新时间": time.strftime("%Y-%m-%dT%H:%M:%S"),
        }
        with self._lock, self._interprocess_lock():
            payload = self._read()
            payload["配置"][profile_id] = profile
            self._write(payload)
        return profile

    def get(self, profile_id: str) -> dict[str, Any] | None:
        with self._lock, self._interprocess_lock():
            profile = self._read()["配置"].get(profile_id)
            return dict(profile) if isinstance(profile, dict) else None

    def update(self, profile_id: str, **changes: Any) -> dict[str, Any]:
        with self._lock, self._interprocess_lock():
            payload = self._read()
            profile = payload["配置"].get(profile_id)
            if not isinstance(profile, dict):
                raise KeyError(profile_id)
            profile.update(changes)
            profile["更新时间"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            payload["配置"][profile_id] = profile
            self._write(payload)
            return dict(profile)

    def append_trial(self, profile_id: str, trial: dict[str, Any]) -> dict[str, Any]:
        with self._lock, self._interprocess_lock():
            payload = self._read()
            profile = payload["配置"].get(profile_id)
            if not isinstance(profile, dict):
                raise KeyError(profile_id)
            trials = list(profile.get("试验") or [])
            trials.append(dict(trial))
            profile["试验"] = trials
            profile["建议"] = evaluate_capacity_trials(trials)
            profile["更新时间"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            payload["配置"][profile_id] = profile
            self._write(payload)
            return dict(profile)

    def activate(self, profile_id: str) -> dict[str, Any]:
        with self._lock, self._interprocess_lock():
            payload = self._read()
            profile = payload["配置"].get(profile_id)
            if not isinstance(profile, dict):
                raise KeyError(profile_id)
            if profile.get("状态") != "完成":
                raise ValueError("只有已完成的容量基准才能激活")
            candidates = profile.get("候选") or []
            trials = profile.get("试验") or []
            expected_keys = {
                str(item.get("key") or f"{item.get('workers')}x{item.get('threads_per_worker')}")
                for item in candidates
                if isinstance(item, dict)
            }
            settled_keys = {
                str(item.get("候选") or f"{item.get('workers')}x{item.get('threads_per_worker')}")
                for item in trials
                if isinstance(item, dict)
            }
            if expected_keys and not expected_keys.issubset(settled_keys):
                raise ValueError("容量基准尚有候选未结算，不能激活部分结果")
            recommendation = profile.get("建议") or {}
            if not isinstance(recommendation, dict) or not recommendation.get("推荐"):
                raise ValueError("该配置没有可激活的建议")
            payload["激活配置ID"] = profile_id
            self._write(payload)
            return dict(profile)

    def active_recommendation(self, *, mode: str) -> dict[str, Any] | None:
        try:
            with self._lock, self._interprocess_lock():
                payload = self._read()
                profile_id = payload.get("激活配置ID")
                profile = payload["配置"].get(profile_id)
                if not isinstance(profile, dict) or profile.get("模式") != mode:
                    return None
                if profile.get("硬件指纹") != hardware_fingerprint():
                    return None
                recommendation = profile.get("建议") or {}
                chosen = recommendation.get("推荐") if isinstance(recommendation, dict) else None
                if not isinstance(chosen, dict):
                    return None
                return {
                    "配置ID": profile_id,
                    "workers": _as_positive_int(chosen.get("workers"), 1),
                    "threads_per_worker": _as_positive_int(chosen.get("threads_per_worker"), 1),
                    "依据": chosen.get("依据"),
                }
        except OSError:
            # Status polling must remain non-blocking when a portable runtime
            # directory is read-only or another process temporarily owns it.
            return None
