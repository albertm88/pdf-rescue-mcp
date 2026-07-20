from __future__ import annotations

import pytest

from pdf_rescue_mcp.throughput_tuning import (
    CapacityCandidate,
    ThroughputProfileStore,
    build_capacity_candidates,
    evaluate_capacity_trials,
)


def _trial(
    workers: int,
    threads: int,
    ppm: float,
    *,
    low_confidence_ratio: float = 0.01,
) -> dict[str, object]:
    return {
        "候选": f"{workers}x{threads}",
        "workers": workers,
        "threads_per_worker": threads,
        "pages_per_minute": ppm,
        "failed_pages": 0,
        "low_confidence_ratio": low_confidence_ratio,
        "available_memory_min_gb": 6.0,
        "system_cpu_percent_p95": 80.0,
        "total_rss_mb_p95": workers * 700.0,
        "thread_utilization_percent_p95": 82.0,
        "external_cpu_percent_p95": 8.0,
    }


def test_capacity_matrix_uses_logical_thread_budget_after_reserve() -> None:
    candidates = build_capacity_candidates(
        logical_cpu_count=16,
        physical_cpu_count=8,
        memory_worker_capacity=4,
        max_workers=4,
    )

    assert [candidate.key for candidate in candidates] == [
        "1x1",
        "1x2",
        "1x3",
        "1x4",
        "2x1",
        "2x2",
        "2x3",
        "2x4",
        "3x1",
        "3x2",
        "3x3",
        "3x4",
        "4x1",
        "4x2",
        "4x3",
    ]


def test_evaluator_prefers_lower_thread_budget_inside_five_percent_band() -> None:
    result = evaluate_capacity_trials(
        [
            _trial(1, 2, 100.0),
            _trial(1, 4, 120.0),
            _trial(2, 2, 127.0),
            _trial(2, 4, 130.0),
        ]
    )

    recommendation = result["推荐"]
    assert recommendation["workers"] == 2
    assert recommendation["threads_per_worker"] == 2
    assert recommendation["pages_per_minute"] == 127.0
    assert recommendation["pages_per_configured_thread_minute"] == 31.75


def test_evaluator_rejects_quality_regression_against_single_worker_baseline() -> None:
    result = evaluate_capacity_trials(
        [
            _trial(1, 4, 120.0, low_confidence_ratio=0.01),
            _trial(2, 2, 132.0, low_confidence_ratio=0.10),
        ]
    )

    recommendation = result["推荐"]
    assert recommendation["workers"] == 1
    assert recommendation["threads_per_worker"] == 4
    rejected = next(trial for trial in result["试验"] if trial["workers"] == 2)
    assert rejected["valid"] is False
    assert "低置信" in "；".join(rejected["rejection_reasons"])


def test_profile_cannot_activate_until_all_candidates_are_completed(tmp_path) -> None:
    store = ThroughputProfileStore(tmp_path / "profiles.json")
    profile = store.create_profile(
        source_pdf="D:/book.pdf",
        mode="book-fast",
        candidates=[CapacityCandidate(1, 2)],
        sample_pages=8,
        warmup_pages=2,
    )
    profile_id = profile["配置ID"]

    with pytest.raises(ValueError, match="完成"):
        store.activate(profile_id)

    store.append_trial(profile_id, _trial(1, 2, 100.0))
    store.update(profile_id, 状态="完成")

    activated = store.activate(profile_id)
    assert activated["配置ID"] == profile_id
    active = store.active_recommendation(mode="book-fast")
    assert active is not None
    assert active["workers"] == 1
    assert active["threads_per_worker"] == 2
