from __future__ import annotations

import json
from pathlib import Path

import fitz

import pdf_rescue_mcp.capacity_benchmark as benchmark_module
from pdf_rescue_mcp.capacity_benchmark import CapacityBenchmarkManager
from pdf_rescue_mcp.throughput_tuning import CapacityCandidate, ThroughputProfileStore


def _make_pdf(path: Path, page_count: int) -> None:
    document = fitz.open()
    try:
        for number in range(page_count):
            page = document.new_page()
            page.insert_text((72, 72), f"sample page {number + 1}")
        document.save(path)
    finally:
        document.close()


def test_profile_is_deferred_when_production_ocr_is_active(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "source.pdf"
    _make_pdf(source, 20)
    started: list[dict[str, object]] = []
    manager = CapacityBenchmarkManager(
        task_starter=lambda **kwargs: (started.append(kwargs), (str(tmp_path / "job"), False))[1],
        task_info_reader=lambda _job_dir: None,
        profile_store=ThroughputProfileStore(tmp_path / "profiles.json"),
    )
    monkeypatch.setattr(benchmark_module, "find_live_ocr_processes", lambda **_kwargs: [4321])

    plan = manager.plan(
        source_pdf=str(source),
        mode="book-fast",
        sample_pages=2,
        warmup_pages=0,
        max_workers=1,
    )
    assert plan["状态"] == "已延期"

    started_result = manager.start(plan["配置ID"])
    assert started_result["状态"] == "已延期"
    assert started == []


def test_profile_waits_for_first_heartbeat_instead_of_ending_immediately(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source = tmp_path / "source.pdf"
    _make_pdf(source, 1)
    job_dir = tmp_path / "job"
    calls = 0

    def task_starter(**_kwargs):
        job_dir.mkdir(exist_ok=True)
        (job_dir / "状态.json").write_text(
            json.dumps({"状态": "启动中", "已处理页数": 0}, ensure_ascii=False),
            encoding="utf-8",
        )
        return str(job_dir), False

    def task_info_reader(_job_dir: str):
        nonlocal calls
        calls += 1
        if calls >= 3:
            (job_dir / "状态.json").write_text(
                json.dumps({"状态": "完成", "已处理页数": 1}, ensure_ascii=False),
                encoding="utf-8",
            )
        return {"存活": False, "启动进程ID": 9876}

    manager = CapacityBenchmarkManager(
        task_starter=task_starter,
        task_info_reader=task_info_reader,
        profile_store=ThroughputProfileStore(tmp_path / "profiles.json"),
    )
    manager.SAMPLE_INTERVAL_SECONDS = 0.0
    monkeypatch.setattr(benchmark_module, "_profile_root", lambda _profile_id: tmp_path / "fixtures")
    monkeypatch.setattr(benchmark_module, "find_live_ocr_processes", lambda **_kwargs: [])

    result = manager._run_candidate(
        "profile-test",
        source=source,
        mode="book-fast",
        candidate=CapacityCandidate(1, 2),
        sample_pages=1,
        warmup_pages=0,
    )

    assert calls >= 3
    assert result["workers"] == 1
    assert result["measured_pages"] == 1


def test_fixture_measurement_pages_are_common_and_workers_do_not_overlap() -> None:
    single = benchmark_module._build_fixture_pages(100, 1, sample_pages=8, warmup_pages=2)
    parallel = benchmark_module._build_fixture_pages(100, 4, sample_pages=8, warmup_pages=2)

    single_measurement = single[0][2:]
    parallel_measurement = [page for group in parallel for page in group[2:]]
    assert sorted(single_measurement) == sorted(parallel_measurement)
    assert len(parallel_measurement) == len(set(parallel_measurement))
    all_parallel_pages = [page for group in parallel for page in group]
    assert len(all_parallel_pages) == len(set(all_parallel_pages))


def test_profile_with_existing_trials_requires_a_new_plan_before_rerun(tmp_path: Path) -> None:
    store = ThroughputProfileStore(tmp_path / "profiles.json")
    profile = store.create_profile(
        source_pdf=str(tmp_path / "source.pdf"),
        mode="book-fast",
        candidates=[CapacityCandidate(1, 2)],
        sample_pages=2,
        warmup_pages=0,
    )
    store.append_trial(
        profile["配置ID"],
        {
            "候选": "1x2",
            "workers": 1,
            "threads_per_worker": 2,
            "pages_per_minute": 10.0,
            "failed_pages": 0,
            "available_memory_min_gb": 4.0,
            "system_cpu_percent_p95": 50.0,
        },
    )
    manager = CapacityBenchmarkManager(
        task_starter=lambda **_kwargs: (str(tmp_path / "job"), False),
        task_info_reader=lambda _job_dir: None,
        profile_store=store,
    )

    result = manager.start(profile["配置ID"])

    assert result["状态"] == "需要重新规划"
