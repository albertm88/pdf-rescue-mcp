from __future__ import annotations

from pathlib import Path

import pytest

from pdf_rescue_mcp.task_store import (
    ATTEMPT_CANCELLED,
    ATTEMPT_COMPLETED,
    ATTEMPT_FAILED,
    PAGE_COMPLETED,
    PAGE_FAILED,
    PAGE_RUNNING,
    TASK_CANCELLED,
    TASK_COMPLETED,
    TASK_QUEUED,
    TASK_RUNNING,
    AttemptNotFoundError,
    InvalidTransitionError,
    SensitiveValueError,
    TaskConflictError,
    TaskStore,
    make_idempotency_key,
)


def _store(tmp_path: Path) -> TaskStore:
    return TaskStore(tmp_path / "runtime" / "tasks.sqlite3")


def _claim(store: TaskStore, key: str = "book-v1", *, total_pages: int | None = 3):
    return store.claim_task(
        idempotency_key=key,
        source_path="/books/农业化学.pdf",
        source_fingerprint="sha256:source",
        output_root="/output",
        mode="book-balanced",
        request={"dpi": 200},
        total_pages=total_pages,
        now=10,
    )


def test_claim_is_idempotent_and_durable_across_store_instances(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _claim(store)
    second = TaskStore(store.database_path).claim_task(
        idempotency_key="book-v1",
        source_path="/renamed/path.pdf",
        mode="different-mode-is-ignored-for-existing-key",
        now=11,
    )

    assert first.created is True
    assert second.created is False
    assert second.task.job_id == first.task.job_id
    assert second.task.source_path == "/books/农业化学.pdf"
    assert second.task.request == {"dpi": 200}
    assert [event.event_type for event in store.list_events(first.task.job_id)] == ["task_claimed"]


def test_idempotency_key_is_stable_and_options_cannot_persist_credentials(tmp_path: Path) -> None:
    first = make_idempotency_key(
        "sha256:abc",
        "book-balanced",
        page_start=1,
        page_end=10,
        options={"dpi": 300, "engine": "paddle"},
    )
    second = make_idempotency_key(
        "sha256:abc",
        "book-balanced",
        page_start=1,
        page_end=10,
        options={"engine": "paddle", "dpi": 300},
    )

    assert first == second
    with pytest.raises(SensitiveValueError):
        _store(tmp_path).claim_task(
            idempotency_key="contains-secret",
            source_path="/books/locked.pdf",
            mode="book-balanced",
            request={"password": "do-not-write-this"},
        )


def test_attempt_and_page_lifecycle_tracks_distinct_liveness_and_progress(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store).task
    attempt = store.start_attempt(
        task.job_id,
        supervisor_id="supervisor-a",
        worker_pid=1234,
        worker_started_at=20,
        now=20,
    )

    store.record_heartbeat(attempt.attempt_id, now=25)
    live_task = store.get_task(task.job_id)
    assert live_task.state == TASK_RUNNING
    assert live_task.last_heartbeat_at == 25
    assert live_task.last_progress_at is None

    started = store.record_page_started(attempt.attempt_id, 1, now=30)
    completed = store.record_page_completed(
        attempt.attempt_id,
        1,
        result={"confidence": 0.93},
        now=40,
    )
    updated_task = store.get_task(task.job_id)
    updated_attempt = store.get_attempt(attempt.attempt_id)

    assert started.state == PAGE_RUNNING
    assert completed.state == PAGE_COMPLETED
    assert updated_task.completed_pages == 1
    assert updated_task.failed_pages == 0
    assert updated_task.last_completed_page == 1
    assert updated_task.last_progress_at == 40
    assert updated_task.current_page is None
    assert updated_attempt.completed_pages == 1
    assert updated_attempt.last_progress_at == 40
    assert updated_task.progress_fraction == pytest.approx(1 / 3)


def test_page_updates_are_idempotent_and_retry_reconciles_failed_count(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store).task
    first_attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=20)

    store.record_page_started(first_attempt.attempt_id, 2, now=21)
    failed = store.record_page_failed(first_attempt.attempt_id, 2, error={"kind": "ocr_timeout"}, now=22)
    duplicate_failure = store.record_page_failed(first_attempt.attempt_id, 2, now=23)
    after_failure = store.get_task(task.job_id)
    assert failed.state == PAGE_FAILED
    assert duplicate_failure.state == PAGE_FAILED
    assert after_failure.failed_pages == 1

    queued = store.finish_attempt(
        first_attempt.attempt_id,
        outcome=ATTEMPT_FAILED,
        retry=True,
        error={"kind": "ocr_timeout"},
        now=24,
    )
    assert queued.state == TASK_QUEUED
    retry_attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-b", now=25)
    store.record_page_started(retry_attempt.attempt_id, 2, now=26)
    recovered = store.record_page_completed(retry_attempt.attempt_id, 2, now=27)
    duplicate_completion = store.record_page_completed(retry_attempt.attempt_id, 2, now=28)
    after_recovery = store.get_task(task.job_id)

    assert retry_attempt.attempt_number == 2
    assert recovered.state == PAGE_COMPLETED
    assert duplicate_completion.state == PAGE_COMPLETED
    assert after_recovery.completed_pages == 1
    assert after_recovery.failed_pages == 0


def test_only_one_active_attempt_and_cancellation_is_durable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store).task
    attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=20)

    with pytest.raises(TaskConflictError):
        store.start_attempt(task.job_id, supervisor_id="supervisor-b", now=21)

    cancelling = store.request_cancel(task.job_id, reason="user requested", now=22)
    assert cancelling.state == "cancelling"
    assert store.get_attempt(attempt.attempt_id).state == "cancelling"

    settled = store.finish_attempt(
        attempt.attempt_id,
        outcome=ATTEMPT_CANCELLED,
        now=23,
    )
    assert settled.state == TASK_CANCELLED
    assert settled.cancellation_reason == "user requested"
    assert settled.finished_at == 23
    assert store.request_cancel(task.job_id, now=24).state == TASK_CANCELLED


def test_finish_attempt_is_terminal_and_requeue_creates_new_attempt_number(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store).task
    attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=20)
    completed = store.finish_attempt(
        attempt.attempt_id,
        outcome=ATTEMPT_COMPLETED,
        result={"manifest": "manifest.yaml"},
        now=30,
    )

    assert completed.state == TASK_COMPLETED
    assert completed.result == {"manifest": "manifest.yaml"}
    assert store.finish_attempt(attempt.attempt_id, outcome=ATTEMPT_COMPLETED, now=31).state == TASK_COMPLETED
    with pytest.raises(InvalidTransitionError):
        store.finish_attempt(attempt.attempt_id, outcome=ATTEMPT_FAILED, now=32)

    requeued = store.requeue_task(task.job_id, reason="manual quality retry", now=33)
    next_attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=34)
    assert requeued.state == TASK_QUEUED
    assert next_attempt.attempt_number == 2


def test_page_boundaries_and_stale_attempts_are_rejected(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store, total_pages=1).task
    attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=20)

    with pytest.raises(ValueError, match="exceeds"):
        store.record_page_started(attempt.attempt_id, 2, now=21)

    store.finish_attempt(attempt.attempt_id, outcome=ATTEMPT_FAILED, retry=True, now=22)
    with pytest.raises(InvalidTransitionError):
        store.record_heartbeat(attempt.attempt_id, now=23)
    with pytest.raises(AttemptNotFoundError):
        store.get_attempt("not-an-attempt")


def test_generic_leases_use_fencing_tokens_and_expiry(tmp_path: Path) -> None:
    store = _store(tmp_path)
    lease = store.acquire_lease("gpu:0", owner_id="supervisor-a", ttl_seconds=10, now=100)
    assert lease is not None
    assert store.acquire_lease("gpu:0", owner_id="supervisor-b", ttl_seconds=10, now=101) is None
    assert store.renew_lease(
        "gpu:0", owner_id="supervisor-a", token=lease.token, ttl_seconds=10, now=105
    ) is not None
    assert store.renew_lease(
        "gpu:0", owner_id="supervisor-a", token="stale", ttl_seconds=10, now=106
    ) is None
    assert store.release_lease("gpu:0", owner_id="supervisor-a", token="stale") is False
    assert store.release_lease("gpu:0", owner_id="supervisor-a", token=lease.token) is True

    replacement = store.acquire_lease("gpu:0", owner_id="supervisor-b", ttl_seconds=5, now=120)
    assert replacement is not None
    assert store.get_lease("gpu:0", now=126) is None
    assert store.acquire_lease("gpu:0", owner_id="supervisor-c", ttl_seconds=5, now=126) is not None


def test_claim_next_queued_task_is_ordered_and_respects_task_leases(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _claim(store, "first").task
    second = store.claim_task(
        idempotency_key="second",
        source_path="/books/second.pdf",
        mode="book-balanced",
        now=11,
    ).task
    held = store.acquire_task_lease(first.job_id, owner_id="other", ttl_seconds=20, now=12)
    assert held is not None

    claim = store.claim_next_queued_task(owner_id="scheduler", ttl_seconds=10, now=13)
    assert claim is not None
    assert claim.task.job_id == second.job_id
    assert claim.lease.resource_key == TaskStore.task_resource_key(second.job_id)
    assert store.claim_next_queued_task(owner_id="another", ttl_seconds=10, now=14) is None

    takeover = store.claim_next_queued_task(owner_id="scheduler", ttl_seconds=10, now=33)
    assert takeover is not None
    assert takeover.task.job_id == first.job_id


def test_event_stream_is_append_only_and_filterable(tmp_path: Path) -> None:
    store = _store(tmp_path)
    task = _claim(store).task
    attempt = store.start_attempt(task.job_id, supervisor_id="supervisor-a", now=20)
    custom = store.append_event(
        task.job_id,
        "quality_sampled",
        payload={"score": 0.81},
        attempt_id=attempt.attempt_id,
        now=21,
    )
    all_events = store.list_events(task.job_id)
    tail = store.list_events(task.job_id, after_event_id=custom.event_id - 1)

    assert [event.event_type for event in all_events] == [
        "task_claimed",
        "attempt_started",
        "quality_sampled",
    ]
    assert [event.event_type for event in tail] == ["quality_sampled"]
    with pytest.raises(SensitiveValueError):
        store.append_event(task.job_id, "bad", payload={"token": "secret"})


def test_task_lease_requires_an_existing_task(tmp_path: Path) -> None:
    with pytest.raises(Exception, match="task not found"):
        _store(tmp_path).acquire_task_lease(
            "missing", owner_id="supervisor-a", ttl_seconds=10, now=1
        )
