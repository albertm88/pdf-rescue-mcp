from __future__ import annotations

from pathlib import Path

from pdf_rescue_mcp.process_controller import ProcessIdentity, WorkerHandle
from pdf_rescue_mcp.supervisor import LocalSupervisor, TASK_ATTEMPT_ENV, TASK_DATABASE_ENV


def _source(tmp_path: Path) -> Path:
    source = tmp_path / "农业书.pdf"
    source.write_bytes(b"test-pdf")
    return source


def test_supervisor_claims_binds_and_settles_a_portable_attempt(tmp_path: Path) -> None:
    supervisor = LocalSupervisor(
        database_path=tmp_path / "runtime" / "tasks.sqlite3",
        owner_id="test-supervisor",
    )
    context = supervisor.begin(
        source_path=_source(tmp_path),
        output_root=tmp_path / "out",
        mode="book-balanced",
        max_pages=3,
        resume=True,
    )

    assert context is not None
    environment = supervisor.child_environment(context)
    assert environment[TASK_DATABASE_ENV] == str(supervisor.database_path)
    assert environment[TASK_ATTEMPT_ENV] == context.attempt_id

    worker = WorkerHandle(
        identity=ProcessIdentity(pid=12345, create_time=100.0),
        command=("python", "-m", "pdf_rescue_mcp.cli"),
        attempt_id=context.attempt_id,
    )
    bound = supervisor.bind_worker(context, worker)
    assert bound.worker_pid == 12345

    settled = supervisor.settle(context, outcome="completed", result={"job_dir": "out"})
    assert settled is not None
    snapshot = supervisor.task_snapshot(context.job_id)
    assert snapshot["task"]["state"] == "completed"
    assert snapshot["lease"] is None


def test_second_local_adapter_cannot_claim_the_same_live_task(tmp_path: Path) -> None:
    database = tmp_path / "runtime" / "tasks.sqlite3"
    first = LocalSupervisor(database_path=database, owner_id="adapter-one")
    second = LocalSupervisor(database_path=database, owner_id="adapter-two")
    source = _source(tmp_path)

    primary = first.begin(
        source_path=source,
        output_root=tmp_path / "out",
        mode="book-fast",
        max_pages=None,
        resume=True,
    )
    duplicate = second.begin(
        source_path=source,
        output_root=tmp_path / "out",
        mode="book-fast",
        max_pages=None,
        resume=True,
    )

    assert primary is not None
    assert duplicate is None
    first.settle(primary, outcome="cancelled")


def test_controller_lease_keeps_second_adapter_observer_only(tmp_path: Path) -> None:
    database = tmp_path / "runtime" / "tasks.sqlite3"
    first = LocalSupervisor(database_path=database, owner_id="controller-one")
    second = LocalSupervisor(database_path=database, owner_id="controller-two")

    lease = first.acquire_controller_lease("batch-ledger:test")

    assert lease is not None
    assert first.owns_controller_lease(lease) is True
    assert second.owns_controller_lease(lease) is False
    assert second.acquire_controller_lease("batch-ledger:test") is None
    assert first.renew_controller_lease(lease) is not None
    assert first.release_controller_lease(lease) is True
    assert second.acquire_controller_lease("batch-ledger:test") is not None


def test_restored_task_monitor_must_win_the_existing_task_lease(tmp_path: Path) -> None:
    database = tmp_path / "runtime" / "tasks.sqlite3"
    first = LocalSupervisor(database_path=database, owner_id="adapter-one")
    second = LocalSupervisor(database_path=database, owner_id="adapter-two")
    primary = first.begin(
        source_path=_source(tmp_path),
        output_root=tmp_path / "out",
        mode="book-fast",
        max_pages=None,
        resume=True,
    )

    assert primary is not None
    assert second.reattach(job_id=primary.job_id, attempt_id=primary.attempt_id) is None

    # Simulate a crashed first adapter after its fencing lease has expired.  The
    # second adapter may then own monitoring of the same durable attempt.
    assert first.store.release_lease(
        primary.lease.resource_key,
        owner_id=first.owner_id,
        token=primary.lease.token,
    )
    recovered = second.reattach(job_id=primary.job_id, attempt_id=primary.attempt_id)

    assert recovered is not None
    assert recovered.lease.owner_id == "adapter-two"
    second.settle(recovered, outcome="cancelled")
