"""Portable supervision-layer facade for local PDF rescue workers.

This module is intentionally below MCP adapters and above the OCR business
pipeline.  It owns only durable task state, local fencing leases, and safe
process identity.  It never imports FastMCP or invokes OCR itself, so stdio,
HTTP, VS Code, Trae, Codex, and AnythingLLM adapters all share the same local
job ownership model.
"""

from __future__ import annotations

import hashlib
import os
import socket
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from .process_controller import ProcessController, WorkerHandle
from .runtime_paths import ensure_runtime_paths
from .task_store import (
    ATTEMPT_CANCELLED,
    ATTEMPT_COMPLETED,
    ATTEMPT_FAILED,
    ATTEMPT_STALLED,
    TASK_QUEUED,
    Attempt,
    InvalidTransitionError,
    Lease,
    Task,
    TaskStore,
    make_idempotency_key,
)


TASK_DATABASE_ENV = "PDF_RESCUE_TASK_DATABASE"
TASK_ATTEMPT_ENV = "PDF_RESCUE_TASK_ATTEMPT_ID"


@dataclass(frozen=True, slots=True)
class SupervisedAttempt:
    """One lease-owned worker attempt that can be passed to a child safely."""

    task: Task
    attempt: Attempt
    lease: Lease

    @property
    def job_id(self) -> str:
        return self.task.job_id

    @property
    def attempt_id(self) -> str:
        return self.attempt.attempt_id


class LocalSupervisor:
    """Coordinate one-machine worker ownership through SQLite and psutil.

    SQLite is deliberately local-only.  A fencing lease prevents two MCP host
    adapters on the same machine from launching the same PDF at once; it is not
    presented as a distributed scheduler or a network-share database.
    """

    LEASE_SECONDS = 45.0
    # A controller lease has a different scope from a PDF-worker lease.  It is
    # used by long-lived batch/watch controllers so that a second stdio MCP
    # adapter can remain an observer instead of becoming a competing scheduler.
    CONTROLLER_LEASE_SECONDS = 45.0

    def __init__(
        self,
        *,
        database_path: str | Path | None = None,
        owner_id: str | None = None,
        store: TaskStore | None = None,
        process_controller: ProcessController | None = None,
    ) -> None:
        if store is not None and database_path is not None:
            raise ValueError("provide either store or database_path, not both")
        if store is None:
            resolved_database = (
                Path(database_path).expanduser().resolve()
                if database_path is not None
                else ensure_runtime_paths().state_dir / "tasks.sqlite3"
            )
            store = TaskStore(resolved_database)
        self.store = store
        self.controller = process_controller or ProcessController()
        self.owner_id = owner_id or _default_owner_id()

    @property
    def database_path(self) -> Path:
        return self.store.database_path

    def begin(
        self,
        *,
        source_path: str | Path,
        output_root: str | Path,
        mode: str,
        max_pages: int | None,
        resume: bool,
    ) -> SupervisedAttempt | None:
        """Claim and lease work, then create a pre-launch worker attempt.

        ``None`` means a different live adapter still owns this exact task.
        Secrets are intentionally absent from the persisted request and task
        key; passwords remain child-process-only inputs.
        """
        source = Path(source_path).expanduser().resolve()
        output = Path(output_root).expanduser().resolve()
        fingerprint = _source_fingerprint(source)
        options = {
            "output_root": str(output),
            "resume": bool(resume),
        }
        key = make_idempotency_key(
            fingerprint,
            mode,
            page_end=max_pages,
            options=options,
        )
        claim = self.store.claim_task(
            idempotency_key=key,
            source_path=source,
            source_fingerprint=fingerprint,
            output_root=output,
            mode=mode,
            request=options,
        )
        task = claim.task
        if task.is_terminal:
            # Starting a new extraction is an explicit user action.  Preserve the
            # completed attempt/event audit, then create the next durable attempt.
            task = self.store.requeue_task(task.job_id, reason="explicit extraction request")
        if task.state != TASK_QUEUED:
            return None

        lease = self.store.acquire_task_lease(
            task.job_id,
            owner_id=self.owner_id,
            ttl_seconds=self.LEASE_SECONDS,
        )
        if lease is None:
            return None
        try:
            attempt = self.store.start_attempt(
                task.job_id,
                supervisor_id=self.owner_id,
                metadata={"mode": mode, "max_pages": max_pages, "resume": bool(resume)},
            )
        except Exception:
            self.store.release_lease(
                self.store.task_resource_key(task.job_id),
                owner_id=self.owner_id,
                token=lease.token,
            )
            raise
        return SupervisedAttempt(task=task, attempt=attempt, lease=lease)

    def child_environment(self, context: SupervisedAttempt) -> dict[str, str]:
        """Return only non-secret task identity variables for an OCR child."""
        return {
            TASK_DATABASE_ENV: str(self.database_path),
            TASK_ATTEMPT_ENV: context.attempt_id,
        }

    def bind_worker(self, context: SupervisedAttempt, worker: WorkerHandle) -> Attempt:
        """Attach a PID-plus-create-time identity after a successful spawn."""
        return self.store.record_heartbeat(
            context.attempt_id,
            worker_pid=worker.identity.pid,
            worker_started_at=worker.identity.create_time,
        )

    def renew(self, context: SupervisedAttempt) -> bool:
        """Renew an owned lease; a false result means this supervisor lost fencing."""
        return (
            self.store.renew_lease(
                context.lease.resource_key,
                owner_id=self.owner_id,
                token=context.lease.token,
                ttl_seconds=self.LEASE_SECONDS,
            )
            is not None
        )

    def acquire_controller_lease(
        self,
        resource_key: str,
        *,
        ttl_seconds: float | None = None,
    ) -> Lease | None:
        """Acquire a local controller lease without creating an OCR task.

        ``resource_key`` deliberately belongs to the supervision layer rather
        than a FastMCP adapter.  This lets stdio, HTTP, VS Code, Trae, Codex,
        and AnythingLLM share one local controller while every other adapter
        performs observation only.
        """
        return self.store.acquire_lease(
            f"controller:{resource_key}",
            owner_id=self.owner_id,
            ttl_seconds=ttl_seconds or self.CONTROLLER_LEASE_SECONDS,
        )

    def renew_controller_lease(
        self,
        lease: Lease,
        *,
        ttl_seconds: float | None = None,
    ) -> Lease | None:
        """Renew exactly the controller fencing token that this host owns."""
        return self.store.renew_lease(
            lease.resource_key,
            owner_id=self.owner_id,
            token=lease.token,
            ttl_seconds=ttl_seconds or self.CONTROLLER_LEASE_SECONDS,
        )

    def release_controller_lease(self, lease: Lease) -> bool:
        """Release only this host's exact controller fencing token."""
        return self.store.release_lease(
            lease.resource_key,
            owner_id=self.owner_id,
            token=lease.token,
        )

    def owns_controller_lease(self, lease: Lease) -> bool:
        """Return whether this exact, unexpired controller token still owns work."""
        current = self.store.get_lease(lease.resource_key)
        return (
            current is not None
            and current.owner_id == self.owner_id
            and current.token == lease.token
        )

    def reattach(
        self,
        *,
        job_id: str,
        attempt_id: str | None,
    ) -> SupervisedAttempt | None:
        """Claim monitoring of an unleased durable attempt after a restart.

        A restored adapter must never start a watcher merely because it can
        read task metadata.  It first acquires the same fenced task lease as a
        launcher; only the winner can supervise or recover the existing
        attempt.  A fresh heartbeat from an old worker remains valid during the
        handoff because this method does not rewrite its attempt record.
        """
        if not attempt_id:
            return None
        try:
            task = self.store.get_task(job_id)
        except Exception:
            return None
        if task.is_terminal:
            return None
        try:
            lease = self.store.acquire_task_lease(
                job_id,
                owner_id=self.owner_id,
                ttl_seconds=self.LEASE_SECONDS,
            )
        except Exception:
            return None
        if lease is None:
            return None
        try:
            attempt = next(
                item for item in self.store.list_attempts(job_id)
                if item.attempt_id == attempt_id
            )
        except (StopIteration, Exception):
            self.store.release_lease(
                lease.resource_key,
                owner_id=self.owner_id,
                token=lease.token,
            )
            return None
        return SupervisedAttempt(task=task, attempt=attempt, lease=lease)

    def request_cancel(self, context: SupervisedAttempt, *, reason: str) -> Task:
        """Persist a stop request before a caller signals the process tree."""
        return self.store.request_cancel(context.job_id, reason=reason)

    def recover_orphan(self, job_id: str, *, reason: str) -> bool:
        """Requeue an unleased active attempt after a supervisor crash.

        Callers must first verify that the recorded worker is no longer alive.
        A still-valid lease is treated as positive evidence of another active
        adapter, so this method never steals a task merely because a process
        restarted.
        """
        task = self.store.get_task(job_id)
        if task.current_attempt_id is None:
            return task.state == TASK_QUEUED
        if self.store.get_lease(self.store.task_resource_key(job_id)) is not None:
            return False
        try:
            task = self.store.finish_attempt(
                task.current_attempt_id,
                outcome=ATTEMPT_STALLED,
                retry=True,
                error={"reason": reason},
            )
        except InvalidTransitionError:
            return False
        if task.is_terminal:
            # This can occur when an old attempt had already recorded a durable
            # cancellation.  The caller's explicit resume request is a new intent.
            task = self.store.requeue_task(job_id, reason="explicit orphan recovery")
        return task.state == TASK_QUEUED

    def settle(
        self,
        context: SupervisedAttempt,
        *,
        outcome: str,
        retry: bool = False,
        result: Mapping[str, Any] | None = None,
        error: Mapping[str, Any] | None = None,
        exit_code: int | None = None,
    ) -> Task | None:
        """Finish an attempt and always release its exact fencing lease.

        It is safe for status polling to race a worker's final heartbeat: an
        already-settled attempt is left intact instead of rewriting history.
        """
        try:
            return self.store.finish_attempt(
                context.attempt_id,
                outcome=outcome,
                retry=retry,
                result=dict(result or {}),
                error=dict(error or {}),
                exit_code=exit_code,
            )
        except InvalidTransitionError:
            return None
        finally:
            self.store.release_lease(
                context.lease.resource_key,
                owner_id=self.owner_id,
                token=context.lease.token,
            )

    def task_snapshot(
        self,
        job_id: str,
        *,
        event_limit: int = 100,
        include_pages: bool = False,
    ) -> dict[str, Any]:
        """Expose read-only supervision evidence for a host-neutral MCP response."""
        task = self.store.get_task(job_id)
        attempts = self.store.list_attempts(job_id)
        pages = self.store.list_page_progress(job_id, limit=10_000) if include_pages else []
        events = self.store.list_events(job_id, limit=event_limit)
        lease = self.store.get_lease(self.store.task_resource_key(job_id))
        return {
            "task": asdict(task),
            "attempts": [asdict(attempt) for attempt in attempts],
            "pages": [asdict(page) for page in pages],
            "events": [asdict(event) for event in events],
            "lease": asdict(lease) if lease is not None else None,
            "database_path": str(self.database_path),
        }

    @staticmethod
    def outcome_for_status(status: str | None) -> str:
        """Map existing Chinese business states onto the durable attempt machine."""
        if status == "完成":
            return ATTEMPT_COMPLETED
        if status == "已取消":
            return ATTEMPT_CANCELLED
        if status == "卡死":
            return ATTEMPT_STALLED
        return ATTEMPT_FAILED


def _source_fingerprint(source: Path) -> str:
    """Fast local fingerprint for idempotency without reading an entire PDF."""
    try:
        stat = source.stat()
        material = f"{source}|{stat.st_size}|{stat.st_mtime_ns}"
    except OSError:
        material = str(source)
    digest = hashlib.sha256(material.encode("utf-8", errors="surrogatepass")).hexdigest()
    return f"local-stat-sha256:{digest}"


def _default_owner_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:12]}"
