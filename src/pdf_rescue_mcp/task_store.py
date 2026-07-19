"""Portable, durable task state for the PDF rescue supervisor.

This module deliberately has no dependency on FastMCP, OCR engines, or a
platform-specific process API.  It is the durable boundary between the three
product layers:

* the business layer emits attempt, heartbeat, and page events;
* the supervision layer owns leases and state transitions; and
* the iteration layer reads the immutable event history and page outcomes.

``TaskStore`` is intended for a *local* SQLite database.  SQLite WAL is useful
for a single machine with several local readers, but the database must not be
put on a network share, synchronized folder, or shared filesystem.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1

TASK_QUEUED = "queued"
TASK_RUNNING = "running"
TASK_CANCELLING = "cancelling"
TASK_COMPLETED = "completed"
TASK_FAILED = "failed"
TASK_CANCELLED = "cancelled"
TASK_STALLED = "stalled"

ATTEMPT_RUNNING = "running"
ATTEMPT_CANCELLING = "cancelling"
ATTEMPT_COMPLETED = "completed"
ATTEMPT_FAILED = "failed"
ATTEMPT_CANCELLED = "cancelled"
ATTEMPT_STALLED = "stalled"

PAGE_PENDING = "pending"
PAGE_RUNNING = "running"
PAGE_COMPLETED = "completed"
PAGE_FAILED = "failed"

TERMINAL_TASK_STATES = frozenset(
    {TASK_COMPLETED, TASK_FAILED, TASK_CANCELLED, TASK_STALLED}
)
TERMINAL_ATTEMPT_STATES = frozenset(
    {ATTEMPT_COMPLETED, ATTEMPT_FAILED, ATTEMPT_CANCELLED, ATTEMPT_STALLED}
)
ACTIVE_ATTEMPT_STATES = frozenset({ATTEMPT_RUNNING, ATTEMPT_CANCELLING})

_TASK_STATES = frozenset(
    {
        TASK_QUEUED,
        TASK_RUNNING,
        TASK_CANCELLING,
        TASK_COMPLETED,
        TASK_FAILED,
        TASK_CANCELLED,
        TASK_STALLED,
    }
)
_ATTEMPT_STATES = frozenset(
    {
        ATTEMPT_RUNNING,
        ATTEMPT_CANCELLING,
        ATTEMPT_COMPLETED,
        ATTEMPT_FAILED,
        ATTEMPT_CANCELLED,
        ATTEMPT_STALLED,
    }
)
_PAGE_STATES = frozenset({PAGE_PENDING, PAGE_RUNNING, PAGE_COMPLETED, PAGE_FAILED})
_SENSITIVE_OPTION_KEYS = frozenset(
    {
        "api_key",
        "authorization",
        "auth_token",
        "password",
        "passphrase",
        "secret",
        "token",
    }
)


class TaskStoreError(RuntimeError):
    """Base error raised by the persistent task state layer."""


class TaskNotFoundError(TaskStoreError):
    """Raised when a requested task does not exist."""


class AttemptNotFoundError(TaskStoreError):
    """Raised when a requested attempt does not exist."""


class TaskConflictError(TaskStoreError):
    """Raised when a concurrent or incompatible operation owns the task."""


class InvalidTransitionError(TaskStoreError):
    """Raised when an operation would violate the durable state machine."""


class SensitiveValueError(TaskStoreError):
    """Raised before an obvious secret can be persisted in task options or events."""


@dataclass(frozen=True, slots=True)
class Task:
    """A durable OCR job, independent of any one worker attempt."""

    job_id: str
    idempotency_key: str
    source_path: str
    source_fingerprint: str | None
    output_root: str | None
    mode: str
    request: Any
    state: str
    created_at: float
    updated_at: float
    started_at: float | None
    finished_at: float | None
    total_pages: int | None
    completed_pages: int
    failed_pages: int
    current_page: int | None
    current_page_started_at: float | None
    last_completed_page: int | None
    last_progress_at: float | None
    last_heartbeat_at: float | None
    current_attempt_id: str | None
    latest_attempt_number: int
    cancellation_requested_at: float | None
    cancellation_reason: str | None
    result: Any
    error: Any

    @property
    def progress_fraction(self) -> float | None:
        """Return the page completion fraction when the total is known."""
        if self.total_pages is None or self.total_pages <= 0:
            return None
        return min(1.0, self.completed_pages / self.total_pages)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_TASK_STATES


@dataclass(frozen=True, slots=True)
class TaskClaim:
    """Result of atomically claiming an idempotency key."""

    task: Task
    created: bool


@dataclass(frozen=True, slots=True)
class Attempt:
    """One isolated worker execution of a task."""

    attempt_id: str
    job_id: str
    attempt_number: int
    supervisor_id: str
    state: str
    worker_pid: int | None
    worker_started_at: float | None
    created_at: float
    started_at: float
    last_heartbeat_at: float | None
    last_progress_at: float | None
    current_page: int | None
    current_page_started_at: float | None
    completed_pages: int
    failed_pages: int
    finished_at: float | None
    exit_code: int | None
    result: Any
    error: Any
    metadata: Any

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_ATTEMPT_STATES


@dataclass(frozen=True, slots=True)
class Lease:
    """A fencing token for one local supervisor's temporary ownership."""

    resource_key: str
    owner_id: str
    token: str
    acquired_at: float
    expires_at: float

    def is_valid_at(self, now: float) -> bool:
        return self.expires_at > now


@dataclass(frozen=True, slots=True)
class TaskLease:
    """A queued task paired with its acquired task lease."""

    task: Task
    lease: Lease


@dataclass(frozen=True, slots=True)
class PageProgress:
    """Latest durable state of a page, not a volatile worker-only counter."""

    job_id: str
    page_number: int
    state: str
    attempt_id: str | None
    started_at: float | None
    completed_at: float | None
    updated_at: float
    result: Any
    error: Any


@dataclass(frozen=True, slots=True)
class TaskEvent:
    """Append-only event suitable for audit and future policy iteration."""

    event_id: int
    job_id: str
    attempt_id: str | None
    event_type: str
    payload: Any
    created_at: float


def make_idempotency_key(
    source_fingerprint: str,
    mode: str,
    *,
    page_start: int | None = None,
    page_end: int | None = None,
    options: Mapping[str, Any] | None = None,
) -> str:
    """Build a stable key from the work definition rather than a source path.

    ``source_fingerprint`` should be supplied by the caller (for example a
    content hash plus file size).  The store intentionally does not hash PDF
    files itself because a state operation must stay fast and side-effect free.
    """
    _require_text(source_fingerprint, "source_fingerprint")
    _require_text(mode, "mode")
    _validate_page_range(page_start, page_end)
    request = dict(options or {})
    _reject_sensitive_keys(request)
    payload = {
        "source_fingerprint": source_fingerprint,
        "mode": mode,
        "page_start": page_start,
        "page_end": page_end,
        "options": request,
    }
    return "sha256:" + hashlib.sha256(_encode_json(payload).encode("utf-8")).hexdigest()


class TaskStore:
    """SQLite-backed task, attempt, event, page, and lease repository.

    Every write uses ``BEGIN IMMEDIATE``.  This gives a small local supervisor
    a clear single-writer boundary while still allowing separate MCP adapters
    and status readers to share one database safely.  The class opens a fresh
    SQLite connection per operation, so instances can be used by multiple
    threads and processes on the same machine.
    """

    TASK_LEASE_PREFIX = "task:"

    def __init__(self, database_path: str | Path, *, timeout_seconds: float = 5.0) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero")
        self.path = Path(database_path).expanduser().resolve()
        self.timeout_seconds = float(timeout_seconds)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialise()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            str(self.path),
            timeout=self.timeout_seconds,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {int(self.timeout_seconds * 1000)}")
        return connection

    def _initialise(self) -> None:
        with self._connect() as connection:
            # WAL is a local-disk optimisation, not a network-filesystem protocol.
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS task_store_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    job_id TEXT PRIMARY KEY,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    source_path TEXT NOT NULL,
                    source_fingerprint TEXT,
                    output_root TEXT,
                    mode TEXT NOT NULL,
                    request_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    total_pages INTEGER,
                    completed_pages INTEGER NOT NULL DEFAULT 0,
                    failed_pages INTEGER NOT NULL DEFAULT 0,
                    current_page INTEGER,
                    current_page_started_at REAL,
                    last_completed_page INTEGER,
                    last_progress_at REAL,
                    last_heartbeat_at REAL,
                    current_attempt_id TEXT,
                    latest_attempt_number INTEGER NOT NULL DEFAULT 0,
                    cancellation_requested_at REAL,
                    cancellation_reason TEXT,
                    result_json TEXT,
                    error_json TEXT,
                    CHECK (state IN ('queued', 'running', 'cancelling', 'completed', 'failed', 'cancelled', 'stalled')),
                    CHECK (total_pages IS NULL OR total_pages >= 0),
                    CHECK (completed_pages >= 0),
                    CHECK (failed_pages >= 0)
                );

                CREATE TABLE IF NOT EXISTS attempts (
                    attempt_id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL REFERENCES tasks(job_id) ON DELETE CASCADE,
                    attempt_number INTEGER NOT NULL,
                    supervisor_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    worker_pid INTEGER,
                    worker_started_at REAL,
                    created_at REAL NOT NULL,
                    started_at REAL NOT NULL,
                    last_heartbeat_at REAL,
                    last_progress_at REAL,
                    current_page INTEGER,
                    current_page_started_at REAL,
                    completed_pages INTEGER NOT NULL DEFAULT 0,
                    failed_pages INTEGER NOT NULL DEFAULT 0,
                    finished_at REAL,
                    exit_code INTEGER,
                    result_json TEXT,
                    error_json TEXT,
                    metadata_json TEXT NOT NULL,
                    UNIQUE (job_id, attempt_number),
                    CHECK (state IN ('running', 'cancelling', 'completed', 'failed', 'cancelled', 'stalled')),
                    CHECK (completed_pages >= 0),
                    CHECK (failed_pages >= 0)
                );

                CREATE UNIQUE INDEX IF NOT EXISTS attempts_one_active_per_task
                ON attempts(job_id)
                WHERE state IN ('running', 'cancelling');

                CREATE TABLE IF NOT EXISTS page_progress (
                    job_id TEXT NOT NULL REFERENCES tasks(job_id) ON DELETE CASCADE,
                    page_number INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
                    started_at REAL,
                    completed_at REAL,
                    updated_at REAL NOT NULL,
                    result_json TEXT,
                    error_json TEXT,
                    PRIMARY KEY (job_id, page_number),
                    CHECK (page_number >= 1),
                    CHECK (state IN ('pending', 'running', 'completed', 'failed'))
                );

                CREATE TABLE IF NOT EXISTS task_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT NOT NULL REFERENCES tasks(job_id) ON DELETE CASCADE,
                    attempt_id TEXT REFERENCES attempts(attempt_id) ON DELETE SET NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS leases (
                    resource_key TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    token TEXT NOT NULL UNIQUE,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    CHECK (expires_at > acquired_at)
                );

                CREATE INDEX IF NOT EXISTS tasks_state_created
                ON tasks(state, created_at, job_id);
                CREATE INDEX IF NOT EXISTS attempts_job_number
                ON attempts(job_id, attempt_number);
                CREATE INDEX IF NOT EXISTS page_progress_job_state
                ON page_progress(job_id, state, page_number);
                CREATE INDEX IF NOT EXISTS task_events_job_event
                ON task_events(job_id, event_id);
                CREATE INDEX IF NOT EXISTS leases_expiry
                ON leases(expires_at);
                """
            )
            connection.execute(
                "INSERT OR REPLACE INTO task_store_meta(key, value) VALUES (?, ?)",
                ("schema_version", str(SCHEMA_VERSION)),
            )

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()

    @property
    def database_path(self) -> Path:
        """The resolved local SQLite path used by this store."""
        return self.path

    @staticmethod
    def task_resource_key(job_id: str) -> str:
        """Return the lease resource key reserved for a task."""
        return f"{TaskStore.TASK_LEASE_PREFIX}{_require_text(job_id, 'job_id')}"

    def claim_task(
        self,
        *,
        idempotency_key: str,
        source_path: str | Path,
        mode: str,
        source_fingerprint: str | None = None,
        output_root: str | Path | None = None,
        request: Mapping[str, Any] | None = None,
        total_pages: int | None = None,
        now: float | None = None,
    ) -> TaskClaim:
        """Create a task once, or return the existing task for the same key.

        No passwords or tokens may be placed in ``request``.  Credentials are
        runtime-only worker inputs and should be supplied through a separate,
        non-persisted channel.
        """
        key = _require_text(idempotency_key, "idempotency_key")
        path = _require_text(str(source_path), "source_path")
        selected_mode = _require_text(mode, "mode")
        fingerprint = _optional_text(source_fingerprint, "source_fingerprint")
        root = str(output_root) if output_root is not None else None
        if root is not None:
            root = _require_text(root, "output_root")
        _validate_total_pages(total_pages)
        request_data = dict(request or {})
        _reject_sensitive_keys(request_data)
        request_json = _encode_json(request_data)
        timestamp = _now(now)

        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?", (key,)
            ).fetchone()
            if existing is not None:
                return TaskClaim(_task_from_row(existing), created=False)

            job_id = uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO tasks(
                    job_id, idempotency_key, source_path, source_fingerprint,
                    output_root, mode, request_json, state, created_at, updated_at,
                    total_pages
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    key,
                    path,
                    fingerprint,
                    root,
                    selected_mode,
                    request_json,
                    TASK_QUEUED,
                    timestamp,
                    timestamp,
                    total_pages,
                ),
            )
            self._append_event(
                connection,
                job_id=job_id,
                attempt_id=None,
                event_type="task_claimed",
                payload={"idempotency_key": key, "mode": selected_mode},
                now=timestamp,
            )
            row = _require_task_row(connection, job_id)
            return TaskClaim(_task_from_row(row), created=True)

    def get_task(self, job_id: str) -> Task:
        """Read one task or raise :class:`TaskNotFoundError`."""
        with self._reader() as connection:
            return _task_from_row(_require_task_row(connection, _require_text(job_id, "job_id")))

    def get_task_by_idempotency_key(self, idempotency_key: str) -> Task | None:
        """Return the task for a caller's deterministic key, if any."""
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM tasks WHERE idempotency_key = ?",
                (_require_text(idempotency_key, "idempotency_key"),),
            ).fetchone()
            return _task_from_row(row) if row is not None else None

    def list_tasks(
        self,
        *,
        states: Sequence[str] | None = None,
        limit: int = 100,
    ) -> list[Task]:
        """List tasks in creation order, optionally restricted to known states."""
        _validate_limit(limit)
        selected_states = tuple(states or ())
        if any(state not in _TASK_STATES for state in selected_states):
            raise ValueError("states must contain only known task states")
        query = "SELECT * FROM tasks"
        arguments: list[Any] = []
        if selected_states:
            query += " WHERE state IN (" + ", ".join("?" for _ in selected_states) + ")"
            arguments.extend(selected_states)
        query += " ORDER BY created_at, job_id LIMIT ?"
        arguments.append(limit)
        with self._reader() as connection:
            return [_task_from_row(row) for row in connection.execute(query, arguments)]

    def set_total_pages(self, job_id: str, total_pages: int, *, now: float | None = None) -> Task:
        """Record the canonical page count once inspection has completed."""
        _validate_total_pages(total_pages)
        assert total_pages is not None
        timestamp = _now(now)
        with self._transaction() as connection:
            task = _require_task_row(connection, _require_text(job_id, "job_id"))
            if total_pages < int(task["completed_pages"]):
                raise InvalidTransitionError("total_pages cannot be below completed_pages")
            connection.execute(
                "UPDATE tasks SET total_pages = ?, updated_at = ? WHERE job_id = ?",
                (total_pages, timestamp, task["job_id"]),
            )
            self._append_event(
                connection,
                job_id=task["job_id"],
                attempt_id=None,
                event_type="total_pages_recorded",
                payload={"total_pages": total_pages},
                now=timestamp,
            )
            return _task_from_row(_require_task_row(connection, task["job_id"]))

    def start_attempt(
        self,
        job_id: str,
        *,
        supervisor_id: str,
        worker_pid: int | None = None,
        worker_started_at: float | None = None,
        metadata: Mapping[str, Any] | None = None,
        now: float | None = None,
    ) -> Attempt:
        """Create the only active attempt allowed for a task.

        Lease ownership is deliberately not implicit here.  A supervisor can
        choose its own queue policy, but should acquire a task lease before
        invoking this method.
        """
        target_job = _require_text(job_id, "job_id")
        owner = _require_text(supervisor_id, "supervisor_id")
        _validate_pid(worker_pid)
        _validate_optional_timestamp(worker_started_at, "worker_started_at")
        metadata_data = dict(metadata or {})
        _reject_sensitive_keys(metadata_data)
        timestamp = _now(now)
        with self._transaction() as connection:
            task = _require_task_row(connection, target_job)
            if task["state"] in TERMINAL_TASK_STATES:
                raise InvalidTransitionError(f"cannot start an attempt for terminal task {target_job}")
            if task["state"] == TASK_CANCELLING:
                raise InvalidTransitionError(f"cannot start an attempt while task {target_job} is cancelling")
            active = connection.execute(
                "SELECT attempt_id FROM attempts WHERE job_id = ? AND state IN ('running', 'cancelling')",
                (target_job,),
            ).fetchone()
            if active is not None:
                raise TaskConflictError(f"task {target_job} already has active attempt {active['attempt_id']}")

            attempt_id = uuid.uuid4().hex
            attempt_number = int(task["latest_attempt_number"]) + 1
            connection.execute(
                """
                INSERT INTO attempts(
                    attempt_id, job_id, attempt_number, supervisor_id, state,
                    worker_pid, worker_started_at, created_at, started_at,
                    last_heartbeat_at, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    attempt_id,
                    target_job,
                    attempt_number,
                    owner,
                    ATTEMPT_RUNNING,
                    worker_pid,
                    worker_started_at,
                    timestamp,
                    timestamp,
                    timestamp,
                    _encode_json(metadata_data),
                ),
            )
            connection.execute(
                """
                UPDATE tasks
                SET state = ?, updated_at = ?, started_at = COALESCE(started_at, ?),
                    last_heartbeat_at = ?, current_attempt_id = ?,
                    latest_attempt_number = ?
                WHERE job_id = ?
                """,
                (
                    TASK_RUNNING,
                    timestamp,
                    timestamp,
                    timestamp,
                    attempt_id,
                    attempt_number,
                    target_job,
                ),
            )
            self._append_event(
                connection,
                job_id=target_job,
                attempt_id=attempt_id,
                event_type="attempt_started",
                payload={
                    "attempt_number": attempt_number,
                    "supervisor_id": owner,
                    "worker_pid": worker_pid,
                },
                now=timestamp,
            )
            return _attempt_from_row(_require_attempt_row(connection, attempt_id))

    def get_attempt(self, attempt_id: str) -> Attempt:
        """Read one worker attempt."""
        with self._reader() as connection:
            return _attempt_from_row(
                _require_attempt_row(connection, _require_text(attempt_id, "attempt_id"))
            )

    def list_attempts(self, job_id: str) -> list[Attempt]:
        """Return all attempts in durable execution order."""
        with self._reader() as connection:
            _require_task_row(connection, _require_text(job_id, "job_id"))
            rows = connection.execute(
                "SELECT * FROM attempts WHERE job_id = ? ORDER BY attempt_number", (job_id,)
            )
            return [_attempt_from_row(row) for row in rows]

    def record_heartbeat(
        self,
        attempt_id: str,
        *,
        worker_pid: int | None = None,
        worker_started_at: float | None = None,
        now: float | None = None,
    ) -> Attempt:
        """Persist a liveness update without confusing it with page progress."""
        _validate_pid(worker_pid)
        _validate_optional_timestamp(worker_started_at, "worker_started_at")
        timestamp = _now(now)
        with self._transaction() as connection:
            attempt = _require_active_attempt(connection, _require_text(attempt_id, "attempt_id"))
            connection.execute(
                """
                UPDATE attempts
                SET last_heartbeat_at = ?,
                    worker_pid = COALESCE(?, worker_pid),
                    worker_started_at = COALESCE(?, worker_started_at)
                WHERE attempt_id = ?
                """,
                (timestamp, worker_pid, worker_started_at, attempt["attempt_id"]),
            )
            connection.execute(
                "UPDATE tasks SET last_heartbeat_at = ?, updated_at = ? WHERE job_id = ?",
                (timestamp, timestamp, attempt["job_id"]),
            )
            return _attempt_from_row(_require_attempt_row(connection, attempt["attempt_id"]))

    def record_page_started(
        self,
        attempt_id: str,
        page_number: int,
        *,
        now: float | None = None,
    ) -> PageProgress:
        """Mark a page as actively processed and expose a progress watchdog point."""
        page = _validate_page_number(page_number)
        timestamp = _now(now)
        with self._transaction() as connection:
            attempt = _require_active_attempt(connection, _require_text(attempt_id, "attempt_id"))
            task = _require_current_task_for_attempt(connection, attempt)
            _validate_page_against_task(task, page)
            existing = _page_row(connection, task["job_id"], page)
            if existing is not None and existing["state"] == PAGE_COMPLETED:
                raise InvalidTransitionError(f"page {page} is already completed")

            failed_delta = -1 if existing is not None and existing["state"] == PAGE_FAILED else 0
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO page_progress(
                        job_id, page_number, state, attempt_id, started_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (task["job_id"], page, PAGE_RUNNING, attempt["attempt_id"], timestamp, timestamp),
                )
            else:
                connection.execute(
                    """
                    UPDATE page_progress
                    SET state = ?, attempt_id = ?, started_at = ?, completed_at = NULL,
                        updated_at = ?, error_json = NULL
                    WHERE job_id = ? AND page_number = ?
                    """,
                    (PAGE_RUNNING, attempt["attempt_id"], timestamp, timestamp, task["job_id"], page),
                )
            connection.execute(
                """
                UPDATE attempts
                SET current_page = ?, current_page_started_at = ?, last_heartbeat_at = ?
                WHERE attempt_id = ?
                """,
                (page, timestamp, timestamp, attempt["attempt_id"]),
            )
            connection.execute(
                """
                UPDATE tasks
                SET current_page = ?, current_page_started_at = ?, last_heartbeat_at = ?,
                    failed_pages = failed_pages + ?, updated_at = ?
                WHERE job_id = ?
                """,
                (page, timestamp, timestamp, failed_delta, timestamp, task["job_id"]),
            )
            self._append_event(
                connection,
                job_id=task["job_id"],
                attempt_id=attempt["attempt_id"],
                event_type="page_started",
                payload={"page_number": page},
                now=timestamp,
            )
            return _page_from_row(_require_page_row(connection, task["job_id"], page))

    def record_page_completed(
        self,
        attempt_id: str,
        page_number: int,
        *,
        result: Any = None,
        now: float | None = None,
    ) -> PageProgress:
        """Durably finish a page and increment the task count only once."""
        page = _validate_page_number(page_number)
        _reject_sensitive_keys(result)
        timestamp = _now(now)
        with self._transaction() as connection:
            attempt = _require_active_attempt(connection, _require_text(attempt_id, "attempt_id"))
            task = _require_current_task_for_attempt(connection, attempt)
            _validate_page_against_task(task, page)
            existing = _page_row(connection, task["job_id"], page)
            if existing is not None and existing["state"] == PAGE_COMPLETED:
                # At-least-once worker delivery must not double count a completed page.
                return _page_from_row(existing)

            failed_delta = -1 if existing is not None and existing["state"] == PAGE_FAILED else 0
            completed_delta = 1
            result_json = _encode_json(result)
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO page_progress(
                        job_id, page_number, state, attempt_id, started_at, completed_at,
                        updated_at, result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task["job_id"],
                        page,
                        PAGE_COMPLETED,
                        attempt["attempt_id"],
                        timestamp,
                        timestamp,
                        timestamp,
                        result_json,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE page_progress
                    SET state = ?, attempt_id = ?, completed_at = ?, updated_at = ?,
                        result_json = ?, error_json = NULL
                    WHERE job_id = ? AND page_number = ?
                    """,
                    (
                        PAGE_COMPLETED,
                        attempt["attempt_id"],
                        timestamp,
                        timestamp,
                        result_json,
                        task["job_id"],
                        page,
                    ),
                )
            connection.execute(
                """
                UPDATE attempts
                SET current_page = CASE WHEN current_page = ? THEN NULL ELSE current_page END,
                    current_page_started_at = CASE WHEN current_page = ? THEN NULL ELSE current_page_started_at END,
                    last_heartbeat_at = ?, last_progress_at = ?,
                    completed_pages = completed_pages + ?
                WHERE attempt_id = ?
                """,
                (page, page, timestamp, timestamp, completed_delta, attempt["attempt_id"]),
            )
            connection.execute(
                """
                UPDATE tasks
                SET completed_pages = completed_pages + ?, failed_pages = failed_pages + ?,
                    current_page = CASE WHEN current_page = ? THEN NULL ELSE current_page END,
                    current_page_started_at = CASE WHEN current_page = ? THEN NULL ELSE current_page_started_at END,
                    last_completed_page = ?, last_progress_at = ?, last_heartbeat_at = ?,
                    updated_at = ?
                WHERE job_id = ?
                """,
                (
                    completed_delta,
                    failed_delta,
                    page,
                    page,
                    page,
                    timestamp,
                    timestamp,
                    timestamp,
                    task["job_id"],
                ),
            )
            self._append_event(
                connection,
                job_id=task["job_id"],
                attempt_id=attempt["attempt_id"],
                event_type="page_completed",
                payload={"page_number": page},
                now=timestamp,
            )
            return _page_from_row(_require_page_row(connection, task["job_id"], page))

    def record_page_failed(
        self,
        attempt_id: str,
        page_number: int,
        *,
        error: Any = None,
        now: float | None = None,
    ) -> PageProgress:
        """Durably record an unresolved page failure without corrupting completion counts."""
        page = _validate_page_number(page_number)
        _reject_sensitive_keys(error)
        timestamp = _now(now)
        with self._transaction() as connection:
            attempt = _require_active_attempt(connection, _require_text(attempt_id, "attempt_id"))
            task = _require_current_task_for_attempt(connection, attempt)
            _validate_page_against_task(task, page)
            existing = _page_row(connection, task["job_id"], page)
            if existing is not None and existing["state"] == PAGE_COMPLETED:
                # A delayed failure report cannot undo a verified completed page.
                return _page_from_row(existing)
            if existing is not None and existing["state"] == PAGE_FAILED:
                return _page_from_row(existing)

            error_json = _encode_json(error)
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO page_progress(
                        job_id, page_number, state, attempt_id, started_at, completed_at,
                        updated_at, error_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task["job_id"],
                        page,
                        PAGE_FAILED,
                        attempt["attempt_id"],
                        timestamp,
                        timestamp,
                        timestamp,
                        error_json,
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE page_progress
                    SET state = ?, attempt_id = ?, completed_at = ?, updated_at = ?,
                        error_json = ?
                    WHERE job_id = ? AND page_number = ?
                    """,
                    (
                        PAGE_FAILED,
                        attempt["attempt_id"],
                        timestamp,
                        timestamp,
                        error_json,
                        task["job_id"],
                        page,
                    ),
                )
            connection.execute(
                """
                UPDATE attempts
                SET current_page = CASE WHEN current_page = ? THEN NULL ELSE current_page END,
                    current_page_started_at = CASE WHEN current_page = ? THEN NULL ELSE current_page_started_at END,
                    last_heartbeat_at = ?, last_progress_at = ?,
                    failed_pages = failed_pages + 1
                WHERE attempt_id = ?
                """,
                (page, page, timestamp, timestamp, attempt["attempt_id"]),
            )
            connection.execute(
                """
                UPDATE tasks
                SET failed_pages = failed_pages + 1,
                    current_page = CASE WHEN current_page = ? THEN NULL ELSE current_page END,
                    current_page_started_at = CASE WHEN current_page = ? THEN NULL ELSE current_page_started_at END,
                    last_progress_at = ?, last_heartbeat_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (page, page, timestamp, timestamp, timestamp, task["job_id"]),
            )
            self._append_event(
                connection,
                job_id=task["job_id"],
                attempt_id=attempt["attempt_id"],
                event_type="page_failed",
                payload={"page_number": page},
                now=timestamp,
            )
            return _page_from_row(_require_page_row(connection, task["job_id"], page))

    def get_page_progress(self, job_id: str, page_number: int) -> PageProgress | None:
        """Return one page's latest state, if it has emitted any event."""
        page = _validate_page_number(page_number)
        with self._reader() as connection:
            _require_task_row(connection, _require_text(job_id, "job_id"))
            row = _page_row(connection, job_id, page)
            return _page_from_row(row) if row is not None else None

    def list_page_progress(
        self,
        job_id: str,
        *,
        states: Sequence[str] | None = None,
        limit: int = 1000,
    ) -> list[PageProgress]:
        """Read page outcomes for resume planning and quality iteration."""
        _validate_limit(limit, maximum=10_000)
        selected_states = tuple(states or ())
        if any(state not in _PAGE_STATES for state in selected_states):
            raise ValueError("states must contain only known page states")
        target_job = _require_text(job_id, "job_id")
        query = "SELECT * FROM page_progress WHERE job_id = ?"
        arguments: list[Any] = [target_job]
        if selected_states:
            query += " AND state IN (" + ", ".join("?" for _ in selected_states) + ")"
            arguments.extend(selected_states)
        query += " ORDER BY page_number LIMIT ?"
        arguments.append(limit)
        with self._reader() as connection:
            _require_task_row(connection, target_job)
            return [_page_from_row(row) for row in connection.execute(query, arguments)]

    def request_cancel(
        self,
        job_id: str,
        *,
        reason: str | None = None,
        now: float | None = None,
    ) -> Task:
        """Persist cooperative cancellation before any process-controller action."""
        target_job = _require_text(job_id, "job_id")
        cancel_reason = _optional_text(reason, "reason")
        timestamp = _now(now)
        with self._transaction() as connection:
            task = _require_task_row(connection, target_job)
            if task["state"] in TERMINAL_TASK_STATES:
                return _task_from_row(task)
            active = connection.execute(
                "SELECT * FROM attempts WHERE job_id = ? AND state IN ('running', 'cancelling')",
                (target_job,),
            ).fetchone()
            if active is None:
                new_state = TASK_CANCELLED
                finished_at: float | None = timestamp
            else:
                new_state = TASK_CANCELLING
                finished_at = None
                connection.execute(
                    "UPDATE attempts SET state = ? WHERE attempt_id = ?",
                    (ATTEMPT_CANCELLING, active["attempt_id"]),
                )
            connection.execute(
                """
                UPDATE tasks
                SET state = ?, cancellation_requested_at = COALESCE(cancellation_requested_at, ?),
                    cancellation_reason = COALESCE(cancellation_reason, ?),
                    finished_at = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (new_state, timestamp, cancel_reason, finished_at, timestamp, target_job),
            )
            self._append_event(
                connection,
                job_id=target_job,
                attempt_id=active["attempt_id"] if active is not None else None,
                event_type="cancel_requested",
                payload={"reason": cancel_reason},
                now=timestamp,
            )
            return _task_from_row(_require_task_row(connection, target_job))

    def finish_attempt(
        self,
        attempt_id: str,
        *,
        outcome: str,
        retry: bool = False,
        result: Any = None,
        error: Any = None,
        exit_code: int | None = None,
        now: float | None = None,
    ) -> Task:
        """Finish an attempt and either terminally settle or requeue its task.

        Set ``retry=True`` after a failed or stalled attempt when the supervisor
        has decided that the existing page cache makes a safe retry possible.
        A recorded cancellation always wins over retry and settles as cancelled.
        """
        selected_outcome = _require_text(outcome, "outcome")
        if selected_outcome not in TERMINAL_ATTEMPT_STATES:
            raise ValueError(f"outcome must be one of {sorted(TERMINAL_ATTEMPT_STATES)}")
        if retry and selected_outcome in {ATTEMPT_COMPLETED, ATTEMPT_CANCELLED}:
            raise ValueError("only failed or stalled attempts may be retried")
        _reject_sensitive_keys(result)
        _reject_sensitive_keys(error)
        if exit_code is not None and not isinstance(exit_code, int):
            raise TypeError("exit_code must be an int or None")
        timestamp = _now(now)
        with self._transaction() as connection:
            attempt = _require_attempt_row(connection, _require_text(attempt_id, "attempt_id"))
            if attempt["state"] in TERMINAL_ATTEMPT_STATES:
                if attempt["state"] == selected_outcome:
                    return _task_from_row(_require_task_row(connection, attempt["job_id"]))
                raise InvalidTransitionError(
                    f"attempt {attempt_id} already finished as {attempt['state']}"
                )
            task = _require_current_task_for_attempt(connection, attempt)
            cancelled = task["cancellation_requested_at"] is not None
            if cancelled:
                next_task_state = TASK_CANCELLED
            elif retry:
                next_task_state = TASK_QUEUED
            else:
                next_task_state = _task_state_for_outcome(selected_outcome)
            terminal_task = next_task_state in TERMINAL_TASK_STATES
            connection.execute(
                """
                UPDATE attempts
                SET state = ?, finished_at = ?, exit_code = ?, result_json = ?, error_json = ?
                WHERE attempt_id = ?
                """,
                (
                    selected_outcome,
                    timestamp,
                    exit_code,
                    _encode_json(result),
                    _encode_json(error),
                    attempt["attempt_id"],
                ),
            )
            connection.execute(
                """
                UPDATE tasks
                SET state = ?, current_attempt_id = NULL, current_page = NULL,
                    current_page_started_at = NULL, updated_at = ?,
                    finished_at = CASE WHEN ? THEN ? ELSE NULL END,
                    result_json = CASE WHEN ? THEN ? ELSE result_json END,
                    error_json = CASE WHEN ? THEN ? ELSE error_json END
                WHERE job_id = ?
                """,
                (
                    next_task_state,
                    timestamp,
                    int(terminal_task),
                    timestamp,
                    int(terminal_task),
                    _encode_json(result),
                    int(terminal_task),
                    _encode_json(error),
                    task["job_id"],
                ),
            )
            self._append_event(
                connection,
                job_id=task["job_id"],
                attempt_id=attempt["attempt_id"],
                event_type="attempt_finished",
                payload={"outcome": selected_outcome, "retry": retry, "exit_code": exit_code},
                now=timestamp,
            )
            return _task_from_row(_require_task_row(connection, task["job_id"]))

    def requeue_task(
        self,
        job_id: str,
        *,
        reason: str | None = None,
        now: float | None = None,
    ) -> Task:
        """Explicitly place a settled task back in the queue for a new attempt."""
        target_job = _require_text(job_id, "job_id")
        retry_reason = _optional_text(reason, "reason")
        timestamp = _now(now)
        with self._transaction() as connection:
            task = _require_task_row(connection, target_job)
            if task["current_attempt_id"] is not None:
                raise TaskConflictError(f"task {target_job} still has an active attempt")
            if task["state"] not in TERMINAL_TASK_STATES:
                raise InvalidTransitionError(f"task {target_job} is not settled")
            connection.execute(
                """
                UPDATE tasks
                SET state = ?, updated_at = ?, finished_at = NULL,
                    cancellation_requested_at = NULL, cancellation_reason = NULL,
                    result_json = NULL, error_json = NULL
                WHERE job_id = ?
                """,
                (TASK_QUEUED, timestamp, target_job),
            )
            self._append_event(
                connection,
                job_id=target_job,
                attempt_id=None,
                event_type="task_requeued",
                payload={"reason": retry_reason},
                now=timestamp,
            )
            return _task_from_row(_require_task_row(connection, target_job))

    def append_event(
        self,
        job_id: str,
        event_type: str,
        *,
        payload: Any = None,
        attempt_id: str | None = None,
        now: float | None = None,
    ) -> TaskEvent:
        """Append a domain event for the iteration layer without mutating state."""
        target_job = _require_text(job_id, "job_id")
        selected_type = _require_text(event_type, "event_type")
        selected_attempt = _optional_text(attempt_id, "attempt_id")
        _reject_sensitive_keys(payload)
        timestamp = _now(now)
        with self._transaction() as connection:
            _require_task_row(connection, target_job)
            if selected_attempt is not None:
                attempt = _require_attempt_row(connection, selected_attempt)
                if attempt["job_id"] != target_job:
                    raise TaskConflictError("attempt does not belong to task")
            return self._append_event(
                connection,
                job_id=target_job,
                attempt_id=selected_attempt,
                event_type=selected_type,
                payload=payload,
                now=timestamp,
            )

    def list_events(
        self,
        job_id: str,
        *,
        after_event_id: int | None = None,
        limit: int = 1000,
    ) -> list[TaskEvent]:
        """Read append-only events in causal insertion order."""
        _validate_limit(limit, maximum=10_000)
        if after_event_id is not None and (not isinstance(after_event_id, int) or after_event_id < 0):
            raise ValueError("after_event_id must be a non-negative int or None")
        target_job = _require_text(job_id, "job_id")
        with self._reader() as connection:
            _require_task_row(connection, target_job)
            if after_event_id is None:
                rows = connection.execute(
                    "SELECT * FROM task_events WHERE job_id = ? ORDER BY event_id LIMIT ?",
                    (target_job, limit),
                )
            else:
                rows = connection.execute(
                    """
                    SELECT * FROM task_events
                    WHERE job_id = ? AND event_id > ?
                    ORDER BY event_id LIMIT ?
                    """,
                    (target_job, after_event_id, limit),
                )
            return [_event_from_row(row) for row in rows]

    def acquire_lease(
        self,
        resource_key: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> Lease | None:
        """Acquire a fencing-token lease, or return ``None`` if it is held.

        An owner must use :meth:`renew_lease` rather than repeatedly acquiring
        the same lease.  That guards against an old worker instance reviving a
        lease after a newer supervisor has taken over.
        """
        resource = _require_text(resource_key, "resource_key")
        owner = _require_text(owner_id, "owner_id")
        ttl = _validate_ttl(ttl_seconds)
        timestamp = _now(now)
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM leases WHERE resource_key = ?", (resource,)
            ).fetchone()
            if existing is not None and float(existing["expires_at"]) > timestamp:
                return None
            token = uuid.uuid4().hex
            expires_at = timestamp + ttl
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO leases(resource_key, owner_id, token, acquired_at, expires_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (resource, owner, token, timestamp, expires_at),
                )
            else:
                cursor = connection.execute(
                    """
                    UPDATE leases
                    SET owner_id = ?, token = ?, acquired_at = ?, expires_at = ?
                    WHERE resource_key = ? AND expires_at <= ?
                    """,
                    (owner, token, timestamp, expires_at, resource, timestamp),
                )
                if cursor.rowcount != 1:
                    return None
            return Lease(resource, owner, token, timestamp, expires_at)

    def renew_lease(
        self,
        resource_key: str,
        *,
        owner_id: str,
        token: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> Lease | None:
        """Extend a still-valid lease only when its exact fencing token matches."""
        resource = _require_text(resource_key, "resource_key")
        owner = _require_text(owner_id, "owner_id")
        selected_token = _require_text(token, "token")
        ttl = _validate_ttl(ttl_seconds)
        timestamp = _now(now)
        expires_at = timestamp + ttl
        with self._transaction() as connection:
            cursor = connection.execute(
                """
                UPDATE leases
                SET expires_at = ?
                WHERE resource_key = ? AND owner_id = ? AND token = ? AND expires_at > ?
                """,
                (expires_at, resource, owner, selected_token, timestamp),
            )
            if cursor.rowcount != 1:
                return None
            row = connection.execute(
                "SELECT * FROM leases WHERE resource_key = ?", (resource,)
            ).fetchone()
            assert row is not None
            return _lease_from_row(row)

    def release_lease(self, resource_key: str, *, owner_id: str, token: str) -> bool:
        """Release only the exact lease owner/token pair."""
        resource = _require_text(resource_key, "resource_key")
        owner = _require_text(owner_id, "owner_id")
        selected_token = _require_text(token, "token")
        with self._transaction() as connection:
            cursor = connection.execute(
                "DELETE FROM leases WHERE resource_key = ? AND owner_id = ? AND token = ?",
                (resource, owner, selected_token),
            )
            return cursor.rowcount == 1

    def get_lease(
        self,
        resource_key: str,
        *,
        now: float | None = None,
        include_expired: bool = False,
    ) -> Lease | None:
        """Read a lease; expired leases are hidden by default."""
        resource = _require_text(resource_key, "resource_key")
        timestamp = _now(now)
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM leases WHERE resource_key = ?", (resource,)
            ).fetchone()
            if row is None or (not include_expired and float(row["expires_at"]) <= timestamp):
                return None
            return _lease_from_row(row)

    def acquire_task_lease(
        self,
        job_id: str,
        *,
        owner_id: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> Lease | None:
        """Acquire the canonical lease for an existing task."""
        target_job = _require_text(job_id, "job_id")
        with self._reader() as connection:
            _require_task_row(connection, target_job)
        return self.acquire_lease(
            self.task_resource_key(target_job),
            owner_id=owner_id,
            ttl_seconds=ttl_seconds,
            now=now,
        )

    def claim_next_queued_task(
        self,
        *,
        owner_id: str,
        ttl_seconds: float,
        now: float | None = None,
    ) -> TaskLease | None:
        """Atomically lease the oldest queued task that is not already leased."""
        owner = _require_text(owner_id, "owner_id")
        ttl = _validate_ttl(ttl_seconds)
        timestamp = _now(now)
        with self._transaction() as connection:
            rows = connection.execute(
                "SELECT * FROM tasks WHERE state = ? ORDER BY created_at, job_id",
                (TASK_QUEUED,),
            ).fetchall()
            for task in rows:
                resource = self.task_resource_key(task["job_id"])
                existing = connection.execute(
                    "SELECT * FROM leases WHERE resource_key = ?", (resource,)
                ).fetchone()
                if existing is not None and float(existing["expires_at"]) > timestamp:
                    continue
                token = uuid.uuid4().hex
                expires_at = timestamp + ttl
                if existing is None:
                    connection.execute(
                        """
                        INSERT INTO leases(resource_key, owner_id, token, acquired_at, expires_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (resource, owner, token, timestamp, expires_at),
                    )
                else:
                    cursor = connection.execute(
                        """
                        UPDATE leases
                        SET owner_id = ?, token = ?, acquired_at = ?, expires_at = ?
                        WHERE resource_key = ? AND expires_at <= ?
                        """,
                        (owner, token, timestamp, expires_at, resource, timestamp),
                    )
                    if cursor.rowcount != 1:
                        continue
                lease = Lease(resource, owner, token, timestamp, expires_at)
                self._append_event(
                    connection,
                    job_id=task["job_id"],
                    attempt_id=None,
                    event_type="task_leased",
                    payload={"owner_id": owner, "expires_at": expires_at},
                    now=timestamp,
                )
                return TaskLease(_task_from_row(task), lease)
            return None

    def cleanup_expired_leases(self, *, now: float | None = None) -> int:
        """Delete expired lease rows; safe housekeeping, never a correctness step."""
        timestamp = _now(now)
        with self._transaction() as connection:
            cursor = connection.execute("DELETE FROM leases WHERE expires_at <= ?", (timestamp,))
            return int(cursor.rowcount)

    def _append_event(
        self,
        connection: sqlite3.Connection,
        *,
        job_id: str,
        attempt_id: str | None,
        event_type: str,
        payload: Any,
        now: float,
    ) -> TaskEvent:
        cursor = connection.execute(
            """
            INSERT INTO task_events(job_id, attempt_id, event_type, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (job_id, attempt_id, event_type, _encode_json(payload), now),
        )
        row = connection.execute(
            "SELECT * FROM task_events WHERE event_id = ?", (cursor.lastrowid,)
        ).fetchone()
        assert row is not None
        return _event_from_row(row)


def _require_text(value: object, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
    return value


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, name)


def _validate_total_pages(value: int | None) -> None:
    if value is not None and (not isinstance(value, int) or value < 0):
        raise ValueError("total_pages must be a non-negative int or None")


def _validate_page_number(value: int) -> int:
    if not isinstance(value, int) or value < 1:
        raise ValueError("page_number must be a positive int")
    return value


def _validate_page_range(page_start: int | None, page_end: int | None) -> None:
    if page_start is not None:
        _validate_page_number(page_start)
    if page_end is not None:
        _validate_page_number(page_end)
    if page_start is not None and page_end is not None and page_start > page_end:
        raise ValueError("page_start cannot be greater than page_end")


def _validate_page_against_task(task: sqlite3.Row, page_number: int) -> None:
    total_pages = task["total_pages"]
    if total_pages is not None and page_number > int(total_pages):
        raise ValueError(f"page_number {page_number} exceeds task total_pages {total_pages}")


def _validate_pid(value: int | None) -> None:
    if value is not None and (not isinstance(value, int) or value <= 0):
        raise ValueError("worker_pid must be a positive int or None")


def _validate_optional_timestamp(value: float | None, name: str) -> None:
    if value is not None and (not isinstance(value, (float, int)) or value < 0):
        raise ValueError(f"{name} must be a non-negative number or None")


def _validate_ttl(value: float) -> float:
    if not isinstance(value, (float, int)) or value <= 0:
        raise ValueError("ttl_seconds must be greater than zero")
    return float(value)


def _validate_limit(value: int, *, maximum: int = 1000) -> None:
    if not isinstance(value, int) or value < 1 or value > maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")


def _now(value: float | None) -> float:
    if value is None:
        return time.time()
    if not isinstance(value, (float, int)) or value < 0:
        raise ValueError("now must be a non-negative number or None")
    return float(value)


def _reject_sensitive_keys(value: Any) -> None:
    """Reject obvious key names before serialising long-lived state.

    This does not try to inspect free-form error strings.  It prevents the
    common accidental path of handing a password/options mapping to a durable
    store while keeping diagnostic text usable.
    """
    if isinstance(value, Mapping):
        for key, child in value.items():
            if isinstance(key, str) and key.lower() in _SENSITIVE_OPTION_KEYS:
                raise SensitiveValueError(f"{key!r} must not be persisted in TaskStore")
            _reject_sensitive_keys(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            _reject_sensitive_keys(child)


def _encode_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise TypeError("TaskStore values must be JSON serialisable") from exc


def _decode_json(value: str | None) -> Any:
    if value is None:
        return None
    return json.loads(value)


def _task_from_row(row: sqlite3.Row) -> Task:
    return Task(
        job_id=str(row["job_id"]),
        idempotency_key=str(row["idempotency_key"]),
        source_path=str(row["source_path"]),
        source_fingerprint=row["source_fingerprint"],
        output_root=row["output_root"],
        mode=str(row["mode"]),
        request=_decode_json(row["request_json"]),
        state=str(row["state"]),
        created_at=float(row["created_at"]),
        updated_at=float(row["updated_at"]),
        started_at=_nullable_float(row["started_at"]),
        finished_at=_nullable_float(row["finished_at"]),
        total_pages=row["total_pages"],
        completed_pages=int(row["completed_pages"]),
        failed_pages=int(row["failed_pages"]),
        current_page=row["current_page"],
        current_page_started_at=_nullable_float(row["current_page_started_at"]),
        last_completed_page=row["last_completed_page"],
        last_progress_at=_nullable_float(row["last_progress_at"]),
        last_heartbeat_at=_nullable_float(row["last_heartbeat_at"]),
        current_attempt_id=row["current_attempt_id"],
        latest_attempt_number=int(row["latest_attempt_number"]),
        cancellation_requested_at=_nullable_float(row["cancellation_requested_at"]),
        cancellation_reason=row["cancellation_reason"],
        result=_decode_json(row["result_json"]),
        error=_decode_json(row["error_json"]),
    )


def _attempt_from_row(row: sqlite3.Row) -> Attempt:
    return Attempt(
        attempt_id=str(row["attempt_id"]),
        job_id=str(row["job_id"]),
        attempt_number=int(row["attempt_number"]),
        supervisor_id=str(row["supervisor_id"]),
        state=str(row["state"]),
        worker_pid=row["worker_pid"],
        worker_started_at=_nullable_float(row["worker_started_at"]),
        created_at=float(row["created_at"]),
        started_at=float(row["started_at"]),
        last_heartbeat_at=_nullable_float(row["last_heartbeat_at"]),
        last_progress_at=_nullable_float(row["last_progress_at"]),
        current_page=row["current_page"],
        current_page_started_at=_nullable_float(row["current_page_started_at"]),
        completed_pages=int(row["completed_pages"]),
        failed_pages=int(row["failed_pages"]),
        finished_at=_nullable_float(row["finished_at"]),
        exit_code=row["exit_code"],
        result=_decode_json(row["result_json"]),
        error=_decode_json(row["error_json"]),
        metadata=_decode_json(row["metadata_json"]),
    )


def _lease_from_row(row: sqlite3.Row) -> Lease:
    return Lease(
        resource_key=str(row["resource_key"]),
        owner_id=str(row["owner_id"]),
        token=str(row["token"]),
        acquired_at=float(row["acquired_at"]),
        expires_at=float(row["expires_at"]),
    )


def _page_from_row(row: sqlite3.Row) -> PageProgress:
    return PageProgress(
        job_id=str(row["job_id"]),
        page_number=int(row["page_number"]),
        state=str(row["state"]),
        attempt_id=row["attempt_id"],
        started_at=_nullable_float(row["started_at"]),
        completed_at=_nullable_float(row["completed_at"]),
        updated_at=float(row["updated_at"]),
        result=_decode_json(row["result_json"]),
        error=_decode_json(row["error_json"]),
    )


def _event_from_row(row: sqlite3.Row) -> TaskEvent:
    return TaskEvent(
        event_id=int(row["event_id"]),
        job_id=str(row["job_id"]),
        attempt_id=row["attempt_id"],
        event_type=str(row["event_type"]),
        payload=_decode_json(row["payload_json"]),
        created_at=float(row["created_at"]),
    )


def _nullable_float(value: object) -> float | None:
    return float(value) if value is not None else None


def _require_task_row(connection: sqlite3.Connection, job_id: str) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM tasks WHERE job_id = ?", (job_id,)).fetchone()
    if row is None:
        raise TaskNotFoundError(f"task not found: {job_id}")
    return row


def _require_attempt_row(connection: sqlite3.Connection, attempt_id: str) -> sqlite3.Row:
    row = connection.execute("SELECT * FROM attempts WHERE attempt_id = ?", (attempt_id,)).fetchone()
    if row is None:
        raise AttemptNotFoundError(f"attempt not found: {attempt_id}")
    return row


def _require_active_attempt(connection: sqlite3.Connection, attempt_id: str) -> sqlite3.Row:
    attempt = _require_attempt_row(connection, attempt_id)
    if attempt["state"] not in ACTIVE_ATTEMPT_STATES:
        raise InvalidTransitionError(f"attempt {attempt_id} is not active")
    return attempt


def _require_current_task_for_attempt(
    connection: sqlite3.Connection, attempt: sqlite3.Row
) -> sqlite3.Row:
    task = _require_task_row(connection, str(attempt["job_id"]))
    if task["current_attempt_id"] != attempt["attempt_id"]:
        raise TaskConflictError(f"attempt {attempt['attempt_id']} no longer owns task {task['job_id']}")
    return task


def _page_row(connection: sqlite3.Connection, job_id: str, page_number: int) -> sqlite3.Row | None:
    return connection.execute(
        "SELECT * FROM page_progress WHERE job_id = ? AND page_number = ?",
        (job_id, page_number),
    ).fetchone()


def _require_page_row(connection: sqlite3.Connection, job_id: str, page_number: int) -> sqlite3.Row:
    row = _page_row(connection, job_id, page_number)
    if row is None:
        raise TaskStoreError(f"page state was not recorded: {job_id}/{page_number}")
    return row


def _task_state_for_outcome(outcome: str) -> str:
    mapping = {
        ATTEMPT_COMPLETED: TASK_COMPLETED,
        ATTEMPT_FAILED: TASK_FAILED,
        ATTEMPT_CANCELLED: TASK_CANCELLED,
        ATTEMPT_STALLED: TASK_STALLED,
    }
    return mapping[outcome]
