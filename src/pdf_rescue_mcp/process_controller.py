"""Safe, portable ownership of OCR worker process trees.

The supervisor owns workers through :class:`ProcessIdentity`, rather than an
unqualified PID.  A PID can be reused after a supervisor restart, so every
signal is guarded by the process creation timestamp captured when the worker
was started.  Platform-specific process-group features are conveniences only:
the recursive psutil fallback is the correctness baseline on Windows, Linux,
and macOS.

This module intentionally has no knowledge of MCP, PDF files, or OCR engines.
It is the narrow process-control boundary used by the supervision layer.
"""

from __future__ import annotations

import math
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import psutil


_CREATE_TIME_TOLERANCE_SECONDS = 0.01


class _PsutilProcess(Protocol):
    """Small structural type shared by psutil and test doubles."""

    pid: int

    def create_time(self) -> float: ...


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """A PID paired with the creation time that makes it safe to own.

    ``create_time`` has platform-dependent precision.  The narrow tolerance
    prevents an insignificant float representation difference from rejecting
    the same process while still making PID reuse overwhelmingly unlikely to
    be mistaken for the original worker.
    """

    pid: int
    create_time: float

    def __post_init__(self) -> None:
        if self.pid <= 0:
            raise ValueError("Process PID must be positive")
        if not math.isfinite(self.create_time) or self.create_time < 0:
            raise ValueError("Process creation time must be a finite timestamp")

    @classmethod
    def from_process(cls, process: _PsutilProcess) -> "ProcessIdentity":
        """Capture the identity of an already-running process."""
        return cls(pid=int(process.pid), create_time=float(process.create_time()))

    def matches(self, process: _PsutilProcess) -> bool:
        """Return whether ``process`` is still the exact process we own."""
        try:
            return int(process.pid) == self.pid and math.isclose(
                float(process.create_time()),
                self.create_time,
                rel_tol=0.0,
                abs_tol=_CREATE_TIME_TOLERANCE_SECONDS,
            )
        # psutil may raise AccessDenied/ZombieProcess here; an uncertain
        # identity must never be treated as permission to signal a process.
        except Exception:
            return False

    def to_dict(self) -> dict[str, int | float]:
        """Return a JSON-safe representation for the supervisor store."""
        return {"pid": self.pid, "create_time": self.create_time}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "ProcessIdentity":
        """Restore an identity persisted by :meth:`to_dict`."""
        return cls(pid=int(value["pid"]), create_time=float(value["create_time"]))


@dataclass(frozen=True, slots=True)
class WorkerHandle:
    """Persistable metadata for a worker that belongs to one task attempt."""

    identity: ProcessIdentity
    command: tuple[str, ...]
    attempt_id: str | None = None

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("Worker command must not be empty")

    @property
    def pid(self) -> int:
        """The worker PID; callers should use ``identity`` for ownership checks."""
        return self.identity.pid

    def to_dict(self) -> dict[str, object]:
        """Return JSON-safe metadata without a live ``Popen`` object."""
        return {
            "identity": self.identity.to_dict(),
            "command": list(self.command),
            "attempt_id": self.attempt_id,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> "WorkerHandle":
        """Restore a persisted handle.

        Handles intentionally do not serialize ``Popen``.  After a supervisor
        restart, psutil plus the identity guard remains sufficient to inspect
        and control a worker safely.
        """
        identity_data = value.get("identity")
        command_data = value.get("command")
        if not isinstance(identity_data, Mapping):
            raise ValueError("Worker handle identity must be an object")
        if not isinstance(command_data, (list, tuple)):
            raise ValueError("Worker handle command must be a list")
        attempt_id = value.get("attempt_id")
        if attempt_id is not None and not isinstance(attempt_id, str):
            raise ValueError("Worker attempt ID must be a string or null")
        return cls(
            identity=ProcessIdentity.from_dict(identity_data),
            command=tuple(str(part) for part in command_data),
            attempt_id=attempt_id,
        )


@dataclass(frozen=True, slots=True)
class TerminationResult:
    """Auditable outcome of a guarded process-tree termination attempt."""

    identity_matched: bool
    terminated_pids: tuple[int, ...] = ()
    killed_pids: tuple[int, ...] = ()
    remaining_pids: tuple[int, ...] = ()
    reason: str | None = None

    @property
    def stopped(self) -> bool:
        """Whether no process still owned by this termination attempt remains."""
        return self.identity_matched and not self.remaining_pids


class ProcessController:
    """Launch and stop one worker process tree with a portable safe baseline.

    Windows uses ``CREATE_NEW_PROCESS_GROUP`` (and optionally
    ``CREATE_NO_WINDOW``); POSIX uses ``start_new_session``.  Neither feature
    is relied upon for correctness because applications may spawn descendants
    outside their process group.  ``terminate_tree`` always discovers and
    controls descendants through psutil.

    The optional collaborators make unit tests independent of the local OS
    and also keep this boundary easy to adapt for embedded hosts.
    """

    def __init__(
        self,
        *,
        platform_name: str | None = None,
        psutil_module: Any | None = None,
        subprocess_module: Any | None = None,
    ) -> None:
        self._platform_name = platform_name or sys.platform
        self._psutil = psutil if psutil_module is None else psutil_module
        self._subprocess = subprocess if subprocess_module is None else subprocess_module

    @property
    def is_windows(self) -> bool:
        """Whether this controller is configured for Windows launch semantics."""
        return self._platform_name.lower().startswith("win")

    def build_popen_kwargs(self, *, hide_window: bool = True) -> dict[str, object]:
        """Return only platform-owned ``Popen`` options.

        Callers may add ordinary I/O, environment, and cwd options in
        :meth:`spawn`; they cannot override session/group ownership settings.
        """
        if self.is_windows:
            flags = int(getattr(self._subprocess, "CREATE_NEW_PROCESS_GROUP", 0))
            if hide_window:
                flags |= int(getattr(self._subprocess, "CREATE_NO_WINDOW", 0))
            return {"creationflags": flags}
        return {"start_new_session": True}

    def spawn(
        self,
        command: Sequence[str | Path],
        *,
        cwd: str | Path | None = None,
        env: Mapping[str, str] | None = None,
        stdin: object | None = None,
        stdout: object | None = None,
        stderr: object | None = None,
        attempt_id: str | None = None,
        hide_window: bool = True,
        **popen_overrides: object,
    ) -> WorkerHandle:
        """Start a worker and return its PID-plus-creation-time handle.

        ``shell=True`` and platform ownership options are intentionally
        rejected: they can detach descendants or make task cleanup ambiguous.
        If identity capture fails after spawning, the just-created process is
        terminated best-effort instead of leaving an unmanaged OCR worker.
        """
        normalized_command = tuple(str(part) for part in command)
        if not normalized_command:
            raise ValueError("Worker command must not be empty")
        forbidden = {"shell", "creationflags", "start_new_session"} & set(popen_overrides)
        if forbidden:
            names = ", ".join(sorted(forbidden))
            raise ValueError(f"Process ownership options are managed by ProcessController: {names}")

        kwargs: dict[str, object] = self.build_popen_kwargs(hide_window=hide_window)
        kwargs.update(popen_overrides)
        kwargs["shell"] = False
        if cwd is not None:
            kwargs["cwd"] = str(cwd)
        if env is not None:
            kwargs["env"] = dict(env)
        if stdin is not None:
            kwargs["stdin"] = stdin
        if stdout is not None:
            kwargs["stdout"] = stdout
        if stderr is not None:
            kwargs["stderr"] = stderr

        process = self._subprocess.Popen(list(normalized_command), **kwargs)
        try:
            identity = self.capture_identity(int(process.pid))
        except Exception:
            self._best_effort_popen_cleanup(process)
            raise
        return WorkerHandle(identity=identity, command=normalized_command, attempt_id=attempt_id)

    # A familiar alias makes the boundary convenient to use from a scheduler.
    start = spawn

    def capture_identity(self, pid: int) -> ProcessIdentity:
        """Capture a currently live process identity through psutil."""
        return ProcessIdentity.from_process(self._psutil.Process(int(pid)))

    def is_alive(self, worker: WorkerHandle | ProcessIdentity) -> bool:
        """Return true only if the exact worker process is still running."""
        return self._matching_process(self._identity_of(worker)) is not None

    def terminate_tree(
        self,
        worker: WorkerHandle | ProcessIdentity,
        *,
        grace_seconds: float = 15.0,
        kill_wait_seconds: float = 5.0,
    ) -> TerminationResult:
        """Recursively terminate, wait, then kill a worker tree if necessary.

        Every child is also captured with a creation timestamp before it is
        signalled.  If a PID is reused during shutdown it is skipped rather
        than risking a signal to an unrelated application.
        """
        if grace_seconds < 0 or kill_wait_seconds < 0:
            raise ValueError("Termination timeouts must not be negative")

        root_identity = self._identity_of(worker)
        root = self._matching_process(root_identity)
        if root is None:
            return TerminationResult(
                identity_matched=False,
                reason="worker_missing_or_pid_reused",
            )

        targets = self._capture_tree(root)
        if not targets:
            return TerminationResult(
                identity_matched=False,
                reason="worker_identity_unavailable",
            )

        terminated: list[int] = []
        signal_targets: list[tuple[ProcessIdentity, Any]] = []
        for identity, process in targets:
            if self._signal_if_current(identity, process, "terminate"):
                terminated.append(identity.pid)
                signal_targets.append((identity, process))

        if not signal_targets:
            return TerminationResult(
                identity_matched=False,
                reason="worker_exited_or_pid_reused_before_termination",
            )

        alive = self._wait_for_alive(signal_targets, grace_seconds)
        killed: list[int] = []
        kill_targets: list[tuple[ProcessIdentity, Any]] = []
        for identity, process in alive:
            if self._signal_if_current(identity, process, "kill"):
                killed.append(identity.pid)
                kill_targets.append((identity, process))

        remaining = self._wait_for_alive(kill_targets, kill_wait_seconds)
        return TerminationResult(
            identity_matched=True,
            terminated_pids=tuple(terminated),
            killed_pids=tuple(killed),
            remaining_pids=tuple(identity.pid for identity, _ in remaining),
            reason=None if not remaining else "processes_remain_after_kill",
        )

    # A concise scheduler-facing alias; the full name makes tree scope explicit.
    stop = terminate_tree

    def _identity_of(self, worker: WorkerHandle | ProcessIdentity) -> ProcessIdentity:
        return worker.identity if isinstance(worker, WorkerHandle) else worker

    def _matching_process(self, identity: ProcessIdentity) -> Any | None:
        try:
            process = self._psutil.Process(identity.pid)
        except Exception:
            return None
        if not identity.matches(process):
            return None
        if not self._is_running(process):
            return None
        return process

    def _capture_tree(self, root: Any) -> list[tuple[ProcessIdentity, Any]]:
        try:
            descendants = list(root.children(recursive=True))
        except Exception:
            descendants = []

        # Terminating children before their parent gives cooperative child
        # processes the opportunity to flush their own work and exit cleanly.
        process_list = [*descendants, root]
        captured: list[tuple[ProcessIdentity, Any]] = []
        seen_pids: set[int] = set()
        for process in process_list:
            try:
                identity = ProcessIdentity.from_process(process)
            except Exception:
                continue
            if identity.pid in seen_pids or not self._is_running(process):
                continue
            seen_pids.add(identity.pid)
            captured.append((identity, process))
        return captured

    def _signal_if_current(self, identity: ProcessIdentity, process: Any, method: str) -> bool:
        # Reopen through psutil immediately before signalling.  A process object
        # by itself is not enough protection against PID reuse.
        current = self._matching_process(identity)
        if current is None:
            return False
        try:
            getattr(current, method)()
        except Exception:
            return False
        return True

    def _wait_for_alive(
        self,
        protected_processes: Sequence[tuple[ProcessIdentity, Any]],
        timeout: float,
    ) -> list[tuple[ProcessIdentity, Any]]:
        if not protected_processes:
            return []
        processes = [process for _, process in protected_processes]
        try:
            _gone, alive_processes = self._psutil.wait_procs(processes, timeout=timeout)
        except Exception:
            alive_processes = [process for process in processes if self._is_running(process)]

        by_pid = {id(process): identity for identity, process in protected_processes}
        alive: list[tuple[ProcessIdentity, Any]] = []
        for process in alive_processes:
            identity = by_pid.get(id(process))
            if identity is not None and self._matching_process(identity) is not None:
                alive.append((identity, process))
        return alive

    @staticmethod
    def _is_running(process: Any) -> bool:
        try:
            checker = getattr(process, "is_running", None)
            return bool(checker()) if checker is not None else True
        except Exception:
            return False

    @staticmethod
    def _best_effort_popen_cleanup(process: Any) -> None:
        try:
            process.terminate()
        except Exception:
            return
