from __future__ import annotations

from types import SimpleNamespace

import pytest

from pdf_rescue_mcp.process_controller import (
    ProcessController,
    ProcessIdentity,
    WorkerHandle,
)


class FakeProcess:
    def __init__(
        self,
        pid: int,
        create_time: float,
        *,
        children: list["FakeProcess"] | None = None,
    ) -> None:
        self.pid = pid
        self._create_time = create_time
        self._children = children or []
        self.running = True
        self.calls: list[str] = []

    def create_time(self) -> float:
        return self._create_time

    def children(self, *, recursive: bool) -> list["FakeProcess"]:
        assert recursive is True
        return list(self._children)

    def is_running(self) -> bool:
        return self.running

    def terminate(self) -> None:
        self.calls.append("terminate")

    def kill(self) -> None:
        self.calls.append("kill")


class FakePsutil:
    def __init__(self, processes: list[FakeProcess], *, keep_alive_after_wait: bool) -> None:
        self.processes = {process.pid: process for process in processes}
        self.keep_alive_after_wait = keep_alive_after_wait
        self.wait_calls: list[tuple[list[int], float]] = []

    def Process(self, pid: int) -> FakeProcess:
        return self.processes[pid]

    def wait_procs(
        self, processes: list[FakeProcess], *, timeout: float
    ) -> tuple[list[FakeProcess], list[FakeProcess]]:
        self.wait_calls.append(([process.pid for process in processes], timeout))
        if self.keep_alive_after_wait:
            return [], list(processes)
        for process in processes:
            process.running = False
        return list(processes), []


def test_windows_popen_options_use_creation_flags_only() -> None:
    subprocess_module = SimpleNamespace(CREATE_NEW_PROCESS_GROUP=0x200, CREATE_NO_WINDOW=0x8000000)
    controller = ProcessController(platform_name="win32", subprocess_module=subprocess_module)

    options = controller.build_popen_kwargs()

    assert options == {"creationflags": 0x8000200}
    assert "start_new_session" not in options


def test_posix_popen_options_start_a_new_session() -> None:
    controller = ProcessController(platform_name="darwin")

    assert controller.build_popen_kwargs() == {"start_new_session": True}


def test_spawn_captures_pid_and_creation_time_with_managed_options() -> None:
    launched: dict[str, object] = {}

    class FakePopen:
        pid = 4321

        def __init__(self, command: list[str], **kwargs: object) -> None:
            launched["command"] = command
            launched["kwargs"] = kwargs

    process = FakeProcess(4321, 123.5)
    controller = ProcessController(
        platform_name="linux",
        psutil_module=FakePsutil([process], keep_alive_after_wait=False),
        subprocess_module=SimpleNamespace(Popen=FakePopen),
    )

    handle = controller.spawn(["python", "worker.py"], attempt_id="attempt-1")

    assert handle == WorkerHandle(
        ProcessIdentity(pid=4321, create_time=123.5),
        ("python", "worker.py"),
        "attempt-1",
    )
    assert launched["command"] == ["python", "worker.py"]
    assert launched["kwargs"] == {"start_new_session": True, "shell": False}


def test_spawn_rejects_unmanaged_process_ownership_options() -> None:
    controller = ProcessController(platform_name="linux")

    with pytest.raises(ValueError, match="managed"):
        controller.spawn(["python", "worker.py"], start_new_session=False)


def test_identity_mismatch_never_signals_a_reused_pid() -> None:
    reused_process = FakeProcess(88, 200.0)
    fake_psutil = FakePsutil([reused_process], keep_alive_after_wait=False)
    controller = ProcessController(platform_name="linux", psutil_module=fake_psutil)
    old_worker = WorkerHandle(ProcessIdentity(pid=88, create_time=100.0), ("worker",))

    result = controller.terminate_tree(old_worker)

    assert result.identity_matched is False
    assert result.reason == "worker_missing_or_pid_reused"
    assert reused_process.calls == []


def test_terminate_tree_signals_children_first_then_kills_stuck_tree() -> None:
    child = FakeProcess(102, 12.0)
    root = FakeProcess(101, 11.0, children=[child])
    fake_psutil = FakePsutil([root, child], keep_alive_after_wait=True)
    controller = ProcessController(platform_name="linux", psutil_module=fake_psutil)
    worker = WorkerHandle(ProcessIdentity(pid=101, create_time=11.0), ("worker",))

    result = controller.terminate_tree(worker, grace_seconds=2.0, kill_wait_seconds=1.0)

    assert child.calls == ["terminate", "kill"]
    assert root.calls == ["terminate", "kill"]
    assert result.identity_matched is True
    assert result.terminated_pids == (102, 101)
    assert result.killed_pids == (102, 101)
    assert result.remaining_pids == (102, 101)
    assert fake_psutil.wait_calls == [([102, 101], 2.0), ([102, 101], 1.0)]


def test_handle_round_trip_is_persistable_without_a_popen_object() -> None:
    worker = WorkerHandle(
        identity=ProcessIdentity(pid=99, create_time=10.5),
        command=("python", "worker.py"),
        attempt_id="abc",
    )

    restored = WorkerHandle.from_dict(worker.to_dict())

    assert restored == worker
