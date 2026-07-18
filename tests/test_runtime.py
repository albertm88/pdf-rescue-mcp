from __future__ import annotations

import sys

import pdf_rescue_mcp.runtime as runtime
from pdf_rescue_mcp.models import ToolStatus


def test_paddle_cpu_build_is_not_reported_as_gpu_ready(monkeypatch) -> None:
    class FakeDevice:
        @staticmethod
        def is_compiled_with_cuda() -> bool:
            return False

    class FakePaddle:
        device = FakeDevice()

    monkeypatch.setattr(runtime, "_package_available", lambda name: name == "paddle")
    monkeypatch.setitem(sys.modules, "paddle", FakePaddle())

    result = runtime._probe_paddle_gpu(
        {"hardware_available": True},
        deep_ocr_probe=True,
    )

    assert result["confirmed"] is False
    assert result["device"] == "cpu"


def test_runtime_profile_keeps_hardware_and_ocr_gpu_separate(monkeypatch) -> None:
    monkeypatch.setattr(
        runtime,
        "_probe_nvidia_gpu",
        lambda: {
            "hardware_available": True,
            "hardware_name": "Test GPU",
            "hardware_memory_gb": 8.0,
            "driver_version": "1.0",
            "device_count": 1,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_probe_paddle_gpu",
        lambda hardware, deep_ocr_probe: {
            "device_count": 1,
            "backend": "cuda",
            "runtime_available": True,
            "confirmed": True,
            "device": "gpu",
            "reason": "已通过飞桨图形处理器与OCR运行库验证。",
        },
    )
    monkeypatch.setattr(
        runtime,
        "_command_status",
        lambda name, candidates, version_args: ToolStatus(name=name, available=False),
    )
    monkeypatch.setattr(runtime, "_package_available", lambda name: True)
    monkeypatch.setattr(
        runtime.psutil,
        "virtual_memory",
        lambda: type("Memory", (), {"total": 16 * 1024**3})(),
    )
    monkeypatch.setattr(runtime, "ensure_runtime_layout", lambda: None)

    profile = runtime.doctor_runtime()

    assert profile.gpu_available is True
    assert profile.gpu.hardware_available is True
    assert profile.gpu.confirmed is True
    assert profile.gpu.device == "gpu"
    assert profile.recommended_mode == "book-quality"


def test_runtime_readiness_reports_portability_risks(monkeypatch) -> None:
    monkeypatch.setattr(runtime.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(runtime.shutil, "which", lambda name: None)

    def fail_layout() -> None:
        raise PermissionError("read-only")

    monkeypatch.setattr(runtime, "ensure_runtime_layout", fail_layout)

    readiness = runtime._runtime_readiness()

    assert readiness.platform_supported is True
    assert readiness.runtime_dirs_writable is False
    assert readiness.uv_available is False
    assert readiness.recommended_runner == "python"
    assert any("macOS" in note for note in readiness.notes)
