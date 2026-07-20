from __future__ import annotations

import importlib.util
import os
import platform
import shutil
import subprocess
import sys

import psutil

from .models import GpuStatus, RuntimeProfile, RuntimeReadiness, ToolStatus
from .ocr_engines import _quiet_native_output, prepare_paddle_gpu_dlls
from .paths import ensure_runtime_layout
from .resource_scheduler import ResourceScheduler, sample_process_cpu_usage


def _decode_command_output(value: bytes | str | None) -> str:
    """Decode external utility output without leaking Windows code-page errors.

    MCP stdio uses UTF-8, while Windows command wrappers can still emit a legacy
    code page.  Keep their bytes inside the runtime probe instead of allowing a
    background ``subprocess`` decoder thread to write a traceback to stderr.
    """
    if not value:
        return ""
    if isinstance(value, str):
        return value
    encodings = ("utf-8", "mbcs") if os.name == "nt" else ("utf-8",)
    for encoding in encodings:
        try:
            return value.decode(encoding)
        except UnicodeDecodeError:
            continue
    return value.decode("utf-8", errors="replace")


def collect_process_resource_usage(pid: int | None) -> dict[str, object]:
    """采集工作进程的轻量资源快照。

    这是监控层的数据，不参与OCR业务流程，也不会触碰工作进程的线程或
    解释器。采样失败时返回明确的不可用状态，避免把缺失的监控数据伪装成
    0% 占用。
    """
    if pid is None:
        return {
            "状态": "不可用",
            "进程ID": None,
            "CPU占用率": None,
            "CPU等效核心数": None,
            "线程CPU占用率": None,
            "线程采样窗口秒": None,
            "进程线程数": None,
            "活跃CPU线程数": None,
            "饱和CPU线程数": None,
            "线程CPU总和等效核心数": None,
            "最大线程CPU占用率": None,
            "内存占用率": None,
            "内存MB": None,
            "运行内存占用MB": None,
            "运行内存占整机比例": None,
            "说明": "尚未取得工作进程PID。",
        }
    try:
        process = psutil.Process(int(pid))
        if not process.is_running():
            return {
                "状态": "不可用",
                "进程ID": int(pid),
                "CPU占用率": None,
                "CPU等效核心数": None,
                "线程CPU占用率": None,
                "线程采样窗口秒": None,
                "进程线程数": None,
                "活跃CPU线程数": None,
                "饱和CPU线程数": None,
                "线程CPU总和等效核心数": None,
                "最大线程CPU占用率": None,
                "内存占用率": None,
                "内存MB": None,
                "运行内存占用MB": None,
                "运行内存占整机比例": None,
                "说明": "工作进程已退出。",
            }
        # CPU占用率按整机逻辑CPU能力归一化；每个OS线程单独提供0–100%采样值。
        cpu_sample = sample_process_cpu_usage(process)
        thread_usage = cpu_sample.thread_cpu_percent or {}
        thread_values = list(thread_usage.values())
        memory_info = process.memory_info()
        memory_percent = round(float(process.memory_percent()), 2)
        memory_mb = round(memory_info.rss / (1024 * 1024), 1)
        try:
            process_thread_count = max(0, int(process.num_threads()))
        except (AttributeError, OSError, TypeError, ValueError, psutil.Error):
            # A thread-count probe is observation-only.  Do not discard a valid
            # CPU/RSS sample merely because a platform denies this one field.
            process_thread_count = None
        return {
            "状态": "可用",
            "进程ID": int(pid),
            "CPU占用率": cpu_sample.cpu_percent,
            "CPU等效核心数": cpu_sample.cpu_core_equivalents,
            "线程CPU占用率": cpu_sample.thread_cpu_percent,
            "线程采样窗口秒": cpu_sample.sample_window_seconds,
            "进程线程数": process_thread_count,
            "活跃CPU线程数": sum(value >= 5.0 for value in thread_values),
            "饱和CPU线程数": sum(value >= 80.0 for value in thread_values),
            "线程CPU总和等效核心数": round(sum(thread_values) / 100.0, 2),
            "最大线程CPU占用率": round(max(thread_values), 1) if thread_values else None,
            "内存占用率": memory_percent,
            "内存MB": memory_mb,
            "运行内存占用MB": memory_mb,
            "运行内存占整机比例": memory_percent,
            "说明": "CPU占用率为该进程占整机逻辑CPU能力的比例；线程CPU占用率按线程ID分别显示，单线程不超过100%。",
        }
    except (OSError, ValueError, psutil.Error) as exc:
        return {
            "状态": "不可用",
            "进程ID": int(pid),
            "CPU占用率": None,
            "CPU等效核心数": None,
            "线程CPU占用率": None,
            "线程采样窗口秒": None,
            "进程线程数": None,
            "活跃CPU线程数": None,
            "饱和CPU线程数": None,
            "线程CPU总和等效核心数": None,
            "最大线程CPU占用率": None,
            "内存占用率": None,
            "内存MB": None,
            "运行内存占用MB": None,
            "运行内存占整机比例": None,
            "说明": f"工作进程资源采样失败：{type(exc).__name__}。",
        }


def _command_status(name: str, candidates: list[str], version_args: list[str]) -> ToolStatus:
    found = None
    for candidate in candidates:
        path = shutil.which(candidate)
        if path:
            found = (candidate, path)
            break
    if not found:
        return ToolStatus(name=name, available=False, notes=["未在系统路径中找到"])

    _, path = found
    version = None
    notes: list[str] = []
    try:
        proc = subprocess.run(
            [path, *version_args],
            capture_output=True,
            timeout=5,
            check=False,
        )
        output = _decode_command_output(proc.stdout or proc.stderr).strip()
        if proc.returncode != 0 and "cannot find" in output.lower():
            return ToolStatus(
                name=name,
                available=False,
                path=path,
                notes=["已找到但无法运行。"],
            )
        version = output.splitlines()[0] if output else None
    except Exception as exc:  # pragma: no cover - defensive runtime probing
        notes.append(f"版本检查失败：{exc}")
    return ToolStatus(name=name, available=True, version=version, notes=notes)


def _package_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def _probe_nvidia_gpu() -> dict[str, object]:
    nvidia_smi = shutil.which("nvidia-smi")
    if not nvidia_smi:
        return {
            "hardware_available": False,
            "reason": "未检测到显卡探测工具，无法确认显卡硬件。",
        }
    try:
        proc = subprocess.run(
            [
                nvidia_smi,
                "--query-gpu=name,memory.total,driver_version,compute_cap",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            timeout=5,
            check=False,
        )
        output = _decode_command_output(proc.stdout)
        if proc.returncode != 0 or not output.strip():
            return {
                "hardware_available": False,
                "reason": "显卡探测工具存在，但没有返回可用显卡信息。",
            }
        rows = [line.strip() for line in output.splitlines() if line.strip()]
        first = [part.strip() for part in rows[0].split(",")]
        memory_gb = None
        if len(first) > 1:
            try:
                memory_gb = round(float(first[1]) / 1024, 2)
            except ValueError:
                pass
        return {
            "hardware_available": True,
            "hardware_name": first[0] if first else None,
            "hardware_memory_gb": memory_gb,
            "driver_version": first[2] if len(first) > 2 else None,
            "compute_capability": first[3] if len(first) > 3 else None,
            "device_count": len(rows),
        }
    except Exception as exc:
        return {
            "hardware_available": False,
            "reason": f"显卡硬件探测失败：{type(exc).__name__}。",
        }


def _gpu_available() -> bool:
    return bool(_probe_nvidia_gpu().get("hardware_available"))


def _paddle_gpu_backend(paddle: object) -> str | None:
    device_api = getattr(paddle, "device", None)
    if device_api is None:
        return None
    if bool(getattr(device_api, "is_compiled_with_cuda", lambda: False)()):
        return "cuda"
    if bool(getattr(device_api, "is_compiled_with_rocm", lambda: False)()):
        return "rocm"
    return None


def _probe_paddle_gpu(hardware: dict[str, object], deep_ocr_probe: bool) -> dict[str, object]:
    if not deep_ocr_probe:
        return {
            "device_count": 0,
            "runtime_available": False,
            "confirmed": False,
            "device": "cpu",
            "reason": "规划阶段未执行图形处理器计算探测，提取时会再次确认。",
        }
    if not _package_available("paddle"):
        return {
            "device_count": 0,
            "runtime_available": False,
            "confirmed": False,
            "device": "cpu",
            "reason": "未安装飞桨运行后端。",
        }
    try:
        prepare_paddle_gpu_dlls()
        with _quiet_native_output():
            import paddle

            backend = _paddle_gpu_backend(paddle)
            if not backend:
                return {
                    "device_count": 0,
                    "runtime_available": False,
                    "confirmed": False,
                    "device": "cpu",
                    "reason": "当前安装的是处理器版飞桨，不支持图形处理器加速。"
                    if hardware.get("hardware_available")
                    else "未检测到可用的图形处理器加速后端。",
                }
            device_count = int(paddle.device.cuda.device_count())
            if device_count < 1:
                return {
                    "device_count": 0,
                    "runtime_available": False,
                    "confirmed": False,
                    "device": "cpu",
                    "reason": "飞桨支持图形处理器加速，但没有发现可用设备。",
                }
            previous_device = paddle.get_device()
            confirmed = False
            try:
                paddle.set_device("gpu:0")
                probe = paddle.to_tensor([1.0], dtype="float32")
                arithmetic = (probe + probe).numpy().tolist()
                image = paddle.ones([1, 1, 8, 8], dtype="float32")
                kernel = paddle.ones([1, 1, 3, 3], dtype="float32")
                convolution = paddle.nn.functional.conv2d(image, kernel).numpy()
                confirmed = bool(
                    arithmetic
                    and abs(float(arithmetic[0]) - 2.0) < 1e-6
                    and convolution.shape == (1, 1, 6, 6)
                    and abs(float(convolution.sum()) - 324.0) < 1e-6
                )
            finally:
                if not confirmed:
                    paddle.set_device("cpu")
                elif previous_device and str(previous_device).startswith("gpu"):
                    paddle.set_device(previous_device)
            return {
                "device_count": device_count,
                "backend": backend,
                "runtime_available": True,
                "confirmed": confirmed,
                "device": "gpu" if confirmed else "cpu",
                "reason": "已通过飞桨图形处理器与OCR运行库验证。"
                if confirmed
                else "飞桨图形处理器计算验证未通过。",
            }
    except Exception as exc:
        return {
            "device_count": 0,
            "runtime_available": False,
            "confirmed": False,
            "device": "cpu",
            "reason": f"飞桨图形处理器验证失败，将使用处理器：{type(exc).__name__}。",
        }


def _ocr_gpu_available() -> bool:
    hardware = _probe_nvidia_gpu()
    return bool(_probe_paddle_gpu(hardware, deep_ocr_probe=True).get("confirmed"))


def _runtime_readiness() -> RuntimeReadiness:
    system_name = platform.system()
    platform_supported = system_name in {"Windows", "Linux", "Darwin"}
    python_supported = sys.version_info >= (3, 11)
    uv_available = shutil.which("uv") is not None
    runtime_dirs_writable = True
    notes: list[str] = []

    try:
        ensure_runtime_layout()
    except OSError as exc:
        runtime_dirs_writable = False
        notes.append(f"项目 tmp 或 logs 目录不可写：{type(exc).__name__}。")

    if not platform_supported:
        notes.append(f"当前系统 {system_name or '未知'} 未纳入自动化验证范围。")
    if not python_supported:
        notes.append("当前 Python 版本低于 3.11，无法保证服务可运行。")
    if uv_available:
        notes.append("已检测到 uv；推荐使用锁文件同步依赖后启动服务。")
    else:
        notes.append("未检测到 uv；请确认当前 Python 环境已安装项目依赖。")
    if system_name == "Darwin":
        notes.append("macOS 默认使用处理器 OCR；图形处理器加速需单独验证运行后端。")

    return RuntimeReadiness(
        platform_supported=platform_supported,
        python_supported=python_supported,
        runtime_dirs_writable=runtime_dirs_writable,
        uv_available=uv_available,
        recommended_runner="uv" if uv_available else "python",
        notes=notes,
    )


def doctor_runtime(deep_ocr_probe: bool = True) -> RuntimeProfile:
    cpu_count = os.cpu_count() or 1
    memory_gb = round(psutil.virtual_memory().total / (1024**3), 2)
    hardware = _probe_nvidia_gpu()
    paddle_gpu = _probe_paddle_gpu(hardware, deep_ocr_probe=deep_ocr_probe)
    gpu_status = GpuStatus(
        hardware_available=bool(hardware.get("hardware_available"))
        or bool(paddle_gpu.get("confirmed")),
        hardware_name=hardware.get("hardware_name"),
        hardware_memory_gb=hardware.get("hardware_memory_gb"),
        driver_version=hardware.get("driver_version"),
        compute_capability=hardware.get("compute_capability"),
        device_count=int(paddle_gpu.get("device_count") or hardware.get("device_count") or 0),
        backend=str(paddle_gpu.get("backend")) if paddle_gpu.get("backend") else None,
        runtime_available=bool(paddle_gpu.get("runtime_available")),
        confirmed=bool(paddle_gpu.get("confirmed")),
        device=str(paddle_gpu.get("device") or "cpu"),
        reason=str(paddle_gpu.get("reason") or hardware.get("reason")) if (paddle_gpu.get("reason") or hardware.get("reason")) else None,
    )
    readiness = _runtime_readiness()
    gpu = gpu_status.confirmed

    worker_plan = ResourceScheduler().plan()
    if gpu and memory_gb >= 16:
        recommended_mode = "book-quality"
        max_workers = worker_plan.target_workers
        max_dpi = 600
    elif memory_gb >= 16 and cpu_count >= 8:
        recommended_mode = "book-balanced"
        max_workers = worker_plan.target_workers
        max_dpi = 500
    elif memory_gb >= 8:
        recommended_mode = "book-balanced-low-memory"
        max_workers = worker_plan.target_workers
        max_dpi = 400
    else:
        recommended_mode = "book-fast-low-memory"
        max_workers = worker_plan.target_workers
        max_dpi = 300

    tools = {
        "tesseract": _command_status("tesseract", ["tesseract"], ["--version"]),
        "ocrmypdf": _command_status("ocrmypdf", ["ocrmypdf"], ["--version"]),
        "ghostscript": _command_status("ghostscript", ["gswin64c", "gswin32c", "gs"], ["--version"]),
        "qpdf": _command_status("qpdf", ["qpdf"], ["--version"]),
        "pdftoppm": _command_status("pdftoppm", ["pdftoppm"], ["-v"]),
    }
    packages = {
        "fitz": _package_available("fitz"),
        "paddleocr": _package_available("paddleocr"),
        "paddle": _package_available("paddle"),
        "pytesseract": _package_available("pytesseract"),
        "PIL": _package_available("PIL"),
    }

    notes: list[str] = list(readiness.notes)
    if not packages["paddleocr"]:
        notes.append("未安装飞桨OCR；纯扫描PDF需要安装OCR扩展后才能识别。")
    elif not packages["paddle"]:
        notes.append("已安装飞桨OCR接口，但缺少飞桨运行后端。")
    elif gpu_status.hardware_available and gpu:
        notes.append(f"已确认OCR将使用图形处理器：{gpu_status.hardware_name or '显卡'}。")
    elif gpu_status.hardware_available and deep_ocr_probe:
        notes.append(gpu_status.reason or "检测到显卡硬件，但图形处理器计算验证未通过，OCR将使用处理器。")
    elif gpu_status.hardware_available:
        notes.append("检测到显卡硬件；规划阶段未加载飞桨验证，实际提取会自动确认运行后端。")
    notes.append(
        "批量书籍worker会结合CPU线程、系统可用内存和每个worker的实时占用动态调度；"
        f"当前规划上限为 {worker_plan.target_workers} 个。"
    )
    if not tools["tesseract"].available:
        notes.append("未检测到备用OCR命令；可搜索PDF修复备用通道不可用。")
    if not tools["ocrmypdf"].available:
        notes.append("未检测到文本层修复工具；暂不能自动重建可搜索PDF文本层。")
    if (
        gpu_status.hardware_available
        and not gpu_status.confirmed
        and gpu_status.compute_capability == "6.1"
    ):
        notes.append("该显卡可使用本项目的兼容图形处理器OCR环境：飞桨图形处理器版3.2.2。")

    return RuntimeProfile(
        platform=f"{platform.system()} {platform.release()} ({platform.machine()})",
        python=sys.version.split()[0],
        cpu_count=cpu_count,
        memory_gb=memory_gb,
        gpu_available=gpu,
        gpu=gpu_status,
        recommended_mode=recommended_mode,
        max_workers=max_workers,
        max_dpi=max_dpi,
        tools=tools,
        python_packages=packages,
        readiness=readiness,
        notes=notes,
    )
