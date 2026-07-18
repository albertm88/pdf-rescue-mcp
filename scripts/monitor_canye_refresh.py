from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path


PROJECT_DIR = Path(__file__).resolve().parents[1]
PDF_PATH: Path
JOB_DIR: Path
NEXT_PDF_PATH: Path | None = None
NEXT_JOB_DIR: Path | None = None
OUTPUT_ROOT: Path
LOG_DIR: Path
LOG_PATH: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="监测书籍任务完成后执行缓存刷新和质量巡检。")
    parser.add_argument("--pdf", type=Path, required=True, help="当前任务的来源PDF。")
    parser.add_argument("--job-dir", type=Path, required=True, help="当前任务目录。")
    parser.add_argument("--output-root", type=Path, required=True, help="书库输出根目录。")
    parser.add_argument("--next-pdf", type=Path, help="完成后可选启动的下一本PDF。")
    parser.add_argument("--next-job-dir", type=Path, help="下一本PDF的任务目录。")
    parser.add_argument(
        "--project-dir",
        type=Path,
        default=PROJECT_DIR,
        help="本项目目录，默认按脚本自身位置自动判断。",
    )
    parser.add_argument("--log-dir", type=Path, help="日志根目录，默认写入项目 logs 目录。")
    args = parser.parse_args()
    if bool(args.next_pdf) != bool(args.next_job_dir):
        parser.error("--next-pdf 与 --next-job-dir 必须同时提供。")
    return args


def configure_paths(args: argparse.Namespace) -> None:
    global PROJECT_DIR, PDF_PATH, JOB_DIR, NEXT_PDF_PATH, NEXT_JOB_DIR, OUTPUT_ROOT, LOG_DIR, LOG_PATH
    PROJECT_DIR = args.project_dir.expanduser().resolve()
    PDF_PATH = args.pdf.expanduser().resolve()
    JOB_DIR = args.job_dir.expanduser().resolve()
    NEXT_PDF_PATH = args.next_pdf.expanduser().resolve() if args.next_pdf else None
    NEXT_JOB_DIR = args.next_job_dir.expanduser().resolve() if args.next_job_dir else None
    OUTPUT_ROOT = args.output_root.expanduser().resolve()
    log_root = args.log_dir.expanduser().resolve() if args.log_dir else PROJECT_DIR / "logs"
    LOG_PATH = _timestamped_log_path(log_root, "monitor-canye-refresh")
    LOG_DIR = LOG_PATH.parent


def _timestamped_log_path(log_root: Path, name: str) -> Path:
    source_root = PROJECT_DIR / "src"
    import sys

    if str(source_root) not in sys.path:
        sys.path.insert(0, str(source_root))
    from pdf_rescue_mcp.paths import timestamped_log_path

    return timestamped_log_path(name, root=log_root)


def decode_output(data: bytes) -> str:
    for encoding in ("utf-8", "gb18030", "cp936"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def write_log(message: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(message.rstrip() + "\n")


def run_uv(
    args: list[str],
    *,
    check: bool = True,
    log_output: bool = True,
) -> subprocess.CompletedProcess[bytes]:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    active_environment = bool(os.environ.get("VIRTUAL_ENV")) or sys.prefix != sys.base_prefix
    if shutil.which("uv") and not active_environment:
        command = ["uv", "run", "--locked", *args]
    elif args and args[0] == "pdf-jiuyuan":
        source_root = PROJECT_DIR / "src"
        existing_python_path = env.get("PYTHONPATH")
        env["PYTHONPATH"] = (
            str(source_root)
            if not existing_python_path
            else os.pathsep.join((str(source_root), existing_python_path))
        )
        command = [sys.executable, "-B", "-m", "pdf_rescue_mcp.cli", *args[1:]]
    else:
        raise RuntimeError("未检测到 uv，且当前命令无法使用 Python 后备启动。")
    write_log(f"\n[{datetime.now().isoformat(timespec='seconds')}] 运行救援子命令。")
    proc = subprocess.run(command, cwd=PROJECT_DIR, capture_output=True, env=env)
    stdout = decode_output(proc.stdout)
    stderr = decode_output(proc.stderr)
    if log_output and stdout.strip():
        write_log(stdout)
    if log_output and stderr.strip():
        write_log("[错误输出]\n" + stderr)
    write_log(f"[返回码] {proc.returncode}")
    if check and proc.returncode != 0:
        raise RuntimeError(f"命令失败：{' '.join(command)}")
    return proc


def read_status() -> dict:
    return read_job_status(JOB_DIR)


def read_job_status(job_dir: Path) -> dict:
    proc = run_uv(["pdf-jiuyuan", "状态", str(job_dir), "--json"], check=False, log_output=False)
    if proc.returncode != 0:
        return {"状态": {"状态": "读取失败"}}
    return json.loads(decode_output(proc.stdout))


def should_start_next_job() -> bool:
    if NEXT_PDF_PATH is None or NEXT_JOB_DIR is None:
        return False
    if not NEXT_PDF_PATH.exists():
        write_log(f"下一本PDF不存在，跳过：{NEXT_PDF_PATH}")
        return False
    status_payload = read_job_status(NEXT_JOB_DIR)
    state = status_payload.get("状态", {})
    state_text = state.get("状态", "未开始")
    target = int(state.get("目标页数") or 0)
    processed = int(state.get("已处理页数") or 0)
    total = int(state.get("PDF总页数") or 0)
    is_sample = bool(state.get("是否抽样")) or bool(total and target and target < total)
    if state_text == "进行中":
        write_log(f"下一本已在进行中，跳过启动：{NEXT_JOB_DIR}")
        return False
    if state_text == "完成" and not is_sample and (not total or processed >= total):
        write_log(f"下一本已完成，跳过启动：{NEXT_JOB_DIR}")
        return False
    return True


def main() -> int:
    configure_paths(parse_args())
    write_log("蚕业卷完成后自动刷新监控已启动。")
    while True:
        status_payload = read_status()
        state = status_payload.get("状态", {})
        state_text = state.get("状态", "未知")
        processed = state.get("已处理页数", 0)
        target = state.get("目标页数", 0)
        write_log(f"当前状态：{state_text}，进度：{processed}/{target}")
        if state_text == "完成":
            break
        time.sleep(300)

    run_uv(
        [
            "pdf-jiuyuan",
            "提取",
            str(PDF_PATH),
            "--output-dir",
            str(JOB_DIR),
            "--mode",
            "book-balanced",
            "--json",
        ]
    )
    run_uv(["pdf-jiuyuan", "质量巡检", str(JOB_DIR), "--max-issues", "80", "--json"])
    run_uv(["ruff", "check", "src", "tests"])
    run_uv(["pytest"])
    write_log("蚕业卷完成后刷新流程已完成。")

    if should_start_next_job():
        write_log(f"开始下一本整书处理：{NEXT_PDF_PATH}")
        run_uv(
            [
                "pdf-jiuyuan",
                "提取",
                str(NEXT_PDF_PATH),
                "--output-dir",
                str(NEXT_JOB_DIR),
                "--mode",
                "book-balanced",
                "--json",
            ]
        )
        run_uv(["pdf-jiuyuan", "质量巡检", str(NEXT_JOB_DIR), "--max-issues", "80", "--json"])
        run_uv(["ruff", "check", "src", "tests"])
        run_uv(["pytest"])
        write_log("茶业卷整书处理和刷新流程已完成。")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        write_log(f"监控流程失败：{type(exc).__name__}: {exc}")
        raise
