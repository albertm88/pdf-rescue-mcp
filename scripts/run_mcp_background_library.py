from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _configure_utf8_stdio() -> None:
    """Keep redirected progress logs readable on Windows and Linux."""
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")


def _payload(result: Any) -> dict[str, Any]:
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    return {}


async def _call(session: ClientSession, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = await session.call_tool(name, arguments=arguments)
    if getattr(result, "isError", False):
        raise RuntimeError(f"MCP工具调用失败：{name}")
    return _payload(result)


def _state_text(status: dict[str, Any]) -> str:
    state = status.get("状态")
    if isinstance(state, dict):
        return str(state.get("状态") or "未知")
    return str(state or "未知")


def _progress(status: dict[str, Any]) -> tuple[int, int, int, int]:
    state = status.get("状态") if isinstance(status.get("状态"), dict) else {}
    return (
        int(state.get("已处理页数") or 0),
        int(state.get("目标页数") or 0),
        int(state.get("低置信页数") or 0),
        int(state.get("失败页数") or 0),
    )


async def _wait_for_job(
    session: ClientSession,
    job_dir: str,
    *,
    mode: str,
    poll_seconds: int,
    stale_seconds: int,
) -> dict[str, Any]:
    last_log = 0.0
    while True:
        try:
            status = await _call(
                session,
                "get_job_status",
                {"job_dir": job_dir, "stalled_after_seconds": stale_seconds},
            )
        except Exception as exc:
            if time.monotonic() - last_log >= 60:
                print(f"等待状态文件：{job_dir}，原因：{type(exc).__name__}", flush=True)
                last_log = time.monotonic()
            await asyncio.sleep(poll_seconds)
            continue

        state_text = _state_text(status)
        processed, target, low, failed = _progress(status)
        if time.monotonic() - last_log >= 30 or state_text != "进行中":
            print(
                f"任务状态：{state_text}，进度：{processed}/{target}，"
                f"低置信页：{low}，失败页：{failed}",
                flush=True,
            )
            last_log = time.monotonic()

        if state_text in {"完成", "未完成", "需要OCR引擎", "需要提供密码", "需要先修复PDF"}:
            return status

        freshness = status.get("状态新鲜度")
        if isinstance(freshness, dict) and freshness.get("疑似中断"):
            print("发现疑似中断，重新通过后台提取工具启动并复用缓存。", flush=True)
            await _call(
                session,
                "extract_book_background",
                {
                    "path": str(status.get("状态", {}).get("来源PDF") or ""),
                    "output_dir": job_dir,
                    "mode": mode,
                    "resume": True,
                },
            )
        await asyncio.sleep(poll_seconds)


async def run(args: argparse.Namespace) -> None:
    project_dir = Path(__file__).resolve().parents[1]
    executable = project_dir / ".venv" / "Scripts" / "pdf-rescue-mcp.exe"
    if not executable.exists():
        raise FileNotFoundError(f"未找到MCP服务命令：{executable}")

    env = os.environ.copy()
    env.update({"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8", "PYTHONLEGACYWINDOWSSTDIO": "0"})
    server = StdioServerParameters(command=str(executable), args=[], env=env, cwd=str(project_dir))

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            initialized = await session.initialize()
            print(f"MCP已连接，协议版本：{initialized.protocolVersion}", flush=True)
            scan = await _call(
                session,
                "scan_pdf_library",
                {"root": str(args.root), "output_dir": str(args.output_dir), "inspect_pages": args.inspect_pages},
            )
            books = scan.get("书籍") or []
            if args.max_books > 0:
                books = books[: args.max_books]
            print(f"MCP扫描到 {len(books)} 个PDF。", flush=True)

            for index, book in enumerate(books, start=1):
                source = str(book.get("PDF路径") or "")
                job_dir = str(book.get("建议输出目录") or "")
                print(f"[{index}/{len(books)}] 开始：{source}", flush=True)
                started = await _call(
                    session,
                    "extract_book_background",
                    {
                        "path": source,
                        "output_dir": job_dir,
                        "mode": args.mode,
                        "max_pages": args.max_pages_per_book,
                        "resume": True,
                    },
                )
                print(json.dumps(started, ensure_ascii=False), flush=True)
                status = await _wait_for_job(
                    session,
                    job_dir,
                    mode=args.mode,
                    poll_seconds=args.poll_seconds,
                    stale_seconds=args.stale_seconds,
                )
                print(f"[{index}/{len(books)}] 完成状态：{_state_text(status)}", flush=True)
                try:
                    audit = await _call(
                        session,
                        "audit_job_quality",
                        {"job_dir": job_dir, "max_issues": 120, "use_latest_rules": True},
                    )
                    print(json.dumps(audit, ensure_ascii=False), flush=True)
                except Exception as exc:
                    print(f"质量巡检失败：{type(exc).__name__}: {exc}", flush=True)


def main() -> None:
    _configure_utf8_stdio()
    parser = argparse.ArgumentParser(description="通过MCP逐本执行书库后台转文本和质量巡检。")
    parser.add_argument("--root", type=Path, required=True, help="PDF书库根目录。")
    parser.add_argument("--output-dir", type=Path, required=True, help="转文本输出根目录。")
    parser.add_argument("--mode", default="book-quality", help="处理模式。")
    parser.add_argument("--max-books", type=int, default=0, help="最多处理书数，0表示全部。")
    parser.add_argument("--max-pages-per-book", type=int, help="每本最多处理页数，用于链路验收。")
    parser.add_argument("--inspect-pages", type=int, default=3, help="扫描阶段每本抽查页数。")
    parser.add_argument("--poll-seconds", type=int, default=20, help="任务状态轮询间隔。")
    parser.add_argument("--stale-seconds", type=int, default=900, help="任务停滞判定秒数。")
    args = parser.parse_args()
    args.root = args.root.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
