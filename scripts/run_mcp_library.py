from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from typing import Any

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


def _result_payload(result: Any) -> Any:
    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        return structured
    texts: list[str] = []
    for item in getattr(result, "content", []) or []:
        if getattr(item, "type", None) == "text":
            texts.append(str(getattr(item, "text", "")))
    return {"文本结果": texts}


async def _call(session: ClientSession, name: str, arguments: dict[str, Any]) -> Any:
    print(f"开始调用MCP工具：{name}", flush=True)
    result = await session.call_tool(name, arguments=arguments)
    payload = _result_payload(result)
    print(json.dumps(payload, ensure_ascii=False), flush=True)
    return payload


async def run(args: argparse.Namespace) -> None:
    project_dir = Path(__file__).resolve().parents[1]
    executable = project_dir / ".venv" / "Scripts" / "pdf-rescue-mcp.exe"
    if not executable.exists():
        raise FileNotFoundError(f"未找到MCP服务命令：{executable}")

    env = os.environ.copy()
    env.update(
        {
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONLEGACYWINDOWSSTDIO": "0",
        }
    )
    server = StdioServerParameters(
        command=str(executable),
        args=[],
        env=env,
        cwd=str(project_dir),
    )

    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            initialized = await session.initialize()
            print(f"MCP已连接，协议版本：{initialized.protocolVersion}", flush=True)
            await _call(
                session,
                "scan_pdf_library",
                {
                    "root": str(args.root),
                    "output_dir": str(args.output_dir),
                    "inspect_pages": args.inspect_pages,
                },
            )
            await _call(
                session,
                "batch_extract_library",
                {
                    "root": str(args.root),
                    "output_dir": str(args.output_dir),
                    "mode": args.mode,
                    "max_books": args.max_books,
                    "max_pages_per_book": args.max_pages_per_book,
                    "resume": True,
                },
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="通过MCP执行书库扫描和整本转文本。")
    parser.add_argument("--root", type=Path, required=True, help="PDF书库根目录。")
    parser.add_argument("--output-dir", type=Path, required=True, help="转文本输出根目录。")
    parser.add_argument("--mode", default="book-quality", help="处理模式。")
    parser.add_argument("--max-books", type=int, default=0, help="最多处理书数，0表示全部。")
    parser.add_argument("--max-pages-per-book", type=int, help="每本最多处理页数，用于链路验收。")
    parser.add_argument("--inspect-pages", type=int, default=3, help="扫描阶段每本抽查页数。")
    args = parser.parse_args()
    args.root = args.root.expanduser().resolve()
    args.output_dir = args.output_dir.expanduser().resolve()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
