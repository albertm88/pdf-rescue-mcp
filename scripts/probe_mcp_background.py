from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def main(pdf: Path, output_dir: Path, max_pages: int) -> None:
    project_dir = Path(__file__).resolve().parents[1]
    executable = project_dir / ".venv" / "Scripts" / "pdf-rescue-mcp.exe"
    env = os.environ.copy()
    env.update({"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8", "PYTHONLEGACYWINDOWSSTDIO": "0"})
    server = StdioServerParameters(command=str(executable), args=[], env=env, cwd=str(project_dir))
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            result = await session.call_tool(
                "extract_book_background",
                arguments={
                    "path": str(pdf),
                    "output_dir": str(output_dir),
                    "mode": "book-quality",
                    "max_pages": max_pages,
                    "resume": True,
                },
            )
            print(json.dumps(getattr(result, "structuredContent", {}), ensure_ascii=False), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-pages", type=int, default=1)
    args = parser.parse_args()
    asyncio.run(main(args.pdf.resolve(), args.output_dir.resolve(), args.max_pages))
