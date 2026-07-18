"""Temporary MCP probe: list the tools exposed by the project-local server."""

from __future__ import annotations

import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def main() -> None:
    server = StdioServerParameters(
        command=sys.executable,
        args=["-B", "scripts/start_mcp.py"],
        cwd=".",
        env={
            **os.environ,
            "PYTHONUTF8": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONLEGACYWINDOWSSTDIO": "0",
        },
    )
    async with stdio_client(server) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            initialized = await session.initialize()
            tools = await session.list_tools()
            print(initialized.serverInfo.name)
            for tool in tools.tools:
                print(f"{tool.name}: {tool.title}")


if __name__ == "__main__":
    asyncio.run(main())
