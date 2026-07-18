from __future__ import annotations

import argparse
import asyncio
import json

from mcp.client.session import ClientSession
from mcp.client.stdio import stdio_client

from mcp_client import make_server_parameters


async def main(tool_name: str) -> None:
    server = make_server_parameters()
    async with stdio_client(server) as (read_stream, write_stream):
        async with ClientSession(read_stream, write_stream) as session:
            result = await session.initialize()
            print(f"initialized {result.protocolVersion}", flush=True)
            tools = await session.list_tools()
            print(f"tools {len(tools.tools)}", flush=True)
            result = await session.call_tool(tool_name, arguments={})
            structured = getattr(result, "structuredContent", None)
            print(json.dumps(structured, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tool_name")
    args = parser.parse_args()
    asyncio.run(main(args.tool_name))
