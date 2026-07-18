"""Portable stdio client configuration for the project-local MCP server."""

from __future__ import annotations

import os
import sys
from pathlib import Path

from mcp.client.stdio import StdioServerParameters


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def runtime_env() -> dict[str, str]:
    env = os.environ.copy()
    env.update({"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"})
    if os.name == "nt":
        env["PYTHONLEGACYWINDOWSSTDIO"] = "0"
    return env


def make_server_parameters() -> StdioServerParameters:
    """Reuse the current Python: client scripts already imported the MCP dependency."""
    command = sys.executable
    args = ["-B", "scripts/start_mcp.py"]
    return StdioServerParameters(
        command=command,
        args=args,
        env=runtime_env(),
        cwd=str(project_root()),
    )
