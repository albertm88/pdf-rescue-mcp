from __future__ import annotations

import sys
from typing import TextIO


def configure_utf8_stdio(streams: tuple[TextIO, ...] | None = None) -> None:
    """Keep Chinese JSON and CLI output stable across consoles and MCP stdio."""
    for stream in streams or (sys.stdin, sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            reconfigure(encoding="utf-8", errors="replace")
