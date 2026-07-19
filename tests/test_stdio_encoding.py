from __future__ import annotations

import json
from pathlib import Path

from pdf_rescue_mcp.stdio_encoding import configure_utf8_stdio


class FakeStream:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def reconfigure(self, **kwargs: str) -> None:
        self.calls.append(kwargs)


def test_utf8_stdio_configures_all_streams() -> None:
    streams = (FakeStream(), FakeStream(), FakeStream())

    configure_utf8_stdio(streams)  # type: ignore[arg-type]

    assert [stream.calls for stream in streams] == [
        [{"encoding": "utf-8", "errors": "replace"}],
        [{"encoding": "utf-8", "errors": "replace"}],
        [{"encoding": "utf-8", "errors": "replace"}],
    ]


def test_platform_mcp_configs_use_portable_locked_runner() -> None:
    root = Path(__file__).resolve().parents[1]
    configs = ("mcp-config.json", "mcp-config.windows.json", "mcp-config.linux.json")

    for filename in configs:
        config = json.loads((root / "examples" / filename).read_text(encoding="utf-8"))
        server = config["mcpServers"]["pdf-rescue"]
        assert server["command"] == "uv"
        assert server["args"] == [
            "run",
            "--locked",
            "--extra",
            "ocr",
            "python",
            "-B",
            "scripts/start_mcp.py",
        ]
        assert server["cwd"] == "{{PROJECT_ROOT}}"
        assert ":" not in server["command"]
        assert "\\" not in server["command"]
        assert server["env"]["PYTHONUTF8"] == "1"
        assert server["env"]["PYTHONIOENCODING"] == "utf-8"


def test_linux_launchers_have_shell_headers() -> None:
    root = Path(__file__).resolve().parents[1]

    for filename in ("start-mcp.sh", "run-pdf-rescue.sh"):
        assert (root / "scripts" / filename).read_text(encoding="utf-8").startswith("#!/usr/bin/env sh")


def test_powershell_scripts_use_utf8_bom() -> None:
    root = Path(__file__).resolve().parents[1]

    scripts = list((root / "scripts").glob("*.ps1"))

    assert scripts
    assert all(script.read_bytes().startswith(b"\xef\xbb\xbf") for script in scripts)
