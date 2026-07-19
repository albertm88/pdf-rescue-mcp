from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SERVER_NAME = "pdf-rescue"
CLIENT_CHOICES = ("generic", "vscode", "claude", "cursor", "trae", "anythingllm", "codex")


def _runner_config(runner: str) -> tuple[str, list[str]]:
    selected = runner
    if selected == "auto":
        if shutil.which("uv"):
            selected = "uv"
        elif shutil.which("python"):
            selected = "python"
        elif os.name == "nt" and shutil.which("py"):
            selected = "py"
        else:
            raise SystemExit("未找到 uv、python 或 py；请先安装运行环境。")
    if selected == "uv":
        if not shutil.which("uv"):
            raise SystemExit("未找到 uv；请安装 uv 或使用 --runner python。")
        return "uv", ["run", "--locked", "--extra", "ocr", "python", "-B", "scripts/start_mcp.py"]
    if selected == "python":
        if not shutil.which("python"):
            raise SystemExit("未找到 python；请使用 --runner uv 或将 Python 加入系统路径。")
        return "python", ["-B", "scripts/start_mcp.py"]
    if selected == "py" and os.name == "nt":
        if not shutil.which("py"):
            raise SystemExit("未找到 py 启动器；请使用 --runner uv 或 python。")
        return "py", ["-3", "-B", "scripts/start_mcp.py"]
    raise SystemExit(f"不支持的运行器：{runner}")


def _server_config(
    command: str,
    command_args: list[str],
    project_root: Path,
    *,
    include_anythingllm_options: bool = False,
) -> dict[str, Any]:
    env = {
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    if os.name == "nt":
        env["PYTHONLEGACYWINDOWSSTDIO"] = "0"

    config: dict[str, Any] = {
        "command": command,
        "args": command_args,
        "cwd": str(project_root),
        "env": env,
    }
    if include_anythingllm_options:
        # OCR may consume substantial CPU and memory. Let an agent start this
        # server on demand instead of auto-starting it with the application.
        config["anythingllm"] = {"autoStart": False}
    return config


def _json_config(client: str, server: dict[str, Any]) -> dict[str, Any]:
    if client == "vscode":
        return {"servers": {SERVER_NAME: {"type": "stdio", **server}}}
    return {"mcpServers": {SERVER_NAME: server}}


def _toml_string(value: str) -> str:
    """Return a TOML basic string; JSON escaping is compatible for this use."""

    return json.dumps(value, ensure_ascii=False)


def _toml_array(values: list[str]) -> str:
    return "[" + ", ".join(_toml_string(value) for value in values) + "]"


def _codex_toml_config(server: dict[str, Any]) -> str:
    """Render the standard stdio server section used by Codex config.toml."""

    server_table = f'mcp_servers.{_toml_string(SERVER_NAME)}'
    env = server["env"]
    assert isinstance(env, dict)
    lines = [
        "# Generated local configuration. Do not commit this file.",
        f"[{server_table}]",
        f"command = {_toml_string(str(server['command']))}",
        f"args = {_toml_array(list(server['args']))}",
        f"cwd = {_toml_string(str(server['cwd']))}",
        "",
        f"[{server_table}.env]",
    ]
    lines.extend(f"{key} = {_toml_string(str(value))}" for key, value in env.items())
    return "\n".join(lines) + "\n"


def _build_config(client: str, command: str, command_args: list[str], project_root: Path) -> str:
    server = _server_config(
        command,
        command_args,
        project_root,
        include_anythingllm_options=client == "anythingllm",
    )
    if client == "codex":
        return _codex_toml_config(server)
    json_client = (
        "generic"
        if client in {"generic", "claude", "cursor", "trae", "anythingllm"}
        else client
    )
    return json.dumps(_json_config(json_client, server), ensure_ascii=False, indent=2) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="生成当前机器可用的MCP配置。")
    parser.add_argument("--output", type=Path, required=True, help="配置文件输出路径。")
    parser.add_argument(
        "--client",
        choices=CLIENT_CHOICES,
        default="generic",
        help="目标客户端；generic、claude、cursor 和 trae 使用标准 mcpServers JSON 格式。",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=PROJECT_ROOT,
        help="项目根目录；生成的 cwd 会写入其绝对路径。",
    )
    parser.add_argument(
        "--runner",
        choices=("auto", "uv", "python", "py"),
        default="auto",
        help="启动运行器；auto 优先 uv，再尝试 python 或 Windows py。",
    )
    args = parser.parse_args()
    command, command_args = _runner_config(args.runner)

    project_root = args.project_root.expanduser().resolve()
    start_script = project_root / "scripts" / "start_mcp.py"
    if not start_script.is_file():
        parser.error(f"项目根目录中未找到 {start_script}")

    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(_build_config(args.client, command, command_args, project_root), encoding="utf-8")
    print(f"已生成 {args.client} 的本机 MCP 配置：{output}")


if __name__ == "__main__":
    main()
