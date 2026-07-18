from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path


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
        return "uv", ["run", "--locked", "python", "-B", "scripts/start_mcp.py"]
    if selected == "python":
        if not shutil.which("python"):
            raise SystemExit("未找到 python；请使用 --runner uv 或将 Python 加入系统路径。")
        return "python", ["-B", "scripts/start_mcp.py"]
    if selected == "py" and os.name == "nt":
        if not shutil.which("py"):
            raise SystemExit("未找到 py 启动器；请使用 --runner uv 或 python。")
        return "py", ["-3", "-B", "scripts/start_mcp.py"]
    raise SystemExit(f"不支持的运行器：{runner}")


def main() -> None:
    parser = argparse.ArgumentParser(description="生成当前机器可用的MCP配置。")
    parser.add_argument("--output", type=Path, required=True, help="配置文件输出路径。")
    parser.add_argument(
        "--runner",
        choices=("auto", "uv", "python", "py"),
        default="auto",
        help="启动运行器；auto 优先 uv，再尝试 python 或 Windows py。",
    )
    args = parser.parse_args()
    command, command_args = _runner_config(args.runner)

    env = {
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    if os.name == "nt":
        env["PYTHONLEGACYWINDOWSSTDIO"] = "0"
    config = {
        "mcpServers": {
            "中文PDF书籍救援": {
                "command": command,
                "args": command_args,
                "cwd": ".",
                "env": env,
            }
        }
    }

    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
    print("MCP 配置已生成。")


if __name__ == "__main__":
    main()
