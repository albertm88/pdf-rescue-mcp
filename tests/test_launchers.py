from __future__ import annotations

import importlib.util
import json
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _load_script_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_mcp_client_reuses_current_python_to_avoid_nested_uv(monkeypatch) -> None:
    module = _load_script_module("test_mcp_client", "scripts/mcp_client.py")

    parameters = module.make_server_parameters()

    assert parameters.command == sys.executable
    assert parameters.args == ["-B", "scripts/start_mcp.py"]
    assert parameters.env["PYTHONUTF8"] == "1"


def test_mcp_client_uses_project_root_as_child_working_directory() -> None:
    module = _load_script_module("test_mcp_client_cwd", "scripts/mcp_client.py")

    parameters = module.make_server_parameters()

    assert parameters.command == sys.executable
    assert parameters.args == ["-B", "scripts/start_mcp.py"]
    assert parameters.cwd == str(ROOT)


def test_config_generator_selects_named_portable_runners(monkeypatch) -> None:
    module = _load_script_module("test_mcp_config_generator", "scripts/generate_mcp_config.py")
    monkeypatch.setattr(module.shutil, "which", lambda name: "uv" if name == "uv" else None)

    command, args = module._runner_config("auto")

    assert command == "uv"
    assert args == ["run", "--locked", "--extra", "ocr", "python", "-B", "scripts/start_mcp.py"]


def test_config_generator_uses_absolute_project_root_for_json_clients() -> None:
    module = _load_script_module("test_mcp_config_generator_json", "scripts/generate_mcp_config.py")

    config = json.loads(
        module._build_config(
            "vscode",
            "uv",
            ["run", "--locked", "--extra", "ocr", "python", "-B", "scripts/start_mcp.py"],
            ROOT,
        )
    )

    server = config["servers"]["pdf-rescue"]
    assert server["type"] == "stdio"
    assert server["cwd"] == str(ROOT)
    assert server["cwd"] != "."
    assert server["args"][2:4] == ["--extra", "ocr"]


def test_config_generator_marks_anythingllm_as_on_demand() -> None:
    module = _load_script_module("test_mcp_config_generator_anythingllm", "scripts/generate_mcp_config.py")

    config = json.loads(module._build_config("anythingllm", "python", ["-B", "scripts/start_mcp.py"], ROOT))

    server = config["mcpServers"]["pdf-rescue"]
    assert server["cwd"] == str(ROOT)
    assert server["anythingllm"] == {"autoStart": False}


def test_config_generator_supports_trae_json() -> None:
    module = _load_script_module("test_mcp_config_generator_trae", "scripts/generate_mcp_config.py")

    config = json.loads(module._build_config("trae", "python", ["-B", "scripts/start_mcp.py"], ROOT))

    server = config["mcpServers"]["pdf-rescue"]
    assert server["command"] == "python"
    assert server["cwd"] == str(ROOT)


def test_config_generator_renders_parseable_codex_toml() -> None:
    module = _load_script_module("test_mcp_config_generator_codex", "scripts/generate_mcp_config.py")

    config = tomllib.loads(
        module._build_config(
            "codex",
            "uv",
            ["run", "--locked", "--extra", "ocr", "python", "-B", "scripts/start_mcp.py"],
            ROOT,
        )
    )

    server = config["mcp_servers"]["pdf-rescue"]
    assert server["command"] == "uv"
    assert server["cwd"] == str(ROOT)
    assert server["env"]["PYTHONUTF8"] == "1"


def test_config_generator_cli_writes_absolute_cwd(monkeypatch, tmp_path: Path) -> None:
    module = _load_script_module("test_mcp_config_generator_cli", "scripts/generate_mcp_config.py")
    output = tmp_path / "anythingllm.json"
    monkeypatch.setattr(
        module,
        "_runner_config",
        lambda _runner: ("uv", ["run", "--locked", "--extra", "ocr", "python", "-B", "scripts/start_mcp.py"]),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "generate_mcp_config.py",
            "--client",
            "anythingllm",
            "--output",
            str(output),
        ],
    )

    module.main()

    generated = json.loads(output.read_text(encoding="utf-8"))
    server = generated["mcpServers"]["pdf-rescue"]
    assert server["cwd"] == str(ROOT)
    assert server["cwd"] != "."
    assert server["anythingllm"] == {"autoStart": False}


def test_checked_in_stdio_templates_are_portable_and_use_ocr_extra() -> None:
    json_templates = (
        "mcp-config.json",
        "mcp-config.windows.json",
        "mcp-config.linux.json",
        "mcp-config.vscode.json",
        "mcp-config.claude-cursor.json",
        "mcp-config.trae.json",
        "mcp-config.anythingllm.json",
    )
    for template_name in json_templates:
        config = json.loads((ROOT / "examples" / template_name).read_text(encoding="utf-8"))
        servers = config.get("servers", config.get("mcpServers"))
        assert servers is not None
        server = servers["pdf-rescue"]
        assert server["command"] == "uv"
        assert server["args"][0:5] == ["run", "--locked", "--extra", "ocr", "python"]
        assert server["cwd"] != "."

    codex = tomllib.loads((ROOT / "examples" / "mcp-config.codex.toml").read_text(encoding="utf-8"))
    server = codex["mcp_servers"]["pdf-rescue"]
    assert server["command"] == "uv"
    assert server["cwd"] == "{{PROJECT_ROOT}}"
    assert server["args"][0:5] == ["run", "--locked", "--extra", "ocr", "python"]


def test_workspace_vscode_config_uses_standard_stdio_runner() -> None:
    config = json.loads((ROOT / ".vscode" / "mcp.json").read_text(encoding="utf-8-sig"))

    server = config["servers"]["pdf-rescue"]
    assert server["type"] == "stdio"
    assert server["command"] == "uv"
    assert server["args"][0:5] == ["run", "--locked", "--extra", "ocr", "python"]
