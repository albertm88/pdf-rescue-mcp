from __future__ import annotations

import importlib.util
import sys
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
    assert args == ["run", "--locked", "python", "-B", "scripts/start_mcp.py"]
