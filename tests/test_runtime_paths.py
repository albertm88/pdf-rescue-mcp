from __future__ import annotations

from pathlib import Path

import pytest

from pdf_rescue_mcp.runtime_paths import RUNTIME_ROOT_ENV, resolve_runtime_paths


def test_explicit_runtime_root_is_portable_and_created_only_on_request(tmp_path: Path) -> None:
    root = tmp_path / "portable-runtime"

    paths = resolve_runtime_paths(environ={RUNTIME_ROOT_ENV: str(root)})

    assert paths.root_dir == root
    assert paths.config_dir == root / "config"
    assert paths.state_dir == root / "state"
    assert paths.cache_dir == root / "cache"
    assert paths.log_dir == root / "logs"
    assert not root.exists()

    assert paths.ensure() is paths
    assert all(directory.is_dir() for directory in paths.directories)


def test_linux_xdg_layout_uses_user_dirs_not_current_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_checkout = tmp_path / "source-checkout"
    source_checkout.mkdir()
    monkeypatch.chdir(source_checkout)
    home = tmp_path / "user-home"
    environment = {
        "XDG_CONFIG_HOME": str(home / "cfg"),
        "XDG_STATE_HOME": str(home / "state"),
        "XDG_CACHE_HOME": str(home / "cache"),
    }

    paths = resolve_runtime_paths(environ=environment, platform="linux", home=home)
    paths.ensure()

    assert paths.config_dir == home / "cfg" / "pdf-rescue-mcp"
    assert paths.state_dir == home / "state" / "pdf-rescue-mcp"
    assert paths.cache_dir == home / "cache" / "pdf-rescue-mcp"
    assert paths.log_dir == home / "state" / "pdf-rescue-mcp" / "logs"
    assert not any(source_checkout.iterdir())


def test_linux_default_layout_uses_xdg_fallbacks(tmp_path: Path) -> None:
    home = tmp_path / "home"

    paths = resolve_runtime_paths(environ={}, platform="linux", home=home)

    assert paths.config_dir == home / ".config" / "pdf-rescue-mcp"
    assert paths.state_dir == home / ".local" / "state" / "pdf-rescue-mcp"
    assert paths.cache_dir == home / ".cache" / "pdf-rescue-mcp"
    assert paths.log_dir == home / ".local" / "state" / "pdf-rescue-mcp" / "logs"


def test_macos_layout_uses_application_support_cache_and_logs(tmp_path: Path) -> None:
    home = tmp_path / "mac-home"

    paths = resolve_runtime_paths(environ={}, platform="darwin", home=home)

    assert paths.config_dir == home / "Library" / "Application Support" / "pdf-rescue-mcp" / "config"
    assert paths.state_dir == home / "Library" / "Application Support" / "pdf-rescue-mcp" / "state"
    assert paths.cache_dir == home / "Library" / "Caches" / "pdf-rescue-mcp"
    assert paths.log_dir == home / "Library" / "Logs" / "pdf-rescue-mcp"


def test_windows_layout_separates_roaming_config_from_local_runtime(tmp_path: Path) -> None:
    home = tmp_path / "windows-home"
    roaming = home / "Roaming"
    local = home / "Local"

    paths = resolve_runtime_paths(
        environ={"APPDATA": str(roaming), "LOCALAPPDATA": str(local)},
        platform="win32",
        home=home,
    )

    assert paths.config_dir == roaming / "pdf-rescue-mcp" / "config"
    assert paths.state_dir == local / "pdf-rescue-mcp" / "state"
    assert paths.cache_dir == local / "pdf-rescue-mcp" / "cache"
    assert paths.log_dir == local / "pdf-rescue-mcp" / "logs"


@pytest.mark.parametrize(
    ("environment", "platform"),
    [
        ({RUNTIME_ROOT_ENV: "relative-runtime"}, "linux"),
        ({"XDG_STATE_HOME": "relative-state"}, "linux"),
    ],
)
def test_relative_environment_directories_are_rejected(
    tmp_path: Path, environment: dict[str, str], platform: str
) -> None:
    with pytest.raises(ValueError, match="absolute path"):
        resolve_runtime_paths(environ=environment, platform=platform, home=tmp_path)


def test_ensure_rejects_a_file_instead_of_overwriting_it(tmp_path: Path) -> None:
    root = tmp_path / "runtime"
    root.mkdir()
    (root / "state").write_text("not a directory", encoding="utf-8")
    paths = resolve_runtime_paths(environ={RUNTIME_ROOT_ENV: str(root)})

    with pytest.raises(NotADirectoryError, match="not a directory"):
        paths.ensure()
