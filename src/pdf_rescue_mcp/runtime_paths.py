"""Cross-platform per-user directories for persistent runtime state.

This module deliberately does not use :mod:`pdf_rescue_mcp.paths`.  The
existing module is project-relative and is useful during development, whereas
long-lived jobs must not assume that the package source tree is writable.

``PDF_RESCUE_RUNTIME_ROOT`` is an opt-in portable layout.  When it is unset,
the layout follows the platform's per-user application-data conventions using
only the Python standard library.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


DEFAULT_APP_NAME = "pdf-rescue-mcp"
RUNTIME_ROOT_ENV = "PDF_RESCUE_RUNTIME_ROOT"


@dataclass(frozen=True, slots=True)
class RuntimePaths:
    """Persistent directories owned by the local PDF Rescue installation.

    ``root_dir`` is set only when the portable root environment variable was
    used.  Calling :meth:`ensure` is explicit so callers can inspect a layout
    without creating directories during imports or dry-run operations.
    """

    config_dir: Path
    state_dir: Path
    cache_dir: Path
    log_dir: Path
    root_dir: Path | None = None

    @property
    def directories(self) -> tuple[Path, Path, Path, Path]:
        """Return directories in creation order."""
        return (self.config_dir, self.state_dir, self.cache_dir, self.log_dir)

    def ensure(self) -> "RuntimePaths":
        """Create the layout without ever deriving it from the current cwd.

        A relative directory is rejected rather than accidentally creating
        runtime files in a source checkout because of a bad environment value.
        """
        for directory in self.directories:
            if not directory.is_absolute():
                raise ValueError(f"Runtime directory must be absolute: {directory}")
            if directory.exists() and not directory.is_dir():
                raise NotADirectoryError(f"Runtime path is not a directory: {directory}")
            directory.mkdir(parents=True, exist_ok=True)
        return self


def resolve_runtime_paths(
    *,
    app_name: str = DEFAULT_APP_NAME,
    environ: Mapping[str, str] | None = None,
    platform: str | None = None,
    home: Path | None = None,
) -> RuntimePaths:
    """Resolve a per-user runtime layout without creating it.

    ``platform`` and ``home`` are injectable chiefly for deterministic tests.
    They also make it possible for launchers to resolve a target user's layout
    without mutating process-global environment variables.
    """
    environment = os.environ if environ is None else environ
    safe_app_name = _validate_app_name(app_name)
    root = _optional_environment_path(environment, RUNTIME_ROOT_ENV)
    if root is not None:
        return RuntimePaths(
            config_dir=root / "config",
            state_dir=root / "state",
            cache_dir=root / "cache",
            log_dir=root / "logs",
            root_dir=root,
        )

    platform_name = sys.platform if platform is None else platform
    home_dir = _home_directory(home)
    if platform_name.startswith("win"):
        return _windows_paths(environment, home_dir, safe_app_name)
    if platform_name == "darwin":
        return _macos_paths(home_dir, safe_app_name)
    return _xdg_paths(environment, home_dir, safe_app_name)


def ensure_runtime_paths(**kwargs: object) -> RuntimePaths:
    """Resolve and create the persistent runtime layout."""
    return resolve_runtime_paths(**kwargs).ensure()


def _windows_paths(
    environ: Mapping[str, str], home: Path, app_name: str
) -> RuntimePaths:
    roaming_base = _environment_path_or_default(
        environ, "APPDATA", home / "AppData" / "Roaming"
    )
    local_base = _environment_path_or_default(
        environ, "LOCALAPPDATA", home / "AppData" / "Local"
    )
    return RuntimePaths(
        config_dir=roaming_base / app_name / "config",
        state_dir=local_base / app_name / "state",
        cache_dir=local_base / app_name / "cache",
        log_dir=local_base / app_name / "logs",
    )


def _macos_paths(home: Path, app_name: str) -> RuntimePaths:
    application_support = home / "Library" / "Application Support" / app_name
    return RuntimePaths(
        config_dir=application_support / "config",
        state_dir=application_support / "state",
        cache_dir=home / "Library" / "Caches" / app_name,
        log_dir=home / "Library" / "Logs" / app_name,
    )


def _xdg_paths(environ: Mapping[str, str], home: Path, app_name: str) -> RuntimePaths:
    config_base = _environment_path_or_default(environ, "XDG_CONFIG_HOME", home / ".config")
    state_base = _environment_path_or_default(
        environ, "XDG_STATE_HOME", home / ".local" / "state"
    )
    cache_base = _environment_path_or_default(environ, "XDG_CACHE_HOME", home / ".cache")
    return RuntimePaths(
        config_dir=config_base / app_name,
        state_dir=state_base / app_name,
        cache_dir=cache_base / app_name,
        log_dir=state_base / app_name / "logs",
    )


def _home_directory(home: Path | None) -> Path:
    candidate = Path.home() if home is None else Path(home).expanduser()
    if not candidate.is_absolute():
        raise ValueError(f"Home directory must be absolute: {candidate}")
    return candidate


def _environment_path_or_default(
    environ: Mapping[str, str], name: str, default: Path
) -> Path:
    return _optional_environment_path(environ, name) or default


def _optional_environment_path(environ: Mapping[str, str], name: str) -> Path | None:
    value = environ.get(name)
    if not value:
        return None
    path = Path(value).expanduser()
    if not path.is_absolute():
        raise ValueError(f"{name} must be an absolute path: {value}")
    return path


def _validate_app_name(app_name: str) -> str:
    if not app_name or app_name in {".", ".."}:
        raise ValueError("Application name must be a non-empty directory name")
    invalid_characters = set('\\/:*?"<>|\x00')
    if any(character in invalid_characters for character in app_name):
        raise ValueError(f"Application name is not a safe directory name: {app_name!r}")
    return app_name
