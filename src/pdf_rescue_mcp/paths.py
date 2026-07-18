"""Project-relative locations for transient files and runtime logs."""

from __future__ import annotations

import logging
import re
import tempfile
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Iterator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
TEMP_CATEGORIES = ("ocr", "mcp", "diagnostics")


def _safe_component(value: str, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip(".-")
    return cleaned or fallback


def temporary_root(category: str) -> Path:
    """Return a typed temporary-file root, such as ``tmp/ocr``."""
    root = PROJECT_ROOT / "tmp" / _safe_component(category, "misc")
    root.mkdir(parents=True, exist_ok=True)
    return root


def ensure_runtime_layout() -> None:
    """Create the stable runtime folder layout on server startup."""
    for category in TEMP_CATEGORIES:
        temporary_root(category)
    (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)


@contextmanager
def temporary_directory(category: str, prefix: str = "run") -> Iterator[Path]:
    """Create an automatically cleaned temporary directory under a typed project root."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    safe_prefix = _safe_component(prefix, "run")
    with tempfile.TemporaryDirectory(
        prefix=f"{safe_prefix}-{timestamp}-",
        dir=temporary_root(category),
    ) as directory:
        yield Path(directory)


def timestamped_log_path(name: str, root: Path | None = None) -> Path:
    """Create ``logs/YYYY-MM-DD/HHMMSS/<name>.log`` without machine-specific paths."""
    now = datetime.now()
    logs_root = root or (PROJECT_ROOT / "logs")
    run_dir = logs_root / now.strftime("%Y-%m-%d") / now.strftime("%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    base_name = _safe_component(name, "runtime")
    candidate = run_dir / f"{base_name}.log"
    suffix = 2
    while candidate.exists():
        candidate = run_dir / f"{base_name}-{suffix}.log"
        suffix += 1
    return candidate


def project_relative_path(path: Path) -> str:
    """Use a stable project-relative path in MCP responses when possible."""
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def configure_file_logging(name: str = "mcp") -> Path:
    """Attach a UTF-8 file handler without writing logs into MCP stdout."""
    ensure_runtime_layout()
    log_path = timestamped_log_path(name)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    if root_logger.level == logging.NOTSET or root_logger.level > logging.INFO:
        root_logger.setLevel(logging.INFO)
    return log_path
