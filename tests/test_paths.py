from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import pdf_rescue_mcp.paths as paths


def test_temporary_directory_is_grouped_by_type_and_cleaned(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)

    with paths.temporary_directory("ocr", prefix="page") as directory:
        assert directory.parent == tmp_path / "tmp" / "ocr"
        marker = directory / "page.png"
        marker.write_text("test", encoding="utf-8")
        assert marker.exists()

    assert not directory.exists()
    assert (tmp_path / "tmp" / "ocr").is_dir()


def test_timestamped_log_path_uses_date_and_time_folders(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 7, 13, 9, 15, 30)

    monkeypatch.setattr(paths, "datetime", FixedDatetime)
    log_path = paths.timestamped_log_path("mcp")

    assert log_path == tmp_path / "logs" / "2026-07-13" / "091530" / "mcp.log"
    assert log_path.parent.is_dir()
    assert paths.project_relative_path(log_path) == "logs/2026-07-13/091530/mcp.log"


def test_file_logging_uses_runtime_layout_and_never_stdout(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    root_logger = logging.getLogger()
    existing_handlers = list(root_logger.handlers)

    try:
        log_path = paths.configure_file_logging("mcp")
        logging.getLogger("pdf_rescue_mcp.test").info("runtime log test")
        for handler in root_logger.handlers:
            handler.flush()
        assert log_path.is_file()
        assert "runtime log test" in log_path.read_text(encoding="utf-8")
        assert (tmp_path / "tmp" / "ocr").is_dir()
        assert (tmp_path / "tmp" / "mcp").is_dir()
        assert (tmp_path / "tmp" / "diagnostics").is_dir()
    finally:
        for handler in list(root_logger.handlers):
            if handler not in existing_handlers:
                root_logger.removeHandler(handler)
                handler.close()
