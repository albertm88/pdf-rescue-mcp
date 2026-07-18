from __future__ import annotations

from pathlib import Path

import pdf_rescue_mcp.server as server
import pdf_rescue_mcp.paths as paths


class FakeProcess:
    pid = 24680


def test_background_book_extraction_returns_task_information(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    source = tmp_path / "扫描书.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)

    result = server.start_book_extraction_background(
        str(source),
        output_dir=str(tmp_path / "输出"),
        mode="book-balanced",
        max_pages=12,
        resume=True,
    )

    assert result["状态"] == "已启动"
    assert result["进程ID"] == 24680
    assert (tmp_path / result["日志路径"]).is_file()
    assert result["日志路径"].startswith("logs/")
    command, kwargs = calls[0]
    assert command[0] == server.sys.executable
    assert command[1:5] == ["-u", "-m", "pdf_rescue_mcp.cli", "提取"]
    assert "--resume" in command
    assert "--max-pages" in command
    assert kwargs["stdin"] is server.subprocess.DEVNULL
    assert kwargs["stderr"] is server.subprocess.STDOUT


def test_background_book_extraction_avoids_windows_flags_on_linux(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(paths, "PROJECT_ROOT", tmp_path)
    source = tmp_path / "book.pdf"
    source.write_bytes(b"not inspected by the launcher")
    calls: list[tuple[list[str], dict]] = []

    def fake_popen(command: list[str], **kwargs):
        calls.append((command, kwargs))
        return FakeProcess()

    monkeypatch.setattr(server.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(server.sys, "platform", "linux")

    server.start_book_extraction_background(str(source), output_dir=str(tmp_path / "output"))

    assert "creationflags" not in calls[0][1]


def _plan(route: str, estimated_seconds: int = 5) -> dict[str, object]:
    return {
        "route": route,
        "estimated_seconds": estimated_seconds,
        "page_count": 12,
        "mode": "book-balanced",
        "warnings": [],
    }


def test_primary_tool_extracts_a_clear_request_without_asking_for_tool_choice(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("direct_text_extract"))

    def fake_extract(*args, **kwargs):
        calls.append(kwargs)
        return {"status": "ok", "job_dir": "D:/out/book-rescue-result"}

    monkeypatch.setattr(server, "run_extract_book_text", fake_extract)

    result = server.rescue_pdf(
        path="D:/books/book.pdf",
        request="请分析这本书并提取成可核对的文本",
    )

    assert result["状态"] == "已完成PDF救援"
    assert result["已执行"] == ["诊断PDF", "规划处理任务", "提取书籍文本"]
    assert calls == [{"output_dir": None, "mode": "book-balanced", "max_pages": None, "resume": True, "password": None}]


def test_primary_tool_starts_long_ocr_in_background(monkeypatch) -> None:
    monkeypatch.setattr(server, "run_plan_pdf_job", lambda *args, **kwargs: _plan("ocr_required", 180))
    launches: list[dict[str, object]] = []

    def fake_background(*args, **kwargs):
        launches.append(kwargs)
        return {"状态": "已启动", "任务目录": "D:/out/book-rescue-result", "进程ID": 12345}

    monkeypatch.setattr(server, "start_book_extraction_background", fake_background)

    result = server.rescue_pdf(path="D:/books/scan.pdf", request="把这个扫描PDF救援成文本")

    assert result["状态"] == "已启动后台救援任务"
    assert result["已执行"][-1] == "后台提取书籍"
    assert launches == [{"output_dir": None, "mode": "book-balanced", "max_pages": None, "resume": True}]


def test_primary_tool_routes_existing_job_to_status(monkeypatch) -> None:
    monkeypatch.setattr(
        server,
        "run_read_job_status",
        lambda job_dir: {"status": {"status": "running"}, "job_dir": job_dir},
    )

    result = server.rescue_pdf(job_dir="D:/out/book-rescue-result", request="现在处理进度怎么样")

    assert result["状态"] == "已读取任务状态"
    assert result["已执行"] == ["查看任务状态"]


def test_primary_tool_diagnoses_before_extracting_when_user_only_asks_about_ocr(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_plan(*args, **kwargs):
        calls.append(kwargs)
        return _plan("ocr_required", 180)

    monkeypatch.setattr(server, "run_plan_pdf_job", fake_plan)
    result = server.rescue_pdf(path="D:/books/scan.pdf", request="检查这个PDF是否需要OCR")

    assert result["状态"] == "已完成诊断和规划"
    assert result["已执行"] == ["诊断PDF", "规划处理任务"]
    assert calls == [{"target_quality": "book-balanced", "password": None}]
