from __future__ import annotations

import json
from pathlib import Path

from pdf_rescue_mcp.history import (
    append_history_event,
    collect_processing_history,
    share_processing_history,
)


def test_history_collects_new_events_and_legacy_status_snapshots(tmp_path: Path) -> None:
    job = tmp_path / "书籍结果"
    append_history_event(
        job,
        "开始处理",
        run_id="run-1",
        values={"来源PDF": str(tmp_path / "第一本.pdf"), "状态": "进行中", "目标页数": 10},
    )
    append_history_event(
        job,
        "处理完成",
        run_id="run-1",
        values={"来源PDF": str(tmp_path / "第一本.pdf"), "状态": "完成", "已处理页数": 10},
    )

    legacy_job = tmp_path / "旧书结果"
    legacy_job.mkdir(parents=True)
    (legacy_job / "状态.json").write_text(
        json.dumps(
            {
                "状态": "完成",
                "来源PDF": str(tmp_path / "旧书.pdf"),
                "目标页数": 3,
                "已处理页数": 3,
                "失败页数": 0,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    result = collect_processing_history(tmp_path, include_sensitive=False)

    assert result["记录数"] == 3
    assert any(item["事件"] == "历史补录" for item in result["记录"])
    assert all(item["来源PDF"] in {"第一本.pdf", "旧书.pdf"} for item in result["记录"])
    assert all(str(tmp_path) not in json.dumps(item, ensure_ascii=False) for item in result["记录"])


def test_share_processing_history_writes_three_formats(tmp_path: Path) -> None:
    job = tmp_path / "结果"
    append_history_event(job, "处理完成", values={"来源PDF": str(tmp_path / "书.pdf"), "状态": "完成"})

    result = share_processing_history(tmp_path)

    assert result["状态"] == "已生成"
    assert result["记录数"] == 1
    for path in result["输出文件"].values():
        assert Path(path).exists()
    assert "书.pdf" in Path(result["输出文件"]["Markdown"]).read_text(encoding="utf-8")
    assert str(tmp_path) not in Path(result["输出文件"]["JSON"]).read_text(encoding="utf-8")
