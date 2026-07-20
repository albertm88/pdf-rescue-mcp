import importlib.util
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).parents[1] / "scripts" / "batch_extract_all.py"
    spec = importlib.util.spec_from_file_location("batch_extract_all_test", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_batch_monitor_report_contains_the_fixed_worker_fields() -> None:
    script = _load_script_module()
    report = script.format_batch_monitor_report(
        {
            "运行中": True,
            "固定监测格式": {
                "书籍": "农业化学卷",
                "已用时间": "12分0秒",
                "剩余时间": "8分0秒",
                "处理速度": "1.000页/分钟（60.00秒/页，近2页/2分0秒）",
                "剩余书本数量": 3,
                "进度": {"文本": "7/100（7.0%）"},
                "OCR线程预算": 4,
                "进程线程数": 27,
                "CPU整机占比": 51.9,
                "CPU等效核心": 8.31,
                "RSS内存": 512.0,
                "当前worker任务列表": [
                    {
                        "标记": "v",
                        "书籍": "已完成书",
                    },
                    {
                        "标记": "-",
                        "书籍": "农业化学卷",
                        "已用时间": "12分0秒",
                        "剩余时间": "8分0秒",
                        "近期实际处理速度": "1.000页/分钟（60.00秒/页）",
                        "实际Worker PID": 97531,
                        "进度": {"文本": "7/100（7.0%）"},
                        "OCR线程预算": 4,
                        "进程线程数": 27,
                        "CPU整机占比": 51.9,
                        "CPU等效核心": 8.31,
                        "RSS内存MB": 512.0,
                    },
                ],
            },
        }
    )

    for field in (
        "书籍:",
        "已用时间:",
        "剩余时间:",
        "处理速度:",
        "当前worker任务列表:",
        "剩余书本数量:",
        "OCR线程预算:",
        "进程线程数:",
        "CPU整机占比:",
        "CPU等效核心:",
        "RSS内存:",
    ):
        assert field in report
    assert "v 已完成书" in report
    assert "- 农业化学卷" in report
    assert "PID: 97531" in report
    assert "CPU整机占比: 51.9%" in report
    assert "CPU等效核心: 8.31核" in report
    assert "831%" not in report
