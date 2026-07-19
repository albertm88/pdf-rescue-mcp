from __future__ import annotations

import html
import json
import os
import re
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable

import yaml

from .models import (
    ChunkRecord,
    ExtractionResult,
    PageRecord,
    PdfType,
    ensure_path,
)
from .ocr_engines import (
    available_ocr_engine,
    create_ocr_adapter,
    ocr_pdf_page,
    render_pdf_page,
    text_from_ocr_blocks_in_reading_order,
)
from .history import append_history_event
from .pdf_inspector import extract_direct_text_pages, inspect_pdf_text_layer
from .zh import zh_data

LOW_CONFIDENCE_THRESHOLD = 0.9
LOW_CONFIDENCE_RETRY_DPI = 300
LOW_CONFIDENCE_MIN_TEXT_RATIO = 0.85
TERM_GLOSSARY_PATH = Path(__file__).with_name("术语词表.yaml")


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _format_duration(seconds: float | int | None) -> str:
    """把秒数格式化为状态接口可直接展示的中文时长。"""
    if seconds is None:
        return "未知"
    total_seconds = max(0, int(round(float(seconds))))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds_part = divmod(remainder, 60)
    if hours:
        return f"{hours}小时{minutes}分{seconds_part}秒"
    if minutes:
        return f"{minutes}分{seconds_part}秒"
    return f"{seconds_part}秒"


def _processing_metrics(status: dict) -> dict[str, object]:
    """从业务层状态文件计算统一的页级指标。

    该函数只读取业务状态，不读取进程、不重启任务；进程资源和恢复信息
    由监控/优化层在MCP返回中补充，保证三层职责不互相阻塞。
    """
    source_pdf = str(status.get("来源PDF") or "")
    book_name = Path(source_pdf).stem if source_pdf else None
    try:
        target_pages = int(status.get("目标页数") or status.get("PDF总页数") or 0)
    except (TypeError, ValueError):
        target_pages = 0
    try:
        processed_pages = int(status.get("已处理页数") or 0)
    except (TypeError, ValueError):
        processed_pages = 0
    progress = round(processed_pages / target_pages * 100, 1) if target_pages else 0.0

    elapsed_seconds: int | None = None
    raw_elapsed = status.get("已耗时秒")
    if raw_elapsed is not None:
        try:
            elapsed_seconds = max(0, int(round(float(raw_elapsed))))
        except (TypeError, ValueError):
            pass
    if status.get("开始时间"):
        try:
            started_at = datetime.fromisoformat(str(status["开始时间"]))
            now = datetime.now(started_at.tzinfo) if started_at.tzinfo else datetime.now()
            live_elapsed = max(0, int(round((now - started_at).total_seconds())))
            # 运行中的状态文件可能只在页面边界更新，查询时用当前时间补齐这一段。
            if elapsed_seconds is None or status.get("状态") in {"进行中", "启动中"}:
                elapsed_seconds = live_elapsed
        except (TypeError, ValueError):
            pass

    speed_seconds: float | None = None
    raw_speed = status.get("平均每页秒")
    if raw_speed is not None:
        try:
            speed_seconds = round(float(raw_speed), 2)
        except (TypeError, ValueError):
            pass
    if speed_seconds is None and elapsed_seconds and processed_pages > 0:
        speed_seconds = round(elapsed_seconds / processed_pages, 2)

    remaining_seconds: int | None = None
    raw_remaining = status.get("预计剩余秒")
    if raw_remaining is not None:
        try:
            remaining_seconds = max(0, int(round(float(raw_remaining))))
        except (TypeError, ValueError):
            pass
    if remaining_seconds is None and speed_seconds is not None:
        remaining_seconds = max(0, int(round(speed_seconds * max(target_pages - processed_pages, 0))))

    return {
        "书籍名": book_name,
        "总处理页数": target_pages,
        "已处理页数": processed_pages,
        "处理进度": progress,
        "处理进度文本": f"{progress:.1f}%",
        "运行时间秒": elapsed_seconds,
        "运行时间": _format_duration(elapsed_seconds),
        "剩余时间秒": remaining_seconds,
        "剩余时间": _format_duration(remaining_seconds),
        "处理速度": speed_seconds,
        "处理速度文本": f"{speed_seconds:.2f}秒/页" if speed_seconds is not None else "未知",
    }


class _CancellationRequested(RuntimeError):
    """Raised at a page boundary when a background worker is asked to stop."""


class _WorkerHeartbeat:
    """A small, independent liveness signal for an externally managed OCR worker.

    Page status only changes after a page has completed.  A difficult page can therefore
    legitimately take much longer than the normal page cadence.  The supervisor therefore
    receives two deliberately distinct signals: a frequent liveness heartbeat and durable
    page-boundary progress.  A live heartbeat with an unchanged page is diagnosable as a
    stuck page rather than silently being treated as healthy OCR.
    """

    INTERVAL_SECONDS = 5

    def __init__(self, root: Path, external_stop_flag: object | None = None) -> None:
        heartbeat_text = os.environ.get("PDF_RESCUE_HEARTBEAT_PATH")
        cancel_text = os.environ.get("PDF_RESCUE_CANCEL_PATH")
        self.path = Path(heartbeat_text) if heartbeat_text else None
        self.cancel_path = Path(cancel_text) if cancel_text else None
        self.root = root
        self.external_stop_flag = external_stop_flag
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state_lock = threading.Lock()
        self._current_page: int | None = None
        self._current_page_started_at: str | None = None
        self._last_completed_page: int | None = None
        self._last_progress_at: str | None = None
        self._task_store: object | None = None
        self._attempt_id: str | None = None
        task_database = os.environ.get("PDF_RESCUE_TASK_DATABASE")
        attempt_id = os.environ.get("PDF_RESCUE_TASK_ATTEMPT_ID")
        if task_database and attempt_id:
            try:
                # Imported lazily: direct library use remains independent from the
                # supervision store, while an isolated MCP worker can emit durable
                # attempt/page events without importing FastMCP.
                from .task_store import TaskStore

                self._task_store = TaskStore(task_database)
                self._attempt_id = attempt_id
            except Exception:
                # The business worker must continue when its optional audit database
                # is temporarily unavailable.  Its file heartbeat remains usable.
                self._task_store = None
                self._attempt_id = None

    @property
    def enabled(self) -> bool:
        return self.path is not None or self._task_store is not None

    def _record_store(self, method: str, *args: object, **kwargs: object) -> None:
        store = self._task_store
        attempt_id = self._attempt_id
        if store is None or attempt_id is None:
            return
        try:
            getattr(store, method)(attempt_id, *args, **kwargs)
        except Exception:
            # A state-store update is advisory to the worker.  The supervisor will
            # reconcile from the atomic page cache and status file if it misses one.
            pass

    def set_total_pages(self, total_pages: int) -> None:
        """Tell the durable supervision store the inspected page count."""
        store = self._task_store
        attempt_id = self._attempt_id
        if store is None or attempt_id is None:
            return
        try:
            attempt = store.get_attempt(attempt_id)
            store.set_total_pages(attempt.job_id, total_pages)
        except Exception:
            pass

    def page_started(self, page_number: int) -> None:
        """Publish the page watchdog boundary immediately before page work."""
        started_at = _now()
        with self._state_lock:
            self._current_page = page_number
            self._current_page_started_at = started_at
        self._record_store("record_page_started", page_number)
        self._write("运行中")

    def page_completed(self, page: PageRecord) -> None:
        """Publish durable forward progress only after cache and status commit."""
        completed_at = _now()
        with self._state_lock:
            self._current_page = None
            self._current_page_started_at = None
            self._last_completed_page = page.page_number
            self._last_progress_at = completed_at
        self._record_store(
            "record_page_completed",
            page.page_number,
            result={
                "source": page.source,
                "confidence": round(float(page.confidence), 4),
                "has_text": bool(page.text.strip()),
            },
        )
        self._write("运行中")

    def page_failed(self, page_number: int, error_type: str) -> None:
        """Record an OCR failure boundary without exposing exception text/secrets."""
        completed_at = _now()
        with self._state_lock:
            self._current_page = None
            self._current_page_started_at = None
            self._last_progress_at = completed_at
        self._record_store("record_page_failed", page_number, error={"kind": error_type})
        self._write("运行中")

    def is_set(self) -> bool:
        external_is_set = getattr(self.external_stop_flag, "is_set", None)
        return bool(
            (callable(external_is_set) and external_is_set())
            or (self.cancel_path is not None and self.cancel_path.exists())
        )

    def _write(self, state: str) -> None:
        with self._state_lock:
            current_page = self._current_page
            current_page_started_at = self._current_page_started_at
            last_completed_page = self._last_completed_page
            last_progress_at = self._last_progress_at
        payload = {
            "状态": state,
            "进程ID": os.getpid(),
            "任务目录": str(self.root),
            "更新时间": _now(),
            "当前页": current_page,
            "当前页开始时间": current_page_started_at,
            "最后完成页": last_completed_page,
            "最后进度时间": last_progress_at,
        }
        self._record_store("record_heartbeat", worker_pid=os.getpid())
        if self.path is None:
            return
        try:
            _write_json(self.path, payload)
        except OSError:
            # A heartbeat must never make OCR fail just because its monitor folder is unavailable.
            pass

    def start(self) -> None:
        if not self.enabled:
            return
        self._write("运行中")

        def _loop() -> None:
            while not self._stop.wait(self.INTERVAL_SECONDS):
                self._write("运行中")

        self._thread = threading.Thread(target=_loop, daemon=True, name="ocr-heartbeat")
        self._thread.start()

    def finish(self, state: str) -> None:
        if not self.enabled:
            return
        self._stop.set()
        self._write(state)


def _read_term_glossary() -> tuple[list[dict], str | None]:
    if not TERM_GLOSSARY_PATH.exists():
        return [], None
    try:
        payload = yaml.safe_load(TERM_GLOSSARY_PATH.read_text(encoding="utf-8")) or {}
    except OSError:
        return [], "术语词表无法读取，已跳过"
    except yaml.YAMLError:
        return [], "术语词表格式错误，已跳过"
    rules = payload.get("规则", []) if isinstance(payload, dict) else []
    if not isinstance(rules, list):
        return [], "术语词表中的规则必须是列表"
    return [rule for rule in rules if isinstance(rule, dict)], None


def _term_glossary_rules() -> list[dict]:
    rules, _ = _read_term_glossary()
    return rules


def _apply_configured_term_glossary(
    text: str,
    book_title: str | None,
) -> tuple[str, list[str], str | None]:
    if not book_title:
        return text, [], None
    corrected = text
    applied_rules: list[str] = []
    rules, error = _read_term_glossary()
    if error:
        return corrected, applied_rules, error
    for rule in rules:
        keywords = rule.get("书名包含", [])
        replacements = rule.get("替换", {})
        if not isinstance(keywords, list) or not all(str(keyword) in book_title for keyword in keywords):
            continue
        if not isinstance(replacements, dict):
            continue
        before = corrected
        for wrong, right in replacements.items():
            corrected = corrected.replace(str(wrong), str(right))
        if corrected != before:
            applied_rules.append(str(rule.get("名称") or "未命名词表"))
    return corrected, applied_rules, None


def get_term_glossary() -> dict:
    rules, error = _read_term_glossary()
    return {"词表路径": str(TERM_GLOSSARY_PATH), "规则": rules, "错误": error}


def add_term_glossary_replacement(
    rule_name: str,
    title_keywords: list[str],
    wrong: str,
    right: str,
) -> dict:
    clean_name = rule_name.strip()
    clean_keywords = [keyword.strip() for keyword in title_keywords if keyword.strip()]
    if not clean_name or not clean_keywords:
        raise ValueError("规则名称和书名包含条件不能为空")
    if not wrong or not right or wrong == right:
        raise ValueError("错字和正字必须不同且不能为空")
    if any("\n" in value or "\r" in value for value in (wrong, right)):
        raise ValueError("词表替换暂不支持跨行文本")
    rules, error = _read_term_glossary()
    if error:
        raise ValueError(f"无法更新术语词表：{error}")
    matched_rule = next(
        (
            rule
            for rule in rules
            if str(rule.get("名称") or "") == clean_name
            and rule.get("书名包含") == clean_keywords
        ),
        None,
    )
    if matched_rule is None:
        matched_rule = {"名称": clean_name, "书名包含": clean_keywords, "替换": {}}
        rules.append(matched_rule)
    replacements = matched_rule.setdefault("替换", {})
    if not isinstance(replacements, dict):
        raise ValueError("目标规则的替换内容格式错误")
    previous = replacements.get(wrong)
    replacements[wrong] = right
    temporary_path = TERM_GLOSSARY_PATH.with_suffix(".tmp")
    temporary_path.write_text(
        yaml.safe_dump({"规则": rules}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    temporary_path.replace(TERM_GLOSSARY_PATH)
    return {
        "词表路径": str(TERM_GLOSSARY_PATH),
        "规则名称": clean_name,
        "书名包含": clean_keywords,
        "错字": wrong,
        "正字": right,
        "原正字": previous,
    }


def _safe_name(path: Path) -> str:
    name = re.sub(r"[^\w\u4e00-\u9fff.-]+", "-", path.stem, flags=re.UNICODE).strip("-")
    return name or "未命名书籍"


def _dpi_for_mode(mode: str) -> int:
    normalized = mode.lower().replace("_", "-")
    if "fast" in normalized:
        return 180
    if "quality" in normalized or "forensic" in normalized or "archival" in normalized:
        return 300
    return 220


def _write_jsonl(path: Path, records: Iterable[dict]) -> None:
    content = "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records)
    _write_text_atomically(path, content)


def _write_json(path: Path, payload: dict) -> None:
    _write_text_atomically(path, json.dumps(payload, ensure_ascii=False, indent=2))


def _write_text_atomically(path: Path, content: str) -> None:
    """Commit a complete UTF-8 file in one replace operation.

    Status, cache and audit records are read by another process while a worker is
    running.  A unique sibling temporary file avoids readers observing a half-written
    JSON document and avoids colliding with an old worker during recovery.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink(missing_ok=True)
            except OSError:
                pass


def _read_json_with_retry(path: Path, *, attempts: int = 3) -> dict:
    """Read a JSON object through a concurrent legacy writer without false failure."""
    last_error: Exception | None = None
    for index in range(max(1, attempts)):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError(f"JSON对象必须是字典：{path}")
            return payload
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            last_error = exc
            if index + 1 < max(1, attempts):
                time.sleep(0.02)
    assert last_error is not None
    raise last_error


def _cleanup_ocr_text(text: str) -> str:
    lines = []
    for line in text.replace("\x00", "").splitlines():
        line = re.sub(r"\s+", " ", line).strip()
        line = re.sub(r"(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])", "", line)
        if line:
            lines.append(line)
    cleaned = "\n".join(lines)
    cleaned = re.sub(r"(?<=[A-Za-z])-\n(?=[a-z])", "", cleaned)
    cleaned = cleaned.replace("0CR", "OCR").replace("ＯＣＲ", "OCR")
    return cleaned.strip()


def _normalize_illustration_caption_markers(text: str) -> tuple[str, bool]:
    normalized, count = re.subn(
        r"(?m)^[∇▽▼>√D](?=\s*\d{1,3}(?:\s*[\u4e00-\u9fff]|$))",
        "△",
        text,
    )
    return normalized, bool(count)


def _looks_like_front_matter(text: str, page_number: int) -> bool:
    if page_number > 20:
        return False
    bordered_text = f"\n{text.strip()}\n"
    split_label_markers = (
        "\n前\n言\n",
        "\n凡\n例\n",
        "\n目\n录\n",
        "\n委\n员",
        "\n主\n任",
        "\n编\n辑",
        "\n印\n制",
    )
    if any(marker in bordered_text for marker in split_label_markers):
        return True
    if page_number > 10:
        return False
    if len(text.strip()) < 120:
        return True
    markers = ("ISBN", "定价", "出版", "发行", "印刷", "开本", "第1版", "第1次")
    return any(marker in text for marker in markers)


def _volume_title_from_book_title(book_title: str | None) -> str | None:
    if not book_title:
        return None
    for separator in ("：", ":"):
        if separator in book_title:
            volume_title = book_title.rsplit(separator, maxsplit=1)[-1].strip()
            return volume_title or None
    prefix = "中国农业百科全书"
    if book_title.startswith(prefix):
        volume_title = book_title.removeprefix(prefix).strip(" -_：:")
        return volume_title or None
    return None


def _volume_title_candidates(book_title: str | None) -> list[str]:
    volume_title = _volume_title_from_book_title(book_title)
    if not volume_title:
        return []
    candidates = [volume_title]
    for separator in ("-", "－", "—", "–"):
        if separator in volume_title:
            candidate = volume_title.rsplit(separator, maxsplit=1)[-1].strip()
            if candidate:
                candidates.append(candidate)
    if "分册" in volume_title:
        candidate = volume_title.rsplit("分册", maxsplit=1)[-1].strip(" -_：:－—–")
        if candidate:
            candidates.append(candidate)
    result: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in result:
            result.append(candidate)
    return result


def _correct_volume_title(text: str, volume_title: str | None) -> tuple[str, bool]:
    if not volume_title or volume_title in text:
        return text, False
    lines = text.splitlines()
    changed = False
    title_chars = set(volume_title)
    char_indexes = [
        index
        for index, line in enumerate(lines)
        if len(line.strip()) == 1 and line.strip() in title_chars
    ]
    merged_single_chars = False
    if 2 <= len(volume_title) <= 6 and title_chars.issubset({lines[index].strip() for index in char_indexes}):
        first_index = char_indexes[0]
        lines[first_index] = volume_title
        for index in reversed(char_indexes[1:]):
            del lines[index]
        changed = True
        merged_single_chars = True

    index = 0
    while index < len(lines):
        line = lines[index].strip()
        next_line = lines[index + 1].strip() if index + 1 < len(lines) else ""
        if next_line and (line + next_line == volume_title or line + next_line + "卷" == volume_title):
            lines[index] = volume_title
            del lines[index + 1]
            changed = True
            continue
        if (
            not merged_single_chars
            and len(line) <= 2
            and line
            and line in volume_title
            and len(volume_title) <= 4
        ):
            lines[index] = volume_title
            changed = True
        index += 1
    return "\n".join(lines), changed


def _dedupe_repeated_volume_titles(text: str, volume_titles: list[str]) -> tuple[str, bool]:
    if not volume_titles:
        return text, False
    title_set = {title for title in volume_titles if title}
    seen: set[str] = set()
    lines: list[str] = []
    changed = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped in title_set:
            if stripped in seen:
                changed = True
                continue
            seen.add(stripped)
        lines.append(line)
    return "\n".join(lines), changed


def _merge_front_matter_split_labels(text: str, page_number: int) -> tuple[str, bool]:
    lines = text.splitlines()
    merged: list[str] = []
    changed = False
    index = 0
    while index < len(lines):
        line = lines[index].strip()
        next_line = lines[index + 1].strip() if index + 1 < len(lines) else ""
        if line == "前" and next_line == "言":
            merged.append("前言")
            index += 2
            changed = True
            continue
        if line == "凡" and next_line == "例":
            merged.append("凡例")
            index += 2
            changed = True
            continue
        if line == "目" and next_line == "录":
            merged.append("目录")
            index += 2
            changed = True
            continue
        if line == "委" and next_line.startswith("员"):
            merged.append("委员" + next_line.removeprefix("员"))
            index += 2
            changed = True
            continue
        if line == "委" and next_line.startswith("("):
            merged.append("委员")
            index += 1
            changed = True
            continue
        if line == "主" and next_line.startswith("任"):
            merged.append("主任" + next_line.removeprefix("任"))
            index += 2
            changed = True
            continue
        if line == "秘" and next_line.startswith("书"):
            merged.append("秘书" + next_line.removeprefix("书"))
            index += 2
            changed = True
            continue
        if line == "总" and next_line.startswith("主编"):
            merged.append("总" + next_line)
            index += 2
            changed = True
            continue
        if line == "编" and next_line.startswith("辑"):
            merged.append("编辑" + next_line.removeprefix("辑"))
            index += 2
            changed = True
            continue
        if line == "印" and next_line.startswith("制"):
            merged.append("印制" + next_line.removeprefix("制"))
            index += 2
            changed = True
            continue
        merged.append(lines[index])
        index += 1
    return "\n".join(merged), changed


def _block_text(block: dict) -> str:
    return str(block.get("text") or block.get("文本") or "").strip()


def _block_confidence(block: dict) -> float:
    try:
        return float(block.get("confidence") or block.get("置信度") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _block_bbox_metrics(block: dict) -> tuple[float, float, float] | None:
    bbox = block.get("bbox") or block.get("位置")
    if not isinstance(bbox, list) or not bbox:
        return None
    try:
        xs = [float(point[0]) for point in bbox]
        ys = [float(point[1]) for point in bbox]
    except (TypeError, ValueError, IndexError):
        return None
    return min(ys), max(ys) - min(ys), max(xs) - min(xs)


def _remove_low_confidence_margin_noise(page: PageRecord) -> bool:
    """移除页顶污块被误认成极短文本的高风险噪声。"""
    metrics = [metric for block in page.blocks if (metric := _block_bbox_metrics(block))]
    if page.page_number <= 10 or len(metrics) < 8:
        return False
    page_content_bottom = max(top + height for top, height, _ in metrics)
    normal_heights = sorted(height for _, height, _ in metrics if height > 0)
    median_height = normal_heights[len(normal_heights) // 2]
    noisy_lines: set[str] = set()
    for block in page.blocks:
        line = _block_text(block)
        metric = _block_bbox_metrics(block)
        if not line or metric is None:
            continue
        top, height, width = metric
        compact_length = len(re.sub(r"\s+", "", line))
        is_short_latin_artifact = bool(
            2 <= compact_length <= 8
            and not re.search(r"[\u4e00-\u9fff]", line)
            and re.fullmatch(r"[A-Za-z0-9 .,'-]+", line)
        )
        if (
            _block_confidence(block) < 0.65
            and (1 <= compact_length <= 4 or is_short_latin_artifact)
            and top <= page_content_bottom * 0.12
            and height >= median_height * 1.35
            and width / compact_length
            >= median_height * (1.4 if is_short_latin_artifact else 2.2)
        ):
            noisy_lines.add(line)
    if not noisy_lines:
        return False
    lines = page.text.splitlines()
    kept_lines = [line for line in lines if line.strip() not in noisy_lines]
    if len(kept_lines) == len(lines):
        return False
    page.text = "\n".join(kept_lines).strip()
    return True


def _remove_top_running_header(page: PageRecord) -> bool:
    """移除带拼音索引的页眉，避免它混入双栏正文。"""
    if page.page_number <= 10 or len(page.blocks) < 8:
        return False
    metrics = [metric for block in page.blocks if (metric := _block_bbox_metrics(block))]
    if len(metrics) < 8:
        return False
    page_content_bottom = max(top + height for top, height, _ in metrics)
    normal_heights = sorted(height for _, height, _ in metrics if height > 0)
    median_height = normal_heights[len(normal_heights) // 2]
    header_pattern = re.compile(
        r"(?:[A-Za-z]{2,20}\s*[\u4e00-\u9fff]|[\u4e00-\u9fff]\s*[A-Za-z]{2,20}|[A-Z]\s*[a-z]|[a-z]\s*[A-Z]|[A-Z])"
    )
    header_lines: set[str] = set()
    for block in page.blocks:
        line = _block_text(block)
        metric = _block_bbox_metrics(block)
        if not line or metric is None or not header_pattern.fullmatch(line):
            continue
        top, height, _ = metric
        is_large_single_letter = bool(re.fullmatch(r"[A-Z]", line)) and height >= median_height * 2.5
        is_standard_header = top <= page_content_bottom * 0.12 and height >= median_height * 1.3
        if is_standard_header or (is_large_single_letter and top <= page_content_bottom * 0.2):
            header_lines.add(line)
    if not header_lines:
        return False
    lines = page.text.splitlines()
    kept_lines = [line for line in lines if line.strip() not in header_lines]
    if len(kept_lines) == len(lines):
        return False
    page.text = "\n".join(kept_lines).strip()
    return True


def _remove_printed_page_number(page: PageRecord) -> bool:
    if page.page_number <= 10 or not page.blocks:
        return False
    positioned: list[tuple[dict, float, float, float, float]] = []
    for block in page.blocks:
        bbox = block.get("bbox") or block.get("位置")
        if not isinstance(bbox, list) or not bbox:
            continue
        try:
            xs = [float(point[0]) for point in bbox]
            ys = [float(point[1]) for point in bbox]
        except (TypeError, ValueError, IndexError):
            continue
        positioned.append((block, min(xs), min(ys), max(xs), max(ys)))
    if not positioned:
        return False
    content_left = min(item[1] for item in positioned)
    content_top = min(item[2] for item in positioned)
    content_right = max(item[3] for item in positioned)
    content_bottom = max(item[4] for item in positioned)
    content_width = max(content_right - content_left, 1.0)
    content_height = max(content_bottom - content_top, 1.0)
    candidates: list[tuple[float, str]] = []
    for block, left, top, right, _ in positioned:
        text = _block_text(block)
        center_x = (left + right) / 2
        relative_x = (center_x - content_left) / content_width
        if (
            re.fullmatch(r"\d{1,4}", text)
            and _block_confidence(block) >= 0.8
            and top >= content_bottom - content_height * 0.06
            and (relative_x <= 0.2 or relative_x >= 0.8)
        ):
            candidates.append((top, text))
    if not candidates:
        return False
    _, printed_page = max(candidates)
    lines = page.text.splitlines()
    if sum(1 for line in lines if line.strip() == printed_page) != 1:
        return False
    page.printed_page = printed_page
    page.text = "\n".join(line for line in lines if line.strip() != printed_page).strip()
    return True


def _remove_front_matter_noise_lines(
    text: str,
    *,
    page: PageRecord,
    book_title: str | None,
) -> tuple[str, bool]:
    if not page.blocks or not _looks_like_front_matter(text, page.page_number):
        return text, False
    volume_title = _volume_title_from_book_title(book_title) or ""
    noisy_lines: set[str] = set()
    for block in page.blocks:
        line = _block_text(block)
        confidence = _block_confidence(block)
        if not line or "\n" in line:
            continue
        if volume_title and line in volume_title:
            continue
        has_chinese = bool(re.search(r"[\u4e00-\u9fff]", line))
        is_short_noise = confidence < 0.55 and (
            len(line) <= 2 or re.fullmatch(r"[^\u4e00-\u9fffA-Za-z0-9]{1,3}", line)
        )
        is_call_number_noise = (
            page.page_number <= 3
            and len(line) <= 16
            and not has_chinese
            and re.search(r"[A-Za-z0-9]", line) is not None
            and re.fullmatch(r"[+A-Za-z0-9./:-]+", line)
            and not line.upper().startswith("ISBN")
        )
        is_library_stamp_noise = page.page_number <= 3 and (
            line in {"图书", "藏书"} or (len(line) <= 8 and "图书馆" in line)
        )
        is_accession_number_noise = page.page_number <= 2 and re.fullmatch(r"\d{3,8}", line)
        if is_short_noise or is_call_number_noise or is_library_stamp_noise or is_accession_number_noise:
            noisy_lines.add(line)
    if not noisy_lines:
        return text, False

    kept_lines = [line for line in text.splitlines() if line.strip() not in noisy_lines]
    return "\n".join(kept_lines).strip(), len(kept_lines) != len(text.splitlines())


def _correct_front_matter_text(
    text: str,
    *,
    book_title: str | None,
    page_number: int,
) -> tuple[str, list[str]]:
    if not text or not _looks_like_front_matter(text, page_number):
        return text, []

    corrected = text
    notes: list[str] = []
    if book_title and "中国农业百科全书" in book_title:
        before = corrected
        for wrong in (
            "中国农业科全书",
            "中国农业百全书",
            "中国农业百科书",
            "中国农业百科金书",
        ):
            corrected = corrected.replace(wrong, "中国农业百科全书")
        if corrected != before:
            notes.append("已按书名信息校正前置页常见OCR错字")

    volume_titles = _volume_title_candidates(book_title)
    volume_changed = False
    has_volume_context = page_number <= 4 or "中国农业百科全书" in corrected
    if has_volume_context:
        for volume_title in volume_titles:
            corrected, changed = _correct_volume_title(corrected, volume_title)
            volume_changed = volume_changed or changed
        corrected, deduped_volume = _dedupe_repeated_volume_titles(corrected, volume_titles)
    else:
        deduped_volume = False
    if volume_changed:
        notes.append("已按书名信息补正前置页卷册标题")
    if deduped_volume:
        notes.append("已合并重复卷册标题")

    corrected, merged_labels = _merge_front_matter_split_labels(corrected, page_number)
    if merged_labels:
        notes.append("已合并前置页分裂标题或职务标签")

    before = corrected
    if "1993年" in corrected and any(marker in corrected for marker in ("第1版", "印刷", "ISBN")):
        corrected = corrected.replace("1093年", "1993年")
    corrected = corrected.replace("16月开本", "16 开本")
    corrected = corrected.replace("第1法印到", "第1次印刷")
    corrected = corrected.replace("第1次印到", "第1次印刷")
    if corrected != before:
        notes.append("已校正版权页常见OCR错字")

    return corrected, notes


def _correct_domain_ocr_terms(text: str, book_title: str | None) -> tuple[str, list[str]]:
    if not text:
        return text, []
    corrected = text
    notes: list[str] = []
    if book_title and "蚕" in book_title:
        before = corrected
        replacements = {
            "白量菌": "白僵菌",
            "白疆菌": "白僵菌",
            "自僵蛹": "白僵蛹",
            "分生抱子": "分生孢子",
            "分生抱于": "分生孢子",
            "营养闲丝": "营养菌丝",
            "上族": "上蔟",
            "上簇": "上蔟",
            "族中": "蔟中",
            "簇中": "蔟中",
            "户体": "尸体",
            "煮虽": "煮茧",
            "毫克/下克": "毫克/千克",
            "井岗霉素": "井冈霉素",
            "七牛膝": "土牛膝",
            "露水卓": "露水草",
            "甾围": "甾酮",
            "口本称": "日本称",
            "稍于后": "稍干后",
            "叶丝营茧": "吐丝营茧",
            "词育温度": "饲育温度",
            "词育温": "饲育温",
            "范围人致为": "范围大致为",
            "生长极度时": "生长极盛时",
            "蒸发。，": "蒸发，",
            "1 meteo-": "meteo-",
            "1 meteorological": "meteorological",
            "赤世菌": "赤僵菌",
            "愕蚕": "樗蚕",
            "篦麻蚕": "蓖麻蚕",
            "篦麻": "蓖麻",
            "具行不规则": "具有不规则",
            "1下堆积": "上下堆积",
            "杂种可台": "杂种可育",
            "2~3大死亡": "2~3天死亡",
            "4~6人死": "4~6天死",
            "叶丝结蚕": "吐丝结茧",
            "茁灰褐色": "茧灰褐色",
            "虽重约": "茧重约",
            "母虽壳": "母茧壳",
            "乌柏": "乌桕",
            "蚕座消洁": "蚕座清洁",
            "眠座坏境": "眠座环境",
            "小柏蚕": "小椿蚕",
            "消除2次传染": "消除二次传染",
            "依…股室内温度": "依一般室内温度",
            "具存节约能源": "具有节约能源",
            "初夏一殷室温": "初夏一般室温",
            "比电为1.110": "比重为1.110",
            "浸演时间": "浸渍时间",
            "在2的甲醛原液": "在2%的甲醛原液",
            "1月中句": "1月中旬",
            "催肯": "催青",
            "解除滞台": "解除滞育",
            "抵抗力激剧减弱": "抵抗力急剧减弱",
            "达到齐--地解除滞育": "达到齐一地解除滞育",
            "内此复式冷藏": "因此复式冷藏",
            "由F气候条件": "由于气候条件",
            "体数是2n-26": "体数是2n=26",
            "干燥蚕座经": "干燥。蚕座经",
            "增人": "增大",
            "人部分": "大部分",
            "司一龄": "同一龄",
            "休腔": "体腔",
            "结虽": "结茧",
            "采虽": "采茧",
            "虽层": "茧层",
            "多角休": "多角体",
            "堇麻蚕": "蓖麻蚕",
            "笔麻蚕": "蓖麻蚕",
            "广广东": "广东",
            "10灭": "10天",
            "哦体": "蛾体",
            "起白第": "起自第",
            "粗人的": "粗大的",
            "长简形": "长筒形",
            "怀形细胞": "杯形细胞",
            "呈届平": "呈扁平",
            "母次眠中": "每次眠中",
            "固食膜": "围食膜",
            "责门瓣": "贲门瓣",
            "儿丁质": "几丁质",
            "坏状": "环状",
            "脉博": "脉搏",
            "随发育面减少": "随发育而减少",
            "含定的": "含一定的",
            "在个龄期中": "在各龄期中",
            "言管": "盲管",
            "洲管状": "圆管状",
            "有定的": "有一定的",
            "士壤": "土壤",
            "更人": "更大",
            "蚕品种虽长椭圆": "蚕品种茧长椭圆",
            "收购种虽": "收购种茧",
            "种虽收购": "种茧收购",
            "杯业上": "蚕业上",
            "并人量": "并大量",
            "和-部分": "和一部分",
            "黄叫虫": "黄叶虫",
            "十下越冬": "土下越冬",
            "浙江下次年": "浙江于次年",
            "下句或": "下旬或",
            "月下句": "月下旬",
            "32大": "32天",
            "士表": "土表",
            "1块裂隙": "土块裂隙",
            "在1:表活动": "在土表活动",
            "5月中句": "5月中旬",
            "出口密度": "虫口密度",
            "蚕病之，": "蚕病之一，",
            "包被着层": "包被着一层",
            "丛梗抱科": "丛梗孢科",
            "Sp carin Sp": "Spicaria sp.",
            "发台阶段": "发育阶段",
            "分生抱了": "分生孢子",
            "芽生泡了": "芽生孢子",
            "飘形小梗": "瓶形小梗",
            "抱子链": "孢子链",
            "尖去活力": "失去活力",
            "人形黑褐色": "大形黑褐色",
            "人蚕感病": "大蚕感病",
            "白僵病-样": "白僵病一样",
            "厂体犹如": "尸体犹如",
            "一么灰粉": "一层灰粉",
            "引范麻蚕种": "引蓖麻蚕种",
            "种饲养方式之.": "一种饲养方式之一，",
            "词育": "饲育",
            "杂交青种": "杂交育种",
            "混含育": "混合育",
            "1~34代": "1~3、4代",
            "各取其11种.": "各取其1/4蚁量，",
            "叶丝结茧": "吐丝结茧",
            "胡罗卜素": "胡萝卜素",
            "基闪型": "基因型",
            "白虽种": "白茧种",
            "绿虽色素": "绿茧色素",
            "愈人": "愈大",
            "由子": "由于",
            "比时液状": "此时液状",
            "丝蛋口": "丝蛋白",
            "叶丝孔": "吐丝孔",
            "虽丝未": "茧丝未",
            "即虽丝": "即茧丝",
            "成-根茧丝": "成一根茧丝",
            "庄口虽量": "庄口茧量",
            "为缴丝工艺": "为缫丝工艺",
            "-个庄口": "一个庄口",
            "茁粒": "茧粒",
            "干虽": "干茧",
            "样虽": "样茧",
            "--般": "一般",
            "试缴": "试缫",
            "检验虽质": "检验茧质",
            "次虽": "次茧",
            "供试虽": "供试茧",
            "虽幅": "茧幅",
            "虽腔": "茧腔",
            "蚕虽出丝率": "蚕茧出丝率",
            "左有": "左右",
            "儿次": "几次",
            "白分率": "百分率",
            "萤层": "茧层",
            "原料茧并生设计": "原料茧并庄设计",
            "尤水干量": "无水干量",
            "煮茧上艺": "煮茧工艺",
            "词时进煮": "同时进煮",
            "每添绪次": "每添绪一次",
            "一根虽丝": "一根茧丝",
            "原料虽量": "原料茧量",
            "煮茧T艺": "煮茧工艺",
            "公定阿潮率": "公定回潮率",
            "虽厚薄": "茧厚薄",
            "-粒缫检验": "一粒缫检验",
            "立缴工艺": "立缫工艺",
            "按卜茧": "按下茧",
            "下虽凡": "下茧凡",
            "其它黄分等": "其它茧分等",
            "穿虽": "穿茧",
            "零星虽量": "零星茧量",
            "…律采用": "一律采用",
            "人样茧": "入样茧",
            "光虽量": "光茧量",
            "为了保证虽质": "为了保证茧质",
            "产虽县": "产茧县",
            "抽6下克": "抽6千克",
            "段缔所": "取缔所",
            "1916年3月江苏省蚕丝试验场": "1946年3月江苏省蚕丝试验场",
            "作、H蚕丝试验场": "作，由蚕丝试验场",
            "女千蚕业学校": "女子蚕业学校",
            "还址吴县": "迁址吴县",
            "学校迁问吴县": "学校迁回吴县",
            "北齐祭的蚕神": "北齐祭祀的蚕神",
            "螺祖": "嫘祖",
            "鳞翅日": "鳞翅目",
            "微包子虫": "微孢子虫",
            "幼出4龄": "幼虫4龄",
            "柞树食叶害虫之…": "柞树食叶害虫之一",
            "尖柞天补蛾": "尖柞舟蛾",
            "雌蛾较人": "雌蛾较大",
            "暗揭色": "暗褐色",
            "休侧": "体侧",
            "下面遍平，上而凸起": "下面扁平，上面凸起",
            "金太宗天公三年": "金太宗天会三年",
            "长期末能": "长期未能",
            "背口是柞蚕茧": "营口是柞蚕茧",
            "数白万": "数百万",
            "柞蚕业人兴": "柞蚕业大兴",
            "平均年产虽量": "平均年产茧量",
            "-把即为": "一把即为",
            "566于克": "566千克",
            "宽剑": "宽甸",
            "电点市": "重点市",
            "辽省风城县": "辽宁省凤城县",
            "作查学科": "柞蚕学科",
            "基木理论": "基本理论",
            "占职1总数": "占职工总数",
            "祚蚕": "柞蚕",
            "先态、生理": "生态、生理",
            "裁桑养蚕": "栽桑养蚕",
            "柞杂-号": "柞杂一号",
            "纽织全国": "组织全国",
            "月本考察": "日本考察",
            "榨蚕业技术": "柞蚕业技术",
            "葡麻蚕微粒子病": "蓖麻蚕微粒子病",
            "浙汇及": "浙江及",
            "杭州人十": "杭州人士",
            "而后直在": "而后一直在",
            "昆虫之-": "昆虫之一",
            "尾虫病毒": "昆虫病毒",
            "凋节卵细胞": "调节卵细胞",
            "民虫激素": "昆虫激素",
            "增产蚕药": "增产蚕茧",
            "蚕虽产量": "蚕茧产量",
            "该项口": "该项目",
            "中国科学人会": "中国科学大会",
            "鹿麻蚕": "蓖麻蚕",
            "宙氏蛾霉": "雷氏蛾霉",
            "murea rileyi Farlow.": "Nomuraea rileyi Farlow.",
            "飘形的分生他子小梗": "瓶形的分生孢子小梗",
            "绿偶菌": "绿僵菌",
            "病虫！体": "病虫尸体",
            "芽管货穿": "芽管贯穿",
            "血色上常": "血色正常",
            "休壁": "体壁",
            "迟绥": "迟缓",
            "10大发病": "10天发病",
            "阅形": "圆形",
            "线债病": "绿僵病",
            "densovirus discase": "densovirus disease",
            "毒病的-种": "病毒病的一种",
            "196~970年": "1969~1970年",
            "称之谓小": "称之为小",
            "densovirns": "densovirus",
            "100S左石": "100S左右",
            "圆简形": "圆筒形",
            "儿乎": "几乎",
            "ij杯形": "而杯形",
            "尚尤它例": "尚无它例",
            "症状比较单，以": "症状比较单一，以",
            "多中症蚕": "多数症蚕",
            "体璧": "体壁",
            "允满": "充满",
            "面清学": "血清学",
            "Spng Eo": "",
            "杂交益种": "杂交蚕种",
            "以定数量": "以一定数量",
            "放长形铅框": "放置长形铅框",
            "密布一，": "密布一层，",
            "22.000粒": "22,000粒",
            "自留利时期": "自留种时期",
            "催旨收蚁": "催青收蚁",
            "次口孵化": "次日孵化",
            "第一人孵化": "第一批孵化",
            "生长发台": "生长发育",
            "年生作枝": "年生柞枝",
            "-殷采用": "一般采用",
            "发芽仪入": "发芽侵入",
            "发台的可能温度": "发育的可能温度",
            "温度足15": "温度是15",
            "蚁蚕及！龄期": "蚁蚕及1龄期",
            "第2大即可": "第2天即可",
            "鸯户桑苗": "蚕户桑苗",
            "蚕虽丰收虽质优良": "蚕茧丰收茧质优良",
            "各1500)亩": "各1500亩",
            "东南业、南亚": "东南亚、南亚",
            "全龄经过月数": "全龄经过日数",
            "惟广": "推广",
            "过人卵": "过大卵",
            "首先足根据": "首先是根据",
            "洗清盐味": "洗净盐味",
            "阴天叮用": "阴天可用",
            "进步除去": "进一步除去",
            "送行盐水": "进行盐水",
            "Moracee": "Moraceae",
            "棻荑花序": "葇荑花序",
            "1919年中华人民共和国建立后": "1949年中华人民共和国建立后",
            "中国1大蚕区": "中国三大蚕区",
            "疗桑树生长": "有桑树生长",
            "1东荆桑": "广东荆桑",
            "叶人花少": "叶大花少",
            "伐条(也称夏伐)次": "伐条(也称夏伐)一次",
            "中十桑": "中干桑",
            "Schneiel": "Schneider",
            "头雌虫产卵数": "每头雌虫产卵数",
            "卵期夏季4~7人": "卵期夏季4~7天",
            "无翅雌成虫雌虫交尾": "无翅雌成虫。雌虫交尾",
            "毕即死上介壳下": "毕即死于介壳下",
            "头蚕自孵化": "一头蚕自孵化",
            "熟蛋吐丝结茧": "熟蚕吐丝结茧",
            "它体内过剩": "体内过剩",
            "sexlinked inheritance": "sex-linked inheritance",
            "止常蚕": "正常蚕",
            "分离汕蚕雌": "分离油蚕雌",
            "不良坏境": "不良环境",
            "解舒丝长长和": "解舒丝长和",
            "sensatian of mulberry silkworm": "sensation of mulberry silkworm",
            "外周坤经": "外周神经",
            "近本壁": "近体壁",
            "幼主神经系统": "幼虫神经系统",
            "额炜经节": "额神经节",
            "心则体": "心侧体",
            "第1.2环节": "第1、2环节",
            "构部第 1神经节": "胸部第1神经节",
            "行翅神经": "后翅神经",
            "相串连": "相串联",
            "味觉器宫": "味觉器官",
            "止趋光性": "正趋光性",
            "siikworm toxicosis": "silkworm toxicosis",
            "工！排放": "工业排放",
            "重要诱囚，止在": "重要诱因，正在",
            "排入人气": "排入大气",
            "泥坏含氟": "泥坯含氟",
            "350~450pp": "350~450ppm",
            "拌随中毒": "伴随中毒",
            "污染桑经蚕吃下": "污染桑叶经蚕吃下",
            "污染乘后": "污染桑叶后",
            "垂桑研究所": "蚕桑研究所",
            "达到城高滴定浓度": "达到最高滴定浓度",
            "是种角族激素": "是一种甾族激素",
            "胆伯醇": "胆甾醇",
            "咽侧休系白色球形小体": "咽侧体是白色球形小体",
            "菲近消化管": "靠近消化管",
            "维持共幼虫的形态": "维持其幼虫的形态",
            "蛋自质": "蛋白质",
            "达数下种之多": "达数十种之多",
            "数样，脑激素": "数种，脑激素",
            "滞台激素": "滞育激素",
            "咽下神经古": "咽下神经节",
            "咽下种经节": "咽下神经节",
            "蛋卵滞育": "蚕卵滞育",
            "滞育词节细胞": "滞育调节细胞",
            "微粒子病": "微孢子病",
            "对卒倒病的差异不人": "对卒倒病的差异不大",
            "强弱顺序足：": "强弱顺序是：",
            "闪蚕的发育阶段": "随蚕的发育阶段",
            "随发台而渐增": "随发育而渐增",
            "做儿种杂交方式": "做几种杂交方式",
            "木发现": "未发现",
            "基因挖制": "基因控制",
            "基囚控制": "基因控制",
            "尤明显区别": "无明显区别",
            "染色休": "染色体",
            "moltinismn": "moltinism",
            "尽快风于": "尽快风干",
            "脱酯大豆粉": "脱脂大豆粉",
            "低温、千燥和黑暗": "低温、干燥和黑暗",
            "β-谷甾醇的乙酵": "β-谷甾醇的乙醚",
            "务需注意": "务必注意",
            "数量性状遗传 inheritance": "数量性状遗传 (inheritance",
            "较人的环境相关": "较大的环境相关",
            "较大的止表型相关": "较大的正表型相关",
            "全虽重": "全茧重",
            "0).3毫米": "0.3毫米",
            "内腹因丝腺": "内膜因丝腺",
            "成树技状": "成树枝状",
            "白色扁乎而": "白色扁平而",
            "第8日前后行直接分裂": "第8日前后进行直接分裂",
            "sex-limited\ntance of mulberry silkworm": "sex-limited\ninheritance of mulberry silkworm",
            "mulberry silkworm morphlogy": "mulberry silkworm morphology",
            "胸足主要在食桑和叶丝时使用": "胸足主要在食桑和吐丝时使用",
            "雌蚕较雄蚕人": "雌蚕较雄蚕大",
            "腹面各有对乳白色": "腹面各有一对乳白色",
            "前-对称前生殖芽": "前一对称前生殖芽",
            "生殖芽在人蚕期": "生殖芽在大蚕期",
            "赫氏宝": "赫氏腺",
            "斑纹限性、虽色限性": "斑纹限性、茧色限性",
            "mulberrygeometrid": "mulberry geometrid",
            "Phtho\nnandria": "Phthonandria",
            "日中倚枝斜立": "日间倚枝斜立",
            "锈抱锈菌": "锈孢锈菌",
            "锈抱子抗寒力": "锈孢子抗寒力",
            "与枝迹十分接近": "与枝痕十分接近",
            "温度高十30℃": "温度高于30℃",
            "毒毛鳌伤": "毒毛螯伤",
            "重度蟹伤": "重度螯伤",
            "成落小蚕死亡": "成批小蚕死亡",
            "上壤含水量": "土壤含水量",
            "最人持水量": "最大持水量",
            "桑了后": "桑籽后",
            "不见桑了": "不见桑籽",
            "出士，春播": "出土，春播",
            "除卓，防除": "除草，防除",
            "另一株桑树的枝于或根上": "另一株桑树的枝干或根上",
            "移裁苗池": "移栽苗地",
            "接博支": "接穗枝",
            "结孔": "结缚",
            "术质部分离": "木质部分离",
            "细士覆盖": "细土覆盖",
            "芽六正反面": "芽片正反面",
            "硝木切弧形": "砧木切弧形",
            "简易萨接法": "简易芽接法",
            "带-个芽": "带一个芽",
            "春期嫁接需20大以上": "春期嫁接需20天以上",
            "夏秋嫁接约15大左右": "夏秋嫁接约15天左右",
            "打插生根": "扦插生根",
            "坡适温度": "最适温度",
            "粘性上容易": "粘性土容易",
            "硬技扦插": "硬枝扦插",
            "屋边砂七中": "屋边砂土中",
            "25天左石": "25天左右",
            "人上诱导多倍体": "人工诱导多倍体",
            "常用的足化学药剂-秋水仙碱处理法": "常用的是化学药剂秋水仙碱处理法",
            "易溶于洒精": "易溶于酒精",
            "堆溶于乙醚": "微溶于乙醚",
            "忙藏时应避光": "贮藏时应避光",
            "桑种子没种催芽": "桑种子浸种催芽",
            "幼粮短而肥大": "幼苗短而肥大",
            "经过段时间后": "经过一段时间后",
            "秋水仙碱济液": "秋水仙碱溶液",
            "桑树台种方法": "桑树育种方法",
            "选种月标": "选种目标",
            "引变处理材料": "诱变处理材料",
            "10~11于伦琴": "10~11千伦琴",
            "30多乍来": "30多年来",
            "特别足对一年代数多": "特别是对一年发生代数多",
            "敌政畏乳油": "敌敌畏乳油",
            "主于和支于组成": "主干和支干组成",
            "夏伐后的发条能力": "夏伐后的发芽能力",
            "中柱内尤明显的中柱鞘": "中柱内无明显的中柱鞘",
            "使灾害减低": "使灾害降低",
            "犬气晴朗之日": "天气晴朗之日",
            "增施--次速效性肥料": "增施一次速效性肥料",
            "清光绪一年(1885)": "清光绪十一年(1885)",
            "实验T厂": "实验工厂",
            "在人湖、南投": "在大湖、南投",
            "中山1大学": "中山大学",
            "【作站的工作": "工作站的工作",
            "常温坏境": "常温环境",
            "每蚁产卵数": "每蛾产卵数",
            "约经--周": "约经一周",
            "长径280\n~290毫米": "长径280~290微米",
            "短径250~280毫米": "短径250~280微米",
            "厚160~190毫米": "厚160~190微米",
            "幼出在中国东北": "幼虫在中国东北",
            "形大带三角形": "形大呈三角形",
            "约4.000倍": "约4,000倍",
            "容器中饲台": "容器中饲养",
            "月适当添水": "并适当添水",
            "条桑虽耐忙藏": "条桑虽耐贮藏",
            "炉:藏时": "贮藏时",
            "喷酒适量": "喷洒适量",
            "疏剪部分条桑养蛋": "疏剪部分条桑养蚕",
            "40%左石": "40%左右",
            "使蚤儿疏密": "使蚕儿疏密",
            "族具": "蔟具",
            "原料虽品质": "原料茧品质",
            "十地面积": "占地面积",
            "蛋丝业": "蚕丝业",
        }
        for wrong, right in replacements.items():
            corrected = corrected.replace(wrong, right)
        corrected = re.sub(r"一(?=\d+\s*℃)", "-", corrected)
        corrected = re.sub(r"(?<=\d)一(?=\d)", "~", corrected)
        corrected = re.sub(r"约经(\d+\s*[~～—-]\s*\d+)口死去", r"约经\1日死去", corrected)
        corrected = re.sub(r"(?<![一二三四五六七八九十])2化性", "二化性", corrected)
        corrected = re.sub(r"(?<!一)化性比二化性", "一化性比二化性", corrected)
        corrected = re.sub(r"小柏\s*\n\s*蚕", "小椿蚕", corrected)
        corrected = re.sub(r"族(?=\s*\n\s*中环境)", "蔟", corrected)
        corrected = re.sub(r"微包\s*\n\s*子虫", "微孢子虫", corrected)
        corrected = re.sub(r"尖柞天\s*\n\s*补蛾", "尖柞舟蛾", corrected)
        corrected = re.sub(
            r"(辽宁蚕业\([^\n]+\)\n)宁省是",
            r"\1辽宁省是",
            corrected,
        )
        corrected = re.sub(r"纽\s*\n\s*织全国", "组织全国", corrected)
        corrected = re.sub(r"196\s*~\s*\n?\s*970年", "1969~1970年", corrected)
        corrected = re.sub(r"放\s*\n\s*长形铅框", "放置长形铅框", corrected)
        corrected = re.sub(r"催\s*\n\s*旨收蚁", "催青收蚁", corrected)
        corrected = re.sub(r"待次\s*\n\s*口孵化", "待次日孵化", corrected)
        corrected = re.sub(r"杂交第2代\(F2\)群体内由F基因", "杂交第2代(F2)群体内由于基因", corrected)
        corrected = re.sub(r"(?<![A-Za-z0-9])F，(?=\s*雌蚕)", "F1，", corrected)
        corrected = re.sub(r"(?<![A-Za-z0-9])F，无论", "F1，无论", corrected)
        corrected = re.sub(r"(?<![A-Za-z0-9])F，(?=\s*抗性)", "F1，", corrected)
        corrected = re.sub(r"(?<![A-Za-z0-9])F回交(?=大)", "F1回交", corrected)
        corrected = re.sub(r"的F\s*\n\s*负符号相反", "的正负符号相反", corrected)
        corrected = re.sub(r"一代杂种\(F\)(?=。|，|\n)", "一代杂种(F1)", corrected)
        corrected = re.sub(r"(?<![一二三四五六七八九十])-种杂种值", "一种杂种值", corrected)
        corrected = re.sub(r"(?<!工)作蚕", "柞蚕", corrected)
        corrected = re.sub(r"(?<!工)作树", "柞树", corrected)
        corrected = corrected.replace(
            "混合育(mixed batches rearing)\n(丁辉)",
            "(丁辉)\n混合育(mixed batches rearing)",
        )
        corrected = corrected.replace("…般", "一般").replace("一-般", "一般")
        if corrected != before:
            notes.append("已校正蚕业专业词常见OCR错字")
    if book_title and "茶" in book_title:
        before = corrected
        corrected = re.sub(r"(?m)^茶\s*\n\s*业(?=\s*\n)", "茶业", corrected)
        if corrected != before:
            notes.append("已合并茶业条题并校正常见OCR断句")
    corrected, glossary_rules, glossary_error = _apply_configured_term_glossary(corrected, book_title)
    if glossary_rules:
        notes.append(f"已应用术语词表：{'、'.join(glossary_rules)}")
    if glossary_error:
        notes.append(glossary_error)
    return corrected, notes


def _page_quality_warnings(page: PageRecord) -> list[str]:
    if page.source == "blank_page":
        return []
    warnings: list[str] = []
    if page.confidence < LOW_CONFIDENCE_THRESHOLD:
        warnings.append(f"页面平均置信度低于 {LOW_CONFIDENCE_THRESHOLD:.2f}")
    if not page.text.strip():
        warnings.append("本页没有识别到文本")
    elif len(page.text.strip()) < 8:
        warnings.append("本页识别文本过短，请复核是否为封面、空白页或漏识别")
    if page.page_number <= 5 and 0 < len(page.text.strip()) < 80:
        warnings.append("疑似封面、扉页或版权页，短文本已保留并列入审计")
    if _is_dense_index_page(page):
        warnings.append("疑似目录或索引密集页，已按完整性优先处理")
    if _is_diagram_like_page(page):
        warnings.append("疑似图表或流程图页面，已保留文字并建议人工核对图中关系")
    elif _is_illustration_mixed_page(page):
        warnings.append("疑似图文混排标注页，已保留图中编号并建议人工核对图中关系")
    return warnings


def _is_dense_index_page(page: PageRecord) -> bool:
    lines = [line.strip() for line in page.text.splitlines() if line.strip()]
    return _has_dense_index_signals(lines) and not _is_diagram_like_page(page)


def _has_dense_index_signals(lines: list[str]) -> bool:
    if len(lines) < 20:
        return False
    digit_tail_count = sum(1 for line in lines if re.search(r"\d+\s*$", line))
    leader_count = sum(1 for line in lines if "…" in line or re.search(r"\.{3,}", line))
    digit_tail_ratio = digit_tail_count / len(lines)
    leader_ratio = leader_count / len(lines)
    return digit_tail_ratio >= 0.35 and (leader_ratio >= 0.2 or len(lines) >= 50)


def _is_diagram_like_page(page: PageRecord) -> bool:
    lines = [line.strip() for line in page.text.splitlines() if line.strip()]
    if len(lines) < 20:
        return False
    # 索引页常把页码拆成孤立数字；先识别这类版式，避免误作图表清理。
    if _has_dense_index_signals(lines):
        return False
    short_ratio = sum(1 for line in lines if len(line) <= 8) / len(lines)
    long_ratio = sum(1 for line in lines if len(line) >= 25) / len(lines)
    figure_caption_count = len(re.findall(r"图\s*\d+", page.text))
    isolated_noise_count = _isolated_diagram_noise_count(lines)
    has_diagram_marker = any(
        marker in page.text for marker in ("箭头", "技术路线", "示意图", "流程图", "模式图", "灵敏度")
    ) or figure_caption_count >= 2
    has_map_caption = bool(re.search(r"图\s*\d+[^\n]*(示意图|分布图|地图)", page.text))
    whole_page_diagram = short_ratio >= 0.55 and long_ratio <= 0.35 and (
        has_diagram_marker or isolated_noise_count >= 4
    )
    mixed_text_diagram = (
        figure_caption_count >= 2
        and isolated_noise_count >= 4
        and short_ratio >= 0.15
        and long_ratio <= 0.45
    )
    map_scale_label_count = sum(1 for line in lines if re.fullmatch(r"\d{1,3}", line))
    map_with_scale_noise = has_map_caption and map_scale_label_count >= 3
    return whole_page_diagram or mixed_text_diagram or map_with_scale_noise


def _is_illustration_mixed_page(page: PageRecord) -> bool:
    """识别正文旁带插图编号的页面，只加审计提醒，不删除合法编号。"""
    lines = [line.strip() for line in page.text.splitlines() if line.strip()]
    if len(lines) < 20 or _has_dense_index_signals(lines):
        return False
    short_ratio = sum(1 for line in lines if len(line) <= 8) / len(lines)
    long_ratio = sum(1 for line in lines if len(line) >= 25) / len(lines)
    caption_count = sum(
        1
        for line in lines
        if re.search(r"(?:发育图|结构图|组织图|示意图|流程图|模式图|分布图|地图)$", line)
    )
    return caption_count >= 1 and _isolated_diagram_noise_count(lines) >= 4 and short_ratio >= 0.35 and long_ratio <= 0.45


def _isolated_diagram_noise_count(lines: Iterable[str]) -> int:
    return sum(1 for line in lines if _is_isolated_diagram_noise_line(line))


def _is_isolated_diagram_noise_line(line: str) -> bool:
    stripped = line.strip()
    noisy_symbols = set("0123456789") | {"O", "o", "C", "c", "口", "囗", "Ω", "○", "。"}
    if stripped in noisy_symbols:
        return True
    compact = re.sub(r"\s+", "", stripped)
    return bool(
        2 <= len(compact) <= 8
        and compact
        and len(set(compact)) == 1
        and set(compact) <= noisy_symbols
    )


def _remove_diagram_noise_lines(page: PageRecord) -> bool:
    if not _is_diagram_like_page(page):
        return False
    lines = page.text.splitlines()
    has_map_caption = bool(re.search(r"图\s*\d+[^\n]*(示意图|分布图|地图)", page.text))
    kept_lines = [
        line
        for line in lines
        if not _is_isolated_diagram_noise_line(line)
        and not (has_map_caption and bool(re.fullmatch(r"\s*\d{1,3}\s*", line)))
    ]
    if len(kept_lines) == len(lines):
        return False
    page.text = "\n".join(kept_lines).strip()
    return True


def _is_photo_plate_caption_line(line: str) -> bool:
    return bool(re.match(r"^\s*(?:[△▲]\s*)?\d+\s*\S+", line))


def _is_low_confidence_photo_plate_noise(block: dict) -> bool:
    line = _block_text(block)
    compact = re.sub(r"\s+", "", line)
    if not compact or _block_confidence(block) >= 0.65:
        return False
    if re.search(r"[\u4e00-\u9fff]", compact):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9 .,'-]{1,32}", compact))


def _remove_photo_plate_noise_lines(page: PageRecord) -> bool:
    """清理图片拼版中低置信的拉丁字符碎片，不触碰中文图注。"""
    caption_lines = [line for line in page.text.splitlines() if _is_photo_plate_caption_line(line)]
    marked_caption_count = sum(1 for line in caption_lines if re.match(r"^\s*[△▲]", line))
    if len(caption_lines) < 3 or marked_caption_count < 1 or not page.blocks:
        return False
    noisy_lines = {
        _block_text(block)
        for block in page.blocks
        if _is_low_confidence_photo_plate_noise(block)
    }
    if not noisy_lines:
        return False
    kept_lines = [line for line in page.text.splitlines() if line.strip() not in noisy_lines]
    if len(kept_lines) == len(page.text.splitlines()):
        return False
    page.text = "\n".join(kept_lines).strip()
    return True


def _dedupe_warnings(warnings: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for warning in warnings:
        if warning not in seen:
            seen.add(warning)
            result.append(warning)
    return result


def _prepare_ocr_page(page: PageRecord, book_title: str | None = None) -> PageRecord:
    ordered_text = text_from_ocr_blocks_in_reading_order(page.blocks) if page.blocks else ""
    raw_block_text = "\n".join(_block_text(block) for block in page.blocks if _block_text(block))
    reading_order_changed = bool(ordered_text and ordered_text != raw_block_text)
    if ordered_text and ordered_text != page.text:
        page.text = ordered_text
    page.text = _cleanup_ocr_text(page.text)
    page.text, marker_normalized = _normalize_illustration_caption_markers(page.text)
    printed_page_removed = _remove_printed_page_number(page)
    margin_noise_removed = _remove_low_confidence_margin_noise(page)
    running_header_removed = _remove_top_running_header(page)
    page.text, noise_removed = _remove_front_matter_noise_lines(
        page.text,
        page=page,
        book_title=book_title,
    )
    page.text, correction_notes = _correct_front_matter_text(
        page.text,
        book_title=book_title,
        page_number=page.page_number,
    )
    page.text, domain_notes = _correct_domain_ocr_terms(page.text, book_title)
    correction_notes.extend(domain_notes)
    if noise_removed:
        correction_notes.insert(0, "已移除前置页噪声行")
    if margin_noise_removed:
        correction_notes.insert(0, "已移除页边低置信图像噪声")
    if running_header_removed:
        correction_notes.insert(0, "已识别并移除拼音索引页眉")
    if reading_order_changed:
        correction_notes.insert(0, "已按书籍分栏顺序重排识别文本")
    if printed_page_removed:
        correction_notes.insert(0, "已识别书内页码并从正文移除")
    if marker_normalized:
        correction_notes.insert(0, "已规范图版编号符号")
    page.text, merged_labels = _merge_front_matter_split_labels(page.text, page.page_number)
    if merged_labels:
        correction_notes.append("已合并前置页分裂标题或职务标签")
    page.warnings = _dedupe_warnings([*page.warnings, *correction_notes])
    diagram_noise_removed = _remove_diagram_noise_lines(page)
    if diagram_noise_removed:
        page.warnings = _dedupe_warnings(
            [
                *page.warnings,
                "已移除图表页孤立噪声符号",
                "疑似图表或流程图页面，已保留文字并建议人工核对图中关系",
            ]
        )
    if _remove_photo_plate_noise_lines(page):
        page.warnings = _dedupe_warnings([*page.warnings, "已移除图版页低置信乱码"])
    _refresh_page_quality_warnings(page, preserve_diagram_warning=diagram_noise_removed)
    return page


def _prepare_cached_ocr_page(page: PageRecord, book_title: str | None = None) -> bool:
    before = page.model_dump(mode="json")
    _prepare_ocr_page(page, book_title=book_title)
    return page.model_dump(mode="json") != before


def _refresh_page_quality_warnings(page: PageRecord, *, preserve_diagram_warning: bool = False) -> bool:
    dynamic_prefixes = (
        "页面平均置信度低于 ",
        "本页没有识别到文本",
        "本页识别文本过短，请复核是否为封面、空白页或漏识别",
        "疑似封面、扉页或版权页，短文本已保留并列入审计",
        "疑似目录或索引密集页，已按完整性优先处理",
        "疑似图表或流程图页面，已保留文字并建议人工核对图中关系",
        "疑似图文混排标注页，已保留图中编号并建议人工核对图中关系",
    )
    quality_warnings = _page_quality_warnings(page)
    diagram_warning = "疑似图表或流程图页面，已保留文字并建议人工核对图中关系"
    if preserve_diagram_warning and diagram_warning not in quality_warnings:
        quality_warnings.append(diagram_warning)
    remaining_quality_warnings = list(quality_warnings)
    refreshed: list[str] = []
    for warning in page.warnings:
        if any(warning.startswith(prefix) for prefix in dynamic_prefixes):
            if warning not in remaining_quality_warnings:
                continue
            remaining_quality_warnings.remove(warning)
        refreshed.append(warning)
    refreshed = _dedupe_warnings([*refreshed, *remaining_quality_warnings])
    changed = refreshed != page.warnings
    page.warnings = refreshed
    return changed


def _needs_low_confidence_review(page: PageRecord) -> bool:
    return page.source != "blank_page" and (
        page.confidence < LOW_CONFIDENCE_THRESHOLD or not page.text.strip()
    )


def _already_upgraded(page: PageRecord) -> bool:
    return any("高分辨率重跑" in warning for warning in page.warnings)


def _needs_rotation_review(page: PageRecord) -> bool:
    if page.confidence >= 0.72:
        return False
    return any(
        marker in warning
        for warning in page.warnings
        for marker in ("高分辨率重跑未改善", "高分辨率重跑失败", "高分辨率重跑文字明显变少")
    )


def _quality_score(page: PageRecord) -> tuple[float, int]:
    return (page.confidence, len(page.text.strip()))


def _has_obvious_text_loss(original: PageRecord, upgraded: PageRecord) -> bool:
    original_length = len(original.text.strip())
    upgraded_length = len(upgraded.text.strip())
    return original_length >= 120 and upgraded_length < original_length * LOW_CONFIDENCE_MIN_TEXT_RATIO


def _should_use_upgraded_page(original: PageRecord, upgraded: PageRecord) -> bool:
    if not upgraded.text.strip():
        return False
    if not original.text.strip():
        return True
    if _has_obvious_text_loss(original, upgraded):
        return False
    return _quality_score(upgraded) > _quality_score(original)


def _low_confidence_report(page: PageRecord) -> dict:
    return {
        "页码": page.page_number,
        "置信度": page.confidence,
        "字数": len(page.text),
        "警告": page.warnings,
    }


def _low_confidence_reports(pages: Iterable[PageRecord]) -> list[dict]:
    return [_low_confidence_report(page) for page in pages if _needs_low_confidence_review(page)]


def _retry_rotated_page(
    pdf_path: Path,
    page: PageRecord,
    *,
    adapter: object,
    engine_name: str,
    dpi: int,
    book_title: str | None,
    password: str | None = None,
) -> PageRecord:
    """只对极低置信页尝试横竖方向，兼顾普通页面速度和表格完整性。"""
    if page.text.strip() and page.confidence >= 0.72:
        return page

    best_page = page
    best_rotation: int | None = None
    for rotation in (90, 270):
        try:
            ocr_kwargs = {
                "adapter": adapter,
                "engine_name": engine_name,
                "dpi": dpi,
                "rotation": rotation,
            }
            if password is not None:
                ocr_kwargs["password"] = password
            _, rotated = ocr_pdf_page(pdf_path, page.page_number, **ocr_kwargs)
            rotated = _prepare_ocr_page(rotated, book_title=book_title)
        except Exception:
            continue
        if _should_use_upgraded_page(best_page, rotated):
            best_page = rotated
            best_rotation = rotation

    if best_rotation is not None:
        best_page.warnings = _dedupe_warnings(
            [
                *best_page.warnings,
                f"已用 {best_rotation} 度旋转页面和 {dpi} 点/英寸高分辨率重跑提升质量",
            ]
        )
    return best_page


def _retry_low_confidence_page(
    pdf_path: Path,
    page: PageRecord,
    *,
    adapter: object,
    engine_name: str,
    dpi: int,
    book_title: str | None = None,
    get_medium_adapter: Callable[[], tuple[str, object]] | None = None,
    password: str | None = None,
) -> PageRecord:
    if _is_dense_index_page(page) and dpi < LOW_CONFIDENCE_RETRY_DPI:
        page.warnings = _dedupe_warnings(
            [*page.warnings, "密集页均衡模式已跳过高分辨率重跑，保留较完整原结果"]
        )
        return page

    retry_dpi = max(dpi, LOW_CONFIDENCE_RETRY_DPI)
    result_page = page
    try:
        ocr_kwargs = {"adapter": adapter, "engine_name": engine_name, "dpi": retry_dpi}
        if password is not None:
            ocr_kwargs["password"] = password
        _, upgraded = ocr_pdf_page(pdf_path, page.page_number, **ocr_kwargs)
        upgraded = _prepare_ocr_page(upgraded, book_title=book_title)
    except Exception as exc:
        page.warnings = _dedupe_warnings(
            [*page.warnings, f"高分辨率重跑失败：{type(exc).__name__}"]
        )
        return page

    if _should_use_upgraded_page(page, upgraded):
        upgraded.warnings = _dedupe_warnings(
            [*upgraded.warnings, f"已用 {retry_dpi} 点/英寸高分辨率重跑提升质量"]
        )
        result_page = upgraded

    elif _has_obvious_text_loss(page, upgraded):
        page.warnings = _dedupe_warnings(
            [*page.warnings, "高分辨率重跑文字明显变少，保留较完整原结果"]
        )

    else:
        page.warnings = _dedupe_warnings([*page.warnings, "高分辨率重跑未改善，保留原结果"])

    result_page = _retry_rotated_page(
        pdf_path,
        result_page,
        adapter=adapter,
        engine_name=engine_name,
        dpi=retry_dpi,
        book_title=book_title,
        password=password,
    )

    if (
        get_medium_adapter is None
        or not _is_dense_index_page(page)
        or not _needs_low_confidence_review(result_page)
    ):
        return result_page

    try:
        medium_engine_name, medium_adapter = get_medium_adapter()
        medium_kwargs = {"adapter": medium_adapter, "engine_name": medium_engine_name, "dpi": dpi}
        if password is not None:
            medium_kwargs["password"] = password
        _, medium_page = ocr_pdf_page(pdf_path, page.page_number, **medium_kwargs)
        medium_page = _prepare_ocr_page(medium_page, book_title=book_title)
    except Exception as exc:
        result_page.warnings = _dedupe_warnings(
            [*result_page.warnings, f"中型中文模型复核失败：{type(exc).__name__}"]
        )
        return result_page

    if _should_use_upgraded_page(result_page, medium_page):
        medium_page.warnings = _dedupe_warnings(
            [*medium_page.warnings, "已用中型中文模型复核提升质量"]
        )
        return medium_page

    if _has_obvious_text_loss(result_page, medium_page):
        result_page.warnings = _dedupe_warnings(
            [*result_page.warnings, "中型中文模型复核文字明显变少，保留较完整原结果"]
        )
    else:
        result_page.warnings = _dedupe_warnings(
            [*result_page.warnings, "中型中文模型复核未改善，保留原结果"]
        )
    return result_page


def _cache_path(cache_dir: Path, page_number: int) -> Path:
    return cache_dir / f"第{page_number:05d}页.json"


def _load_cached_page(cache_dir: Path, page_number: int) -> PageRecord | None:
    path = _cache_path(cache_dir, page_number)
    if not path.exists():
        return None
    return PageRecord.model_validate(_read_json_with_retry(path))


def _write_cached_page(cache_dir: Path, page: PageRecord) -> None:
    path = _cache_path(cache_dir, page.page_number)
    _write_json(path, page.model_dump(mode="json"))


def _write_status(
    path: Path,
    *,
    source_pdf: Path,
    status: str,
    target_pages: int,
    processed_pages: int,
    text_pages: int,
    blank_pages: int,
    failed_pages: int,
    low_confidence_pages: int,
    engine: str,
    total_pages: int | None = None,
    is_sample: bool = False,
    started_at: datetime | None = None,
    mode: str | None = None,
    ocr_device: str | None = None,
    gpu_confirmed: bool = False,
) -> None:
    now = datetime.now()
    payload = {
        "状态": status,
        "来源PDF": str(source_pdf),
        "目标页数": target_pages,
        "已处理页数": processed_pages,
        "有文本页数": text_pages,
        "空白页数": blank_pages,
        "失败页数": failed_pages,
        "低置信页数": low_confidence_pages,
        "引擎": zh_data(engine),
        "OCR设备": zh_data(ocr_device) if ocr_device else None,
        "图形处理器已确认": gpu_confirmed,
        "更新时间": now.isoformat(timespec="seconds"),
    }
    if started_at is not None:
        elapsed_seconds = max(0, round((now - started_at).total_seconds()))
        average_page_seconds = round(elapsed_seconds / processed_pages, 2) if processed_pages else None
        remaining_pages = max(target_pages - processed_pages, 0)
        estimated_remaining_seconds = (
            round(remaining_pages * average_page_seconds) if average_page_seconds else None
        )
        payload.update(
            {
                "开始时间": started_at.isoformat(timespec="seconds"),
                "已耗时秒": elapsed_seconds,
                "平均每页秒": average_page_seconds,
                "预计剩余秒": estimated_remaining_seconds,
            }
        )
    if total_pages is not None:
        payload["PDF总页数"] = total_pages
        payload["是否抽样"] = is_sample
    if mode:
        payload["模式"] = zh_data(mode)
    # 业务层把页级指标写入状态文件，CLI、批量管理器和MCP查询共享同一来源。
    metrics = _processing_metrics(payload)
    payload.update(
        {
            "书籍名": metrics["书籍名"],
            "总处理页数": metrics["总处理页数"],
            "处理进度": metrics["处理进度"],
            "处理进度文本": metrics["处理进度文本"],
            "运行时间": metrics["运行时间"],
            "剩余时间": metrics["剩余时间"],
            "处理速度": metrics["处理速度"],
            "处理速度文本": metrics["处理速度文本"],
        }
    )
    _write_json(path, payload)


_ENTRY_HEADING_RE = re.compile(
    r"^\s*([\u4e00-\u9fff][\u4e00-\u9fff·-]{1,23})\s*[（(][A-Za-z][A-Za-z .,'-]{1,80}[）)]"
)


def _split_entry_sections(text: str) -> list[tuple[str | None, str]]:
    """按可明确识别的标题分段，便于逐段校验和阅读。"""
    sections: list[tuple[str | None, list[str]]] = []
    current_title: str | None = None
    current_lines: list[str] = []
    for line in text.splitlines():
        title_match = _ENTRY_HEADING_RE.match(line)
        if title_match:
            if current_lines:
                sections.append((current_title, current_lines))
            current_title = title_match.group(1)
            current_lines = [line]
            continue
        current_lines.append(line)
    if current_lines:
        sections.append((current_title, current_lines))
    return [(title, "\n".join(lines).strip()) for title, lines in sections if "\n".join(lines).strip()]


def _chunk_text(text: str, max_chars: int) -> list[str]:
    raw_paragraphs = [
        part.strip()
        for part in re.split(r"\n{2,}|(?<=[。！？!?])\s+", text)
        if part.strip()
    ]
    paragraphs: list[str] = []
    for paragraph in raw_paragraphs or [text.strip()]:
        if len(paragraph) <= max_chars:
            paragraphs.append(paragraph)
            continue
        buffer = ""
        for line in [item.strip() for item in paragraph.splitlines() if item.strip()]:
            if len(line) > max_chars:
                if buffer:
                    paragraphs.append(buffer)
                    buffer = ""
                paragraphs.extend(line[index : index + max_chars] for index in range(0, len(line), max_chars))
                continue
            if len(buffer) + len(line) + 1 > max_chars:
                if buffer:
                    paragraphs.append(buffer)
                buffer = line
            else:
                buffer = f"{buffer}\n{line}".strip() if buffer else line
        if buffer:
            paragraphs.append(buffer)
    buffer = ""
    chunks: list[str] = []

    def flush() -> None:
        nonlocal buffer
        chunk = buffer.strip()
        if chunk:
            chunks.append(chunk)
        buffer = ""

    for paragraph in paragraphs:
        if not paragraph:
            continue
        if len(buffer) + len(paragraph) + 2 > max_chars:
            flush()
        buffer = f"{buffer}\n\n{paragraph}".strip() if buffer else paragraph
    flush()
    return chunks


def _chunk_page_text(
    page: PageRecord,
    pdf_path: Path,
    book_title: str | None,
    max_chars: int = 900,
) -> list[ChunkRecord]:
    chunks: list[ChunkRecord] = []
    chunk_index = 1
    for chapter, section_text in _split_entry_sections(page.text):
        for text in _chunk_text(section_text, max_chars):
            chunks.append(
                ChunkRecord(
                    chunk_id=f"{pdf_path.stem}-p{page.page_number:04d}-{chunk_index:03d}",
                    book_title=book_title,
                    chapter=chapter,
                    page_start=page.page_number,
                    page_end=page.page_number,
                    text=text,
                    confidence=page.confidence,
                    source_page=page.page_number,
                    source_path=str(pdf_path),
                )
            )
            chunk_index += 1
    return chunks


def _direct_pages(pdf_path: Path, password: str | None = None) -> list[PageRecord]:
    records: list[PageRecord] = []
    direct_pages = (
        extract_direct_text_pages(pdf_path)
        if password is None
        else extract_direct_text_pages(pdf_path, password=password)
    )
    for page_number, text in direct_pages:
        cleaned = _cleanup_ocr_text(text)
        records.append(
            PageRecord(
                page_number=page_number,
                text=cleaned,
                confidence=1.0 if cleaned else 0.0,
                source="pdf_text_layer",
                warnings=[] if cleaned else ["本页没有可抽取文本"],
            )
        )
    return records


def _direct_page_map(
    pdf_path: Path,
    page_numbers: set[int],
    password: str | None = None,
) -> dict[int, PageRecord]:
    """为混合PDF保留已确认可用的原生文本页，避免无谓的二次OCR。"""
    if not page_numbers:
        return {}
    return {
        page.page_number: page
        for page in _direct_pages(pdf_path, password=password)
        if page.page_number in page_numbers and page.text.strip()
    }


def _extract_ocr_pages_resumable(
    pdf_path: Path,
    cache_dir: Path,
    status_path: Path,
    target_pages: int,
    resume: bool,
    dpi: int,
    book_title: str | None = None,
    total_pages: int | None = None,
    is_sample: bool = False,
    started_at: datetime | None = None,
    failed_path: Path | None = None,
    low_path: Path | None = None,
    mode: str | None = None,
    direct_pages: dict[int, PageRecord] | None = None,
    password: str | None = None,
    progress_callback: "Callable[[int, int, float, str | None], None] | None" = None,
    stop_flag: object | None = None,
) -> tuple[str, list[PageRecord], list[dict], list[dict]]:
    if resume:
        cached_pages = [_load_cached_page(cache_dir, page_number) for page_number in range(1, target_pages + 1)]
        if all(page is not None for page in cached_pages):
            pages = [page for page in cached_pages if page is not None]
            for page in pages:
                if _prepare_cached_ocr_page(page, book_title=book_title):
                    _write_cached_page(cache_dir, page)
            needs_review = [
                page
                for page in pages
                if _needs_low_confidence_review(page)
                and (not _already_upgraded(page) or _needs_rotation_review(page))
            ]
            if not needs_review:
                low_reports = _low_confidence_reports(pages)
                if failed_path:
                    _write_jsonl(failed_path, [])
                if low_path:
                    _write_jsonl(low_path, low_reports)
                _write_status(
                    status_path,
                    source_pdf=pdf_path,
                    status="完成",
                    target_pages=target_pages,
                    processed_pages=len(pages),
                    text_pages=sum(1 for item in pages if item.text.strip()),
                    blank_pages=sum(1 for item in pages if item.source == "blank_page"),
                    failed_pages=0,
                    low_confidence_pages=len(low_reports),
                    engine="page_cache",
                    total_pages=total_pages,
                    is_sample=is_sample,
                    started_at=started_at,
                    mode=mode,
                    ocr_device=next((page.ocr_device for page in pages if page.ocr_device), "缓存"),
                    gpu_confirmed=any(page.gpu_confirmed for page in pages),
                )
                heartbeat = stop_flag if isinstance(stop_flag, _WorkerHeartbeat) else None
                if heartbeat is not None:
                    # A resumed attempt that only assembles an already-complete cache
                    # still needs a durable page snapshot for status/iteration reads.
                    for page in pages:
                        heartbeat.page_started(page.page_number)
                        heartbeat.page_completed(page)
                return "page_cache", pages, [], low_reports

    stop_requested = getattr(stop_flag, "is_set", None)
    if callable(stop_requested) and stop_requested():
        raise _CancellationRequested("OCR任务在初始化前已收到停止请求")

    engine_name, adapter = create_ocr_adapter()
    ocr_device = str(getattr(adapter, "device", "cpu"))
    gpu_confirmed = bool(getattr(adapter, "gpu_confirmed", False))
    reported_engine = f"{engine_name}_hybrid" if direct_pages else engine_name
    medium_adapter_cache: tuple[str, object] | None = None
    pages: list[PageRecord] = []
    failed_reports: list[dict] = []
    low_reports: list[dict] = []

    _write_status(
        status_path,
        source_pdf=pdf_path,
        status="进行中",
        target_pages=target_pages,
        processed_pages=0,
        text_pages=0,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine=reported_engine,
        total_pages=total_pages,
        is_sample=is_sample,
        started_at=started_at,
        mode=mode,
        ocr_device=ocr_device,
        gpu_confirmed=gpu_confirmed,
    )
    if failed_path:
        _write_jsonl(failed_path, failed_reports)
    if low_path:
        _write_jsonl(low_path, low_reports)

    def get_medium_adapter() -> tuple[str, object]:
        nonlocal medium_adapter_cache
        if medium_adapter_cache is None:
            medium_adapter_cache = create_ocr_adapter(model_size="medium")
        return medium_adapter_cache

    for page_number in range(1, target_pages + 1):
        if callable(stop_requested) and stop_requested():
            _write_status(
                status_path,
                source_pdf=pdf_path,
                status="已取消",
                target_pages=target_pages,
                processed_pages=len(pages),
                text_pages=sum(1 for item in pages if item.text.strip()),
                blank_pages=sum(1 for item in pages if item.source == "blank_page"),
                failed_pages=len(failed_reports),
                low_confidence_pages=len(low_reports),
                engine=reported_engine,
                total_pages=total_pages,
                is_sample=is_sample,
                started_at=started_at,
                mode=mode,
                ocr_device=ocr_device,
                gpu_confirmed=gpu_confirmed,
            )
            raise _CancellationRequested("OCR任务已在页边界停止")
        # This is intentionally before loading a cache or calling OCR.  It gives the
        # supervisor an unambiguous page-level watchdog marker even when the renderer
        # or native OCR library wedges before it can produce a cache file.
        heartbeat = stop_flag if isinstance(stop_flag, _WorkerHeartbeat) else None
        if heartbeat is not None:
            heartbeat.page_started(page_number)
        cached = _load_cached_page(cache_dir, page_number) if resume else None
        cache_dirty = False
        pending_failure_report: dict | None = None
        if cached is not None:
            page = cached
            cache_dirty = _prepare_cached_ocr_page(page, book_title=book_title)
        elif direct_pages and page_number in direct_pages:
            page = direct_pages[page_number]
            cache_dirty = True
        else:
            try:
                ocr_kwargs = {"adapter": adapter, "engine_name": engine_name, "dpi": dpi}
                if password is not None:
                    ocr_kwargs["password"] = password
                _, page = ocr_pdf_page(pdf_path, page_number, **ocr_kwargs)
                page = _prepare_ocr_page(page, book_title=book_title)
            except Exception as exc:
                page = PageRecord(
                    page_number=page_number,
                    text="",
                    confidence=0.0,
                    source=engine_name,
                    warnings=[f"OCR失败：{type(exc).__name__}"],
                    blocks=[],
                )
                pending_failure_report = {
                    "页码": page_number,
                    "错误类型": type(exc).__name__,
                    "错误信息": str(exc),
                }
                cache_dirty = True

        if _needs_low_confidence_review(page) and (
            not _already_upgraded(page) or _needs_rotation_review(page)
        ):
            medium_getter = get_medium_adapter if dpi >= LOW_CONFIDENCE_RETRY_DPI else None
            page = _retry_low_confidence_page(
                pdf_path,
                page,
                adapter=adapter,
                engine_name=engine_name,
                dpi=dpi,
                book_title=book_title,
                get_medium_adapter=medium_getter,
            )
            cache_dirty = True

        if cache_dirty:
            _write_cached_page(cache_dir, page)

        pages.append(page)
        if _needs_low_confidence_review(page):
            low_reports.append(_low_confidence_report(page))
        if pending_failure_report and not page.text.strip():
            failed_reports.append(pending_failure_report)
        if failed_path:
            _write_jsonl(failed_path, failed_reports)
        if low_path:
            _write_jsonl(low_path, low_reports)

        _write_status(
            status_path,
            source_pdf=pdf_path,
            status="进行中",
            target_pages=target_pages,
            processed_pages=len(pages),
            text_pages=sum(1 for item in pages if item.text.strip()),
            blank_pages=sum(1 for item in pages if item.source == "blank_page"),
            failed_pages=len(failed_reports),
            low_confidence_pages=len(low_reports),
            engine=reported_engine,
            total_pages=total_pages,
            is_sample=is_sample,
            started_at=started_at,
            mode=mode,
            ocr_device=ocr_device,
            gpu_confirmed=gpu_confirmed,
        )
        # The cache, failure/low-confidence records, and business status have all
        # committed atomically at this point.  Only now is it correct to advance
        # the durable supervision progress cursor.
        if heartbeat is not None:
            if pending_failure_report and not page.text.strip():
                heartbeat.page_failed(page_number, str(pending_failure_report["错误类型"]))
            else:
                heartbeat.page_completed(page)
        if progress_callback is not None:
            now = datetime.now()
            elapsed = max(0.0, (now - started_at).total_seconds()) if started_at else 0.0
            avg = elapsed / len(pages) if pages else 0.0
            remaining = max(target_pages - len(pages), 0)
            eta = round(remaining * avg) if avg else None
            pct = round(len(pages) / target_pages * 100, 1) if target_pages else 0.0
            eta_text = f"{eta}秒（约{eta // 60}分）" if eta is not None else "未知"
            msg = (
                f"第 {len(pages)}/{target_pages} 页 | {pct}% | "
                f"平均 {round(avg, 1)}秒/页 | 剩余 {eta_text}"
            )
            try:
                progress_callback(len(pages), target_pages, pct, msg)
            except Exception:
                pass

    return reported_engine, pages, failed_reports, low_reports


def _write_markdown(path: Path, source_pdf: Path, pages: list[PageRecord]) -> None:
    lines = [f"# {source_pdf.stem}", "", f"来源：`{source_pdf}`", ""]
    for page in pages:
        lines.extend([f"## 第 {page.page_number} 页", ""])
        if page.source == "blank_page":
            lines.append("[疑似空白页]")
        else:
            lines.append(page.text.strip() or "[未提取到文本]")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def _write_audit(
    path: Path,
    result: ExtractionResult,
    pages: list[PageRecord],
    low_reports: list[dict],
    failed_reports: list[dict],
) -> None:
    rows = []
    for page in pages:
        warning_text = "；".join(page.warnings)
        rows.append(
            "<tr>"
            f"<td>{page.page_number}</td>"
            f"<td>{html.escape(str(zh_data(page.source)))}</td>"
            f"<td>{page.confidence:.3f}</td>"
            f"<td>{len(page.text)}</td>"
            f"<td>{html.escape(warning_text)}</td>"
            "</tr>"
        )
    body = "\n".join(rows)
    path.write_text(
        f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <title>PDF救援审计报告</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1f2937; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #d1d5db; padding: 0.45rem; text-align: left; }}
    th {{ background: #f3f4f6; }}
  </style>
</head>
<body>
  <h1>PDF救援审计报告</h1>
  <p>状态：{html.escape(str(zh_data(result.status)))} | 引擎：{html.escape(str(zh_data(result.engine)))}</p>
  <p>总页数：{len(pages)} | 低置信页：{len(low_reports)} | 失败页：{len(failed_reports)}</p>
  <table>
    <thead>
      <tr><th>页码</th><th>来源</th><th>置信度</th><th>字数</th><th>警告</th></tr>
    </thead>
    <tbody>
      {body}
    </tbody>
  </table>
</body>
</html>
""",
        encoding="utf-8",
    )


def _empty_toc(path: Path) -> None:
    path.write_text(json.dumps({"条目": []}, ensure_ascii=False, indent=2), encoding="utf-8")


def _compact_page_evidence(record: dict, *, include_blocks: bool) -> dict:
    if include_blocks:
        return record
    compacted = dict(record)
    blocks = compacted.pop("识别块", None)
    if blocks is None:
        blocks = compacted.pop("blocks", None)
    if blocks is not None:
        compacted["识别块数量"] = len(blocks)
        compacted["识别块已省略"] = True
    return compacted


def get_page_evidence(job_dir: str | Path, page_number: int, *, include_blocks: bool = False) -> dict:
    root = ensure_path(job_dir)
    pages_path = root / "数据" / "页面.jsonl"
    if not pages_path.exists():
        pages_path = root / "data" / "pages.jsonl"
    if not pages_path.exists():
        cached = _cached_page_evidence(root, page_number)
        if cached is not None:
            return _compact_page_evidence(cached, include_blocks=include_blocks)
        raise FileNotFoundError(str(pages_path))
    with pages_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            if record.get("页码") == page_number or record.get("page_number") == page_number:
                return _compact_page_evidence(record, include_blocks=include_blocks)
    cached = _cached_page_evidence(root, page_number)
    if cached is not None:
        return _compact_page_evidence(cached, include_blocks=include_blocks)
    raise ValueError(f"未在页面记录中找到第 {page_number} 页：{pages_path}")


def _cached_page_evidence(root: Path, page_number: int) -> dict | None:
    cached = _load_cached_page(root / "缓存" / "页面OCR", page_number)
    if cached is None:
        return None
    status_path = root / "状态.json"
    book_title = None
    if status_path.exists():
        try:
            status = json.loads(status_path.read_text(encoding="utf-8"))
            source_pdf = status.get("来源PDF")
            if source_pdf:
                book_title = Path(source_pdf).stem
        except Exception:
            book_title = None
    _prepare_cached_ocr_page(cached, book_title=book_title)
    return zh_data(cached)


def export_page_image_evidence(
    job_dir: str | Path,
    page_number: int,
    dpi: int = 160,
    password: str | None = None,
) -> dict:
    if page_number < 1:
        raise ValueError("页码必须从 1 开始")
    root = ensure_path(job_dir)
    status_path = root / "状态.json"
    if not status_path.exists():
        raise FileNotFoundError(str(status_path))
    status = json.loads(status_path.read_text(encoding="utf-8"))
    source_pdf = status.get("来源PDF")
    if not source_pdf:
        raise ValueError("状态文件中缺少来源PDF")
    evidence_dir = root / "审计" / "页面证据"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    output_path = evidence_dir / f"第{page_number:05d}页-{dpi}dpi.png"
    render_kwargs = {"dpi": dpi}
    if password is not None:
        render_kwargs["password"] = password
    render_pdf_page(Path(source_pdf), page_number - 1, output_path, **render_kwargs)
    try:
        page_record = get_page_evidence(root, page_number)
    except Exception:
        page_record = _cached_page_evidence(root, page_number)
    return {
        "页码": page_number,
        "来源PDF": str(source_pdf),
        "分辨率": dpi,
        "图像路径": str(output_path),
        "页面记录": page_record,
    }


def _worker_heartbeat_freshness(root: Path, stalled_after_seconds: int) -> dict:
    """Read the worker heartbeat without treating an incomplete write as a task failure."""
    path = root / "后台任务心跳.json"
    if not path.exists():
        return {
            "存在": False,
            "活跃": False,
            "距上次心跳秒数": None,
            "进程ID": None,
            "当前页": None,
            "最后完成页": None,
            "最后进度时间": None,
        }
    try:
        payload = _read_json_with_retry(path)
        age = max(0, round((datetime.now() - datetime.fromtimestamp(path.stat().st_mtime)).total_seconds()))
        active = payload.get("状态") == "运行中" and age < stalled_after_seconds
        return {
            "存在": True,
            "活跃": active,
            "距上次心跳秒数": age,
            "进程ID": payload.get("进程ID"),
            "状态": payload.get("状态"),
            "当前页": payload.get("当前页"),
            "当前页开始时间": payload.get("当前页开始时间"),
            "最后完成页": payload.get("最后完成页"),
            "最后进度时间": payload.get("最后进度时间"),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return {
            "存在": True,
            "活跃": False,
            "距上次心跳秒数": None,
            "进程ID": None,
            "当前页": None,
            "最后完成页": None,
            "最后进度时间": None,
        }


def _status_freshness(
    status: dict,
    stalled_after_seconds: int,
    worker_heartbeat: dict | None = None,
) -> dict:
    updated_text = str(status.get("更新时间") or "")
    seconds_since_update: int | None = None
    if updated_text:
        try:
            updated_at = datetime.fromisoformat(updated_text)
            now = datetime.now(updated_at.tzinfo) if updated_at.tzinfo else datetime.now()
            seconds_since_update = max(0, round((now - updated_at).total_seconds()))
        except ValueError:
            pass
    state = str(status.get("状态") or "未知")
    target_pages = int(status.get("目标页数") or 0)
    processed_pages = int(status.get("已处理页数") or 0)
    worker_active = bool((worker_heartbeat or {}).get("活跃"))
    suspected_stalled = bool(
        state in {"进行中", "启动中"}
        and processed_pages < target_pages
        and seconds_since_update is not None
        and seconds_since_update >= stalled_after_seconds
        and not worker_active
    )
    if suspected_stalled:
        runtime_state = "疑似中断"
    elif state in {"进行中", "启动中"}:
        runtime_state = "活跃（工作进程心跳）" if worker_active else "活跃"
    else:
        runtime_state = state
    return {
        "运行判断": runtime_state,
        "疑似中断": suspected_stalled,
        "距上次更新秒数": seconds_since_update,
        "中断判定阈值秒": stalled_after_seconds,
    }


def read_job_status(job_dir: str | Path, *, stalled_after_seconds: int = 600) -> dict:
    root = ensure_path(job_dir)
    status_path = root / "状态.json"
    quality_path = root / "数据" / "质量.json"
    low_path = root / "数据" / "低置信页.jsonl"
    failed_path = root / "数据" / "失败页.jsonl"
    if not status_path.exists():
        raise FileNotFoundError(str(status_path))

    try:
        status = _read_json_with_retry(status_path)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        # A legacy/non-atomic external writer may be replacing this file at the
        # instant a client polls.  Return a retriable state rather than making the
        # MCP request fail and incorrectly suggesting that OCR itself has failed.
        return {
            "任务目录": str(root),
            "状态文件": str(status_path),
            "状态新鲜度": {"运行判断": "状态刷新中", "疑似中断": False},
            "工作进程心跳": _worker_heartbeat_freshness(root, stalled_after_seconds),
            "任务指标": {},
            "状态": {"状态": "刷新中", "读取错误": type(exc).__name__},
            "低置信页": [],
            "失败页": [],
        }
    low_pages: list[dict] = []
    failed_pages: list[dict] = []
    if low_path.exists():
        try:
            low_pages = [
                json.loads(line)
                for line in low_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            low_pages = []
    if failed_path.exists():
        try:
            failed_pages = [
                json.loads(line)
                for line in failed_path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, ValueError, TypeError, json.JSONDecodeError):
            failed_pages = []

    target_pages = int(status.get("目标页数") or 0)
    processed_pages = int(status.get("已处理页数") or 0)
    progress = round(processed_pages / target_pages, 4) if target_pages else 0.0
    worker_heartbeat = _worker_heartbeat_freshness(root, stalled_after_seconds)
    return {
        "任务目录": str(root),
        "状态文件": str(status_path),
        "质量报告": str(quality_path) if quality_path.exists() else None,
        "低置信页文件": str(low_path) if low_path.exists() else None,
        "失败页文件": str(failed_path) if failed_path.exists() else None,
        "进度": progress,
        "状态新鲜度": _status_freshness(status, stalled_after_seconds, worker_heartbeat),
        "工作进程心跳": worker_heartbeat,
        "任务指标": _processing_metrics(status),
        "状态": status,
        "低置信页": low_pages,
        "失败页": failed_pages,
    }


def _page_record_from_dict(record: dict) -> PageRecord:
    if "page_number" in record:
        return PageRecord.model_validate(record)
    source = str(record.get("来源") or "unknown")
    source = {
        "PDF文本层": "pdf_text_layer",
        "现有OCR文本层": "existing_ocr_text_layer",
        "逐页缓存": "page_cache",
        "疑似空白页": "blank_page",
        "飞桨OCR": "paddleocr",
        "备用OCR": "tesseract",
    }.get(source, source)
    return PageRecord(
        page_number=int(record.get("页码") or 0),
        printed_page=record.get("书内页码"),
        text=str(record.get("文本") or ""),
        confidence=float(record.get("置信度") or 0.0),
        source=source,
        warnings=list(record.get("警告") or []),
        blocks=list(record.get("识别块") or []),
    )


def _load_pages_for_audit(root: Path) -> tuple[str, list[PageRecord]]:
    pages_path = root / "数据" / "页面.jsonl"
    if not pages_path.exists():
        pages_path = root / "data" / "pages.jsonl"
    if pages_path.exists():
        pages = [
            _page_record_from_dict(json.loads(line))
            for line in pages_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        return "页面文件", sorted(pages, key=lambda item: item.page_number)

    cache_dir = root / "缓存" / "页面OCR"
    if not cache_dir.exists():
        return "无页面记录", []
    pages = [
        PageRecord.model_validate(json.loads(path.read_text(encoding="utf-8")))
        for path in sorted(cache_dir.glob("*.json"))
    ]
    return "逐页缓存", sorted(pages, key=lambda item: item.page_number)


def _has_split_label_residue(text: str) -> bool:
    bordered = f"\n{text.strip()}\n"
    return any(marker in bordered for marker in ("\n前\n言\n", "\n凡\n例\n", "\n目\n录\n", "\n委\n员", "\n主\n任"))


def _residual_diagram_noise_count(page: PageRecord) -> int:
    lines = [line.strip() for line in page.text.splitlines() if line.strip()]
    if not lines:
        return 0
    if not _is_diagram_like_page(page):
        return 0
    noise_count = _isolated_diagram_noise_count(lines)
    return noise_count if noise_count >= 2 else 0


def audit_job_quality(
    job_dir: str | Path,
    *,
    max_issues: int = 80,
    use_latest_rules: bool = True,
) -> dict:
    root = ensure_path(job_dir)
    status_path = root / "状态.json"
    if not status_path.exists():
        raise FileNotFoundError(str(status_path))
    status = json.loads(status_path.read_text(encoding="utf-8"))
    source_pdf = status.get("来源PDF")
    book_title = Path(source_pdf).stem if source_pdf else None
    page_source, pages = _load_pages_for_audit(root)

    target_pages = int(status.get("目标页数") or status.get("PDF总页数") or 0)
    loaded_numbers = {page.page_number for page in pages}
    unchecked_pages = [page for page in range(1, target_pages + 1) if page not in loaded_numbers]

    issue_pages: list[dict] = []
    auto_refresh_pages: list[int] = []
    low_confidence_pages = 0
    guarded_dense_low_confidence_pages = 0
    blank_pages = 0
    residual_split_label_pages = 0
    residual_diagram_noise_pages = 0
    warning_refresh_pages: list[int] = []
    reordered_column_pages: list[int] = []
    printed_page_number_pages: list[int] = []
    margin_noise_cleaned_pages: list[int] = []
    running_header_cleaned_pages: list[int] = []
    diagram_noise_cleaned_pages: list[int] = []
    photo_plate_noise_cleaned_pages: list[int] = []
    illustration_review_pages: list[int] = []

    for page in pages:
        working_page = page.model_copy(deep=True)
        text_changed_by_latest_rules = False
        if use_latest_rules:
            before_text = working_page.text
            before_warnings = list(working_page.warnings)
            _prepare_cached_ocr_page(working_page, book_title=book_title)
            text_changed_by_latest_rules = working_page.text != before_text
            warnings_changed_by_latest_rules = working_page.warnings != before_warnings
            if text_changed_by_latest_rules:
                auto_refresh_pages.append(page.page_number)
            elif warnings_changed_by_latest_rules:
                warning_refresh_pages.append(page.page_number)
        warning_text = "\n".join(working_page.warnings)
        if "分栏顺序重排" in warning_text:
            reordered_column_pages.append(page.page_number)
        if "书内页码并从正文移除" in warning_text:
            printed_page_number_pages.append(page.page_number)
        if "页边低置信图像噪声" in warning_text:
            margin_noise_cleaned_pages.append(page.page_number)
        if "拼音索引页眉" in warning_text:
            running_header_cleaned_pages.append(page.page_number)
        if "图表页孤立噪声" in warning_text:
            diagram_noise_cleaned_pages.append(page.page_number)
        if "图版页低置信乱码" in warning_text:
            photo_plate_noise_cleaned_pages.append(page.page_number)
        if "图文混排标注页" in warning_text:
            illustration_review_pages.append(page.page_number)

        problems: list[str] = []
        if page.confidence < LOW_CONFIDENCE_THRESHOLD:
            low_confidence_pages += 1
            guarded_dense_page = any("目录或索引密集页" in warning for warning in page.warnings) and any(
                marker in warning
                for warning in page.warnings
                for marker in ("完整性优先", "保留较完整原结果", "跳过高分辨率重跑")
            )
            if guarded_dense_page:
                guarded_dense_low_confidence_pages += 1
            else:
                problems.append("页面置信度偏低")
        if not page.text.strip():
            blank_pages += 1
            if page.source != "blank_page":
                problems.append("没有识别到文本")
        if _has_split_label_residue(working_page.text):
            residual_split_label_pages += 1
            problems.append("仍有分裂标题残留")
        diagram_noise_count = _residual_diagram_noise_count(working_page)
        if diagram_noise_count:
            residual_diagram_noise_pages += 1
            problems.append(f"仍有疑似图表孤立噪声符号 {diagram_noise_count} 个")
        if _is_illustration_mixed_page(working_page):
            problems.append("图文混排标注页，建议核对图中编号和关系")
        if text_changed_by_latest_rules:
            problems.append("按当前规则重跑可刷新清洗结果")

        if problems and len(issue_pages) < max_issues:
            issue_pages.append(
                {
                    "页码": page.page_number,
                    "置信度": round(page.confidence, 4),
                    "字数": len(page.text),
                    "问题": problems,
                    "现有警告": page.warnings,
                }
            )

    suggestions: list[str] = []
    if unchecked_pages:
        suggestions.append("任务尚未覆盖全部页面，整本完成后再做最终巡检。")
    if auto_refresh_pages:
        suggestions.append("有页面可由当前清洗规则自动刷新，建议整本任务完成后用同一输出目录重跑一次提取。")
    if warning_refresh_pages:
        suggestions.append("有页面仅需刷新质量警告，不影响正文片段。")
    actionable_low_confidence_pages = low_confidence_pages - guarded_dense_low_confidence_pages
    if actionable_low_confidence_pages:
        suggestions.append("低置信页需要优先抽图核对，必要时提高分辨率或切换高质量模式。")
    elif guarded_dense_low_confidence_pages:
        suggestions.append("低置信页集中在密集目录或索引页，已按完整性优先保护，可抽样复核。")
    if residual_split_label_pages or residual_diagram_noise_pages:
        suggestions.append("仍有残留版面问题，建议导出对应页面图像证据并补充规则。")
    if not suggestions:
        suggestions.append("未发现明显质量风险，可进入人工抽查或下游文本处理。")

    return {
        "任务目录": str(root),
        "页面来源": page_source,
        "状态": status.get("状态", "未知"),
        "目标页数": target_pages,
        "已巡检页数": len(pages),
        "尚未巡检页数": len(unchecked_pages),
        "尚未巡检页样例": unchecked_pages[:20],
        "低置信页数": low_confidence_pages,
        "密集索引保护低置信页数": guarded_dense_low_confidence_pages,
        "无文本页数": blank_pages,
        "可自动刷新页数": len(auto_refresh_pages),
        "可自动刷新页样例": auto_refresh_pages[:20],
        "仅警告可刷新页数": len(warning_refresh_pages),
        "仅警告可刷新页样例": warning_refresh_pages[:20],
        "分栏重排页数": len(reordered_column_pages),
        "分栏重排页样例": reordered_column_pages[:20],
        "书内页码移除页数": len(printed_page_number_pages),
        "书内页码移除页样例": printed_page_number_pages[:20],
        "页边噪声清理页数": len(margin_noise_cleaned_pages),
        "页边噪声清理页样例": margin_noise_cleaned_pages[:20],
        "拼音索引页眉清理页数": len(running_header_cleaned_pages),
        "拼音索引页眉清理页样例": running_header_cleaned_pages[:20],
        "图表噪声清理页数": len(diagram_noise_cleaned_pages),
        "图表噪声清理页样例": diagram_noise_cleaned_pages[:20],
        "图版乱码清理页数": len(photo_plate_noise_cleaned_pages),
        "图版乱码清理页样例": photo_plate_noise_cleaned_pages[:20],
        "图文混排标注页数": len(illustration_review_pages),
        "图文混排标注页样例": illustration_review_pages[:20],
        "分裂标题残留页数": residual_split_label_pages,
        "图表噪声残留页数": residual_diagram_noise_pages,
        "问题页数": len(issue_pages),
        "问题页": issue_pages,
        "建议": suggestions,
    }


def extract_book_text(
    path: str | Path,
    output_dir: str | Path | None = None,
    mode: str = "book-balanced",
    max_pages: int | None = None,
    resume: bool = True,
    password: str | None = None,
    progress_callback: Callable[[int, int, float, str | None], None] | None = None,
    stop_flag: object | None = None,
) -> ExtractionResult:
    started_at = datetime.now()
    pdf_path = ensure_path(path)
    configured_root = os.environ.get("PDF_RESCUE_OUTPUT_ROOT")
    root = (
        (ensure_path(output_dir) / f"{_safe_name(pdf_path)}-rescue-result")
        if output_dir
        else ((ensure_path(configured_root) if configured_root else pdf_path.parent / "pdf_rescue_output")
              / f"{_safe_name(pdf_path)}-rescue-result")
    )
    heartbeat = _WorkerHeartbeat(root, external_stop_flag=stop_flag)
    heartbeat.start()
    try:
        inspection = inspect_pdf_text_layer(pdf_path, max_pages=max_pages, password=password)
    except Exception as exc:
        append_history_event(
            root,
            "诊断失败",
            values={"来源PDF": str(pdf_path), "错误类型": type(exc).__name__, "错误信息": str(exc)},
        )
        raise
    # 文本层检查不需要OCR；整本任务必须检查全部页面，避免目录有文本而正文是纯扫描时误走直抽路线。
    text_dir = root / "文本"
    data_dir = root / "数据"
    audit_dir = root / "审计"
    cache_dir = root / "缓存" / "页面OCR"
    for directory in (text_dir, data_dir, audit_dir, cache_dir):
        directory.mkdir(parents=True, exist_ok=True)

    manifest_path = root / "清单.yaml"
    markdown_path = text_dir / "全书.md"
    pages_path = data_dir / "页面.jsonl"
    chunks_path = data_dir / "片段.jsonl"
    quality_path = data_dir / "质量.json"
    toc_path = data_dir / "目录.json"
    status_path = root / "状态.json"
    failed_path = data_dir / "失败页.jsonl"
    low_path = data_dir / "低置信页.jsonl"
    audit_path = audit_dir / "审计.html"

    warnings = list(inspection.warnings)
    next_steps: list[str] = []
    failed_reports: list[dict] = []
    low_reports: list[dict] = []
    ocr_device: str | None = None
    gpu_confirmed = False

    use_direct = inspection.pdf_type == PdfType.BORN_DIGITAL_FULL_TEXT or (
        inspection.pdf_type == PdfType.SEARCHABLE_SCANNED
        and mode in {"book-fast", "book-balanced", "book-balanced-low-memory"}
    )
    hybrid_direct_pages: dict[int, PageRecord] = {}
    if inspection.pdf_type == PdfType.MIXED:
        direct_candidates = {
            report.page_number
            for report in inspection.pages
            if report.likely_has_usable_text and not report.likely_scanned
        }
        hybrid_direct_pages = _direct_page_map(pdf_path, direct_candidates, password=password)
    engine = "pdf_text_layer"
    pages: list[PageRecord] = []
    status = "ok"
    target_pages = min(inspection.page_count, max_pages) if max_pages else inspection.page_count
    is_sample = target_pages < inspection.page_count
    dpi = _dpi_for_mode(mode)
    heartbeat.set_total_pages(target_pages)
    history_start = append_history_event(
        root,
        "开始处理",
        values={
            "来源PDF": str(pdf_path),
            "PDF类型": zh_data(inspection.pdf_type),
            "处理动作": zh_data(inspection.recommended_action),
            "模式": zh_data(mode),
            "目标页数": target_pages,
            "是否抽样": is_sample,
        },
    )
    run_id = str(history_start["运行编号"])

    if inspection.pdf_type == PdfType.PASSWORD_PROTECTED:
        status = "needs_password"
        engine = "none"
        warnings.append("PDF受密码保护，未启动OCR；请提供密码后重新提取。")
        next_steps.append("重新调用提取书籍文本并提供PDF密码。")
    elif inspection.pdf_type == PdfType.CORRUPTED:
        status = "needs_repair"
        engine = "none"
        warnings.append("PDF结构损坏或无法读取，未启动OCR。")
        next_steps.append("先使用PDF修复工具修复文件，再重新检查和提取。")
    elif use_direct:
        pages = _direct_pages(pdf_path, password=password)[:target_pages]
        if inspection.pdf_type == PdfType.SEARCHABLE_SCANNED:
            warnings.append("正在复用现有OCR文本层；请查看质量报告中的扫描页风险。")
        for page in pages:
            heartbeat.page_started(page.page_number)
            page.warnings.extend(_page_quality_warnings(page))
            if page.source != "blank_page" and (
                page.confidence < LOW_CONFIDENCE_THRESHOLD or not page.text.strip()
            ):
                low_reports.append(
                    {
                        "页码": page.page_number,
                        "置信度": page.confidence,
                        "字数": len(page.text),
                        "警告": page.warnings,
                    }
                )
    else:
        if inspection.pdf_type == PdfType.MIXED:
            if hybrid_direct_pages:
                warnings.append(
                    f"混合PDF已保留 {len(hybrid_direct_pages)} 页可用原生文本，其余页面进行OCR。"
                )
            else:
                warnings.append("混合PDF未发现可直接复用的文本页，全部页面进行OCR。")
        engine_name = available_ocr_engine()
        if engine_name:
            try:
                engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
                    pdf_path,
                    cache_dir,
                    status_path,
                    target_pages,
                    resume=resume,
                    dpi=dpi,
                    book_title=pdf_path.stem,
                    total_pages=inspection.page_count,
                    is_sample=is_sample,
                    started_at=started_at,
                    failed_path=failed_path,
                    low_path=low_path,
                    mode=mode,
                    direct_pages=hybrid_direct_pages or None,
                    password=password,
                    progress_callback=progress_callback,
                    stop_flag=heartbeat,
                )
                ocr_devices = {page.ocr_device for page in pages if page.ocr_device}
                ocr_device = "gpu" if "gpu" in ocr_devices else (next(iter(ocr_devices), None))
                gpu_confirmed = any(page.gpu_confirmed for page in pages)
            except _CancellationRequested:
                status = "cancelled"
                engine = "cancelled"
                warnings.append("OCR任务已收到停止请求，已在安全页边界停止；可用恢复任务继续。")
                next_steps.append("如需继续，请使用恢复任务从逐页缓存断点续传。")
            except Exception:
                status = "needs_ocr_engine"
                engine = "none"
                warnings.append("OCR引擎初始化失败；请检查飞桨运行后端和模型文件。")
                next_steps.extend(
                    [
                        "重新安装OCR扩展：uv sync --extra ocr",
                        "首次运行需要下载中文OCR模型，请保持网络可用。",
                    ]
                )
        else:
            status = "needs_ocr_engine"
            engine = "none"
            warnings.append("未安装OCR引擎；本次只写入检查结果。")
            next_steps.extend(
                [
                    "安装OCR扩展：uv sync --extra ocr",
                    "安装飞桨OCR以识别中文扫描书。",
                    "如需修复可搜索PDF文本层，请安装备用OCR与文本层修复工具。",
                ]
            )

    if pages:
        pages = sorted(pages, key=lambda item: item.page_number)
        _write_markdown(markdown_path, pdf_path, pages)
        _write_jsonl(pages_path, [zh_data(page) for page in pages])
        chunks: list[ChunkRecord] = []
        for page in pages:
            chunks.extend(_chunk_page_text(page, pdf_path, pdf_path.stem))
        _write_jsonl(chunks_path, [zh_data(chunk) for chunk in chunks])
        _empty_toc(toc_path)
    else:
        markdown_path = None
        chunks_path = None
        pages_path = None
        _empty_toc(toc_path)

    _write_jsonl(failed_path, failed_reports)
    _write_jsonl(low_path, low_reports)

    quality = {
        "检查结果": zh_data(inspection),
        "模式": zh_data(mode),
        "PDF总页数": inspection.page_count,
        "是否抽样": is_sample,
        "分辨率": dpi,
        "引擎": zh_data(engine),
        "OCR设备": zh_data(ocr_device) if ocr_device else None,
        "图形处理器已确认": gpu_confirmed,
        "目标页数": target_pages,
        "已输出页数": len(pages),
        "有文本页数": sum(1 for item in pages if item.text.strip()),
        "空白页数": sum(1 for item in pages if item.source == "blank_page"),
        "低置信页数": len(low_reports),
        "失败页数": len(failed_reports),
        "警告": zh_data(warnings),
    }
    _write_json(quality_path, quality)

    _write_status(
        status_path,
        source_pdf=pdf_path,
        status="完成" if status == "ok" else ("已取消" if status == "cancelled" else "未完成"),
        target_pages=target_pages,
        processed_pages=len(pages),
        text_pages=sum(1 for item in pages if item.text.strip()),
        blank_pages=sum(1 for item in pages if item.source == "blank_page"),
        failed_pages=len(failed_reports),
        low_confidence_pages=len(low_reports),
        engine=engine,
        total_pages=inspection.page_count,
        is_sample=is_sample,
        started_at=started_at,
        mode=mode,
        ocr_device=ocr_device,
        gpu_confirmed=gpu_confirmed,
    )
    if use_direct:
        # Direct-text pages have no per-page status writes.  Advance their
        # supervision cursor only after the final authoritative status snapshot.
        for page in pages:
            heartbeat.page_completed(page)

    result = ExtractionResult(
        status=status,
        job_dir=str(root),
        pdf_type=inspection.pdf_type,
        engine=engine,
        manifest_path=str(manifest_path),
        markdown_path=str(markdown_path) if markdown_path else None,
        chunks_path=str(chunks_path) if chunks_path else None,
        pages_path=str(pages_path) if pages_path else None,
        quality_path=str(quality_path),
        audit_path=str(audit_path),
        ocr_device=ocr_device,
        gpu_confirmed=gpu_confirmed,
        warnings=warnings,
        next_steps=next_steps,
    )

    manifest = {
        "来源PDF": str(pdf_path),
        "状态": zh_data(result.status),
        "PDF类型": zh_data(result.pdf_type),
        "引擎": zh_data(result.engine),
        "模式": zh_data(mode),
        "输出": zh_data(result),
        "状态文件": str(status_path),
        "失败页": str(failed_path),
        "低置信页": str(low_path),
    }
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), encoding="utf-8")
    _write_audit(audit_path, result, pages, low_reports, failed_reports)
    append_history_event(
        root,
        "处理完成" if status == "ok" else "处理未完成",
        run_id=run_id,
        values={
            "来源PDF": str(pdf_path),
            "PDF类型": zh_data(inspection.pdf_type),
            "状态": zh_data(result.status),
            "模式": zh_data(mode),
            "引擎": zh_data(engine),
            "OCR设备": zh_data(ocr_device) if ocr_device else None,
            "图形处理器已确认": gpu_confirmed,
            "目标页数": target_pages,
            "已处理页数": len(pages),
            "有文本页数": sum(1 for item in pages if item.text.strip()),
            "空白页数": sum(1 for item in pages if item.source == "blank_page"),
            "低置信页数": len(low_reports),
            "失败页数": len(failed_reports),
            "质量报告": str(quality_path),
            "审计报告": str(audit_path),
        },
    )
    heartbeat.finish("已完成" if status == "ok" else ("已取消" if status == "cancelled" else "未完成"))
    return result


def _mode_from_status(value: object) -> str:
    text = str(value or "").strip()
    aliases = {
        "书籍快速模式": "book-fast",
        "书籍均衡模式": "book-balanced",
        "书籍均衡低内存模式": "book-balanced-low-memory",
        "书籍高质量模式": "book-quality",
        "书籍快速低内存模式": "book-fast-low-memory",
        "书籍取证级模式": "book-forensic",
    }
    return aliases.get(text, text if text.startswith("book-") else "book-balanced")


def resume_job(
    job_dir: str | Path,
    *,
    mode: str | None = None,
    stalled_after_seconds: int = 600,
    force: bool = False,
    password: str | None = None,
    progress_callback: Callable[[int, int, float, str | None], None] | None = None,
) -> dict:
    root = ensure_path(job_dir)
    status_payload = read_job_status(root, stalled_after_seconds=stalled_after_seconds)
    status = status_payload["状态"]
    freshness = status_payload["状态新鲜度"]
    state = str(status.get("状态") or "未知")
    if state == "完成":
        return {
            "动作": "无需恢复",
            "原因": "任务已经完成",
            "任务状态": status_payload,
        }
    if state == "进行中" and not freshness["疑似中断"] and not force:
        return {
            "动作": "未恢复",
            "原因": "任务仍在活跃更新；如已确认原进程不存在，可使用强制恢复",
            "任务状态": status_payload,
        }

    source_pdf = status.get("来源PDF")
    if not source_pdf:
        raise ValueError("状态文件中缺少来源PDF，无法恢复任务")
    source_path = ensure_path(source_pdf)
    if not source_path.exists():
        raise FileNotFoundError(str(source_path))
    target_pages = int(status.get("目标页数") or 0)
    total_pages = int(status.get("PDF总页数") or 0)
    max_pages = target_pages if total_pages and 0 < target_pages < total_pages else None
    selected_mode = mode or _mode_from_status(status.get("模式"))
    result = extract_book_text(
        source_path,
        # ``extract_book_text`` derives a ``*-rescue-result`` child from its
        # output root.  Reuse this job's parent so a direct library resume does
        # not create ``job-rescue-result/job-rescue-result`` nesting.
        output_dir=root.parent,
        mode=selected_mode,
        max_pages=max_pages,
        resume=True,
        password=password,
        progress_callback=progress_callback,
    )
    return {
        "动作": "已恢复并完成",
        "恢复模式": zh_data(selected_mode),
        "复用缓存": True,
        "任务结果": zh_data(result),
    }
