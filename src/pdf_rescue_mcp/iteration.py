"""Versioned, advisory-only improvement planning for completed OCR jobs.

The iteration layer deliberately has no dependency on the OCR pipeline, the
supervisor, a network client, or a persistence backend.  It turns immutable
snapshots of task state and quality-audit evidence into an auditable *proposal*.
The caller must present any action requiring approval to a human and invoke the
business/supervision layer separately; this module can never restart a worker,
change a policy, or modify an output directory on its own.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
import hashlib
import json
from typing import Any


ITERATION_PLAN_SCHEMA_VERSION = "1.0"
DEFAULT_STRATEGY_VERSION = "1.0.0"
_MAX_PAGE_SAMPLES = 20


def _as_mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _first_value(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value is not None:
            return value
    return None


def _integer(value: object, default: int = 0) -> int:
    """Return a non-negative integer without letting malformed status crash planning."""
    try:
        return max(0, int(float(value)))
    except (TypeError, ValueError):
        return default


def _number(value: object) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _page_sample(value: object) -> list[int]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes, Mapping)):
        return []
    pages: list[int] = []
    seen: set[int] = set()
    for item in value:
        page = _integer(item, default=0)
        if page and page not in seen:
            pages.append(page)
            seen.add(page)
        if len(pages) >= _MAX_PAGE_SAMPLES:
            break
    return pages


def _normalise_timestamp(value: datetime | str | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat(timespec="seconds")
    text = str(value).strip()
    if not text:
        raise ValueError("generated_at must not be empty")
    return text


def _canonical_digest(value: object) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _resolve_task_view(task_status: Mapping[str, Any]) -> dict[str, Any]:
    """Extract stable, non-sensitive evidence from status API variants."""
    status_body = _as_mapping(task_status.get("状态"))
    metrics = _as_mapping(task_status.get("任务指标"))
    freshness = _as_mapping(task_status.get("状态新鲜度"))
    heartbeat = _as_mapping(task_status.get("工作进程心跳"))
    resource = _as_mapping(metrics.get("资源占用率"))

    state = str(
        _first_value(status_body, "状态", "state")
        or _first_value(task_status, "状态", "state")
        or "未知"
    )
    target_pages = _integer(
        _first_value(metrics, "总处理页数", "target_pages")
        or _first_value(status_body, "目标页数", "PDF总页数", "target_pages", "page_count")
        or _first_value(task_status, "目标页数", "target_pages")
    )
    processed_pages = _integer(
        _first_value(metrics, "已处理页数", "processed_pages")
        or _first_value(status_body, "已处理页数", "processed_pages")
        or _first_value(task_status, "已处理页数", "processed_pages")
    )
    progress = _number(
        _first_value(metrics, "处理进度", "progress_percent")
        or _first_value(task_status, "进度", "progress")
    )
    if progress is None and target_pages:
        progress = round(processed_pages / target_pages * 100, 1)

    cpu_percent = _number(
        _first_value(resource, "CPU占用率", "CPU百分比", "cpu_percent", "cpu_usage_percent")
        or _first_value(task_status, "cpu_percent", "cpu_usage_percent")
    )
    memory_percent = _number(
        _first_value(resource, "内存占用率", "内存百分比", "memory_percent", "memory_usage_percent")
        or _first_value(task_status, "memory_percent", "memory_usage_percent")
    )
    stalled = bool(
        _first_value(freshness, "疑似中断", "suspected_stalled")
        or _first_value(task_status, "疑似中断", "suspected_stalled")
    )
    runtime_state = str(
        _first_value(freshness, "运行判断", "runtime_state")
        or _first_value(task_status, "运行判断", "runtime_state")
        or ""
    )
    return {
        "state": state,
        "target_pages": target_pages,
        "processed_pages": processed_pages,
        "progress_percent": round(progress, 1) if progress is not None else None,
        "suspected_stalled": stalled,
        "runtime_state": runtime_state or None,
        "heartbeat_active": bool(_first_value(heartbeat, "活跃", "active")),
        "cpu_percent": round(cpu_percent, 1) if cpu_percent is not None else None,
        "memory_percent": round(memory_percent, 1) if memory_percent is not None else None,
    }


def _resolve_quality_view(quality_audit: Mapping[str, Any]) -> dict[str, Any]:
    """Reduce quality audit output to the facts used by the policy rules."""
    low_confidence = _integer(_first_value(quality_audit, "低置信页数", "low_confidence_pages"))
    protected_low_confidence = _integer(
        _first_value(quality_audit, "密集索引保护低置信页数", "protected_low_confidence_pages")
    )
    return {
        "audit_state": str(_first_value(quality_audit, "状态", "state") or "未知"),
        "target_pages": _integer(_first_value(quality_audit, "目标页数", "target_pages")),
        "inspected_pages": _integer(_first_value(quality_audit, "已巡检页数", "inspected_pages")),
        "unchecked_pages": _integer(_first_value(quality_audit, "尚未巡检页数", "unchecked_pages")),
        "unchecked_page_sample": _page_sample(
            _first_value(quality_audit, "尚未巡检页样例", "unchecked_page_sample")
        ),
        "low_confidence_pages": low_confidence,
        "protected_low_confidence_pages": min(protected_low_confidence, low_confidence),
        "blank_pages": _integer(_first_value(quality_audit, "无文本页数", "blank_pages")),
        "auto_refresh_pages": _integer(_first_value(quality_audit, "可自动刷新页数", "auto_refresh_pages")),
        "auto_refresh_page_sample": _page_sample(
            _first_value(quality_audit, "可自动刷新页样例", "auto_refresh_page_sample")
        ),
        "warning_refresh_pages": _integer(
            _first_value(quality_audit, "仅警告可刷新页数", "warning_refresh_pages")
        ),
        "residual_split_label_pages": _integer(
            _first_value(quality_audit, "分裂标题残留页数", "residual_split_label_pages")
        ),
        "residual_diagram_noise_pages": _integer(
            _first_value(quality_audit, "图表噪声残留页数", "residual_diagram_noise_pages")
        ),
        "illustration_review_pages": _integer(
            _first_value(quality_audit, "图文混排标注页数", "illustration_review_pages")
        ),
        "issue_pages": _integer(_first_value(quality_audit, "问题页数", "issue_pages")),
        "suggestions": [
            str(item)
            for item in (_first_value(quality_audit, "建议", "suggestions") or [])
            if isinstance(item, (str, int, float))
        ][:_MAX_PAGE_SAMPLES],
    }


def _summarise_events(task_events: Iterable[Mapping[str, Any]] | None) -> dict[str, Any]:
    events = list(task_events or [])
    categories: Counter[str] = Counter()
    ignored = 0
    last_event: dict[str, str] | None = None
    for event in events:
        if not isinstance(event, Mapping):
            ignored += 1
            continue
        event_type = str(
            _first_value(event, "event_type", "event", "事件", "type", "类型") or "unknown"
        )
        categories[event_type] += 1
        timestamp = _first_value(event, "created_at", "timestamp", "时间", "记录时间")
        if timestamp is not None:
            last_event = {"type": event_type, "timestamp": str(timestamp)}
    return {
        "count": sum(categories.values()),
        "by_type": dict(sorted(categories.items())),
        "last_event": last_event,
        "ignored_count": ignored,
    }


def _risk(
    risk_id: str,
    severity: str,
    summary: str,
    evidence_refs: list[str],
) -> dict[str, Any]:
    return {
        "risk_id": risk_id,
        "severity": severity,
        "summary": summary,
        "evidence_refs": evidence_refs,
    }


def _action(
    action_id: str,
    layer: str,
    title: str,
    rationale: str,
    *,
    requires_human_approval: bool,
    evidence_refs: list[str],
    target_pages: list[int] | None = None,
) -> dict[str, Any]:
    """Return a declarative action.  It intentionally contains no command or callback."""
    return {
        "action_id": action_id,
        "layer": layer,
        "title": title,
        "rationale": rationale,
        "target_pages": list(target_pages or []),
        "requires_human_approval": requires_human_approval,
        "execution": "advisory_only",
        "can_auto_apply": False,
        "rollback": "本模块未执行任何变更；未批准时直接忽略。批准后的实际操作须由业务或监管层记录并按其备份/恢复流程回滚。",
        "evidence_refs": evidence_refs,
    }


def build_iteration_plan(
    task_status: Mapping[str, Any] | None = None,
    quality_audit: Mapping[str, Any] | None = None,
    task_events: Iterable[Mapping[str, Any]] | None = None,
    *,
    strategy_version: str = DEFAULT_STRATEGY_VERSION,
    generated_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Build an auditable, non-executable update proposal from job evidence.

    ``task_status`` may be either the business status object or the combined
    ``read_job_status``/MCP status response.  ``quality_audit`` accepts the
    return object of :func:`pdf_rescue_mcp.book_pipeline.audit_job_quality`.
    Both inputs are read only and are never retained or mutated.

    The returned JSON-compatible dictionary is intentionally declarative.  In
    particular, every recommendation has ``execution == 'advisory_only'`` and
    ``can_auto_apply == False``.  A caller may serialize it for audit, but must
    obtain approval and call a different layer to perform a real operation.
    """
    version = str(strategy_version).strip()
    if not version:
        raise ValueError("strategy_version must not be empty")

    task = _resolve_task_view(_as_mapping(task_status))
    quality = _resolve_quality_view(_as_mapping(quality_audit))
    events = _summarise_events(task_events)
    evidence = {"task": task, "quality": quality, "events": events}
    risks: list[dict[str, Any]] = []
    actions: list[dict[str, Any]] = []

    terminal_failure = task["state"] in {"失败", "已失败", "failed", "error"}
    if task["suspected_stalled"] or terminal_failure:
        state_text = "监管层判定任务疑似中断" if task["suspected_stalled"] else "任务状态为失败"
        risks.append(_risk("task_recovery_required", "high", state_text, ["task.runtime_state", "task.state"]))
        actions.append(
            _action(
                "review_and_recover_task",
                "supervision",
                "核对任务后再恢复",
                "先确认源文件、缓存和进程已停止，再由监管层按恢复策略续跑，避免重复 OCR 或覆盖产物。",
                requires_human_approval=True,
                evidence_refs=["task.runtime_state", "task.state"],
            )
        )

    incomplete_pages = max(quality["unchecked_pages"], task["target_pages"] - task["processed_pages"], 0)
    if incomplete_pages:
        risks.append(
            _risk(
                "incomplete_coverage",
                "high" if terminal_failure else "medium",
                f"仍有 {incomplete_pages} 页未完成处理或巡检，当前结果不应作为全书最终版本。",
                ["task.target_pages", "task.processed_pages", "quality.unchecked_pages"],
            )
        )
        actions.append(
            _action(
                "complete_page_coverage",
                "business",
                "完成缺失页处理与巡检",
                "优先在同一任务目录基于现有页级缓存续跑，完成后重新生成质量审计。",
                requires_human_approval=True,
                evidence_refs=["task.target_pages", "task.processed_pages", "quality.unchecked_pages"],
                target_pages=quality["unchecked_page_sample"],
            )
        )

    actionable_low_confidence = max(
        0, quality["low_confidence_pages"] - quality["protected_low_confidence_pages"]
    )
    if actionable_low_confidence:
        severity = "high" if actionable_low_confidence >= 10 else "medium"
        risks.append(
            _risk(
                "low_confidence_text",
                severity,
                f"有 {actionable_low_confidence} 页低置信文本未受密集索引保护，可能影响正文可信度。",
                ["quality.low_confidence_pages", "quality.protected_low_confidence_pages"],
            )
        )
        actions.append(
            _action(
                "sample_low_confidence_pages",
                "iteration",
                "抽样核对低置信页面",
                "先人工抽图核验，再决定是否提高 DPI、切换质量模式或仅重跑指定页面。",
                requires_human_approval=True,
                evidence_refs=["quality.low_confidence_pages", "quality.protected_low_confidence_pages"],
            )
        )

    if quality["blank_pages"]:
        risks.append(
            _risk(
                "blank_page_output",
                "high",
                f"有 {quality['blank_pages']} 页没有输出文本，需要区分原始空白页与 OCR 漏识别。",
                ["quality.blank_pages"],
            )
        )
        actions.append(
            _action(
                "review_blank_pages",
                "business",
                "核验无文本页面",
                "依据原始页图确认是否为空白页；非空白页由人工批准后进行针对性重跑。",
                requires_human_approval=True,
                evidence_refs=["quality.blank_pages"],
            )
        )

    if quality["auto_refresh_pages"]:
        risks.append(
            _risk(
                "new_cleanup_rules_available",
                "medium",
                f"当前规则可刷新 {quality['auto_refresh_pages']} 页，重跑会改变已生成文本。",
                ["quality.auto_refresh_pages"],
            )
        )
        actions.append(
            _action(
                "approve_rule_refresh",
                "iteration",
                "评审规则刷新影响",
                "比较抽样页的新旧输出；确认质量收益和回归风险后，再创建可回滚的新输出版本。",
                requires_human_approval=True,
                evidence_refs=["quality.auto_refresh_pages"],
                target_pages=quality["auto_refresh_page_sample"],
            )
        )

    residual_layout = quality["residual_split_label_pages"] + quality["residual_diagram_noise_pages"]
    if residual_layout or quality["illustration_review_pages"]:
        risks.append(
            _risk(
                "layout_or_illustration_risk",
                "medium",
                "仍存在版面残留或图文混排标注，自动清洗可能损伤真实内容。",
                [
                    "quality.residual_split_label_pages",
                    "quality.residual_diagram_noise_pages",
                    "quality.illustration_review_pages",
                ],
            )
        )
        actions.append(
            _action(
                "manual_layout_review",
                "iteration",
                "人工复核版面与图文混排页面",
                "保留原始页图和页级缓存作为证据；任何新规则先在少量代表页灰度验证。",
                requires_human_approval=True,
                evidence_refs=[
                    "quality.residual_split_label_pages",
                    "quality.residual_diagram_noise_pages",
                    "quality.illustration_review_pages",
                ],
            )
        )

    high_resource = any(value is not None and value >= 90.0 for value in (task["cpu_percent"], task["memory_percent"]))
    if high_resource:
        risks.append(
            _risk(
                "resource_pressure",
                "medium",
                "采样显示工作进程资源占用率达到 90% 以上，持续高并发可能降低监督和交互响应。",
                ["task.cpu_percent", "task.memory_percent"],
            )
        )
        actions.append(
            _action(
                "review_resource_policy",
                "supervision",
                "评审资源上限与并发策略",
                "在下一次任务前调整监管层资源预算；不得在运行中静默修改现有任务参数。",
                requires_human_approval=True,
                evidence_refs=["task.cpu_percent", "task.memory_percent"],
            )
        )

    if not quality_audit:
        risks.append(
            _risk(
                "missing_quality_evidence",
                "medium",
                "没有提供质量审计证据，不能据此批准自动或批量改进。",
                ["quality"],
            )
        )
        actions.append(
            _action(
                "generate_quality_audit",
                "business",
                "先生成质量审计",
                "在任务稳定或完成后调用质量审计，再基于实际页级证据生成下一版计划。",
                requires_human_approval=False,
                evidence_refs=["quality"],
            )
        )

    if not actions:
        actions.append(
            _action(
                "preserve_current_version",
                "iteration",
                "保留当前输出版本并进行人工抽查",
                "未发现需策略变更的强证据；继续保留现有产物与审计记录，避免无依据重跑。",
                requires_human_approval=False,
                evidence_refs=["task", "quality"],
            )
        )

    approval_reasons = [action["title"] for action in actions if action["requires_human_approval"]]
    evidence_digest = _canonical_digest(evidence)
    plan_key = {
        "schema_version": ITERATION_PLAN_SCHEMA_VERSION,
        "strategy_version": version,
        "evidence_digest": evidence_digest,
        "risk_ids": [item["risk_id"] for item in risks],
        "action_ids": [item["action_id"] for item in actions],
    }
    plan_id = f"iteration-{_canonical_digest(plan_key)[:16]}"
    status = "requires_human_approval" if approval_reasons else "advisory_review_ready"
    return {
        "plan_schema_version": ITERATION_PLAN_SCHEMA_VERSION,
        "strategy_version": version,
        "plan_id": plan_id,
        "generated_at": _normalise_timestamp(generated_at),
        "status": status,
        "evidence_digest": evidence_digest,
        "evidence_summary": evidence,
        "risks": risks,
        "recommended_actions": actions,
        "requires_human_approval": bool(approval_reasons),
        "approval_reasons": approval_reasons,
        "rollback": {
            "policy": "本计划仅输出建议，不执行 OCR、进程控制、文件写入或策略变更。",
            "after_approval": "获批操作必须创建独立输出版本并记录输入、规则版本和责任人；回滚时恢复先前输出版本。",
        },
        "governance": {
            "advisory_only": True,
            "can_auto_apply": False,
            "self_modification": False,
            "network_access": False,
            "persistence": False,
        },
    }


def propose_iteration_plan(
    task_status: Mapping[str, Any] | None = None,
    quality_audit: Mapping[str, Any] | None = None,
    task_events: Iterable[Mapping[str, Any]] | None = None,
    *,
    strategy_version: str = DEFAULT_STRATEGY_VERSION,
    generated_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Compatibility-oriented alias for :func:`build_iteration_plan`."""
    return build_iteration_plan(
        task_status,
        quality_audit,
        task_events,
        strategy_version=strategy_version,
        generated_at=generated_at,
    )
