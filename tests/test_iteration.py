from __future__ import annotations

from copy import deepcopy

import pytest

from pdf_rescue_mcp.iteration import (
    DEFAULT_STRATEGY_VERSION,
    ITERATION_PLAN_SCHEMA_VERSION,
    build_iteration_plan,
    propose_iteration_plan,
)


def _action_ids(plan: dict) -> set[str]:
    return {str(item["action_id"]) for item in plan["recommended_actions"]}


def _risk_ids(plan: dict) -> set[str]:
    return {str(item["risk_id"]) for item in plan["risks"]}


def test_plan_is_versioned_advisory_and_uses_quality_evidence() -> None:
    task_status = {
        "状态": {"状态": "已完成", "目标页数": 120, "已处理页数": 120},
        "状态新鲜度": {"运行判断": "已完成", "疑似中断": False},
        "任务指标": {
            "总处理页数": 120,
            "已处理页数": 120,
            "资源占用率": {"CPU百分比": 23.5, "内存百分比": 31.0},
        },
    }
    quality_audit = {
        "状态": "已完成",
        "目标页数": 120,
        "已巡检页数": 120,
        "尚未巡检页数": 0,
        "低置信页数": 6,
        "密集索引保护低置信页数": 2,
        "无文本页数": 1,
        "可自动刷新页数": 2,
        "可自动刷新页样例": [9, 14],
        "图表噪声残留页数": 1,
        "建议": ["低置信页需要优先抽图核对"],
    }
    task_before = deepcopy(task_status)
    audit_before = deepcopy(quality_audit)

    plan = build_iteration_plan(
        task_status,
        quality_audit,
        [{"事件": "quality_audit", "记录时间": "2026-07-19T12:00:00+00:00"}],
        generated_at="2026-07-19T12:01:00+00:00",
    )

    assert plan["plan_schema_version"] == ITERATION_PLAN_SCHEMA_VERSION
    assert plan["strategy_version"] == DEFAULT_STRATEGY_VERSION
    assert plan["plan_id"].startswith("iteration-")
    assert plan["generated_at"] == "2026-07-19T12:01:00+00:00"
    assert plan["requires_human_approval"] is True
    assert {"low_confidence_text", "blank_page_output", "new_cleanup_rules_available"} <= _risk_ids(plan)
    assert {
        "sample_low_confidence_pages",
        "review_blank_pages",
        "approve_rule_refresh",
        "manual_layout_review",
    } <= _action_ids(plan)
    assert plan["evidence_summary"]["events"]["by_type"] == {"quality_audit": 1}
    assert all(action["execution"] == "advisory_only" for action in plan["recommended_actions"])
    assert all(action["can_auto_apply"] is False for action in plan["recommended_actions"])
    assert plan["governance"] == {
        "advisory_only": True,
        "can_auto_apply": False,
        "self_modification": False,
        "network_access": False,
        "persistence": False,
    }
    assert task_status == task_before
    assert quality_audit == audit_before


def test_plan_flags_stalled_incomplete_job_and_missing_audit() -> None:
    plan = build_iteration_plan(
        {
            "状态": {"状态": "进行中", "目标页数": 20, "已处理页数": 7},
            "状态新鲜度": {"运行判断": "疑似中断", "疑似中断": True},
        },
        generated_at="2026-07-19T12:01:00+00:00",
    )

    assert {"task_recovery_required", "incomplete_coverage", "missing_quality_evidence"} <= _risk_ids(plan)
    assert {
        "review_and_recover_task",
        "complete_page_coverage",
        "generate_quality_audit",
    } <= _action_ids(plan)
    assert plan["status"] == "requires_human_approval"
    assert plan["evidence_summary"]["task"]["processed_pages"] == 7


def test_healthy_evidence_preserves_current_version_without_approval() -> None:
    status = {"状态": {"状态": "已完成", "目标页数": 2, "已处理页数": 2}}
    audit = {
        "状态": "已完成",
        "目标页数": 2,
        "已巡检页数": 2,
        "尚未巡检页数": 0,
        "低置信页数": 0,
        "无文本页数": 0,
        "可自动刷新页数": 0,
        "图表噪声残留页数": 0,
        "分裂标题残留页数": 0,
        "图文混排标注页数": 0,
    }

    first = build_iteration_plan(status, audit, generated_at="2026-07-19T12:01:00+00:00")
    second = propose_iteration_plan(status, audit, generated_at="2026-07-19T13:01:00+00:00")

    assert _action_ids(first) == {"preserve_current_version"}
    assert first["requires_human_approval"] is False
    assert first["status"] == "advisory_review_ready"
    assert first["plan_id"] == second["plan_id"]
    assert first["evidence_digest"] == second["evidence_digest"]


def test_plan_handles_untrusted_events_without_execution_surface() -> None:
    plan = build_iteration_plan(
        {},
        {},
        [{"event": "worker_started", "timestamp": "t1"}, "not-a-record", {"event": "worker_started"}],
        generated_at="2026-07-19T12:01:00+00:00",
    )

    assert plan["evidence_summary"]["events"] == {
        "count": 2,
        "by_type": {"worker_started": 2},
        "last_event": {"type": "worker_started", "timestamp": "t1"},
        "ignored_count": 1,
    }
    serialized = str(plan)
    assert "subprocess" not in serialized
    assert "command" not in serialized


def test_plan_understands_the_durable_supervision_event_and_resource_schema() -> None:
    plan = build_iteration_plan(
        {"任务指标": {"资源占用率": {"CPU占用率": 95.0, "内存占用率": 91.0}}},
        {},
        [{"event_type": "page_completed", "created_at": 123.0}],
        generated_at="2026-07-19T12:01:00+00:00",
    )

    assert plan["evidence_summary"]["events"]["by_type"] == {"page_completed": 1}
    assert plan["evidence_summary"]["task"]["cpu_percent"] == 95.0
    assert "review_resource_policy" in _action_ids(plan)


def test_empty_strategy_version_is_rejected() -> None:
    with pytest.raises(ValueError, match="strategy_version"):
        build_iteration_plan(strategy_version="   ")
