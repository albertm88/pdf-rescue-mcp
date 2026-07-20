from __future__ import annotations

import asyncio

from pdf_rescue_mcp.server import mcp


EXPECTED_TOOL_NAMES = [
    "rescue_pdf",
    "run_health_check",
    "inspect_pdf_text_layer",
    "diagnose_pdf",
    "plan_pdf_job",
    "extract_book_text",
    "extract_book_background",
    "get_page_evidence",
    "get_term_glossary",
    "update_term_glossary",
    "export_page_image_evidence",
    "get_job_status",
    "get_processing_history",
    "share_processing_history",
    "resume_job",
    "cancel_job",
    "audit_job_quality",
    "get_iteration_plan",
    "scan_pdf_library",
    "batch_extract_library",
    "get_batch_status",
    "stop_batch",
    "plan_ocr_capacity_profile",
    "start_ocr_capacity_profile",
    "get_ocr_capacity_profile",
    "activate_ocr_capacity_profile",
]


def test_mcp_tool_contract_is_stable() -> None:
    tools = asyncio.run(mcp.list_tools())
    names = [tool.name for tool in tools]

    assert names == EXPECTED_TOOL_NAMES
    assert len(names) == len(set(names))
    assert all(tool.title and tool.description for tool in tools)
    primary = tools[0]
    assert primary.name == "rescue_pdf"
    assert {"path", "request"}.issubset(primary.inputSchema["properties"])
