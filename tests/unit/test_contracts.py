from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.dtc_mcp.models import EvidenceMetadata, ErrorCode, ToolError, ToolResponse, ToolStatus


def evidence(*, rows=1, truncated=False):
    return EvidenceMetadata(
        tables=("fleet_health_summary_ravi_v2",),
        query_type="fleet_health_summary",
        query_hash="abc",
        filters_applied={"tenant_scope": "server"},
        data_freshness="producer_checkpoint_unknown",
        scope_ref="tenant-hash",
        as_of=datetime.now(timezone.utc),
        row_count=rows,
        truncated=truncated,
        effective_limit=10,
        duration_ms=1.2,
        trace_id="trace-1",
    )


def test_success_empty_and_truncated_contracts_are_distinct():
    success = ToolResponse(ok=True, tool_name="x", data=[{"value": 1}], row_count=1, evidence=evidence(), request_id="r", status=ToolStatus.SUCCESS)
    empty = ToolResponse(ok=True, tool_name="x", data=[], row_count=0, evidence=evidence(rows=0), request_id="r", status=ToolStatus.EMPTY)
    truncated = ToolResponse(ok=True, tool_name="x", data=[{"value": 1}], row_count=1, truncated=True, evidence=evidence(truncated=True), request_id="r", status=ToolStatus.SUCCESS)
    assert success.ok and empty.status == ToolStatus.EMPTY and truncated.truncated


def test_repository_error_and_invalid_contract_input():
    response = ToolResponse(ok=False, tool_name="x", row_count=0, error=ToolError(code=ErrorCode.UPSTREAM_UNAVAILABLE, message="Analytics query failed", retryable=True), request_id="r", status=ToolStatus.ERROR)
    assert response.error and response.error.retryable
    with pytest.raises(ValidationError):
        ToolResponse(ok=True, tool_name="x", row_count=-1, request_id="r", status=ToolStatus.SUCCESS)


def test_stable_json_and_evidence_metadata():
    response = ToolResponse(ok=True, tool_name="x", data=[], row_count=0, evidence=evidence(rows=0), request_id="r", status=ToolStatus.EMPTY)
    assert response.stable_json() == response.stable_json()
    assert '"query_hash":"abc"' in response.stable_json()
    assert "raw_sql" not in response.stable_json()
