import asyncio
import json

import pytest

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.repository import RepositoryExecutor
from src.dtc_mcp.security import ContextProvider, context_from_verified_claims
from src.dtc_mcp.server import create_server


ALL_SCOPES = ["dtc:fleet:read", "dtc:vehicle:read", "dtc:maintenance:read", "dtc:schema:read", "dtc:sql:execute"]
CORE_CALLS = [
    ("get_fleet_health_summary", {}, "dtc:fleet:read"),
    ("get_top_dtcs", {"limit": 10}, "dtc:fleet:read"),
    ("get_fault_trends", {"days": 30, "limit": 20}, "dtc:fleet:read"),
    ("get_vehicle_health", {"uniqueid": "v1"}, "dtc:vehicle:read"),
    ("get_vehicle_faults", {"uniqueid": "v1", "days": 30, "limit": 20}, "dtc:vehicle:read"),
    ("get_dtc_fleet_impact", {"dtc_code": "P1", "limit": 20}, "dtc:fleet:read"),
    ("get_dtc_cooccurrence", {"dtc_code": "P1", "limit": 20}, "dtc:fleet:read"),
    ("get_maintenance_priority", {"limit": 20}, "dtc:maintenance:read"),
]
INVALID_CALLS = [
    ("get_fleet_health_summary", {"tenant_id": "forged"}),
    ("get_top_dtcs", {"limit": 0}),
    ("get_fault_trends", {"days": 0}),
    ("get_vehicle_health", {}),
    ("get_vehicle_faults", {}),
    ("get_dtc_fleet_impact", {"dtc_code": "not valid!"}),
    ("get_dtc_cooccurrence", {"dtc_code": "not valid!"}),
    ("get_maintenance_priority", {"limit": 0}),
]


class FakeClient:
    def __init__(self, rows=None, error=None):
        self.rows = [{"value": 1}] if rows is None else rows
        self.error = error
        self.calls = []

    def execute(self, query, params, settings=None):
        self.calls.append((query, params, settings))
        if self.error:
            raise self.error
        return self.rows


def context(scopes=ALL_SCOPES, ids=("101",)):
    return context_from_verified_claims({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ids, "scopes": scopes, "request_id": "r", "trace_id": "trace"})


def server(client=None, scopes=ALL_SCOPES, ids=("101",), settings=None):
    settings = settings or DTCSettings()
    client = client or FakeClient()
    return create_server(settings=settings, contexts=ContextProvider(context(scopes, ids)), executor=RepositoryExecutor(lambda: client, settings)), client


@pytest.mark.parametrize("name,args,_scope", CORE_CALLS)
def test_every_core_tool_success_and_evidence(name, args, _scope):
    mcp, client = server()
    response = asyncio.run(mcp._tool_manager.call_tool(name, args, convert_result=False))
    assert response.ok and response.row_count == 1 and response.evidence
    assert response.evidence.filters_applied["tenant_scope"] == "server"
    assert client.calls[-1][1]["tenant_ids"] == ["101"]


@pytest.mark.parametrize("name,args,scope", CORE_CALLS)
def test_every_core_tool_rejects_missing_scope(name, args, scope):
    mcp, _ = server(scopes=[])
    response = asyncio.run(mcp._tool_manager.call_tool(name, args, convert_result=False))
    assert not response.ok and response.error.code.value == "FORBIDDEN"


@pytest.mark.parametrize("name,args,_scope", CORE_CALLS)
def test_every_core_tool_handles_empty_results(name, args, _scope):
    mcp, _ = server(FakeClient(rows=[]))
    response = asyncio.run(mcp._tool_manager.call_tool(name, args, convert_result=False))
    assert response.ok and response.status.value == "empty" and response.data == []


@pytest.mark.parametrize("name,args,_scope", CORE_CALLS)
def test_every_core_tool_bounds_rows_and_repository_failures(name, args, _scope):
    settings = DTCSettings(max_result_rows=2)
    mcp, _ = server(FakeClient(rows=[{"n": 1}, {"n": 2}, {"n": 3}]), settings=settings)
    bounded = asyncio.run(mcp._tool_manager.call_tool(name, args, convert_result=False))
    assert bounded.row_count <= 2 and bounded.truncated
    failed_mcp, _ = server(FakeClient(error=RuntimeError("password=secret")), settings=settings)
    failed = asyncio.run(failed_mcp._tool_manager.call_tool(name, args, convert_result=False))
    assert not failed.ok and failed.error.code.value == "UPSTREAM_UNAVAILABLE" and "secret" not in failed.stable_json()


@pytest.mark.parametrize("name,args", INVALID_CALLS)
def test_every_core_tool_rejects_invalid_input(name, args):
    mcp, _ = server()
    if name == "get_fleet_health_summary":
        with pytest.raises(TypeError, match="tenant_id"):
            mcp._tool_manager._tools[name].fn(**args)
        return
    try:
        response = asyncio.run(mcp._tool_manager.call_tool(name, args, convert_result=False))
    except Exception as exc:
        pytest.fail(f"{name} did not return its structured invalid-argument contract: {exc}")
    else:
        assert not response.ok and response.error.code.value == "INVALID_ARGUMENT"


def test_empty_invalid_repository_failure_and_tenant_isolation():
    mcp, client = server(FakeClient(rows=[]), ids=("101", "102"))
    empty = asyncio.run(mcp._tool_manager.call_tool("get_top_dtcs", {"limit": 5}, convert_result=False))
    assert empty.ok and empty.status.value == "empty" and empty.data == []
    invalid = asyncio.run(mcp._tool_manager.call_tool("get_vehicle_health", {}, convert_result=False))
    assert not invalid.ok and invalid.error.code.value == "INVALID_ARGUMENT"
    assert client.calls[0][1]["tenant_ids"] == ["101", "102"]
    failed_mcp, _ = server(FakeClient(error=RuntimeError("password=secret")))
    failed = asyncio.run(failed_mcp._tool_manager.call_tool("get_top_dtcs", {}, convert_result=False))
    assert not failed.ok and failed.error.code.value == "UPSTREAM_UNAVAILABLE" and "secret" not in failed.stable_json()


def test_tool_discovery_snapshot_and_model_inputs_have_no_tenant_fields():
    mcp, _ = server()
    tools = {tool.name: tool for tool in mcp._tool_manager.list_tools()}
    assert sorted(tools) == sorted([name for name, _, _ in CORE_CALLS] + ["get_dtc_mcp_server_status", "list_dtc_analytics_tables", "get_dtc_table_schema", "get_dtc_metric_definition", "get_dtc_code_info", "run_validated_dtc_sql"])
    forbidden = {"tenant_id", "customer_id", "customer_name", "clientLoginId", "allowed_customer_ids"}
    for tool in tools.values():
        properties = (tool.parameters or {}).get("properties", {})
        assert not forbidden.intersection(properties)


def test_resources_are_allowlisted_consistent_and_bounded():
    mcp, _ = server()

    async def read(uri):
        resource = await mcp._resource_manager.get_resource(uri)
        assert resource is not None
        return await resource.read()

    tables = asyncio.run(read("dtc://catalog/tables"))
    schema = asyncio.run(read("dtc://schema/fleet_health_summary_ravi_v2"))
    metric = asyncio.run(read("dtc://definitions/fleet_health_score"))
    schema_payload = json.loads(schema)
    assert "clientLoginId" not in schema_payload["columns"] and "customer_name" not in schema_payload["columns"]
    assert json.loads(metric)["source_table"] == "fleet_health_summary_ravi_v2"
    assert len(tables) < 64_000 and len(schema) < 64_000 and len(metric) < 64_000
    with pytest.raises(Exception):
        asyncio.run(read("dtc://schema/system.tables"))


def test_dtc_lookup_not_found_is_empty():
    mcp, _ = server(FakeClient(rows=[]))
    result = asyncio.run(mcp._tool_manager.call_tool("get_dtc_code_info", {"dtc_code": "P404"}, convert_result=False))
    assert result.ok and result.status.value == "empty"


def test_diagnostic_catalog_invalid_and_disabled_sql_use_common_envelope():
    mcp, _ = server()
    status = asyncio.run(mcp._tool_manager.call_tool("get_dtc_mcp_server_status", {}, convert_result=False))
    catalog = asyncio.run(mcp._tool_manager.call_tool("list_dtc_analytics_tables", {}, convert_result=False))
    invalid = asyncio.run(mcp._tool_manager.call_tool("get_top_dtcs", {"limit": 999}, convert_result=False))
    dynamic = asyncio.run(mcp._tool_manager.call_tool("run_validated_dtc_sql", {"question_or_reason": "custom analysis", "sql": "SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=3 ORDER BY dtc_code"}, convert_result=False))
    assert status.ok and status.evidence.source == "runtime"
    assert catalog.ok and catalog.evidence.source == "approved_catalog"
    assert invalid.error.code.value == "INVALID_ARGUMENT"
    assert dynamic.error.code.value == "FORBIDDEN"
