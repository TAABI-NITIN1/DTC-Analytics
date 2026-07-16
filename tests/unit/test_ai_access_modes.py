import json

import pytest

import src.ai_analyst as analyst
from src.dtc_mcp.client import choose_data_route
from src.dtc_mcp.models import ToolResponse
from src.dtc_mcp.security import tenant_scope_fingerprint


def call(name="get_fleet_health"):
    return [{"name": name, "args": {}, "id": "call-1"}]


def run(mode, monkeypatch, *, mcp_result=None, direct_result=None, name="get_fleet_health"):
    direct_result = direct_result or {"data": [{"source": "direct"}], "count": 1}
    monkeypatch.setitem(analyst._TOOL_HANDLERS, name, lambda args: direct_result)
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: mcp_result or {"ok": True, "data": [{"source": "mcp"}], "row_count": 1, "evidence": {"query_hash": "h"}})
    messages, _, _ = analyst._run_tool_calls_parallel(call(name), [], None, "request", [], "question", {"_dtc_data_access_mode": mode, "_trusted_customer_name": "trusted"})
    return json.loads(messages[0].content)


def test_direct_mode_compatibility(monkeypatch):
    assert run("direct", monkeypatch)["data"][0]["source"] == "direct"


def test_mcp_mode_never_calls_direct_handler(monkeypatch):
    monkeypatch.setitem(analyst._TOOL_HANDLERS, "get_fleet_health", lambda args: pytest.fail("direct DB path used"))
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: {"ok": True, "data": [{"source": "mcp"}], "row_count": 1, "evidence": {"query_hash": "h"}})
    messages, _, data = analyst._run_tool_calls_parallel(call(), [], None, "request", [], "question", {"_dtc_data_access_mode": "mcp"})
    assert json.loads(messages[0].content)["data"][0]["source"] == "mcp" and data[0]["source"] == "mcp"


def test_shadow_returns_direct_result_and_compares_mcp(monkeypatch, caplog):
    caplog.set_level("INFO")
    result = run("shadow", monkeypatch, mcp_result={"ok": True, "data": [{"source": "mcp"}], "row_count": 1})
    assert result["data"][0]["source"] == "direct"
    assert "MCP SHADOW" in caplog.text


def test_mcp_unavailable_and_dynamic_sql_disabled_do_not_fall_back(monkeypatch):
    monkeypatch.delenv("DTC_MCP_DIRECT_FALLBACK_ENABLED", raising=False)
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: {"error": "unavailable", "code": "UPSTREAM_UNAVAILABLE"})
    monkeypatch.setitem(analyst._TOOL_HANDLERS, "get_fleet_health", lambda args: pytest.fail("unexpected direct fallback"))
    messages, _, _ = analyst._run_tool_calls_parallel(call(), [], None, "request", [], "question", {"_dtc_data_access_mode": "mcp"})
    assert json.loads(messages[0].content)["code"] == "UPSTREAM_UNAVAILABLE"
    sql_messages, _, _ = analyst._run_tool_calls_parallel([{"name": "run_sql", "args": {"query": "SELECT 1"}, "id": "sql"}], [], None, "request", [], "custom", {"_dtc_data_access_mode": "mcp"})
    assert "error" in json.loads(sql_messages[0].content)


def test_mcp_arguments_strip_model_tenant_and_keep_evidence(monkeypatch):
    args = analyst._mcp_arguments("get_vehicle_health", {"uniqueid": "v1", "customer_name": "forged", "tenant_id": "other"}, "q")
    assert args == {"uniqueid": "v1"}
    result = run("mcp", monkeypatch, mcp_result={"ok": True, "data": [], "row_count": 0, "evidence": {"query_hash": "grounded"}})
    assert result["evidence"]["query_hash"] == "grounded"


def test_explicit_direct_fallback_uses_trusted_scope(monkeypatch):
    captured = {}
    monkeypatch.setenv("DTC_MCP_DIRECT_FALLBACK_ENABLED", "true")
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: {"error": "offline"})
    monkeypatch.setitem(analyst._TOOL_HANDLERS, "get_fleet_health", lambda args: captured.update(args) or {"data": []})
    tenant = {"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"}
    messages, _, _ = analyst._run_tool_calls_parallel(call(), [], None, "request", [], "question", {"_dtc_data_access_mode": "mcp", "_trusted_customer_name": "trusted-customer", "_dtc_tenant_context": tenant})
    assert captured["customer_name"] == "trusted-customer"
    result = json.loads(messages[0].content)
    assert result["_fallback"] == "explicit_direct" and result["_trace_id"] == "trace"


def test_route_policy_prefers_reuse_domain_catalog_then_sql():
    assert choose_data_route(reusable_evidence=True, domain_tool='tool', needs_metadata=True, dynamic_sql_enabled=True) == 'reuse_evidence'
    assert choose_data_route(reusable_evidence=False, domain_tool='tool', needs_metadata=True, dynamic_sql_enabled=True) == 'domain_tool'
    assert choose_data_route(reusable_evidence=False, domain_tool=None, needs_metadata=True, dynamic_sql_enabled=True) == 'schema_catalog'
    assert choose_data_route(reusable_evidence=False, domain_tool=None, needs_metadata=False, dynamic_sql_enabled=True) == 'validated_sql'
    assert choose_data_route(reusable_evidence=False, domain_tool=None, needs_metadata=False, dynamic_sql_enabled=False) == 'unsupported'


def test_tenant_scoped_conversation_evidence_is_reused(monkeypatch):
    tenant = analyst.TenantContext.model_validate({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"})
    arguments_hash = analyst.hashlib.sha256(json.dumps({}, sort_keys=True).encode()).hexdigest()
    prior_response = {"ok": True, "tool_name": "get_fleet_health_summary", "data": [{"healthy": 2}], "row_count": 1, "truncated": False, "evidence": {"query_hash": "grounded"}, "limitations": [], "error": None, "request_id": "old", "status": "success"}
    context = {"_dtc_tenant_context": tenant, "_dtc_conversation_state_verified": True, "_dtc_conversation_state": {"active_customer_scope": tenant_scope_fingerprint(tenant), "last_tool": "get_fleet_health_summary", "last_arguments_hash": arguments_hash, "last_response": prior_response}}
    monkeypatch.setattr(analyst, "invoke_mcp_tool", lambda *args: pytest.fail("fresh MCP call should not run"))
    result = analyst._invoke_mcp_analyst_tool("get_fleet_health", {}, context, "same question")
    assert result["_reused_evidence"] is True and result["evidence"]["query_hash"] == "grounded"


def test_evidence_is_not_reused_across_tenant_scopes(monkeypatch):
    tenant = analyst.TenantContext.model_validate({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"})
    called = []
    response = ToolResponse.model_validate({"ok": True, "tool_name": "get_fleet_health_summary", "data": [], "row_count": 0, "truncated": False, "evidence": None, "limitations": [], "error": None, "request_id": "new", "status": "empty"})
    monkeypatch.setattr(analyst, "invoke_mcp_tool", lambda *args: called.append(True) or response)
    context = {"_dtc_tenant_context": tenant, "_dtc_conversation_state": {"active_customer_scope": "other", "last_tool": "get_fleet_health_summary", "last_arguments_hash": "x", "last_response": {"ok": True, "evidence": {"query_hash": "old"}}}}
    result = analyst._invoke_mcp_analyst_tool("get_fleet_health", {}, context, "question")
    assert called and "_reused_evidence" not in result


def test_conversation_state_is_structured_bounded_and_scope_bound():
    tenant = analyst.TenantContext.model_validate({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"})
    evidence = {"query_hash": "grounded", "query_window": {"start": "2026-01-01", "end": "2026-01-31"}}
    result = {"ok": True, "data": [{"rank": i} for i in range(25)], "evidence": evidence, "_tool_name": "get_top_dtcs", "_arguments_hash": "args"}
    state = analyst._conversation_state({"intent": "fleet_summary", "tool_results": {"call": result}}, {"_dtc_tenant_context": tenant, "vehicle_number": "TRUCK-1"})
    assert state["active_customer_scope"] == tenant_scope_fingerprint(tenant)
    assert state["last_tool"] == "get_top_dtcs" and state["last_evidence"] == evidence
    assert len(state["confirmed_facts"]) == 20 and "user_id" not in json.dumps(state)


def test_empty_evidence_returns_a_scoped_retry_message():
    state = {"messages": [analyst.HumanMessage(content="How many critical vehicles?")], "intent": "fleet_summary", "tool_results": {"call": {"ok": True, "data": [], "evidence": None}}, "failure_reasons": [], "nodes_executed": [], "token_usage": {"prompt": 0, "completion": 0}}
    result = analyst._node_explain(state)
    assert "insufficient_evidence" in result["failure_reasons"]
    assert "could not fetch enough reliable data" in result["messages"][0].content


def test_unsupported_numeric_claim_is_blocked_without_evidence():
    state = {"messages": [analyst.HumanMessage(content="Give me the total")], "intent": "fleet_summary", "tool_results": {"call": {"error": "offline"}}, "recommendation": "There are 999 critical vehicles.", "failure_reasons": [], "nodes_executed": [], "token_usage": {"prompt": 0, "completion": 0}}
    result = analyst._node_explain(state)
    assert "999" not in result["messages"][0].content
    assert "insufficient_evidence" in result["failure_reasons"]


def test_mcp_definition_fast_path_uses_no_llm_tokens(monkeypatch):
    tenant = analyst.TenantContext.model_validate({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:schema:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"})
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: {"ok": True, "data": [{"dtc_code": "P0123", "description": "Sensor fault", "system": "engine", "action_required": "Inspect sensor"}], "row_count": 1, "truncated": False, "limitations": [], "evidence": {"cache_status": "miss", "truncated": False}, "request_id": "request", "_tool_name": "get_dtc_code_info", "_arguments_hash": "args"})
    response = analyst._fast_path_response([{"role": "user", "content": "What is code P0123?"}], {"_dtc_tenant_context": tenant, "customer_name": "c"})
    assert response["token_usage"] == {"prompt": 0, "completion": 0}
    assert response["tools_called"] == ["get_dtc_code_info"] and "Sensor fault" in response["text"]


def test_fast_path_discloses_cached_and_truncated_evidence(monkeypatch):
    tenant = analyst.TenantContext.model_validate({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": [], "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace", "auth_source": "test"})
    monkeypatch.setattr(analyst, "_invoke_mcp_analyst_tool", lambda *args: {"ok": True, "data": [{"fleet_health_score": 80, "total_vehicles": 4, "vehicles_with_active_faults": 2, "vehicles_with_critical_faults": 1}], "row_count": 1, "truncated": True, "limitations": [], "evidence": {"cache_status": "hit", "data_freshness": "cached age_seconds=10", "truncated": True}, "request_id": "request", "_tool_name": "get_fleet_health_summary", "_arguments_hash": "args"})
    response = analyst._fast_path_response([{"role": "user", "content": "fleet summary"}], {"_dtc_tenant_context": tenant, "customer_name": "c"})
    assert "truncated" in response["text"].lower() and "cached age_seconds=10" in response["text"]


def test_explain_compacts_tool_payload_before_llm(monkeypatch):
    captured = {}
    result = {
        "ok": True,
        "tool_name": "get_top_dtcs",
        "data": [{"dtc_code": f"P{i:04d}", "description": "x" * 500} for i in range(20)],
        "row_count": 20,
        "evidence": {"query_hash": "grounded", "data_freshness": "fresh"},
    }
    fake_response = type("Response", (), {"content": "Three representative rows were reviewed.", "usage_metadata": {}})()
    class LLM:
        def invoke(self, messages):
            captured["prompt"] = messages[-1].content
            return fake_response
    monkeypatch.setattr(analyst, "_reasoning_llm", lambda: LLM())
    state = {"messages": [analyst.HumanMessage(content="Show top DTCs")], "intent": "fleet_summary", "tool_results": {"call": result}, "failure_reasons": [], "nodes_executed": [], "token_usage": {"prompt": 0, "completion": 0}}
    analyst._node_explain(state)
    assert "P0002" in captured["prompt"] and "P0003" not in captured["prompt"]
    assert len(captured["prompt"]) < 3000


def test_judge_and_persistence_are_only_scheduled_after_response(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AI_ANALYST_PERSIST_OBSERVABILITY", "1")
    monkeypatch.setattr(analyst, "mlflow", None)
    monkeypatch.setattr(analyst, "_evaluation_sampled", lambda request_id: True)
    monkeypatch.setattr(analyst, "evaluate_trace", lambda **kwargs: pytest.fail("judge ran on response thread"))
    scheduled = {}
    monkeypatch.setattr(analyst, "_schedule_post_response_work", lambda **kwargs: scheduled.update(kwargs) or True)

    def invoke(state, config):
        completed = dict(state)
        completed.update({"messages": state["messages"] + [analyst.AIMessage(content="Grounded answer")], "intent": "fleet_summary", "request_id": "request", "trace_log": [], "tool_results": {}, "tools_used": [], "nodes_executed": ["explain"], "failure_reasons": []})
        return completed

    monkeypatch.setattr(analyst._agent_graph, "invoke", invoke)
    response = analyst._chat_with_configured_data_access([{"role": "user", "content": "Question"}], {})
    assert response["text"] == "Grounded answer"
    assert response["evaluation"] == {"status": "queued"}
    assert response["post_response_scheduled"] is True and scheduled["sampled"] is True and scheduled["persist"] is True
