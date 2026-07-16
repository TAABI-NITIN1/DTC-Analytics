import pytest

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.security import SecurityError, context_from_verified_claims, development_context, reject_customer_arguments, require_scope, sign_conversation_state, verify_conversation_state
from src.clickhouse_utils import get_dtc_mcp_clickhouse_client


def claims(**overrides):
    values = {"user_id": "u1", "tenant_id": "tenant-a", "customer_id": "customer-a", "allowed_customer_ids": ["101"], "roles": ["analyst"], "scopes": ["dtc:fleet:read"], "request_id": "r1", "trace_id": "t1"}
    values.update(overrides)
    return values


def test_missing_tenant_and_role_without_scope_are_rejected():
    with pytest.raises(SecurityError) as missing:
        context_from_verified_claims(claims(allowed_customer_ids=[]))
    assert missing.value.code == ErrorCode.UNAUTHENTICATED
    with pytest.raises(SecurityError) as denied:
        require_scope(context_from_verified_claims(claims()), "dtc:vehicle:read")
    assert denied.value.code == ErrorCode.FORBIDDEN


def test_forged_customer_arguments_are_rejected():
    with pytest.raises(SecurityError) as exc:
        reject_customer_arguments({"customer_name": "other"})
    assert exc.value.code == ErrorCode.SCOPE_VIOLATION


def test_multiple_allowed_customers_are_preserved():
    context = context_from_verified_claims(claims(allowed_customer_ids=["101", "102"]))
    assert context.allowed_customer_ids == ("101", "102")


def test_development_bypass_is_disabled_in_production(monkeypatch):
    monkeypatch.setenv("DTC_MCP_DEV_CONTEXT_ENABLED", "true")
    with pytest.raises(SecurityError) as exc:
        development_context(DTCSettings(environment="production"))
    assert exc.value.code == ErrorCode.FORBIDDEN


def test_production_requires_dedicated_read_only_clickhouse_config(monkeypatch):
    monkeypatch.setenv("DEPLOYMENT_ENV", "production")
    for suffix in ("Host", "Port", "User", "Password", "Database"):
        monkeypatch.delenv(f"DTC_MCP_CH_DB_{suffix}", raising=False)
    with pytest.raises(RuntimeError, match="Dedicated DTC MCP"):
        get_dtc_mcp_clickhouse_client()


def test_conversation_state_signature_detects_tampering(monkeypatch):
    monkeypatch.setenv("DTC_MCP_CONVERSATION_STATE_HMAC_SECRET", "test-only-secret")
    state = {"active_customer_scope": "scope", "last_tool": "get_top_dtcs"}
    signature = sign_conversation_state(state)
    assert verify_conversation_state(state, signature)
    assert not verify_conversation_state({**state, "last_tool": "run_validated_dtc_sql"}, signature)
