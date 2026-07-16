import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from src.dtc_mcp.client import DTCMCPClient, MCPClientError
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.security import context_from_verified_claims


def context():
    return context_from_verified_claims({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "scopes": ["dtc:fleet:read"], "request_id": "r", "trace_id": "trace"})


def payload():
    return {"ok": True, "tool_name": "get_fleet_health_summary", "data": [], "row_count": 0, "truncated": False, "evidence": None, "limitations": [], "error": None, "request_id": "r", "status": "empty"}


class FakeSession:
    def __init__(self, *, delay=0, fail=False):
        self.delay = delay
        self.fail = fail
        self.calls = []

    async def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.fail:
            raise OSError("offline")
        return SimpleNamespace(isError=False, structuredContent=payload(), content=[])

    async def list_tools(self):
        return SimpleNamespace(tools=[SimpleNamespace(name="get_fleet_health_summary")])


def factory(session, closed):
    @asynccontextmanager
    async def open_session():
        try:
            yield session
        finally:
            closed.append(True)
    return open_session


def test_typed_invocation_discovery_and_cleanup():
    session, closed = FakeSession(), []
    client = DTCMCPClient(DTCSettings(), context(), session_factory=factory(session, closed))
    response = asyncio.run(client.call_tool("get_fleet_health_summary", {}))
    assert response.ok and response.status.value == "empty"
    assert asyncio.run(client.list_tools()) == ("get_fleet_health_summary",)
    assert len(closed) == 2


def test_timeout_and_unavailable_are_bounded():
    timeout_client = DTCMCPClient(DTCSettings(query_timeout_seconds=0.01), context(), session_factory=factory(FakeSession(delay=0.1), []))
    with pytest.raises(MCPClientError):
        asyncio.run(timeout_client.call_tool("get_fleet_health_summary", {}))
    failed = DTCMCPClient(DTCSettings(), context(), session_factory=factory(FakeSession(fail=True), []))
    for _ in range(3):
        with pytest.raises(MCPClientError):
            asyncio.run(failed.call_tool("get_fleet_health_summary", {}))
    assert failed.circuit_open_until > 0
    with pytest.raises(MCPClientError, match="circuit"):
        asyncio.run(failed.call_tool("get_fleet_health_summary", {}))
