import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from starlette.testclient import TestClient

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.repository import RepositoryExecutor
from src.dtc_mcp.security import ContextProvider, context_from_verified_claims
from src.dtc_mcp.server import create_http_app, create_server


def test_stdio_initialize_list_status_and_clean_shutdown(capsys):
    async def run():
        env = dict(os.environ)
        env.update({
            "DTC_MCP_TRANSPORT": "stdio",
            "DTC_MCP_DEV_CONTEXT_ENABLED": "true",
            "DTC_MCP_DEV_USER_ID": "dev-user",
            "DTC_MCP_DEV_TENANT_ID": "dev-tenant",
            "DTC_MCP_DEV_CUSTOMER_ID": "dev-customer",
            "DTC_MCP_DEV_ALLOWED_CUSTOMER_IDS": "101",
            "DTC_MCP_DEV_SCOPES": "dtc:fleet:read,dtc:vehicle:read,dtc:maintenance:read,dtc:schema:read",
        })
        params = StdioServerParameters(command=sys.executable, args=["-m", "src.dtc_mcp"], cwd=os.getcwd(), env=env)
        async with stdio_client(params) as streams:
            async with ClientSession(*streams) as session:
                initialized = await session.initialize()
                tools = await session.list_tools()
                status = await session.call_tool("get_dtc_mcp_server_status", {})
                assert initialized.serverInfo.name == "TAABI DTC Analytics MCP"
                assert "get_fleet_health_summary" in {tool.name for tool in tools.tools}
                assert not status.isError
    asyncio.run(run())
    assert capsys.readouterr().out == ""


def test_http_health_startup_and_signed_identity_boundary(monkeypatch):
    settings = DTCSettings(environment="production", transport="streamable_http", allowed_origins=("https://internal.example",))
    context = context_from_verified_claims({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "scopes": ["dtc:fleet:read"], "request_id": "r", "trace_id": "trace"})
    fake = type("Client", (), {"execute": lambda self, query, params, settings=None: [(1,)] if query == "SELECT 1" else []})()
    mcp = create_server(settings=settings, contexts=ContextProvider(None), executor=RepositoryExecutor(lambda: fake, settings))
    app = create_http_app(mcp)
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/ready").status_code == 200
        assert client.post("/mcp").status_code == 401
        secret = "test-only-secret"
        monkeypatch.setenv("DTC_MCP_IDENTITY_HMAC_SECRET", secret)
        payload = base64.urlsafe_b64encode(context.model_dump_json().encode()).decode().rstrip("=")
        signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        response = client.post("/mcp", headers={"X-DTC-Identity": payload, "X-DTC-Identity-Signature": signature})
        assert response.status_code != 401
        rejected_origin = client.post("/mcp", headers={"Origin": "https://evil.example", "X-DTC-Identity": payload, "X-DTC-Identity-Signature": signature})
        assert rejected_origin.status_code == 403
        allowed_origin = client.post("/mcp", headers={"Origin": "https://internal.example", "X-DTC-Identity": payload, "X-DTC-Identity-Signature": signature})
        assert allowed_origin.status_code not in {401, 403}
