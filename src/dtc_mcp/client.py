from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncIterator, Callable

import httpx
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamable_http_client

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import TenantContext, ToolResponse


class MCPClientError(Exception):
    pass


def choose_data_route(*, reusable_evidence: bool, domain_tool: str | None, needs_metadata: bool, dynamic_sql_enabled: bool) -> str:
    """Apply the analyst's least-powerful-data-path routing order."""
    if reusable_evidence:
        return "reuse_evidence"
    if domain_tool:
        return "domain_tool"
    if needs_metadata:
        return "schema_catalog"
    if dynamic_sql_enabled:
        return "validated_sql"
    return "unsupported"


class DTCMCPClient:
    def __init__(self, settings: DTCSettings, context: TenantContext, *, session_factory: Callable[[], Any] | None = None):
        self.settings = settings
        self.context = context
        self.session_factory = session_factory
        self.failures = 0
        self.circuit_open_until = 0.0

    def _signed_headers(self) -> dict[str, str]:
        secret = os.getenv("DTC_MCP_IDENTITY_HMAC_SECRET", "")
        if not secret:
            raise MCPClientError("Production MCP identity signing is not configured")
        payload = base64.urlsafe_b64encode(self.context.model_dump_json().encode()).decode().rstrip("=")
        signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return {"X-DTC-Identity": payload, "X-DTC-Identity-Signature": signature, "X-Request-ID": self.context.request_id, "X-Trace-ID": self.context.trace_id}

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[ClientSession]:
        if self.session_factory:
            async with self.session_factory() as session:
                yield session
            return
        if self.settings.transport == "stdio":
            env = dict(os.environ)
            env.update({
                "DTC_MCP_TRANSPORT": "stdio",
                "DTC_MCP_DEV_CONTEXT_ENABLED": "true",
                "DTC_MCP_DEV_USER_ID": self.context.user_id,
                "DTC_MCP_DEV_TENANT_ID": self.context.tenant_id,
                "DTC_MCP_DEV_CUSTOMER_ID": self.context.customer_id,
                "DTC_MCP_DEV_ALLOWED_CUSTOMER_IDS": ",".join(self.context.allowed_customer_ids),
                "DTC_MCP_DEV_ROLES": ",".join(sorted(self.context.roles)),
                "DTC_MCP_DEV_SCOPES": ",".join(sorted(self.context.scopes)),
            })
            parameters = StdioServerParameters(command=sys.executable, args=["-m", "src.dtc_mcp"], env=env, cwd=os.getcwd())
            async with stdio_client(parameters) as streams:
                async with ClientSession(*streams, read_timeout_seconds=timedelta(seconds=self.settings.query_timeout_seconds)) as session:
                    await session.initialize()
                    yield session
        else:
            async with httpx.AsyncClient(headers=self._signed_headers(), timeout=self.settings.query_timeout_seconds) as http_client:
                async with streamable_http_client(self.settings.streamable_http_url, http_client=http_client) as streams:
                    async with ClientSession(streams[0], streams[1], read_timeout_seconds=timedelta(seconds=self.settings.query_timeout_seconds)) as session:
                        await session.initialize()
                        yield session

    async def list_tools(self) -> tuple[str, ...]:
        async with self.connect() as session:
            result = await session.list_tools()
            return tuple(tool.name for tool in result.tools)

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> ToolResponse[Any]:
        if time.monotonic() < self.circuit_open_until:
            raise MCPClientError("MCP circuit is temporarily open")
        last_error: Exception | None = None
        for attempt in range(2):
            try:
                async with asyncio.timeout(self.settings.query_timeout_seconds):
                    async with self.connect() as session:
                        result = await session.call_tool(name, arguments)
                if result.isError:
                    raise MCPClientError("MCP tool returned an error")
                payload = result.structuredContent
                if payload is None and result.content:
                    payload = json.loads(getattr(result.content[0], "text", "{}"))
                response = ToolResponse[Any].model_validate(payload)
                self.failures = 0
                return response
            except (TimeoutError, httpx.TransportError, OSError, MCPClientError) as exc:
                last_error = exc
                if attempt == 0:
                    await asyncio.sleep(0)
                    continue
        self.failures += 1
        if self.failures >= 3:
            self.circuit_open_until = time.monotonic() + 30
        raise MCPClientError("MCP tool call failed") from last_error


def invoke_tool(settings: DTCSettings, context: TenantContext, name: str, arguments: dict[str, Any]) -> ToolResponse[Any]:
    return asyncio.run(DTCMCPClient(settings, context).call_tool(name, arguments))
