from __future__ import annotations

import logging
import sys
import hashlib
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route
from pydantic import ValidationError

from src.dtc_mcp import __version__
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.dtc_repository import DTCRepository
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.maintenance_repository import MaintenanceRepository
from src.dtc_mcp.repository import RepositoryExecutor
from src.dtc_mcp.models import EvidenceMetadata, QueryMetadata, RepositoryResult, ToolResponse, ToolStatus
from src.dtc_mcp.resources import register_resources
from src.dtc_mcp.schema_repository import SchemaRepository
from src.dtc_mcp.security import ContextProvider, OriginValidationMiddleware, SecurityError, VerifiedIdentityMiddleware, development_context, require_scope
from src.dtc_mcp.tools import BoundedInput, DTCInput, DTCListInput, ToolService, TrendInput, VehicleFaultInput, VehicleInput
from src.dtc_mcp.vehicle_repository import VehicleRepository
from src.dtc_mcp.validated_sql import SQLPolicy, ValidatedSQLInput, ValidatedSQLService


CORE_TOOLS = (
    "get_fleet_health_summary", "get_top_dtcs", "get_fault_trends", "get_vehicle_health",
    "get_vehicle_faults", "get_dtc_fleet_impact", "get_dtc_cooccurrence", "get_maintenance_priority",
)


@asynccontextmanager
async def _lifespan(_server):
    yield {"version": __version__}


def create_server(*, settings: DTCSettings | None = None, contexts: ContextProvider | None = None, executor: RepositoryExecutor | None = None) -> FastMCP:
    settings = settings or DTCSettings.from_env()
    if contexts is None:
        try:
            default_context = development_context(settings) if settings.transport == "stdio" else None
        except SecurityError:
            default_context = None
        contexts = ContextProvider(default_context)
    executor = executor or RepositoryExecutor(settings=settings)
    schema = SchemaRepository()
    service = ToolService(contexts, FleetRepository(executor), VehicleRepository(executor), DTCRepository(executor), MaintenanceRepository(executor), schema, settings.transport)
    sql_service = ValidatedSQLService(executor, SQLPolicy(settings))
    mcp = FastMCP(
        "TAABI DTC Analytics MCP",
        instructions="Read-only, tenant-scoped DTC analytics. Prefer domain tools; dynamic SQL is a disabled-by-default fallback.",
        log_level="WARNING",
        lifespan=_lifespan,
        stateless_http=True,
        json_response=True,
    )

    @mcp.tool(description="Report MCP runtime readiness and registered capability counts. Never returns credentials or network secrets.")
    def get_dtc_mcp_server_status() -> ToolResponse[dict[str, Any]]:
        data = {"name": "TAABI DTC Analytics MCP", "version": __version__, "transport": settings.transport, "environment": settings.environment, "database_ready": "ready" if executor.ready() else "unavailable", "registered_tool_count": len(CORE_TOOLS) + 6, "dynamic_sql_enabled": settings.dynamic_sql_enabled, "capabilities": ["domain_tools", "schema_resources", "tenant_isolation", "evidence", "validated_sql"]}
        now = datetime.now(timezone.utc)
        evidence = EvidenceMetadata(source="runtime", tables=(), query_type="server_status", query_hash=hashlib.sha256(b"server_status").hexdigest(), filters_applied={}, data_freshness="current", scope_ref="diagnostic", as_of=now, row_count=1, truncated=False, effective_limit=1, duration_ms=0, trace_id="diagnostic")
        return ToolResponse(ok=True, tool_name="get_dtc_mcp_server_status", data=data, row_count=1, evidence=evidence, request_id="diagnostic", status=ToolStatus.SUCCESS)

    def catalog_result(context, name: str, value: Any, tables: tuple[str, ...] = ()) -> RepositoryResult:
        rows = value if isinstance(value, list) else [value]
        query_hash = hashlib.sha256(name.encode()).hexdigest()
        evidence = EvidenceMetadata(source="approved_catalog", tables=tables, query_type=name, query_hash=query_hash, filters_applied={"allowlist": True}, data_freshness="versioned_catalog", scope_ref=hashlib.sha256(context.tenant_id.encode()).hexdigest()[:16], as_of=datetime.now(timezone.utc), row_count=len(rows), truncated=False, effective_limit=max(1, len(rows)), duration_ms=0, trace_id=context.trace_id)
        metadata = QueryMetadata(query_hash=query_hash, tables=tables, row_count=len(rows), truncated=False, effective_limit=max(1, len(rows)), execution_latency_ms=0)
        return RepositoryResult(rows=rows, metadata=metadata, evidence=evidence)

    @mcp.tool(description="Return the authorized tenant's current fleet health snapshot. Use for fleet KPIs; not for individual vehicles.")
    def get_fleet_health_summary():
        return service.get_fleet_health_summary()

    @mcp.tool(description="Return the authorized tenant's most frequent DTCs in deterministic order. Use for fleet DTC ranking.")
    def get_top_dtcs(limit: int = 10):
        try:
            return service.get_top_dtcs(BoundedInput(limit=limit))
        except ValidationError:
            return service.invalid("get_top_dtcs")

    @mcp.tool(description="Return bounded daily fleet fault trends for the authorized tenant. Use for time-series changes, not raw events.")
    def get_fault_trends(days: int = 30, limit: int = 200):
        try:
            return service.get_fault_trends(TrendInput(days=days, limit=limit))
        except ValidationError:
            return service.invalid("get_fault_trends")

    @mcp.tool(description="Return health details for one vehicle owned by the authorized tenant. Supply exactly one vehicle identifier.")
    def get_vehicle_health(uniqueid: str | None = None, vehicle_number: str | None = None):
        try:
            return service.get_vehicle_health(VehicleInput(uniqueid=uniqueid, vehicle_number=vehicle_number))
        except ValidationError:
            return service.invalid("get_vehicle_health")

    @mcp.tool(description="Return bounded fault episodes for one authorized vehicle. Use unresolved_only for currently active faults.")
    def get_vehicle_faults(uniqueid: str | None = None, vehicle_number: str | None = None, days: int = 90, limit: int = 50, unresolved_only: bool = False):
        try:
            return service.get_vehicle_faults(VehicleFaultInput(uniqueid=uniqueid, vehicle_number=vehicle_number, days=days, limit=limit, unresolved_only=unresolved_only))
        except ValidationError:
            return service.invalid("get_vehicle_faults")

    @mcp.tool(description="Return fleet impact metrics for DTCs within the authorized tenant. Optionally filter one validated DTC code.")
    def get_dtc_fleet_impact(dtc_code: str | None = None, limit: int = 20):
        try:
            return service.get_dtc_fleet_impact(DTCListInput(dtc_code=dtc_code, limit=limit))
        except ValidationError:
            return service.invalid("get_dtc_fleet_impact")

    @mcp.tool(description="Return DTC pair co-occurrence within the authorized tenant. This shows correlation, not causation.")
    def get_dtc_cooccurrence(dtc_code: str | None = None, limit: int = 20):
        try:
            return service.get_dtc_cooccurrence(DTCListInput(dtc_code=dtc_code, limit=limit))
        except ValidationError:
            return service.invalid("get_dtc_cooccurrence")

    @mcp.tool(description="Return the authorized tenant's bounded maintenance-priority ranking. Operational review is required before action.")
    def get_maintenance_priority(limit: int = 20):
        try:
            return service.get_maintenance_priority(BoundedInput(limit=limit))
        except ValidationError:
            return service.invalid("get_maintenance_priority")

    @mcp.tool(description="List approved DTC analytics tables. This never introspects unrestricted or system tables.")
    def list_dtc_analytics_tables():
        return service._call("list_dtc_analytics_tables", "dtc:schema:read", lambda context: catalog_result(context, "list_dtc_analytics_tables", schema.list_approved_tables()))

    @mcp.tool(description="Return safe metadata for one approved DTC analytics table; unknown tables are rejected.")
    def get_dtc_table_schema(approved_table: str):
        return service._call("get_dtc_table_schema", "dtc:schema:read", lambda context: catalog_result(context, "get_dtc_table_schema", schema.get_approved_table_schema(approved_table), (approved_table,)), {"approved_table": approved_table})

    @mcp.tool(description="Return the governed business definition for one approved DTC metric.")
    def get_dtc_metric_definition(metric_name: str):
        return service._call("get_dtc_metric_definition", "dtc:schema:read", lambda context: catalog_result(context, "get_dtc_metric_definition", schema.get_metric_definition(metric_name)), {"metric_name": metric_name})

    @mcp.tool(description="Return global reference information for one validated DTC code. It does not reveal tenant data.")
    def get_dtc_code_info(dtc_code: str):
        try:
            value = DTCInput(dtc_code=dtc_code)
            return service._call("get_dtc_code_info", "dtc:schema:read", lambda context: service.dtcs.get_dtc_code_info(context, dtc_code=value.dtc_code), value.model_dump())
        except ValidationError:
            return service.invalid("get_dtc_code_info")

    @mcp.tool(description="Restricted fallback for approved read-only ClickHouse questions not covered by domain tools. Disabled by default; SQL is AST-validated and tenant scope is server-injected.")
    def run_validated_dtc_sql(question_or_reason: str, sql: str, maximum_rows: int | None = None):
        try:
            values = ValidatedSQLInput(question_or_reason=question_or_reason, sql=sql, maximum_rows=maximum_rows)
            return service._call("run_validated_dtc_sql", "dtc:sql:execute", lambda context: sql_service.run(values, context), values.model_dump())
        except ValidationError:
            return service.invalid("run_validated_dtc_sql")

    register_resources(mcp, schema, list(CORE_TOOLS))
    mcp._dtc_dependencies = {"settings": settings, "contexts": contexts, "executor": executor, "service": service, "schema": schema, "sql_service": sql_service}
    return mcp


def create_http_app(mcp: FastMCP | None = None) -> Starlette:
    mcp = mcp or create_server(settings=DTCSettings.from_env())
    dependencies = mcp._dtc_dependencies
    settings = dependencies["settings"]

    async def health(_request):
        return JSONResponse({"status": "ok", "service": "TAABI DTC Analytics MCP", "version": __version__})

    async def ready(_request):
        database_ready = dependencies["executor"].ready()
        return JSONResponse({"status": "ready" if database_ready else "not_ready", "database": "ready" if database_ready else "unavailable"}, status_code=200 if database_ready else 503)

    streamable_app = mcp.streamable_http_app()

    @asynccontextmanager
    async def http_lifespan(_app):
        async with mcp.session_manager.run():
            yield

    app = Starlette(routes=[Route("/health", health), Route("/ready", ready), Mount("/", app=streamable_app)], lifespan=http_lifespan)
    app.add_middleware(VerifiedIdentityMiddleware, contexts=dependencies["contexts"], environment=settings.environment)
    app.add_middleware(OriginValidationMiddleware, allowed_origins=settings.allowed_origins)
    return app


def main() -> None:
    logging.basicConfig(stream=sys.stderr, level=logging.INFO)
    settings = DTCSettings.from_env()
    mcp = create_server(settings=settings)
    if settings.transport == "stdio":
        mcp.run("stdio")
    else:
        import uvicorn
        if settings.http_workers > 1:
            uvicorn.run("src.dtc_mcp.server:create_http_app", factory=True, host=settings.http_host, port=settings.http_port, workers=settings.http_workers, limit_concurrency=settings.http_limit_concurrency, log_config=None)
        else:
            uvicorn.run(create_http_app(mcp), host=settings.http_host, port=settings.http_port, limit_concurrency=settings.http_limit_concurrency, log_config=None)


if __name__ == "__main__":
    main()
