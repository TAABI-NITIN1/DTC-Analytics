from __future__ import annotations

import hashlib
from typing import Any, Callable

from pydantic import Field

from src.dtc_mcp.dtc_repository import DTCRepository
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.maintenance_repository import MaintenanceRepository
from src.dtc_mcp.models import ErrorCode, RepositoryResult, StrictModel, ToolError, ToolResponse, ToolStatus
from src.dtc_mcp.observability import audited_call
from src.dtc_mcp.repository import RepositoryError
from src.dtc_mcp.schema_repository import SchemaRepository
from src.dtc_mcp.security import ContextProvider, SecurityError, require_scope
from src.dtc_mcp.vehicle_repository import VehicleRepository


class BoundedInput(StrictModel):
    limit: int = Field(default=20, ge=1, le=200)


class TrendInput(StrictModel):
    days: int = Field(default=30, ge=1, le=3650)
    limit: int = Field(default=200, ge=1, le=200)


class VehicleInput(StrictModel):
    uniqueid: str | None = Field(default=None, min_length=1, max_length=128)
    vehicle_number: str | None = Field(default=None, min_length=1, max_length=128)


class VehicleFaultInput(VehicleInput):
    days: int = Field(default=90, ge=1, le=3650)
    limit: int = Field(default=50, ge=1, le=200)
    unresolved_only: bool = False


class DTCInput(StrictModel):
    dtc_code: str = Field(min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")


class DTCListInput(StrictModel):
    dtc_code: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    limit: int = Field(default=20, ge=1, le=200)


class ToolService:
    def __init__(self, contexts: ContextProvider, fleet: FleetRepository, vehicles: VehicleRepository, dtcs: DTCRepository, maintenance: MaintenanceRepository, schema: SchemaRepository, transport: str = "internal"):
        self.contexts = contexts
        self.fleet = fleet
        self.vehicles = vehicles
        self.dtcs = dtcs
        self.maintenance = maintenance
        self.schema = schema
        self.transport = transport

    def invalid(self, tool_name: str, message: str = "Tool input is invalid") -> ToolResponse[list[dict[str, Any]]]:
        context = self.contexts.default
        return ToolResponse(ok=False, tool_name=tool_name, row_count=0, error=ToolError(code=ErrorCode.INVALID_ARGUMENT, message=message), request_id=context.request_id if context else "unavailable", status=ToolStatus.ERROR)

    def _call(self, tool_name: str, scope: str, callback: Callable[[Any], RepositoryResult], parameters: dict[str, Any] | None = None) -> ToolResponse[list[dict[str, Any]]]:
        try:
            try:
                context = self.contexts.get()
            except SecurityError as exc:
                with audited_call(tool_name, request_id="unavailable", trace_id="unavailable", scope_ref="unavailable", transport=self.transport) as audit:
                    audit.update(status="rejected", error_code=exc.code.value)
                raise
            scope_ref = hashlib.sha256(context.tenant_id.encode()).hexdigest()[:16]
            with audited_call(tool_name, request_id=context.request_id, trace_id=context.trace_id, scope_ref=scope_ref, user_id=context.user_id, tenant_id=context.tenant_id, session_id=context.session_id, ai_run_id=context.ai_run_id, parameters=parameters, roles=tuple(sorted(context.roles)), scopes=tuple(sorted(context.scopes)), transport=self.transport) as audit:
                require_scope(context, scope)
                result = callback(context)
                audit.update(row_count=result.metadata.row_count, truncated=result.metadata.truncated, cache_status=result.evidence.cache_status, cache_key_hash=result.evidence.cache_key_hash, invalidation_version=result.evidence.cache_invalidation_version, cache_error=result.evidence.cache_error, tables_accessed=result.evidence.tables, query_hash=result.evidence.query_hash, database_latency_ms=result.metadata.execution_latency_ms)
            return ToolResponse(
                ok=True,
                tool_name=tool_name,
                data=result.rows,
                row_count=result.metadata.row_count,
                truncated=result.metadata.truncated,
                evidence=result.evidence,
                limitations=("Data freshness follows the latest completed analytics producer checkpoint.",),
                request_id=context.request_id,
                status=ToolStatus.EMPTY if not result.rows else ToolStatus.SUCCESS,
            )
        except (RepositoryError, SecurityError) as exc:
            context = self.contexts.default
            return ToolResponse(ok=False, tool_name=tool_name, row_count=0, error=ToolError(code=exc.code, message=str(exc), retryable=getattr(exc, "retryable", False)), request_id=context.request_id if context else "unavailable", status=ToolStatus.ERROR)
        except ValueError as exc:
            context = self.contexts.default
            return ToolResponse(ok=False, tool_name=tool_name, row_count=0, error=ToolError(code=ErrorCode.INVALID_ARGUMENT, message=str(exc)), request_id=context.request_id if context else "unavailable", status=ToolStatus.ERROR)

    def get_fleet_health_summary(self) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_fleet_health_summary", "dtc:fleet:read", self.fleet.get_fleet_health_summary)

    def get_top_dtcs(self, values: BoundedInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_top_dtcs", "dtc:fleet:read", lambda context: self.fleet.get_top_dtcs(context, limit=values.limit), values.model_dump())

    def get_fault_trends(self, values: TrendInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_fault_trends", "dtc:fleet:read", lambda context: self.fleet.get_fault_trends(context, days=values.days, limit=values.limit), values.model_dump())

    def get_vehicle_health(self, values: VehicleInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_vehicle_health", "dtc:vehicle:read", lambda context: self.vehicles.get_vehicle_health(context, uniqueid=values.uniqueid, vehicle_number=values.vehicle_number), values.model_dump())

    def get_vehicle_faults(self, values: VehicleFaultInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_vehicle_faults", "dtc:vehicle:read", lambda context: self.vehicles.get_vehicle_faults(context, uniqueid=values.uniqueid, vehicle_number=values.vehicle_number, days=values.days, limit=values.limit, unresolved_only=values.unresolved_only), values.model_dump())

    def get_dtc_fleet_impact(self, values: DTCListInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_dtc_fleet_impact", "dtc:fleet:read", lambda context: self.dtcs.get_dtc_fleet_impact(context, dtc_code=values.dtc_code, limit=values.limit), values.model_dump())

    def get_dtc_cooccurrence(self, values: DTCListInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_dtc_cooccurrence", "dtc:fleet:read", lambda context: self.dtcs.get_dtc_cooccurrence(context, dtc_code=values.dtc_code, limit=values.limit), values.model_dump())

    def get_maintenance_priority(self, values: BoundedInput) -> ToolResponse[list[dict[str, Any]]]:
        return self._call("get_maintenance_priority", "dtc:maintenance:read", lambda context: self.maintenance.get_maintenance_priority(context, limit=values.limit), values.model_dump())
