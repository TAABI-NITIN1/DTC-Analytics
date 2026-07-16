from __future__ import annotations

from datetime import date, timedelta

from src.clickhouse_utils_v2 import OBD_SOLUTION_TYPES, V2_TABLES
from src.dtc_mcp.models import RepositoryResult, TenantContext, TimeRange
from src.dtc_mcp.repository import RepositoryExecutor


class VehicleRepository:
    def __init__(self, executor: RepositoryExecutor):
        self.executor = executor

    def _identity(self, uniqueid: str | None, vehicle_number: str | None) -> tuple[str, dict]:
        if bool(uniqueid) == bool(vehicle_number):
            raise ValueError("Provide exactly one of uniqueid or vehicle_number")
        return ("uniqueid = {vehicle_id:String}", {"vehicle_id": uniqueid}) if uniqueid else ("vehicle_number = {vehicle_id:String}", {"vehicle_id": vehicle_number})

    def list_authorized_customer_names(self, context: TenantContext, *, limit: int = 500) -> RepositoryResult:
        master = V2_TABLES["vehicle_master"]
        faults = V2_TABLES["vehicle_fault_master"]
        return self.executor.execute(
            f"""
            SELECT DISTINCT m.customer_name
            FROM {master} AS m
            INNER JOIN {faults} AS f ON f.uniqueid = m.uniqueid AND f.clientLoginId = m.clientLoginId
            WHERE m.clientLoginId IN {{tenant_ids:Array(String)}}
              AND m.solutionType IN {{obd_solution_types:Array(String)}}
              AND m.customer_name != ''
            ORDER BY m.customer_name
            LIMIT {{query_limit:UInt32}}
            """,
            parameters={"tenant_ids": list(context.allowed_customer_ids), "obd_solution_types": list(OBD_SOLUTION_TYPES)},
            columns=("customer_name",), tables=(master, faults), context=context, limit=limit,
            query_type="authorized_customer_names", filters_applied={"tenant_scope": "server", "vehicle_scope": "obd_fault_master"},
        )

    def get_authorized_customer_ids(self, context: TenantContext, customer_name: str, *, limit: int = 50) -> RepositoryResult:
        master = V2_TABLES["vehicle_master"]
        faults = V2_TABLES["vehicle_fault_master"]
        return self.executor.execute(
            f"""
            SELECT DISTINCT toString(m.clientLoginId)
            FROM {master} AS m
            INNER JOIN {faults} AS f ON f.uniqueid = m.uniqueid AND f.clientLoginId = m.clientLoginId
            WHERE m.clientLoginId IN {{tenant_ids:Array(String)}}
              AND m.solutionType IN {{obd_solution_types:Array(String)}}
              AND m.customer_name = {{customer_name:String}}
            ORDER BY toString(m.clientLoginId)
            LIMIT {{query_limit:UInt32}}
            """,
            parameters={"tenant_ids": list(context.allowed_customer_ids), "obd_solution_types": list(OBD_SOLUTION_TYPES), "customer_name": customer_name},
            columns=("clientLoginId",), tables=(master, faults), context=context, limit=limit,
            query_type="authorized_customer_scope", filters_applied={"tenant_scope": "server", "vehicle_scope": "obd_fault_master", "customer_name": "bound"},
        )

    def get_vehicle_health(self, context: TenantContext, *, uniqueid: str | None = None, vehicle_number: str | None = None) -> RepositoryResult:
        predicate, params = self._identity(uniqueid, vehicle_number)
        table = V2_TABLES["vehicle_health_summary"]
        columns = ("uniqueid", "vehicle_number", "customer_name", "active_fault_count", "critical_fault_count", "total_episodes", "episodes_last_30_days", "avg_resolution_time", "last_fault_ts", "vehicle_health_score", "driver_related_faults", "most_common_dtc", "has_engine_issue", "has_emission_issue", "has_safety_issue", "has_electrical_issue")
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND {predicate} ORDER BY uniqueid LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids), **params}, columns=columns, tables=(table,), context=context, limit=1, query_type="vehicle_health", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound"})

    def get_vehicle_overview(self, context: TenantContext, *, uniqueid: str) -> RepositoryResult:
        health = V2_TABLES["vehicle_health_summary"]
        master = V2_TABLES["vehicle_master"]
        columns = ("uniqueid", "vehicle_number", "vehicle_model", "vehicle_type", "customer_name", "vehicle_health_score", "active_fault_count", "critical_fault_count")
        query = f"""
            SELECT h.uniqueid, h.vehicle_number, ifNull(m.model, ''), ifNull(m.vehicle_type, ''),
                   h.customer_name, h.vehicle_health_score, h.active_fault_count, h.critical_fault_count
            FROM {health} AS h
            LEFT JOIN {master} AS m ON m.uniqueid = h.uniqueid AND m.clientLoginId = h.clientLoginId
            WHERE h.clientLoginId IN {{tenant_ids:Array(String)}} AND h.uniqueid = {{vehicle_id:String}}
            ORDER BY h.uniqueid
            LIMIT {{query_limit:UInt32}}
        """
        return self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids), "vehicle_id": uniqueid}, columns=columns, tables=(health, master), context=context, limit=1, query_type="vehicle_overview", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound"})

    def get_vehicle_faults(self, context: TenantContext, *, uniqueid: str | None = None, vehicle_number: str | None = None, days: int = 90, limit: int = 50, unresolved_only: bool = False) -> RepositoryResult:
        predicate, params = self._identity(uniqueid, vehicle_number)
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        table = V2_TABLES["vehicle_fault_master"]
        columns = ("episode_id", "uniqueid", "vehicle_number", "dtc_code", "system", "subsystem", "description", "severity_level", "is_resolved", "event_date", "first_ts", "last_ts", "occurrence_count", "resolution_time_sec", "driver_related", "vehicle_health_score")
        unresolved = " AND is_resolved = 0" if unresolved_only else ""
        result = self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND {predicate} AND event_date >= today() - {{days:UInt32}}{unresolved} ORDER BY event_date DESC, severity_level DESC, episode_id ASC LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids), "days": days, **params}, columns=columns, tables=(table,), context=context, limit=limit, query_type="vehicle_faults", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound", "days": days, "unresolved_only": unresolved_only})
        result.evidence.query_window = TimeRange(start=date.today() - timedelta(days=days), end=date.today())
        return result
    def get_vehicle_fault_timeline(self, context: TenantContext, *, uniqueid: str, days: int = 90, limit: int = 200) -> RepositoryResult:
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        table = V2_TABLES["vehicle_fault_master"]
        columns = ("event_date", "dtc_code", "severity_level", "is_resolved", "occurrence_count", "first_ts", "last_ts")
        result = self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND uniqueid = {{vehicle_id:String}} AND event_date >= today() - {{days:UInt32}} ORDER BY event_date ASC, first_ts ASC, dtc_code ASC LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids), "vehicle_id": uniqueid, "days": days}, columns=columns, tables=(table,), context=context, limit=limit, query_type="vehicle_fault_timeline", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound", "days": days})
        result.evidence.query_window = TimeRange(start=date.today() - timedelta(days=days), end=date.today())
        return result

    def get_vehicle_fault_summary(self, context: TenantContext, *, uniqueid: str, days: int = 90, limit: int = 200) -> RepositoryResult:
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        table = V2_TABLES["vehicle_fault_master"]
        columns = ("dtc_code", "episode_count", "active_episodes", "max_severity", "days_persistence")
        query = f"""
            SELECT dtc_code,
                   count() AS episode_count,
                   countIf(is_resolved = 0) AS active_episodes,
                   max(severity_level) AS max_severity,
                   max(dateDiff('day', toDate(event_date), today())) AS days_persistence
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND uniqueid = {{vehicle_id:String}}
              AND event_date >= today() - {{days:UInt32}}
            GROUP BY dtc_code
            ORDER BY active_episodes DESC, max_severity DESC, episode_count DESC, dtc_code ASC
            LIMIT {{query_limit:UInt32}}
        """
        result = self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids), "vehicle_id": uniqueid, "days": days}, columns=columns, tables=(table,), context=context, limit=limit, query_type="vehicle_fault_summary", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound", "days": days})
        result.evidence.query_window = TimeRange(start=date.today() - timedelta(days=days), end=date.today())
        return result

    def get_vehicle_timeline_summary(self, context: TenantContext, *, uniqueid: str, days: int = 90, limit: int = 200) -> RepositoryResult:
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        table = V2_TABLES["vehicle_fault_master"]
        columns = ("event_date", "active_episodes", "critical_episodes")
        query = f"""
            SELECT event_date, countIf(is_resolved = 0), countIf(severity_level >= 3 AND is_resolved = 0)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND uniqueid = {{vehicle_id:String}}
              AND event_date >= today() - {{days:UInt32}}
            GROUP BY event_date
            ORDER BY event_date
            LIMIT {{query_limit:UInt32}}
        """
        result = self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids), "vehicle_id": uniqueid, "days": days}, columns=columns, tables=(table,), context=context, limit=limit, query_type="vehicle_timeline_summary", filters_applied={"tenant_scope": "server", "vehicle_identifier": "bound", "days": days})
        result.evidence.query_window = TimeRange(start=date.today() - timedelta(days=days), end=date.today())
        return result

    def list_customer_vehicles(self, context: TenantContext, *, limit: int = 500) -> RepositoryResult:
        table = V2_TABLES["vehicle_health_summary"]
        columns = ("uniqueid", "vehicle_number")
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} ORDER BY vehicle_number, uniqueid LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids)}, columns=columns, tables=(table,), context=context, limit=limit, query_type="customer_vehicles", filters_applied={"tenant_scope": "server"})
