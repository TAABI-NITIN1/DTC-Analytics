from __future__ import annotations

from src.clickhouse_utils_v2 import V2_TABLES
from src.dtc_mcp.models import RepositoryResult, TenantContext
from src.dtc_mcp.repository import RepositoryExecutor


class DTCRepository:
    def __init__(self, executor: RepositoryExecutor):
        self.executor = executor

    def _scoped(self, context: TenantContext, table_key: str, columns: tuple[str, ...], query_type: str, *, where: str = "", limit: int = 20, order: str, **params) -> RepositoryResult:
        table = V2_TABLES[table_key]
        suffix = f" AND {where}" if where else ""
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}}{suffix} ORDER BY {order} LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids), **params}, columns=columns, tables=(table,), context=context, limit=limit, query_type=query_type, filters_applied={"tenant_scope": "server", **{key: "bound" for key in params}})

    def get_dtc_fleet_impact(self, context: TenantContext, *, dtc_code: str | None = None, limit: int = 20) -> RepositoryResult:
        columns = ("dtc_code", "system", "subsystem", "vehicles_affected", "active_vehicles", "avg_resolution_time", "driver_related_ratio", "fleet_risk_score")
        table = V2_TABLES["dtc_fleet_impact"]
        predicate = " AND dtc_code = {dtc_code:String}" if dtc_code else ""
        query = f"""
            SELECT dtc_code, anyLast(system), anyLast(subsystem), sum(vehicles_affected), sum(active_vehicles),
                   avg(avg_resolution_time), avg(driver_related_ratio), avg(fleet_risk_score)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}{predicate}
            GROUP BY dtc_code
            ORDER BY avg(fleet_risk_score) DESC, dtc_code ASC
            LIMIT {{query_limit:UInt32}}
        """
        params = {"tenant_ids": list(context.allowed_customer_ids), **({"dtc_code": dtc_code} if dtc_code else {})}
        return self.executor.execute(query, parameters=params, columns=columns, tables=(table,), context=context, limit=limit, query_type="dtc_fleet_impact", filters_applied={"tenant_scope": "server", **({"dtc_code": "bound"} if dtc_code else {})})

    def get_dtc_affected_vehicles(self, context: TenantContext, *, dtc_code: str, limit: int = 100) -> RepositoryResult:
        columns = ("uniqueid", "vehicle_number", "count() AS episode_count", "countIf(is_resolved = 0) AS active_episodes", "max(severity_level) AS max_severity")
        table = V2_TABLES["vehicle_fault_master"]
        output = ("uniqueid", "vehicle_number", "episode_count", "active_episodes", "max_severity")
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND dtc_code = {{dtc_code:String}} GROUP BY uniqueid,vehicle_number ORDER BY active_episodes DESC, episode_count DESC, uniqueid ASC LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids), "dtc_code": dtc_code}, columns=output, tables=(table,), context=context, limit=limit, query_type="dtc_affected_vehicles", filters_applied={"tenant_scope": "server", "dtc_code": "bound"})

    def get_dtc_cooccurrence(self, context: TenantContext, *, dtc_code: str | None = None, limit: int = 20) -> RepositoryResult:
        columns = ("dtc_code_a", "dtc_code_b", "cooccurrence_count", "vehicles_affected", "avg_time_gap_sec", "last_seen_ts")
        table = V2_TABLES["dtc_cooccurrence"]
        predicate = " AND (dtc_code_a = {dtc_code:String} OR dtc_code_b = {dtc_code:String})" if dtc_code else ""
        query = f"""
            SELECT dtc_code_a, dtc_code_b, sum(cooccurrence_count), sum(vehicles_affected),
                   avg(avg_time_gap_sec), max(last_seen_ts)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}{predicate}
            GROUP BY dtc_code_a, dtc_code_b
            ORDER BY sum(cooccurrence_count) DESC, dtc_code_a ASC, dtc_code_b ASC
            LIMIT {{query_limit:UInt32}}
        """
        params = {"tenant_ids": list(context.allowed_customer_ids), **({"dtc_code": dtc_code} if dtc_code else {})}
        return self.executor.execute(query, parameters=params, columns=columns, tables=(table,), context=context, limit=limit, query_type="dtc_cooccurrence", filters_applied={"tenant_scope": "server", **({"dtc_code": "bound"} if dtc_code else {})})

    def get_dtc_code_info(self, context: TenantContext, *, dtc_code: str) -> RepositoryResult:
        table = V2_TABLES["dtc_master"]
        columns = ("dtc_code", "system", "subsystem", "description", "primary_cause", "secondary_causes", "symptoms", "impact_if_unresolved", "fuel_mileage_impact", "severity_level", "safety_risk_level", "action_required", "repair_complexity", "estimated_repair_hours", "driver_related", "driver_behaviour_category", "fleet_management_action", "recommended_preventive_action")
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE dtc_code = {{dtc_code:String}} ORDER BY dtc_code LIMIT {{query_limit:UInt32}}", parameters={"dtc_code": dtc_code}, columns=columns, tables=(table,), context=context, limit=1, query_type="dtc_code_info", filters_applied={"dtc_code": "bound", "global_reference_data": True})
