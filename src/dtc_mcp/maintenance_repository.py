from __future__ import annotations

from src.clickhouse_utils_v2 import V2_TABLES
from src.dtc_mcp.models import RepositoryResult, TenantContext
from src.dtc_mcp.repository import RepositoryExecutor


class MaintenanceRepository:
    def __init__(self, executor: RepositoryExecutor):
        self.executor = executor

    def get_maintenance_priority(self, context: TenantContext, *, limit: int = 20) -> RepositoryResult:
        table = V2_TABLES["maintenance_priority"]
        columns = ("uniqueid", "vehicle_number", "dtc_code", "description", "severity_level", "fault_duration_sec", "episodes_last_30_days", "maintenance_priority_score", "recommended_action")
        return self.executor.execute(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} ORDER BY maintenance_priority_score DESC, uniqueid ASC, dtc_code ASC LIMIT {{query_limit:UInt32}}", parameters={"tenant_ids": list(context.allowed_customer_ids)}, columns=columns, tables=(table,), context=context, limit=limit, query_type="maintenance_priority", filters_applied={"tenant_scope": "server"})

    def get_maintenance_recommendations(self, context: TenantContext, *, limit: int = 20) -> RepositoryResult:
        table = V2_TABLES["maintenance_priority"]
        details = V2_TABLES["dtc_master"]
        columns = ("uniqueid", "dtc_code", "severity_level", "subsystem", "recommended_action")
        query = f"""
            SELECT mp.uniqueid, mp.dtc_code, mp.severity_level, ifNull(d.subsystem, ''), mp.recommended_action
            FROM {table} AS mp
            LEFT JOIN {details} AS d ON d.dtc_code = mp.dtc_code
            WHERE mp.clientLoginId IN {{tenant_ids:Array(String)}}
            ORDER BY mp.maintenance_priority_score DESC, mp.uniqueid ASC, mp.dtc_code ASC
            LIMIT {{query_limit:UInt32}}
        """
        return self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids)}, columns=columns, tables=(table, details), context=context, limit=limit, query_type="maintenance_recommendations", filters_applied={"tenant_scope": "server"})
