from __future__ import annotations

from datetime import date, timedelta

from src.clickhouse_utils_v2 import OBD_SOLUTION_TYPES, V2_TABLES
from src.dtc_mcp.models import RepositoryResult, TenantContext, TimeRange
from src.dtc_mcp.repository import RepositoryExecutor


class FleetRepository:
    def __init__(self, executor: RepositoryExecutor):
        self.executor = executor

    def _run(self, query: str, columns: tuple[str, ...], table: str, context: TenantContext, limit: int, query_type: str, **parameters) -> RepositoryResult:
        return self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids), **parameters}, columns=columns, tables=(table,), context=context, limit=limit, query_type=query_type, filters_applied={"tenant_scope": "server", **{k: v for k, v in parameters.items() if k != "tenant_ids"}})

    def get_fleet_health_summary(self, context: TenantContext) -> RepositoryResult:
        table = V2_TABLES["fleet_health_summary"]
        columns = ("total_vehicles", "vehicles_with_active_faults", "vehicles_with_critical_faults", "driver_related_faults", "fleet_health_score", "most_common_dtc", "most_common_system", "active_fault_trend")
        query = f"""
            SELECT sum(total_vehicles), sum(vehicles_with_active_faults),
                   sum(vehicles_with_critical_faults), sum(driver_related_faults),
                   if(sum(total_vehicles) > 0, sum(fleet_health_score * total_vehicles) / sum(total_vehicles), 100.0),
                   argMaxIf(most_common_dtc, vehicles_with_active_faults, most_common_dtc != ''),
                   argMaxIf(most_common_system, vehicles_with_active_faults, most_common_system != ''),
                   argMax(active_fault_trend, vehicles_with_active_faults)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}
            HAVING sum(total_vehicles) > 0
            LIMIT {{query_limit:UInt32}}
        """
        return self._run(query, columns, table, context, 1, "fleet_health_summary")

    def get_top_dtcs(self, context: TenantContext, *, limit: int = 10) -> RepositoryResult:
        table = V2_TABLES["fleet_dtc_distribution"]
        columns = ("dtc_code", "description", "system", "subsystem", "severity_level", "vehicles_affected", "active_vehicles", "total_occurrences", "total_episodes", "avg_resolution_time", "driver_related_count")
        query = f"""
            SELECT dtc_code, anyLast(description), anyLast(system), anyLast(subsystem), max(severity_level),
                   sum(vehicles_affected), sum(active_vehicles), sum(total_occurrences), sum(total_episodes),
                   avg(avg_resolution_time), sum(driver_related_count)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}
            GROUP BY dtc_code
            ORDER BY sum(total_occurrences) DESC, dtc_code ASC
            LIMIT {{query_limit:UInt32}}
        """
        return self._run(query, columns, table, context, limit, "top_dtcs")

    def get_fault_trends(self, context: TenantContext, *, days: int = 30, limit: int = 200) -> RepositoryResult:
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        table = V2_TABLES["fleet_fault_trends"]
        columns = ("date", "active_faults", "critical_faults", "new_faults", "resolved_faults", "driver_related_faults", "fleet_health_score")
        query = f"""
            SELECT date, sum(active_faults), sum(critical_faults), sum(new_faults), sum(resolved_faults),
                   sum(driver_related_faults), avg(fleet_health_score)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND date >= today() - {{days:UInt32}}
            GROUP BY date
            ORDER BY date ASC
            LIMIT {{query_limit:UInt32}}
        """
        result = self._run(query, columns, table, context, limit, "fault_trends", days=days)
        result.evidence.query_window = TimeRange(start=date.today() - timedelta(days=days), end=date.today())
        return result

    def get_high_risk_vehicles(self, context: TenantContext, *, limit: int = 20) -> RepositoryResult:
        table = V2_TABLES["vehicle_health_summary"]
        columns = ("uniqueid", "vehicle_number", "vehicle_health_score", "active_fault_count", "critical_fault_count", "episodes_last_30_days")
        return self._run(f"SELECT {','.join(columns)} FROM {table} WHERE clientLoginId IN {{tenant_ids:Array(String)}} ORDER BY critical_fault_count DESC, active_fault_count DESC, vehicle_health_score ASC, uniqueid ASC LIMIT {{query_limit:UInt32}}", columns, table, context, limit, "high_risk_vehicles")

    def get_system_health(self, context: TenantContext, *, limit: int = 50) -> RepositoryResult:
        table = V2_TABLES["fleet_system_health"]
        columns = ("system", "vehicles_affected", "active_faults", "critical_faults", "risk_score", "trend")
        query = f"""
            SELECT multiIf(system = '' OR system = 'nan', 'other', system) AS sys,
                   sum(vehicles_affected), sum(active_faults), sum(critical_faults),
                   avg(risk_score), anyLast(trend)
            FROM {table}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}
            GROUP BY sys
            ORDER BY avg(risk_score) DESC, sys ASC
            LIMIT {{query_limit:UInt32}}
        """
        return self._run(query, columns, table, context, limit, "system_health")

    def get_fleet_kpis(self, context: TenantContext, *, days: int = 30) -> RepositoryResult:
        days = min(max(1, days), self.executor.settings.max_lookback_days)
        health = V2_TABLES["vehicle_health_summary"]
        faults = V2_TABLES["vehicle_fault_master"]
        columns = ("total_vehicles", "vehicles_with_dtcs", "critical_vehicles", "avg_resolution_days", "fleet_health_score", "maintenance_due", "total_dtc_alerts", "critical_alerts")
        query = f"""
            SELECT count(),
              (SELECT uniqExactIf(uniqueid, is_resolved = 0) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}}),
              (SELECT uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}}),
              (SELECT avgIf(resolution_time_sec / 86400.0, is_resolved = 1 AND resolution_time_sec > 0) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}} AND event_date >= today() - {{days:UInt32}}),
              avg(vehicle_health_score),
              (SELECT uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}}),
              (SELECT sumIf(occurrence_count, event_date >= today() - {{days:UInt32}}) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}}),
              (SELECT countIf(severity_level >= 3 AND event_date >= today() - {{days:UInt32}}) FROM {faults} WHERE clientLoginId IN {{tenant_ids:Array(String)}})
            FROM {health}
            WHERE clientLoginId IN {{tenant_ids:Array(String)}}
            HAVING count() > 0
            LIMIT {{query_limit:UInt32}}
        """
        return self.executor.execute(query, parameters={"tenant_ids": list(context.allowed_customer_ids), "days": days}, columns=columns, tables=(health, faults), context=context, limit=1, query_type="fleet_kpis", filters_applied={"tenant_scope": "server", "days": days})

    def get_customer_overview(self, context: TenantContext, *, limit: int = 50) -> RepositoryResult:
        health = V2_TABLES["vehicle_health_summary"]
        faults = V2_TABLES["vehicle_fault_master"]
        master = V2_TABLES["vehicle_master"]
        columns = ("customer_name", "vehicle_count", "active_fault_vehicles", "critical_fault_vehicles", "avg_health_score")
        query = f"""
            SELECT ifNull(nullIf(h.customer_name, ''), 'Unassigned') AS cust_name,
                   count() AS vehicle_count,
                   countIf(h.active_fault_count > 0) AS active_fault_vehicles,
                   countIf(h.critical_fault_count > 0) AS critical_fault_vehicles,
                   avg(h.vehicle_health_score) AS avg_health_score
            FROM {health} AS h
            INNER JOIN (
                SELECT DISTINCT f.clientLoginId
                FROM {faults} AS f
                INNER JOIN {master} AS m ON m.uniqueid = f.uniqueid AND m.clientLoginId = f.clientLoginId
                WHERE f.clientLoginId IN {{tenant_ids:Array(String)}}
                  AND m.solutionType IN {{obd_solution_types:Array(String)}}
            ) AS eligible ON eligible.clientLoginId = h.clientLoginId
            WHERE h.clientLoginId IN {{tenant_ids:Array(String)}}
            GROUP BY cust_name
            ORDER BY vehicle_count DESC, cust_name ASC
            LIMIT {{query_limit:UInt32}}
        """
        return self.executor.execute(
            query,
            parameters={"tenant_ids": list(context.allowed_customer_ids), "obd_solution_types": list(OBD_SOLUTION_TYPES)},
            columns=columns, tables=(health, faults, master), context=context, limit=limit,
            query_type="customer_overview", filters_applied={"tenant_scope": "server", "vehicle_scope": "obd_fault_master"},
        )
