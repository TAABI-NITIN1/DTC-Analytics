from __future__ import annotations

from datetime import date, datetime
import math
from typing import List, Optional

import strawberry
from graphql import GraphQLError
from strawberry.extensions import SchemaExtension
from strawberry.types import Info

from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES
from src.dtc_mcp.dtc_repository import DTCRepository
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.maintenance_repository import MaintenanceRepository
from src.dtc_mcp.models import TenantContext
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor
from src.dtc_mcp.security import SecurityError, require_scope
from src.dtc_mcp.vehicle_repository import VehicleRepository


_CUSTOMER_SCOPED_FIELDS = {
    "customer_vehicles", "fleet_kpis", "fleet_overview", "fleet_trend", "top_risk_vehicles",
    "dtc_summary", "top_dtc_codes", "severity_breakdown", "dtc_trend", "dtc_alerts_trend",
    "dtc_affected_vehicles", "selected_dtc_kpis", "selected_dtc_weekly_trend",
    "selected_dtc_cooccurrence", "selected_dtc_vehicles", "vehicles_with_dtcs", "vehicle_faults",
    "vehicle_timeline", "maintenance_recommendations", "fleet_health_snap", "fleet_system_health",
    "fleet_fault_trends", "dtc_fleet_impact", "dtc_cooccurrence", "enhanced_maintenance",
}
_CUSTOMER_SCOPED_GRAPHQL_FIELDS = {name.split("_")[0] + "".join(part.title() for part in name.split("_")[1:]) for name in _CUSTOMER_SCOPED_FIELDS}


class TenantScopeExtension(SchemaExtension):
    def resolve(self, next_, root, info, *args, **kwargs):
        context = info.context if isinstance(info.context, dict) else {}
        tenant = context.get("tenant_context")
        if not isinstance(tenant, TenantContext):
            raise GraphQLError("UNAUTHENTICATED")
        requested = kwargs.get("customerName", kwargs.get("customer_name"))
        if requested:
            _customer_scope(info, tenant, str(requested))
        return next_(root, info, *args, **kwargs)


def _customer_scope(info: Info, tenant: TenantContext, customer_name: str) -> TenantContext:
    context = info.context if isinstance(info.context, dict) else {}
    cache = context.setdefault("dtc_customer_scope_cache", {})
    if customer_name in cache:
        return cache[customer_name]
    rows = _rows(lambda: _repositories(info)["vehicles"].get_authorized_customer_ids(tenant, customer_name))
    customer_ids = tuple(str(row["clientLoginId"]) for row in rows if row.get("clientLoginId") is not None)
    if not customer_ids:
        raise GraphQLError("FORBIDDEN")
    scoped = tenant.model_copy(update={"customer_id": customer_name, "allowed_customer_ids": customer_ids})
    cache[customer_name] = scoped
    return scoped


def _tenant(info: Info, scope: str, customer_name: str | None = None) -> TenantContext:
    context = info.context if isinstance(info.context, dict) else {}
    tenant = context.get("tenant_context")
    if not isinstance(tenant, TenantContext):
        raise GraphQLError("UNAUTHENTICATED")
    try:
        require_scope(tenant, scope)
    except SecurityError as exc:
        raise GraphQLError(exc.code.value) from exc
    return _customer_scope(info, tenant, customer_name) if customer_name else tenant


def _repositories(info: Info) -> dict[str, object]:
    context = info.context if isinstance(info.context, dict) else {}
    repositories = context.get("dtc_repositories")
    if repositories is None:
        executor = RepositoryExecutor()
        repositories = {
            "fleet": FleetRepository(executor),
            "vehicles": VehicleRepository(executor),
            "dtcs": DTCRepository(executor),
            "maintenance": MaintenanceRepository(executor),
        }
        context["dtc_repositories"] = repositories
    return repositories


def _rows(call):
    try:
        return call().rows
    except RepositoryError as exc:
        raise GraphQLError(exc.code.value) from exc


# def _query_rows(sql: str, params: dict | None = None):
#     client = get_clickhouse_client()
#     return client.execute(sql, params or {})
def _query_rows(sql: str, params: dict | None = None):
    client = get_clickhouse_client()
    result = client.execute(sql, params or {})
    return result


def _safe_float(value, default: float = 0.0) -> float:
    """Convert DB numeric values to GraphQL-safe finite floats."""
    try:
        parsed = float(value)
    except Exception:
        return default
    if not math.isfinite(parsed):
        return default
    return parsed


def _safe_round(value, ndigits: int, default: float = 0.0) -> float:
    return round(_safe_float(value, default=default), ndigits)


def _table_exists(table_name: str) -> bool:
    rows = _query_rows(
        """
        SELECT count()
        FROM system.tables
        WHERE database = currentDatabase()
          AND name = %(table)s
        """,
        {'table': table_name},
    )
    return bool(rows and int(rows[0][0]) > 0)


def _customer_and(customer_name: str | None, params: dict) -> str:
    """Simple AND customer_name = ? clause (no alias)."""
    if not customer_name:
        return ''
    params['_cust'] = customer_name
    return " AND customer_name = %(_cust)s"


def _customer_client_and(customer_name: str | None, params: dict, client_expr: str = 'clientLoginId') -> str:
    """Filter clientLoginId-keyed tables by customer_name via vehicle_master."""
    if not customer_name:
        return ''
    params['_cust'] = customer_name
    vm = V2_TABLES['vehicle_master']
    return (
        f" AND {client_expr} IN ("
        f"SELECT DISTINCT clientLoginId FROM {vm} WHERE customer_name = %(_cust)s"
        f")"
    )


def _fault_counts_subquery(customer_name: str | None, params: dict) -> str:
    vfm = V2_TABLES['vehicle_fault_master']
    where = _customer_and(customer_name, params)
    return f"""
        SELECT
            uniqueid,
            countIf(is_resolved = 0) AS active_fault_count,
            countIf(is_resolved = 0 AND severity_level >= 3) AS critical_fault_count
        FROM {vfm}
        WHERE 1=1 {where}
        GROUP BY uniqueid
    """


# -- Fleet-level types --

@strawberry.type
class FleetOverview:
    event_date: str
    total_vehicles: int
    active_fault_vehicles: int
    critical_fault_vehicles: int
    fleet_health_score: float


@strawberry.type
class FleetKpis:
    total_vehicles: int
    vehicles_with_dtcs: int
    critical_vehicles: int
    avg_resolution_days: float
    fleet_health_score: float
    maintenance_due: int
    total_dtc_alerts: int
    critical_alerts: int


@strawberry.type
class FleetTrendPoint:
    event_date: str
    active_fault_vehicles: int
    critical_fault_vehicles: int
    fleet_health_score: float


@strawberry.type
class RiskVehicle:
    uniqueid: str
    vehicle_number: str
    vehicle_model: str
    customer_name: str
    health_score: float
    active_fault_count: int
    critical_fault_count: int
    longest_active_days: int


# -- DTC-level types --

@strawberry.type
class DtcSummary:
    dtc_code: str
    occurrences: int
    vehicles_affected: int
    avg_persistence: float


@strawberry.type
class TopDtcCode:
    dtc_code: str
    description: str
    occurrences: int
    vehicles_affected: int


@strawberry.type
class SeverityBreakdown:
    level: str
    count: int
    percentage: float


@strawberry.type
class DtcTrendPoint:
    event_date: str
    occurrences: int


@strawberry.type
class DtcAlertsTrendPoint:
    event_date: str
    total_alerts: int
    critical_alerts: int


@strawberry.type
class DtcVehicle:
    uniqueid: str
    episode_count: int
    active_episodes: int
    max_severity: int


@strawberry.type
class SelectedDtcKpis:
    dtc_code: str
    system: str
    subsystem: str
    affected_vehicles: int
    active_episodes: int
    critical_episodes: int
    avg_resolution_days: float
    driver_related_ratio: float
    last_seen_date: str


@strawberry.type
class SelectedDtcWeeklyTrendPoint:
    week_start: str
    occurrences: int
    affected_vehicles: int
    active_episodes: int
    critical_episodes: int


@strawberry.type
class SelectedDtcCooccurrence:
    co_dtc_code: str
    cooccurrence_count: int
    vehicles_affected: int


@strawberry.type
class SelectedDtcVehicle:
    uniqueid: str
    vehicle_number: str
    episode_count: int
    active_episodes: int
    max_severity: int
    last_reported_date: str
    health_score: float


@strawberry.type
class VehicleWithDtcs:
    uniqueid: str
    vehicle_number: str
    customer_name: str
    dtc_codes: list[str]
    status: str
    last_reported: str
    active_count: int
    critical_count: int


# -- Vehicle-level types --

@strawberry.type
class VehicleOverview:
    uniqueid: str
    vehicle_number: str
    vehicle_model: str
    vehicle_type: str
    customer_name: str
    health_score: float
    active_fault_count: int
    critical_fault_count: int


@strawberry.type
class VehicleFault:
    dtc_code: str
    episode_count: int
    active_episodes: int
    max_severity: int
    days_persistence: int


@strawberry.type
class VehicleTimelinePoint:
    event_date: str
    active_episodes: int
    critical_episodes: int


# -- Customer / Maintenance types --

@strawberry.type
class CustomerOverview:
    customer_name: str
    vehicle_count: int
    active_fault_vehicles: int
    critical_fault_vehicles: int
    avg_health_score: float


@strawberry.type
class MaintenanceRecommendation:
    uniqueid: str
    dtc_code: str
    severity_level: int
    subsystem: str
    recommendation: str


@strawberry.type
class CustomerVehicle:
    uniqueid: str
    vehicle_number: str


# -- New V2 analytics types --

@strawberry.type
class FleetSystemHealth:
    system: str
    vehicles_affected: int
    active_faults: int
    critical_faults: int
    risk_score: float
    trend: str


@strawberry.type
class FleetFaultTrendPoint:
    event_date: str
    active_faults: int
    new_faults: int
    resolved_faults: int
    driver_related_faults: int
    fleet_health_score: float


@strawberry.type
class DtcFleetImpact:
    dtc_code: str
    system: str
    subsystem: str
    vehicles_affected: int
    active_vehicles: int
    avg_resolution_time: float
    driver_related_ratio: float
    fleet_risk_score: float


@strawberry.type
class DtcCooccurrence:
    dtc_code_a: str
    dtc_code_b: str
    cooccurrence_count: int
    vehicles_affected: int
    avg_time_gap_sec: float


@strawberry.type
class DtcDetail:
    dtc_code: str
    system: str
    subsystem: str
    description: str
    severity_level: int
    primary_cause: str
    symptoms: str
    impact_if_unresolved: str
    fuel_mileage_impact: str
    action_required: str
    repair_complexity: str
    estimated_repair_hours: float
    driver_related: bool
    driver_behaviour_category: str
    recommended_preventive_action: str


@strawberry.type
class VehicleHealthDetail:
    uniqueid: str
    vehicle_number: str
    customer_name: str
    vehicle_health_score: float
    active_fault_count: int
    critical_fault_count: int
    total_episodes: int
    episodes_last_30_days: int
    avg_resolution_time: float
    driver_related_faults: int
    most_common_dtc: str
    has_engine_issue: bool
    has_emission_issue: bool
    has_safety_issue: bool
    has_electrical_issue: bool


@strawberry.type
class EnhancedMaintenanceRec:
    uniqueid: str
    vehicle_number: str
    dtc_code: str
    description: str
    severity_level: int
    fault_duration_sec: int
    episodes_last_30_days: int
    maintenance_priority_score: float
    recommended_action: str


@strawberry.type
class FleetHealthSnap:
    total_vehicles: int
    vehicles_with_active_faults: int
    vehicles_with_critical_faults: int
    driver_related_faults: int
    fleet_health_score: float
    most_common_dtc: str
    most_common_system: str
    active_fault_trend: str


@strawberry.type
class Query:

    # -- Customer selectors --

    @strawberry.field
    def customer_names(self, info: Info) -> list[str]:
        """Customer names mapped to the authenticated identity's allowed IDs."""
        tenant = _tenant(info, "dtc:fleet:read")
        rows = _rows(lambda: _repositories(info)["vehicles"].list_authorized_customer_names(tenant))
        return [str(row["customer_name"]) for row in rows]

    @strawberry.field
    def customer_vehicles(self, info: Info, customer_name: str) -> list[CustomerVehicle]:
        """Vehicles belonging to a specific customer (for vehicle dropdown)."""
        tenant = _tenant(info, "dtc:vehicle:read", customer_name)
        rows = _rows(lambda: _repositories(info)["vehicles"].list_customer_vehicles(tenant))
        return [
            CustomerVehicle(uniqueid=str(row["uniqueid"]), vehicle_number=str(row["vehicle_number"] or row["uniqueid"]))
            for row in rows
        ]

    # -- Fleet KPIs (enriched) --

    @strawberry.field
    def fleet_kpis(self, info: Info, days: int = 30, customer_name: Optional[str] = None) -> FleetKpis | None:
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["fleet"].get_fleet_kpis(tenant, days=days))
        if not rows:
            return None
        row = rows[0]
        return FleetKpis(
            total_vehicles=int(row["total_vehicles"] or 0),
            vehicles_with_dtcs=int(row["vehicles_with_dtcs"] or 0),
            critical_vehicles=int(row["critical_vehicles"] or 0),
            avg_resolution_days=_safe_round(row["avg_resolution_days"], 1),
            fleet_health_score=_safe_round(row["fleet_health_score"], 2),
            maintenance_due=int(row["maintenance_due"] or 0),
            total_dtc_alerts=int(row["total_dtc_alerts"] or 0),
            critical_alerts=int(row["critical_alerts"] or 0),
        )

    @strawberry.field
    def fleet_overview(self, days: int = 30, customer_name: Optional[str] = None) -> FleetOverview | None:
        vhs = V2_TABLES['vehicle_health_summary']
        cn = customer_name or None

        p: dict = {}
        c = _customer_and(cn, p)
        fault_counts = _fault_counts_subquery(cn, p)

        rows = _query_rows(
            f'''
            WITH fault_counts AS (
                {fault_counts}
            )
            SELECT
                count() AS total_vehicles,
                countIf(coalesce(fc.active_fault_count, 0) > 0) AS active_fault_vehicles,
                countIf(coalesce(fc.critical_fault_count, 0) > 0) AS critical_fault_vehicles,
                avg(vhs.vehicle_health_score) AS fleet_health_score
            FROM {vhs} vhs
            LEFT JOIN fault_counts fc ON fc.uniqueid = vhs.uniqueid
            WHERE 1=1 {c}
            ''',
            p,
        )
        if not rows or not int(rows[0][0]):
            return None
        total_v, active_v, crit_v, score = rows[0]

        return FleetOverview(
            event_date=str(date.today()),
            total_vehicles=int(total_v),
            active_fault_vehicles=int(active_v or 0),
            critical_fault_vehicles=int(crit_v or 0),
            fleet_health_score=_safe_float(score),
        )

    @strawberry.field
    def fleet_trend(self, days: int = 30, customer_name: Optional[str] = None) -> list[FleetTrendPoint]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'days': int(days)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                event_date,
                uniqExactIf(uniqueid, is_resolved = 0)                          AS active_fault_vehicles,
                uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3)  AS critical_fault_vehicles,
                avg(vehicle_health_score)                                        AS fleet_health_score
            FROM {vfm}
            WHERE event_date >= today() - %(days)s
              AND is_resolved = 0
              {c}
            GROUP BY event_date
            ORDER BY event_date
            ''',
            p,
        )
        return [
            FleetTrendPoint(
                event_date=str(event_date),
                active_fault_vehicles=int(active or 0),
                critical_fault_vehicles=int(critical or 0),
                fleet_health_score=_safe_float(score),
            )
            for event_date, active, critical, score in rows
        ]

    @strawberry.field
    def top_risk_vehicles(self, limit: int = 20, customer_name: Optional[str] = None) -> list[RiskVehicle]:
        vhs = V2_TABLES['vehicle_health_summary']
        vfm = V2_TABLES['vehicle_fault_master']
        vm = V2_TABLES['vehicle_master']
        cn = customer_name or None
        p: dict = {'limit': int(limit)}
        cust = ''
        if cn:
            p['_cust'] = cn
            cust = " AND vs.customer_name = %(_cust)s"
        rows = _query_rows(
            f'''
            SELECT
                vs.uniqueid,
                vs.vehicle_number,
                ifNull(m.model, '') AS vehicle_model,
                vs.customer_name,
                vs.vehicle_health_score,
                vs.active_fault_count,
                vs.critical_fault_count,
                ifNull(max(dateDiff('day', toDate(ep.event_date), today())), 0) AS longest_active_days
            FROM {vhs} vs
            LEFT JOIN {vm} m ON m.uniqueid = vs.uniqueid
            LEFT JOIN {vfm} ep
                ON ep.uniqueid = vs.uniqueid AND ep.is_resolved = 0
            WHERE vs.active_fault_count > 0
              {cust}
            GROUP BY vs.uniqueid, vs.vehicle_number, m.model, vs.customer_name,
                     vs.vehicle_health_score, vs.active_fault_count, vs.critical_fault_count
            ORDER BY vs.critical_fault_count DESC, vs.vehicle_health_score ASC, vs.active_fault_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            RiskVehicle(
                uniqueid=str(uid),
                vehicle_number=str(vn or ''),
                vehicle_model=str(vm_val or ''),
                customer_name=str(cn_val or ''),
                health_score=_safe_float(hs),
                active_fault_count=int(afc),
                critical_fault_count=int(cfc),
                longest_active_days=int(lad),
            )
            for uid, vn, vm_val, cn_val, hs, afc, cfc, lad in rows
        ]

    # -- DTC-level resolvers --

    @strawberry.field
    def dtc_summary(self, days: int = 30, limit: int = 15, customer_name: Optional[str] = None) -> list[DtcSummary]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'days': int(days), 'limit': int(limit)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                dtc_code,
                sum(occurrence_count) AS occurrences,
                uniqExact(uniqueid) AS vehicles_affected,
                avg(resolution_time_sec / 86400.0) AS avg_persistence
            FROM {vfm}
            WHERE event_date >= today() - %(days)s
              {c}
            GROUP BY dtc_code
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            DtcSummary(
                dtc_code=str(code),
                occurrences=int(occurrences),
                vehicles_affected=int(vehicles_affected),
                avg_persistence=_safe_float(avg_persistence),
            )
            for code, occurrences, vehicles_affected, avg_persistence in rows
        ]

    @strawberry.field
    def top_dtc_codes(self, days: int = 30, limit: int = 10, customer_name: Optional[str] = None) -> list[TopDtcCode]:
        """Top DTC codes with descriptions from dtc_master."""
        vfm = V2_TABLES['vehicle_fault_master']
        dm = V2_TABLES['dtc_master']
        cn = customer_name or None
        p: dict = {'days': int(days), 'limit': int(limit)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                v.dtc_code,
                ifNull(d.description, '') AS description,
                sum(v.occurrence_count) AS occurrences,
                uniqExact(v.uniqueid) AS vehicles_affected
            FROM {vfm} v
            LEFT JOIN {dm} d ON d.dtc_code = v.dtc_code
            WHERE v.event_date >= today() - %(days)s
              {c}
            GROUP BY v.dtc_code, d.description
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            TopDtcCode(
                dtc_code=str(code),
                description=str(desc) if desc else f'DTC {code}',
                occurrences=int(occ),
                vehicles_affected=int(va),
            )
            for code, desc, occ, va in rows
        ]

    @strawberry.field
    def severity_breakdown(self, days: int = 30, customer_name: Optional[str] = None) -> list[SeverityBreakdown]:
        """Severity distribution for donut chart."""
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'days': int(days)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                multiIf(severity_level >= 3, 'Critical',
                         severity_level = 2, 'Moderate',
                         'Minor') AS level,
                count() AS cnt
            FROM {vfm}
            WHERE event_date >= today() - %(days)s
              {c}
            GROUP BY level
            ORDER BY cnt DESC
            ''',
            p,
        )
        total = sum(int(r[1]) for r in rows) or 1
        return [
            SeverityBreakdown(
                level=str(lv),
                count=int(cnt),
                percentage=round(int(cnt) / total * 100, 1),
            )
            for lv, cnt in rows
        ]

    @strawberry.field
    def dtc_trend(self, dtc_code: str, days: int = 30, customer_name: Optional[str] = None) -> list[DtcTrendPoint]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'dtc_code': dtc_code, 'days': int(days)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT event_date, sum(occurrence_count) AS occurrences
            FROM {vfm}
            WHERE dtc_code = %(dtc_code)s
              AND event_date >= today() - %(days)s
              {c}
            GROUP BY event_date
            ORDER BY event_date
            ''',
            p,
        )
        return [
            DtcTrendPoint(event_date=str(event_date), occurrences=int(occurrences))
            for event_date, occurrences in rows
        ]

    @strawberry.field
    def dtc_alerts_trend(self, days: int = 30, customer_name: Optional[str] = None) -> list[DtcAlertsTrendPoint]:
        """Daily total + critical alerts for line chart."""
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'days': int(days)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                event_date,
                sum(occurrence_count) AS total_alerts,
                countIf(severity_level >= 3) AS critical_alerts
            FROM {vfm}
            WHERE event_date >= today() - %(days)s
              {c}
            GROUP BY event_date
            ORDER BY event_date
            ''',
            p,
        )
        return [
            DtcAlertsTrendPoint(
                event_date=str(ed),
                total_alerts=int(ta),
                critical_alerts=int(ca),
            )
            for ed, ta, ca in rows
        ]

    @strawberry.field
    def dtc_affected_vehicles(self, dtc_code: str, limit: int = 100, customer_name: Optional[str] = None) -> list[DtcVehicle]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'dtc_code': dtc_code, 'limit': int(limit)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                uniqueid,
                count() AS episode_count,
                countIf(is_resolved = 0) AS active_episodes,
                max(severity_level) AS max_severity
            FROM {vfm}
            WHERE dtc_code = %(dtc_code)s
              {c}
            GROUP BY uniqueid
            ORDER BY active_episodes DESC, max_severity DESC, episode_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            DtcVehicle(
                uniqueid=str(uniqueid),
                episode_count=int(episode_count),
                active_episodes=int(active_episodes),
                max_severity=int(max_severity),
            )
            for uniqueid, episode_count, active_episodes, max_severity in rows
        ]

    @strawberry.field
    def selected_dtc_kpis(self, dtc_code: str, days: int = 30, customer_name: Optional[str] = None) -> SelectedDtcKpis | None:
        """Single selected DTC KPI snapshot scoped to customer if provided."""
        vfm = V2_TABLES['vehicle_fault_master']
        p: dict = {'dtc_code': dtc_code, 'days': int(days)}
        c = _customer_and(customer_name or None, p)
        rows = _query_rows(
            f'''
            SELECT
                anyLast(system) AS system,
                anyLast(subsystem) AS subsystem,
                uniqExact(uniqueid) AS affected_vehicles,
                countIf(is_resolved = 0) AS active_episodes,
                countIf(is_resolved = 0 AND severity_level >= 3) AS critical_episodes,
                avgIf(resolution_time_sec / 86400.0, is_resolved = 1 AND resolution_time_sec > 0) AS avg_resolution_days,
                if(count() > 0, countIf(driver_related = 1) / count(), 0.0) AS driver_related_ratio,
                max(event_date) AS last_seen_date
            FROM {vfm}
            WHERE dtc_code = %(dtc_code)s
              AND event_date >= today() - %(days)s
              {c}
            ''',
            p,
        )
        if not rows:
            return None
        r = rows[0]
        if int(r[2] or 0) <= 0:
            return None
        return SelectedDtcKpis(
            dtc_code=str(dtc_code),
            system=str(r[0] or ''),
            subsystem=str(r[1] or ''),
            affected_vehicles=int(r[2] or 0),
            active_episodes=int(r[3] or 0),
            critical_episodes=int(r[4] or 0),
            avg_resolution_days=_safe_round(r[5], 1),
            driver_related_ratio=_safe_round(r[6], 3),
            last_seen_date=str(r[7] or ''),
        )

    @strawberry.field
    def selected_dtc_weekly_trend(self, dtc_code: str, days: int = 56, customer_name: Optional[str] = None) -> list[SelectedDtcWeeklyTrendPoint]:
        """Weekly trend for the selected DTC code."""
        vfm = V2_TABLES['vehicle_fault_master']
        p: dict = {'dtc_code': dtc_code, 'days': int(days)}
        c = _customer_and(customer_name or None, p)
        rows = _query_rows(
            f'''
            SELECT
                toStartOfWeek(event_date, 1) AS week_start,
                sum(occurrence_count) AS occurrences,
                uniqExact(uniqueid) AS affected_vehicles,
                countIf(is_resolved = 0) AS active_episodes,
                countIf(is_resolved = 0 AND severity_level >= 3) AS critical_episodes
            FROM {vfm}
            WHERE dtc_code = %(dtc_code)s
              AND event_date >= today() - %(days)s
              {c}
            GROUP BY week_start
            ORDER BY week_start
            ''',
            p,
        )
        return [
            SelectedDtcWeeklyTrendPoint(
                week_start=str(ws),
                occurrences=int(occ or 0),
                affected_vehicles=int(va or 0),
                active_episodes=int(ae or 0),
                critical_episodes=int(ce or 0),
            )
            for ws, occ, va, ae, ce in rows
        ]

    @strawberry.field
    def selected_dtc_cooccurrence(self, dtc_code: str, days: int = 30, limit: int = 10, customer_name: Optional[str] = None) -> list[SelectedDtcCooccurrence]:
        """Top co-occurring DTC codes with selected DTC."""
        vfm = V2_TABLES['vehicle_fault_master']
        p: dict = {'dtc_code': dtc_code, 'days': int(days), 'limit': int(limit)}
        customer_clause_target = ''
        customer_clause_other = ''
        if customer_name:
            p['_cust'] = customer_name
            customer_clause_target = 'AND customer_name = %(_cust)s'
            customer_clause_other = 'AND other.customer_name = %(_cust)s'

        rows = _query_rows(
            f'''
            WITH target_vehicles AS (
                SELECT DISTINCT uniqueid
                FROM {vfm}
                WHERE dtc_code = %(dtc_code)s
                  AND event_date >= today() - %(days)s
                  {customer_clause_target}
            )
            SELECT
                other.dtc_code AS co_dtc_code,
                count() AS cooccurrence_count,
                uniqExact(other.uniqueid) AS vehicles_affected
            FROM {vfm} AS other
            INNER JOIN target_vehicles tv ON other.uniqueid = tv.uniqueid
            WHERE other.dtc_code != %(dtc_code)s
              AND other.event_date >= today() - %(days)s
              {customer_clause_other}
            GROUP BY co_dtc_code
            ORDER BY vehicles_affected DESC, cooccurrence_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            SelectedDtcCooccurrence(
                co_dtc_code=str(code),
                cooccurrence_count=int(cnt or 0),
                vehicles_affected=int(va or 0),
            )
            for code, cnt, va in rows
        ]

    @strawberry.field
    def selected_dtc_vehicles(self, dtc_code: str, days: int = 90, limit: int = 100, customer_name: Optional[str] = None) -> list[SelectedDtcVehicle]:
        """Vehicles affected by selected DTC with severity and recency metrics."""
        vfm = V2_TABLES['vehicle_fault_master']
        vhs = V2_TABLES['vehicle_health_summary']
        p: dict = {'dtc_code': dtc_code, 'days': int(days), 'limit': int(limit)}
        customer_clause = ''
        if customer_name:
            p['_cust'] = customer_name
            customer_clause = 'AND f.customer_name = %(_cust)s'

        rows = _query_rows(
            f'''
            SELECT
                f.uniqueid,
                anyLast(f.vehicle_number) AS vehicle_number,
                count() AS episode_count,
                countIf(f.is_resolved = 0) AS active_episodes,
                max(f.severity_level) AS max_severity,
                max(f.event_date) AS last_reported_date,
                anyLast(v.vehicle_health_score) AS health_score
            FROM {vfm} f
            LEFT JOIN {vhs} v ON v.uniqueid = f.uniqueid
            WHERE f.dtc_code = %(dtc_code)s
              AND f.event_date >= today() - %(days)s
              {customer_clause}
            GROUP BY f.uniqueid
            ORDER BY active_episodes DESC, max_severity DESC, episode_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            SelectedDtcVehicle(
                uniqueid=str(uid),
                vehicle_number=str(vn or uid),
                episode_count=int(ec or 0),
                active_episodes=int(ae or 0),
                max_severity=int(ms or 0),
                last_reported_date=str(lrd or ''),
                health_score=_safe_round(hs, 1),
            )
            for uid, vn, ec, ae, ms, lrd, hs in rows
        ]

    @strawberry.field
    def vehicles_with_dtcs(self, limit: int = 50, customer_name: Optional[str] = None) -> list[VehicleWithDtcs]:
        """Vehicles with their active DTC code lists."""
        vhs = V2_TABLES['vehicle_health_summary']
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'limit': int(limit)}
        cust = ''
        if cn:
            p['_cust'] = cn
            cust = " AND vs.customer_name = %(_cust)s"
        rows = _query_rows(
            f'''
            SELECT
                vs.uniqueid,
                vs.vehicle_number,
                vs.customer_name,
                groupArray(DISTINCT ep.dtc_code) AS dtc_codes,
                if(vs.critical_fault_count > 0, 'Critical',
                   if(vs.active_fault_count > 0, 'Active', 'Normal')) AS status,
                max(ep.event_date) AS last_reported,
                vs.active_fault_count,
                vs.critical_fault_count
            FROM {vhs} vs
            INNER JOIN {vfm} ep
                ON ep.uniqueid = vs.uniqueid AND ep.is_resolved = 0
            WHERE vs.active_fault_count > 0
              {cust}
            GROUP BY vs.uniqueid, vs.vehicle_number, vs.customer_name, vs.active_fault_count, vs.critical_fault_count
            ORDER BY vs.critical_fault_count DESC, vs.active_fault_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            VehicleWithDtcs(
                uniqueid=str(uid),
                vehicle_number=str(vn or ''),
                customer_name=str(cn_val or ''),
                dtc_codes=[str(c_item) for c_item in codes] if codes else [],
                status=str(st),
                last_reported=str(lr),
                active_count=int(ac),
                critical_count=int(cc),
            )
            for uid, vn, cn_val, codes, st, lr, ac, cc in rows
        ]

    # -- Vehicle-level resolvers --

    @strawberry.field
    def vehicle_overview(self, info: Info, uniqueid: str) -> VehicleOverview | None:
        tenant = _tenant(info, "dtc:vehicle:read")
        rows = _rows(lambda: _repositories(info)["vehicles"].get_vehicle_overview(tenant, uniqueid=uniqueid))
        if not rows:
            return None
        row = rows[0]
        return VehicleOverview(
            uniqueid=str(row["uniqueid"]),
            vehicle_number=str(row["vehicle_number"] or ''),
            vehicle_model=str(row["vehicle_model"] or ''),
            vehicle_type=str(row["vehicle_type"] or ''),
            customer_name=str(row["customer_name"] or ''),
            health_score=_safe_float(row["vehicle_health_score"]),
            active_fault_count=int(row["active_fault_count"] or 0),
            critical_fault_count=int(row["critical_fault_count"] or 0),
        )

    @strawberry.field
    def vehicle_faults(self, info: Info, uniqueid: str, days: int = 90, limit: int = 200, customer_name: Optional[str] = None) -> list[VehicleFault]:
        tenant = _tenant(info, "dtc:vehicle:read", customer_name)
        rows = _rows(lambda: _repositories(info)["vehicles"].get_vehicle_fault_summary(tenant, uniqueid=uniqueid, days=days, limit=limit))
        return [
            VehicleFault(
                dtc_code=str(row["dtc_code"]),
                episode_count=int(row["episode_count"] or 0),
                active_episodes=int(row["active_episodes"] or 0),
                max_severity=int(row["max_severity"] or 0),
                days_persistence=int(row["days_persistence"] or 0),
            )
            for row in rows
        ]

    @strawberry.field
    def vehicle_timeline(self, info: Info, uniqueid: str, days: int = 90, customer_name: Optional[str] = None) -> list[VehicleTimelinePoint]:
        tenant = _tenant(info, "dtc:vehicle:read", customer_name)
        rows = _rows(lambda: _repositories(info)["vehicles"].get_vehicle_timeline_summary(tenant, uniqueid=uniqueid, days=days))
        return [
            VehicleTimelinePoint(
                event_date=str(row["event_date"]),
                active_episodes=int(row["active_episodes"] or 0),
                critical_episodes=int(row["critical_episodes"] or 0),
            )
            for row in rows
        ]

    # -- Customer / Maintenance resolvers --

    @strawberry.field
    def customer_overview(self, info: Info, limit: int = 50) -> list[CustomerOverview]:
        tenant = _tenant(info, "dtc:fleet:read")
        rows = _rows(lambda: _repositories(info)["fleet"].get_customer_overview(tenant, limit=limit))
        return [
            CustomerOverview(
                customer_name=str(row["customer_name"]),
                vehicle_count=int(row["vehicle_count"] or 0),
                active_fault_vehicles=int(row["active_fault_vehicles"] or 0),
                critical_fault_vehicles=int(row["critical_fault_vehicles"] or 0),
                avg_health_score=_safe_float(row["avg_health_score"]),
            )
            for row in rows
        ]

    @strawberry.field
    def maintenance_recommendations(self, info: Info, limit: int = 20, customer_name: Optional[str] = None) -> list[MaintenanceRecommendation]:
        tenant = _tenant(info, "dtc:maintenance:read", customer_name)
        rows = _rows(lambda: _repositories(info)["maintenance"].get_maintenance_recommendations(tenant, limit=limit))
        return [
            MaintenanceRecommendation(
                uniqueid=str(row["uniqueid"]),
                dtc_code=str(row["dtc_code"]),
                severity_level=int(row["severity_level"] or 0),
                subsystem=str(row["subsystem"] or ''),
                recommendation=str(row["recommended_action"] or 'Schedule preventive maintenance.'),
            )
            for row in rows
        ]


    # -- New V2 analytics resolvers --

    @strawberry.field
    def fleet_health_snap(self, info: Info, customer_name: Optional[str] = None) -> FleetHealthSnap | None:
        """Pre-computed fleet health summary from fleet_health_summary table."""
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["fleet"].get_fleet_health_summary(tenant))
        if not rows:
            return None
        row = rows[0]
        return FleetHealthSnap(
            total_vehicles=int(row["total_vehicles"] or 0),
            vehicles_with_active_faults=int(row["vehicles_with_active_faults"] or 0),
            vehicles_with_critical_faults=int(row["vehicles_with_critical_faults"] or 0),
            driver_related_faults=int(row["driver_related_faults"] or 0),
            fleet_health_score=_safe_round(row["fleet_health_score"], 2),
            most_common_dtc=str(row["most_common_dtc"] or ''),
            most_common_system=str(row["most_common_system"] or ''),
            active_fault_trend=str(row["active_fault_trend"] or 'stable'),
        )

    @strawberry.field
    def fleet_system_health(self, info: Info, customer_name: Optional[str] = None) -> list[FleetSystemHealth]:
        """Per-system health breakdown."""
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["fleet"].get_system_health(tenant))
        return [
            FleetSystemHealth(
                system=str(row["system"]),
                vehicles_affected=int(row["vehicles_affected"] or 0),
                active_faults=int(row["active_faults"] or 0),
                critical_faults=int(row["critical_faults"] or 0),
                risk_score=_safe_round(row["risk_score"], 2),
                trend=str(row["trend"] or 'stable'),
            )
            for row in rows
        ]

    @strawberry.field
    def fleet_fault_trends(self, info: Info, days: int = 30, customer_name: Optional[str] = None) -> list[FleetFaultTrendPoint]:
        """Daily fleet fault trends (new, resolved, driver-related) from pre-computed table."""
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["fleet"].get_fault_trends(tenant, days=days))
        return [
            FleetFaultTrendPoint(
                event_date=str(row["date"]),
                active_faults=int(row["active_faults"] or 0),
                new_faults=int(row["new_faults"] or 0),
                resolved_faults=int(row["resolved_faults"] or 0),
                driver_related_faults=int(row["driver_related_faults"] or 0),
                fleet_health_score=_safe_round(row["fleet_health_score"], 2),
            )
            for row in rows
        ]

    @strawberry.field
    def dtc_fleet_impact(self, info: Info, limit: int = 20, customer_name: Optional[str] = None) -> list[DtcFleetImpact]:
        """DTC fleet impact ranking by risk score."""
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["dtcs"].get_dtc_fleet_impact(tenant, limit=limit))
        return [
            DtcFleetImpact(
                dtc_code=str(row["dtc_code"]),
                system=str(row["system"] or ''),
                subsystem=str(row["subsystem"] or ''),
                vehicles_affected=int(row["vehicles_affected"] or 0),
                active_vehicles=int(row["active_vehicles"] or 0),
                avg_resolution_time=_safe_round(row["avg_resolution_time"], 1),
                driver_related_ratio=_safe_round(row["driver_related_ratio"], 3),
                fleet_risk_score=_safe_round(row["fleet_risk_score"], 2),
            )
            for row in rows
        ]

    @strawberry.field
    def dtc_cooccurrence(self, info: Info, limit: int = 20, customer_name: Optional[str] = None) -> list[DtcCooccurrence]:
        """Top DTC co-occurrence pairs."""
        tenant = _tenant(info, "dtc:fleet:read", customer_name)
        rows = _rows(lambda: _repositories(info)["dtcs"].get_dtc_cooccurrence(tenant, limit=limit))
        return [
            DtcCooccurrence(
                dtc_code_a=str(row["dtc_code_a"]),
                dtc_code_b=str(row["dtc_code_b"]),
                cooccurrence_count=int(row["cooccurrence_count"] or 0),
                vehicles_affected=int(row["vehicles_affected"] or 0),
                avg_time_gap_sec=_safe_round(row["avg_time_gap_sec"], 1),
            )
            for row in rows
        ]

    @strawberry.field
    def dtc_detail(self, info: Info, dtc_code: str) -> DtcDetail | None:
        """Rich detail for a single DTC code from dtc_master."""
        tenant = _tenant(info, "dtc:schema:read")
        rows = _rows(lambda: _repositories(info)["dtcs"].get_dtc_code_info(tenant, dtc_code=dtc_code))
        if not rows:
            return None
        row = rows[0]
        return DtcDetail(
            dtc_code=str(row["dtc_code"]), system=str(row["system"] or ''),
            subsystem=str(row["subsystem"] or ''), description=str(row["description"] or ''),
            severity_level=int(row["severity_level"] or 1), primary_cause=str(row["primary_cause"] or ''),
            symptoms=str(row["symptoms"] or ''), impact_if_unresolved=str(row["impact_if_unresolved"] or ''),
            fuel_mileage_impact=str(row["fuel_mileage_impact"] or ''), action_required=str(row["action_required"] or ''),
            repair_complexity=str(row["repair_complexity"] or ''), estimated_repair_hours=_safe_float(row["estimated_repair_hours"]),
            driver_related=bool(row["driver_related"]), driver_behaviour_category=str(row["driver_behaviour_category"] or ''),
            recommended_preventive_action=str(row["recommended_preventive_action"] or ''),
        )

    @strawberry.field
    def vehicle_health_detail(self, info: Info, uniqueid: str) -> VehicleHealthDetail | None:
        """Enhanced vehicle health with system flags."""
        tenant = _tenant(info, "dtc:vehicle:read")
        rows = _rows(lambda: _repositories(info)["vehicles"].get_vehicle_health(tenant, uniqueid=uniqueid))
        if not rows:
            return None
        row = rows[0]
        return VehicleHealthDetail(
            uniqueid=str(row["uniqueid"]), vehicle_number=str(row["vehicle_number"] or ''),
            customer_name=str(row["customer_name"] or ''), vehicle_health_score=_safe_float(row["vehicle_health_score"]),
            active_fault_count=int(row["active_fault_count"] or 0), critical_fault_count=int(row["critical_fault_count"] or 0),
            total_episodes=int(row["total_episodes"] or 0), episodes_last_30_days=int(row["episodes_last_30_days"] or 0),
            avg_resolution_time=_safe_round(row["avg_resolution_time"], 1), driver_related_faults=int(row["driver_related_faults"] or 0),
            most_common_dtc=str(row["most_common_dtc"] or ''), has_engine_issue=bool(row["has_engine_issue"]),
            has_emission_issue=bool(row["has_emission_issue"]), has_safety_issue=bool(row["has_safety_issue"]),
            has_electrical_issue=bool(row["has_electrical_issue"]),
        )

    @strawberry.field
    def enhanced_maintenance(self, info: Info, limit: int = 30, customer_name: Optional[str] = None) -> list[EnhancedMaintenanceRec]:
        """Maintenance priorities with scores and durations."""
        tenant = _tenant(info, "dtc:maintenance:read", customer_name)
        rows = _rows(lambda: _repositories(info)["maintenance"].get_maintenance_priority(tenant, limit=limit))
        return [
            EnhancedMaintenanceRec(
                uniqueid=str(row["uniqueid"]), vehicle_number=str(row["vehicle_number"] or ''),
                dtc_code=str(row["dtc_code"]), description=str(row["description"] or ''),
                severity_level=int(row["severity_level"] or 0), fault_duration_sec=int(row["fault_duration_sec"] or 0),
                episodes_last_30_days=int(row["episodes_last_30_days"] or 0),
                maintenance_priority_score=_safe_round(row["maintenance_priority_score"], 1),
                recommended_action=str(row["recommended_action"] or 'Schedule preventive maintenance.'),
            )
            for row in rows
        ]


schema = strawberry.Schema(query=Query, extensions=[TenantScopeExtension])
