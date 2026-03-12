from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import strawberry

from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES


def _query_rows(sql: str, params: dict | None = None):
    client = get_clickhouse_client()
    return client.execute(sql, params or {})


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
    def customer_names(self) -> list[str]:
        """Distinct non-empty customer names for dropdown / landing page."""
        vhs = V2_TABLES['vehicle_health_summary']
        rows = _query_rows(
            f'''
            SELECT DISTINCT customer_name
            FROM {vhs}
            WHERE customer_name != ''
            ORDER BY customer_name
            ''',
        )
        return [str(r[0]) for r in rows]

    @strawberry.field
    def customer_vehicles(self, customer_name: str) -> list[CustomerVehicle]:
        """Vehicles belonging to a specific customer (for vehicle dropdown)."""
        vhs = V2_TABLES['vehicle_health_summary']
        rows = _query_rows(
            f'''
            SELECT uniqueid, vehicle_number
            FROM {vhs}
            WHERE customer_name = %(cust)s
            ORDER BY vehicle_number
            ''',
            {'cust': customer_name},
        )
        return [
            CustomerVehicle(uniqueid=str(uid), vehicle_number=str(vn or uid))
            for uid, vn in rows
        ]

    # -- Fleet KPIs (enriched) --

    @strawberry.field
    def fleet_kpis(self, days: int = 30, customer_name: Optional[str] = None) -> FleetKpis | None:
        vhs = V2_TABLES['vehicle_health_summary']
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None

        # Fleet snapshot from vehicle_health_summary
        p1: dict = {}
        c1 = _customer_and(cn, p1)
        snap = _query_rows(
            f'''
            SELECT
                count() AS total_vehicles,
                countIf(active_fault_count > 0) AS vehicles_with_dtcs,
                countIf(critical_fault_count > 0) AS critical_vehicles,
                avg(vehicle_health_score) AS fleet_health_score
            FROM {vhs}
            WHERE 1=1 {c1}
            ''',
            p1,
        )
        total_v = int(snap[0][0]) if snap else 0
        active_v = int(snap[0][1]) if snap else 0
        crit_v = int(snap[0][2]) if snap else 0
        health = float(snap[0][3] or 0) if snap else 0.0

        # Average resolution days (resolved episodes)
        p3: dict = {'days': int(days)}
        c3 = _customer_and(cn, p3)
        res_rows = _query_rows(
            f'''
            SELECT avg(resolution_time_sec / 86400.0) AS avg_days
            FROM {vfm}
            WHERE is_resolved = 1
              AND event_date >= today() - %(days)s
              AND resolution_time_sec > 0
              {c3}
            ''',
            p3,
        )
        avg_res = float(res_rows[0][0] or 0) if res_rows else 0.0

        # Maintenance due
        p4: dict = {}
        c4 = _customer_and(cn, p4)
        maint_rows = _query_rows(
            f'''
            SELECT count(DISTINCT uniqueid)
            FROM {vfm}
            WHERE is_resolved = 0 AND severity_level >= 3
              {c4}
            ''',
            p4,
        )
        maint_due = int(maint_rows[0][0]) if maint_rows else 0

        # Total DTC alerts (sum of occurrence counts in recent window)
        p5: dict = {'days': int(days)}
        c5 = _customer_and(cn, p5)
        alert_rows = _query_rows(
            f'''
            SELECT sum(occurrence_count) AS total_alerts
            FROM {vfm}
            WHERE event_date >= today() - %(days)s
              {c5}
            ''',
            p5,
        )
        total_alerts = int(alert_rows[0][0] or 0) if alert_rows else 0

        # Critical alerts
        p6: dict = {'days': int(days)}
        c6 = _customer_and(cn, p6)
        crit_alert_rows = _query_rows(
            f'''
            SELECT count()
            FROM {vfm}
            WHERE severity_level >= 3
              AND event_date >= today() - %(days)s
              {c6}
            ''',
            p6,
        )
        crit_alerts = int(crit_alert_rows[0][0]) if crit_alert_rows else 0

        return FleetKpis(
            total_vehicles=total_v,
            vehicles_with_dtcs=active_v,
            critical_vehicles=crit_v,
            avg_resolution_days=round(avg_res, 1),
            fleet_health_score=round(health, 2),
            maintenance_due=maint_due,
            total_dtc_alerts=total_alerts,
            critical_alerts=crit_alerts,
        )

    @strawberry.field
    def fleet_overview(self, days: int = 30, customer_name: Optional[str] = None) -> FleetOverview | None:
        vhs = V2_TABLES['vehicle_health_summary']
        cn = customer_name or None

        p: dict = {}
        c = _customer_and(cn, p)

        rows = _query_rows(
            f'''
            SELECT
                count() AS total_vehicles,
                countIf(active_fault_count > 0) AS active_fault_vehicles,
                countIf(critical_fault_count > 0) AS critical_fault_vehicles,
                avg(vehicle_health_score) AS fleet_health_score
            FROM {vhs}
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
            fleet_health_score=float(score or 0.0),
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
                fleet_health_score=float(score or 0.0),
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
                health_score=float(hs or 0.0),
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
                avg_persistence=float(avg_persistence or 0.0),
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
    def vehicle_overview(self, uniqueid: str) -> VehicleOverview | None:
        vhs = V2_TABLES['vehicle_health_summary']
        vm = V2_TABLES['vehicle_master']
        rows = _query_rows(
            f'''
            SELECT
                vs.uniqueid,
                vs.vehicle_number,
                ifNull(m.model, '') AS vehicle_model,
                ifNull(m.vehicle_type, '') AS vehicle_type,
                vs.customer_name,
                vs.vehicle_health_score,
                vs.active_fault_count,
                vs.critical_fault_count
            FROM {vhs} vs
            LEFT JOIN {vm} m ON m.uniqueid = vs.uniqueid
            WHERE vs.uniqueid = %(uniqueid)s
            LIMIT 1
            ''',
            {'uniqueid': uniqueid},
        )
        if not rows:
            return None
        uid, vehicle_number, vehicle_model, vehicle_type, cust_name, health_score, active_fault_count, critical_fault_count = rows[0]
        return VehicleOverview(
            uniqueid=str(uid),
            vehicle_number=str(vehicle_number or ''),
            vehicle_model=str(vehicle_model or ''),
            vehicle_type=str(vehicle_type or ''),
            customer_name=str(cust_name or ''),
            health_score=float(health_score or 0.0),
            active_fault_count=int(active_fault_count),
            critical_fault_count=int(critical_fault_count),
        )

    @strawberry.field
    def vehicle_faults(self, uniqueid: str, days: int = 90, limit: int = 200, customer_name: Optional[str] = None) -> list[VehicleFault]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'uniqueid': uniqueid, 'days': int(days), 'limit': int(limit)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                dtc_code,
                count() AS episode_count,
                countIf(is_resolved = 0) AS active_episodes,
                max(severity_level) AS max_severity,
                max(dateDiff('day', toDate(event_date), today())) AS days_persistence
            FROM {vfm}
            WHERE uniqueid = %(uniqueid)s
              AND event_date >= today() - %(days)s
              {c}
            GROUP BY dtc_code
            ORDER BY active_episodes DESC, max_severity DESC, episode_count DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            VehicleFault(
                dtc_code=str(dtc_code),
                episode_count=int(episode_count),
                active_episodes=int(active_episodes),
                max_severity=int(max_severity),
                days_persistence=int(dp),
            )
            for dtc_code, episode_count, active_episodes, max_severity, dp in rows
        ]

    @strawberry.field
    def vehicle_timeline(self, uniqueid: str, days: int = 90, customer_name: Optional[str] = None) -> list[VehicleTimelinePoint]:
        vfm = V2_TABLES['vehicle_fault_master']
        cn = customer_name or None
        p: dict = {'uniqueid': uniqueid, 'days': int(days)}
        c = _customer_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                event_date,
                countIf(is_resolved = 0) AS active_episodes,
                countIf(severity_level >= 3 AND is_resolved = 0) AS critical_episodes
            FROM {vfm}
            WHERE uniqueid = %(uniqueid)s
              AND event_date >= today() - %(days)s
              {c}
            GROUP BY event_date
            ORDER BY event_date
            ''',
            p,
        )
        return [
            VehicleTimelinePoint(
                event_date=str(event_date),
                active_episodes=int(active_episodes),
                critical_episodes=int(critical_episodes),
            )
            for event_date, active_episodes, critical_episodes in rows
        ]

    # -- Customer / Maintenance resolvers --

    @strawberry.field
    def customer_overview(self, limit: int = 50) -> list[CustomerOverview]:
        vhs = V2_TABLES['vehicle_health_summary']
        rows = _query_rows(
            f'''
            SELECT
                ifNull(nullIf(customer_name, ''), 'Unassigned') AS cust_name,
                count() AS vehicle_count,
                countIf(active_fault_count > 0) AS active_fault_vehicles,
                countIf(critical_fault_count > 0) AS critical_fault_vehicles,
                avg(vehicle_health_score) AS avg_health_score
            FROM {vhs}
            GROUP BY cust_name
            ORDER BY vehicle_count DESC
            LIMIT %(limit)s
            ''',
            {'limit': int(limit)},
        )
        return [
            CustomerOverview(
                customer_name=str(customer_name),
                vehicle_count=int(vehicle_count),
                active_fault_vehicles=int(active_fault_vehicles),
                critical_fault_vehicles=int(critical_fault_vehicles),
                avg_health_score=float(avg_health_score or 0.0),
            )
            for customer_name, vehicle_count, active_fault_vehicles, critical_fault_vehicles, avg_health_score in rows
        ]

    @strawberry.field
    def maintenance_recommendations(self, limit: int = 20, customer_name: Optional[str] = None) -> list[MaintenanceRecommendation]:
        mp = V2_TABLES['maintenance_priority']
        dm = V2_TABLES['dtc_master']
        vhs = V2_TABLES['vehicle_health_summary']
        cn = customer_name or None
        p: dict = {'limit': int(limit)}
        c = ''
        if cn:
            p['_cust'] = cn
            c = f" AND mp.uniqueid IN (SELECT uniqueid FROM {vhs} WHERE customer_name = %(_cust)s)"
        rows = _query_rows(
            f'''
            SELECT
                mp.uniqueid,
                mp.dtc_code,
                mp.severity_level,
                ifNull(d.subsystem, '') AS subsystem,
                mp.recommended_action
            FROM {mp} mp
            LEFT JOIN {dm} d ON d.dtc_code = mp.dtc_code
            WHERE 1=1
              {c}
            ORDER BY mp.maintenance_priority_score DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            MaintenanceRecommendation(
                uniqueid=str(uid),
                dtc_code=str(dc),
                severity_level=int(sv),
                subsystem=str(ss or ''),
                recommendation=str(rec) if rec else 'Schedule preventive maintenance.',
            )
            for uid, dc, sv, ss, rec in rows
        ]


    # -- New V2 analytics resolvers --

    @strawberry.field
    def fleet_health_snap(self, customer_name: Optional[str] = None) -> FleetHealthSnap | None:
        """Pre-computed fleet health summary from fleet_health_summary table."""
        fhs = V2_TABLES['fleet_health_summary']
        cn = customer_name or None
        p: dict = {}
        c = _customer_client_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                sum(total_vehicles),
                sum(vehicles_with_active_faults),
                sum(vehicles_with_critical_faults),
                sum(driver_related_faults),
                if(
                    sum(toFloat64(total_vehicles)) > 0,
                    sum(fleet_health_score * toFloat64(total_vehicles)) / sum(toFloat64(total_vehicles)),
                    100.0
                ),
                argMaxIf(most_common_dtc, vehicles_with_active_faults, most_common_dtc != ''),
                argMaxIf(most_common_system, vehicles_with_active_faults, most_common_system != ''),
                argMax(active_fault_trend, vehicles_with_active_faults)
            FROM {fhs}
            WHERE 1=1 {c}
            ''',
            p,
        )
        if not rows or not rows[0][0]:
            return None
        r = rows[0]
        return FleetHealthSnap(
            total_vehicles=int(r[0]),
            vehicles_with_active_faults=int(r[1]),
            vehicles_with_critical_faults=int(r[2]),
            driver_related_faults=int(r[3]),
            fleet_health_score=round(float(r[4] or 0), 2),
            most_common_dtc=str(r[5] or ''),
            most_common_system=str(r[6] or ''),
            active_fault_trend=str(r[7] or 'stable'),
        )

    @strawberry.field
    def fleet_system_health(self, customer_name: Optional[str] = None) -> list[FleetSystemHealth]:
        """Per-system health breakdown."""
        fsh = V2_TABLES['fleet_system_health']
        cn = customer_name or None
        p: dict = {}
        c = _customer_client_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                multiIf(system = '' OR system = 'nan', 'other', system) AS sys,
                sum(vehicles_affected),
                sum(active_faults),
                sum(critical_faults),
                avg(risk_score),
                anyLast(trend)
            FROM {fsh}
            WHERE 1=1 {c}
            GROUP BY sys
            ORDER BY avg(risk_score) DESC
            ''',
            p,
        )
        return [
            FleetSystemHealth(
                system=str(s),
                vehicles_affected=int(va),
                active_faults=int(af),
                critical_faults=int(cf),
                risk_score=round(float(rs or 0), 2),
                trend=str(t or 'stable'),
            )
            for s, va, af, cf, rs, t in rows
        ]

    @strawberry.field
    def fleet_fault_trends(self, days: int = 30, customer_name: Optional[str] = None) -> list[FleetFaultTrendPoint]:
        """Daily fleet fault trends (new, resolved, driver-related) from pre-computed table."""
        fft = V2_TABLES['fleet_fault_trends']
        cn = customer_name or None
        p: dict = {'days': int(days)}
        c = _customer_client_and(cn, p)
        rows = _query_rows(
            f'''
            SELECT
                date,
                sum(active_faults),
                sum(new_faults),
                sum(resolved_faults),
                sum(driver_related_faults),
                avg(fleet_health_score)
            FROM {fft}
            WHERE date >= today() - %(days)s
              {c}
            GROUP BY date
            ORDER BY date
            ''',
            p,
        )
        return [
            FleetFaultTrendPoint(
                event_date=str(d),
                active_faults=int(af),
                new_faults=int(nf),
                resolved_faults=int(rf),
                driver_related_faults=int(drf),
                fleet_health_score=round(float(fhs or 0), 2),
            )
            for d, af, nf, rf, drf, fhs in rows
        ]

    @strawberry.field
    def dtc_fleet_impact(self, limit: int = 20, customer_name: Optional[str] = None) -> list[DtcFleetImpact]:
        """DTC fleet impact ranking by risk score."""
        dfi = V2_TABLES['dtc_fleet_impact']
        rows = _query_rows(
            f'''
            SELECT
                dtc_code,
                anyLast(system),
                anyLast(subsystem),
                sum(vehicles_affected),
                sum(active_vehicles),
                avg(avg_resolution_time),
                avg(driver_related_ratio),
                avg(fleet_risk_score)
            FROM {dfi}
            GROUP BY dtc_code
            ORDER BY avg(fleet_risk_score) DESC
            LIMIT %(limit)s
            ''',
            {'limit': int(limit)},
        )
        return [
            DtcFleetImpact(
                dtc_code=str(dc),
                system=str(s or ''),
                subsystem=str(ss or ''),
                vehicles_affected=int(va),
                active_vehicles=int(av),
                avg_resolution_time=round(float(art or 0), 1),
                driver_related_ratio=round(float(drr or 0), 3),
                fleet_risk_score=round(float(frs or 0), 2),
            )
            for dc, s, ss, va, av, art, drr, frs in rows
        ]

    @strawberry.field
    def dtc_cooccurrence(self, limit: int = 20, customer_name: Optional[str] = None) -> list[DtcCooccurrence]:
        """Top DTC co-occurrence pairs."""
        coo = V2_TABLES['dtc_cooccurrence']
        rows = _query_rows(
            f'''
            SELECT
                dtc_code_a,
                dtc_code_b,
                sum(cooccurrence_count),
                sum(vehicles_affected),
                avg(avg_time_gap_sec)
            FROM {coo}
            GROUP BY dtc_code_a, dtc_code_b
            ORDER BY sum(cooccurrence_count) DESC
            LIMIT %(limit)s
            ''',
            {'limit': int(limit)},
        )
        return [
            DtcCooccurrence(
                dtc_code_a=str(a),
                dtc_code_b=str(b),
                cooccurrence_count=int(cc),
                vehicles_affected=int(va),
                avg_time_gap_sec=round(float(atg or 0), 1),
            )
            for a, b, cc, va, atg in rows
        ]

    @strawberry.field
    def dtc_detail(self, dtc_code: str) -> DtcDetail | None:
        """Rich detail for a single DTC code from dtc_master."""
        dm = V2_TABLES['dtc_master']
        rows = _query_rows(
            f'''
            SELECT
                dtc_code, system, subsystem, description, severity_level,
                primary_cause, symptoms, impact_if_unresolved,
                fuel_mileage_impact, action_required, repair_complexity,
                estimated_repair_hours, driver_related,
                driver_behaviour_category, recommended_preventive_action
            FROM {dm}
            WHERE dtc_code = %(dtc_code)s
            LIMIT 1
            ''',
            {'dtc_code': dtc_code},
        )
        if not rows:
            return None
        r = rows[0]
        return DtcDetail(
            dtc_code=str(r[0]),
            system=str(r[1] or ''),
            subsystem=str(r[2] or ''),
            description=str(r[3] or ''),
            severity_level=int(r[4] or 1),
            primary_cause=str(r[5] or ''),
            symptoms=str(r[6] or ''),
            impact_if_unresolved=str(r[7] or ''),
            fuel_mileage_impact=str(r[8] or ''),
            action_required=str(r[9] or ''),
            repair_complexity=str(r[10] or ''),
            estimated_repair_hours=float(r[11] or 0),
            driver_related=bool(r[12]),
            driver_behaviour_category=str(r[13] or ''),
            recommended_preventive_action=str(r[14] or ''),
        )

    @strawberry.field
    def vehicle_health_detail(self, uniqueid: str) -> VehicleHealthDetail | None:
        """Enhanced vehicle health with system flags."""
        vhs = V2_TABLES['vehicle_health_summary']
        rows = _query_rows(
            f'''
            SELECT
                uniqueid, vehicle_number, customer_name,
                vehicle_health_score, active_fault_count, critical_fault_count,
                total_episodes, episodes_last_30_days, avg_resolution_time,
                driver_related_faults, most_common_dtc,
                has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue
            FROM {vhs}
            WHERE uniqueid = %(uniqueid)s
            LIMIT 1
            ''',
            {'uniqueid': uniqueid},
        )
        if not rows:
            return None
        r = rows[0]
        return VehicleHealthDetail(
            uniqueid=str(r[0]),
            vehicle_number=str(r[1] or ''),
            customer_name=str(r[2] or ''),
            vehicle_health_score=float(r[3] or 0),
            active_fault_count=int(r[4]),
            critical_fault_count=int(r[5]),
            total_episodes=int(r[6]),
            episodes_last_30_days=int(r[7]),
            avg_resolution_time=round(float(r[8] or 0), 1),
            driver_related_faults=int(r[9]),
            most_common_dtc=str(r[10] or ''),
            has_engine_issue=bool(r[11]),
            has_emission_issue=bool(r[12]),
            has_safety_issue=bool(r[13]),
            has_electrical_issue=bool(r[14]),
        )

    @strawberry.field
    def enhanced_maintenance(self, limit: int = 30, customer_name: Optional[str] = None) -> list[EnhancedMaintenanceRec]:
        """Maintenance priorities with scores and durations."""
        mp = V2_TABLES['maintenance_priority']
        vhs = V2_TABLES['vehicle_health_summary']
        cn = customer_name or None
        p: dict = {'limit': int(limit)}
        c = ''
        if cn:
            p['_cust'] = cn
            c = f" AND mp.uniqueid IN (SELECT uniqueid FROM {vhs} WHERE customer_name = %(_cust)s)"
        rows = _query_rows(
            f'''
            SELECT
                mp.uniqueid,
                mp.vehicle_number,
                mp.dtc_code,
                mp.description,
                mp.severity_level,
                mp.fault_duration_sec,
                mp.episodes_last_30_days,
                mp.maintenance_priority_score,
                mp.recommended_action
            FROM {mp} mp
            WHERE 1=1
              {c}
            ORDER BY mp.maintenance_priority_score DESC
            LIMIT %(limit)s
            ''',
            p,
        )
        return [
            EnhancedMaintenanceRec(
                uniqueid=str(uid),
                vehicle_number=str(vn or ''),
                dtc_code=str(dc),
                description=str(desc or ''),
                severity_level=int(sv),
                fault_duration_sec=int(fds),
                episodes_last_30_days=int(e30),
                maintenance_priority_score=round(float(mps or 0), 1),
                recommended_action=str(ra) if ra else 'Schedule preventive maintenance.',
            )
            for uid, vn, dc, desc, sv, fds, e30, mps, ra in rows
        ]


schema = strawberry.Schema(query=Query)