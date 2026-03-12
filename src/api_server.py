from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from strawberry.fastapi import GraphQLRouter
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime
import asyncio
import logging

from src.config import load_env
from src.clickhouse_utils import get_clickhouse_client
from src.graphql_schema import schema as graphql_schema


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / '.env'
load_env(ENV_PATH)

UI_ROOT = PROJECT_ROOT / 'ui'

app = FastAPI(title='DTC Analytics API')
app.mount('/static', StaticFiles(directory=UI_ROOT), name='static')
app.include_router(GraphQLRouter(graphql_schema), prefix='/graphql')


def _query_rows(sql: str, params: dict | None = None):
    client = get_clickhouse_client()
    return client.execute(sql, params or {})


def _events_count():
    try:
        return int(_query_rows('SELECT count() FROM events_enriched')[0][0])
    except Exception:
        return 0


def _parse_datetime(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        try:
            return datetime.strptime(value, '%Y-%m-%d')
        except ValueError:
            return None


def _build_event_where(start_date: str | None, end_date: str | None, uniqueid: str | None):
    clauses = []
    params: dict[str, object] = {}
    start_dt = _parse_datetime(start_date)
    end_dt = _parse_datetime(end_date)

    if start_dt:
        clauses.append('TimeStamp >= toDateTime(%(start_ts)s)')
        params['start_ts'] = start_dt.strftime('%Y-%m-%d %H:%M:%S')
    if end_dt:
        clauses.append('TimeStamp <= toDateTime(%(end_ts)s)')
        params['end_ts'] = end_dt.strftime('%Y-%m-%d %H:%M:%S')
    if uniqueid:
        clauses.append('uniqueid = %(uniqueid)s')
        params['uniqueid'] = uniqueid

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ''
    return where_sql, params


@app.get('/')
def index():
    return FileResponse(UI_ROOT / 'index.html')


@app.get('/api/known-unknown')
def known_unknown(
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events = events_available and (start_date or end_date or uniqueid)

    if use_events:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        rows = _query_rows(
            f"""
            SELECT
                if(code = '' OR code IS NULL, 'unknown', 'known') AS bucket,
                uniqExact(dtc_code) AS unique_codes,
                uniqExact(uniqueid) AS unique_vehicles,
                count() AS events
            FROM events_enriched
            {where_sql}
            GROUP BY bucket
            ORDER BY bucket
            """,
            params,
        )
    elif events_available:
        rows = _query_rows(
            """
            SELECT
                if(code = '' OR code IS NULL, 'unknown', 'known') AS bucket,
                uniqExact(dtc_code) AS unique_codes,
                uniqExact(uniqueid) AS unique_vehicles,
                count() AS events
            FROM events_enriched
            GROUP BY bucket
            ORDER BY bucket
            """
        )
    else:
        rows = _query_rows(
            """
            SELECT
                bucket,
                max(unique_codes) AS unique_codes,
                max(unique_vehicles) AS unique_vehicles,
                sum(events) AS events
            FROM agg_known_unknown
            GROUP BY bucket
            ORDER BY bucket
            """
        )
    return {
        'rows': [
            {
                'bucket': bucket,
                'unique_codes': int(unique_codes),
                'unique_vehicles': int(unique_vehicles),
                'events': int(events),
            }
            for bucket, unique_codes, unique_vehicles, events in rows
        ]
    }


@app.get('/api/top-codes')
def top_codes(
    limit: int = Query(10, ge=1, le=50),
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events = events_available and (start_date or end_date or uniqueid)

    if use_events:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        params['limit'] = limit
        rows = _query_rows(
            f"""
            SELECT dtc_code, count() AS occurrences
            FROM events_enriched
            {where_sql}
            {'AND' if where_sql else 'WHERE'} dtc_code != ''
            GROUP BY dtc_code
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            params,
        )
    elif events_available:
        rows = _query_rows(
            """
            SELECT dtc_code, count() AS occurrences
            FROM events_enriched
            WHERE dtc_code != ''
            GROUP BY dtc_code
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            {'limit': limit},
        )
    else:
        rows = _query_rows(
            """
            SELECT code AS dtc_code, occurrence_count AS occurrences
            FROM agg_code_frequency
            ORDER BY occurrence_count DESC
            LIMIT %(limit)s
            """,
            {'limit': limit},
        )
    return {
        'rows': [
            {'dtc_code': dtc_code, 'occurrences': int(occurrences)}
            for dtc_code, occurrences in rows
        ]
    }


@app.get('/api/system-counts')
def system_counts(
    limit: int = Query(10, ge=1, le=50),
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events = events_available and (start_date or end_date or uniqueid)

    if use_events:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        params['limit'] = limit
        rows = _query_rows(
            f"""
            SELECT system, count() AS occurrences
            FROM events_enriched
            {where_sql}
            GROUP BY system
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            params,
        )
    else:
        rows = _query_rows(
            """
            SELECT system, sum(count) AS occurrences
            FROM agg_system_counts
            GROUP BY system
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            {'limit': limit},
        )
    return {
        'rows': [
            {'system': system, 'occurrences': int(occurrences)}
            for system, occurrences in rows
        ]
    }


@app.get('/api/subsystem-counts')
def subsystem_counts(
    limit: int = Query(10, ge=1, le=50),
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events = events_available and (start_date or end_date or uniqueid)

    if use_events:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        params['limit'] = limit
        rows = _query_rows(
            f"""
            SELECT subsystem, count() AS occurrences
            FROM events_enriched
            {where_sql}
            GROUP BY subsystem
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            params,
        )
    else:
        rows = _query_rows(
            """
            SELECT subsystem, sum(count) AS occurrences
            FROM agg_subsystem_counts
            GROUP BY subsystem
            ORDER BY occurrences DESC
            LIMIT %(limit)s
            """,
            {'limit': limit},
        )
    return {
        'rows': [
            {'subsystem': subsystem, 'occurrences': int(occurrences)}
            for subsystem, occurrences in rows
        ]
    }


@app.get('/api/severity-hourly')
def severity_hourly(
    hours: int = Query(72, ge=1, le=720),
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events_table = events_available and bool(start_date or end_date or uniqueid)
    if use_events_table:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        rows = _query_rows(
            f"""
            SELECT toStartOfHour(TimeStamp) AS event_hour, severity_level, count() AS occurrences
            FROM events_enriched
            {where_sql}
            GROUP BY event_hour, severity_level
            ORDER BY event_hour, severity_level
            """,
            params,
        )
    else:
        rows = _query_rows(
            """
            SELECT event_hour, severity_level, sum(occurrences) AS occurrences
            FROM agg_hourly_severity
            WHERE event_hour >= now() - INTERVAL %(hours)s HOUR
            GROUP BY event_hour, severity_level
            ORDER BY event_hour, severity_level
            """,
            {'hours': hours},
        )
    return {
        'rows': [
            {
                'event_hour': event_hour.isoformat() if hasattr(event_hour, 'isoformat') else str(event_hour),
                'severity_level': int(severity_level),
                'occurrences': int(occurrences),
            }
            for event_hour, severity_level, occurrences in rows
        ]
    }


@app.get('/api/vehicle-coverage')
def vehicle_coverage(
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    events_available = _events_count() > 0
    use_events = events_available and (start_date or end_date or uniqueid)

    if use_events:
        where_sql, params = _build_event_where(start_date, end_date, uniqueid)
        rows = _query_rows(
            f"""
            SELECT
                uniqExactIf(uniqueid, code = '' OR code IS NULL) AS unknown_vehicles,
                uniqExactIf(uniqueid, code != '' AND code IS NOT NULL) AS known_vehicles
            FROM events_enriched
            {where_sql}
            """,
            params,
        )
        unknown_vehicles, known_vehicles = rows[0] if rows else (0, 0)
    elif events_available:
        rows = _query_rows(
            """
            SELECT
                uniqExactIf(uniqueid, code = '' OR code IS NULL) AS unknown_vehicles,
                uniqExactIf(uniqueid, code != '' AND code IS NOT NULL) AS known_vehicles
            FROM events_enriched
            """
        )
        unknown_vehicles, known_vehicles = rows[0] if rows else (0, 0)
    else:
        rows = _query_rows(
            """
            SELECT
                maxIf(unique_vehicles, bucket = 'unknown') AS unknown_vehicles,
                maxIf(unique_vehicles, bucket = 'known') AS known_vehicles
            FROM agg_vehicle_coverage
            """
        )
        unknown_vehicles, known_vehicles = rows[0] if rows else (0, 0)
    return {
        'unknown_vehicles': int(unknown_vehicles),
        'known_vehicles': int(known_vehicles),
    }


@app.get('/api/episode-counts')
def episode_counts(
    days: int = Query(30, ge=1, le=365),
    start_date: str | None = None,
    end_date: str | None = None,
    uniqueid: str | None = None,
):
    clauses = []
    params: dict[str, object] = {}
    if start_date:
        clauses.append('event_date >= toDate(%(start_date)s)')
        params['start_date'] = start_date
    if end_date:
        clauses.append('event_date <= toDate(%(end_date)s)')
        params['end_date'] = end_date
    if uniqueid:
        clauses.append('uniqueid = %(uniqueid)s')
        params['uniqueid'] = uniqueid
    if not clauses:
        clauses.append('event_date >= today() - %(days)s')
        params['days'] = days
    where_sql = f"WHERE {' AND '.join(clauses)}"
    rows = _query_rows(
        f"""
        SELECT event_date, count() AS episodes
        FROM episodes
        {where_sql}
        GROUP BY event_date
        ORDER BY event_date
        """,
        params,
    )
    return {
        'rows': [
            {
                'event_date': event_date.isoformat() if hasattr(event_date, 'isoformat') else str(event_date),
                'episodes': int(episodes),
            }
            for event_date, episodes in rows
        ]
    }


@app.get('/api/fleet/overview')
def fleet_overview(days: int = Query(30, ge=1, le=365)):
    rows = _query_rows(
        """
        SELECT
            total_vehicles_sum AS total_vehicles,
            active_vehicles_sum AS vehicles_with_active_faults,
            critical_vehicles_sum AS vehicles_with_critical_faults,
            driver_related_faults_sum AS driver_related_faults,
            if(total_vehicle_weight > 0, weighted_health_score / total_vehicle_weight, 100.0) AS fleet_health_score,
            updated_at
        FROM (
            SELECT
                sum(total_vehicles) AS total_vehicles_sum,
                sum(vehicles_with_active_faults) AS active_vehicles_sum,
                sum(vehicles_with_critical_faults) AS critical_vehicles_sum,
                sum(driver_related_faults) AS driver_related_faults_sum,
                sum(fleet_health_score * toFloat64(total_vehicles)) AS weighted_health_score,
                sum(toFloat64(total_vehicles)) AS total_vehicle_weight,
                max(updated_at) AS updated_at
            FROM fleet_health_summary_ravi_v2
        )
        """,
    )
    if not rows:
        return {'row': None}
    (
        total_vehicles,
        active_vehicles,
        critical_vehicles,
        driver_faults,
        fleet_health_score,
        updated_at,
    ) = rows[0]
    return {
        'row': {
            'event_date': updated_at.isoformat() if hasattr(updated_at, 'isoformat') else str(updated_at),
            'total_vehicles': int(total_vehicles),
            'vehicles_with_active_faults': int(active_vehicles),
            'vehicles_with_critical_faults': int(critical_vehicles),
            'vehicles_with_persistent_faults': int(active_vehicles),
            'new_fault_episodes': 0,
            'resolved_fault_episodes': 0,
            'fleet_health_score': float(fleet_health_score or 0),
        }
    }


@app.get('/api/fleet/trend')
def fleet_trend(days: int = Query(30, ge=1, le=365)):
    rows = _query_rows(
        """
        SELECT
            date,
            active_faults_sum AS active_faults,
            critical_faults_sum AS critical_faults,
            if(active_fault_weight > 0, weighted_health_score / active_fault_weight, 100.0) AS fleet_health_score
        FROM (
            SELECT
                date,
                sum(active_faults) AS active_faults_sum,
                sum(critical_faults) AS critical_faults_sum,
                sum(fleet_health_score * greatest(toFloat64(active_faults), 1.0)) AS weighted_health_score,
                sum(greatest(toFloat64(active_faults), 1.0)) AS active_fault_weight
            FROM fleet_fault_trends_ravi_v2
            WHERE date >= today() - %(days)s
            GROUP BY date
        )
        ORDER BY date
        """,
        {'days': days},
    )
    return {
        'rows': [
            {
                'event_date': event_date.isoformat() if hasattr(event_date, 'isoformat') else str(event_date),
                'vehicles_with_active_faults': int(active),
                'vehicles_with_critical_faults': int(critical),
                'fleet_health_score': float(score or 0),
            }
            for event_date, active, critical, score in rows
        ]
    }


@app.get('/api/fleet/top-risk-vehicles')
def fleet_top_risk_vehicles(limit: int = Query(20, ge=1, le=200)):
    rows = _query_rows(
        """
        SELECT
            uniqueid,
            vehicle_health_score,
            vehicle_number,
            active_fault_count,
            critical_fault_count,
            total_episodes,
            episodes_last_30_days,
            last_fault_ts
        FROM vehicle_health_summary_ravi_v2
        WHERE active_fault_count > 0
        ORDER BY vehicle_health_score ASC, critical_fault_count DESC, active_fault_count DESC
        LIMIT %(limit)s
        """,
        {'limit': limit},
    )
    return {
        'rows': [
            {
                'uniqueid': uniqueid,
                'health_score': float(health_score or 0),
                'risk_badge': 'high' if float(health_score or 0) < 40 else 'medium' if float(health_score or 0) < 70 else 'low',
                'active_fault_count': int(active_fault_count),
                'critical_fault_count': int(critical_fault_count),
                'persistent_fault_count': int(total_episodes),
                'recurring_fault_count': int(episodes_last_30_days),
                'last_detected_ts': int(last_fault_ts or 0),
                'last_engine_on_ts': 0,
            }
            for (
                uniqueid,
                health_score,
                vehicle_number,
                active_fault_count,
                critical_fault_count,
                total_episodes,
                episodes_last_30_days,
                last_fault_ts,
            ) in rows
        ]
    }


@app.get('/api/vehicle/{uniqueid}/overview')
def vehicle_overview(uniqueid: str):
    rows = _query_rows(
        """
        SELECT
            vehicle_health_score,
            vehicle_number,
            customer_name,
            active_fault_count,
            critical_fault_count,
            total_episodes,
            episodes_last_30_days,
            last_fault_ts
        FROM vehicle_health_summary_ravi_v2
        WHERE uniqueid = %(uniqueid)s
        """,
        {'uniqueid': uniqueid},
    )
    if not rows:
        return {'row': None}
    (
        health_score,
        vehicle_number,
        customer_name,
        active_fault_count,
        critical_fault_count,
        total_episodes,
        episodes_last_30_days,
        last_detected_ts,
    ) = rows[0]
    return {
        'row': {
            'uniqueid': uniqueid,
            'health_score': float(health_score or 0),
            'risk_badge': 'high' if float(health_score or 0) < 40 else 'medium' if float(health_score or 0) < 70 else 'low',
            'active_fault_count': int(active_fault_count),
            'critical_fault_count': int(critical_fault_count),
            'persistent_fault_count': int(total_episodes),
            'recurring_fault_count': int(episodes_last_30_days),
            'last_detected_ts': int(last_detected_ts or 0),
            'last_engine_on_ts': 0,
        }
    }


@app.get('/api/vehicle/{uniqueid}/active-faults')
def vehicle_active_faults(uniqueid: str, limit: int = Query(100, ge=1, le=500)):
    rows = _query_rows(
        """
        SELECT
            dtc_code,
            description,
            severity_level,
            first_ts,
            last_ts,
            resolution_time_sec,
            occurrence_count,
            COUNT() AS episodes,
            is_resolved
        FROM vehicle_fault_master_ravi_v2
        WHERE uniqueid = %(uniqueid)s AND is_resolved = 0
        GROUP BY dtc_code, description, severity_level, first_ts, last_ts, resolution_time_sec, occurrence_count, is_resolved
        ORDER BY severity_level DESC, resolution_time_sec DESC
        LIMIT %(limit)s
        """,
        {'uniqueid': uniqueid, 'limit': limit},
    )
    return {
        'rows': [
            {
                'dtc_code': dtc_code,
                'description': description,
                'severity_level': int(severity_level),
                'first_seen_ts': int(first_ts or 0),
                'last_seen_ts': int(last_ts or 0),
                'persistence_sec': int(resolution_time_sec or 0),
                'occurrences': int(occurrence_count),
                'episodes': int(episodes),
                'is_active': 1 if is_resolved == 0 else 0,
            }
            for (
                dtc_code,
                description,
                severity_level,
                first_ts,
                last_ts,
                resolution_time_sec,
                occurrence_count,
                episodes,
                is_resolved,
            ) in rows
        ]
    }


@app.get('/api/vehicle/{uniqueid}/timeline')
def vehicle_timeline(uniqueid: str, days: int = Query(90, ge=1, le=730)):
    rows = _query_rows(
        """
        SELECT
            dtc_code,
            episode_id,
            first_ts,
            last_ts,
            resolution_time_sec,
            is_resolved
        FROM vehicle_fault_master_ravi_v2
        WHERE uniqueid = %(uniqueid)s
          AND event_date >= today() - %(days)s
        ORDER BY first_ts DESC
        """,
        {'uniqueid': uniqueid, 'days': days},
    )
    return {
        'rows': [
            {
                'dtc_code': dtc_code,
                'episode_id': episode_id,
                'first_ts': int(first_ts),
                'last_ts': int(last_ts),
                'persistence_sec': int(resolution_time_sec or 0),
                'is_resolved': int(is_resolved),
            }
            for dtc_code, episode_id, first_ts, last_ts, resolution_time_sec, is_resolved in rows
        ]
    }


@app.get('/api/dtc/{dtc_code}/overview')
def dtc_overview(dtc_code: str, days: int = Query(30, ge=1, le=365)):
    rows = _query_rows(
        """
        SELECT
            description,
            system,
            subsystem,
            severity_level,
            vehicles_affected,
            active_vehicles,
            avg_resolution_time,
            driver_related_ratio,
            fleet_risk_score,
            updated_at
        FROM dtc_fleet_impact_ravi_v2
        WHERE dtc_code = %(dtc_code)s
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        {'dtc_code': dtc_code},
    )
    if not rows:
        return {'row': None}
    (
        description,
        system,
        subsystem,
        severity_level,
        vehicles_affected,
        active_vehicles,
        avg_resolution_time,
        driver_related_ratio,
        fleet_risk_score,
        updated_at,
    ) = rows[0]
    return {
        'row': {
            'event_date': updated_at.isoformat() if hasattr(updated_at, 'isoformat') else str(updated_at),
            'dtc_code': dtc_code,
            'description': description,
            'subsystem': subsystem,
            'severity_level': int(severity_level),
            'vehicles_affected': int(vehicles_affected),
            'active_vehicles': int(active_vehicles),
            'resolved_vehicles': int(vehicles_affected) - int(active_vehicles),
            'avg_persistence_sec': float(avg_resolution_time or 0),
            'recurrence_rate': float(driver_related_ratio or 0),
            'new_occurrences': 0,
        }
    }


@app.get('/api/dtc/{dtc_code}/trend')
def dtc_trend(dtc_code: str, days: int = Query(30, ge=1, le=365)):
    rows = _query_rows(
        """
        SELECT
            event_date,
            COUNT() AS occurrences,
            uniqExactIf(uniqueid, is_resolved = 0) AS active_vehicles
        FROM vehicle_fault_master_ravi_v2
        WHERE dtc_code = %(dtc_code)s
          AND event_date >= today() - %(days)s
        GROUP BY event_date
        ORDER BY event_date
        """,
        {'dtc_code': dtc_code, 'days': days},
    )
    return {
        'rows': [
            {
                'event_date': event_date.isoformat() if hasattr(event_date, 'isoformat') else str(event_date),
                'occurrences': int(occurrences),
                'active_vehicles': int(active_vehicles),
            }
            for event_date, occurrences, active_vehicles in rows
        ]
    }


@app.get('/api/dtc/{dtc_code}/affected-vehicles')
def dtc_affected_vehicles(dtc_code: str, limit: int = Query(100, ge=1, le=500)):
    rows = _query_rows(
        """
        SELECT
            uniqueid,
            any(description) AS description,
            any(severity_level) AS severity_level,
            max(resolution_time_sec) AS max_resolution_time,
            count() AS episodes,
            maxIf(is_resolved, 1) AS any_resolved,
            max(last_ts) AS last_seen_ts
        FROM vehicle_fault_master_ravi_v2
        WHERE dtc_code = %(dtc_code)s
        GROUP BY uniqueid
        ORDER BY maxIf(is_resolved=0, 1) DESC, severity_level DESC, max_resolution_time DESC
        LIMIT %(limit)s
        """,
        {'dtc_code': dtc_code, 'limit': limit},
    )
    return {
        'rows': [
            {
                'uniqueid': uniqueid,
                'description': description,
                'severity_level': int(severity_level),
                'persistence_sec': int(max_resolution_time or 0),
                'episodes': int(episodes),
                'is_active': 1 if any_resolved == 0 else 0,
                'last_seen_ts': int(last_seen_ts or 0),
            }
            for uniqueid, description, severity_level, max_resolution_time, episodes, any_resolved, last_seen_ts in rows
        ]
    }


@app.websocket('/ws/summary')
async def summary_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            rows = _query_rows(
                """
                SELECT
                    if(code = '' OR code IS NULL, 'unknown', 'known') AS bucket,
                    count() AS events
                FROM events_enriched
                GROUP BY bucket
                ORDER BY bucket
                """
            )
            payload = {
                'updated_at': datetime.utcnow().isoformat() + 'Z',
                'known_events': 0,
                'unknown_events': 0,
            }
            for bucket, events in rows:
                if bucket == 'known':
                    payload['known_events'] = int(events)
                elif bucket == 'unknown':
                    payload['unknown_events'] = int(events)
            await websocket.send_json(payload)
            await asyncio.sleep(10)
    except WebSocketDisconnect:
        return


@app.get('/health')
def health():
    try:
        _query_rows('SELECT 1')
        return {'status': 'ok'}
    except Exception:
        logging.exception('Health check failed')
        return {'status': 'error'}


# ── AI Analyst Chat ─────────────────────────────────────────────

class ChatRequest(BaseModel):
    messages: list[dict]
    context: dict | None = None  # {mode, customer_name, vehicle_number, dtc_code}

@app.post('/api/ai/chat')
def ai_chat(req: ChatRequest):
    from src.ai_analyst import chat
    result = chat(req.messages, context=req.context)
    return result
