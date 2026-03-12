"""v2 analytics pipeline — SQL-first approach.

All heavy analytics done inside ClickHouse.  Python is only an orchestrator
that fires SQL statements and logs progress.

Entry point: run_full_analytics_v2()
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

from src.clickhouse_utils import (
    ensure_cdc_infrastructure,
    finish_cdc_run_log,
    get_cdc_checkpoint,
    get_clickhouse_client,
    get_table_watermark,
    start_cdc_run_log,
    upsert_cdc_checkpoint,
    upsert_cdc_source_watermarks,
)
from src.clickhouse_utils_v2 import (
    V2_TABLES,
    ensure_v2_tables,
    populate_dtc_master,
    populate_vehicle_master,
)
from src.config import get_pipeline_cfg

log = logging.getLogger(__name__)

CDC_PIPELINE_NAME_V2 = 'dtc_analytics_ravi_v2'

# Source tables
_SRC_HISTORY = (os.getenv('DTC_HISTORY_TABLE') or 'aimm.dtc_data_history').strip()
_SRC_ENGINE  = (os.getenv('ENGINE_CYCLES_TABLE') or 'aimm.engineoncycles').strip()

# V2 target table shortcuts (filled at call time from V2_TABLES)
_T_VM   = V2_TABLES['vehicle_master']
_T_DM   = V2_TABLES['dtc_master']
_T_EXP  = V2_TABLES['dtc_events_exploded']
_T_VFM  = V2_TABLES['vehicle_fault_master']
_T_FHS  = V2_TABLES['fleet_health_summary']
_T_FDD  = V2_TABLES['fleet_dtc_distribution']
_T_FSH  = V2_TABLES['fleet_system_health']
_T_FFT  = V2_TABLES['fleet_fault_trends']
_T_VHS  = V2_TABLES['vehicle_health_summary']
_T_DFI  = V2_TABLES['dtc_fleet_impact']
_T_MP   = V2_TABLES['maintenance_priority']
_T_COO  = V2_TABLES['dtc_cooccurrence']


# ── helpers ────────────────────────────────────────────────────────────────

def _count(client, table: str) -> int:
    return client.execute(f'SELECT count() FROM {table}')[0][0]


def _exec(client, sql: str, settings: dict | None = None):
    """Execute a SQL statement with optional settings."""
    client.execute(sql, settings=settings)


def _exec_log(client, label: str, sql: str, settings: dict | None = None):
    """Execute SQL and log."""
    log.info('[%s] executing …', label)
    _exec(client, sql, settings)
    log.info('[%s] done', label)


# ── SQL templates ──────────────────────────────────────────────────────────

def _sql_populate_exploded(since_ts: int, until_ts: int) -> str:
    """Explode raw DTC history into the V2 event table and join vehicle metadata."""
    return f"""
INSERT INTO {_T_EXP}
    (clientLoginId, uniqueid, vehicle_number, ts, dtc_code,
     lat, lng, dtc_pgn, created_at)
SELECT
    vm.clientLoginId,
    toString(e.uniqueid),
    vm.vehicle_number,
    toUInt32(toUnixTimestamp(e.ts)),
    dtc_code,
    toFloat64(ifNull(e.lat, 0)),
    toFloat64(ifNull(e.lng, 0)),
    toString(ifNull(e.dtc_pgn, '')),
    now()
FROM {_SRC_HISTORY} AS e
ARRAY JOIN e.dtc_code AS dtc_code
INNER JOIN {_T_VM} AS vm ON toString(e.uniqueid) = vm.uniqueid
WHERE toUInt32(toUnixTimestamp(e.ts)) > {since_ts}
  AND toUInt32(toUnixTimestamp(e.ts)) <= {until_ts}
  AND dtc_code != '0'
"""


def _sql_create_tmp_engine_ts() -> str:
    """Materialize per-vehicle sorted engine cycle timestamps (UInt32)."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_engine_ts_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, cycle_end_u32)
AS
SELECT
    toString(uniqueid) AS uniqueid,
    toUInt32(toUnixTimestamp(assumeNotNull(cycle_end_ts))) AS cycle_end_u32
FROM {_SRC_ENGINE}
WHERE cycle_end_ts IS NOT NULL
"""


def _sql_create_tmp_events_lagged() -> str:
    """Add prev_ts via lag() window — the foundation for episode splitting."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_events_lag_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    uniqueid,
    dtc_code,
    ts,
    lagInFrame(ts, 1, 0)
        OVER (PARTITION BY uniqueid, dtc_code ORDER BY ts
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_ts
FROM {_T_EXP}
"""


def _sql_create_tmp_events_breaks(gap_seconds: int) -> str:
    """Flag episode boundaries: time-gap OR engine-boundary.

    Engine boundary detection uses a LEFT JOIN against the engine cycle
    timestamps table: if any cycle_end falls between prev_ts and ts for the
    same vehicle, that event marks a new episode.
    """
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_events_break_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    el.uniqueid,
    el.dtc_code,
    el.ts,
    el.prev_ts,
    CASE
        WHEN el.prev_ts = 0 THEN 1
        WHEN (el.ts - el.prev_ts) > {gap_seconds} THEN 1
        WHEN eb.has_boundary = 1 THEN 1
        ELSE 0
    END AS is_break
FROM _tmp_events_lag_ravi AS el
LEFT JOIN (
    -- Pre-compute: for each (uniqueid, prev_ts, ts) pair, does an engine
    -- cycle boundary exist between prev_ts and ts?
    SELECT
        el2.uniqueid,
        el2.dtc_code,
        el2.ts,
        toUInt8(count() > 0) AS has_boundary
    FROM _tmp_events_lag_ravi AS el2
    INNER JOIN _tmp_engine_ts_ravi AS ec
        ON ec.uniqueid = el2.uniqueid
       AND ec.cycle_end_u32 > el2.prev_ts
       AND ec.cycle_end_u32 <= el2.ts
    WHERE el2.prev_ts > 0
    GROUP BY el2.uniqueid, el2.dtc_code, el2.ts
) AS eb
    ON  el.uniqueid  = eb.uniqueid
    AND el.dtc_code  = eb.dtc_code
    AND el.ts        = eb.ts
"""


def _sql_create_tmp_events_episoded() -> str:
    """Assign episode_id = cumulative sum of break flags per (uniqueid, dtc_code)."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_events_episode_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    uniqueid,
    dtc_code,
    ts,
    sum(is_break)
        OVER (PARTITION BY uniqueid, dtc_code ORDER BY ts
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_id
FROM _tmp_events_break_ravi
"""


def _sql_create_tmp_episode_agg() -> str:
    """Aggregate raw events into episode summaries (one row per episode)."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_episode_agg_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, episode_id)
AS
SELECT
    ev.uniqueid,
    ev.dtc_code,
    ev.episode_id,
    min(ev.ts)   AS first_ts,
    max(ev.ts)   AS last_ts,
    toUInt32(count()) AS occurrence_count
FROM _tmp_events_episode_ravi AS ev
GROUP BY ev.uniqueid, ev.dtc_code, ev.episode_id
"""


def _sql_create_tmp_episode_resolved(cutoff_ts: int) -> str:
    """Add resolution flag + resolution_time + gap_from_previous."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_episode_resolved_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, first_ts)
AS
WITH
max_engine AS (
    SELECT uniqueid, max(cycle_end_u32) AS max_cycle_ts
    FROM _tmp_engine_ts_ravi
    GROUP BY uniqueid
),
with_resolution AS (
    SELECT
        ea.*,
        toUInt32(greatest(0, ea.last_ts - ea.first_ts)) AS resolution_time_sec,
        if(
            ea.last_ts <= {cutoff_ts}
            AND ifNull(me.max_cycle_ts, 0) > ea.last_ts,
            toUInt8(1), toUInt8(0)
        ) AS is_resolved
    FROM _tmp_episode_agg_ravi AS ea
    LEFT JOIN max_engine AS me ON ea.uniqueid = me.uniqueid
),
with_lag AS (
    SELECT
        wr.*,
        lagInFrame(wr.last_ts, 1, 0)
            OVER (PARTITION BY wr.uniqueid, wr.dtc_code ORDER BY wr.first_ts
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_last_ts
    FROM with_resolution AS wr
)
SELECT
    uniqueid, dtc_code, episode_id, first_ts, last_ts,
    occurrence_count, resolution_time_sec, is_resolved,
    toUInt32(greatest(0, if(prev_last_ts = 0, 0,
                 toInt64(first_ts) - toInt64(prev_last_ts)))) AS gap_from_previous_episode
FROM with_lag
"""


def _sql_create_tmp_vehicle_risk(scaling_constant: float) -> str:
    """Per-vehicle health risk score from active (unresolved) episodes."""
    return f"""
CREATE TABLE IF NOT EXISTS _tmp_vehicle_risk_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid)
AS
WITH
ep_counts AS (
    SELECT uniqueid, dtc_code, count() AS ep_count
    FROM _tmp_episode_agg_ravi
    GROUP BY uniqueid, dtc_code
)
SELECT
    er.uniqueid AS uniqueid,
    toFloat64(sum(
        multiIf(toInt8(ifNull(dm.severity_level, 1)) <= 1, 1.0,
                toInt8(ifNull(dm.severity_level, 1))  = 2, 3.0,
                toInt8(ifNull(dm.severity_level, 1))  = 3, 7.0, 12.0)
        * (1.0 + least(toFloat64(er.resolution_time_sec) / 86400.0, 30.0) / 10.0)
        * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count, 1)) - 1.0) * 0.5)
    )) AS risk
FROM _tmp_episode_resolved_ravi AS er
LEFT JOIN {_T_DM} AS dm ON er.dtc_code = dm.dtc_code
LEFT JOIN ep_counts AS ec ON er.uniqueid = ec.uniqueid AND er.dtc_code = ec.dtc_code
WHERE er.is_resolved = 0
GROUP BY er.uniqueid
"""


def _sql_populate_vehicle_fault_master_final(scaling_constant: float) -> str:
    """Final INSERT joining all pre-computed temp tables."""
    return f"""
INSERT INTO {_T_VFM}
    (episode_id, clientLoginId, uniqueid, vehicle_number, customer_name,
     model, manufacturing_year, dtc_code,
     system, subsystem, description, severity_level,
     first_ts, last_ts, event_date, occurrence_count, resolution_time_sec,
     is_resolved, resolution_reason,
     gap_from_previous_episode, engine_cycles_during, driver_related,
     has_engine_issue, has_coolant_issue, has_safety_issue,
     has_emission_issue, has_electrical_issue,
     vehicle_health_score,
     created_at, updated_at)
SELECT
    concat(toString(er.uniqueid), '_', er.dtc_code, '_', toString(er.episode_id)),
    vm.clientLoginId,
    er.uniqueid,
    vm.vehicle_number,
    vm.customer_name,
    vm.model,
    vm.manufacturing_year,
    er.dtc_code,
    ifNull(nullIf(nullIf(dm.system, 'nan'), 'None'), ''),
    ifNull(nullIf(nullIf(dm.subsystem, 'nan'), 'None'), ''),
    ifNull(nullIf(dm.description, 'nan'), ''),
    toInt8(greatest(ifNull(toInt16(dm.severity_level), 1), 1)),
    er.first_ts,
    er.last_ts,
    toDate(toDateTime(er.first_ts)),
    er.occurrence_count,
    er.resolution_time_sec,
    er.is_resolved,
    '',
    er.gap_from_previous_episode,
    toUInt32(ifNull(ece.eng_cycles, 0)),
    toUInt8(ifNull(dm.driver_related, 0)),
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%powertrain%'
            OR lower(ifNull(dm.system,'')) LIKE '%engine%'),
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%coolant%'),
    toUInt8(lower(ifNull(dm.system,''))    LIKE '%safety%'),
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%emission%'
            OR lower(ifNull(dm.system,'')) LIKE '%emission%'),
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%electrical%'
            OR lower(ifNull(dm.system,'')) LIKE '%electrical%'),
    toFloat32(greatest(0.0, 100.0 - least(ifNull(vr.risk, 0.0) * {scaling_constant}, 100.0))),
    now(),
    now()
FROM _tmp_episode_resolved_ravi AS er
INNER JOIN {_T_VM} AS vm ON er.uniqueid = vm.uniqueid
LEFT JOIN  {_T_DM} AS dm ON er.dtc_code = dm.dtc_code
LEFT JOIN  _tmp_vehicle_risk_ravi AS vr ON er.uniqueid = vr.uniqueid
LEFT JOIN  (
    SELECT
        ea.uniqueid, ea.dtc_code, ea.episode_id,
        toUInt32(count()) AS eng_cycles
    FROM _tmp_episode_agg_ravi AS ea
    INNER JOIN _tmp_engine_ts_ravi AS ec
        ON ec.uniqueid = ea.uniqueid
       AND ec.cycle_end_u32 >= ea.first_ts
       AND ec.cycle_end_u32 <= ea.last_ts
    GROUP BY ea.uniqueid, ea.dtc_code, ea.episode_id
) AS ece
    ON er.uniqueid  = ece.uniqueid
   AND er.dtc_code  = ece.dtc_code
   AND er.episode_id = ece.episode_id
"""


# ── Derived analytics: all pure SQL ────────────────────────────────────────

def _sql_fleet_health_summary(scaling_constant: float) -> str:
    return f"""
INSERT INTO {_T_FHS}
    (clientLoginId, total_vehicles, vehicles_with_active_faults,
     vehicles_with_critical_faults, driver_related_faults,
     fleet_health_score, most_common_dtc, most_common_system,
     active_fault_trend, created_at, updated_at)

WITH
all_vehicles AS (
    SELECT clientLoginId, count() AS total FROM {_T_VM} GROUP BY clientLoginId
),

-- Count episodes per (uniqueid, dtc_code) for recurrence
ep_counts AS (
    SELECT uniqueid, dtc_code, uniqExact(episode_id) AS ep_count
    FROM {_T_VFM}
    GROUP BY uniqueid, dtc_code
),

fault_stats AS (
    SELECT
        f.clientLoginId,
        uniqExactIf(f.uniqueid, f.is_resolved = 0) AS active_veh,
        uniqExactIf(f.uniqueid, f.is_resolved = 0 AND f.severity_level >= 3) AS critical_veh,
        countIf(f.driver_related = 1) AS driver_faults,
        -- risk from active episodes
        sum(if(f.is_resolved = 0,
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0)
            * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
            * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count,1)) - 1.0) * 0.5),
        0)) AS risk_sum,
        topKWeighted(1)(f.dtc_code, f.occurrence_count) AS top_dtc_arr,
        topKWeighted(1)(f.system, f.occurrence_count)    AS top_sys_arr
    FROM {_T_VFM} AS f
    LEFT JOIN ep_counts AS ec ON f.uniqueid = ec.uniqueid AND f.dtc_code = ec.dtc_code
    GROUP BY f.clientLoginId
),

-- Compute real active_fault_trend: compare new unresolved faults last 7d vs prior 7d
trending AS (
    SELECT
        clientLoginId,
        countIf(is_resolved = 0 AND event_date >= today() - 7)                              AS recent_active,
        countIf(is_resolved = 0 AND event_date >= today() - 14 AND event_date < today() - 7) AS prior_active
    FROM {_T_VFM}
    GROUP BY clientLoginId
)

SELECT
    av.clientLoginId,
    av.total,
    ifNull(fs.active_veh, 0),
    ifNull(fs.critical_veh, 0),
    ifNull(fs.driver_faults, 0),
    toFloat32(
        if(
            ifNull(fs.active_veh, 0) > 0,
            greatest(
                0.0,
                100.0 - least(
                    (ifNull(fs.risk_sum, 0.0) / greatest(toFloat64(fs.active_veh), 1.0)) * {scaling_constant},
                    100.0
                )
            ),
            100.0
        )
    ),
    if(length(fs.top_dtc_arr) > 0, fs.top_dtc_arr[1], ''),
    if(length(fs.top_sys_arr)  > 0, fs.top_sys_arr[1], ''),
    multiIf(
        ifNull(tr.recent_active, 0) > ifNull(tr.prior_active, 0) * 1.1 + 1, 'increasing',
        ifNull(tr.prior_active, 0) > 0 AND ifNull(tr.recent_active, 0) < ifNull(tr.prior_active, 0) * 0.9, 'decreasing',
        'stable'
    ),
    now(), now()
FROM all_vehicles AS av
LEFT JOIN fault_stats AS fs ON av.clientLoginId = fs.clientLoginId
LEFT JOIN trending     AS tr ON av.clientLoginId = tr.clientLoginId
"""


def _sql_fleet_dtc_distribution() -> str:
    return f"""
INSERT INTO {_T_FDD}
    (clientLoginId, dtc_code, description, system, subsystem, severity_level,
     vehicles_affected, active_vehicles, total_occurrences, total_episodes,
     avg_resolution_time, driver_related_count, created_at, updated_at)
SELECT
    f.clientLoginId,
    f.dtc_code,
    any(f.description),
    any(f.system),
    any(f.subsystem),
    any(f.severity_level),
    uniqExact(f.uniqueid),
    uniqExactIf(f.uniqueid, f.is_resolved = 0),
    sum(f.occurrence_count),
    count(),
    if(countIf(f.resolution_time_sec > 0) > 0,
       toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                 / countIf(f.resolution_time_sec > 0)), 0),
    countIf(f.driver_related = 1),
    now(), now()
FROM {_T_VFM} AS f
GROUP BY f.clientLoginId, f.dtc_code
"""


def _sql_fleet_system_health() -> str:
    return f"""
INSERT INTO {_T_FSH}
    (clientLoginId, system, vehicles_affected, active_faults, critical_faults,
     risk_score, trend, created_at, updated_at)
SELECT
    f.clientLoginId,
    multiIf(f.system = '' OR f.system = 'nan', 'other', f.system) AS sys,
    uniqExactIf(f.uniqueid, f.is_resolved = 0),
    countIf(f.is_resolved = 0),
    countIf(f.is_resolved = 0 AND f.severity_level >= 3),
    toFloat32(sumIf(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0),
        f.is_resolved = 0)),
    'stable',
    now(), now()
FROM {_T_VFM} AS f
GROUP BY f.clientLoginId, sys
"""


def _sql_fleet_fault_trends(scaling_constant: float) -> str:
    return f"""
INSERT INTO {_T_FFT}
    (clientLoginId, date, active_faults, critical_faults, new_faults, resolved_faults,
     driver_related_faults, fleet_health_score, created_at, updated_at)
SELECT
    f.clientLoginId,
    f.event_date,
    countIf(f.is_resolved = 0)                        AS active_faults,
    countIf(f.is_resolved = 0 AND f.severity_level >= 3) AS critical_faults,
    count()                                           AS new_faults,
    countIf(f.is_resolved = 1)                        AS resolved_faults,
    countIf(f.driver_related = 1)                     AS driver_related_faults,
    toFloat32(greatest(0.0, 100.0 - least(
        sumIf(
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0),
            f.is_resolved = 0
        ) / greatest(uniqExactIf(f.uniqueid, f.is_resolved = 0), 1)
        * {scaling_constant}, 100.0))) AS fleet_health_score,
    now(), now()
FROM {_T_VFM} AS f
GROUP BY f.clientLoginId, f.event_date
ORDER BY f.clientLoginId, f.event_date
"""


def _sql_vehicle_health_summary(scaling_constant: float) -> str:
    thirty_days_ago_ts = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
    return f"""
INSERT INTO {_T_VHS}
    (clientLoginId, uniqueid, vehicle_number, customer_name,
     active_fault_count, critical_fault_count, total_episodes,
     episodes_last_30_days, avg_resolution_time, last_fault_ts,
     vehicle_health_score, driver_related_faults, most_common_dtc,
     has_engine_issue, has_emission_issue, has_safety_issue,
     has_electrical_issue, created_at, updated_at)

WITH
-- Count episodes per (uniqueid, dtc_code) for recurrence factor
ep_counts AS (
    SELECT uniqueid, dtc_code, uniqExact(episode_id) AS ep_count
    FROM {_T_VFM}
    GROUP BY uniqueid, dtc_code
),

-- Per-vehicle risk from active episodes
vehicle_risk AS (
    SELECT
        f.uniqueid,
        sum(
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0)
            * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
            * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count,1)) - 1.0) * 0.5)
        ) AS risk
    FROM {_T_VFM} AS f
    LEFT JOIN ep_counts AS ec ON f.uniqueid = ec.uniqueid AND f.dtc_code = ec.dtc_code
    WHERE f.is_resolved = 0
    GROUP BY f.uniqueid
),

-- Per-vehicle fault stats
vehicle_stats AS (
    SELECT
        f.clientLoginId,
        f.uniqueid,
        countIf(f.is_resolved = 0)                        AS active,
        countIf(f.is_resolved = 0 AND f.severity_level >= 3) AS critical,
        count()                                            AS total_eps,
        countIf(f.first_ts >= {thirty_days_ago_ts})        AS last_30,
        if(countIf(f.resolution_time_sec > 0) > 0,
           toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                     / countIf(f.resolution_time_sec > 0)), 0) AS avg_res,
        max(f.last_ts)                                     AS last_ts,
        countIf(f.driver_related = 1)                      AS driver_faults,
        topKWeighted(1)(f.dtc_code, f.occurrence_count)    AS top_dtc_arr,
        -- subsystem / system flags (OR across all episodes)
        maxIf(1, f.has_engine_issue     = 1)               AS has_engine,
        maxIf(1, f.has_emission_issue   = 1)               AS has_emission,
        maxIf(1, f.has_safety_issue     = 1)               AS has_safety,
        maxIf(1, f.has_electrical_issue = 1)               AS has_electrical
    FROM {_T_VFM} AS f
    GROUP BY f.clientLoginId, f.uniqueid
)

-- Every vehicle in vehicle_master gets a row (LEFT JOIN)
SELECT
    vm.clientLoginId,
    vm.uniqueid,
    vm.vehicle_number,
    vm.customer_name,
    ifNull(vs.active, 0),
    ifNull(vs.critical, 0),
    ifNull(vs.total_eps, 0),
    ifNull(vs.last_30, 0),
    ifNull(vs.avg_res, 0),
    ifNull(vs.last_ts, 0),
    toFloat32(greatest(0.0, 100.0 - least(ifNull(vr.risk, 0.0) * {scaling_constant}, 100.0))),
    ifNull(vs.driver_faults, 0),
    if(length(ifNull(vs.top_dtc_arr, [])) > 0, vs.top_dtc_arr[1], ''),
    toUInt8(ifNull(vs.has_engine, 0)),
    toUInt8(ifNull(vs.has_emission, 0)),
    toUInt8(ifNull(vs.has_safety, 0)),
    toUInt8(ifNull(vs.has_electrical, 0)),
    now(), now()
FROM {_T_VM} AS vm
LEFT JOIN vehicle_stats AS vs ON vm.uniqueid = vs.uniqueid
                              AND vm.clientLoginId = vs.clientLoginId
LEFT JOIN vehicle_risk  AS vr ON vm.uniqueid = vr.uniqueid
"""


def _sql_dtc_fleet_impact() -> str:
    return f"""
INSERT INTO {_T_DFI}
    (clientLoginId, dtc_code, system, subsystem,
     vehicles_affected, active_vehicles,
     avg_resolution_time, driver_related_ratio, fleet_risk_score,
     created_at, updated_at)
SELECT
    f.clientLoginId,
    f.dtc_code,
    any(f.system),
    any(f.subsystem),
    uniqExact(f.uniqueid),
    uniqExactIf(f.uniqueid, f.is_resolved = 0),
    if(countIf(f.resolution_time_sec > 0) > 0,
       toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                 / countIf(f.resolution_time_sec > 0)), 0),
    if(count() > 0,
       toFloat32(countIf(f.driver_related = 1) / count()), 0),
    toFloat32(sumIf(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0),
        f.is_resolved = 0)),
    now(), now()
FROM {_T_VFM} AS f
GROUP BY f.clientLoginId, f.dtc_code
"""


def _sql_maintenance_priority() -> str:
    thirty_days_ago_ts = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())
    return f"""
INSERT INTO {_T_MP}
    (clientLoginId, uniqueid, vehicle_number, dtc_code, description,
     severity_level, fault_duration_sec, episodes_last_30_days,
     maintenance_priority_score, recommended_action,
     created_at, updated_at)

WITH
recent_counts AS (
    SELECT uniqueid, dtc_code, count() AS eps_30
    FROM {_T_VFM}
    WHERE first_ts >= {thirty_days_ago_ts}
    GROUP BY uniqueid, dtc_code
)

SELECT
    f.clientLoginId,
    f.uniqueid,
    f.vehicle_number,
    f.dtc_code,
    f.description,
    f.severity_level,
    f.resolution_time_sec,
    ifNull(rc.eps_30, 0),
    toFloat32(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0)
        * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
        * (1.0 + ifNull(rc.eps_30, 0))
    ),
    ifNull(dm.action_required, ''),
    now(), now()
FROM {_T_VFM} AS f
LEFT JOIN recent_counts AS rc
    ON f.uniqueid = rc.uniqueid AND f.dtc_code = rc.dtc_code
LEFT JOIN {_T_DM} AS dm
    ON f.dtc_code = dm.dtc_code
WHERE f.is_resolved = 0
"""


def _sql_cooccurrence() -> str:
    """DTC co-occurrence: which DTC pairs appear together on the same vehicle.

    Uses a compact approach: for each vehicle, collect distinct DTCs,
    then enumerate pairs with arrayJoin of pre-computed combinations.
    No self-join, no time-bucketing — just per-vehicle DTC co-presence.
    """
    return f"""
INSERT INTO {_T_COO}
    (clientLoginId, dtc_code_a, dtc_code_b,
     cooccurrence_count, vehicles_affected, avg_time_gap_sec,
     last_seen_ts, created_at, updated_at)

WITH
-- Per-vehicle: sorted list of distinct DTCs + stats
per_vehicle AS (
    SELECT
        any(clientLoginId) AS clientLoginId,
        uniqueid,
        arraySort(groupUniqArray(dtc_code)) AS dtcs,
        count() AS total_episodes,
        max(last_ts) AS max_ts
    FROM {_T_VFM}
    GROUP BY uniqueid
    HAVING length(dtcs) >= 2
),
-- Generate all ordered pairs per vehicle using arrayMap
pairs AS (
    SELECT
        clientLoginId,
        uniqueid,
        max_ts,
        total_episodes,
        arrayJoin(
            arrayFlatten(
                arrayMap(
                    i -> arrayMap(
                        j -> tuple(dtcs[i], dtcs[j]),
                        range(i + 1, length(dtcs) + 1)
                    ),
                    range(1, length(dtcs))
                )
            )
        ) AS pair
    FROM per_vehicle
    WHERE length(dtcs) <= 200
)
SELECT
    any(clientLoginId)   AS clientLoginId,
    pair.1               AS dtc_code_a,
    pair.2               AS dtc_code_b,
    sum(total_episodes)  AS cooccurrence_count,
    count()              AS vehicles_affected,
    toFloat32(0)         AS avg_time_gap_sec,
    max(max_ts)          AS last_seen_ts,
    now(), now()
FROM pairs
GROUP BY dtc_code_a, dtc_code_b
HAVING vehicles_affected >= 2
"""


# ── Pipeline entry point ──────────────────────────────────────────────────

def run_full_analytics_v2() -> dict:
    """Execute the full v2 analytics pipeline — all heavy work in ClickHouse SQL."""
    pipeline_cfg = get_pipeline_cfg()
    gap_seconds = int(pipeline_cfg.get('episode_gap_seconds', 1800))
    final_day_cutoff_seconds = int(pipeline_cfg.get('final_day_cutoff_seconds', 86400))
    analytics_window_days = int(pipeline_cfg.get('analytics_window_days', 90))
    scaling_constant = float(os.getenv('HEALTH_SCALING_CONSTANT') or 0.2)

    client = get_clickhouse_client()
    sql_settings = {'max_execution_time': 900, 'max_memory_usage': 20_000_000_000}

    # ── 0. Ensure all v2 tables ────────────────────────────────────────
    ensure_v2_tables(client)

    # ── 1. Dimension tables (lightweight, keep Python) ──────────────────
    log.info('Step 1: Populating dimension tables …')
    dtc_count = populate_dtc_master(client)
    vehicle_count = populate_vehicle_master(client)
    log.info('Dimensions: %d DTC codes, %d vehicles', dtc_count, vehicle_count)
    if vehicle_count == 0:
        log.error('No vehicles after solutionType filter — aborting')
        return {'error': 'no vehicles after solutionType filter'}

    # ── 2. CDC bookkeeping preamble ─────────────────────────────────────
    cdc_enabled = bool(pipeline_cfg.get('cdc_enabled', True))
    run_window_end = datetime.now(timezone.utc)
    until_ts = int(run_window_end.timestamp())
    analytics_window_start_ts = int((run_window_end - timedelta(days=analytics_window_days)).timestamp())
    run_id = None
    pipeline_name = CDC_PIPELINE_NAME_V2
    run_window_start = run_window_end - timedelta(days=analytics_window_days)

    if cdc_enabled:
        source_table = os.getenv('DTC_HISTORY_TABLE') or 'ss_dtc_data_history'
        bootstrap_days = int(pipeline_cfg.get('cdc_bootstrap_days', 90))
        ensure_cdc_infrastructure(client, source_table=source_table)
        checkpoint = get_cdc_checkpoint(client, pipeline_name)
        run_window_start = checkpoint if checkpoint else (
            run_window_end - timedelta(days=max(bootstrap_days, analytics_window_days)))
        run_id = start_cdc_run_log(client, pipeline_name, run_window_start, run_window_end)

    # ── 3. Populate dtc_events_exploded via INSERT…SELECT ───────────────
    log.info('Step 3: Populating dtc_events_exploded (INSERT…SELECT, no Python RAM) …')
    _exec(client, f'TRUNCATE TABLE {_T_EXP}')
    _exec(client, _sql_populate_exploded(analytics_window_start_ts, until_ts), sql_settings)
    exploded_count = _count(client, _T_EXP)
    log.info('dtc_events_exploded: %d rows', exploded_count)

    if exploded_count == 0:
        log.warning('No exploded events — nothing to process.')
        summary = {k: 0 for k in V2_TABLES}
        summary['dtc_master'] = dtc_count
        summary['vehicle_master'] = vehicle_count
        return summary

    # ── 4. Temp table: engine cycle timestamps ──────────────────────────
    log.info('Step 4: Materializing engine cycle timestamps …')
    _exec(client, 'DROP TABLE IF EXISTS _tmp_engine_ts_ravi')
    _exec(client, _sql_create_tmp_engine_ts(), sql_settings)
    eng_count = _count(client, '_tmp_engine_ts_ravi')
    log.info('Engine cycle timestamps: %d', eng_count)

    # ── 5. Episode detection: lag → breaks → episode_id ─────────────────
    log.info('Step 5a: Computing lagged timestamps …')
    _exec(client, 'DROP TABLE IF EXISTS _tmp_events_lag_ravi')
    _exec(client, _sql_create_tmp_events_lagged(), sql_settings)
    log.info('Step 5a done: %d rows', _count(client, '_tmp_events_lag_ravi'))

    log.info('Step 5b: Detecting episode boundaries (gap=%ds + engine boundaries) …', gap_seconds)
    _exec(client, 'DROP TABLE IF EXISTS _tmp_events_break_ravi')
    _exec(client, _sql_create_tmp_events_breaks(gap_seconds), sql_settings)
    breaks = client.execute('SELECT sum(is_break) FROM _tmp_events_break_ravi')[0][0]
    log.info('Step 5b done: %d break points detected', breaks)

    log.info('Step 5c: Assigning episode IDs …')
    _exec(client, 'DROP TABLE IF EXISTS _tmp_events_episode_ravi')
    _exec(client, _sql_create_tmp_events_episoded(), sql_settings)
    log.info('Step 5c done: %d rows', _count(client, '_tmp_events_episode_ravi'))

    # ── 6. Build vehicle_fault_master (via temp tables) ───────────────
    log.info('Step 6a: Aggregating episodes …')
    _exec(client, 'DROP TABLE IF EXISTS _tmp_episode_agg_ravi')
    _exec(client, _sql_create_tmp_episode_agg(), sql_settings)
    log.info('Step 6a done: %d episode rows', _count(client, '_tmp_episode_agg_ravi'))

    global_max_ts = client.execute(f'SELECT max(ts) FROM {_T_EXP}')[0][0]
    cutoff_ts = global_max_ts - final_day_cutoff_seconds
    log.info('Step 6b: Computing resolution flags (cutoff_ts=%d) …', cutoff_ts)
    _exec(client, 'DROP TABLE IF EXISTS _tmp_episode_resolved_ravi')
    _exec(client, _sql_create_tmp_episode_resolved(cutoff_ts), sql_settings)
    log.info('Step 6b done: %d rows', _count(client, '_tmp_episode_resolved_ravi'))

    log.info('Step 6c: Computing per-vehicle risk scores …')
    _exec(client, 'DROP TABLE IF EXISTS _tmp_vehicle_risk_ravi')
    _exec(client, _sql_create_tmp_vehicle_risk(scaling_constant), sql_settings)
    log.info('Step 6c done: %d vehicles with risk', _count(client, '_tmp_vehicle_risk_ravi'))

    log.info('Step 6d: Final INSERT into vehicle_fault_master …')
    _exec(client, f'TRUNCATE TABLE {_T_VFM}')
    _exec(client, _sql_populate_vehicle_fault_master_final(scaling_constant), sql_settings)
    vfm_count = _count(client, _T_VFM)
    log.info('vehicle_fault_master: %d rows', vfm_count)

    # ── 7‑9. Derived analytics tables ───────────────────────────────────
    analytics_steps = [
        ('fleet_health_summary',   _T_FHS, _sql_fleet_health_summary(scaling_constant)),
        ('fleet_dtc_distribution', _T_FDD, _sql_fleet_dtc_distribution()),
        ('fleet_system_health',    _T_FSH, _sql_fleet_system_health()),
        ('fleet_fault_trends',     _T_FFT, _sql_fleet_fault_trends(scaling_constant)),
        ('vehicle_health_summary', _T_VHS, _sql_vehicle_health_summary(scaling_constant)),
        ('dtc_fleet_impact',       _T_DFI, _sql_dtc_fleet_impact()),
        ('maintenance_priority',   _T_MP,  _sql_maintenance_priority()),
    ]
    counts: dict[str, int] = {}
    for label, table, sql in analytics_steps:
        log.info('Step 7+: Building %s …', label)
        _exec(client, f'TRUNCATE TABLE {table}')
        _exec(client, sql, sql_settings)
        c = _count(client, table)
        counts[label] = c
        log.info('%s: %d rows', label, c)

    # ── 10. Co-occurrence ───────────────────────────────────────────────
    log.info('Step 10: Computing DTC co-occurrence (self-join in ClickHouse) …')
    _exec(client, f'TRUNCATE TABLE {_T_COO}')
    _exec(client, _sql_cooccurrence(), {**sql_settings, 'max_execution_time': 1800})
    coo_count = _count(client, _T_COO)
    log.info('dtc_cooccurrence: %d rows', coo_count)

    # ── 11. Cleanup temp tables ─────────────────────────────────────────
    for tmp in ('_tmp_vehicle_risk_ravi', '_tmp_episode_resolved_ravi',
                '_tmp_episode_agg_ravi',
                '_tmp_events_episode_ravi', '_tmp_events_break_ravi',
                '_tmp_events_lag_ravi', '_tmp_engine_ts_ravi'):
        _exec(client, f'DROP TABLE IF EXISTS {tmp}')
    log.info('Temp tables cleaned up.')

    # ── 12. CDC bookkeeping ─────────────────────────────────────────────
    summary = {
        'dtc_master': dtc_count,
        'vehicle_master': vehicle_count,
        'dtc_events_exploded': exploded_count,
        'vehicle_fault_master': vfm_count,
        **counts,
        'dtc_cooccurrence': coo_count,
    }

    if cdc_enabled:
        source_watermarks_current = _collect_source_watermarks_v2(client)
        upsert_cdc_checkpoint(client, pipeline_name, run_window_end)
        upsert_cdc_source_watermarks(client, pipeline_name, source_watermarks_current)
        total_output = sum(summary.values())
        finish_cdc_run_log(
            client,
            run_id=run_id or 'unknown',
            pipeline_name=pipeline_name,
            status='success',
            window_start=run_window_start,
            window_end=run_window_end,
            source_rows=exploded_count,
            output_rows=total_output,
            message='v2 SQL pipeline completed',
        )

    log.info('v2 analytics pipeline completed: %s', summary)
    return summary


def _collect_source_watermarks_v2(client) -> dict[str, datetime]:
    table_map = {
        'dtc_history': os.getenv('DTC_HISTORY_TABLE') or 'ss_dtc_data_history',
        'vehicle_profile': os.getenv('VEHICLE_PROFILE_TABLE') or 'vehicle_profile_ss',
        'engine_cycles': os.getenv('ENGINE_CYCLES_TABLE') or 'ss_engineoncycles',
        'dtc_codes': os.getenv('DTC_CODES_TABLE') or 'dtc_codes_updated',
    }
    candidate_columns = {
        'dtc_history': ['updatedAt', 'updated_at_source', 'createdAt', 'ts', 'ingestion_ts'],
        'vehicle_profile': ['updatedAt', 'updated_at', 'createdAt', 'created_at', 'ingestion_ts'],
        'engine_cycles': ['updatedAt', 'updated_at', 'createdAt', 'created_at', 'cycle_end_ts', 'ts'],
        'dtc_codes': ['updatedAt', 'updated_at', 'createdAt', 'created_at', 'ingestion_ts'],
    }
    output = {}
    for source_name, table_name in table_map.items():
        watermark = get_table_watermark(client, table_name, candidate_columns.get(source_name, []))
        if watermark is not None:
            output[source_name] = watermark
    return output
