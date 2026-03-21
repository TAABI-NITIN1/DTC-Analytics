"""ClickHouse table definitions and data operations for the v2 analytics platform.

Two master tables + eight derived analytics tables.
Tenant key: clientLoginId from vehicle_profile_ss.
Vehicle filter: solutionType IN ('obd_solution','obd_analog_solution','obd_fuel+fuel_solution').
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone

from src.clickhouse_utils import (
    CDC_TABLES,
    get_clickhouse_client,
    get_cdc_checkpoint,
    get_cdc_source_watermarks,
    get_table_watermark,
    upsert_cdc_checkpoint,
    upsert_cdc_source_watermarks,
    start_cdc_run_log,
    finish_cdc_run_log,
    ensure_cdc_infrastructure,
)

# ---------------------------------------------------------------------------
# Table name registry
# ---------------------------------------------------------------------------

V2_TABLES = {
    # Dimension / lookup
    'dtc_master': 'dtc_master_ravi_v2',
    'vehicle_master': 'vehicle_master_ravi_v2',
    # Processing
    'dtc_events_exploded': 'dtc_events_exploded_ravi_v2',
    # Operational master
    'vehicle_fault_master': 'vehicle_fault_master_ravi_v2',
    # Analytics (derived)
    'fleet_health_summary': 'fleet_health_summary_ravi_v2',
    'fleet_dtc_distribution': 'fleet_dtc_distribution_ravi_v2',
    'fleet_system_health': 'fleet_system_health_ravi_v2',
    'fleet_fault_trends': 'fleet_fault_trends_ravi_v2',
    'vehicle_health_summary': 'vehicle_health_summary_ravi_v2',
    'dtc_fleet_impact': 'dtc_fleet_impact_ravi_v2',
    'maintenance_priority': 'maintenance_priority_ravi_v2',
    'dtc_cooccurrence': 'dtc_cooccurrence_ravi_v2',
}

VALID_SOLUTION_TYPES = (
    'obd_solution',
    'obd_analog_solution',
    'obd_fuel+fuel_solution',
)

# ---------------------------------------------------------------------------
# DDL: Dimension / Lookup tables
# ---------------------------------------------------------------------------

_DDL_DTC_MASTER = """
CREATE TABLE IF NOT EXISTS {table} (
    dtc_code              String,
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    description           String        DEFAULT '',
    primary_cause         String        DEFAULT '',
    secondary_causes      String        DEFAULT '',
    symptoms              String        DEFAULT '',
    impact_if_unresolved  String        DEFAULT '',
    fuel_mileage_impact   String        DEFAULT '',
    vehicle_health_impact String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    safety_risk_level     String        DEFAULT '',
    action_required       String        DEFAULT '',
    repair_complexity     String        DEFAULT '',
    estimated_repair_hours Float32      DEFAULT 0,
    driver_related        UInt8         DEFAULT 0,
    driver_behaviour_category  String   DEFAULT '',
    driver_behaviour_trigger   String   DEFAULT '',
    driver_training_required   UInt8    DEFAULT 0,
    fleet_management_action    String   DEFAULT '',
    recommended_preventive_action String DEFAULT '',
    oem_specific          UInt8         DEFAULT 0,
    manufacturer_notes    String        DEFAULT '',
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dtc_code)
"""

_DDL_VEHICLE_MASTER = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    customer_name         String        DEFAULT '',
    model                 String        DEFAULT '',
    manufacturing_year    UInt16        DEFAULT 0,
    vehicle_type          String        DEFAULT '',
    solutionType          String        DEFAULT '',
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid)
"""

# ---------------------------------------------------------------------------
# DDL: Processing table
# ---------------------------------------------------------------------------

_DDL_DTC_EVENTS_EXPLODED = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    ts                    UInt32,
    dtc_code              String,
    lat                   Float64       DEFAULT 0,
    lng                   Float64       DEFAULT 0,
    dtc_pgn               String        DEFAULT '',
    created_at            DateTime      DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(toDateTime(ts))
ORDER BY (clientLoginId, uniqueid, dtc_code, ts)
"""

# ---------------------------------------------------------------------------
# DDL: Operational master
# ---------------------------------------------------------------------------

_DDL_VEHICLE_FAULT_MASTER = """
CREATE TABLE IF NOT EXISTS {table} (
    episode_id            String,
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    customer_name         String        DEFAULT '',
    model                 String        DEFAULT '',
    manufacturing_year    UInt16        DEFAULT 0,
    dtc_code              String,
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    description           String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    first_ts              UInt32,
    last_ts               UInt32,
    event_date            Date,
    occurrence_count      UInt32        DEFAULT 0,
    resolution_time_sec   UInt32        DEFAULT 0,
    is_resolved           UInt8         DEFAULT 0,
    resolution_reason     String        DEFAULT '',
    gap_from_previous_episode UInt32    DEFAULT 0,
    engine_cycles_during  UInt32        DEFAULT 0,
    driver_related        UInt8         DEFAULT 0,
    has_engine_issue      UInt8         DEFAULT 0,
    has_coolant_issue     UInt8         DEFAULT 0,
    has_safety_issue      UInt8         DEFAULT 0,
    has_emission_issue    UInt8         DEFAULT 0,
    has_electrical_issue  UInt8         DEFAULT 0,
    vehicle_health_score  Float32       DEFAULT 100,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (clientLoginId, uniqueid, dtc_code, first_ts)
"""

# ---------------------------------------------------------------------------
# DDL: Derived analytics tables
# ---------------------------------------------------------------------------

_DDL_FLEET_HEALTH_SUMMARY = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId                String,
    total_vehicles               UInt32  DEFAULT 0,
    vehicles_with_active_faults  UInt32  DEFAULT 0,
    vehicles_with_critical_faults UInt32 DEFAULT 0,
    driver_related_faults        UInt32  DEFAULT 0,
    fleet_health_score           Float32 DEFAULT 100,
    most_common_dtc              String  DEFAULT '',
    most_common_system           String  DEFAULT '',
    active_fault_trend           String  DEFAULT 'stable',
    created_at                   DateTime DEFAULT now(),
    updated_at                   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId)
"""

_DDL_FLEET_DTC_DISTRIBUTION = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    dtc_code              String,
    description           String        DEFAULT '',
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    vehicles_affected     UInt32        DEFAULT 0,
    active_vehicles       UInt32        DEFAULT 0,
    total_occurrences     UInt64        DEFAULT 0,
    total_episodes        UInt32        DEFAULT 0,
    avg_resolution_time   Float32       DEFAULT 0,
    driver_related_count  UInt32        DEFAULT 0,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code)
"""

_DDL_FLEET_SYSTEM_HEALTH = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    system                String,
    vehicles_affected     UInt32        DEFAULT 0,
    active_faults         UInt32        DEFAULT 0,
    critical_faults       UInt32        DEFAULT 0,
    risk_score            Float32       DEFAULT 0,
    trend                 String        DEFAULT 'stable',
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, system)
"""

_DDL_FLEET_FAULT_TRENDS = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    date                  Date,
    active_faults         UInt32        DEFAULT 0,
    critical_faults       UInt32        DEFAULT 0,
    new_faults            UInt32        DEFAULT 0,
    resolved_faults       UInt32        DEFAULT 0,
    driver_related_faults UInt32        DEFAULT 0,
    fleet_health_score    Float32       DEFAULT 100,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (clientLoginId, date)
"""

_DDL_VEHICLE_HEALTH_SUMMARY = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    customer_name         String        DEFAULT '',
    active_fault_count    UInt32        DEFAULT 0,
    critical_fault_count  UInt32        DEFAULT 0,
    total_episodes        UInt32        DEFAULT 0,
    episodes_last_30_days UInt32        DEFAULT 0,
    avg_resolution_time   Float32       DEFAULT 0,
    last_fault_ts         UInt32        DEFAULT 0,
    vehicle_health_score  Float32       DEFAULT 100,
    driver_related_faults UInt32        DEFAULT 0,
    most_common_dtc       String        DEFAULT '',
    has_engine_issue      UInt8         DEFAULT 0,
    has_emission_issue    UInt8         DEFAULT 0,
    has_safety_issue      UInt8         DEFAULT 0,
    has_electrical_issue  UInt8         DEFAULT 0,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid)
"""

_DDL_DTC_FLEET_IMPACT = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    dtc_code              String,
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    vehicles_affected     UInt32        DEFAULT 0,
    active_vehicles       UInt32        DEFAULT 0,
    avg_resolution_time   Float32       DEFAULT 0,
    driver_related_ratio  Float32       DEFAULT 0,
    fleet_risk_score      Float32       DEFAULT 0,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code)
"""

_DDL_MAINTENANCE_PRIORITY = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    dtc_code              String,
    description           String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    fault_duration_sec    UInt32        DEFAULT 0,
    episodes_last_30_days UInt32        DEFAULT 0,
    maintenance_priority_score Float32  DEFAULT 0,
    recommended_action    String        DEFAULT '',
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid, dtc_code)
"""

_DDL_DTC_COOCCURRENCE = """
CREATE TABLE IF NOT EXISTS {table} (
    clientLoginId         String,
    dtc_code_a            String,
    dtc_code_b            String,
    cooccurrence_count    UInt32        DEFAULT 0,
    vehicles_affected     UInt32        DEFAULT 0,
    avg_time_gap_sec      Float32       DEFAULT 0,
    last_seen_ts          UInt32        DEFAULT 0,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code_a, dtc_code_b)
"""

# Ordered list for ensure_v2_tables()
_DDL_REGISTRY = [
    ('dtc_master', _DDL_DTC_MASTER),
    ('vehicle_master', _DDL_VEHICLE_MASTER),
    ('dtc_events_exploded', _DDL_DTC_EVENTS_EXPLODED),
    ('vehicle_fault_master', _DDL_VEHICLE_FAULT_MASTER),
    ('fleet_health_summary', _DDL_FLEET_HEALTH_SUMMARY),
    ('fleet_dtc_distribution', _DDL_FLEET_DTC_DISTRIBUTION),
    ('fleet_system_health', _DDL_FLEET_SYSTEM_HEALTH),
    ('fleet_fault_trends', _DDL_FLEET_FAULT_TRENDS),
    ('vehicle_health_summary', _DDL_VEHICLE_HEALTH_SUMMARY),
    ('dtc_fleet_impact', _DDL_DTC_FLEET_IMPACT),
    ('maintenance_priority', _DDL_MAINTENANCE_PRIORITY),
    ('dtc_cooccurrence', _DDL_DTC_COOCCURRENCE),
]


# ---------------------------------------------------------------------------
# Table creation
# ---------------------------------------------------------------------------

def ensure_v2_tables(client=None):
    """Create all v2 tables if they don't exist."""
    if client is None:
        client = get_clickhouse_client()
    for key, ddl_template in _DDL_REGISTRY:
        table_name = V2_TABLES[key]
        client.execute(ddl_template.format(table=table_name))
    client.execute(
        f"ALTER TABLE {V2_TABLES['fleet_fault_trends']} "
        "ADD COLUMN IF NOT EXISTS critical_faults UInt32 DEFAULT 0 "
        "AFTER active_faults"
    )
    logging.info('All v2 tables ensured.')


# ---------------------------------------------------------------------------
# Data insertion helpers
# ---------------------------------------------------------------------------

def _insert_batch(client, table: str, columns: list[str], rows: list[tuple], batch_size: int = 50_000):
    """Insert rows into table in batches."""
    if not rows:
        return
    col_list = ', '.join(columns)
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        client.execute(
            f'INSERT INTO {table} ({col_list}) VALUES',
            batch,
        )


def populate_dtc_master(client=None):
    """Seed dtc_master_ravi_v2 from dtc_codes_updated."""
    if client is None:
        client = get_clickhouse_client()
    table = V2_TABLES['dtc_master']
    now_utc = datetime.now(timezone.utc)

    # Fetch from source
    from src.ingest import fetch_dtc_codes
    from src.analytics_pipeline import _normalize_codes
    codes_df = _normalize_codes(fetch_dtc_codes())

    if codes_df.empty:
        logging.warning('No DTC codes found in source table')
        return 0

    rows = []
    for r in codes_df.itertuples(index=False):
        rows.append((
            str(r.dtc_code),
            str(getattr(r, 'system', '') or ''),
            str(getattr(r, 'subsystem', '') or ''),
            str(getattr(r, 'description', '') or ''),
            int(getattr(r, 'severity_level', 1) or 1),
            now_utc,
            now_utc,
        ))

    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(client, table,
                  ['dtc_code', 'system', 'subsystem', 'description', 'severity_level', 'created_at', 'updated_at'],
                  rows)
    logging.info('dtc_master populated: %d codes', len(rows))
    return len(rows)


def populate_vehicle_master(client=None):
    """Seed vehicle_master_ravi_v2 from vehicle profile, filtered by solutionType."""
    if client is None:
        client = get_clickhouse_client()
    table = V2_TABLES['vehicle_master']
    now_utc = datetime.now(timezone.utc)

    profile_table = (os.getenv('VEHICLE_PROFILE_TABLE') or 'aimm.vehicle_profile').strip()
    history_table = (os.getenv('DTC_HISTORY_TABLE') or 'aimm.dtc_data_history').strip()
    try:
        describe_rows = client.execute(f'DESCRIBE TABLE {profile_table}')
        columns = {str(row[0]) for row in describe_rows}
    except Exception:
        logging.warning('Could not describe %s — fallback to extracting from %s', profile_table, history_table)
        try:
            rows_raw = client.execute(f"""
                SELECT DISTINCT toString(uniqueid) AS uniqueid, '' AS vehicle_number, '' AS customer_name,
                       '' AS model, 0 AS mfg_year, '' AS vehicle_type, 'obd_solution' AS solution_type,
                       'unknown' AS client_login_id
                FROM {history_table}
            """)
            if not rows_raw:
                logging.warning('No vehicles found in DTC events fallback')
                return 0
            client.execute(f'TRUNCATE TABLE {table}')
            rows = []
            for r in rows_raw:
                uniqueid, vehicle_number, customer_name, model, mfg_year, vehicle_type, solution_type, client_login_id = r
                rows.append((
                    str(client_login_id),
                    str(uniqueid),
                    str(vehicle_number),
                    str(customer_name),
                    str(model),
                    int(mfg_year),
                    str(vehicle_type),
                    str(solution_type),
                    now_utc,
                    now_utc,
                ))
            _insert_batch(client, table,
                          ['clientLoginId', 'uniqueid', 'vehicle_number', 'customer_name', 'model',
                           'manufacturing_year', 'vehicle_type', 'solutionType', 'created_at', 'updated_at'],
                          rows)
            logging.info('vehicle_master populated from events fallback: %d vehicles', len(rows))
            return len(rows)
        except Exception:
            logging.exception('Fallback also failed')
            return 0

    from src.ingest import _first_present

    uniqueid_col = _first_present(columns, ['uniqueid', 'deviceUniqueId_fk', 'deviceuniqueid_fk', 'unique_id'])
    vehicle_number_col = _first_present(columns, ['vehicleNumber', 'vehicle_number', 'registration_number', 'vehicle_no'])
    model_col = _first_present(columns, ['vehicleModel', 'vehicle_model', 'model'])
    year_col = _first_present(columns, ['manufacturingYear', 'manufacturing_year', 'model_year'])
    vehicle_type_col = _first_present(columns, ['vehicleType', 'vehicle_type'])
    customer_name_col = _first_present(columns, ['clientName_client', 'client_name', 'customer_name'])
    client_login_col = _first_present(columns, ['clientLoginId', 'client_login_id'])
    solution_type_col = _first_present(columns, ['solutionType', 'solution_type'])

    if not uniqueid_col:
        logging.warning('No uniqueid column in %s', profile_table)
        return 0

    col_types = {str(r[0]): str(r[1]).lower() for r in describe_rows}
    select_parts = [
        f'toString({uniqueid_col}) AS uniqueid',
        f"any(toString({vehicle_number_col}))" if vehicle_number_col else "''",
        f"any(toString({customer_name_col}))" if customer_name_col else "''",
        f"any(toString({model_col}))" if model_col else "''",
    ]

    if year_col:
        if 'string' in col_types.get(year_col, ''):
            select_parts.append(f'max(toUInt16OrZero({year_col}))')
        else:
            select_parts.append(f'max(toUInt16(ifNull({year_col}, 0)))')
    else:
        select_parts.append('toUInt16(0)')

    select_parts.append(f"any(toString({vehicle_type_col}))" if vehicle_type_col else "''")
    select_parts.append(f"any(toString({solution_type_col}))" if solution_type_col else "''")
    select_parts.append(f"any(toString({client_login_col}))" if client_login_col else "''")

    solution_filter = ''
    if solution_type_col:
        placeholders = ', '.join(f"'{s}'" for s in VALID_SOLUTION_TYPES)
        solution_filter = f'WHERE {solution_type_col} IN ({placeholders})'

    query = f'''
        SELECT {", ".join(select_parts)}
        FROM {profile_table}
        {solution_filter}
        GROUP BY uniqueid
    '''

    rows_raw = client.execute(query)
    if not rows_raw:
        logging.warning('No vehicles found matching solutionType filter in %s', profile_table)

    rows_by_uniqueid = {}
    for r in rows_raw:
        uniqueid, vehicle_number, customer_name, model, mfg_year, vehicle_type, solution_type, client_login_id = r
        rows_by_uniqueid[str(uniqueid)] = (
            str(client_login_id),
            str(uniqueid),
            str(vehicle_number),
            str(customer_name),
            str(model),
            int(mfg_year),
            str(vehicle_type),
            str(solution_type),
            now_utc,
            now_utc,
        )

    rows = list(rows_by_uniqueid.values())
    if not rows:
        logging.warning('No vehicles found in either %s or %s', profile_table, history_table)
        return 0

    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(client, table,
                  ['clientLoginId', 'uniqueid', 'vehicle_number', 'customer_name', 'model',
                   'manufacturing_year', 'vehicle_type', 'solutionType', 'created_at', 'updated_at'],
                  rows)
    logging.info(
        'vehicle_master populated: %d vehicles from filtered vehicle profile',
        len(rows),
    )
    return len(rows)


# ---------------------------------------------------------------------------
# Analytics table writers (full replace per run)
# ---------------------------------------------------------------------------

def write_vehicle_fault_master(client, rows: list[tuple]):
    """Full replace of vehicle_fault_master."""
    table = V2_TABLES['vehicle_fault_master']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        [
            'episode_id', 'clientLoginId', 'uniqueid', 'vehicle_number', 'customer_name',
            'model', 'manufacturing_year', 'dtc_code', 'system', 'subsystem', 'description',
            'severity_level', 'first_ts', 'last_ts', 'event_date', 'occurrence_count',
            'resolution_time_sec', 'is_resolved', 'resolution_reason',
            'gap_from_previous_episode', 'engine_cycles_during', 'driver_related',
            'has_engine_issue', 'has_coolant_issue', 'has_safety_issue',
            'has_emission_issue', 'has_electrical_issue', 'vehicle_health_score',
            'created_at', 'updated_at',
        ],
        rows,
    )


def write_dtc_events_exploded(client, rows: list[tuple]):
    """Full replace of dtc_events_exploded."""
    table = V2_TABLES['dtc_events_exploded']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'uniqueid', 'vehicle_number', 'ts', 'dtc_code', 'lat', 'lng', 'dtc_pgn', 'created_at'],
        rows,
    )


def write_fleet_health_summary(client, rows: list[tuple]):
    table = V2_TABLES['fleet_health_summary']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'total_vehicles', 'vehicles_with_active_faults',
         'vehicles_with_critical_faults', 'driver_related_faults', 'fleet_health_score',
         'most_common_dtc', 'most_common_system', 'active_fault_trend',
         'created_at', 'updated_at'],
        rows,
    )


def write_fleet_dtc_distribution(client, rows: list[tuple]):
    table = V2_TABLES['fleet_dtc_distribution']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'dtc_code', 'description', 'system', 'subsystem', 'severity_level',
         'vehicles_affected', 'active_vehicles', 'total_occurrences', 'total_episodes',
         'avg_resolution_time', 'driver_related_count', 'created_at', 'updated_at'],
        rows,
    )


def write_fleet_system_health(client, rows: list[tuple]):
    table = V2_TABLES['fleet_system_health']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'system', 'vehicles_affected', 'active_faults', 'critical_faults',
         'risk_score', 'trend', 'created_at', 'updated_at'],
        rows,
    )


def write_fleet_fault_trends(client, rows: list[tuple]):
    table = V2_TABLES['fleet_fault_trends']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'date', 'active_faults', 'critical_faults', 'new_faults', 'resolved_faults',
         'driver_related_faults', 'fleet_health_score', 'created_at', 'updated_at'],
        rows,
    )


def write_vehicle_health_summary(client, rows: list[tuple]):
    table = V2_TABLES['vehicle_health_summary']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'uniqueid', 'vehicle_number', 'customer_name',
         'active_fault_count', 'critical_fault_count', 'total_episodes',
         'episodes_last_30_days', 'avg_resolution_time', 'last_fault_ts',
         'vehicle_health_score', 'driver_related_faults', 'most_common_dtc',
         'has_engine_issue', 'has_emission_issue', 'has_safety_issue',
         'has_electrical_issue', 'created_at', 'updated_at'],
        rows,
    )


def write_dtc_fleet_impact(client, rows: list[tuple]):
    table = V2_TABLES['dtc_fleet_impact']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'dtc_code', 'system', 'subsystem', 'vehicles_affected',
         'active_vehicles', 'avg_resolution_time', 'driver_related_ratio',
         'fleet_risk_score', 'created_at', 'updated_at'],
        rows,
    )


def write_maintenance_priority(client, rows: list[tuple]):
    table = V2_TABLES['maintenance_priority']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'uniqueid', 'vehicle_number', 'dtc_code', 'description',
         'severity_level', 'fault_duration_sec', 'episodes_last_30_days',
         'maintenance_priority_score', 'recommended_action', 'created_at', 'updated_at'],
        rows,
    )


def write_dtc_cooccurrence(client, rows: list[tuple]):
    table = V2_TABLES['dtc_cooccurrence']
    client.execute(f'TRUNCATE TABLE {table}')
    _insert_batch(
        client, table,
        ['clientLoginId', 'dtc_code_a', 'dtc_code_b', 'cooccurrence_count',
         'vehicles_affected', 'avg_time_gap_sec', 'last_seen_ts',
         'created_at', 'updated_at'],
        rows,
    )
