from clickhouse_driver import Client
import clickhouse_connect
import logging
import re
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from src.config import get_clickhouse_cfg, get_vehicle_clickhouse_cfg


ANALYTICS_TABLES = {
    'normalized_dtc_events': 'normalized_dtc_events_ravi',
    'vehicle_fault_episodes': 'vehicle_fault_episodes_ravi',
    'vehicle_current_status': 'vehicle_current_status_ravi',
    'dtc_daily_stats': 'dtc_daily_stats_ravi',
    'fleet_daily_stats': 'fleet_daily_stats_ravi',
}

CDC_TABLES = {
    'exploded_source': 'ss_dtc_data_history_exploded_ravi',
    'checkpoint': 'cdc_checkpoint_ravi',
    'run_log': 'cdc_run_log_ravi',
    'source_watermark': 'cdc_source_watermark_ravi',
}


def _format_clickhouse_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return '1' if value else '0'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (list, tuple, set)):
        inner = ', '.join(_format_clickhouse_literal(v) for v in value)
        return f'({inner})'
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def _apply_pyformat_params(query: str, params: dict | None) -> str:
    if not params:
        return query
    rendered = query
    for key, value in params.items():
        rendered = rendered.replace(f"%({key})s", _format_clickhouse_literal(value))
    return rendered


class ClickHouseHTTPAdapter:
    def __init__(self, client, *, host=None, port=None, user=None, password=None, database=None):
        self._client = client
        self._conn_params = dict(host=host, port=port, user=user, password=password, database=database)

    def _reconnect(self):
        p = self._conn_params
        if p.get('host'):
            logging.info('Reconnecting to ClickHouse %s:%s ...', p['host'], p['port'])
            self._client = clickhouse_connect.get_client(
                host=p['host'],
                port=int(p['port'] or 8123),
                username=p['user'],
                password=p['password'],
                database=p['database'],
                send_receive_timeout=600,
                query_limit=0,
                compress=False,
            )

    def _execute_insert(self, query: str, rows):
        match = re.search(
            r"INSERT\s+INTO\s+([`\w\.]+)\s*\(([^\)]*)\)\s*VALUES",
            query,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not match:
            raise ValueError('Unsupported INSERT format for HTTP adapter')

        table = match.group(1).replace('`', '')
        raw_cols = match.group(2)
        column_names = [column.strip().strip('`') for column in raw_cols.split(',') if column.strip()]
        for attempt in range(3):
            try:
                self._client.insert(table=table, data=rows, column_names=column_names)
                return []
            except Exception as exc:
                err_str = str(exc).lower()
                exc_type = type(exc).__name__.lower()
                retryable = ('stream' in err_str or 'incompleteread' in err_str
                             or 'connection' in err_str
                             or 'stream' in exc_type or 'protocol' in exc_type
                             or 'incomplete' in exc_type or 'reset' in err_str)
                if attempt < 2 and retryable:
                    import time
                    logging.warning('ClickHouse insert error (attempt %d/3), retrying in 5s: %s [%s]', attempt + 1, type(exc).__name__, exc)
                    time.sleep(5)
                    self._reconnect()
                    continue
                raise

    def query_df(self, query: str):
        """Execute SELECT and return (column_names, result_rows)."""
        rendered = _apply_pyformat_params(query, None)
        for attempt in range(3):
            try:
                result = self._client.query(
                    rendered,
                    settings={'max_block_size': '65536'},
                )
                return result.column_names, result.result_rows
            except Exception as exc:
                err_str = str(exc).lower()
                exc_type = type(exc).__name__.lower()
                retryable = ('stream' in err_str or 'incompleteread' in err_str
                             or 'connection' in err_str
                             or 'stream' in exc_type or 'protocol' in exc_type
                             or 'incomplete' in exc_type or 'reset' in err_str)
                if attempt < 2 and retryable:
                    import time
                    logging.warning('ClickHouse query_df error (attempt %d/3), retrying: %s', attempt + 1, type(exc).__name__)
                    time.sleep(5)
                    self._reconnect()
                    continue
                raise

    def execute(self, query: str, params=None, settings=None):
        if isinstance(params, list):
            return self._execute_insert(query, params)

        rendered_query = _apply_pyformat_params(query, params if isinstance(params, dict) else None)
        upper_query = rendered_query.lstrip().upper()

        if upper_query.startswith('SELECT') or upper_query.startswith('SHOW') or upper_query.startswith('DESCRIBE') or upper_query.startswith('EXISTS') or upper_query.startswith('WITH'):
            query_settings = {
                'max_block_size': '65536',
                'max_execution_time': '300',
                'send_progress_in_http_headers': '0',
            }
            if settings:
                query_settings.update({k: str(v) for k, v in settings.items()})
            for attempt in range(3):
                try:
                    result = self._client.query(
                        rendered_query,
                        settings=query_settings,
                    )
                    return result.result_rows
                except Exception as exc:
                    err_str = str(exc).lower()
                    exc_type = type(exc).__name__.lower()
                    retryable = ('stream' in err_str or 'incompleteread' in err_str
                                 or 'connection' in err_str
                                 or 'stream' in exc_type or 'protocol' in exc_type
                                 or 'incomplete' in exc_type or 'reset' in err_str)
                    if attempt < 2 and retryable:
                        import time
                        logging.warning('ClickHouse query error (attempt %d/3), retrying in 5s: %s [%s]', attempt + 1, type(exc).__name__, exc)
                        time.sleep(5)
                        self._reconnect()
                        continue
                    raise

        cmd_settings = {k: str(v) for k, v in settings.items()} if settings else None
        for attempt in range(3):
            try:
                self._client.command(rendered_query, settings=cmd_settings or {})
                return []
            except Exception as exc:
                err_str = str(exc).lower()
                exc_type = type(exc).__name__.lower()
                retryable = ('stream' in err_str or 'incompleteread' in err_str
                             or 'connection' in err_str
                             or 'stream' in exc_type or 'protocol' in exc_type
                             or 'incomplete' in exc_type or 'reset' in err_str)
                if attempt < 2 and retryable:
                    import time
                    logging.warning('ClickHouse command error (attempt %d/3), retrying in 5s: %s [%s]', attempt + 1, type(exc).__name__, exc)
                    time.sleep(5)
                    self._reconnect()
                    continue
                raise


def _build_clickhouse_client(host, port, user, password, database):
    effective_port = int(port or 8123)

    if effective_port == 8123:
        http_client = clickhouse_connect.get_client(
            host=host,
            port=effective_port,
            username=user,
            password=password,
            database=database,
            send_receive_timeout=600,
            query_limit=0,
            compress=False,
        )
        return ClickHouseHTTPAdapter(http_client, host=host, port=effective_port, user=user, password=password, database=database)

    return Client(
        host=host,
        port=effective_port,
        user=user,
        password=password,
        database=database,
    )


def get_clickhouse_client(host=None, port=None, user=None, password=None, database=None):
    cfg = get_clickhouse_cfg()
    return _build_clickhouse_client(
        host=host or cfg.get('host'),
        port=port or cfg.get('port') or 8123,
        user=user or cfg.get('user'),
        password=password or cfg.get('password'),
        database=database or cfg.get('database'),
    )


def get_vehicle_clickhouse_client(host=None, port=None, user=None, password=None, database=None):
    cfg = get_vehicle_clickhouse_cfg()

    # VEHICLE_CH_DB is optional; fall back to primary CH_DB when missing
    primary_cfg = get_clickhouse_cfg()

    effective_host = host or cfg.get('host') or primary_cfg.get('host')
    effective_port = port or cfg.get('port') or primary_cfg.get('port')
    effective_user = user or cfg.get('user') or primary_cfg.get('user')
    effective_password = password or cfg.get('password') or primary_cfg.get('password')
    effective_database = database or cfg.get('database') or primary_cfg.get('database')

    return _build_clickhouse_client(
        host=effective_host,
        port=effective_port,
        user=effective_user,
        password=effective_password,
        database=effective_database,
    )


def ensure_tables(client: Client, reset: bool = False):
    # Creates minimal tables for events, episodes, state, and aggregates.
    # Adjust types and engines to your environment and scale.

    if reset:
        agg_tables = [
            'agg_known_unknown',
            'agg_unknown_priority',
            'agg_vehicle_coverage',
            'agg_system_counts',
            'agg_subsystem_counts',
            'agg_system_severity',
            'agg_subsystem_severity',
            'agg_code_frequency',
            'agg_hourly_severity',
            'agg_daily_counts',
            'agg_vehicle_code_timeline',
            'fleet_kpis_daily',
            'fleet_active_vehicles_trend',
            'fleet_dtc_occurrence_trend',
            'fleet_severity_distribution_daily',
            'top_risk_vehicles_daily',
            'dtc_code_summary',
            'dtc_code_trend_daily',
            'vehicle_snapshot_daily',
            'vehicle_active_issues',
            'dtc_state_v2',
        ]
        for tbl in agg_tables:
            client.execute(f"DROP TABLE IF EXISTS {tbl}")

    client.execute("""
    CREATE TABLE IF NOT EXISTS raw_dtc_data_history (
        uniqueid String,
        ts UInt32,
        lat Float64,
        lng Float64,
        dtc_code String,
        dtc_pgn String,
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toDate(toDateTime(ts))
    ORDER BY (uniqueid, ts, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_events_exploded (
        uniqueid String,
        ts UInt32,
        lat Float64,
        lng Float64,
        dtc_code String,
        dtc_pgn String,
        ingestion_ts DateTime
    ) ENGINE = MergeTree()
    PARTITION BY toDate(toDateTime(ts))
    ORDER BY (uniqueid, dtc_code, ts)
    """)

    client.execute("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_dtc_explode
    TO dtc_events_exploded
    AS
    SELECT
        uniqueid,
        ts,
        lat,
        lng,
        arrayJoin(
            arrayFilter(
                x -> length(x) > 0 AND x != '0',
                splitByChar(
                    ',',
                    replaceRegexpAll(
                        replaceRegexpAll(
                            replaceRegexpAll(toString(dtc_code), '\\{', ''),
                            '\\}', ''
                        ),
                        '\\s', ''
                    )
                )
            )
        ) AS dtc_code,
        dtc_pgn,
        ingestion_ts
    FROM raw_dtc_data_history
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS events_enriched (
        uniqueid String,
        ts UInt32,
        TimeStamp DateTime,
        dtc_code String,
        dtc_pgn String,
        lat Float64,
        lng Float64,
        code String,
        description String,
        severity_level Int8,
        subsystem String,
        system String,
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toDate(TimeStamp)
    ORDER BY (uniqueid, dtc_code, ts)
    """)

    # Ensure schema stays aligned when the table already exists
    client.execute("ALTER TABLE events_enriched ADD COLUMN IF NOT EXISTS system String")

    client.execute("""
    CREATE TABLE IF NOT EXISTS episodes (
        uniqueid String,
        dtc_code String,
        episode_id UInt64,
        first_ts UInt32,
        last_ts UInt32,
        occurrences UInt32,
        persistence_sec UInt32,
        is_resolved UInt8,
        resolution_reason String,
        event_date Date
    ) ENGINE = MergeTree()
    PARTITION BY event_date
    ORDER BY (uniqueid, dtc_code, episode_id)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_state (
        uniqueid String,
        dtc_code String,
        last_dtc_ts UInt32,
        episode_id UInt64,
        last_engine_on_ts UInt32
    ) ENGINE = TinyLog()
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_state_v2 (
        uniqueid String,
        dtc_code String,
        last_dtc_ts UInt32,
        last_engine_on_ts UInt32,
        is_active UInt8,
        last_episode_id UInt64,
        update_ts DateTime
    ) ENGINE = ReplacingMergeTree(update_ts)
    ORDER BY (uniqueid, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS aggregates_hourly (
        event_hour DateTime,
        dtc_code String,
        total_occurrences UInt64
    ) ENGINE = MergeTree()
    PARTITION BY toDate(event_hour)
    ORDER BY (event_hour, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_known_unknown (
        bucket String,
        unique_codes UInt64,
        unique_vehicles UInt64,
        events UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY bucket
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_unknown_priority (
        dtc_code String,
        count UInt64,
        priority String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (priority, dtc_code, count)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_vehicle_coverage (
        bucket String,
        unique_vehicles UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY bucket
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_system_counts (
        system String,
        count UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY system
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_subsystem_counts (
        subsystem String,
        count UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY subsystem
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_system_severity (
        system String,
        severity_level Int8,
        count UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (system, severity_level)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_subsystem_severity (
        subsystem String,
        severity_level Int8,
        count UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (subsystem, severity_level)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_code_frequency (
        code String,
        occurrence_count UInt64,
        description String,
        symptoms String,
        causes String,
        severity_level Int8
    ) ENGINE = ReplacingMergeTree()
    ORDER BY code
    """)

    client.execute("ALTER TABLE agg_code_frequency ADD COLUMN IF NOT EXISTS severity_level Int8")

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_vehicle_code_timeline (
        event_date Date,
        uniqueid String,
        dtc_code String,
        occurrences UInt64
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY event_date
    ORDER BY (event_date, uniqueid, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_hourly_severity (
        event_hour DateTime,
        severity_level Int8,
        occurrences UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_hour, severity_level)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS agg_daily_counts (
        event_date Date,
        occurrences UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY event_date
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_master (
        uniqueid String,
        vehicle_number String,
        model String,
        manufacturing_year UInt16,
        customer_name String,
        tank_capacity Float32,
        updated_at DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(updated_at)
    ORDER BY uniqueid
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_kpis_daily (
        event_date Date,
        total_vehicles UInt64,
        active_vehicles UInt64,
        critical_vehicles UInt64,
        new_dtcs UInt64,
        resolved_dtcs UInt64,
        chronic_vehicles UInt64,
        health_score Float32,
        avg_resolution_sec Float32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY event_date
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_active_vehicles_trend (
        event_date Date,
        active_vehicles UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY event_date
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_dtc_occurrence_trend (
        event_date Date,
        occurrences UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY event_date
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_severity_distribution_daily (
        event_date Date,
        severity_level Int8,
        active_vehicles UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, severity_level)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS top_risk_vehicles_daily (
        event_date Date,
        uniqueid String,
        active_dtc_count UInt64,
        critical_dtc_count UInt64,
        longest_persistence_sec UInt64,
        recurrence_count UInt64,
        health_score Float32,
        last_engine_on_ts UInt32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, uniqueid)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_code_summary (
        event_date Date,
        dtc_code String,
        description String,
        subsystem String,
        severity_level Int8,
        vehicles_affected UInt64,
        active_vehicles UInt64,
        resolved_vehicles UInt64,
        avg_persistence_sec Float32,
        recurrence_rate Float32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_code_trend_daily (
        event_date Date,
        dtc_code String,
        occurrences UInt64,
        active_vehicles UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_snapshot_daily (
        event_date Date,
        uniqueid String,
        vehicle_number String,
        model String,
        manufacturing_year UInt16,
        customer_name String,
        tank_capacity Float32,
        health_score Float32,
        last_engine_on_ts UInt32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, uniqueid)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_active_issues (
        event_date Date,
        uniqueid String,
        dtc_code String,
        description String,
        severity_level Int8,
        days_active UInt16,
        occurrences UInt64,
        episodes UInt64,
        status String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, uniqueid, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_risk_snapshot_daily (
        event_date Date,
        total_vehicles UInt64,
        vehicles_with_active_faults UInt64,
        vehicles_with_critical_faults UInt64,
        vehicles_with_persistent_faults UInt64,
        new_fault_episodes UInt64,
        resolved_fault_episodes UInt64,
        fleet_health_score Float32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY event_date
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS fleet_fault_concentration_daily (
        event_date Date,
        dtc_code String,
        description String,
        subsystem String,
        severity_level Int8,
        occurrences UInt64,
        affected_vehicles UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_health_snapshot_daily (
        event_date Date,
        uniqueid String,
        health_score Float32,
        risk_badge String,
        active_fault_count UInt64,
        critical_fault_count UInt64,
        persistent_fault_count UInt64,
        recurring_fault_count UInt64,
        last_detected_ts UInt32,
        last_engine_on_ts UInt32
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, uniqueid)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS vehicle_fault_details_daily (
        event_date Date,
        uniqueid String,
        dtc_code String,
        description String,
        severity_level Int8,
        first_seen_ts UInt32,
        last_seen_ts UInt32,
        persistence_sec UInt64,
        occurrences UInt64,
        episodes UInt64,
        is_active UInt8
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, uniqueid, dtc_code)
    """)

    client.execute("""
    CREATE TABLE IF NOT EXISTS dtc_behavior_snapshot_daily (
        event_date Date,
        dtc_code String,
        description String,
        subsystem String,
        severity_level Int8,
        vehicles_affected UInt64,
        active_vehicles UInt64,
        resolved_vehicles UInt64,
        avg_persistence_sec Float32,
        recurrence_rate Float32,
        new_occurrences UInt64
    ) ENGINE = ReplacingMergeTree()
    ORDER BY (event_date, dtc_code)
    """)


def get_max_event_ts(client: Client) -> int | None:
    try:
        result = client.execute('SELECT max(ts) FROM events_enriched')
        if result and result[0][0] is not None:
            return int(result[0][0])
    except Exception:
        logging.exception('Failed to fetch max ts from ClickHouse')
    return None


def get_max_raw_ts(client: Client) -> int | None:
    try:
        result = client.execute('SELECT max(ts) FROM raw_dtc_data_history')
        if result and result[0][0] is not None:
            return int(result[0][0])
    except Exception:
        logging.exception('Failed to fetch max ts from raw_dtc_data_history')
    return None


def insert_events(client: Client, rows):
    # rows: iterable of tuples matching events_enriched columns
    if not rows:
        return
    client.execute(
        'INSERT INTO events_enriched (uniqueid, ts, TimeStamp, dtc_code, dtc_pgn, lat, lng, code, description, severity_level, subsystem, system, ingestion_ts) VALUES',
        rows,
    )


def insert_raw_dtc_history(client: Client, rows):
    if not rows:
        return
    client.execute(
        'INSERT INTO raw_dtc_data_history (uniqueid, ts, lat, lng, dtc_code, dtc_pgn, ingestion_ts) VALUES',
        rows,
    )


def insert_episodes(client: Client, rows):
    if not rows:
        return
    client.execute(
        'INSERT INTO episodes (uniqueid, dtc_code, episode_id, first_ts, last_ts, occurrences, persistence_sec, is_resolved, resolution_reason, event_date) VALUES',
        rows,
    )


def upsert_state(client: Client, rows):
    # Simple implementation: insert rows into dtc_state; for production consider Merge/Replace logic
    if not rows:
        return
    client.execute(
        'INSERT INTO dtc_state (uniqueid, dtc_code, last_dtc_ts, episode_id, last_engine_on_ts) VALUES',
        rows,
    )


def ensure_cdc_infrastructure(client: Client, source_table: str = 'ss_dtc_data_history'):
    exploded_table = CDC_TABLES['exploded_source']
    checkpoint_table = CDC_TABLES['checkpoint']
    run_log_table = CDC_TABLES['run_log']
    source_watermark_table = CDC_TABLES['source_watermark']

    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {exploded_table} (
        uniqueid String,
        ts UInt32,
        ts_dt DateTime64(3, 'UTC'),
        lat Float64,
        lng Float64,
        dtc_code String,
        dtc_pgn String,
        created_at_source Nullable(DateTime64(3, 'UTC')),
        updated_at_source Nullable(DateTime64(3, 'UTC')),
        can_raw_data Nullable(String),
        createdAt DateTime,
        updatedAt DateTime,
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toDate(ts_dt)
    ORDER BY (uniqueid, dtc_code, ts, updatedAt)
    """)

    client.execute(f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ss_dtc_data_history_exploded_ravi
    TO {exploded_table}
    AS
    SELECT
        toString(uniqueid) AS uniqueid,
        toUInt32(toUnixTimestamp(ts)) AS ts,
        ts AS ts_dt,
        toFloat64(lat) AS lat,
        toFloat64(lng) AS lng,
        arrayJoin(ifNull(dtc_code, [])) AS dtc_code,
        toString(dtc_pgn) AS dtc_pgn,
        created_at_source,
        updated_at_source,
        can_raw_data,
        createdAt,
        updatedAt,
        now() AS ingestion_ts
    FROM {source_table}
    """)

    exploded_count = client.execute(f'SELECT count() FROM {exploded_table}')[0][0]
    if int(exploded_count) == 0:
        bounds = client.execute(
            f"SELECT min(ts), max(ts) FROM {source_table}"
        )
        min_ts = bounds[0][0] if bounds else None
        max_ts = bounds[0][1] if bounds else None

        if min_ts is not None and max_ts is not None:
            start_month = datetime(min_ts.year, min_ts.month, 1)
            end_month = datetime(max_ts.year, max_ts.month, 1)

            current = start_month
            while current <= end_month:
                if current.month == 12:
                    next_month = datetime(current.year + 1, 1, 1)
                else:
                    next_month = datetime(current.year, current.month + 1, 1)

                client.execute(
                    f"""
                    INSERT INTO {exploded_table} (
                        uniqueid, ts, ts_dt, lat, lng, dtc_code, dtc_pgn,
                        created_at_source, updated_at_source, can_raw_data, createdAt, updatedAt, ingestion_ts
                    )
                    SELECT
                                                toString(src.uniqueid) AS uniqueid,
                                                toUInt32(toUnixTimestamp(src.ts)) AS ts,
                                                src.ts AS ts_dt,
                                                toFloat64(src.lat) AS lat,
                                                toFloat64(src.lng) AS lng,
                                                arrayJoin(ifNull(src.dtc_code, [])) AS dtc_code,
                                                toString(src.dtc_pgn) AS dtc_pgn,
                                                src.created_at_source,
                                                src.updated_at_source,
                                                src.can_raw_data,
                                                src.createdAt,
                                                src.updatedAt,
                        now() AS ingestion_ts
                                        FROM {source_table} AS src
                                        WHERE src.ts >= toDateTime64(%(window_start)s, 3, 'UTC')
                                            AND src.ts < toDateTime64(%(window_end)s, 3, 'UTC')
                    """,
                    {
                        'window_start': current.strftime('%Y-%m-%d %H:%M:%S'),
                        'window_end': next_month.strftime('%Y-%m-%d %H:%M:%S'),
                    },
                )

                current = next_month

    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {checkpoint_table} (
        pipeline_name String,
        watermark_updated_at DateTime64(3, 'UTC'),
        update_ts DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(update_ts)
    ORDER BY pipeline_name
    """)

    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {run_log_table} (
        run_id String,
        pipeline_name String,
        status String,
        window_start DateTime64(3, 'UTC'),
        window_end DateTime64(3, 'UTC'),
        source_rows UInt64,
        output_rows UInt64,
        message String,
        created_ts DateTime DEFAULT now(),
        update_ts DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(update_ts)
    ORDER BY (pipeline_name, run_id)
    """)

    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {source_watermark_table} (
        pipeline_name String,
        source_name String,
        watermark_ts DateTime64(3, 'UTC'),
        update_ts DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(update_ts)
    ORDER BY (pipeline_name, source_name)
    """)


def _to_utc_datetime(value) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def get_table_watermark(client: Client, table_name: str, candidate_columns: list[str]) -> datetime | None:
    try:
        describe_rows = client.execute(f'DESCRIBE TABLE {table_name}')
    except Exception:
        logging.exception('Could not describe source table %s for watermark', table_name)
        return None

    type_by_col = {str(row[0]): str(row[1]).lower() for row in describe_rows}
    chosen_col = None
    for column in candidate_columns:
        if column in type_by_col:
            chosen_col = column
            break

    if not chosen_col:
        return None

    col_type = type_by_col.get(chosen_col, '')
    try:
        if 'datetime' in col_type or col_type.startswith('date'):
            rows = client.execute(f'SELECT max({chosen_col}) FROM {table_name}')
        elif any(token in col_type for token in ['int', 'uint', 'float', 'decimal']):
            rows = client.execute(f'SELECT toDateTime(max({chosen_col})) FROM {table_name}')
        else:
            return None
    except Exception:
        logging.exception('Could not compute watermark for %s.%s', table_name, chosen_col)
        return None

    if not rows:
        return None
    return _to_utc_datetime(rows[0][0])


def get_cdc_source_watermarks(client: Client, pipeline_name: str) -> dict[str, datetime]:
    table_name = CDC_TABLES['source_watermark']
    rows = client.execute(
        f'''
        SELECT source_name, watermark_ts
        FROM {table_name}
        WHERE pipeline_name = %(pipeline_name)s
        ORDER BY update_ts DESC
        ''',
        {'pipeline_name': pipeline_name},
    )

    latest = {}
    for source_name, watermark_ts in rows:
        key = str(source_name)
        if key in latest:
            continue
        normalized = _to_utc_datetime(watermark_ts)
        if normalized is not None:
            latest[key] = normalized
    return latest


def upsert_cdc_source_watermarks(client: Client, pipeline_name: str, source_watermarks: dict[str, datetime]):
    if not source_watermarks:
        return

    table_name = CDC_TABLES['source_watermark']
    rows = []
    now_utc = datetime.now(timezone.utc)
    for source_name, watermark_ts in source_watermarks.items():
        normalized = _to_utc_datetime(watermark_ts)
        if normalized is None:
            continue
        rows.append((pipeline_name, str(source_name), normalized, now_utc))

    if not rows:
        return

    client.execute(
        f'''
        INSERT INTO {table_name} (pipeline_name, source_name, watermark_ts, update_ts)
        VALUES
        ''',
        rows,
    )


def get_cdc_checkpoint(client: Client, pipeline_name: str) -> datetime | None:
    table_name = CDC_TABLES['checkpoint']
    rows = client.execute(
        f"""
        SELECT watermark_updated_at
        FROM {table_name}
        WHERE pipeline_name = %(pipeline_name)s
        ORDER BY update_ts DESC
        LIMIT 1
        """,
        {'pipeline_name': pipeline_name},
    )
    if not rows:
        return None
    value = rows[0][0]
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value).replace('Z', '+00:00'))


def upsert_cdc_checkpoint(client: Client, pipeline_name: str, watermark_updated_at: datetime):
    table_name = CDC_TABLES['checkpoint']
    client.execute(
        f"""
        INSERT INTO {table_name} (pipeline_name, watermark_updated_at, update_ts)
        VALUES
        """,
        [(
            pipeline_name,
            watermark_updated_at,
            datetime.now(timezone.utc),
        )],
    )


def start_cdc_run_log(client: Client, pipeline_name: str, window_start: datetime, window_end: datetime) -> str:
    run_id = str(uuid4())
    table_name = CDC_TABLES['run_log']
    client.execute(
        f"""
        INSERT INTO {table_name} (
            run_id, pipeline_name, status, window_start, window_end,
            source_rows, output_rows, message, created_ts, update_ts
        ) VALUES
        """,
        [(
            run_id,
            pipeline_name,
            'running',
            window_start,
            window_end,
            0,
            0,
            '',
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )],
    )
    return run_id


def finish_cdc_run_log(
    client: Client,
    run_id: str,
    pipeline_name: str,
    status: str,
    window_start: datetime,
    window_end: datetime,
    source_rows: int,
    output_rows: int,
    message: str = '',
):
    table_name = CDC_TABLES['run_log']
    client.execute(
        f"""
        INSERT INTO {table_name} (
            run_id, pipeline_name, status, window_start, window_end,
            source_rows, output_rows, message, created_ts, update_ts
        ) VALUES
        """,
        [(
            run_id,
            pipeline_name,
            status,
            window_start,
            window_end,
            int(source_rows),
            int(output_rows),
            str(message or ''),
            datetime.now(timezone.utc),
            datetime.now(timezone.utc),
        )],
    )


def ensure_analytics_tables(client: Client):
    normalized_table = ANALYTICS_TABLES['normalized_dtc_events']
    episodes_table = ANALYTICS_TABLES['vehicle_fault_episodes']
    vehicle_status_table = ANALYTICS_TABLES['vehicle_current_status']
    dtc_daily_table = ANALYTICS_TABLES['dtc_daily_stats']
    fleet_daily_table = ANALYTICS_TABLES['fleet_daily_stats']

    client.execute("""
    CREATE TABLE IF NOT EXISTS {normalized_table} (
        uniqueid String,
        ts UInt32,
        dtc_code String,
        dtc_pgn String,
        severity_level Int8,
        subsystem String,
        system String,
        description String,
        lat Float64,
        lng Float64,
        customer_name String DEFAULT '',
        analysis_ts DateTime DEFAULT now(),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY toDate(toDateTime(ts))
    ORDER BY (uniqueid, dtc_code, ts)
    """.format(normalized_table=normalized_table))

    client.execute(f'ALTER TABLE {normalized_table} ADD COLUMN IF NOT EXISTS customer_name String DEFAULT \'\'') 
    client.execute(f'ALTER TABLE {normalized_table} ADD COLUMN IF NOT EXISTS analysis_ts DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {normalized_table} ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {normalized_table} ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()')

    client.execute("""
    CREATE TABLE IF NOT EXISTS {episodes_table} (
        uniqueid String,
        dtc_code String,
        episode_id UInt64,
        first_ts UInt32,
        last_ts UInt32,
        occurrences UInt32,
        resolution_time_sec UInt32,
        is_resolved UInt8,
        severity_level Int8,
        subsystem String,
        event_date Date,
        customer_name String DEFAULT '',
        analysis_ts DateTime DEFAULT now(),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY event_date
    ORDER BY (uniqueid, dtc_code, episode_id)
    """.format(episodes_table=episodes_table))

    # Keep compatibility with previously created table versions
    client.execute(f'ALTER TABLE {episodes_table} ADD COLUMN IF NOT EXISTS event_date Date')
    client.execute(f'ALTER TABLE {episodes_table} ADD COLUMN IF NOT EXISTS customer_name String DEFAULT \'\'') 
    client.execute(f'ALTER TABLE {episodes_table} ADD COLUMN IF NOT EXISTS analysis_ts DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {episodes_table} ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {episodes_table} ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()')

    client.execute("""
    CREATE TABLE IF NOT EXISTS {vehicle_status_table} (
        uniqueid String,
        active_fault_count UInt32,
        critical_fault_count UInt32,
        has_engine_issue UInt8,
        has_emission_issue UInt8,
        has_safety_issue UInt8,
        has_electrical_issue UInt8,
        health_score Float32,
        vehicle_number String,
        vehicle_model String,
        manufacturing_year UInt16,
        vehicle_type String,
        customer_name String,
        analysis_ts DateTime DEFAULT now(),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        snapshot_ts DateTime DEFAULT now()
    ) ENGINE = ReplacingMergeTree(snapshot_ts)
    ORDER BY uniqueid
    """.format(vehicle_status_table=vehicle_status_table))

    # Keep compatibility with previously created table versions
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS active_fault_count UInt32')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS critical_fault_count UInt32')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS has_engine_issue UInt8')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS has_emission_issue UInt8')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS has_safety_issue UInt8')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS has_electrical_issue UInt8')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS health_score Float32')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS vehicle_number String')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS vehicle_model String')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS manufacturing_year UInt16')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS vehicle_type String')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS customer_name String')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS analysis_ts DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {vehicle_status_table} ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()')

    client.execute("""
    CREATE TABLE IF NOT EXISTS {dtc_daily_table} (
        dtc_code String,
        date Date,
        customer_name String DEFAULT '',
        occurrences UInt64,
        vehicles_affected UInt64,
        avg_persistence Float64,
        analysis_ts DateTime DEFAULT now(),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY date
    ORDER BY (date, customer_name, dtc_code)
    """.format(dtc_daily_table=dtc_daily_table))

    client.execute(f'ALTER TABLE {dtc_daily_table} ADD COLUMN IF NOT EXISTS customer_name String DEFAULT \'\'') 
    client.execute(f'ALTER TABLE {dtc_daily_table} ADD COLUMN IF NOT EXISTS analysis_ts DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {dtc_daily_table} ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {dtc_daily_table} ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()')

    client.execute("""
    CREATE TABLE IF NOT EXISTS {fleet_daily_table} (
        date Date,
        customer_name String DEFAULT '',
        active_fault_vehicles UInt64,
        critical_fault_vehicles UInt64,
        fleet_health_score Float32,
        analysis_ts DateTime DEFAULT now(),
        created_at DateTime DEFAULT now(),
        updated_at DateTime DEFAULT now(),
        ingestion_ts DateTime DEFAULT now()
    ) ENGINE = MergeTree()
    PARTITION BY date
    ORDER BY (date, customer_name)
    """.format(fleet_daily_table=fleet_daily_table))

    client.execute(f'ALTER TABLE {fleet_daily_table} ADD COLUMN IF NOT EXISTS customer_name String DEFAULT \'\'') 
    client.execute(f'ALTER TABLE {fleet_daily_table} ADD COLUMN IF NOT EXISTS analysis_ts DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {fleet_daily_table} ADD COLUMN IF NOT EXISTS created_at DateTime DEFAULT now()')
    client.execute(f'ALTER TABLE {fleet_daily_table} ADD COLUMN IF NOT EXISTS updated_at DateTime DEFAULT now()')


def replace_analytics_tables(client: Client, normalized_rows, episode_rows, vehicle_status_rows, dtc_daily_rows, fleet_daily_rows):
    normalized_table = ANALYTICS_TABLES['normalized_dtc_events']
    episodes_table = ANALYTICS_TABLES['vehicle_fault_episodes']
    vehicle_status_table = ANALYTICS_TABLES['vehicle_current_status']
    dtc_daily_table = ANALYTICS_TABLES['dtc_daily_stats']
    fleet_daily_table = ANALYTICS_TABLES['fleet_daily_stats']

    ensure_analytics_tables(client)

    client.execute(f'TRUNCATE TABLE {normalized_table}')
    client.execute(f'TRUNCATE TABLE {episodes_table}')
    client.execute(f'TRUNCATE TABLE {vehicle_status_table}')
    client.execute(f'TRUNCATE TABLE {dtc_daily_table}')
    client.execute(f'TRUNCATE TABLE {fleet_daily_table}')

    analysis_ts = datetime.now(timezone.utc)

    if normalized_rows:
        normalized_by_date = {}
        for row in normalized_rows:
            ts_val = int(row[1])
            date_key = datetime.utcfromtimestamp(ts_val).date()
            normalized_by_date.setdefault(date_key, []).append(row)

        for chunk_rows in normalized_by_date.values():
            chunk_with_audit = [
                (
                    *row,
                    analysis_ts,
                    analysis_ts,
                    analysis_ts,
                )
                for row in chunk_rows
            ]
            client.execute(
                f'INSERT INTO {normalized_table} (uniqueid, ts, dtc_code, dtc_pgn, severity_level, subsystem, system, description, lat, lng, customer_name, analysis_ts, created_at, updated_at) VALUES',
                chunk_with_audit,
            )

    if episode_rows:
        episodes_by_date = {}
        for row in episode_rows:
            date_key = row[11]
            episodes_by_date.setdefault(date_key, []).append(row)

        for chunk_rows in episodes_by_date.values():
            chunk_with_audit = [
                (
                    *row,
                    analysis_ts,
                    analysis_ts,
                    analysis_ts,
                )
                for row in chunk_rows
            ]
            client.execute(
                f'INSERT INTO {episodes_table} (uniqueid, dtc_code, episode_id, first_ts, last_ts, occurrences, resolution_time_sec, is_resolved, severity_level, subsystem, event_date, customer_name, analysis_ts, created_at, updated_at) VALUES',
                chunk_with_audit,
            )
    if vehicle_status_rows:
        vehicle_with_audit = [
            (
                *row,
                analysis_ts,
                analysis_ts,
                analysis_ts,
            )
            for row in vehicle_status_rows
        ]
        client.execute(
            f'INSERT INTO {vehicle_status_table} (uniqueid, active_fault_count, critical_fault_count, has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue, health_score, vehicle_number, vehicle_model, manufacturing_year, vehicle_type, customer_name, analysis_ts, created_at, updated_at) VALUES',
            vehicle_with_audit,
        )
    if dtc_daily_rows:
        dtc_daily_by_date = {}
        for row in dtc_daily_rows:
            date_key = row[1]
            dtc_daily_by_date.setdefault(date_key, []).append(row)

        for chunk_rows in dtc_daily_by_date.values():
            chunk_with_audit = [
                (
                    *row,
                    analysis_ts,
                    analysis_ts,
                    analysis_ts,
                )
                for row in chunk_rows
            ]
            client.execute(
                f'INSERT INTO {dtc_daily_table} (dtc_code, date, customer_name, occurrences, vehicles_affected, avg_persistence, analysis_ts, created_at, updated_at) VALUES',
                chunk_with_audit,
            )
    if fleet_daily_rows:
        fleet_with_audit = [
            (
                *row,
                analysis_ts,
                analysis_ts,
                analysis_ts,
            )
            for row in fleet_daily_rows
        ]
        client.execute(
            f'INSERT INTO {fleet_daily_table} (date, customer_name, active_fault_vehicles, critical_fault_vehicles, fleet_health_score, analysis_ts, created_at, updated_at) VALUES',
            fleet_with_audit,
        )


def upsert_analytics_tables_incremental(
    client: Client,
    normalized_rows,
    episode_rows,
    vehicle_status_rows,
    dtc_daily_rows,
    fleet_daily_rows,
    analysis_ts: datetime,
    active_profile_vehicle_ids: list[str] | None = None,
):
    normalized_table = ANALYTICS_TABLES['normalized_dtc_events']
    episodes_table = ANALYTICS_TABLES['vehicle_fault_episodes']
    vehicle_status_table = ANALYTICS_TABLES['vehicle_current_status']
    dtc_daily_table = ANALYTICS_TABLES['dtc_daily_stats']
    fleet_daily_table = ANALYTICS_TABLES['fleet_daily_stats']

    ensure_analytics_tables(client)

    if normalized_rows:
        uniqueids = sorted({str(row[0]) for row in normalized_rows})
        min_ts = min(int(row[1]) for row in normalized_rows)
        max_ts = max(int(row[1]) for row in normalized_rows)

        created_map = {}
        existing_rows = client.execute(
            f'''
            SELECT uniqueid, ts, dtc_code, min(created_at) AS created_at
            FROM {normalized_table}
            WHERE uniqueid IN %(uids)s
              AND ts >= %(min_ts)s
              AND ts <= %(max_ts)s
            GROUP BY uniqueid, ts, dtc_code
            ''',
            {
                'uids': tuple(uniqueids),
                'min_ts': int(min_ts),
                'max_ts': int(max_ts),
            },
        )
        for uid, ts_val, code, created_at in existing_rows:
            created_map[(str(uid), int(ts_val), str(code))] = created_at

        client.execute(
            f'''
            ALTER TABLE {normalized_table}
            DELETE WHERE uniqueid IN %(uids)s
              AND ts >= %(min_ts)s
              AND ts <= %(max_ts)s
            ''',
            {
                'uids': tuple(uniqueids),
                'min_ts': int(min_ts),
                'max_ts': int(max_ts),
            },
        )

        payload = []
        for row in normalized_rows:
            key = (str(row[0]), int(row[1]), str(row[2]))
            created_at = created_map.get(key, analysis_ts)
            payload.append((*row, analysis_ts, created_at, analysis_ts))

        client.execute(
            f'INSERT INTO {normalized_table} (uniqueid, ts, dtc_code, dtc_pgn, severity_level, subsystem, system, description, lat, lng, customer_name, analysis_ts, created_at, updated_at) VALUES',
            payload,
        )

    if episode_rows:
        uniqueids = sorted({str(row[0]) for row in episode_rows})
        min_event_date = min(row[11] for row in episode_rows)
        max_event_date = max(row[11] for row in episode_rows)

        created_map = {}
        existing_rows = client.execute(
            f'''
            SELECT uniqueid, dtc_code, episode_id, event_date, min(created_at) AS created_at
            FROM {episodes_table}
            WHERE uniqueid IN %(uids)s
              AND event_date >= %(min_date)s
              AND event_date <= %(max_date)s
            GROUP BY uniqueid, dtc_code, episode_id, event_date
            ''',
            {
                'uids': tuple(uniqueids),
                'min_date': min_event_date,
                'max_date': max_event_date,
            },
        )
        for uid, code, eid, event_date, created_at in existing_rows:
            created_map[(str(uid), str(code), int(eid), event_date)] = created_at

        client.execute(
            f'''
            ALTER TABLE {episodes_table}
            DELETE WHERE uniqueid IN %(uids)s
              AND event_date >= %(min_date)s
              AND event_date <= %(max_date)s
            ''',
            {
                'uids': tuple(uniqueids),
                'min_date': min_event_date,
                'max_date': max_event_date,
            },
        )

        payload = []
        for row in episode_rows:
            key = (str(row[0]), str(row[1]), int(row[2]), row[11])
            created_at = created_map.get(key, analysis_ts)
            payload.append((*row, analysis_ts, created_at, analysis_ts))

        client.execute(
            f'INSERT INTO {episodes_table} (uniqueid, dtc_code, episode_id, first_ts, last_ts, occurrences, resolution_time_sec, is_resolved, severity_level, subsystem, event_date, customer_name, analysis_ts, created_at, updated_at) VALUES',
            payload,
        )

    if vehicle_status_rows:
        uniqueids = sorted({str(row[0]) for row in vehicle_status_rows})

        if active_profile_vehicle_ids:
            profile_ids = tuple(sorted({str(value) for value in active_profile_vehicle_ids}))
            client.execute(
                f'ALTER TABLE {vehicle_status_table} DELETE WHERE uniqueid NOT IN %(profile_ids)s',
                {'profile_ids': profile_ids},
            )

        created_map = {}
        existing_rows = client.execute(
            f'''
            SELECT uniqueid, min(created_at) AS created_at
            FROM {vehicle_status_table}
            WHERE uniqueid IN %(uids)s
            GROUP BY uniqueid
            ''',
            {'uids': tuple(uniqueids)},
        )
        for uid, created_at in existing_rows:
            created_map[str(uid)] = created_at

        client.execute(
            f'ALTER TABLE {vehicle_status_table} DELETE WHERE uniqueid IN %(uids)s',
            {'uids': tuple(uniqueids)},
        )

        payload = []
        for row in vehicle_status_rows:
            created_at = created_map.get(str(row[0]), analysis_ts)
            payload.append((*row, analysis_ts, created_at, analysis_ts))

        client.execute(
            f'INSERT INTO {vehicle_status_table} (uniqueid, active_fault_count, critical_fault_count, has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue, health_score, vehicle_number, vehicle_model, manufacturing_year, vehicle_type, customer_name, analysis_ts, created_at, updated_at) VALUES',
            payload,
        )

    if dtc_daily_rows:
        min_date = min(row[1] for row in dtc_daily_rows)
        max_date = max(row[1] for row in dtc_daily_rows)

        created_map = {}
        existing_rows = client.execute(
            f'''
            SELECT dtc_code, date, customer_name, min(created_at) AS created_at
            FROM {dtc_daily_table}
            WHERE date >= %(min_date)s
              AND date <= %(max_date)s
            GROUP BY dtc_code, date, customer_name
            ''',
            {
                'min_date': min_date,
                'max_date': max_date,
            },
        )
        for code, date_val, cname, created_at in existing_rows:
            created_map[(str(code), date_val, str(cname))] = created_at

        client.execute(
            f'''
            ALTER TABLE {dtc_daily_table}
            DELETE WHERE date >= %(min_date)s
              AND date <= %(max_date)s
            ''',
            {
                'min_date': min_date,
                'max_date': max_date,
            },
        )

        payload = []
        for row in dtc_daily_rows:
            key = (str(row[0]), row[1], str(row[2]))
            created_at = created_map.get(key, analysis_ts)
            payload.append((*row, analysis_ts, created_at, analysis_ts))

        client.execute(
            f'INSERT INTO {dtc_daily_table} (dtc_code, date, customer_name, occurrences, vehicles_affected, avg_persistence, analysis_ts, created_at, updated_at) VALUES',
            payload,
        )

    if fleet_daily_rows:
        min_date = min(row[0] for row in fleet_daily_rows)
        max_date = max(row[0] for row in fleet_daily_rows)

        created_map = {}
        existing_rows = client.execute(
            f'''
            SELECT date, customer_name, min(created_at) AS created_at
            FROM {fleet_daily_table}
            WHERE date >= %(min_date)s
              AND date <= %(max_date)s
            GROUP BY date, customer_name
            ''',
            {
                'min_date': min_date,
                'max_date': max_date,
            },
        )
        for date_val, cname, created_at in existing_rows:
            created_map[(date_val, str(cname))] = created_at

        client.execute(
            f'''
            ALTER TABLE {fleet_daily_table}
            DELETE WHERE date >= %(min_date)s
              AND date <= %(max_date)s
            ''',
            {
                'min_date': min_date,
                'max_date': max_date,
            },
        )

        payload = []
        for row in fleet_daily_rows:
            created_at = created_map.get((row[0], str(row[1])), analysis_ts)
            payload.append((*row, analysis_ts, created_at, analysis_ts))

        client.execute(
            f'INSERT INTO {fleet_daily_table} (date, customer_name, active_fault_vehicles, critical_fault_vehicles, fleet_health_score, analysis_ts, created_at, updated_at) VALUES',
            payload,
        )
    else:
        today_date = analysis_ts.date()
        old_created = client.execute(
            f"SELECT min(created_at) FROM {fleet_daily_table} WHERE date = %(dt)s",
            {'dt': today_date},
        )
        created_at = old_created[0][0] if old_created and old_created[0][0] is not None else analysis_ts

        client.execute(
            f"ALTER TABLE {fleet_daily_table} DELETE WHERE date = %(dt)s",
            {'dt': today_date},
        )
        fleet_rows = client.execute(
            f'''
            SELECT
                toDate(%(dt)s) AS date,
                customer_name,
                countIf(active_fault_count > 0) AS active_fault_vehicles,
                countIf(critical_fault_count > 0) AS critical_fault_vehicles,
                toFloat32(avg(health_score)) AS fleet_health_score
            FROM {vehicle_status_table}
            GROUP BY customer_name
            ''',
            {'dt': today_date.strftime('%Y-%m-%d')},
        )
        for row in (fleet_rows or []):
            date_val, cname, active_fv, critical_fv, fhs = row
            client.execute(
                f'INSERT INTO {fleet_daily_table} (date, customer_name, active_fault_vehicles, critical_fault_vehicles, fleet_health_score, analysis_ts, created_at, updated_at) VALUES',
                [(
                    date_val,
                    str(cname or ''),
                    int(active_fv),
                    int(critical_fv),
                    float(fhs or 0.0),
                    analysis_ts,
                    created_at,
                    analysis_ts,
                )],
            )
