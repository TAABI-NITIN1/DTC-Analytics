import ast
import logging
import os
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from src.clickhouse_utils import CDC_TABLES, get_clickhouse_client


DEFAULT_DTC_CODES_TABLE = 'dtc_codes_updated'
DEFAULT_ENGINE_CYCLES_TABLE = 'ss_engineoncycles'
DEFAULT_VEHICLE_PROFILE_TABLE = 'vehicle_profile_ss'
DEFAULT_HISTORY_TABLE = 'ss_dtc_data_history'


def _table_from_env(env_key: str, default: str) -> str:
    value = (os.getenv(env_key) or '').strip()
    return value or default


def _safe_columns(table: str, client) -> set[str]:
    try:
        rows = client.execute(f'DESCRIBE TABLE {table}')
        return {str(row[0]) for row in rows}
    except Exception:
        logging.exception('Could not describe table %s', table)
        return set()


def _first_present(columns: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return None


def _resolve_history_parquet_path() -> Path | None:
    raw_path = (os.getenv('DTC_HISTORY_PARQUET_PATH') or '').strip().strip('"')
    if not raw_path:
        return None

    path = Path(raw_path)
    if not path.is_absolute():
        project_root = Path(__file__).resolve().parents[1]
        path = project_root / path

    return path if path.exists() else None


def _read_history_from_parquet(path: Path, since_ts: int | None = None, until_ts: int | None = None) -> pd.DataFrame:
    required_columns = ['uniqueid', 'unique_id', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']
    max_rows = int(os.getenv('DTC_HISTORY_TEST_LIMIT') or 0)

    def _read_one_parquet(file_path: Path) -> pd.DataFrame:
        try:
            return pd.read_parquet(file_path, columns=required_columns)
        except Exception:
            return pd.read_parquet(file_path)

    if path.is_file() and max_rows > 0:
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(path)
        batches = []
        rows_read = 0
        batch_size = min(max_rows, 200_000)

        for batch in parquet_file.iter_batches(columns=required_columns, batch_size=batch_size):
            batches.append(batch.to_pandas())
            rows_read += batch.num_rows
            if rows_read >= max_rows:
                break

        history_df = pd.concat(batches, ignore_index=True) if batches else pd.DataFrame()
    elif path.is_file():
        history_df = _read_one_parquet(path)
    else:
        files = sorted(path.rglob('*.parquet'))
        if not files:
            return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])
        frames = []
        total_rows = 0
        for file_path in files:
            frame = _read_one_parquet(file_path)
            if frame.empty:
                continue
            if max_rows > 0 and total_rows >= max_rows:
                break
            if max_rows > 0 and (total_rows + len(frame)) > max_rows:
                frame = frame.head(max_rows - total_rows)
            frames.append(frame)
            total_rows += len(frame)

        history_df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if history_df.empty:
        return history_df

    history_df['ts'] = pd.to_numeric(history_df.get('ts'), errors='coerce')
    history_df = history_df[history_df['ts'].notna()].copy()
    history_df['ts'] = history_df['ts'].astype(int)

    if since_ts is None and until_ts is None:
        return history_df.copy()

    effective_since = int(since_ts) if since_ts is not None else int(history_df['ts'].min()) - 1
    effective_until = int(until_ts) if until_ts is not None else int(history_df['ts'].max())

    filtered = history_df[(history_df['ts'] > effective_since) & (history_df['ts'] <= effective_until)].copy()
    return filtered


def _normalize_history_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])

    normalized = df.copy()

    if 'uniqueid' not in normalized.columns and 'unique_id' in normalized.columns:
        normalized = normalized.rename(columns={'unique_id': 'uniqueid'})

    for col_name, default in [('lat', 0.0), ('lng', 0.0), ('dtc_pgn', '')]:
        if col_name not in normalized.columns:
            normalized[col_name] = default

    needed = ['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']
    for col in needed:
        if col not in normalized.columns:
            normalized[col] = '' if col in {'uniqueid', 'dtc_code', 'dtc_pgn'} else 0

    normalized['uniqueid'] = normalized['uniqueid'].astype(str)
    normalized['dtc_code'] = normalized['dtc_code'].astype(str)
    normalized['dtc_pgn'] = normalized['dtc_pgn'].astype(str)

    normalized['lat'] = pd.to_numeric(normalized['lat'], errors='coerce').fillna(0.0)
    normalized['lng'] = pd.to_numeric(normalized['lng'], errors='coerce').fillna(0.0)
    normalized['ts'] = pd.to_numeric(normalized['ts'], errors='coerce').fillna(0).astype(int)

    return normalized[needed]


def fetch_dtc_codes():
    table_name = _table_from_env('DTC_CODES_TABLE', DEFAULT_DTC_CODES_TABLE)
    client = get_clickhouse_client()
    rows = client.execute(f'SELECT * FROM {table_name}')

    if not rows:
        return pd.DataFrame()

    columns = [row[0] for row in client.execute(f'DESCRIBE TABLE {table_name}')]
    return pd.DataFrame(rows, columns=columns)


def fetch_engineoncycles():
    table_name = _table_from_env('ENGINE_CYCLES_TABLE', DEFAULT_ENGINE_CYCLES_TABLE)
    client = get_clickhouse_client()
    columns = _safe_columns(table_name, client)
    row_limit = int(os.getenv('ENGINE_CYCLES_TEST_LIMIT') or 0)

    uniqueid_expr = 'uniqueid' if 'uniqueid' in columns else ('unique_id' if 'unique_id' in columns else None)
    cycle_end_expr = None
    for candidate in ['cycle_end_ts', 'engine_cycle_end_ts', 'engine_off_ts', 'engineoff_ts', 'ts']:
        if candidate in columns:
            cycle_end_expr = candidate
            break

    if not uniqueid_expr or not cycle_end_expr:
        logging.warning('Table %s does not have required engine cycle columns', table_name)
        return pd.DataFrame(columns=['uniqueid', 'cycle_end_ts'])

    query = f'''
        SELECT
            toString({uniqueid_expr}) AS uniqueid,
            toUInt32({cycle_end_expr}) AS cycle_end_ts
        FROM {table_name}
        {'LIMIT ' + str(row_limit) if row_limit > 0 else ''}
    '''
    rows = client.execute(query)
    return pd.DataFrame(rows, columns=['uniqueid', 'cycle_end_ts'])


def fetch_vehicle_profile():
    table_name = _table_from_env('VEHICLE_PROFILE_TABLE', DEFAULT_VEHICLE_PROFILE_TABLE)
    client = get_clickhouse_client()
    try:
        describe_rows = client.execute(f'DESCRIBE TABLE {table_name}')
        columns = {str(row[0]) for row in describe_rows}
        column_types = {str(row[0]): str(row[1]).lower() for row in describe_rows}
    except Exception:
        logging.exception('Could not describe table %s', table_name)
        columns = set()
        column_types = {}

    uniqueid_col = _first_present(columns, ['uniqueid', 'deviceUniqueId_fk', 'deviceuniqueid_fk', 'unique_id'])
    vehicle_number_col = _first_present(columns, ['vehicleNumber', 'vehicle_number', 'registration_number', 'vehicle_no'])
    model_col = _first_present(columns, ['vehicleModel', 'vehicle_model', 'model'])
    manufacturing_year_col = _first_present(columns, ['manufacturingYear', 'manufacturing_year', 'model_year'])
    vehicle_type_col = _first_present(columns, ['vehicleType', 'vehicle_type'])
    customer_name_col = _first_present(columns, ['clientName_client', 'client_name', 'customer_name'])

    if manufacturing_year_col:
        year_type = column_types.get(manufacturing_year_col, '')
        if 'string' in year_type:
            manufacturing_year_expr = f'toUInt16OrZero({manufacturing_year_col})'
        else:
            manufacturing_year_expr = f'toUInt16(ifNull({manufacturing_year_col}, 0))'
    else:
        manufacturing_year_expr = '0'

    if not uniqueid_col:
        logging.warning('Table %s does not have a uniqueid-compatible column', table_name)
        return pd.DataFrame(
            columns=['uniqueid', 'vehicle_number', 'vehicle_model', 'manufacturing_year', 'vehicle_type', 'customer_name']
        )

    query = f'''
        SELECT
            toString({uniqueid_col}) AS uniqueid,
            {f'toString({vehicle_number_col})' if vehicle_number_col else "''"} AS vehicle_number,
            {f'toString({model_col})' if model_col else "''"} AS vehicle_model,
            {manufacturing_year_expr} AS manufacturing_year,
            {f'toString({vehicle_type_col})' if vehicle_type_col else "''"} AS vehicle_type,
            {f'toString({customer_name_col})' if customer_name_col else "''"} AS customer_name
        FROM {table_name}
    '''
    rows = client.execute(query)
    return pd.DataFrame(
        rows,
        columns=['uniqueid', 'vehicle_number', 'vehicle_model', 'manufacturing_year', 'vehicle_type', 'customer_name'],
    )


def fetch_dtc_history(since_ts: int | None = None, until_ts: int | None = None):
    now_ts = int(datetime.utcnow().timestamp())
    window_since = since_ts if since_ts is not None else int((datetime.utcnow() - timedelta(days=7)).timestamp())
    window_until = until_ts if until_ts is not None else now_ts

    parquet_path = _resolve_history_parquet_path()
    if parquet_path is not None:
        if since_ts is None and until_ts is None:
            df = _read_history_from_parquet(parquet_path)
        else:
            df = _read_history_from_parquet(parquet_path, window_since, window_until)
        return _normalize_history_columns(df)

    table_name = _table_from_env('DTC_HISTORY_TABLE', DEFAULT_HISTORY_TABLE)
    client = get_clickhouse_client()
    rows = client.execute(
        f'''
        SELECT
            toString(uniqueid) AS uniqueid,
            toUInt32(toUnixTimestamp(ts)) AS ts,
            toFloat64(lat) AS lat,
            toFloat64(lng) AS lng,
            toString(arrayStringConcat(ifNull(dtc_code, []), ',')) AS dtc_code,
            toString(dtc_pgn) AS dtc_pgn
        FROM {table_name}
        WHERE toUInt32(toUnixTimestamp(ts)) > %(since)s
          AND toUInt32(toUnixTimestamp(ts)) <= %(until)s
        ''',
        {'since': int(window_since), 'until': int(window_until)},
    )

    history_df = pd.DataFrame(rows, columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])
    return _normalize_history_columns(history_df)


def fetch_dtc_history_incremental(
    window_start: datetime,
    window_end: datetime,
    replay_hours: int = 1,
):
    client = get_clickhouse_client()
    exploded_table = CDC_TABLES['exploded_source']

    effective_start = (window_start - timedelta(hours=int(replay_hours))).astimezone(timezone.utc)
    effective_end = window_end.astimezone(timezone.utc)

    rows = client.execute(
        f'''
        SELECT
            uniqueid,
            ts,
            lat,
            lng,
            dtc_code,
            dtc_pgn,
            updatedAt
        FROM {exploded_table}
        WHERE updatedAt >= %(window_start)s
          AND updatedAt < %(window_end)s
        ORDER BY updatedAt, uniqueid, ts
        ''',
        {
            'window_start': effective_start.strftime('%Y-%m-%d %H:%M:%S'),
            'window_end': effective_end.strftime('%Y-%m-%d %H:%M:%S'),
        },
    )

    if not rows:
        return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']), None

    df = pd.DataFrame(rows, columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn', 'updatedAt'])
    max_updated_at = pd.to_datetime(df['updatedAt'], utc=True).max().to_pydatetime()

    history_df = df[['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']].copy()
    history_df = _normalize_history_columns(history_df)
    return history_df, max_updated_at


def fetch_dtc_history_for_uniqueids(
    uniqueids: list[str],
    since_ts: int,
    until_ts: int,
):
    if not uniqueids:
        return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])

    client = get_clickhouse_client()
    exploded_table = CDC_TABLES['exploded_source']

    # ts_dt filter enables partition pruning (table partitioned by toDate(ts_dt))
    rows = client.execute(
        f'''
        SELECT
            uniqueid,
            ts,
            lat,
            lng,
            dtc_code,
            dtc_pgn
        FROM {exploded_table}
        WHERE uniqueid IN %(uniqueids)s
          AND ts_dt >= toDateTime64(%(since_dt)s, 3, 'UTC')
          AND ts_dt <= toDateTime64(%(until_dt)s, 3, 'UTC')
          AND ts > %(since)s
          AND ts <= %(until)s
        ''',
        {
            'uniqueids': tuple(str(value) for value in uniqueids),
            'since': int(since_ts),
            'until': int(until_ts),
            'since_dt': datetime.utcfromtimestamp(int(since_ts)).strftime('%Y-%m-%d %H:%M:%S'),
            'until_dt': datetime.utcfromtimestamp(int(until_ts)).strftime('%Y-%m-%d %H:%M:%S'),
        },
    )

    history_df = pd.DataFrame(rows, columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])
    return _normalize_history_columns(history_df)


def explode_and_clean_dtc_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    def to_list(val):
        if val is None:
            return []
        if isinstance(val, (list, tuple, set)):
            return list(val)
        if isinstance(val, np.ndarray):
            return val.tolist()

        try:
            if pd.isna(val):
                return []
        except Exception:
            pass

        try:
            parsed = ast.literal_eval(str(val))
            if isinstance(parsed, (list, tuple, set, np.ndarray)):
                return list(parsed)
            return [parsed]
        except Exception:
            return [val]

    df = df.copy()
    df['dtc_code'] = df['dtc_code'].apply(to_list)
    df = df.explode('dtc_code', ignore_index=True)

    df = df[df['dtc_code'].astype(str).str.len() > 0]
    df = df[df['dtc_code'] != '0']

    if 'ts' in df.columns:
        df['TimeStamp'] = pd.to_datetime(df['ts'], unit='s', utc=True).dt.tz_convert('Asia/Kolkata')
        ts_pos = df.columns.get_loc('ts')
        cols = list(df.columns)
        cols.insert(ts_pos + 1, cols.pop(cols.index('TimeStamp')))
        df = df.loc[:, cols]

    return df


def enrich_history_with_codes(history_df: pd.DataFrame, codes_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df
    codes = codes_df.rename(columns={'subsytem': 'subsystem'})
    hist = history_df.rename(columns={'subsytem': 'subsystem'})
    merged = hist.merge(codes, left_on='dtc_code', right_on='code', how='left', suffixes=('', '_code'))
    return merged


def default_since_ts(days_back=7):
    return int((datetime.utcnow() - timedelta(days=days_back)).timestamp())
