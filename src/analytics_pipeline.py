from __future__ import annotations

import logging
import os
from bisect import bisect_right
from datetime import datetime, timedelta, timezone

import pandas as pd

from src.clickhouse_utils import (
    ensure_cdc_infrastructure,
    finish_cdc_run_log,
    get_cdc_checkpoint,
    get_cdc_source_watermarks,
    get_clickhouse_client,
    get_table_watermark,
    replace_analytics_tables,
    start_cdc_run_log,
    upsert_analytics_tables_incremental,
    upsert_cdc_checkpoint,
    upsert_cdc_source_watermarks,
)
from src.config import get_pipeline_cfg
from src.ingest import (
    fetch_dtc_codes,
    fetch_dtc_history,
    fetch_dtc_history_for_uniqueids,
    fetch_dtc_history_incremental,
    fetch_engineoncycles,
    fetch_vehicle_profile,
)


def _severity_weight(severity: int) -> float:
    if severity <= 1:
        return 1.0
    if severity == 2:
        return 3.0
    if severity == 3:
        return 7.0
    return 12.0


def _clean_and_explode_history(history_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])

    data = history_df.copy()
    data['dtc_code'] = data['dtc_code'].fillna('').astype(str)

    data['dtc_code'] = (
        data['dtc_code']
        .str.replace('{', '', regex=False)
        .str.replace('}', '', regex=False)
        .str.replace(' ', '', regex=False)
    )

    data['dtc_code'] = data['dtc_code'].str.split(',')
    data = data.explode('dtc_code', ignore_index=True)
    data['dtc_code'] = data['dtc_code'].fillna('').astype(str).str.strip()
    data = data[(data['dtc_code'] != '') & (data['dtc_code'] != '0')]

    data['ts'] = pd.to_numeric(data['ts'], errors='coerce').fillna(0).astype(int)
    data['lat'] = pd.to_numeric(data['lat'], errors='coerce').fillna(0.0)
    data['lng'] = pd.to_numeric(data['lng'], errors='coerce').fillna(0.0)

    return data[['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']].copy()


def _normalize_codes(codes_df: pd.DataFrame) -> pd.DataFrame:
    if codes_df.empty:
        return pd.DataFrame(columns=['dtc_code', 'description', 'severity_level', 'subsystem', 'system'])

    data = codes_df.copy()
    rename_map = {}
    if 'code' in data.columns:
        rename_map['code'] = 'dtc_code'
    if 'subsytem' in data.columns and 'subsystem' not in data.columns:
        rename_map['subsytem'] = 'subsystem'
    data = data.rename(columns=rename_map)

    for column, default in [
        ('description', ''),
        ('severity_level', 1),
        ('subsystem', ''),
        ('system', ''),
    ]:
        if column not in data.columns:
            data[column] = default

    data['dtc_code'] = data['dtc_code'].astype(str).str.strip()
    data['severity_level'] = pd.to_numeric(data['severity_level'], errors='coerce').fillna(1).astype(int)
    data['subsystem'] = data['subsystem'].fillna('').astype(str)
    data['system'] = data['system'].fillna('').astype(str)
    data['description'] = data['description'].fillna('').astype(str)

    return data[['dtc_code', 'description', 'severity_level', 'subsystem', 'system']].drop_duplicates('dtc_code')


def _normalize_engine_cycles(engine_df: pd.DataFrame) -> pd.DataFrame:
    if engine_df.empty:
        return pd.DataFrame(columns=['uniqueid', 'cycle_end_ts'])

    data = engine_df.copy()
    if 'uniqueid' not in data.columns:
        for candidate in ['unique_id', 'deviceUniqueId_fk']:
            if candidate in data.columns:
                data = data.rename(columns={candidate: 'uniqueid'})
                break

    cycle_col = None
    for candidate in ['cycle_end_ts', 'engine_cycle_end_ts', 'engine_off_ts', 'engineoff_ts', 'ts']:
        if candidate in data.columns:
            cycle_col = candidate
            break

    if cycle_col is None or 'uniqueid' not in data.columns:
        return pd.DataFrame(columns=['uniqueid', 'cycle_end_ts'])

    out = data[['uniqueid', cycle_col]].copy().rename(columns={cycle_col: 'cycle_end_ts'})
    out['uniqueid'] = out['uniqueid'].astype(str)
    out['cycle_end_ts'] = pd.to_numeric(out['cycle_end_ts'], errors='coerce').fillna(0).astype(int)
    out = out[out['cycle_end_ts'] > 0]
    return out


def _has_engine_boundary(sorted_cycle_ends: list[int], prev_ts: int, current_ts: int) -> bool:
    if not sorted_cycle_ends:
        return False
    pos = bisect_right(sorted_cycle_ends, prev_ts)
    if pos >= len(sorted_cycle_ends):
        return False
    return sorted_cycle_ends[pos] <= current_ts


def _compute_episodes(events_df: pd.DataFrame, engine_df: pd.DataFrame, gap_seconds: int) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame(
            columns=[
                'uniqueid',
                'dtc_code',
                'episode_id',
                'first_ts',
                'last_ts',
                'occurrences',
                'resolution_time_sec',
                'severity_level',
                'subsystem',
            ]
        )

    cycles_by_vehicle = {
        uid: sorted(group['cycle_end_ts'].astype(int).tolist())
        for uid, group in engine_df.groupby('uniqueid')
    } if not engine_df.empty else {}

    rows = []
    for (uniqueid, dtc_code), group in events_df.sort_values(['uniqueid', 'dtc_code', 'ts']).groupby(['uniqueid', 'dtc_code']):
        cycle_list = cycles_by_vehicle.get(uniqueid, [])
        episode_number = 1
        first_ts = None
        last_ts = None
        count = 0

        for rec in group.itertuples(index=False):
            ts_val = int(rec.ts)
            if first_ts is None:
                first_ts = ts_val
                last_ts = ts_val
                count = 1
                continue

            time_gap = ts_val - int(last_ts)
            has_boundary = _has_engine_boundary(cycle_list, int(last_ts), ts_val)
            new_episode = (time_gap > gap_seconds) or has_boundary

            if new_episode:
                rows.append(
                    {
                        'uniqueid': uniqueid,
                        'dtc_code': dtc_code,
                        'episode_id': episode_number,
                        'first_ts': int(first_ts),
                        'last_ts': int(last_ts),
                        'occurrences': int(count),
                        'resolution_time_sec': int(max(0, int(last_ts) - int(first_ts))),
                        'severity_level': int(getattr(rec, 'severity_level', 1) or 1),
                        'subsystem': str(getattr(rec, 'subsystem', '') or ''),
                    }
                )
                episode_number += 1
                first_ts = ts_val
                last_ts = ts_val
                count = 1
            else:
                last_ts = ts_val
                count += 1

        if first_ts is not None and last_ts is not None:
            last_row = group.iloc[-1]
            rows.append(
                {
                    'uniqueid': uniqueid,
                    'dtc_code': dtc_code,
                    'episode_id': episode_number,
                    'first_ts': int(first_ts),
                    'last_ts': int(last_ts),
                    'occurrences': int(count),
                    'resolution_time_sec': int(max(0, int(last_ts) - int(first_ts))),
                    'severity_level': int(last_row.get('severity_level', 1) or 1),
                    'subsystem': str(last_row.get('subsystem', '') or ''),
                }
            )

    return pd.DataFrame(rows)


def _mark_resolution(episodes_df: pd.DataFrame, events_df: pd.DataFrame, engine_df: pd.DataFrame, final_day_cutoff_seconds: int) -> pd.DataFrame:
    if episodes_df.empty:
        episodes_df['is_resolved'] = []
        return episodes_df

    max_ts = int(events_df['ts'].max()) if not events_df.empty else int(episodes_df['last_ts'].max())
    cutoff_ts = max_ts - int(final_day_cutoff_seconds)

    engine_after = (
        engine_df.groupby('uniqueid')['cycle_end_ts'].max().to_dict()
        if not engine_df.empty
        else {}
    )

    resolved = episodes_df.copy()
    resolved['is_resolved'] = resolved.apply(
        lambda r: 1
        if (int(r['last_ts']) <= cutoff_ts and int(engine_after.get(str(r['uniqueid']), 0)) > int(r['last_ts']))
        else 0,
        axis=1,
    )
    return resolved


def _slice_history_window(history_df: pd.DataFrame, since_ts: int) -> pd.DataFrame:
    if history_df is None or history_df.empty:
        return pd.DataFrame(columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn'])

    data = history_df.copy()
    data['ts'] = pd.to_numeric(data['ts'], errors='coerce').fillna(0).astype(int)
    data = data[data['ts'] >= int(since_ts)]
    return data


def _collect_source_watermarks(client) -> dict[str, datetime]:
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


def run_full_analytics() -> dict:
    pipeline_cfg = get_pipeline_cfg()
    gap_seconds = int(pipeline_cfg.get('episode_gap_seconds', 86400))
    final_day_cutoff_seconds = int(pipeline_cfg.get('final_day_cutoff_seconds', 86400))
    analytics_window_days = int(pipeline_cfg.get('analytics_window_days', 90))
    scaling_constant = float(os.getenv('HEALTH_SCALING_CONSTANT') or 0.2)

    client = get_clickhouse_client()
    cdc_enabled = bool(pipeline_cfg.get('cdc_enabled', True))
    run_id = None
    run_window_start = datetime.now(timezone.utc)
    run_window_end = datetime.now(timezone.utc)
    source_rows = 0
    incremental_mode = False
    history_for_daily = None
    source_trigger_reason = 'bootstrap_full_rebuild'
    source_watermarks_current = {}
    analytics_window_start_ts = int((run_window_end - timedelta(days=analytics_window_days)).timestamp())

    if cdc_enabled:
        source_table = os.getenv('DTC_HISTORY_TABLE') or 'ss_dtc_data_history'
        pipeline_name = str(pipeline_cfg.get('cdc_pipeline_name') or 'dtc_analytics_ravi')
        replay_hours = int(pipeline_cfg.get('cdc_replay_hours', 1))
        bootstrap_days = int(pipeline_cfg.get('cdc_bootstrap_days', 90))

        ensure_cdc_infrastructure(client, source_table=source_table)
        checkpoint = get_cdc_checkpoint(client, pipeline_name)
        source_watermarks_current = _collect_source_watermarks(client)
        source_watermarks_prev = get_cdc_source_watermarks(client, pipeline_name)

        run_window_end = datetime.now(timezone.utc)
        analytics_window_start_ts = int((run_window_end - timedelta(days=analytics_window_days)).timestamp())
        initial_window_days = max(bootstrap_days, analytics_window_days)
        run_window_start = checkpoint if checkpoint else (run_window_end - timedelta(days=initial_window_days))
        run_id = start_cdc_run_log(client, pipeline_name, run_window_start, run_window_end)

        if checkpoint is None:
            # First bootstrap: skip large incremental fetch and run full source rebuild.
            source_trigger_reason = 'bootstrap_full_rebuild'
            bootstrap_since_ts = max(int(run_window_start.timestamp()), analytics_window_start_ts)
            history_raw = fetch_dtc_history(
                since_ts=bootstrap_since_ts,
                until_ts=int(run_window_end.timestamp()),
            )
            source_rows = len(history_raw)
        else:
            incremental_df, _ = fetch_dtc_history_incremental(
                window_start=run_window_start,
                window_end=run_window_end,
                replay_hours=replay_hours,
            )
            source_rows = len(incremental_df)

            if incremental_df.empty:
                changed_sources = []
                for source_name, current_ts in source_watermarks_current.items():
                    previous_ts = source_watermarks_prev.get(source_name)
                    if previous_ts is None or current_ts > previous_ts:
                        changed_sources.append(source_name)

                if not changed_sources:
                    upsert_cdc_checkpoint(client, pipeline_name, run_window_end)
                    finish_cdc_run_log(
                        client,
                        run_id=run_id,
                        pipeline_name=pipeline_name,
                        status='success',
                        window_start=run_window_start,
                        window_end=run_window_end,
                        source_rows=0,
                        output_rows=0,
                        message='No new source rows in CDC window or dependent source tables',
                    )
                    return {
                        'normalized_events': 0,
                        'episodes': 0,
                        'vehicle_status': 0,
                        'dtc_daily_stats': 0,
                        'fleet_daily_stats': 0,
                        'cdc_noop': True,
                    }

                incremental_mode = True
                source_trigger_reason = f"source_change:{','.join(sorted(changed_sources))}"
                history_raw = fetch_dtc_history(
                    since_ts=analytics_window_start_ts,
                    until_ts=int(run_window_end.timestamp()),
                )
                source_rows = len(history_raw)
                history_for_daily = history_raw.copy()
            else:
                source_trigger_reason = 'dtc_history_incremental'
                incremental_mode = True
                impacted_uniqueids = sorted(incremental_df['uniqueid'].astype(str).unique().tolist())
                run_until_ts = int(run_window_end.timestamp())

                history_raw = fetch_dtc_history_for_uniqueids(
                    uniqueids=impacted_uniqueids,
                    since_ts=0,
                    until_ts=run_until_ts,
                )

                min_impacted_ts = int(pd.to_numeric(incremental_df['ts'], errors='coerce').fillna(0).astype(int).min())
                max_impacted_ts = int(pd.to_numeric(incremental_df['ts'], errors='coerce').fillna(0).astype(int).max())
                daily_since_ts = max(
                    analytics_window_start_ts,
                    min_impacted_ts - max(gap_seconds, final_day_cutoff_seconds, replay_hours * 3600) - 1,
                )

                history_for_daily = fetch_dtc_history(
                    since_ts=daily_since_ts,
                    until_ts=max_impacted_ts,
                )
    else:
        history_raw = fetch_dtc_history(
            since_ts=analytics_window_start_ts,
            until_ts=int(run_window_end.timestamp()),
        )

    history_raw = _slice_history_window(history_raw, analytics_window_start_ts)
    if history_for_daily is not None:
        history_for_daily = _slice_history_window(history_for_daily, analytics_window_start_ts)

    codes_df = _normalize_codes(fetch_dtc_codes())
    engine_df = _normalize_engine_cycles(fetch_engineoncycles())
    vehicle_df = fetch_vehicle_profile()

    normalized_events = _clean_and_explode_history(history_raw)
    enriched_events = normalized_events.merge(codes_df, on='dtc_code', how='left')

    enriched_events['severity_level'] = pd.to_numeric(enriched_events.get('severity_level'), errors='coerce').fillna(1).astype(int)
    enriched_events['subsystem'] = enriched_events.get('subsystem', '').fillna('').astype(str)
    enriched_events['system'] = enriched_events.get('system', '').fillna('').astype(str)
    enriched_events['description'] = enriched_events.get('description', '').fillna('').astype(str)

    episodes = _compute_episodes(enriched_events, engine_df, gap_seconds)
    episodes = _mark_resolution(episodes, enriched_events, engine_df, final_day_cutoff_seconds)

    if incremental_mode and history_for_daily is not None:
        daily_events_base = _clean_and_explode_history(history_for_daily)
        daily_enriched_events = daily_events_base.merge(codes_df, on='dtc_code', how='left')
        daily_enriched_events['severity_level'] = pd.to_numeric(daily_enriched_events.get('severity_level'), errors='coerce').fillna(1).astype(int)
        daily_enriched_events['subsystem'] = daily_enriched_events.get('subsystem', '').fillna('').astype(str)
        daily_enriched_events['system'] = daily_enriched_events.get('system', '').fillna('').astype(str)
        daily_enriched_events['description'] = daily_enriched_events.get('description', '').fillna('').astype(str)
        episodes_for_daily = _compute_episodes(daily_enriched_events, engine_df, gap_seconds)
    else:
        daily_enriched_events = enriched_events
        episodes_for_daily = episodes

    active_episodes = episodes[episodes['is_resolved'] == 0].copy()
    episode_count_per_fault = episodes.groupby(['uniqueid', 'dtc_code'])['episode_id'].nunique().to_dict()

    def _fault_score(row):
        sev = _severity_weight(int(row.get('severity_level', 1) or 1))
        duration_days = float(row.get('resolution_time_sec', 0)) / 86400.0
        persistence_factor = 1.0 + min(duration_days, 30.0) / 10.0
        recurrence = float(episode_count_per_fault.get((row['uniqueid'], row['dtc_code']), 1))
        recurrence_factor = 1.0 + max(0.0, recurrence - 1.0) * 0.5
        return sev * persistence_factor * recurrence_factor

    if active_episodes.empty:
        risk_by_vehicle = pd.Series(dtype=float)
    else:
        active_episodes['fault_score'] = active_episodes.apply(_fault_score, axis=1)
        risk_by_vehicle = active_episodes.groupby('uniqueid')['fault_score'].sum()

    active_counts = active_episodes.groupby('uniqueid')['dtc_code'].nunique() if not active_episodes.empty else pd.Series(dtype=float)
    critical_counts = active_episodes[active_episodes['severity_level'] >= 3].groupby('uniqueid')['dtc_code'].nunique() if not active_episodes.empty else pd.Series(dtype=float)

    profile_vehicle_ids = set(vehicle_df['uniqueid'].astype(str).tolist()) if not vehicle_df.empty else set()
    if profile_vehicle_ids:
        all_vehicle_ids = profile_vehicle_ids
    else:
        all_vehicle_ids = set(enriched_events['uniqueid'].astype(str).unique().tolist())

    status_rows = []
    active_by_vehicle = active_episodes.groupby('uniqueid') if not active_episodes.empty else {}
    vehicle_lookup = {
        str(r.uniqueid): r
        for r in vehicle_df.itertuples(index=False)
    } if not vehicle_df.empty else {}

    for uniqueid in sorted(all_vehicle_ids):
        risk = float(risk_by_vehicle.get(uniqueid, 0.0)) if len(risk_by_vehicle) else 0.0
        health_score = max(0.0, 100.0 - min(risk * scaling_constant, 100.0))

        vehicle_active = active_by_vehicle.get_group(uniqueid) if (not active_episodes.empty and uniqueid in active_by_vehicle.groups) else pd.DataFrame()
        subsystem_values = set(vehicle_active['subsystem'].astype(str).str.lower().tolist()) if not vehicle_active.empty else set()

        vehicle_meta = vehicle_lookup.get(uniqueid)
        status_rows.append(
            (
                uniqueid,
                int(active_counts.get(uniqueid, 0)) if len(active_counts) else 0,
                int(critical_counts.get(uniqueid, 0)) if len(critical_counts) else 0,
                1 if any('powertrain' in s or 'engine' in s for s in subsystem_values) else 0,
                1 if any('emission' in s for s in subsystem_values) else 0,
                1 if any('safety' in s for s in subsystem_values) else 0,
                1 if any('electrical' in s for s in subsystem_values) else 0,
                float(round(health_score, 2)),
                str(getattr(vehicle_meta, 'vehicle_number', '') or ''),
                str(getattr(vehicle_meta, 'vehicle_model', '') or ''),
                int(getattr(vehicle_meta, 'manufacturing_year', 0) or 0),
                str(getattr(vehicle_meta, 'vehicle_type', '') or ''),
                str(getattr(vehicle_meta, 'customer_name', '') or ''),
            )
        )

    daily_enriched_events['date'] = pd.to_datetime(daily_enriched_events['ts'], unit='s', utc=True).dt.date

    # Map customer_name onto daily events for per-customer grouping
    daily_enriched_events['customer_name'] = daily_enriched_events['uniqueid'].astype(str).map(
        lambda uid: str(getattr(vehicle_lookup.get(uid), 'customer_name', '') or '')
    )

    dtc_daily = (
        daily_enriched_events.groupby(['dtc_code', 'date', 'customer_name'])
        .agg(
            occurrences=('dtc_code', 'count'),
            vehicles_affected=('uniqueid', pd.Series.nunique),
        )
        .reset_index()
    )

    persistence_by_dtc_day = episodes_for_daily.copy()
    if not persistence_by_dtc_day.empty:
        persistence_by_dtc_day['date'] = pd.to_datetime(persistence_by_dtc_day['first_ts'], unit='s', utc=True).dt.date
        persistence_by_dtc_day['customer_name'] = persistence_by_dtc_day['uniqueid'].astype(str).map(
            lambda uid: str(getattr(vehicle_lookup.get(uid), 'customer_name', '') or '')
        )
        persistence_agg = (
            persistence_by_dtc_day.groupby(['dtc_code', 'date', 'customer_name'])['resolution_time_sec']
            .mean()
            .reset_index(name='avg_persistence')
        )
        dtc_daily = dtc_daily.merge(persistence_agg, on=['dtc_code', 'date', 'customer_name'], how='left')
    else:
        dtc_daily['avg_persistence'] = 0.0
    dtc_daily['avg_persistence'] = dtc_daily['avg_persistence'].fillna(0.0)

    today_date = run_window_end.date()

    normalized_rows = [
        (
            str(r.uniqueid),
            int(r.ts),
            str(r.dtc_code),
            str(r.dtc_pgn or ''),
            int(r.severity_level),
            str(r.subsystem or ''),
            str(r.system or ''),
            str(r.description or ''),
            float(r.lat),
            float(r.lng),
            str(getattr(vehicle_lookup.get(str(r.uniqueid)), 'customer_name', '') or ''),
        )
        for r in enriched_events.itertuples(index=False)
    ]

    episode_rows = [
        (
            str(r.uniqueid),
            str(r.dtc_code),
            int(r.episode_id),
            int(r.first_ts),
            int(r.last_ts),
            int(r.occurrences),
            int(r.resolution_time_sec),
            int(r.is_resolved),
            int(r.severity_level),
            str(r.subsystem or ''),
            pd.to_datetime(r.first_ts, unit='s', utc=True).date(),
            str(getattr(vehicle_lookup.get(str(r.uniqueid)), 'customer_name', '') or ''),
        )
        for r in episodes.itertuples(index=False)
    ]

    dtc_daily_rows = [
        (
            str(r.dtc_code),
            r.date,
            str(r.customer_name),
            int(r.occurrences),
            int(r.vehicles_affected),
            float(r.avg_persistence),
        )
        for r in dtc_daily.itertuples(index=False)
    ]

    if daily_enriched_events.empty:
        fleet_daily_rows = [
            (
                today_date,
                '',
                0,
                0,
                100.0,
            )
        ]
    else:
        fleet_vehicle_day = daily_enriched_events[['date', 'uniqueid', 'severity_level', 'customer_name']].copy()
        fleet_vehicle_day['severity_level'] = pd.to_numeric(
            fleet_vehicle_day['severity_level'],
            errors='coerce',
        ).fillna(1).astype(int)
        fleet_vehicle_day['is_critical'] = (fleet_vehicle_day['severity_level'] >= 3).astype(int)
        fleet_vehicle_day['risk_component'] = fleet_vehicle_day['severity_level'].apply(_severity_weight)

        fleet_vehicle_daily = (
            fleet_vehicle_day.groupby(['date', 'customer_name', 'uniqueid'])
            .agg(
                has_fault=('uniqueid', 'size'),
                has_critical=('is_critical', 'max'),
                risk_score=('risk_component', 'sum'),
            )
            .reset_index()
        )

        fleet_daily = (
            fleet_vehicle_daily.groupby(['date', 'customer_name'])
            .agg(
                active_fault_vehicles=('has_fault', lambda s: int((s > 0).sum())),
                critical_fault_vehicles=('has_critical', 'sum'),
                avg_vehicle_risk=('risk_score', 'mean'),
            )
            .reset_index()
        )

        fleet_daily['fleet_health_score'] = fleet_daily['avg_vehicle_risk'].apply(
            lambda v: max(0.0, 100.0 - min(float(v) * scaling_constant, 100.0))
        )

        fleet_daily_rows = [
            (
                r.date,
                str(r.customer_name),
                int(r.active_fault_vehicles),
                int(r.critical_fault_vehicles),
                float(round(float(r.fleet_health_score), 2)),
            )
            for r in fleet_daily.sort_values('date').itertuples(index=False)
        ]

    run_analysis_ts = datetime.now(timezone.utc)
    if incremental_mode:
        upsert_analytics_tables_incremental(
            client,
            normalized_rows=normalized_rows,
            episode_rows=episode_rows,
            vehicle_status_rows=status_rows,
            dtc_daily_rows=dtc_daily_rows,
            fleet_daily_rows=fleet_daily_rows,
            analysis_ts=run_analysis_ts,
            active_profile_vehicle_ids=sorted(profile_vehicle_ids) if profile_vehicle_ids else None,
        )
    else:
        replace_analytics_tables(
            client,
            normalized_rows=normalized_rows,
            episode_rows=episode_rows,
            vehicle_status_rows=status_rows,
            dtc_daily_rows=dtc_daily_rows,
            fleet_daily_rows=fleet_daily_rows,
        )

    summary = {
        'normalized_events': len(normalized_rows),
        'episodes': len(episode_rows),
        'vehicle_status': len(status_rows),
        'dtc_daily_stats': len(dtc_daily_rows),
        'fleet_daily_stats': len(fleet_daily_rows),
    }

    if cdc_enabled:
        pipeline_name = str(pipeline_cfg.get('cdc_pipeline_name') or 'dtc_analytics_ravi')
        upsert_cdc_checkpoint(client, pipeline_name, run_window_end)
        upsert_cdc_source_watermarks(client, pipeline_name, source_watermarks_current)
        finish_cdc_run_log(
            client,
            run_id=run_id or 'unknown',
            pipeline_name=pipeline_name,
            status='success',
            window_start=run_window_start,
            window_end=run_window_end,
            source_rows=source_rows,
            output_rows=(len(normalized_rows) + len(episode_rows) + len(status_rows) + len(dtc_daily_rows) + len(fleet_daily_rows)),
            message=f"CDC run completed ({source_trigger_reason})",
        )

    logging.info('Analytics pipeline completed: %s', summary)
    return summary
