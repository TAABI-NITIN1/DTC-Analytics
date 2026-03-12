"""v2 analytics pipeline — populates master + analytics tables from raw sources.

Entry point: run_full_analytics_v2()
"""

from __future__ import annotations

import gc
import logging
import os
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from itertools import combinations

import pandas as pd

from src.clickhouse_utils import (
    ensure_cdc_infrastructure,
    finish_cdc_run_log,
    get_cdc_checkpoint,
    get_cdc_source_watermarks,
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
    write_dtc_events_exploded,
    write_vehicle_fault_master,
    write_fleet_health_summary,
    write_fleet_dtc_distribution,
    write_fleet_system_health,
    write_fleet_fault_trends,
    write_vehicle_health_summary,
    write_dtc_fleet_impact,
    write_maintenance_priority,
    write_dtc_cooccurrence,
)
from src.config import get_pipeline_cfg
from src.ingest import (
    fetch_dtc_codes,
    fetch_dtc_history,
    fetch_dtc_history_incremental,
    fetch_dtc_history_for_uniqueids,
    fetch_engineoncycles,
)

# Re-use v1 helpers unchanged
from src.analytics_pipeline import (
    _severity_weight,
    _clean_and_explode_history,
    _normalize_codes,
    _normalize_engine_cycles,
    _has_engine_boundary,
    _compute_episodes,
    _mark_resolution,
    _slice_history_window,
)

CDC_PIPELINE_NAME_V2 = 'dtc_analytics_ravi_v2'


# ------------------------------------------------------------------
# Helper: build vehicle + dtc lookup dicts
# ------------------------------------------------------------------

def _load_vehicle_lookup(client) -> dict:
    """Return {uniqueid: row_dict} from vehicle_master_ravi_v2."""
    table = V2_TABLES['vehicle_master']
    rows = client.execute(
        f'''SELECT clientLoginId, uniqueid, vehicle_number, customer_name,
                   model, manufacturing_year, vehicle_type, solutionType
            FROM {table} FINAL'''
    )
    lookup = {}
    for r in rows:
        lookup[str(r[1])] = {
            'clientLoginId': str(r[0]),
            'uniqueid': str(r[1]),
            'vehicle_number': str(r[2]),
            'customer_name': str(r[3]),
            'model': str(r[4]),
            'manufacturing_year': int(r[5]),
            'vehicle_type': str(r[6]),
            'solutionType': str(r[7]),
        }
    return lookup


def _load_dtc_lookup(client) -> dict:
    """Return {dtc_code: row_dict} from dtc_master_ravi_v2."""
    table = V2_TABLES['dtc_master']
    rows = client.execute(
        f'''SELECT dtc_code, system, subsystem, description, severity_level,
                   driver_related, action_required
            FROM {table} FINAL'''
    )
    lookup = {}
    for r in rows:
        lookup[str(r[0])] = {
            'dtc_code': str(r[0]),
            'system': str(r[1]),
            'subsystem': str(r[2]),
            'description': str(r[3]),
            'severity_level': int(r[4]),
            'driver_related': int(r[5]),
            'action_required': str(r[6]),
        }
    return lookup


# ------------------------------------------------------------------
# Co-occurrence: find DTC pairs within a time window per vehicle
# ------------------------------------------------------------------

def _compute_cooccurrence(events_df: pd.DataFrame, vehicle_lookup: dict,
                          window_seconds: int = 60) -> list[tuple]:
    """For each vehicle, find DTC code pairs that fire within *window_seconds*.

    Returns list of (clientLoginId, dtc_code_a, dtc_code_b,
                     cooccurrence_count, vehicles_affected,
                     avg_time_gap_sec, last_seen_ts, created_at, updated_at).
    """
    if events_df.empty:
        return []

    pair_stats: dict[tuple, dict] = defaultdict(lambda: {
        'count': 0, 'vehicles': set(), 'gap_sum': 0.0, 'last_ts': 0,
    })

    vehicle_groups = events_df.sort_values('ts').groupby('uniqueid')
    num_veh = len(vehicle_groups)
    processed = 0
    for uniqueid, group in vehicle_groups:
        vmeta = vehicle_lookup.get(str(uniqueid))
        if not vmeta:
            continue
        client_login_id = vmeta['clientLoginId']

        records = list(group[['ts', 'dtc_code']].itertuples(index=False))
        n = len(records)
        for i in range(n):
            j = i + 1
            while j < n and (int(records[j].ts) - int(records[i].ts)) <= window_seconds:
                code_a, code_b = sorted([str(records[i].dtc_code), str(records[j].dtc_code)])
                if code_a != code_b:
                    key = (client_login_id, code_a, code_b)
                    gap = abs(int(records[j].ts) - int(records[i].ts))
                    pair_stats[key]['count'] += 1
                    pair_stats[key]['vehicles'].add(str(uniqueid))
                    pair_stats[key]['gap_sum'] += gap
                    pair_stats[key]['last_ts'] = max(pair_stats[key]['last_ts'], int(records[j].ts))
                j += 1
        processed += 1
        if processed % 200 == 0:
            logging.info('  cooccurrence: %d/%d vehicles processed', processed, num_veh)

    now_utc = datetime.now(timezone.utc)
    rows = []
    for (cli, code_a, code_b), s in pair_stats.items():
        avg_gap = s['gap_sum'] / s['count'] if s['count'] else 0
        rows.append((
            cli, code_a, code_b,
            s['count'], len(s['vehicles']),
            round(avg_gap, 2), s['last_ts'],
            now_utc, now_utc,
        ))
    return rows


# ------------------------------------------------------------------
# Derived analytics builders
# ------------------------------------------------------------------

def _build_fleet_health_summary(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                                scaling_constant: float) -> list[tuple]:
    """One row per clientLoginId."""
    now_utc = datetime.now(timezone.utc)
    if episodes_df.empty:
        return []

    # group by clientLoginId
    by_client: dict[str, dict] = defaultdict(lambda: {
        'all_vehicles': set(), 'active_fault_vehicles': set(),
        'critical_fault_vehicles': set(), 'driver_related': 0,
        'dtc_counts': defaultdict(int), 'system_counts': defaultdict(int),
        'risk_sum': 0.0,
    })

    episode_count_per_fault = episodes_df.groupby(['uniqueid', 'dtc_code'])['episode_id'].nunique().to_dict()

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        if not cli:
            continue
        bucket = by_client[cli]
        bucket['all_vehicles'].add(uid)
        if r.is_resolved == 0:
            bucket['active_fault_vehicles'].add(uid)
            if int(r.severity_level) >= 3:
                bucket['critical_fault_vehicles'].add(uid)
            # fault score
            sev = _severity_weight(int(r.severity_level))
            dur = float(r.resolution_time_sec) / 86400.0
            persistence = 1.0 + min(dur, 30.0) / 10.0
            recurrence = float(episode_count_per_fault.get((r.uniqueid, r.dtc_code), 1))
            recurrence_f = 1.0 + max(0.0, recurrence - 1.0) * 0.5
            bucket['risk_sum'] += sev * persistence * recurrence_f
        if int(getattr(r, 'driver_related', 0)):
            bucket['driver_related'] += 1
        bucket['dtc_counts'][str(r.dtc_code)] += 1
        bucket['system_counts'][str(getattr(r, 'system', ''))] += 1

    # Also count vehicles with no faults
    for uid, vmeta in vehicle_lookup.items():
        cli = vmeta.get('clientLoginId', '')
        if cli:
            by_client[cli]['all_vehicles'].add(uid)

    rows = []
    for cli, b in by_client.items():
        total = len(b['all_vehicles'])
        health = max(0.0, 100.0 - min(b['risk_sum'] * scaling_constant, 100.0)) if total else 100.0
        most_dtc = max(b['dtc_counts'], key=b['dtc_counts'].get, default='')
        most_sys = max(b['system_counts'], key=b['system_counts'].get, default='')
        rows.append((
            cli,
            total,
            len(b['active_fault_vehicles']),
            len(b['critical_fault_vehicles']),
            b['driver_related'],
            round(health, 2),
            most_dtc,
            most_sys,
            'stable',  # trend placeholder
            now_utc, now_utc,
        ))
    return rows


def _build_fleet_dtc_distribution(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                                  dtc_lookup: dict) -> list[tuple]:
    now_utc = datetime.now(timezone.utc)
    if episodes_df.empty:
        return []

    key_stats: dict[tuple, dict] = defaultdict(lambda: {
        'vehicles': set(), 'active_vehicles': set(), 'total_occ': 0,
        'total_eps': 0, 'res_time_sum': 0.0, 'res_count': 0,
        'driver_related': 0,
    })

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        if not cli:
            continue
        code = str(r.dtc_code)
        bucket = key_stats[(cli, code)]
        bucket['vehicles'].add(uid)
        bucket['total_eps'] += 1
        bucket['total_occ'] += int(r.occurrence_count)
        if r.is_resolved == 0:
            bucket['active_vehicles'].add(uid)
        if int(r.resolution_time_sec) > 0:
            bucket['res_time_sum'] += float(r.resolution_time_sec)
            bucket['res_count'] += 1
        if int(getattr(r, 'driver_related', 0)):
            bucket['driver_related'] += 1

    rows = []
    for (cli, code), s in key_stats.items():
        dm = dtc_lookup.get(code, {})
        avg_res = s['res_time_sum'] / s['res_count'] if s['res_count'] else 0
        rows.append((
            cli, code,
            dm.get('description', ''), dm.get('system', ''), dm.get('subsystem', ''),
            dm.get('severity_level', 1),
            len(s['vehicles']), len(s['active_vehicles']),
            s['total_occ'], s['total_eps'],
            round(avg_res, 2), s['driver_related'],
            now_utc, now_utc,
        ))
    return rows


def _build_fleet_system_health(episodes_df: pd.DataFrame, vehicle_lookup: dict) -> list[tuple]:
    now_utc = datetime.now(timezone.utc)
    if episodes_df.empty:
        return []

    key_stats: dict[tuple, dict] = defaultdict(lambda: {
        'vehicles': set(), 'active': 0, 'critical': 0, 'risk': 0.0,
    })

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        system = str(getattr(r, 'system', '') or '')
        if not cli or not system:
            continue
        bucket = key_stats[(cli, system)]
        bucket['vehicles'].add(uid)
        if r.is_resolved == 0:
            bucket['active'] += 1
            if int(r.severity_level) >= 3:
                bucket['critical'] += 1
            bucket['risk'] += _severity_weight(int(r.severity_level))

    rows = []
    for (cli, system), s in key_stats.items():
        rows.append((
            cli, system,
            len(s['vehicles']), s['active'], s['critical'],
            round(s['risk'], 2), 'stable',
            now_utc, now_utc,
        ))
    return rows


def _build_fleet_fault_trends(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                              scaling_constant: float) -> list[tuple]:
    now_utc = datetime.now(timezone.utc)
    if episodes_df.empty:
        return []

    day_stats: dict[tuple, dict] = defaultdict(lambda: {
        'active': 0, 'new': 0, 'resolved': 0, 'driver_related': 0,
        'risk_vehicles': defaultdict(float),
    })

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        if not cli:
            continue
        d = pd.Timestamp(int(r.first_ts), unit='s', tz='UTC').date()
        bucket = day_stats[(cli, d)]
        bucket['active'] += 1
        bucket['new'] += 1
        if r.is_resolved:
            bucket['resolved'] += 1
        if int(getattr(r, 'driver_related', 0)):
            bucket['driver_related'] += 1
        bucket['risk_vehicles'][uid] += _severity_weight(int(r.severity_level))

    rows = []
    for (cli, d), s in sorted(day_stats.items()):
        avg_risk = sum(s['risk_vehicles'].values()) / max(len(s['risk_vehicles']), 1)
        health = max(0.0, 100.0 - min(avg_risk * scaling_constant, 100.0))
        rows.append((
            cli, d,
            s['active'], s['new'], s['resolved'], s['driver_related'],
            round(health, 2),
            now_utc, now_utc,
        ))
    return rows


def _build_vehicle_health_summary(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                                  scaling_constant: float) -> list[tuple]:
    now_utc = datetime.now(timezone.utc)
    thirty_days_ago_ts = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

    # Init from vehicle_master (every vehicle gets a row)
    per_vehicle: dict[str, dict] = {}
    for uid, vmeta in vehicle_lookup.items():
        per_vehicle[uid] = {
            'cli': vmeta['clientLoginId'],
            'vehicle_number': vmeta['vehicle_number'],
            'customer_name': vmeta['customer_name'],
            'active': 0, 'critical': 0, 'total_eps': 0, 'last_30': 0,
            'res_time_sum': 0.0, 'res_count': 0, 'last_ts': 0,
            'risk': 0.0, 'driver_related': 0,
            'dtc_counts': defaultdict(int),
            'subsystems': set(),
        }

    episode_count_per_fault = episodes_df.groupby(['uniqueid', 'dtc_code'])['episode_id'].nunique().to_dict() if not episodes_df.empty else {}

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        if uid not in per_vehicle:
            continue
        v = per_vehicle[uid]
        v['total_eps'] += 1
        v['dtc_counts'][str(r.dtc_code)] += 1
        v['last_ts'] = max(v['last_ts'], int(r.last_ts))
        if int(r.first_ts) >= thirty_days_ago_ts:
            v['last_30'] += 1
        if int(r.resolution_time_sec) > 0:
            v['res_time_sum'] += float(r.resolution_time_sec)
            v['res_count'] += 1
        if int(getattr(r, 'driver_related', 0)):
            v['driver_related'] += 1
        sub = str(getattr(r, 'subsystem', '') or '').lower()
        sys_ = str(getattr(r, 'system', '') or '').lower()
        v['subsystems'].add(sub)
        v['subsystems'].add(sys_)
        if r.is_resolved == 0:
            v['active'] += 1
            if int(r.severity_level) >= 3:
                v['critical'] += 1
            sev = _severity_weight(int(r.severity_level))
            dur = float(r.resolution_time_sec) / 86400.0
            persistence = 1.0 + min(dur, 30.0) / 10.0
            recurrence = float(episode_count_per_fault.get((r.uniqueid, r.dtc_code), 1))
            recurrence_f = 1.0 + max(0.0, recurrence - 1.0) * 0.5
            v['risk'] += sev * persistence * recurrence_f

    rows = []
    for uid, v in per_vehicle.items():
        health = max(0.0, 100.0 - min(v['risk'] * scaling_constant, 100.0))
        avg_res = v['res_time_sum'] / v['res_count'] if v['res_count'] else 0
        most_dtc = max(v['dtc_counts'], key=v['dtc_counts'].get, default='') if v['dtc_counts'] else ''
        subs = v['subsystems']
        rows.append((
            v['cli'], uid, v['vehicle_number'], v['customer_name'],
            v['active'], v['critical'], v['total_eps'], v['last_30'],
            round(avg_res, 2), v['last_ts'], round(health, 2),
            v['driver_related'], most_dtc,
            1 if any('powertrain' in s or 'engine' in s for s in subs) else 0,
            1 if any('emission' in s for s in subs) else 0,
            1 if any('safety' in s for s in subs) else 0,
            1 if any('electrical' in s for s in subs) else 0,
            now_utc, now_utc,
        ))
    return rows


def _build_dtc_fleet_impact(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                            dtc_lookup: dict) -> list[tuple]:
    now_utc = datetime.now(timezone.utc)
    if episodes_df.empty:
        return []

    key_stats: dict[tuple, dict] = defaultdict(lambda: {
        'vehicles': set(), 'active_vehicles': set(),
        'res_time_sum': 0.0, 'res_count': 0,
        'driver_related': 0, 'total': 0,
        'risk_sum': 0.0,
    })

    for r in episodes_df.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        if not cli:
            continue
        code = str(r.dtc_code)
        bucket = key_stats[(cli, code)]
        bucket['vehicles'].add(uid)
        bucket['total'] += 1
        if r.is_resolved == 0:
            bucket['active_vehicles'].add(uid)
            bucket['risk_sum'] += _severity_weight(int(r.severity_level))
        if int(r.resolution_time_sec) > 0:
            bucket['res_time_sum'] += float(r.resolution_time_sec)
            bucket['res_count'] += 1
        if int(getattr(r, 'driver_related', 0)):
            bucket['driver_related'] += 1

    rows = []
    for (cli, code), s in key_stats.items():
        dm = dtc_lookup.get(code, {})
        avg_res = s['res_time_sum'] / s['res_count'] if s['res_count'] else 0
        driver_ratio = s['driver_related'] / s['total'] if s['total'] else 0
        rows.append((
            cli, code,
            dm.get('system', ''), dm.get('subsystem', ''),
            len(s['vehicles']), len(s['active_vehicles']),
            round(avg_res, 2), round(driver_ratio, 4),
            round(s['risk_sum'], 2),
            now_utc, now_utc,
        ))
    return rows


def _build_maintenance_priority(episodes_df: pd.DataFrame, vehicle_lookup: dict,
                                dtc_lookup: dict) -> list[tuple]:
    """Active (unresolved) faults ranked by maintenance priority score."""
    now_utc = datetime.now(timezone.utc)
    thirty_days_ago_ts = int((datetime.now(timezone.utc) - timedelta(days=30)).timestamp())

    if episodes_df.empty:
        return []

    active = episodes_df[episodes_df['is_resolved'] == 0].copy()
    if active.empty:
        return []

    # Count recent episodes per (uniqueid, dtc_code)
    recent = episodes_df[episodes_df['first_ts'].astype(int) >= thirty_days_ago_ts]
    recent_counts = recent.groupby(['uniqueid', 'dtc_code']).size().to_dict() if not recent.empty else {}

    rows = []
    for r in active.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        cli = vmeta.get('clientLoginId', '')
        if not cli:
            continue
        code = str(r.dtc_code)
        dm = dtc_lookup.get(code, {})
        sev = int(r.severity_level)
        dur = int(r.resolution_time_sec)
        eps_30 = recent_counts.get((r.uniqueid, r.dtc_code), 0)
        # Priority score: severity × (1 + duration_days/10) × (1 + recent_episodes)
        priority = _severity_weight(sev) * (1.0 + min(dur / 86400.0, 30.0) / 10.0) * (1.0 + eps_30)
        rows.append((
            cli, uid, vmeta.get('vehicle_number', ''),
            code, dm.get('description', ''), sev,
            dur, eps_30, round(priority, 2),
            dm.get('action_required', ''),
            now_utc, now_utc,
        ))
    return rows


# ------------------------------------------------------------------
# Main entry point
# ------------------------------------------------------------------

def run_full_analytics_v2() -> dict:
    """Execute the full v2 analytics pipeline (bootstrap or CDC-incremental)."""
    pipeline_cfg = get_pipeline_cfg()
    gap_seconds = int(pipeline_cfg.get('episode_gap_seconds', 86400))
    final_day_cutoff_seconds = int(pipeline_cfg.get('final_day_cutoff_seconds', 86400))
    analytics_window_days = int(pipeline_cfg.get('analytics_window_days', 90))
    scaling_constant = float(os.getenv('HEALTH_SCALING_CONSTANT') or 0.2)
    cooccurrence_window = int(os.getenv('COOCCURRENCE_WINDOW_SEC') or 60)

    client = get_clickhouse_client()

    # ---- 0. Ensure all v2 tables exist ---------------------------------
    ensure_v2_tables(client)

    # ---- 1. Populate dimension tables ----------------------------------
    logging.info('Populating dimension tables ...')
    dtc_count = populate_dtc_master(client)
    vehicle_count = populate_vehicle_master(client)
    logging.info('Dimensions: %d DTC codes, %d vehicles', dtc_count, vehicle_count)

    vehicle_lookup = _load_vehicle_lookup(client)
    dtc_lookup = _load_dtc_lookup(client)
    valid_uniqueids = set(vehicle_lookup.keys())

    if not valid_uniqueids:
        logging.error('No vehicles after solutionType filter – aborting')
        return {'error': 'no vehicles after solutionType filter'}

    # ---- 2. CDC logic --------------------------------------------------
    cdc_enabled = bool(pipeline_cfg.get('cdc_enabled', True))
    run_window_end = datetime.now(timezone.utc)
    analytics_window_start_ts = int((run_window_end - timedelta(days=analytics_window_days)).timestamp())
    run_id = None
    pipeline_name = CDC_PIPELINE_NAME_V2

    if cdc_enabled:
        source_table = os.getenv('DTC_HISTORY_TABLE') or 'ss_dtc_data_history'
        bootstrap_days = int(pipeline_cfg.get('cdc_bootstrap_days', 90))
        ensure_cdc_infrastructure(client, source_table=source_table)
        checkpoint = get_cdc_checkpoint(client, pipeline_name)
        run_window_start = checkpoint if checkpoint else (run_window_end - timedelta(days=max(bootstrap_days, analytics_window_days)))
        run_id = start_cdc_run_log(client, pipeline_name, run_window_start, run_window_end)

    # ---- 3. Fetch raw events (filtered to valid vehicles, batched) ------
    sorted_uids = sorted(valid_uniqueids)
    batch_size = int(os.getenv('VEHICLE_FETCH_BATCH_SIZE') or 200)
    until_ts = int(run_window_end.timestamp())
    logging.info('Fetching DTC history for %d vehicles in batches of %d (last %d days) ...',
                 len(sorted_uids), batch_size, analytics_window_days)
    frames = []
    for i in range(0, len(sorted_uids), batch_size):
        batch = sorted_uids[i:i + batch_size]
        logging.info('  batch %d/%d (%d vehicles) ...',
                     i // batch_size + 1,
                     (len(sorted_uids) + batch_size - 1) // batch_size,
                     len(batch))
        df = fetch_dtc_history_for_uniqueids(
            uniqueids=batch,
            since_ts=analytics_window_start_ts,
            until_ts=until_ts,
        )
        logging.info('    -> %d rows', len(df))
        if not df.empty:
            frames.append(df)
    history_raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
        columns=['uniqueid', 'ts', 'lat', 'lng', 'dtc_code', 'dtc_pgn']
    )
    history_raw = _slice_history_window(history_raw, analytics_window_start_ts)
    logging.info('Fetched %d raw history rows', len(history_raw))

    codes_df = _normalize_codes(fetch_dtc_codes())
    engine_df = _normalize_engine_cycles(fetch_engineoncycles())

    # ---- 4. Explode + enrich with DTC info ----------------------------
    logging.info('Cleaning and exploding history ...')
    normalized_events = _clean_and_explode_history(history_raw)
    del history_raw  # free memory
    logging.info('Events after explode: %d', len(normalized_events))

    logging.info('Enriching events with DTC metadata ...')
    enriched_events = normalized_events.merge(codes_df, on='dtc_code', how='left')
    del normalized_events  # free memory
    enriched_events['severity_level'] = pd.to_numeric(enriched_events.get('severity_level'), errors='coerce').fillna(1).astype(int)
    enriched_events['subsystem'] = enriched_events.get('subsystem', '').fillna('').astype(str)
    enriched_events['system'] = enriched_events.get('system', '').fillna('').astype(str)
    enriched_events['description'] = enriched_events.get('description', '').fillna('').astype(str)

    # ---- 5. Write dtc_events_exploded (batched to limit memory) ----------
    now_utc = datetime.now(timezone.utc)
    WRITE_BATCH = 500_000
    exploded_batch: list[tuple] = []
    total_exploded = 0
    for r in enriched_events.itertuples(index=False):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid, {})
        exploded_batch.append((
            vmeta.get('clientLoginId', ''),
            uid,
            vmeta.get('vehicle_number', ''),
            int(r.ts),
            str(r.dtc_code),
            float(r.lat),
            float(r.lng),
            str(getattr(r, 'dtc_pgn', '') or ''),
            now_utc,
        ))
        if len(exploded_batch) >= WRITE_BATCH:
            write_dtc_events_exploded(client, exploded_batch)
            total_exploded += len(exploded_batch)
            logging.info('  dtc_events_exploded progress: %d rows written', total_exploded)
            exploded_batch = []
    if exploded_batch:
        write_dtc_events_exploded(client, exploded_batch)
        total_exploded += len(exploded_batch)
    logging.info('dtc_events_exploded: %d rows total', total_exploded)

    # ---- 6. Episode detection (per-vehicle to limit memory) ------------
    logging.info('Starting per-vehicle episode detection on %d events ...', len(enriched_events))
    vehicle_groups = enriched_events.groupby('uniqueid')
    num_vehicles = len(vehicle_groups)
    episode_frames: list[pd.DataFrame] = []
    processed_v = 0
    for uid, veh_events in vehicle_groups:
        veh_episodes = _compute_episodes(veh_events, engine_df, gap_seconds)
        if not veh_episodes.empty:
            episode_frames.append(veh_episodes)
        processed_v += 1
        if processed_v % 200 == 0:
            logging.info('  episode detection: %d/%d vehicles done', processed_v, num_vehicles)
    episodes = pd.concat(episode_frames, ignore_index=True) if episode_frames else pd.DataFrame(
        columns=['uniqueid', 'dtc_code', 'episode_id', 'first_ts', 'last_ts',
                 'occurrences', 'resolution_time_sec', 'severity_level', 'subsystem']
    )
    logging.info('Episode detection complete: %d episodes from %d vehicles', len(episodes), num_vehicles)
    del episode_frames  # free intermediate list
    gc.collect()
    episodes = _mark_resolution(episodes, enriched_events, engine_df, final_day_cutoff_seconds)
    logging.info('Episodes after resolution marking: %d', len(episodes))

    # ---- 7. Build vehicle_fault_master rows ----------------------------
    episode_count_per_fault = episodes.groupby(['uniqueid', 'dtc_code'])['episode_id'].nunique().to_dict() if not episodes.empty else {}

    # Pre-compute per-vehicle health risk scores (avoids O(n²) inner loop)
    vehicle_risk_cache: dict[str, float] = {}
    if not episodes.empty:
        active_eps = episodes[episodes['is_resolved'] == 0]
        for uid_key, grp in active_eps.groupby('uniqueid'):
            risk_sum = 0.0
            for ar in grp.itertuples(index=False):
                sev = _severity_weight(int(ar.severity_level))
                dur = float(ar.resolution_time_sec) / 86400.0
                persistence = 1.0 + min(dur, 30.0) / 10.0
                rec = float(episode_count_per_fault.get((ar.uniqueid, ar.dtc_code), 1))
                rec_f = 1.0 + max(0.0, rec - 1.0) * 0.5
                risk_sum += sev * persistence * rec_f
            vehicle_risk_cache[str(uid_key)] = risk_sum

    # Pre-group engine cycles by uniqueid for O(1) lookup
    engine_by_vehicle: dict[str, pd.DataFrame] = {}
    if not engine_df.empty:
        for uid_key, grp in engine_df.groupby('uniqueid'):
            engine_by_vehicle[str(uid_key)] = grp

    fault_rows = []
    prev_last_ts: dict[tuple, int] = {}
    logging.info('Building vehicle_fault_master from %d episodes ...', len(episodes))

    sorted_episodes = episodes.sort_values(['uniqueid', 'dtc_code', 'first_ts'])
    ep_total = len(sorted_episodes)
    for idx, r in enumerate(sorted_episodes.itertuples(index=False)):
        uid = str(r.uniqueid)
        vmeta = vehicle_lookup.get(uid)
        if not vmeta:
            continue
        code = str(r.dtc_code)
        dm = dtc_lookup.get(code, {})
        cli = vmeta['clientLoginId']

        # Episode id as deterministic string
        ep_id = f"{uid}_{code}_{r.episode_id}"

        # Gap from previous episode for same (uniqueid, dtc_code)
        key = (uid, code)
        gap_from_prev = 0
        if key in prev_last_ts:
            gap_from_prev = max(0, int(r.first_ts) - prev_last_ts[key])
        prev_last_ts[key] = int(r.last_ts)

        # Engine cycles during episode (using pre-grouped data)
        eng_cycles = 0
        veh_engine = engine_by_vehicle.get(uid)
        if veh_engine is not None:
            mask = (veh_engine['cycle_end_ts'] >= int(r.first_ts)) & \
                   (veh_engine['cycle_end_ts'] <= int(r.last_ts))
            eng_cycles = int(mask.sum())

        sub = str(dm.get('subsystem', '')).lower()
        sys_ = str(dm.get('system', '')).lower()
        driver_related = int(dm.get('driver_related', 0))

        # Per-vehicle health score (pre-computed, O(1) lookup)
        risk = vehicle_risk_cache.get(uid, 0.0)
        health_score = max(0.0, 100.0 - min(risk * scaling_constant, 100.0))

        event_date = pd.Timestamp(int(r.first_ts), unit='s', tz='UTC').date()

        fault_rows.append((
            ep_id, cli, uid,
            vmeta.get('vehicle_number', ''),
            vmeta.get('customer_name', ''),
            vmeta.get('model', ''),
            vmeta.get('manufacturing_year', 0),
            code,
            dm.get('system', ''), dm.get('subsystem', ''), dm.get('description', ''),
            dm.get('severity_level', 1),
            int(r.first_ts), int(r.last_ts), event_date,
            int(r.occurrences), int(r.resolution_time_sec),
            int(r.is_resolved), '',  # resolution_reason placeholder
            gap_from_prev, eng_cycles, driver_related,
            1 if ('powertrain' in sub or 'engine' in sys_) else 0,
            1 if 'coolant' in sub else 0,
            1 if 'safety' in sys_ else 0,
            1 if 'emission' in sys_ else 0,
            1 if 'electrical' in sys_ else 0,
            round(health_score, 2),
            now_utc, now_utc,
        ))
        if (idx + 1) % 50000 == 0:
            logging.info('  fault_master progress: %d/%d episodes', idx + 1, ep_total)

    write_vehicle_fault_master(client, fault_rows)
    logging.info('vehicle_fault_master: %d rows', len(fault_rows))

    # ---- 8. Build a reusable episodes DF with extra columns for analytics
    if fault_rows:
        ep_cols = [
            'episode_id', 'clientLoginId', 'uniqueid', 'vehicle_number', 'customer_name',
            'model', 'manufacturing_year', 'dtc_code', 'system', 'subsystem', 'description',
            'severity_level', 'first_ts', 'last_ts', 'event_date', 'occurrence_count',
            'resolution_time_sec', 'is_resolved', 'resolution_reason',
            'gap_from_previous_episode', 'engine_cycles_during', 'driver_related',
            'has_engine_issue', 'has_coolant_issue', 'has_safety_issue',
            'has_emission_issue', 'has_electrical_issue', 'vehicle_health_score',
            'created_at', 'updated_at',
        ]
        episodes_rich = pd.DataFrame(fault_rows, columns=ep_cols)
    else:
        episodes_rich = pd.DataFrame()

    # ---- 9. Derived analytics tables -----------------------------------
    logging.info('Building derived analytics tables ...')

    fleet_health_rows = _build_fleet_health_summary(episodes_rich, vehicle_lookup, scaling_constant)
    write_fleet_health_summary(client, fleet_health_rows)
    logging.info('fleet_health_summary: %d rows', len(fleet_health_rows))

    fleet_dtc_rows = _build_fleet_dtc_distribution(episodes_rich, vehicle_lookup, dtc_lookup)
    write_fleet_dtc_distribution(client, fleet_dtc_rows)
    logging.info('fleet_dtc_distribution: %d rows', len(fleet_dtc_rows))

    fleet_sys_rows = _build_fleet_system_health(episodes_rich, vehicle_lookup)
    write_fleet_system_health(client, fleet_sys_rows)
    logging.info('fleet_system_health: %d rows', len(fleet_sys_rows))

    fleet_trend_rows = _build_fleet_fault_trends(episodes_rich, vehicle_lookup, scaling_constant)
    write_fleet_fault_trends(client, fleet_trend_rows)
    logging.info('fleet_fault_trends: %d rows', len(fleet_trend_rows))

    veh_health_rows = _build_vehicle_health_summary(episodes_rich, vehicle_lookup, scaling_constant)
    write_vehicle_health_summary(client, veh_health_rows)
    logging.info('vehicle_health_summary: %d rows', len(veh_health_rows))

    dtc_impact_rows = _build_dtc_fleet_impact(episodes_rich, vehicle_lookup, dtc_lookup)
    write_dtc_fleet_impact(client, dtc_impact_rows)
    logging.info('dtc_fleet_impact: %d rows', len(dtc_impact_rows))

    maint_rows = _build_maintenance_priority(episodes_rich, vehicle_lookup, dtc_lookup)
    write_maintenance_priority(client, maint_rows)
    logging.info('maintenance_priority: %d rows', len(maint_rows))

    # Co-occurrence (computed from raw exploded events, not episodes)
    logging.info('Computing DTC co-occurrence from %d events ...', len(enriched_events))
    cooccur_rows = _compute_cooccurrence(enriched_events, vehicle_lookup, cooccurrence_window)
    del enriched_events  # free large DataFrame
    gc.collect()
    write_dtc_cooccurrence(client, cooccur_rows)
    logging.info('dtc_cooccurrence: %d rows', len(cooccur_rows))

    # ---- 10. CDC bookkeeping -------------------------------------------
    summary = {
        'dtc_master': dtc_count,
        'vehicle_master': vehicle_count,
        'dtc_events_exploded': len(exploded_rows),
        'vehicle_fault_master': len(fault_rows),
        'fleet_health_summary': len(fleet_health_rows),
        'fleet_dtc_distribution': len(fleet_dtc_rows),
        'fleet_system_health': len(fleet_sys_rows),
        'fleet_fault_trends': len(fleet_trend_rows),
        'vehicle_health_summary': len(veh_health_rows),
        'dtc_fleet_impact': len(dtc_impact_rows),
        'maintenance_priority': len(maint_rows),
        'dtc_cooccurrence': len(cooccur_rows),
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
            source_rows=len(history_raw),
            output_rows=total_output,
            message=f'v2 pipeline bootstrap completed',
        )

    logging.info('v2 analytics pipeline completed: %s', summary)
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
