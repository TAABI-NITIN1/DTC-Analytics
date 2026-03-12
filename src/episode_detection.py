import pandas as pd
from clickhouse_driver import Client
from datetime import datetime
import logging
import hashlib

from src.config import get_pipeline_cfg


PIPELINE_CFG = get_pipeline_cfg()
EPISODE_GAP_SECONDS = int(PIPELINE_CFG.get('episode_gap_seconds', 1800))
FINAL_DAY_CUTOFF_SECONDS = int(PIPELINE_CFG.get('final_day_cutoff_seconds', 86400))


def run_episode_detection(client: Client, enriched_batch, engine_cycles=None):
    """
    Simple batch episode detection sketch.

    Args:
        client: ClickHouse client
        enriched_batch: None, DataFrame or iterable of dicts/tuples. If None, function is a no-op.
    """
    if enriched_batch is None:
        logging.info('No enriched batch provided — skipping episode detection')
        return

    # Accept pandas DataFrame or list of dicts
    if isinstance(enriched_batch, pd.DataFrame):
        df = enriched_batch
    else:
        try:
            df = pd.DataFrame(enriched_batch)
        except Exception as e:
            logging.error('Cannot coerce enriched_batch to DataFrame: %s', e)
            return

    if df.empty:
        logging.info('enriched batch empty')
        return

    # Ensure required columns exist
    required = {'uniqueid', 'ts', 'dtc_code'}
    if not required.issubset(set(df.columns)):
        logging.error('enriched batch missing required columns: %s', required - set(df.columns))
        return

    # Clean up ts values to avoid NaN -> int conversion errors
    df['ts'] = pd.to_numeric(df['ts'], errors='coerce')
    df = df[df['ts'].notna()]
    if df.empty:
        logging.info('enriched batch empty after dropping rows without ts')
        return

    engine_map = {}
    if engine_cycles is not None and not engine_cycles.empty:
        cycles = engine_cycles.copy()
        if 'cycle_end_ts' in cycles.columns:
            cycles['cycle_end_ts'] = pd.to_numeric(cycles['cycle_end_ts'], errors='coerce')
            cycles = cycles[cycles['cycle_end_ts'].notna()]
            engine_map = cycles.groupby('uniqueid')['cycle_end_ts'].apply(lambda s: sorted(s.astype(int).tolist())).to_dict()

    global_max_ts = int(df['ts'].max()) if not df.empty else 0
    ambiguity_cutoff_ts = max(0, global_max_ts - FINAL_DAY_CUTOFF_SECONDS)

    # Episode grouping by gap threshold per (uniqueid, dtc_code)
    df = df.sort_values(['uniqueid', 'dtc_code', 'ts'])
    rows_events = []
    rows_episodes = []
    state_rows = []
    state_rows_v2 = []

    for (uid, code), group in df.groupby(['uniqueid', 'dtc_code']):
        times = group['ts'].astype(int).tolist()
        if not times:
            continue

        # Build episode ranges based on detection gap
        episode_ranges = []
        start_idx = 0
        for i in range(1, len(times)):
            if (times[i] - times[i - 1]) > EPISODE_GAP_SECONDS:
                episode_ranges.append((start_idx, i - 1))
                start_idx = i
        episode_ranges.append((start_idx, len(times) - 1))

        engine_times = engine_map.get(uid, [])
        last_engine_on_ts = int(engine_times[-1]) if engine_times else 0

        last_episode_id = 0
        last_episode_last_ts = 0
        last_episode_is_resolved = 0

        for episode_number, (start_i, end_i) in enumerate(episode_ranges, start=1):
            first_ts = times[start_i]
            last_ts = times[end_i]
            occurrences = (end_i - start_i + 1)
            persistence_sec = max(0, last_ts - first_ts)

            deterministic_seed = f"{uid}|{code}|{first_ts}|{episode_number}"
            digest = hashlib.sha1(deterministic_seed.encode('utf-8')).hexdigest()[:15]
            episode_id = int(digest, 16)

            next_episode_start = times[episode_ranges[episode_number][0]] if episode_number < len(episode_ranges) else None

            candidate_engine_ts = None
            for cycle_ts in engine_times:
                if cycle_ts > last_ts and (next_episode_start is None or cycle_ts < next_episode_start):
                    candidate_engine_ts = cycle_ts
                    break

            has_reappearance_after_candidate = False
            if candidate_engine_ts is not None:
                for event_ts in times:
                    if event_ts > candidate_engine_ts and (next_episode_start is None or event_ts < next_episode_start):
                        has_reappearance_after_candidate = True
                        break

            is_recent_ambiguous = last_ts > ambiguity_cutoff_ts
            if candidate_engine_ts is not None and not has_reappearance_after_candidate and not is_recent_ambiguous:
                is_resolved = 1
                resolution_reason = 'engine_cycle_no_reappearance'
            elif is_recent_ambiguous:
                is_resolved = 0
                resolution_reason = 'recent_window_ambiguous'
            else:
                is_resolved = 0
                resolution_reason = ''

            rows_episodes.append((
                uid,
                code,
                episode_id,
                first_ts,
                last_ts,
                occurrences,
                persistence_sec,
                is_resolved,
                resolution_reason,
                datetime.utcfromtimestamp(first_ts).date(),
            ))

            last_episode_id = episode_id
            last_episode_last_ts = last_ts
            last_episode_is_resolved = is_resolved

        # collect event rows for insertion (minimal columns to match helper insert)
        for _, r in group.iterrows():
            ts_val = int(r.get('ts'))
            sev_raw = r.get('severity_level')
            severity_val = 0 if pd.isna(sev_raw) else int(sev_raw)
            rows_events.append((
                r.get('uniqueid'),
                ts_val,
                datetime.utcfromtimestamp(ts_val),
                r.get('dtc_code'),
                r.get('dtc_pgn'),
                r.get('lat') or 0.0,
                r.get('lng') or 0.0,
                r.get('code') or '',
                r.get('description') or '',
                severity_val,
                r.get('subsystem') or '',
                r.get('system') or '',
                datetime.utcnow(),
            ))

        # update state using latest episode for this vehicle+dtc
        state_rows.append((uid, code, last_episode_last_ts, last_episode_id, last_engine_on_ts or 0))
        state_rows_v2.append((
            uid,
            code,
            last_episode_last_ts,
            last_engine_on_ts or 0,
            0 if last_episode_is_resolved else 1,
            last_episode_id,
            datetime.utcnow(),
        ))

    # write to ClickHouse using simple inserts
    try:
        if rows_events:
            client.execute('INSERT INTO events_enriched (uniqueid, ts, TimeStamp, dtc_code, dtc_pgn, lat, lng, code, description, severity_level, subsystem, system, ingestion_ts) VALUES', rows_events)
        if rows_episodes:
            client.execute('INSERT INTO episodes (uniqueid, dtc_code, episode_id, first_ts, last_ts, occurrences, persistence_sec, is_resolved, resolution_reason, event_date) VALUES', rows_episodes)
        if state_rows:
            client.execute('INSERT INTO dtc_state (uniqueid, dtc_code, last_dtc_ts, episode_id, last_engine_on_ts) VALUES', state_rows)
        if state_rows_v2:
            client.execute('INSERT INTO dtc_state_v2 (uniqueid, dtc_code, last_dtc_ts, last_engine_on_ts, is_active, last_episode_id, update_ts) VALUES', state_rows_v2)
    except Exception as e:
        logging.exception('Failed to write results to ClickHouse: %s', e)
