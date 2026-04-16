from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from typing import Any

from langchain_core.messages import AIMessage

from src.clickhouse_utils import get_clickhouse_client
from src.observability_store import OBS_TABLES, ensure_observability_tables, persist_observability_run

try:
    from langsmith import Client as LangSmithClient
except Exception:
    LangSmithClient = None


SYNC_STATE_TABLE = 'ai_obs_sync_state'


def _as_plain_dict(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    try:
        return dict(value)
    except Exception:
        return {}


def _extract_text_from_messages(messages_obj: Any) -> str:
    if not isinstance(messages_obj, list) or not messages_obj:
        return ''
    last = messages_obj[-1]
    if isinstance(last, AIMessage):
        return str(last.content or '')
    if isinstance(last, dict):
        if 'content' in last:
            return str(last.get('content') or '')
        data = last.get('data') if isinstance(last.get('data'), dict) else {}
        return str(data.get('content') or '')
    return str(getattr(last, 'content', '') or '')


def _to_datetime_utc(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None
    return None


def _to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _ensure_sync_state_table() -> None:
    client = get_clickhouse_client()
    client.execute(
        f'''
        CREATE TABLE IF NOT EXISTS {SYNC_STATE_TABLE} (
            pipeline_key String,
            last_synced_at DateTime,
            updated_at DateTime,
            note String
        )
        ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (pipeline_key)
        '''
    )


def _read_last_synced_at(pipeline_key: str) -> datetime | None:
    client = get_clickhouse_client()
    escaped_key = (pipeline_key or '').replace("'", "''")
    rows = client.execute(
        f"SELECT last_synced_at FROM {SYNC_STATE_TABLE} WHERE pipeline_key = '{escaped_key}' ORDER BY updated_at DESC LIMIT 1"
    )
    if not rows:
        return None
    value = rows[0][0] if rows[0] else None
    return _to_datetime_utc(value)


def _write_last_synced_at(pipeline_key: str, last_synced_at: datetime, note: str = '') -> None:
    client = get_clickhouse_client()
    now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
    synced_naive = _to_naive_utc(last_synced_at)
    insert_query = (
        f"INSERT INTO {SYNC_STATE_TABLE} (pipeline_key, last_synced_at, updated_at, note) VALUES"
    )
    client.execute(
        insert_query,
        [
            (
                str(pipeline_key or 'langsmith_root_runs'),
                synced_naive,
                now_naive,
                str(note or ''),
            )
        ],
    )


def _extract_run_payload(run: Any) -> tuple[str, dict] | None:
    outputs = _as_plain_dict(getattr(run, 'outputs', {}) or {})
    extra = _as_plain_dict(getattr(run, 'extra', {}) or {})
    metadata = _as_plain_dict(extra.get('metadata', {}) or {})
    version = _as_plain_dict(outputs.get('version', {}) or {})

    request_id = str(metadata.get('request_id') or getattr(run, 'id', '') or '')
    if not request_id:
        return None

    query = str(metadata.get('query') or '')
    if not query:
        inputs = _as_plain_dict(getattr(run, 'inputs', {}) or {})
        query = str(inputs.get('query') or '')

    text = str(outputs.get('text') or '')
    if not text:
        text = _extract_text_from_messages(outputs.get('messages'))

    payload = {
        'request_id': request_id,
        'intent': str(outputs.get('intent') or metadata.get('intent') or ''),
        'text': text,
        'trace_log': outputs.get('trace_log') if isinstance(outputs.get('trace_log'), list) else [],
        'evaluation': _as_plain_dict(outputs.get('evaluation', {}) or {}),
        'nodes_executed': outputs.get('nodes_executed') if isinstance(outputs.get('nodes_executed'), list) else [],
        'version': version,
        'tools_called': outputs.get('tools_called') if isinstance(outputs.get('tools_called'), list) else [],
        'sql_events': outputs.get('sql_events') if isinstance(outputs.get('sql_events'), list) else [],
        'token_usage': _as_plain_dict(outputs.get('token_usage') or {}),
        'failure_reasons': outputs.get('failure_reasons') if isinstance(outputs.get('failure_reasons'), list) else [],
    }
    return query, payload


def _existing_request_ids(candidate_ids: list[str]) -> set[str]:
    if not candidate_ids:
        return set()

    client = get_clickhouse_client()
    escaped = []
    for rid in candidate_ids:
        if not rid:
            continue
        escaped.append("'" + rid.replace("'", "''") + "'")
    if not escaped:
        return set()

    query = (
        f"SELECT request_id FROM {OBS_TABLES['requests']} "
        f"WHERE request_id IN ({', '.join(escaped)})"
    )
    rows = client.execute(query)
    return {str(r[0]) for r in rows if r and r[0] is not None}


def sync_langsmith_root_runs_to_clickhouse(
    project_name: str | None = None,
    *,
    limit: int = 200,
    lookback_days: int = 14,
    use_checkpoint: bool = True,
    pipeline_key: str = 'langsmith_root_runs',
    safety_lookback_minutes: int = 5,
) -> dict:
    if LangSmithClient is None:
        return {'ok': False, 'error': 'langsmith package is unavailable in this environment.'}

    project = project_name or os.getenv('LANGCHAIN_PROJECT') or os.getenv('LANGSMITH_PROJECT') or 'default'
    fallback_cutoff = datetime.now(timezone.utc) - timedelta(days=max(int(lookback_days), 1))

    ensure_observability_tables()
    if use_checkpoint:
        _ensure_sync_state_table()

    checkpoint_cutoff = None
    if use_checkpoint:
        last_synced_at = _read_last_synced_at(pipeline_key)
        if isinstance(last_synced_at, datetime):
            checkpoint_cutoff = last_synced_at - timedelta(minutes=max(int(safety_lookback_minutes), 0))

    cutoff = checkpoint_cutoff or fallback_cutoff
    client = LangSmithClient()

    try:
        runs = list(client.list_runs(project_name=project, is_root=True, limit=max(int(limit), 1)))
    except Exception as exc:
        return {'ok': False, 'error': f'LangSmith list_runs failed: {exc}', 'project': project}

    eligible_runs = []
    max_seen_start: datetime | None = None
    for run in runs:
        start_dt = _to_datetime_utc(getattr(run, 'start_time', None))
        if start_dt is not None and start_dt < cutoff:
            continue
        run_id = str(getattr(run, 'id', '') or '')
        if run_id:
            eligible_runs.append(run)
            if isinstance(start_dt, datetime):
                if max_seen_start is None or start_dt > max_seen_start:
                    max_seen_start = start_dt

    ids = []
    for run in eligible_runs:
        md = _as_plain_dict(_as_plain_dict(getattr(run, 'extra', {}) or {}).get('metadata', {}) or {})
        ids.append(str(md.get('request_id') or getattr(run, 'id', '') or ''))

    existing = _existing_request_ids(ids)

    inserted = 0
    skipped_existing = 0
    skipped_empty = 0
    failed = 0

    for run in eligible_runs:
        extracted = _extract_run_payload(run)
        if extracted is None:
            skipped_empty += 1
            continue

        query, payload = extracted
        request_id = str(payload.get('request_id') or '')
        if request_id in existing:
            skipped_existing += 1
            continue

        try:
            persist_observability_run(query=query, payload=payload)
            inserted += 1
        except Exception:
            failed += 1

    if use_checkpoint and isinstance(max_seen_start, datetime):
        try:
            _write_last_synced_at(
                pipeline_key=pipeline_key,
                last_synced_at=max_seen_start,
                note=f'inserted={inserted}, skipped_existing={skipped_existing}, failed={failed}',
            )
        except Exception:
            pass

    return {
        'ok': True,
        'project': project,
        'pipeline_key': pipeline_key,
        'cutoff_iso': cutoff.isoformat(),
        'fetched': len(runs),
        'eligible': len(eligible_runs),
        'inserted': inserted,
        'skipped_existing': skipped_existing,
        'skipped_empty': skipped_empty,
        'failed': failed,
    }
