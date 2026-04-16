import logging
import os
import time

from src.config import load_env
from src.langsmith_sync import sync_langsmith_root_runs_to_clickhouse


logging.basicConfig(
    level=getattr(logging, os.getenv('LANGSMITH_SYNC_LOG_LEVEL', 'INFO').upper(), logging.INFO),
    format='%(asctime)s %(levelname)s %(message)s',
)
log = logging.getLogger('langsmith_sync_worker')


def _to_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def main() -> None:
    load_env()

    enabled = str(os.getenv('LANGSMITH_SYNC_ENABLED', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    if not enabled:
        log.info('LANGSMITH_SYNC_ENABLED is disabled. Exiting worker.')
        return

    project = os.getenv('LANGSMITH_SYNC_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or os.getenv('LANGSMITH_PROJECT') or 'default'
    interval_sec = max(_to_int('LANGSMITH_SYNC_INTERVAL_SEC', 120), 15)
    limit = max(_to_int('LANGSMITH_SYNC_LIMIT', 200), 1)
    lookback_days = max(_to_int('LANGSMITH_SYNC_LOOKBACK_DAYS', 14), 1)
    use_checkpoint = str(os.getenv('LANGSMITH_SYNC_USE_CHECKPOINT', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
    pipeline_key = str(os.getenv('LANGSMITH_SYNC_PIPELINE_KEY', 'langsmith_root_runs')).strip() or 'langsmith_root_runs'
    safety_lookback_minutes = max(_to_int('LANGSMITH_SYNC_SAFETY_LOOKBACK_MINUTES', 5), 0)

    log.info(
        'Starting LangSmith->ClickHouse sync worker | project=%s interval_sec=%s limit=%s lookback_days=%s checkpoint=%s pipeline_key=%s safety_lookback_minutes=%s',
        project,
        interval_sec,
        limit,
        lookback_days,
        use_checkpoint,
        pipeline_key,
        safety_lookback_minutes,
    )

    while True:
        try:
            result = sync_langsmith_root_runs_to_clickhouse(
                project_name=project,
                limit=limit,
                lookback_days=lookback_days,
                use_checkpoint=use_checkpoint,
                pipeline_key=pipeline_key,
                safety_lookback_minutes=safety_lookback_minutes,
            )
            if result.get('ok'):
                log.info(
                    'sync ok | fetched=%s eligible=%s inserted=%s skipped_existing=%s skipped_empty=%s failed=%s',
                    result.get('fetched', 0),
                    result.get('eligible', 0),
                    result.get('inserted', 0),
                    result.get('skipped_existing', 0),
                    result.get('skipped_empty', 0),
                    result.get('failed', 0),
                )
            else:
                log.warning('sync failed | error=%s', result.get('error', 'unknown'))
        except Exception as exc:
            log.exception('sync loop error: %s', exc)

        time.sleep(interval_sec)


if __name__ == '__main__':
    main()
