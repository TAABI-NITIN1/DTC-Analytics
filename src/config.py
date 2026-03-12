import os
from dotenv import load_dotenv


def load_env(env_path=None):
    # Load environment variables from .env if present
    load_dotenv(dotenv_path=env_path, override=True)


def _get_clickhouse_cfg(prefix: str):
    return {
        'host': os.getenv(f'{prefix}_Host'),
        'port': int(os.getenv(f'{prefix}_Port') or 0),
        'user': os.getenv(f'{prefix}_User'),
        'password': os.getenv(f'{prefix}_Password'),
        'database': os.getenv(f'{prefix}_Database'),
    }


def get_clickhouse_cfg():
    return _get_clickhouse_cfg('CH_DB')


def get_vehicle_clickhouse_cfg():
    return _get_clickhouse_cfg('VEHICLE_CH_DB')


def get_pipeline_cfg():
    return {
        'fetch_window_days': int(os.getenv('FETCH_WINDOW_DAYS') or 30),
        'reset_agg_tables': str(os.getenv('RESET_AGG_TABLES') or 'false').lower() in {'1', 'true', 'yes'},
        'analytics_window_days': int(os.getenv('ANALYTICS_WINDOW_DAYS') or 90),
        'episode_gap_seconds': int(os.getenv('EPISODE_GAP_SECONDS') or 1800),
        'final_day_cutoff_seconds': int(os.getenv('FINAL_DAY_CUTOFF_SECONDS') or 86400),
        'cdc_enabled': str(os.getenv('CDC_ENABLED') or 'true').lower() in {'1', 'true', 'yes'},
        'cdc_pipeline_name': os.getenv('CDC_PIPELINE_NAME') or 'dtc_analytics_ravi',
        'cdc_replay_hours': int(os.getenv('CDC_REPLAY_HOURS') or 1),
        'cdc_bootstrap_days': int(os.getenv('CDC_BOOTSTRAP_DAYS') or 90),
    }
