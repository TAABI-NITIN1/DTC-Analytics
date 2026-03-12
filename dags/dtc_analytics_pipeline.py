from airflow.decorators import dag, task
from datetime import datetime, timedelta
from pathlib import Path
import os

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault('DTC_PIPELINE_ROOT', str(PROJECT_ROOT))


def _read_venv_requirements() -> list[str]:
    req_file = PROJECT_ROOT / 'dags' / 'venv_requirements.txt'
    if not req_file.exists():
        return []
    return [
        line.strip()
        for line in req_file.read_text(encoding='utf-8').splitlines()
        if line.strip() and not line.strip().startswith('#')
    ]


VENV_REQUIREMENTS = _read_venv_requirements()


default_args = {
    'owner': 'data',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}


@dag(
    dag_id='dtc_clickhouse_pipeline',
    start_date=datetime(2026, 1, 1),
    schedule='0 */6 * * *',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
)
def dtc_analytics_pipeline():

    @task.virtualenv(requirements=VENV_REQUIREMENTS, system_site_packages=False)
    def run_full_pipeline():
        from pathlib import Path
        import os
        import sys

        project_root = Path(os.getenv('DTC_PIPELINE_ROOT') or '.')
        src_path = project_root / 'src'
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))
        if str(src_path) not in sys.path:
            sys.path.append(str(src_path))

        from src.config import load_env
        from src.analytics_pipeline_v2_sql import run_full_analytics_v2

        load_env(project_root / '.env')
        return run_full_analytics_v2()

    @task.virtualenv(requirements=VENV_REQUIREMENTS, system_site_packages=False)
    def validate_outputs():
        from pathlib import Path
        import os
        import sys

        project_root = Path(os.getenv('DTC_PIPELINE_ROOT') or '.')
        src_path = project_root / 'src'
        if str(project_root) not in sys.path:
            sys.path.append(str(project_root))
        if str(src_path) not in sys.path:
            sys.path.append(str(src_path))

        from src.config import load_env
        from src.clickhouse_utils import get_clickhouse_client
        from src.clickhouse_utils_v2 import V2_TABLES

        load_env(project_root / '.env')
        client = get_clickhouse_client()

        checks = {
            'dtc_events_exploded': f"SELECT count() FROM {V2_TABLES['dtc_events_exploded']}",
            'vehicle_fault_master': f"SELECT count() FROM {V2_TABLES['vehicle_fault_master']}",
            'fleet_health_summary': f"SELECT count() FROM {V2_TABLES['fleet_health_summary']}",
            'fleet_fault_trends': f"SELECT count() FROM {V2_TABLES['fleet_fault_trends']}",
            'vehicle_health_summary': f"SELECT count() FROM {V2_TABLES['vehicle_health_summary']}",
            'dtc_fleet_impact': f"SELECT count() FROM {V2_TABLES['dtc_fleet_impact']}"
        }

        result = {}
        for key, query in checks.items():
            rows = client.execute(query)
            result[key] = int(rows[0][0]) if rows else 0

        if result['dtc_events_exploded'] == 0:
            raise ValueError('Validation failed: dtc_events_exploded is empty')

        if result['vehicle_fault_master'] == 0:
            raise ValueError('Validation failed: vehicle_fault_master is empty')

        return result

    run = run_full_pipeline()
    validate_outputs().set_upstream(run)


dag = dtc_analytics_pipeline()

