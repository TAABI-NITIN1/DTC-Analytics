# DTC Pipeline

Airflow pipeline for ClickHouse-native DTC analytics with CDC foundations.

Current source flow:

1. Read DTC history from ClickHouse source table `ss_dtc_data_history`
2. Maintain exploded source table `ss_dtc_data_history_exploded_ravi` (MV + bootstrap backfill)
3. Fetch reference tables from ClickHouse: `dtc_codes_updated`, `ss_engineoncycles`, `vehicle_profile_ss`
4. Clean and explode `dtc_code`
5. Join DTC knowledge fields (severity/subsystem/system)
6. Compute episode/resolution/health analytics
7. Write analytics-optimized tables to ClickHouse

## Shared VM isolation model

This DAG uses task-level isolated environments via `@task.virtualenv`.

- Do not create/activate venv in DAG parse code.
- Task dependencies are pinned in `dags/venv_requirements.txt`.
- This keeps your DAG isolated on shared Airflow workers without affecting other teams.

## Quick start (PowerShell)

```powershell
cd c:\Users\Client\OneDrive - RPG Enterprises\Attachments\Ravi\dtc\DTC_Pipeline
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## DAG flow

`dtc_clickhouse_pipeline`

1. Run end-to-end analytics build
2. Validate output table row counts

## CDC status

Stage-1 CDC is enabled:

- Checkpoint table: `cdc_checkpoint_ravi`
- Run log table: `cdc_run_log_ravi`
- Replay window: controlled by `CDC_REPLAY_HOURS`
- Bootstrap window: controlled by `CDC_BOOTSTRAP_DAYS`

Current behavior preserves output correctness by rebuilding analytics outputs after CDC window detection. Stage-2 will switch writes to impacted-slice upserts.

## Derived analytics tables (ClickHouse)

- `normalized_dtc_events_ravi`
- `vehicle_fault_episodes_ravi`
- `vehicle_current_status_ravi`
- `dtc_daily_stats_ravi`
- `fleet_daily_stats_ravi`

## New API endpoints

- `/api/fleet/overview`
- `/api/fleet/trend`
- `/api/fleet/top-risk-vehicles`
- `/api/vehicle/{uniqueid}/overview`
- `/api/vehicle/{uniqueid}/active-faults`
- `/api/vehicle/{uniqueid}/timeline`
- `/api/dtc/{dtc_code}/overview`
- `/api/dtc/{dtc_code}/trend`
- `/api/dtc/{dtc_code}/affected-vehicles`
