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

## Developer monitoring and version tracking

This project supports developer-only observability through LangSmith (trace-level)
and MLflow (aggregate metrics/evaluation trends). No customer-facing UI changes are required.

### Recommended environment variables

- `LANGSMITH_TRACING=true`
- `LANGSMITH_API_KEY=...`
- `LANGSMITH_PROJECT=AI for Vehicle Health`
- `MLFLOW_TRACKING_URI=http://localhost:5000`

Version metadata (attached to runtime traces and eval logs):

- `AI_ANALYST_RELEASE_VERSION=2026.03.22.1`
- `AI_ANALYST_SERVICE_VERSION=backend-v1`
- `AI_ANALYST_MODEL_NAME=gpt-3.5-turbo`
- `AI_ANALYST_DATASET_VERSION=v1`
- `DEPLOYMENT_ENV=dev`
- `GIT_COMMIT_SHA=<short-or-full-sha>`

### Evaluation modes

Run heuristic-only scoring (default):

`python evaluation/run_evaluation.py`

Run semantic LLM-judge scoring (costly but richer):

- `EVAL_USE_LLM_JUDGE=1`
- Optional: `EVAL_JUDGE_MODEL=gpt-4o-mini`
- `python evaluation/run_evaluation.py`

## LangGraph Studio (local graph debugging)

This repository now includes `langgraph.json` configured for the AI analyst graph:

- Graph id: `ai_analyst`
- Graph entrypoint: `src/ai_analyst.py:agent_graph`

### Run locally (PowerShell)

```powershell
cd c:\Users\Client\Downloads\DTC_Analytics_Local
.\myvenv\Scripts\Activate.ps1
pip install "langgraph-cli[inmem]"
langgraph dev
```

Then open the Studio URL shown in terminal to inspect node execution, state transitions,
and routing in the graph while keeping ClickHouse + Streamlit observability for production analytics.

## Automatic LangSmith -> ClickHouse Sync

To continuously mirror LangSmith root traces into ClickHouse observability tables,
run the sync worker.

### Environment variables

- `LANGSMITH_SYNC_ENABLED=1`
- `LANGSMITH_SYNC_INTERVAL_SEC=120`
- `LANGSMITH_SYNC_LIMIT=200`
- `LANGSMITH_SYNC_LOOKBACK_DAYS=14`
- `LANGSMITH_SYNC_USE_CHECKPOINT=1`
- `LANGSMITH_SYNC_PIPELINE_KEY=langsmith_root_runs`
- `LANGSMITH_SYNC_SAFETY_LOOKBACK_MINUTES=5`
- Optional: `LANGSMITH_SYNC_PROJECT=AI for Vehicle Health`
	- If empty, worker uses `LANGCHAIN_PROJECT` or `LANGSMITH_PROJECT`.

### Local run (PowerShell)

```powershell
cd c:\Users\Client\Downloads\DTC_Analytics_Local
.\myvenv\Scripts\Activate.ps1
python run_langsmith_sync_worker.py
```

### Docker Compose

The compose stack now includes service `langsmith-sync-worker`.

```powershell
docker compose up -d langsmith-sync-worker
docker logs -f langsmith-sync-worker
```

This service periodically pulls LangSmith root runs and persists them into:

- `ai_obs_requests`
- `ai_obs_nodes`
- `ai_obs_sql_events`
- `ai_obs_node_scores`
- `ai_obs_prompt_scores`
- `ai_obs_sql_planner_events`

### Incremental checkpoint behavior

When `LANGSMITH_SYNC_USE_CHECKPOINT=1`, sync stores and reuses a watermark in table
`ai_obs_sync_state` and only imports runs newer than the last successful sync,
with a small safety overlap controlled by `LANGSMITH_SYNC_SAFETY_LOOKBACK_MINUTES`.
