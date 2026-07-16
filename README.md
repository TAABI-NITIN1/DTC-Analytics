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

Run a LangSmith experiment against a dataset named "first 100 questions testing":

`python evaluation/run_langsmith_evaluation.py --dataset-name "first 100 questions testing" --seed-from-local --limit 100`

Required for LangSmith runs:

- `LANGSMITH_API_KEY=...`
- `LANGSMITH_PROJECT=...` or `LANGCHAIN_PROJECT=...`

Run semantic LLM-judge scoring (costly but richer):

- `EVAL_USE_LLM_JUDGE=1`
- Optional: `EVAL_JUDGE_MODEL=gpt-4o-mini`
- `python evaluation/run_evaluation.py`

### Local-only conversational evaluation

The multi-turn conversational evaluation framework stores all generated eval artifacts locally.
It does **not** create ClickHouse evaluation tables and should not persist synthetic eval
traffic into existing ClickHouse observability tables.

Local-only dry run, without calling the AI model:

```powershell
$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_DRY_RUN="1"
python evaluation/conversational_runner.py --limit-scenarios 2
```

Real local-only run:

```powershell
$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_LOCAL_ONLY="1"
python evaluation/conversational_runner.py --limit-scenarios 3 --max-turns 2
```

Outputs are written to:

```text
evaluation/artifacts/<run_id>/
```

Each run includes local artifacts such as:

```text
run_manifest.json
scenario_catalog_snapshot.json
turns.jsonl
scenarios_rollup.jsonl
sql_events.jsonl
metrics.csv
tokens.csv
summary.json
benchmark_delta.json
optional_xlsx/benchmark_summary.xlsx
```

Important files:

- `docs/local_only_conversational_evaluation_plan.md`
- `evaluation/conversational_runner.py`
- `evaluation/local_store.py`
- `evaluation/conversational_scenarios/fleet_diagnostics_core.json`

LangSmith tracing can remain enabled for debugging; local artifacts store `request_id`
for trace correlation.

### Unified data collection (single-turn + multi-turn + Excel)

Run the full local data collection pipeline against the real backend API. This captures:

- question → answer pairs
- heuristic metrics (intent, tools, SQL, latency, tokens)
- trace judge scores from `chat().evaluation` (grounded in tool/SQL trace)
- batch LLM judge scores when enabled (`EVAL_USE_LLM_JUDGE=1`)
- LangSmith `request_id` for trace correlation

Start with the existing **60** single-turn questions plus the multi-turn scenario suite:

```powershell
cd c:\Users\Client\Downloads\DTC_Analytics_Local
.\myvenv\Scripts\Activate.ps1
pip install openpyxl

$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_USE_LLM_JUDGE="1"
$env:EVAL_STORE_FULL_ANSWER="1"
$env:EVAL_CUSTOMER_NAME="VRL LOGISTICS LIMITED"
$env:LANGSMITH_TRACING="true"

python evaluation/run_data_collection.py `
  --api-base-url http://127.0.0.1:8005 `
  --single-turn-limit 60 `
  --write-excel `
  --store-full-answer
```

Main deliverable:

```text
evaluation/artifacts/<run_id>/full_evaluation_report.xlsx
```

Excel sheets: `Summary`, `SingleTurn`, `MultiTurn`, `Aggregates`.

#### Scale to 1000 questions later

1. Create a JSON file with the same schema as `evaluation/fleet_questions.json` (array of objects with `id`, `question`, `context`, `expected_intent`, `expected_tools`, `reference_answer`).
2. Run with resume support if the job is interrupted:

```powershell
python evaluation/run_data_collection.py `
  --api-base-url http://127.0.0.1:8005 `
  --questions-file evaluation/questions_1000.json `
  --single-turn-limit 1000 `
  --resume `
  --write-excel `
  --store-full-answer
```

Or run single-turn bulk only:

```powershell
python evaluation/bulk_eval_runner.py `
  --api-base-url http://127.0.0.1:8005 `
  --questions-file evaluation/questions_1000.json `
  --limit 1000 `
  --resume `
  --write-excel `
  --store-full-answer
```

To reduce judge cost at 1000 scale, disable the batch judge and keep the trace judge:

```powershell
python evaluation/run_data_collection.py --no-batch-judge ...
```

Important files:

- `evaluation/run_data_collection.py` — orchestrator
- `evaluation/bulk_eval_runner.py` — single-turn API runner with checkpoint JSONL
- `evaluation/excel_export.py` — unified Excel writer
- `evaluation/conversational_runner.py` — multi-turn runner

### Phase 2: 1000 conversation sessions (dynamic follow-ups)

Phase 2 evaluates **1000 conversation sessions** (not individual turns) via the same
`POST /api/ai/chat` path as Phase 1. Session mix:

| Type | Count | Follow-ups |
|------|-------|------------|
| `single` | 650 | Seed only (1 turn) |
| `static_multi` | 250 | Pre-scripted turns 2–4 |
| `dynamic_multi` | 100 | Turn 1 seed; turns 2+ from runtime simulated fleet-user LLM |

#### Generate the session catalog

```powershell
cd c:\Users\Client\Downloads\DTC_Analytics_Local
$env:EVAL_CUSTOMER_NAME="VRL LOGISTICS LIMITED"

python evaluation/generate_session_catalog.py `
  --output evaluation/conversational_scenarios/sessions_1000.json `
  --single-count 650 --static-multi-count 250 --dynamic-multi-count 100 `
  --customer-name "VRL LOGISTICS LIMITED"
```

Outputs:

- `evaluation/conversational_scenarios/sessions_1000.json`
- `evaluation/conversational_scenarios/catalog_manifest.json`

#### Smoke test (50 sessions)

```powershell
$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_USE_LLM_JUDGE="1"
$env:EVAL_STORE_FULL_ANSWER="1"
$env:EVAL_CUSTOMER_NAME="VRL LOGISTICS LIMITED"
$env:LANGSMITH_TRACING="true"

python evaluation/run_1000_collection.py `
  --api-base-url http://127.0.0.1:8005 `
  --limit-sessions 50 `
  --write-excel `
  --store-full-answer
```

#### Full 1000-session run (resume-safe)

```powershell
python evaluation/run_1000_collection.py `
  --api-base-url http://127.0.0.1:8005 `
  --sessions-file evaluation/conversational_scenarios/sessions_1000.json `
  --write-excel `
  --store-full-answer `
  --resume `
  --use-batch-judge
```

#### Chunked resume (recommended for very long runs)

Each chunk processes up to `--limit-sessions` **sessions that are not yet in** `session_rollups.jsonl`
(stable when combined with `--resume`; see `evaluation/session_runner.py`). Intermediate chunks can skip Excel with `--no-write-excel`.

```powershell
# Hosted backend (your VM):
.\evaluation\run_phase2_chunks.ps1 `
  -ApiBaseUrl http://4.224.101.147:8005 `
  -RunId eval_20260520_085108_190cf6 `
  -ChunkSize 75
```

Local default (`-ApiBaseUrl` omitted): `http://127.0.0.1:8005`.

Equivalent manual loop:

```powershell
python evaluation/run_1000_collection.py --run-id eval_20260520_085108_190cf6 `
  --resume --limit-sessions 75 --store-full-answer --use-batch-judge --no-write-excel ...
# repeat until checkpoint has 1000 lines, then:
python evaluation/run_1000_collection.py --run-id eval_20260520_085108_190cf6 `
  --resume --write-excel --store-full-answer --use-batch-judge
```

To reduce judge cost at full scale, disable the batch judge and keep the trace judge:

```powershell
python evaluation/run_1000_collection.py --no-batch-judge --resume ...
```

Compare against a Phase 1 baseline run:

```powershell
python evaluation/run_1000_collection.py `
  --baseline-dir evaluation/artifacts/eval_20260520_065502_5e18bb `
  ...
```

#### Phase 2 environment variables

| Variable | Purpose | Default |
|----------|---------|---------|
| `EVAL_SESSIONS_FILE` | Path to session catalog JSON | `evaluation/conversational_scenarios/sessions_1000.json` |
| `EVAL_SIMULATOR_MODEL` | LLM for dynamic follow-ups | `gpt-4o-mini` |
| `EVAL_GENERATOR_MODEL` | Catalog LLM expansion (optional) | `template-only` |
| `EVAL_LIMIT_SESSIONS` | Cap sessions for smoke/partial runs | `0` (all) |
| `EVAL_GENERATE_CATALOG` | Force catalog regeneration | `0` |
| `EVAL_RESUME` | Skip sessions already in checkpoint | `0` |
| `EVAL_VRL_FRACTION` | Share of sessions for primary customer (VRL) | `0.7` |
| `EVAL_OTHER_CUSTOMERS` | Comma-separated other fleet customers (optional) | from ClickHouse |
| `EVAL_HEALTH_TIMEOUT_SEC` | Health check timeout (use 45+ for remote VM) | `30` |

#### Phase 2 deliverables

```text
evaluation/artifacts/<run_id>/
  session_turns.jsonl       # one row per turn (gates, dimensions, validation)
  session_rollups.jsonl     # one row per session (deduped on load)
  sql_events.jsonl          # sanitized SQL fingerprints per turn
  trace_events.jsonl        # compact LangGraph node trace per turn
  validation/findings.jsonl # MVP validator findings
  validation/scores.jsonl   # per-turn validation scores
  session_summary.json
  collection_summary.json
  analytics_summary.json    # run/session/trace KPIs (post-run)
  dashboard_bundle.json     # chart-ready export for eval-dashboard
  analytics_cache.parquet   # optional fast notebook load
  full_evaluation_report.xlsx
evaluation/artifacts/index.json   # run registry for experiment compare
```

Excel sheets: `Summary`, `Sessions`, `Turns`, `Aggregates`, plus `RunKPIs`, `Failures`, `TraceNodes`, `Experiments` when `analytics_summary.json` exists.

#### Evaluation analytics (operational intelligence)

After a benchmark finishes, `session_runner` / `run_1000_collection.py` write `analytics_summary.json` and append to `evaluation/artifacts/index.json`.

Summarize and export static dashboard bundles:

```powershell
python -m evaluation.analytics summarize --run-id eval_20260520_085108_190cf6
python -m evaluation.analytics export-dashboard --run-id eval_20260520_085108_190cf6 --baseline eval_20260520_065502_5e18bb --copy-to-eval-dashboard
python -m evaluation.analytics compare --baseline eval_20260520_065502_5e18bb --candidate eval_20260520_085108_190cf6
```

**Vite dashboard** (`eval-dashboard/`) — eight static views (Executive, Cost, Speed, Quality, Conversation, Tool/SQL, Failure, Experiment):

```powershell
cd eval-dashboard
npm install
npm run dev    # http://localhost:5174
```

See [docs/eval_analytics_blueprint.md](docs/eval_analytics_blueprint.md) for the full metric blueprint.

**Notebooks** (Plotly exploration):

```powershell
pip install jupyter plotly ipykernel
$env:EVAL_ANALYTICS_RUN_ID="eval_20260520_085108_190cf6"
$env:EVAL_BASELINE_RUN_ID="eval_20260520_065502_5e18bb"
jupyter notebook ai_fleet_evaluation_analytics.ipynb
```

Regenerate notebook from template: `python scripts/generate_unified_analytics_notebook.py`

Analytics package: `evaluation/analytics/` (`loader`, `compute`, `export_dashboard`, `pricing`, `failure_analytics`, `experiments`).

Important files:

- `evaluation/generate_session_catalog.py` — catalog generator
- `evaluation/simulated_user.py` — runtime dynamic follow-up LLM
- `evaluation/session_runner.py` — unified session API runner with checkpoint/resume + trace/SQL/validation artifacts
- `evaluation/run_phase2_chunks.ps1` — chunked resume helper
- `ai_fleet_evaluation_analytics.ipynb` — canonical analytics notebook
- `eval-dashboard/` — static Vite dashboard for leadership/ops views

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
