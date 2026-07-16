# DTC CDC Pipeline Runbook (Layman Guide)

## Final Status (Current)
- Import-related failures are fixed.
- `cdc_precheck_status` is running successfully.
- DAG is intentionally thin and clean.
- Heavy logic is centralized in include-side runtime/helpers.
- No virtualenv creation or requirements installation is done inside the DAG.
- Nightly parity audit DAG has been added and documented.
- Derived-table parity was audited; `vehicle_health_summary` and `fleet_health_summary` were corrected in place.
- Remaining open item: normalize `customer_name` consistently in source/derived matching paths.

## Why Errors Were Coming

### 1) Module import path issue
- Airflow worker process was executing in a context where plain sibling imports were not always resolvable.
- Result: `ModuleNotFoundError: analytics_pipeline_v2_sql`.

### 2) Missing optional dependency issue
- After import path was fixed, the next failure came from module loading.
- `clickhouse_utils.py` imported `clickhouse_driver` at top-level.
- In worker image, `clickhouse_driver` was not installed.
- Result: `ModuleNotFoundError: clickhouse_driver`.

## What We Changed

### A) Runtime import hardening
In `dtc_dag_runtime.py`:
- Added robust symbol import helpers that try package-qualified imports first, then fallback.
- Added include-folder bootstrap into `sys.path` at startup.

Impact:
- Runtime can resolve helper modules consistently in Airflow task processes.

### B) Optional ClickHouse driver handling
In `clickhouse_utils.py`:
- Made `clickhouse_driver` optional at import time.
- Kept HTTP client path (`clickhouse_connect`, usually port `8123`) as default working path.
- Added clear runtime error only if a native port is requested without `clickhouse_driver` installed.

Impact:
- Module import no longer crashes during precheck.

## Complete Working Flow (End-to-End)

Think of it as a 5-step assembly line:

1. **`cdc_precheck_status`**
- Reads last checkpoint and source watermarks.
- Builds current run window: `checkpoint -> now`.
- Captures pre-run table counts and context.
- Passes this context to next task.

2. **`run_full_pipeline`**
- Runs SQL-first analytics pipeline.
- If incremental mode: recomputes only impacted scope.
- If full mode: recomputes everything.
- Writes to v2 base + derived tables.

3. **`cdc_postcommit`**
- On success: advances checkpoint to run end and updates source watermarks.
- Writes success in CDC run log.
- On failure: logs failed status and does not advance checkpoint.

4. **`cdc_validation_report`**
- Produces compact CDC report:
  - run mode,
  - impacted counts,
  - source change counts,
  - drift checks,
  - idempotency signature,
  - skipped/no-op markers.

5. **`validate_outputs`**
- Verifies critical output tables are non-empty.
- Fails run if critical outputs are empty.

## How One Table Change Reflects in Others

## Mental model
- Upstream source change -> impacted keys -> base table refresh -> derived table refresh.
- Only related records should refresh in incremental mode.

### Example 1: New DTC event for one vehicle
Suppose vehicle `MH12AB1234` gets new code `P0300` at 10:30.

What happens next run:
- Precheck detects window from old checkpoint to now.
- Event is detected in changed data.
- Impacted scope includes this vehicle/code/client.
- System updates base operational rows first, then analytics rows that depend on them.
- Unrelated vehicles should not be recomputed in incremental mode.

### Example 2: DTC code metadata changes (severity update)
Suppose severity of `P0300` is changed in DTC master source.

What happens next run:
- Source profile detects code-table change.
- Impacted code list is generated.
- Rows tied to that code are recomputed.
- Fleet severity/priority analytics update accordingly.

## CDC Safety Guarantees
- **Checkpoint safety:** checkpoint moves only after successful commit.
- **Replay protection:** failed runs are retried without losing uncommitted window.
- **Incremental correctness:** only impacted scope is recomputed where possible.
- **Validation guardrail:** empty critical outputs fail fast.

## Main Files and Their Role
- `dags_live/analytics/OBD/dtc_analytics_pipeline.py`
  - Thin orchestration DAG only.
- `dags_live/analytics/OBD/dtc_parity_audit_dag.py`
  - Nightly parity audit DAG.
  - Reuses the v2 runtime to check for extra/missing keys and table drift.
- `includes/OBD/DTC_Analytics_Ravi/dtc_dag_runtime.py`
  - Runtime entrypoint and task-level execution wrappers.
- `includes/OBD/DTC_Analytics_Ravi/analytics_pipeline_v2_sql.py`
  - Core SQL-first CDC + analytics orchestration.
- `includes/OBD/DTC_Analytics_Ravi/clickhouse_utils.py`
  - ClickHouse connectivity and CDC metadata utilities.
- `includes/OBD/DTC_Analytics_Ravi/clickhouse_utils_v2.py`
  - v2 table definitions and related helper operations.

## What To Check After Every Run
- `cdc_precheck_status` succeeded and emitted context.
- `run_full_pipeline` completed with expected counts.
- `cdc_postcommit` updated checkpoint/watermarks.
- `cdc_validation_report` has sane drift/idempotency signals.
- `validate_outputs` confirms critical tables are non-empty.
- Nightly parity audit has no drift failures.

## Nightly Parity Audit
- DAG: `dags_live/analytics/OBD/dtc_parity_audit_dag.py`
- Schedule: `0 2 * * *`
- Purpose: detect drift between VFM, VHS, fleet summary tables, and DTC impact tables.
- Fails the run if any of these checks are non-zero:
  - extra or missing `vehicle_health_summary` keys
  - missing `fleet_health_summary` clients or total-vehicle drift
  - key drift in `fleet_dtc_distribution`
  - key drift in `dtc_fleet_impact`
  - key drift in `fleet_system_health`

## Quick Troubleshooting Guide
- If import error appears:
  - Verify runtime import helpers and `sys.path` bootstrap in `dtc_dag_runtime.py`.
- If ClickHouse driver error appears:
  - Use HTTP port (`8123`) path or install `clickhouse_driver` only if native port mode is required.
- If nightly parity audit fails:
  - Read the `drift_failures` payload in task logs.
  - Re-run the targeted aggregate query for the affected table and client.
- If outputs are empty:
  - Check source window, impacted scope logic, and upstream source availability.

## Episode Logic (Layman)

Current behavior is **engine-boundary only** for splitting episodes.

### What is an episode?
- An episode means one continuous stretch of the same DTC fault for the same vehicle.

### When does a new episode start?
1. First-ever event for that vehicle + DTC combination.
2. Or when an eligible engine cycle boundary is found between two fault events.

### What is an eligible engine boundary?
- Engine cycle is completed.
- Engine cycle duration is at least 30 minutes by default (`ENGINE_BOUNDARY_MIN_CYCLE_SECONDS=1800`).

### What does not split episodes anymore?
- Time gap alone does not split episodes now.
- Even if events are far apart, if no eligible engine boundary exists between them, they stay in the same episode.

### Easy example
Suppose for vehicle `MH12AB1234` and code `P0300`, event timestamps are:
- 10:00
- 10:10
- 11:30

Engine cycles in between:
- cycle end at 10:40, duration 20 min
- cycle end at 11:00, duration 35 min

Result:
- 20-min cycle is ignored (< 30 min)
- 35-min cycle is eligible
- event at 11:30 starts a **new episode**

So grouping becomes:
- Episode 1: early events before eligible boundary
- Episode 2: events after eligible boundary
