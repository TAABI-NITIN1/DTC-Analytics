# DTC Analytics v2: Full Flow Reference

This document explains the full analytics flow in detail:
- What each table does.
- Which columns are used.
- Which formulas are used.
- Which groupings/aggregations are used.
- How the pipeline behaves when new data arrives or source data changes.

It reflects the current implementation in:
- `analytics_pipeline_v2_sql.py`
- `clickhouse_utils_v2.py`
- `clickhouse_utils.py`
- `dtc_dag_runtime.py`
- `dtc_analytics_pipeline.py`

---

## 1) What the pipeline is trying to do

Goal:
- Convert raw DTC events into meaningful fault episodes.
- Score vehicle/fleet health.
- Build analytics tables for operations, trends, and maintenance.
- Run incrementally with CDC (change data capture) so only changed scope is recomputed.

Execution cadence:
- Airflow DAG schedule is currently set to run every minute (`schedule='*/1 * * * *'`) for temporary catch-up.
  - Normal steady-state schedule can be switched back to every 6 hours (`schedule='0 */6 * * *'`) when caught up.
- Task chain:
1. `cdc_precheck_status`
2. `run_full_pipeline`
3. `cdc_postcommit`
4. `cdc_validation_report`
5. `validate_outputs`

Nightly audit:
- Separate DAG: `dags_live/analytics/OBD/dtc_parity_audit_dag.py`
- Schedule: `0 2 * * *`
- Checks key parity and row-count drift across the derived tables.
- Fails fast if any audit metric is non-zero.

Current progress snapshot:
- Nightly parity audit is implemented and documented.
- `vehicle_health_summary_ravi_v2` parity was reconciled against VFM.
- `fleet_health_summary_ravi_v2` was corrected for client-level drift.
- Other derived tables checked cleanly against source-side aggregates.
- Remaining open work: source-side `customer_name` normalization where matching still depends on exact text.

---

## 2) Physical table map (V2 tables)

Table names used in ClickHouse:

1. `dtc_master_ravi_v2`
2. `vehicle_master_ravi_v2`
3. `dtc_events_exploded_ravi_v2`
4. `vehicle_fault_master_ravi_v2`
5. `fleet_health_summary_ravi_v2`
6. `fleet_dtc_distribution_ravi_v2`
7. `fleet_system_health_ravi_v2`
8. `fleet_fault_trends_ravi_v2`
9. `vehicle_health_summary_ravi_v2`
10. `dtc_fleet_impact_ravi_v2`
11. `maintenance_priority_ravi_v2`
12. `dtc_cooccurrence_ravi_v2`

Tenant key across analytics:
- `clientLoginId`

---

## 3) CDC and run-window logic

### 3.1 Run window selection

Main config knobs used:
- `analytics_window_days`
- `cdc_replay_hours`
- `cdc_bootstrap_days`
- `cdc_force_full_refresh`
- `cdc_enabled`
- `cdc_max_window_hours` (caps how large a single run can be)
- `cdc_max_source_rows_per_run` (caps how much raw source is processed per run)
- `cdc_min_process_start` (hard floor; ignore older data)

Window calculations (Airflow precheck → runtime):
- `run_window_end = now()`
- Read checkpoint from `cdc_checkpoint_ravi` (invalid checkpoints before 2000-01-01 UTC are ignored).
- If checkpoint exists: `run_window_start = checkpoint`
- Else (bootstrap):
  - Prefer `run_window_start = min(source_ts)` *on/after* `cdc_min_process_start` when available.
  - Otherwise fall back to: `run_window_start = now - max(cdc_bootstrap_days, analytics_window_days)`
- Apply floor: `run_window_start = max(run_window_start, cdc_min_process_start)`

Batching / safety caps (incremental mode only, applied during precheck):
- If `cdc_max_window_hours > 0`:
  - `run_window_end = min(run_window_end, run_window_start + cdc_max_window_hours)`
- If `cdc_max_source_rows_per_run > 0`:
  - Estimate source rows for `[run_window_start, run_window_end]` and repeatedly halve `run_window_end`
    until the estimate fits the cap (minimum 15 minutes window).

How `since_ts` / `until_ts` are used to pull DTC events:
- `until_ts = run_window_end`
- In incremental mode: `incremental_start_ts = run_window_start - cdc_replay_hours`
  - When `cdc_context` is provided (normal DAG flow), `since_ts = incremental_start_ts`.
  - Otherwise: `since_ts = max(now - analytics_window_days, incremental_start_ts)`.
- `since_ts` is always floored to `>= (cdc_min_process_start - 1s)`.

Why replay overlap exists:
- The replay overlap protects against late-arriving records near window boundaries and keeps episode continuity at window edges.

### 3.2 CDC metadata tables

CDC metadata tables:
- `cdc_checkpoint_ravi`
- `cdc_run_log_ravi`
- `cdc_source_watermark_ravi`
- `cdc_impacted_keys_ravi`

Current latest-value retrieval strategy:
- `argMax(..., update_ts)` is used for deterministic latest-row reads.

---

## 4) Source systems and watched change columns

Source profiles are introspected dynamically and then used for impacted-key detection.

Profiles (see `clickhouse_utils.CDC_SOURCE_PROFILES` for the full ordered lists):

1. `vehicle_profile`
- Table env/default: `VEHICLE_PROFILE_TABLE` / `aimm.vehicle_profile`
- Updated candidates (checked in order):
  - `updated_at`, `updatedAt`, `updatedAt_entity`, `updatedAt_client`, `updated_at_source`, `updated_ts`, `updated_ts_ms`, `last_updated_at`
- Created candidates:
  - `created_at`, `createdAt`, `createdAt_entity`, `createdAt_client`, `created_at_source`, `created_ts`, `created_ts_ms`

2. `dtc_codes`
- Table env/default: `DTC_CODES_TABLE` / `dtc_codes_updated`
- Updated candidates:
  - `__lastupdatetime`, `updatedAt`, `updated_at`, `updated_at_source`, `updated_ts`, `updated_ts_ms`
- Created candidates:
  - `__lastcreatedtime`, `createdAt`, `created_at`, `created_at_source`, `created_ts`, `created_ts_ms`

3. `engine_cycles`
- Table env/default: `ENGINE_CYCLES_TABLE` / `aimm.engineoncycles`
- Updated candidates:
  - `updatedAt`, `updated_at`, `updated_at_source`, `cycle_end_ts`, `ts`, `ts_ms`
- Created candidates:
  - `createdAt`, `created_at`, `created_at_source`, `cycle_start_ts`, `ts`, `ts_ms`

4. `dtc_history`
- Table env/default: `DTC_HISTORY_TABLE` / `aimm.dtc_data_history`
- Updated candidates:
  - `updatedAt`, `updated_at`, `updated_at_source`, `updated_ts`, `updated_ts_ms`
- Created candidates:
  - `createdAt`, `created_at`, `created_at_source`, `created_ts`, `created_ts_ms`

Normalization note:
- For impacted-key detection, numeric timestamp columns (epoch seconds or epoch millis) are normalized to UTC before applying the `(window_start, window_end)` predicate.

Impacted-key extraction rules:
- Impacted uniqueids from `dtc_history` window in exploded table (`ts > since_ts AND ts <= until_ts`).
- Impacted uniqueids from changed `vehicle_profile` rows.
- Impacted uniqueids from changed `engine_cycles` rows.
- Impacted dtc_codes from changed `dtc_codes` rows.
- Impacted `(uniqueid, dtc_code)` pairs from exploded table by impacted predicates.
- Impacted clients resolved by:
  - uniqueids -> `vehicle_master`
  - dtc_codes -> `vehicle_fault_master`

---

## 5) Stage-by-stage flow and formulas

## 5.1 Dimension load

### A) `dtc_master_ravi_v2`

Purpose:
- Canonical DTC code attributes and severity metadata.

Source:
- `dtc_codes_updated` (or env override).

Key transform rules:
- Normalize aliases:
  - `code`, `dtc`, `dtcCode` -> `dtc_code`
  - `severity`, `severityLevel` -> `severity_level`
  - `desc` -> `description`
- Default missing values.
- Keep first row per `dtc_code`.

Current population scope:
- The loader currently writes only:
  - `dtc_code`, `system`, `subsystem`, `description`, `severity_level`
  - plus optional source timestamp fields when present.
- Extended DDL fields (for example `driver_related`, `action_required`, `primary_cause`, `symptoms`) remain defaults unless additional mapping logic is added.

Behavior:
- Table is fully truncated and reloaded each pipeline run.

### B) `vehicle_master_ravi_v2`

Purpose:
- Canonical OBD vehicle roster with tenant mapping.

Source:
- Primary: `aimm.vehicle_profile` (or env override).
- Fallback: distinct `uniqueid` from `aimm.dtc_data_history`.

Important filter:
- `solutionType IN ('obd_solution', 'obd_analog_solution', 'obd_fuel+fuel_solution')`

Column mapping is dynamic using first available source column names.

Behavior:
- Table is fully truncated and reloaded each run.

---

## 5.2 Processing table: exploded events

### `dtc_events_exploded_ravi_v2`

Purpose:
- One row per `(clientLoginId, uniqueid, ts, dtc_code)` event.

Two source paths:
1. Pre-exploded source path (`DTC_EXPLODED_SOURCE_TABLE`) using `ts_dt`.
2. Raw history path (`aimm.dtc_data_history`) using `ARRAY JOIN e.dtc_code`.

Shared rules:
- Join with `vehicle_master` on `uniqueid`.
- Left join with `dtc_master` on `dtc_code`.
- Drop sentinel code `'0'`.
- Keep source audit columns where available.

Incremental write behavior:
- Delete current window range from exploded table:
  - `DELETE WHERE ts > since_ts AND ts <= until_ts`
- Insert recalculated rows for same window.

Full refresh behavior:
- Truncate and reinsert for full analytics window.

---

## 5.3 Episode detection (core logic)

Temporary tables used:
- `_tmp_engine_ts_ravi`
- `_tmp_events_lag_ravi`
- `_tmp_events_break_ravi`
- `_tmp_events_episode_ravi`
- `_tmp_episode_agg_ravi`
- `_tmp_episode_resolved_ravi`
- `_tmp_vehicle_risk_ravi`

### A) Engine boundary table

From engine cycles, compute:
- `cycle_end_u32`
- `boundary_eligible`

Boundary eligibility rule:
- `boundary_eligible = 1` if cycle duration >= `ENGINE_BOUNDARY_MIN_CYCLE_SECONDS` (default 1800 sec).

### B) Lag events

For each `(uniqueid, dtc_code)` ordered by `ts`:
- `prev_ts = lag(ts, 1, 0)`

### C) Break detection

`is_break` rules:
1. If `prev_ts = 0` then break.
2. Else if there exists an eligible engine cycle with:
- `cycle_end_u32 > prev_ts`
- `cycle_end_u32 <= ts`
then break.
3. Else not a break.

Note:
- Current production logic is engine-boundary only.
- `EPISODE_GAP_SECONDS` is configured but not currently applied in SQL split logic.

### D) Episode id assignment

For each `(uniqueid, dtc_code)`:
- `episode_id = cumulative sum(is_break)`

### E) Episode aggregation

Group by `(uniqueid, dtc_code, episode_id)`:
- `first_ts = min(ts)`
- `last_ts = max(ts)`
- `occurrence_count = count()`

### F) Resolution and episode-level metrics

`resolution_time_sec` formula:
- `max(0, last_ts - first_ts)`

`is_resolved` formula:
- `1` if both true:
  - `last_ts <= cutoff_ts`
  - `max_engine_cycle_ts_for_vehicle > last_ts`
- else `0`

Where:
- `cutoff_ts = global_max_event_ts - FINAL_DAY_CUTOFF_SECONDS`

`gap_from_previous_episode`:
- lag of prior episode end for same `(uniqueid, dtc_code)`
- `max(0, first_ts - prev_last_ts)`

---

## 5.4 Vehicle risk and operational master

### A) `_tmp_vehicle_risk_ravi`

Risk is computed only from unresolved episodes.

Severity multiplier:
- severity <= 1 -> 1.0
- severity = 2 -> 3.0
- severity = 3 -> 7.0
- severity >= 4 -> 12.0

Duration factor:
- `1 + min(resolution_time_sec / 86400, 30) / 10`

Recurrence factor:
- `1 + max(ep_count - 1, 0) * 0.5`
- `ep_count` is number of episodes for same `(uniqueid, dtc_code)`.

Vehicle risk formula:
- Sum of: `severity_multiplier * duration_factor * recurrence_factor`
  over unresolved episodes of that vehicle.

### B) `vehicle_fault_master_ravi_v2`

Grain:
- One row per episode with id:
- `episode_id = uniqueid + '_' + dtc_code + '_' + episode_id_number`

Key joins:
- Episode table + `vehicle_master` + `dtc_master` + vehicle risk + engine-cycle counts within episode interval.

Main derived fields:
- `event_date = toDate(toDateTime(first_ts, 'Asia/Kolkata'))` (IST day boundary)
- `event_date_ist = toDate(toDateTime(first_ts, 'Asia/Kolkata'))` (same as `event_date`, kept for compatibility)
- `engine_cycles_during = count(engine cycle ends within [first_ts, last_ts])`
- Issue flags from text matching in `system` and `subsystem`:
  - engine, coolant, safety, emission, electrical

Vehicle health score formula:
- `vehicle_health_score = max(0, 100 - min(risk * HEALTH_SCALING_CONSTANT, 100))`

Default scaling constant:
- `HEALTH_SCALING_CONSTANT = 0.2`

---

## 5.5 Derived analytics tables

All are built from `vehicle_fault_master_ravi_v2`.

### 1) `fleet_health_summary_ravi_v2`

Grain:
- `clientLoginId`

Aggregations:
- `total_vehicles`: count from `vehicle_master`
- `active_veh`: `uniqExactIf(uniqueid, is_resolved = 0)`
- `critical_veh`: `uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3)`
- `driver_faults`: `countIf(driver_related = 1)`
- `risk_sum`: sum of unresolved risk terms
- `most_common_dtc`: `topKWeighted(1)(dtc_code, occurrence_count)`
- `most_common_system`: `topKWeighted(1)(system, occurrence_count)`

Fleet health formula:
- If active vehicles > 0:
  - `100 - min((risk_sum / active_veh) * scaling_constant, 100)`
- Else `100`

Trend formula:
- Compare unresolved count in recent 7 days vs prior 7 days:
  - increasing / decreasing / stable

### 2) `fleet_dtc_distribution_ravi_v2`

Grain:
- `(clientLoginId, dtc_code)`

Aggregations:
- `vehicles_affected = uniqExact(uniqueid)`
- `active_vehicles = uniqExactIf(uniqueid, is_resolved=0)`
- `total_occurrences = sum(occurrence_count)`
- `total_episodes = count()`
- `avg_resolution_time = sumIf(resolution_time_sec>0)/countIf(resolution_time_sec>0)`
- `driver_related_count = countIf(driver_related=1)`

### 3) `fleet_system_health_ravi_v2`

Grain:
- `(clientLoginId, system)` with empty/nan normalized to `other`

Aggregations:
- `vehicles_affected = uniqExactIf(uniqueid, is_resolved=0)`
- `active_faults = countIf(is_resolved=0)`
- `critical_faults = countIf(is_resolved=0 AND severity_level>=3)`
- `risk_score = sumIf(severity_multiplier, is_resolved=0)`
- `trend` currently fixed to `stable`

### 4) `fleet_fault_trends_ravi_v2`

Grain:
- `(clientLoginId, date)` (IST day boundary)

Aggregations:
- `active_faults = countIf(is_resolved=0)`
- `critical_faults = countIf(is_resolved=0 AND severity_level>=3)`
- `new_faults = count()`
- `resolved_faults = countIf(is_resolved=1)`
- `driver_related_faults = countIf(driver_related=1)`

Daily fleet health formula:
- `100 - min((sum unresolved severity multipliers / unresolved unique vehicles) * scaling_constant, 100)`

### 5) `vehicle_health_summary_ravi_v2`

Grain:
- `(clientLoginId, uniqueid)`

Design detail:
- Final SELECT is from `vehicle_master` left-joined to stats.
- So every known vehicle gets a row, even with no fault history.

Aggregations:
- active/critical fault counts
- total episodes
- episodes in last 30 days
- average resolution time
- last fault timestamp
- most common dtc
- driver-related count
- issue flags (engine/emission/safety/electrical)

Health formula:
- Same risk model then scaled to 0..100.

### 6) `dtc_fleet_impact_ravi_v2`

Grain:
- `(clientLoginId, dtc_code)`

Aggregations:
- `vehicles_affected = uniqExact(uniqueid)`
- `active_vehicles = uniqExactIf(uniqueid, is_resolved=0)`
- `avg_resolution_time`
- `driver_related_ratio = countIf(driver_related=1)/count()`
- `fleet_risk_score = sumIf(severity_multiplier, is_resolved=0)`

### 7) `maintenance_priority_ravi_v2`

Grain:
- Active episode rows (not grouped)

Filters:
- `WHERE is_resolved = 0`

Helper CTE:
- `eps_30`: episode count per `(uniqueid, dtc_code)` in last 30 days.

Priority score formula:
- `severity_multiplier * (1 + min(resolution_time_sec/86400,30)/10) * (1 + eps_30)`

Recommended action:
- from `dtc_master.action_required`

### 8) `dtc_cooccurrence_ravi_v2`

Grain (current, tenant-safe):
- `(clientLoginId, dtc_code_a, dtc_code_b)`

How it is built:
1. For each vehicle within each client, collect distinct dtc set.
2. Generate all ordered code pairs from that set.
3. Aggregate pairs at client level.

Metrics:
- `cooccurrence_count = sum(total_episodes of contributing vehicles)`
- `vehicles_affected = count(contributing vehicles)`
- `last_seen_ts = max(vehicle max last_ts)`
- `avg_time_gap_sec` currently constant `0`.

Important semantic note:
- Current cooccurrence means co-presence in vehicle history, not necessarily same time window/episode overlap.

---

## 6) Column-to-aggregation quick map

This section answers: which columns are used for what aggregation.

### `fleet_health_summary`
- Group by: `clientLoginId`
- Uses: `uniqueid`, `is_resolved`, `severity_level`, `resolution_time_sec`, `occurrence_count`, `dtc_code`, `system`, `driver_related`, `event_date`

### `fleet_dtc_distribution`
- Group by: `clientLoginId`, `dtc_code`
- Uses: `uniqueid`, `is_resolved`, `occurrence_count`, `resolution_time_sec`, `driver_related`, `description`, `system`, `subsystem`, `severity_level`

### `fleet_system_health`
- Group by: `clientLoginId`, normalized `system`
- Uses: `uniqueid`, `is_resolved`, `severity_level`, `system`

### `fleet_fault_trends`
- Group by: `clientLoginId`, `event_date`
- Uses: `is_resolved`, `severity_level`, `uniqueid`, `driver_related`

### `vehicle_health_summary`
- Group by in stats CTE: `clientLoginId`, `uniqueid`
- Uses: `is_resolved`, `severity_level`, `first_ts`, `last_ts`, `resolution_time_sec`, `occurrence_count`, issue flags, `driver_related`, `dtc_code`

### `dtc_fleet_impact`
- Group by: `clientLoginId`, `dtc_code`
- Uses: `uniqueid`, `is_resolved`, `resolution_time_sec`, `driver_related`, `severity_level`, `system`, `subsystem`

### `maintenance_priority`
- No final group-by, row-level for unresolved records
- Uses: `severity_level`, `resolution_time_sec`, `dtc_code`, `uniqueid`, `is_resolved`, plus `eps_30` from last-30-days grouped counts

### `dtc_cooccurrence`
- Group by: `clientLoginId`, `dtc_code_a`, `dtc_code_b`
- Uses: `clientLoginId`, `uniqueid`, `dtc_code`, `last_ts`

---

## 7) How pipeline behaves when new data comes or source changes

## 7.1 New `dtc_history` event rows arrive

What happens:
1. New rows fall into `since_ts..until_ts` window.
2. Matching window in exploded table is deleted and rebuilt.
3. New impacted uniqueids are captured.
4. `vehicle_fault_master` rows for impacted uniqueids/dtc_codes are deleted and recalculated.
5. Derived tables for impacted clients are deleted and recalculated.
6. Cooccurrence for impacted clients is deleted and recalculated.

Result:
- Only impacted scope is recomputed, not the whole tenant universe.

## 7.2 Late events arrive near boundary

Because replay overlap exists (`cdc_replay_hours`):
- Previous boundary period is recalculated again.
- This prevents missed episodes around the cut line.

## 7.3 `vehicle_profile` changes (metadata update)

Examples:
- vehicle number changed
- client mapping changed
- model/year updates

What happens:
- Changed rows are detected by created/updated timestamp candidates.
- Impacted uniqueids are marked.
- Their `vehicle_fault_master` rows are rebuilt.
- All derived tables for impacted clients are rebuilt.

## 7.4 `engine_cycles` changes

What happens:
- Impacted uniqueids are detected.
- Episode boundaries may change.
- Resolution status may change.
- Vehicle risk and all downstream metrics can change.

## 7.5 `dtc_codes` changes

What changes:
- Severity, system/subsystem, description, action_required, driver-related tag.

What happens:
- Impacted dtc codes are detected.
- `vehicle_fault_master` rows for those dtc codes are rebuilt.
- Derived tables for impacted clients are rebuilt.

## 7.6 No source changes in a run

What happens:
- Pipeline can enter no-op incremental path.
- It records `no_op_incremental = true` and skipped table list.

## 7.7 Full refresh mode

When `cdc_force_full_refresh = true`:
- Key analytics tables are truncated and rebuilt for full analytics window.
- Useful for recovery/backfill consistency.

## 7.8 CDC disabled

When `cdc_enabled = false`:
- CDC metadata updates are skipped.
- Pipeline still computes analytics but without checkpoint lifecycle behavior.

---

## 8) Delete/insert behavior by stage (incremental mode)

1. Exploded events:
- Delete by time window and reinsert window.

2. Vehicle fault master:
- Delete where `uniqueid IN impacted_uniqueids` OR `dtc_code IN impacted_dtc_codes`.
- Reinsert only impacted scope.

3. Derived analytics tables:
- For each derived table, delete by `clientLoginId IN impacted_client_ids`, then reinsert for those clients.

4. Cooccurrence:
- Delete by impacted clients, then rebuild for those clients.

Why this works:
- Keeps table size manageable and avoids full rebuild each run.
- Recomputed scope is aligned to where source changes occurred.

Operational note on ClickHouse mutations:
- `ALTER TABLE ... DELETE` on MergeTree tables is mutation-based and asynchronous.
- So delete-then-insert is not an atomic transaction boundary.
- During mutation lag, temporary overlap or stale reads are possible until background mutation completion.

---

## 9) Validation and operational safeguards

Validation output includes:
- Source change counts by source table.
- Impacted key counts.
- Before/after table counts.
- Unaffected-row drift checks (full validation mode).
- Idempotency signature:
  - vfm count
  - unique episode ids
  - last update timestamp
  - exploded event-key uniqueness in window

Runtime hardening now present:
- Deterministic latest CDC reads using `argMax`.
- Tenant-safe cooccurrence grouping.
- Environment-based credentials.

---

## 10) Important implementation notes

1. Episode splitting policy:
- Currently engine-boundary only.
- Config value `episode_gap_seconds` exists but is not currently applied in SQL splitting.

2. Cooccurrence semantics:
- Pair means co-presence in vehicle history under a tenant.
- `avg_time_gap_sec` currently fixed to `0`.

3. Health score scale:
- Score is clamped 0..100 via scaling constant.
- Increasing unresolved severity/duration/recurrence lowers score.

4. Vehicle coverage:
- `vehicle_health_summary` keeps all vehicles from vehicle master, including vehicles with zero faults (score defaults high, counts zero).

5. Fleet health score variants:
- `fleet_health_summary` and `fleet_fault_trends` both output fleet health scores, but their numerators are intentionally different.
- Summary table uses the richer unresolved risk term (severity * duration * recurrence).
- Trend table uses unresolved severity-weighted daily aggregation.

6. `fleet_system_health` trend column:
- Trend is currently constant `'stable'` in SQL (placeholder, not a computed trend algorithm yet).

7. Maintenance priority vs vehicle risk recurrence:
- Vehicle risk recurrence uses total episode history factor (`ep_count`).
- Maintenance priority recurrence uses last-30-days episode count (`eps_30`).
- This is intentional, so the two scores should not be expected to match one-to-one.

8. CDC safety floors / batching:
- `cdc_min_process_start` (default `2025-01-01T00:00:00+00:00`) is a hard floor; events earlier than this are ignored.
- Precheck caps the run window using:
  - `cdc_max_window_hours`
  - `cdc_max_source_rows_per_run` (row-estimation + halving window end, minimum 15 min window)
- Invalid checkpoints before `2000-01-01T00:00:00+00:00` are ignored (treated as no checkpoint).

---

## 11) Formula sheet (quick copy)

Severity multiplier:
- `sev<=1 -> 1`
- `sev=2 -> 3`
- `sev=3 -> 7`
- `sev>=4 -> 12`

Duration factor:
- `1 + min(resolution_time_sec / 86400, 30) / 10`

Recurrence factor:
- `1 + max(ep_count - 1, 0) * 0.5`

Vehicle risk:
- Sum over unresolved episodes:
- `severity_multiplier * duration_factor * recurrence_factor`

Vehicle health score:
- `max(0, 100 - min(risk * scaling_constant, 100))`

Maintenance priority score:
- `severity_multiplier * duration_factor * (1 + eps_30)`

Fleet health score (summary/trends variants):
- Both are 0..100 clamped inverse scores using the same scaling constant.
- Summary formula uses unresolved risk terms with duration + recurrence factors.
- Trends formula uses unresolved severity-multiplier aggregation per date.

---

## 12) End-to-end summary in one line

Raw DTC events are exploded, split into engine-boundary episodes, resolved/unresolved status is inferred from post-fault engine activity, risk is computed from severity-duration-recurrence, and tenant-level/vehicle-level analytics tables are incrementally rebuilt only for impacted keys/clients each run.

---

## 13) Reconciliation status (current code)

Items implemented in current code:
- Deterministic latest CDC reads use `argMax(..., update_ts)` for checkpoint/source watermark retrieval paths.
- Cooccurrence grouping is tenant-safe at `(clientLoginId, dtc_code_a, dtc_code_b)`.
- `_collect_source_watermarks_v2` helper exists and is callable in post-run path.
- `ensure_v2_tables` now checks existing columns before issuing audit-column `ALTER` statements.

Still-open semantic limitations:
- Episode splitting is engine-boundary only (configured `episode_gap_seconds` is not applied in split SQL).
- Cooccurrence `avg_time_gap_sec` is currently a placeholder (`0`) and not yet computed from temporal proximity.
