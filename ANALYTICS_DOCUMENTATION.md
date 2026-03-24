# DTC Analytics V2 Pipeline - Complete Documentation

**Date:** March 24, 2026  
**System:** DTC-Analytics V2 (SQL-First Approach)  
**Database:** ClickHouse  
**Purpose:** Comprehensive diagnostic and fleet health analytics pipeline

---

## Table of Contents

1. [Pipeline Architecture Overview](#1-pipeline-architecture-overview)
2. [Data Sources & Configuration](#2-data-sources--configuration)
3. [Dimension Tables](#3-dimension-tables)
4. [Processing Pipeline](#4-processing-pipeline)
5. [Episode Detection Logic](#5-episode-detection-logic)
6. [Vehicle Fault Master (Operational Master)](#6-vehicle-fault-master-operational-master)
7. [Analytics Tables (Level 1)](#7-analytics-tables-level-1)
8. [Formulas & Calculations](#8-formulas--calculations)
9. [Data Flow Diagram](#9-data-flow-diagram)
10. [Configuration Parameters](#10-configuration-parameters)

---

## 1. Pipeline Architecture Overview

### System Design

The DTC Analytics V2 pipeline follows a **SQL-first approach** where all heavy lifting is done inside ClickHouse. Python serves as an orchestrator that:
- Executes SQL statements
- Manages temp tables
- Logs progress
- Handles CDC (Change Data Capture) bookkeeping

### Processing Steps (High-Level)

```
Source Data (DTC History + Engine Cycles)
        ↓
   ↓─────────────────────────────────────────────────────────────────────────┐
   │                    Dimension Tables                                     │
   │                   (DTC Master, Vehicle Master)                          │
   └─────────────────────────────────────────────────────────────────────────┘
        ↓
   DTC Events Exploded (Raw events with vehicle context)
        ↓
   ↓─────────────────────────────────────────────────────────────────────────┐
   │                   Episode Detection Pipeline                             │
   │  (Lagging → Breaks → Episode IDs → Aggregation → Resolution Flagging)  │
   └─────────────────────────────────────────────────────────────────────────┘
        ↓
   Vehicle Fault Master (Central Fact Table)
        ↓
   ↓─────────────────────────────────────────────────────────────────────────┐
   │                  8 Analytics Derived Tables                             │
   │   (Fleet Health, DTC Distribution, System Health, Trends, etc.)        │
   └─────────────────────────────────────────────────────────────────────────┘
```

### Key Principle: Episode

**Definition:** An episode is a logical grouping of multiple DTC occurrences for the same vehicle and DTC code, separated by either:
1. **Time gap** - Default 1800 seconds (30 minutes)
2. **Engine cycle boundary** - Engine restart detected in engine cycle data

This prevents artifacts where the same fault might be recorded as multiple events.

---

## 2. Data Sources & Configuration

### Source Tables

| Source | Default Table | Environment Variable | Purpose |
|--------|---------------|----------------------|---------|
| **DTC History** | `aimm.dtc_data_history` | `DTC_HISTORY_TABLE` | Raw fault diagnostic codes with timestamps |
| **Engine Cycles** | `aimm.engineoncycles` | `ENGINE_CYCLES_TABLE` | Engine on/off cycle boundaries for episode detection |

### Tenant & Vehicle Filtering

- **Tenant Key:** `clientLoginId` (from vehicle_profile_ss)
- **Vehicle Filter:** Only vehicles with `solutionType` IN (`'obd_solution'`, `'obd_analog_solution'`, `'obd_fuel+fuel_solution'`)

### Key Configuration Parameters

| Parameter | Default | Environment Variable | Impact |
|-----------|---------|----------------------|--------|
| `episode_gap_seconds` | 1800 | `EPISODE_GAP_SECONDS` | Gap threshold between DTC events to mark episode boundary |
| `final_day_cutoff_seconds` | 86400 | `FINAL_DAY_CUTOFF_SECONDS` | Time window for marking episodes as "resolved" |
| `analytics_window_days` | 90 | `ANALYTICS_WINDOW_DAYS` | Historical data window for analysis |
| `cdc_bootstrap_days` | 90 | `CDC_BOOTSTRAP_DAYS` | Initial catchup window for CDC |
| `cdc_enabled` | true | `CDC_ENABLED` | Enable/disable incremental processing |
| `health_scaling_constant` | 0.2 | `HEALTH_SCALING_CONSTANT` | Risk multiplier for health score calculation |

---

## 3. Dimension Tables

Dimension tables contain reference data and are maintained via Python (lightweight operations).

### 3.1 DTC Master Table

**Table Name:** `dtc_master_ravi_v2`  
**Purpose:** Reference table for all DTC codes with metadata, severity, and remediation guidance

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS dtc_master_ravi_v2 (
    dtc_code              String,                -- Primary key (e.g., "P0128")
    system                String        DEFAULT '', -- System category (Engine, Transmission, Safety)
    subsystem             String        DEFAULT '', -- Subsystem (e.g., Powertrain, Coolant)
    description           String        DEFAULT '', -- Human-readable description
    primary_cause         String        DEFAULT '', -- Main root cause
    secondary_causes      String        DEFAULT '', -- Alternative causes
    symptoms              String        DEFAULT '', -- Vehicle symptoms user might observe
    impact_if_unresolved  String        DEFAULT '', -- Consequences of not fixing
    fuel_mileage_impact   String        DEFAULT '', -- Effect on fuel economy (if any)
    vehicle_health_impact String        DEFAULT '', -- Impact on vehicle health
    severity_level        Int8          DEFAULT 1,  -- 1=Info, 2=Minor, 3=Major, 4=Critical
    safety_risk_level     String        DEFAULT '', -- Safety implications
    action_required       String        DEFAULT '', -- Recommended maintenance action
    repair_complexity     String        DEFAULT '', -- DIY vs Pro repair assessment
    estimated_repair_hours Float32      DEFAULT 0,  -- Estimated hours to fix
    driver_related        UInt8         DEFAULT 0,  -- 1 if driver behavior can cause/fix it
    driver_behaviour_category  String   DEFAULT '', -- Aggressive, wasteful, etc.
    driver_behaviour_trigger   String   DEFAULT '', -- What driver action triggers it
    driver_training_required   UInt8    DEFAULT 0,  -- If training can help
    fleet_management_action    String   DEFAULT '', -- Fleet-level action
    recommended_preventive_action String DEFAULT '', -- Preventive maintenance
    oem_specific          UInt8         DEFAULT 0,  -- Manufacturer-specific
    manufacturer_notes    String        DEFAULT '', -- OEM guidance
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dtc_code)
```

**Key Fields Used in Analytics:**
- `dtc_code` - Unique identifier
- `system`, `subsystem` - For fleet system health analysis
- `description` - Output in reports
- `severity_level` - Risk calculation multiplier
- `driver_related` - Driver-related fault identification
- `action_required` - Maintenance priority recommendations

**Data Lineage:**
- Populated from external DTC reference data
- Updated via Python function: `populate_dtc_master(client)`

---

### 3.2 Vehicle Master Table

**Table Name:** `vehicle_master_ravi_v2`  
**Purpose:** Reference table for all vehicles in the fleet with metadata

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS vehicle_master_ravi_v2 (
    clientLoginId         String,     -- Tenant identifier
    uniqueid              String,     -- Vehicle unique identifier (VIN or device ID)
    vehicle_number        String        DEFAULT '', -- Fleet vehicle number
    customer_name         String        DEFAULT '', -- Customer/driver name
    model                 String        DEFAULT '', -- Vehicle model
    manufacturing_year    UInt16        DEFAULT 0,  -- Year manufactured
    vehicle_type          String        DEFAULT '', -- Type (truck, car, etc.)
    solutionType          String        DEFAULT '', -- OBD solution type
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid)
```

**Filtering Logic:**
- Only includes vehicles where `solutionType` IN (`'obd_solution'`, `'obd_analog_solution'`, `'obd_fuel+fuel_solution'`)

**Data Lineage:**
- Populated from vehicle_profile_ss table
- Updated via Python function: `populate_vehicle_master(client)`

---

## 4. Processing Pipeline

### 4.1 DTC Events Exploded Table

**Table Name:** `dtc_events_exploded_ravi_v2`  
**Purpose:** Raw events with vehicle metadata - foundation for episode detection  
**Input Tables:** `aimm.dtc_data_history` + `vehicle_master_ravi_v2`

**Schema:**

```sql
CREATE TABLE IF NOT EXISTS dtc_events_exploded_ravi_v2 (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    ts                    UInt32,         -- Unix timestamp of event
    dtc_code              String,         -- Single DTC code (exploded from array)
    lat                   Float64       DEFAULT 0,  -- Latitude if available
    lng                   Float64       DEFAULT 0,  -- Longitude if available
    dtc_pgn               String        DEFAULT '', -- DTC PGN (parameter group number)
    created_at            DateTime      DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toDate(toDateTime(ts))
ORDER BY (clientLoginId, uniqueid, dtc_code, ts)
```

**Data Transformation:**
- Takes raw DTC history where multiple DTCs are in an array
- Uses `ARRAY JOIN` to explode each array into individual rows
- Joins with `vehicle_master` to get vehicle details

### 4.1.1 SQL Query: Populate DTC Events Exploded

```sql
INSERT INTO dtc_events_exploded_ravi_v2
    (clientLoginId, uniqueid, vehicle_number, ts, dtc_code,
     lat, lng, dtc_pgn, created_at)
SELECT
    vm.clientLoginId,
    toString(e.uniqueid),
    vm.vehicle_number,
    toUInt32(toUnixTimestamp(e.ts)),
    dtc_code,
    toFloat64(ifNull(e.lat, 0)),
    toFloat64(ifNull(e.lng, 0)),
    toString(ifNull(e.dtc_pgn, '')),
    now()
FROM aimm.dtc_data_history AS e
ARRAY JOIN e.dtc_code AS dtc_code          -- Explode multiple DTCs into rows
INNER JOIN vehicle_master_ravi_v2 AS vm ON toString(e.uniqueid) = vm.uniqueid
WHERE toUInt32(toUnixTimestamp(e.ts)) > {analytics_window_start_ts}
  AND toUInt32(toUnixTimestamp(e.ts)) <= {until_ts}
  AND dtc_code != '0'  -- Exclude null/zero DTCs
```

**Key Points:**
- `ARRAY JOIN` unpacks multiple DTCs from a single record into separate rows
- Filters by time window (configurable, default 90 days)
- Excludes DTC code '0' (no fault indicator)
- Converts timestamps to Unix UInt32 format for efficiency

---

## 5. Episode Detection Logic

Episodes are fundamental to the analytics system. The detection involves **5 temporary tables** created in sequence:

### 5.1 Step 1: Engine Cycle Timestamps (`_tmp_engine_ts_ravi`)

**Purpose:** Materialize engine cycle end timestamps for all vehicles

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_engine_ts_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, cycle_end_u32)
AS
SELECT
    toString(uniqueid) AS uniqueid,
    toUInt32(toUnixTimestamp(assumeNotNull(cycle_end_ts))) AS cycle_end_u32
FROM aimm.engineoncycles
WHERE cycle_end_ts IS NOT NULL
```

**Purpose:** Pre-computed table of engine boundaries to detect when an engine was restarted

---

### 5.2 Step 2: Lagged Timestamps (`_tmp_events_lag_ravi`)

**Purpose:** Add previous event timestamp for each event within a DTC code per vehicle

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_events_lag_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    uniqueid,
    dtc_code,
    ts,
    lagInFrame(ts, 1, 0)
        OVER (PARTITION BY uniqueid, dtc_code ORDER BY ts
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_ts
FROM dtc_events_exploded_ravi_v2
```

**Window Function Logic:**
- `PARTITION BY uniqueid, dtc_code` - Separate groups per vehicle & DTC
- `ORDER BY ts` - Sort chronologically
- `lagInFrame(ts, 1, 0)` - Get previous timestamp (0 if first event)

**Output:** Each row now has `ts` (current time) and `prev_ts` (previous event time)

---

### 5.3 Step 3: Break Detection (`_tmp_events_break_ravi`)

**Purpose:** Identify episode boundaries based on time gaps OR engine cycle boundaries

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_events_break_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    el.uniqueid,
    el.dtc_code,
    el.ts,
    el.prev_ts,
    CASE
        WHEN el.prev_ts = 0 THEN 1                    -- First event (always new episode)
        WHEN (el.ts - el.prev_ts) > {gap_seconds} THEN 1  -- Time gap exceeded (default 1800s)
        WHEN eb.has_boundary = 1 THEN 1               -- Engine boundary detected
        ELSE 0
    END AS is_break
FROM _tmp_events_lag_ravi AS el
LEFT JOIN (
    -- Subquery: For each event, check if engine cycle exists between prev_ts and ts
    SELECT
        el2.uniqueid,
        el2.dtc_code,
        el2.ts,
        toUInt8(count() > 0) AS has_boundary
    FROM _tmp_events_lag_ravi AS el2
    INNER JOIN _tmp_engine_ts_ravi AS ec
        ON ec.uniqueid = el2.uniqueid
       AND ec.cycle_end_u32 > el2.prev_ts
       AND ec.cycle_end_u32 <= el2.ts
    WHERE el2.prev_ts > 0  -- Only check if not the first event
    GROUP BY el2.uniqueid, el2.dtc_code, el2.ts
) AS eb
    ON  el.uniqueid  = eb.uniqueid
    AND el.dtc_code  = eb.dtc_code
    AND el.ts        = eb.ts
```

**Break Conditions:**
1. `prev_ts = 0` → First occurrence of DTC for vehicle (always new episode)
2. `ts - prev_ts > gap_seconds` → Gap between events > threshold (marks episode boundary)
3. Engine cycle between prev_ts and ts → Detected restart (engine off then on again)

**Output:** Column `is_break` = 1 if episode boundary, 0 otherwise

---

### 5.4 Step 4: Episode ID Assignment (`_tmp_events_episode_ravi`)

**Purpose:** Assign unique episode IDs using cumulative sum of break flags

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_events_episode_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, ts)
AS
SELECT
    uniqueid,
    dtc_code,
    ts,
    sum(is_break)
        OVER (PARTITION BY uniqueid, dtc_code ORDER BY ts
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_id
FROM _tmp_events_break_ravi
```

**Window Function Logic:**
- Cumulative sum of `is_break` column
- Increments each time a break is encountered
- Groups by vehicle + DTC code

**Example:**
```
| uniqueid | dtc_code | ts  | is_break | episode_id |
|----------|----------|-----|----------|-----------|
| V1       | P0128    | 100 | 1        | 1         |
| V1       | P0128    | 105 | 0        | 1         |
| V1       | P0128    | 110 | 0        | 1         |
| V1       | P0128    | 2000| 1        | 2         |  ← Time gap > 1800s
| V1       | P0128    | 2005| 0        | 2         |
```

---

### 5.5 Step 5: Episode Aggregation (`_tmp_episode_agg_ravi`)

**Purpose:** Aggregate raw events into episode summaries (one row per episode)

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_episode_agg_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, episode_id)
AS
SELECT
    ev.uniqueid,
    ev.dtc_code,
    ev.episode_id,
    min(ev.ts)   AS first_ts,           -- Start time of episode
    max(ev.ts)   AS last_ts,            -- End time of episode
    toUInt32(count()) AS occurrence_count  -- Number of events in episode
FROM _tmp_events_episode_ravi AS ev
GROUP BY ev.uniqueid, ev.dtc_code, ev.episode_id
```

**Aggregations:**
- `first_ts` - Earliest event in episode
- `last_ts` - Latest event in episode
- `occurrence_count` - How many individual DTC triggers in this episode
- **Note:** `duration = last_ts - first_ts`

---

### 5.6 Step 6: Resolution Flagging (`_tmp_episode_resolved_ravi`)

**Purpose:** Add resolution flag, resolution time, and gap from previous episode

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_episode_resolved_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid, dtc_code, first_ts)
AS
WITH
max_engine AS (
    -- Find latest engine cycle for each vehicle
    SELECT uniqueid, max(cycle_end_u32) AS max_cycle_ts
    FROM _tmp_engine_ts_ravi
    GROUP BY uniqueid
),
with_resolution AS (
    -- Calculate resolution time and flag resolved episodes
    SELECT
        ea.*,
        toUInt32(greatest(0, ea.last_ts - ea.first_ts)) AS resolution_time_sec,
        if(
            ea.last_ts <= {cutoff_ts}  -- Last event before cutoff date
            AND ifNull(me.max_cycle_ts, 0) > ea.last_ts,  -- Engine restarted after last event
            toUInt8(1), toUInt8(0)
        ) AS is_resolved
    FROM _tmp_episode_agg_ravi AS ea
    LEFT JOIN max_engine AS me ON ea.uniqueid = me.uniqueid
),
with_lag AS (
    -- Lag previous episode end time
    SELECT
        wr.*,
        lagInFrame(wr.last_ts, 1, 0)
            OVER (PARTITION BY wr.uniqueid, wr.dtc_code ORDER BY wr.first_ts
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS prev_last_ts
    FROM with_resolution AS wr
)
SELECT
    uniqueid, dtc_code, episode_id, first_ts, last_ts,
    occurrence_count, resolution_time_sec, is_resolved,
    toUInt32(greatest(0, if(prev_last_ts = 0, 0,
                 toInt64(first_ts) - toInt64(prev_last_ts)))) AS gap_from_previous_episode
FROM with_lag
```

**Key Calculations:**

| Field | Formula | Meaning |
|-------|---------|---------|
| `resolution_time_sec` | `last_ts - first_ts` | How long the episode lasted |
| `is_resolved` | Last event before cutoff AND engine restarted after | Flag if resolved |
| `gap_from_previous_episode` | `first_ts - prev_last_ts` | Time since previous episode ended |

**Resolution Logic:**
- Episode is marked `is_resolved = 1` if:
  1. Last DTC event is before cutoff (default 1 day ago)
  2. AND engine has restarted after that event

---

### 5.7 Step 7: Per-Vehicle Risk Scoring (`_tmp_vehicle_risk_ravi`)

**Purpose:** Calculate health risk score for each vehicle from active (unresolved) episodes

**SQL Query:**

```sql
CREATE TABLE IF NOT EXISTS _tmp_vehicle_risk_ravi
ENGINE = MergeTree()
ORDER BY (uniqueid)
AS
WITH
ep_counts AS (
    -- Count episodes per DTC code for this vehicle
    SELECT uniqueid, dtc_code, count() AS ep_count
    FROM _tmp_episode_agg_ravi
    GROUP BY uniqueid, dtc_code
)
SELECT
    er.uniqueid AS uniqueid,
    toFloat64(sum(
        -- SEVERITY MULTIPLIER
        multiIf(toInt8(ifNull(dm.severity_level, 1)) <= 1, 1.0,
                toInt8(ifNull(dm.severity_level, 1))  = 2, 3.0,
                toInt8(ifNull(dm.severity_level, 1))  = 3, 7.0, 12.0)
        -- × DURATION FACTOR
        * (1.0 + least(toFloat64(er.resolution_time_sec) / 86400.0, 30.0) / 10.0)
        -- × RECURRENCE FACTOR
        * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count, 1)) - 1.0) * 0.5)
    )) AS risk
FROM _tmp_episode_resolved_ravi AS er
LEFT JOIN dtc_master_ravi_v2 AS dm ON er.dtc_code = dm.dtc_code
LEFT JOIN ep_counts AS ec ON er.uniqueid = ec.uniqueid AND er.dtc_code = ec.dtc_code
WHERE er.is_resolved = 0  -- Only active faults
GROUP BY er.uniqueid
```

**Risk Scoring Formula:**
```
risk_per_vehicle = Σ(severity_multiplier × duration_factor × recurrence_factor)
```

See [Section 8: Formulas](#8-formulas--calculations) for detailed formula breakdown.

---

## 6. Vehicle Fault Master (Operational Master)

**Table Name:** `vehicle_fault_master_ravi_v2`  
**Purpose:** Central fact table containing all episode data - source for all analytics  
**Input:** All temp tables from episode detection pipeline

### 6.1 Schema

```sql
CREATE TABLE IF NOT EXISTS vehicle_fault_master_ravi_v2 (
    episode_id            String,              -- Unique episode identifier
    clientLoginId         String,              -- Tenant
    uniqueid              String,              -- Vehicle ID
    vehicle_number        String        DEFAULT '',
    customer_name         String        DEFAULT '',
    model                 String        DEFAULT '',
    manufacturing_year    UInt16        DEFAULT 0,
    dtc_code              String,              -- Diagnostic code
    system                String        DEFAULT '',  -- e.g., Powertrain, Safety
    subsystem             String        DEFAULT '',  -- e.g., Coolant, Emission
    description           String        DEFAULT '',  -- Human-readable description
    severity_level        Int8          DEFAULT 1,   -- 1-4 (critical)
    first_ts              UInt32,              -- Episode start
    last_ts               UInt32,              -- Episode end
    event_date            Date,                -- Date of first event
    occurrence_count      UInt32        DEFAULT 0,   -- DTC occurrences in episode
    resolution_time_sec   UInt32        DEFAULT 0,   -- Duration in seconds
    is_resolved           UInt8         DEFAULT 0,   -- 1 if resolved
    resolution_reason     String        DEFAULT '', -- (reserved for future use)
    gap_from_previous_episode UInt32    DEFAULT 0,   -- Gap since last episode
    engine_cycles_during  UInt32        DEFAULT 0,   -- Engine cycles during episode
    driver_related        UInt8         DEFAULT 0,   -- From DTC master flag
    has_engine_issue      UInt8         DEFAULT 0,   -- 1 if subsystem contains "powertrain"
    has_coolant_issue     UInt8         DEFAULT 0,   -- 1 if subsystem contains "coolant"
    has_safety_issue      UInt8         DEFAULT 0,   -- 1 if system contains "safety"
    has_emission_issue    UInt8         DEFAULT 0,   -- 1 if subsystem/system contains "emission"
    has_electrical_issue  UInt8         DEFAULT 0,   -- 1 if subsystem contains "electrical"
    vehicle_health_score  Float32       DEFAULT 100, -- Calculated 0-100
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (clientLoginId, uniqueid, dtc_code, first_ts)
```

### 6.2 Data Population SQL

```sql
INSERT INTO vehicle_fault_master_ravi_v2
    (episode_id, clientLoginId, uniqueid, vehicle_number, customer_name,
     model, manufacturing_year, dtc_code,
     system, subsystem, description, severity_level,
     first_ts, last_ts, event_date, occurrence_count, resolution_time_sec,
     is_resolved, resolution_reason,
     gap_from_previous_episode, engine_cycles_during, driver_related,
     has_engine_issue, has_coolant_issue, has_safety_issue,
     has_emission_issue, has_electrical_issue,
     vehicle_health_score,
     created_at, updated_at)
SELECT
    -- Episode Identifier
    concat(toString(er.uniqueid), '_', er.dtc_code, '_', toString(er.episode_id)) AS episode_id,
    
    -- Vehicle & Tenant Details
    vm.clientLoginId,
    er.uniqueid,
    vm.vehicle_number,
    vm.customer_name,
    vm.model,
    vm.manufacturing_year,
    
    -- DTC Details
    er.dtc_code,
    ifNull(nullIf(nullIf(dm.system, 'nan'), 'None'), ''),
    ifNull(nullIf(nullIf(dm.subsystem, 'nan'), 'None'), ''),
    ifNull(nullIf(dm.description, 'nan'), ''),
    toInt8(greatest(ifNull(toInt16(dm.severity_level), 1), 1)),
    
    -- Temporal Data
    er.first_ts,
    er.last_ts,
    toDate(toDateTime(er.first_ts)) AS event_date,
    er.occurrence_count,
    er.resolution_time_sec,
    er.is_resolved,
    '' AS resolution_reason,
    
    -- Episode Gaps & Engine Data
    er.gap_from_previous_episode,
    toUInt32(ifNull(ece.eng_cycles, 0)) AS engine_cycles_during,
    
    -- Flags from DTC Master
    toUInt8(ifNull(dm.driver_related, 0)),
    
    -- System Classification Flags (derived from subsystem/system)
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%powertrain%'
            OR lower(ifNull(dm.system,'')) LIKE '%engine%') AS has_engine_issue,
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%coolant%') AS has_coolant_issue,
    toUInt8(lower(ifNull(dm.system,'')) LIKE '%safety%') AS has_safety_issue,
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%emission%'
            OR lower(ifNull(dm.system,'')) LIKE '%emission%') AS has_emission_issue,
    toUInt8(lower(ifNull(dm.subsystem,'')) LIKE '%electrical%'
            OR lower(ifNull(dm.system,'')) LIKE '%electrical%') AS has_electrical_issue,
    
    -- HEALTH SCORE CALCULATION
    toFloat32(greatest(0.0, 100.0 - least(ifNull(vr.risk, 0.0) * {scaling_constant}, 100.0))),
    
    now(),
    now()

FROM _tmp_episode_resolved_ravi AS er
INNER JOIN vehicle_master_ravi_v2 AS vm ON er.uniqueid = vm.uniqueid
LEFT JOIN dtc_master_ravi_v2 AS dm ON er.dtc_code = dm.dtc_code
LEFT JOIN _tmp_vehicle_risk_ravi AS vr ON er.uniqueid = vr.uniqueid
LEFT JOIN (
    -- Count engine cycles during episode
    SELECT
        ea.uniqueid, ea.dtc_code, ea.episode_id,
        toUInt32(count()) AS eng_cycles
    FROM _tmp_episode_agg_ravi AS ea
    INNER JOIN _tmp_engine_ts_ravi AS ec
        ON ec.uniqueid = ea.uniqueid
       AND ec.cycle_end_u32 >= ea.first_ts
       AND ec.cycle_end_u32 <= ea.last_ts
    GROUP BY ea.uniqueid, ea.dtc_code, ea.episode_id
) AS ece
    ON er.uniqueid  = ece.uniqueid
   AND er.dtc_code  = ece.dtc_code
   AND er.episode_id = ece.episode_id
```

### 6.3 Key Calculations in Vehicle Fault Master

#### Vehicle Health Score

**Formula:**
$$\text{vehicle\_health\_score} = 100 - \min(\text{risk\_score} \times K, 100)$$

Where:
- `risk_score` = Sum of risk from all active (unresolved) episodes
- $K$ = Scaling constant (default 0.2, from `HEALTH_SCALING_CONSTANT`)
- Score capped at 0 (minimum) to 100 (maximum)

#### System Issue Flags

Used for categorizing fault types:
- `has_engine_issue` = 1 if subsystem LIKE '%powertrain%' OR system LIKE '%engine%'
- `has_coolant_issue` = 1 if subsystem LIKE '%coolant%'
- `has_safety_issue` = 1 if system LIKE '%safety%'
- `has_emission_issue` = 1 if subsystem LIKE '%emission%' OR system LIKE '%emission%'
- `has_electrical_issue` = 1 if subsystem LIKE '%electrical%' OR system LIKE '%electrical%'

---

## 7. Analytics Tables (Level 1)

All 8 analytics tables are derived from `vehicle_fault_master_ravi_v2`. Each is specialized for a particular analysis view.

---

### 7.1 Fleet Health Summary

**Table Name:** `fleet_health_summary_ravi_v2`  
**Dimension:** Per `clientLoginId` (one row per fleet/tenant)  
**Update Frequency:** Every pipeline run  
**Purpose:** High-level fleet health scoreboard

#### 7.1.1 Schema

```sql
CREATE TABLE IF NOT EXISTS fleet_health_summary_ravi_v2 (
    clientLoginId                String,
    total_vehicles               UInt32  DEFAULT 0,  -- All vehicles in fleet
    vehicles_with_active_faults  UInt32  DEFAULT 0,  -- Vehicles with unresolved faults
    vehicles_with_critical_faults UInt32 DEFAULT 0,  -- Vehicles with severity >= 3
    driver_related_faults        UInt32  DEFAULT 0,  -- Count of driver-behavior faults
    fleet_health_score           Float32 DEFAULT 100, -- Aggregate 0-100 score
    most_common_dtc              String  DEFAULT '',  -- Top DTC code by occurrence
    most_common_system           String  DEFAULT '',  -- Top system by occurrence
    active_fault_trend           String  DEFAULT 'stable', -- 'increasing'|'decreasing'|'stable'
    created_at                   DateTime DEFAULT now(),
    updated_at                   DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId)
```

#### 7.1.2 SQL Query

```sql
INSERT INTO fleet_health_summary_ravi_v2
    (clientLoginId, total_vehicles, vehicles_with_active_faults,
     vehicles_with_critical_faults, driver_related_faults,
     fleet_health_score, most_common_dtc, most_common_system,
     active_fault_trend, created_at, updated_at)

WITH
all_vehicles AS (
    -- Count total vehicles per fleet
    SELECT clientLoginId, count() AS total 
    FROM vehicle_master_ravi_v2 
    GROUP BY clientLoginId
),

ep_counts AS (
    -- Count episodes per DTC code for recurrence calculation
    SELECT uniqueid, dtc_code, uniqExact(episode_id) AS ep_count
    FROM vehicle_fault_master_ravi_v2
    GROUP BY uniqueid, dtc_code
),

fault_stats AS (
    -- Aggregate fault statistics per fleet
    SELECT
        f.clientLoginId,
        
        -- Count distinct vehicles with active faults
        uniqExactIf(f.uniqueid, f.is_resolved = 0) AS active_veh,
        
        -- Count distinct vehicles with critical (severity >= 3) active faults
        uniqExactIf(f.uniqueid, f.is_resolved = 0 AND f.severity_level >= 3) AS critical_veh,
        
        -- Count driver-related incidents
        countIf(f.driver_related = 1) AS driver_faults,
        
        -- Sum risk from all active unresolved episodes (will be normalized)
        sum(if(f.is_resolved = 0,
            -- Severity multiplier: 1.0, 3.0, 7.0, or 12.0
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0)
            -- × Duration factor
            * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
            -- × Recurrence factor
            * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count,1)) - 1.0) * 0.5),
        0)) AS risk_sum,
        
        -- Top K DTCs (weighted by occurrence)
        topKWeighted(1)(f.dtc_code, f.occurrence_count) AS top_dtc_arr,
        topKWeighted(1)(f.system, f.occurrence_count)    AS top_sys_arr
        
    FROM vehicle_fault_master_ravi_v2 AS f
    LEFT JOIN ep_counts AS ec ON f.uniqueid = ec.uniqueid AND f.dtc_code = ec.dtc_code
    GROUP BY f.clientLoginId
),

trending AS (
    -- Compare fault trend: last 7 days vs prior 7 days
    SELECT
        clientLoginId,
        countIf(is_resolved = 0 AND event_date >= today() - 7) AS recent_active,
        countIf(is_resolved = 0 AND event_date >= today() - 14 AND event_date < today() - 7) AS prior_active
    FROM vehicle_fault_master_ravi_v2
    GROUP BY clientLoginId
)

SELECT
    av.clientLoginId,
    av.total,
    ifNull(fs.active_veh, 0),
    ifNull(fs.critical_veh, 0),
    ifNull(fs.driver_faults, 0),
    
    -- Fleet Health Score: Normalized risk, scaled by constant
    toFloat32(
        if(
            ifNull(fs.active_veh, 0) > 0,
            greatest(
                0.0,
                100.0 - least(
                    -- Divide total risk by number of vehicles to get per-vehicle avg
                    (ifNull(fs.risk_sum, 0.0) / greatest(toFloat64(fs.active_veh), 1.0)) 
                    * {scaling_constant},  -- Apply scaling constant
                    100.0  -- Cap at 100
                )
            ),
            100.0  -- If no active vehicles, score is 100 (healthy)
        )
    ),
    
    if(length(fs.top_dtc_arr) > 0, fs.top_dtc_arr[1], ''),
    if(length(fs.top_sys_arr)  > 0, fs.top_sys_arr[1], ''),
    
    -- Trend: Compare recent vs prior week
    multiIf(
        ifNull(tr.recent_active, 0) > ifNull(tr.prior_active, 0) * 1.1 + 1, 'increasing',
        ifNull(tr.prior_active, 0) > 0 AND ifNull(tr.recent_active, 0) < ifNull(tr.prior_active, 0) * 0.9, 'decreasing',
        'stable'
    ),
    now(), now()

FROM all_vehicles AS av
LEFT JOIN fault_stats AS fs ON av.clientLoginId = fs.clientLoginId
LEFT JOIN trending AS tr ON av.clientLoginId = tr.clientLoginId
```

#### 7.1.3 Metrics Explanation

| Metric | Calculation | Use Case |
|--------|-----------|----------|
| `total_vehicles` | COUNT from vehicle_master | Baseline for fleet size |
| `vehicles_with_active_faults` | UNIQ WHERE is_resolved=0 | Affected vehicles count |
| `vehicles_with_critical_faults` | UNIQ WHERE severity>=3 AND is_resolved=0 | Critical issues |
| `driver_related_faults` | COUNT WHERE driver_related=1 | Driver behavior |
| `fleet_health_score` | 100 - (avg_risk × K), capped 0-100 | Overall health 0-100 |
| `most_common_dtc` | TOP 1 by occurrence count | Top issue for fleet |
| `most_common_system` | TOP 1 system by occurrence | Top system category |
| `active_fault_trend` | Last 7d vs prior 7d comparison | Trend indicator |

---

### 7.2 Fleet DTC Distribution

**Table Name:** `fleet_dtc_distribution_ravi_v2`  
**Dimension:** Per `clientLoginId` + `dtc_code` (multiple rows per fleet)  
**Purpose:** Which DTCs affect the fleet most, breakdown per code

#### 7.2.1 Schema

```sql
CREATE TABLE IF NOT EXISTS fleet_dtc_distribution_ravi_v2 (
    clientLoginId         String,
    dtc_code              String,
    description           String        DEFAULT '',
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    vehicles_affected     UInt32        DEFAULT 0,    -- Count of distinct vehicles
    active_vehicles       UInt32        DEFAULT 0,    -- Vehicles with unresolved faults
    total_occurrences     UInt64        DEFAULT 0,    -- Sum of occurrence_count
    total_episodes        UInt32        DEFAULT 0,    -- Count of episodes
    avg_resolution_time   Float32       DEFAULT 0,    -- Average resolution time in sec
    driver_related_count  UInt32        DEFAULT 0,    -- Count of driver-related incidents
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code)
```

#### 7.2.2 SQL Query

```sql
INSERT INTO fleet_dtc_distribution_ravi_v2
    (clientLoginId, dtc_code, description, system, subsystem, severity_level,
     vehicles_affected, active_vehicles, total_occurrences, total_episodes,
     avg_resolution_time, driver_related_count, created_at, updated_at)
SELECT
    f.clientLoginId,
    f.dtc_code,
    
    -- Pick any value (since same DTC code has same description)
    any(f.description),
    any(f.system),
    any(f.subsystem),
    any(f.severity_level),
    
    -- Distinct vehicles affected by this DTC
    uniqExact(f.uniqueid) AS vehicles_affected,
    
    -- Distinct vehicles with ACTIVE (unresolved) occurrences
    uniqExactIf(f.uniqueid, f.is_resolved = 0) AS active_vehicles,
    
    -- Total occurrences = sum of all DTC triggers within all episodes
    sum(f.occurrence_count) AS total_occurrences,
    
    -- Total episodes count for this DTC code
    count() AS total_episodes,
    
    -- Average resolution time (only for resolved episodes with resolution_time > 0)
    if(countIf(f.resolution_time_sec > 0) > 0,
       toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                 / countIf(f.resolution_time_sec > 0)), 0),
    
    -- Count driver-related incidents for this DTC
    countIf(f.driver_related = 1),
    
    now(), now()

FROM vehicle_fault_master_ravi_v2 AS f
GROUP BY f.clientLoginId, f.dtc_code
```

#### 7.2.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `vehicles_affected` | UNIQ(uniqueid) | How many vehicles got this DTC |
| `active_vehicles` | UNIQ WHERE is_resolved=0 | How many still have it |
| `total_occurrences` | SUM(occurrence_count) | Total triggers across all instances |
| `total_episodes` | COUNT(*) | How many episodes of this DTC |
| `avg_resolution_time` | AVG(resolution_time_sec) for resolved=1 | How long to fix (seconds) |
| `driver_related_count` | COUNT WHERE driver_related=1 | If driver behavior causes it |

---

### 7.3 Fleet System Health

**Table Name:** `fleet_system_health_ravi_v2`  
**Dimension:** Per `clientLoginId` + `system` (e.g., Powertrain, Safety)  
**Purpose:** Risk breakdown by vehicle system category

#### 7.3.1 Schema

```sql
CREATE TABLE IF NOT EXISTS fleet_system_health_ravi_v2 (
    clientLoginId         String,
    system                String,         -- System category
    vehicles_affected     UInt32  DEFAULT 0,   -- Active vehicles with faults in this system
    active_faults         UInt32  DEFAULT 0,   -- Count of active faults
    critical_faults       UInt32  DEFAULT 0,   -- Count with severity >= 3
    risk_score            Float32 DEFAULT 0,   -- Aggregate severity score
    trend                 String  DEFAULT 'stable',
    created_at            DateTime DEFAULT now(),
    updated_at            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, system)
```

#### 7.3.2 SQL Query

```sql
INSERT INTO fleet_system_health_ravi_v2
    (clientLoginId, system, vehicles_affected, active_faults, critical_faults,
     risk_score, trend, created_at, updated_at)
SELECT
    f.clientLoginId,
    -- Standardize empty system values
    multiIf(f.system = '' OR f.system = 'nan', 'other', f.system) AS sys,
    
    -- Count distinct vehicles with active faults in this system
    uniqExactIf(f.uniqueid, f.is_resolved = 0) AS vehicles_affected,
    
    -- Count total active fault occurrences
    countIf(f.is_resolved = 0) AS active_faults,
    
    -- Count critical-level faults (severity >= 3)
    countIf(f.is_resolved = 0 AND f.severity_level >= 3) AS critical_faults,
    
    -- Sum severity scores only for active faults
    toFloat32(sumIf(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0),
        f.is_resolved = 0)) AS risk_score,
    
    'stable' AS trend,
    now(), now()

FROM vehicle_fault_master_ravi_v2 AS f
GROUP BY f.clientLoginId, sys
```

#### 7.3.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `vehicles_affected` | UNIQ WHERE is_resolved=0 | Vehicles with active issues in this system |
| `active_faults` | COUNT WHERE is_resolved=0 | Total active fault count |
| `critical_faults` | COUNT WHERE severity>=3 AND is_resolved=0 | Severe issues count |
| `risk_score` | SUM(severity_multiplier) for active | Aggregate risk |

---

### 7.4 Fleet Fault Trends

**Table Name:** `fleet_fault_trends_ravi_v2`  
**Dimension:** Per `clientLoginId` + `date` (time-series data)  
**Purpose:** Daily trend tracking for fleet health monitoring

#### 7.4.1 Schema

```sql
CREATE TABLE IF NOT EXISTS fleet_fault_trends_ravi_v2 (
    clientLoginId         String,
    date                  Date,                      -- Daily snapshot
    active_faults         UInt32  DEFAULT 0,         -- Count of unresolved faults
    critical_faults       UInt32  DEFAULT 0,         -- Severity >= 3
    new_faults            UInt32  DEFAULT 0,         -- Faults detected on this day
    resolved_faults       UInt32  DEFAULT 0,         -- Faults resolved on this day
    driver_related_faults UInt32  DEFAULT 0,         -- Driver-behavior incidents
    fleet_health_score    Float32 DEFAULT 100,       -- Daily health 0-100
    created_at            DateTime DEFAULT now(),
    updated_at            DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (clientLoginId, date)
```

#### 7.4.2 SQL Query

```sql
INSERT INTO fleet_fault_trends_ravi_v2
    (clientLoginId, date, active_faults, critical_faults, new_faults, resolved_faults,
     driver_related_faults, fleet_health_score, created_at, updated_at)
SELECT
    f.clientLoginId,
    f.event_date AS date,
    
    -- Count unresolved faults as of this date (point-in-time)
    countIf(f.is_resolved = 0) AS active_faults,
    
    -- Unresolved faults with severity >= 3
    countIf(f.is_resolved = 0 AND f.severity_level >= 3) AS critical_faults,
    
    -- NEW faults detected on this date (all faults)
    count() AS new_faults,
    
    -- Faults resolved on this date
    countIf(f.is_resolved = 1) AS resolved_faults,
    
    -- Driver-related incidents detected this date
    countIf(f.driver_related = 1) AS driver_related_faults,
    
    -- Daily fleet health score
    toFloat32(greatest(0.0, 100.0 - least(
        sumIf(
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0),
            f.is_resolved = 0
        ) / greatest(uniqExactIf(f.uniqueid, f.is_resolved = 0), 1)
        * {scaling_constant}, 100.0))) AS fleet_health_score,
    
    now(), now()

FROM vehicle_fault_master_ravi_v2 AS f
GROUP BY f.clientLoginId, f.event_date
ORDER BY f.clientLoginId, f.event_date
```

#### 7.4.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `active_faults` | COUNT WHERE is_resolved=0 | Current unresolved faults |
| `critical_faults` | COUNT WHERE severity>=3 AND is_resolved=0 | Severe issues |
| `new_faults` | COUNT ALL | Detected on this date |
| `resolved_faults` | COUNT WHERE is_resolved=1 | Fixed on this date |
| `driver_related_faults` | COUNT WHERE driver_related=1 | Driver behavior incidents |
| `fleet_health_score` | 100 - (avg_severity × K), capped | Daily health 0-100 |

---

### 7.5 Vehicle Health Summary

**Table Name:** `vehicle_health_summary_ravi_v2`  
**Dimension:** Per `clientLoginId` + `uniqueid` (one row per vehicle)  
**Purpose:** Per-vehicle detailed health and fault breakdown

#### 7.5.1 Schema

```sql
CREATE TABLE IF NOT EXISTS vehicle_health_summary_ravi_v2 (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    customer_name         String        DEFAULT '',
    active_fault_count    UInt32        DEFAULT 0,    -- Unresolved faults
    critical_fault_count  UInt32        DEFAULT 0,    -- Severity >= 3
    total_episodes        UInt32        DEFAULT 0,    -- All time episodes
    episodes_last_30_days UInt32        DEFAULT 0,    -- Recent activity
    avg_resolution_time   Float32       DEFAULT 0,    -- Average seconds
    last_fault_ts         UInt32        DEFAULT 0,    -- Latest fault time
    vehicle_health_score  Float32       DEFAULT 100,  -- 0-100 score
    driver_related_faults UInt32        DEFAULT 0,    -- Driver behavior count
    most_common_dtc       String        DEFAULT '',   -- Top DTC for vehicle
    has_engine_issue      UInt8         DEFAULT 0,    -- Boolean flags
    has_emission_issue    UInt8         DEFAULT 0,
    has_safety_issue      UInt8         DEFAULT 0,
    has_electrical_issue  UInt8         DEFAULT 0,
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid)
```

#### 7.5.2 SQL Query

```sql
INSERT INTO vehicle_health_summary_ravi_v2
    (clientLoginId, uniqueid, vehicle_number, customer_name,
     active_fault_count, critical_fault_count, total_episodes,
     episodes_last_30_days, avg_resolution_time, last_fault_ts,
     vehicle_health_score, driver_related_faults, most_common_dtc,
     has_engine_issue, has_emission_issue, has_safety_issue,
     has_electrical_issue, created_at, updated_at)

WITH
ep_counts AS (
    -- Episode recurrence per DTC code
    SELECT uniqueid, dtc_code, uniqExact(episode_id) AS ep_count
    FROM vehicle_fault_master_ravi_v2
    GROUP BY uniqueid, dtc_code
),

vehicle_risk AS (
    -- Per-vehicle active risk score
    SELECT
        f.uniqueid,
        sum(
            multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                    f.severity_level=3,7.0, 12.0)
            * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
            * (1.0 + greatest(0.0, toFloat64(ifNull(ec.ep_count,1)) - 1.0) * 0.5)
        ) AS risk
    FROM vehicle_fault_master_ravi_v2 AS f
    LEFT JOIN ep_counts AS ec ON f.uniqueid = ec.uniqueid AND f.dtc_code = ec.dtc_code
    WHERE f.is_resolved = 0
    GROUP BY f.uniqueid
),

vehicle_stats AS (
    -- Per-vehicle fault statistics
    SELECT
        f.clientLoginId,
        f.uniqueid,
        
        -- Fault counts
        countIf(f.is_resolved = 0) AS active,
        countIf(f.is_resolved = 0 AND f.severity_level >= 3) AS critical,
        count() AS total_eps,
        
        -- Recent activity (last 30 days)
        countIf(f.first_ts >= {thirty_days_ago_ts}) AS last_30,
        
        -- Resolution time (only for resolved)
        if(countIf(f.resolution_time_sec > 0) > 0,
           toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                     / countIf(f.resolution_time_sec > 0)), 0) AS avg_res,
        
        -- Latest fault
        max(f.last_ts) AS last_ts,
        
        -- Driver-related
        countIf(f.driver_related = 1) AS driver_faults,
        
        -- Top DTC
        topKWeighted(1)(f.dtc_code, f.occurrence_count) AS top_dtc_arr,
        
        -- System issue flags (any TRUE across episodes)
        maxIf(1, f.has_engine_issue     = 1) AS has_engine,
        maxIf(1, f.has_emission_issue   = 1) AS has_emission,
        maxIf(1, f.has_safety_issue     = 1) AS has_safety,
        maxIf(1, f.has_electrical_issue = 1) AS has_electrical
        
    FROM vehicle_fault_master_ravi_v2 AS f
    GROUP BY f.clientLoginId, f.uniqueid
)

-- LEFT JOIN to include all vehicles (even without faults)
SELECT
    vm.clientLoginId,
    vm.uniqueid,
    vm.vehicle_number,
    vm.customer_name,
    
    -- Fault counts (0 if no faults)
    ifNull(vs.active, 0),
    ifNull(vs.critical, 0),
    ifNull(vs.total_eps, 0),
    ifNull(vs.last_30, 0),
    ifNull(vs.avg_res, 0),
    ifNull(vs.last_ts, 0),
    
    -- Health score (100 for vehicles with no faults)
    toFloat32(greatest(0.0, 100.0 - least(ifNull(vr.risk, 0.0) * {scaling_constant}, 100.0))),
    
    ifNull(vs.driver_faults, 0),
    if(length(ifNull(vs.top_dtc_arr, [])) > 0, vs.top_dtc_arr[1], ''),
    
    -- System flags
    toUInt8(ifNull(vs.has_engine, 0)),
    toUInt8(ifNull(vs.has_emission, 0)),
    toUInt8(ifNull(vs.has_safety, 0)),
    toUInt8(ifNull(vs.has_electrical, 0)),
    
    now(), now()

FROM vehicle_master_ravi_v2 AS vm
LEFT JOIN vehicle_stats AS vs ON vm.uniqueid = vs.uniqueid
                              AND vm.clientLoginId = vs.clientLoginId
LEFT JOIN vehicle_risk  AS vr ON vm.uniqueid = vr.uniqueid
```

#### 7.5.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `active_fault_count` | COUNT WHERE is_resolved=0 | Current unresolved faults |
| `critical_fault_count` | COUNT WHERE severity>=3 AND is_resolved=0 | Severe issues |
| `total_episodes` | COUNT(*) | All time episodes |
| `episodes_last_30_days` | COUNT WHERE first_ts >= 30d ago | Recent activity indicator |
| `avg_resolution_time` | AVG(resolution_time_sec) | Average fix time |
| `last_fault_ts` | MAX(last_ts) | When vehicle was last active |
| `vehicle_health_score` | 100 - min(risk × K, 100) | 0-100 health |
| `most_common_dtc` | TOP 1 by occurrence | Primary fault |

---

### 7.6 DTC Fleet Impact

**Table Name:** `dtc_fleet_impact_ravi_v2`  
**Dimension:** Per `clientLoginId` + `dtc_code`  
**Purpose:** DTC-level impact assessment for fleet

#### 7.6.1 Schema

```sql
CREATE TABLE IF NOT EXISTS dtc_fleet_impact_ravi_v2 (
    clientLoginId         String,
    dtc_code              String,
    system                String        DEFAULT '',
    subsystem             String        DEFAULT '',
    vehicles_affected     UInt32        DEFAULT 0,    -- Total distinct vehicles
    active_vehicles       UInt32        DEFAULT 0,    -- With unresolved instances
    avg_resolution_time   Float32       DEFAULT 0,    -- Average fix time
    driver_related_ratio  Float32       DEFAULT 0,    -- % that are driver-related
    fleet_risk_score      Float32       DEFAULT 0,    -- Total severity score
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code)
```

#### 7.6.2 SQL Query

```sql
INSERT INTO dtc_fleet_impact_ravi_v2
    (clientLoginId, dtc_code, system, subsystem,
     vehicles_affected, active_vehicles,
     avg_resolution_time, driver_related_ratio, fleet_risk_score,
     created_at, updated_at)
SELECT
    f.clientLoginId,
    f.dtc_code,
    any(f.system),
    any(f.subsystem),
    
    -- Distinct vehicles affected
    uniqExact(f.uniqueid) AS vehicles_affected,
    
    -- Distinct vehicles with active instances
    uniqExactIf(f.uniqueid, f.is_resolved = 0) AS active_vehicles,
    
    -- Average resolution time
    if(countIf(f.resolution_time_sec > 0) > 0,
       toFloat32(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                 / countIf(f.resolution_time_sec > 0)), 0),
    
    -- Ratio of driver-related instances
    if(count() > 0,
       toFloat32(countIf(f.driver_related = 1) / count()), 0) AS driver_related_ratio,
    
    -- Sum severity for active faults
    toFloat32(sumIf(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0),
        f.is_resolved = 0)) AS fleet_risk_score,
    
    now(), now()

FROM vehicle_fault_master_ravi_v2 AS f
GROUP BY f.clientLoginId, f.dtc_code
```

#### 7.6.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `vehicles_affected` | UNIQ(uniqueid) | How many vehicles got this DTC |
| `active_vehicles` | UNIQ WHERE is_resolved=0 | Still experiencing it |
| `avg_resolution_time` | AVG(resolution_time_sec) | Average time to fix |
| `driver_related_ratio` | COUNT(driver_related=1) / COUNT(*) | Percentage driver-caused |
| `fleet_risk_score` | SUM(severity) for active | Aggregate risk impact |

---

### 7.7 Maintenance Priority

**Table Name:** `maintenance_priority_ravi_v2`  
**Dimension:** Per `clientLoginId` + `uniqueid` + `dtc_code` (ranked list)  
**Purpose:** Prioritized maintenance action list

#### 7.7.1 Schema

```sql
CREATE TABLE IF NOT EXISTS maintenance_priority_ravi_v2 (
    clientLoginId         String,
    uniqueid              String,
    vehicle_number        String        DEFAULT '',
    dtc_code              String,
    description           String        DEFAULT '',
    severity_level        Int8          DEFAULT 1,
    fault_duration_sec    UInt32        DEFAULT 0,    -- How long unresolved
    episodes_last_30_days UInt32        DEFAULT 0,    -- Recurrence in month
    maintenance_priority_score Float32  DEFAULT 0,    -- Composite score
    recommended_action    String        DEFAULT '',   -- From DTC master
    created_at            DateTime      DEFAULT now(),
    updated_at            DateTime      DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, uniqueid, dtc_code)
```

#### 7.7.2 SQL Query

```sql
INSERT INTO maintenance_priority_ravi_v2
    (clientLoginId, uniqueid, vehicle_number, dtc_code, description,
     severity_level, fault_duration_sec, episodes_last_30_days,
     maintenance_priority_score, recommended_action,
     created_at, updated_at)

WITH
recent_counts AS (
    -- Episodes in last 30 days
    SELECT uniqueid, dtc_code, count() AS eps_30
    FROM vehicle_fault_master_ravi_v2
    WHERE first_ts >= {thirty_days_ago_ts}
    GROUP BY uniqueid, dtc_code
)

SELECT
    f.clientLoginId,
    f.uniqueid,
    f.vehicle_number,
    f.dtc_code,
    f.description,
    f.severity_level,
    f.resolution_time_sec,
    ifNull(rc.eps_30, 0) AS episodes_last_30_days,
    
    -- PRIORITY SCORE = Severity × Duration Factor × Recurrence Factor
    toFloat32(
        multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                f.severity_level=3,7.0, 12.0)
        * (1.0 + least(f.resolution_time_sec / 86400.0, 30.0) / 10.0)
        * (1.0 + ifNull(rc.eps_30, 0))  -- More episodes = higher priority
    ),
    
    ifNull(dm.action_required, ''),
    now(), now()

FROM vehicle_fault_master_ravi_v2 AS f
LEFT JOIN recent_counts AS rc
    ON f.uniqueid = rc.uniqueid AND f.dtc_code = rc.dtc_code
LEFT JOIN dtc_master_ravi_v2 AS dm
    ON f.dtc_code = dm.dtc_code
WHERE f.is_resolved = 0  -- Only unresolved faults
```

#### 7.7.3 Metrics Explanation

| Metric | Calculation | Meaning |
|--------|-----------|---------|
| `fault_duration_sec` | resolution_time_sec | How long issue has existed |
| `episodes_last_30_days` | COUNT WHERE first_ts >= 30d | Recent recurrence count |
| `maintenance_priority_score` | severity × duration × recurrence | Composite priority 0-∞ |
| `recommended_action` | From DTC master | What to do |

**Priority Score Formula:**
$$\text{priority} = S \times D \times R$$

Where:
- $S$ = Severity multiplier (1.0 to 12.0)
- $D$ = Duration factor: $1.0 + \min(\text{duration\_days} / 30, 1)$
- $R$ = Recurrence factor: $1 + \text{episodes\_30d}$

**Sort by:** `maintenance_priority_score DESC` to get ranked action list

---

### 7.8 DTC Co-occurrence

**Table Name:** `dtc_cooccurrence_ravi_v2`  
**Dimension:** Per `dtc_code_a` + `dtc_code_b` (pairs)  
**Purpose:** Identify which DTCs appear together on same vehicles

#### 7.8.1 Schema

```sql
CREATE TABLE IF NOT EXISTS dtc_cooccurrence_ravi_v2 (
    clientLoginId         String,
    dtc_code_a            String,         -- First DTC (ordered)
    dtc_code_b            String,         -- Second DTC (ordered)
    cooccurrence_count    UInt32  DEFAULT 0,  -- Total co-occurrences
    vehicles_affected     UInt32  DEFAULT 0,  -- Vehicles with both DTCs
    avg_time_gap_sec      Float32 DEFAULT 0,  -- (reserved for future)
    last_seen_ts          UInt32  DEFAULT 0,  -- Latest co-occurrence
    created_at            DateTime DEFAULT now(),
    updated_at            DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (clientLoginId, dtc_code_a, dtc_code_b)
```

#### 7.8.2 SQL Query

```sql
INSERT INTO dtc_cooccurrence_ravi_v2
    (clientLoginId, dtc_code_a, dtc_code_b,
     cooccurrence_count, vehicles_affected, avg_time_gap_sec,
     last_seen_ts, created_at, updated_at)

WITH
-- For each vehicle, collect all distinct DTCs
per_vehicle AS (
    SELECT
        any(clientLoginId) AS clientLoginId,
        uniqueid,
        arraySort(groupUniqArray(dtc_code)) AS dtcs,
        count() AS total_episodes,
        max(last_ts) AS max_ts
    FROM vehicle_fault_master_ravi_v2
    GROUP BY uniqueid
    HAVING length(dtcs) >= 2  -- Only vehicles with 2+ DTC codes
),

-- Generate all ordered pairs (DTC_A, DTC_B) where A < B per vehicle
pairs AS (
    SELECT
        clientLoginId,
        uniqueid,
        max_ts,
        total_episodes,
        arrayJoin(
            arrayFlatten(
                arrayMap(
                    i -> arrayMap(
                        j -> tuple(dtcs[i], dtcs[j]),
                        range(i + 1, length(dtcs) + 1)
                    ),
                    range(1, length(dtcs))
                )
            )
        ) AS pair
    FROM per_vehicle
    WHERE length(dtcs) <= 200  -- Limit array size for performance
)

SELECT
    any(clientLoginId) AS clientLoginId,
    pair.1 AS dtc_code_a,
    pair.2 AS dtc_code_b,
    sum(total_episodes) AS cooccurrence_count,
    count() AS vehicles_affected,
    toFloat32(0) AS avg_time_gap_sec,
    max(max_ts) AS last_seen_ts,
    now(), now()

FROM pairs
GROUP BY dtc_code_a, dtc_code_b
HAVING vehicles_affected >= 2  -- Only pairs on 2+ vehicles
```

#### 7.8.3 Algorithm Explanation

**Step 1:** Per-vehicle DTC collection
- For each vehicle, collect all distinct DTC codes
- Filter to vehicles with 2+ DTCs

**Step 2:** Pair generation
- Use `arrayMap` to generate all combinations
- `arrayJoin` to flatten into rows
- Maintains ordering (A, B) where A < B

**Step 3:** Aggregation
- Count co-occurrences (episodes)
- Count vehicles with both DTCs
- Find latest occurrence

**Performance Note:** Complex co-occurrence on large datasets may be slow; current limit is 200 DTCs per vehicle.

---

## 8. Formulas & Calculations

### 8.1 Health Score Calculation

**Fleet Health Score:**
$$\text{fleet\_health\_score} = 100 - \min\left(\frac{\text{avg\_risk\_per\_active\_vehicle} \times K}{1}, 100\right)$$

**Vehicle Health Score:**
$$\text{vehicle\_health\_score} = \max\left(0, 100 - \min(\text{risk\_active} \times K, 100)\right)$$

Where:
- $K$ = Scaling constant (default 0.2, from `HEALTH_SCALING_CONSTANT`)
- `risk_active` = Sum of risk from all unresolved episodes for vehicle/fleet
- Score range: **0** (worst) to **100** (best)

### 8.2 Risk Score Calculation (Per-Vehicle from Active Episodes)

$$\text{risk\_per\_vehicle} = \sum_{\text{active episodes}} \left( S_i \times D_i \times R_i \right)$$

**Components:**

#### Severity Multiplier ($S_i$)
$$S_i = \begin{cases}
1.0 & \text{if severity\_level} \le 1 \text{ (Info)} \\
3.0 & \text{if severity\_level} = 2 \text{ (Minor)} \\
7.0 & \text{if severity\_level} = 3 \text{ (Major)} \\
12.0 & \text{if severity\_level} \ge 4 \text{ (Critical)}
\end{cases}$$

#### Duration Factor ($D_i$)
$$D_i = 1.0 + \min\left(\frac{\text{resolution\_time\_seconds}}{86400}, 30\right) / 10.0$$

Where:
- Minimum: 1.0 (for just-occurred faults)
- Maximum: 1.0 + 30/10 = 4.0 (capped at 30 days)
- Example: 1-day fault = $1.0 + 1/10 = 1.1$

#### Recurrence Factor ($R_i$)
$$R_i = 1.0 + \max(0, \text{episode\_count} - 1) \times 0.5$$

Where:
- First episode: $1.0 + 0 \times 0.5 = 1.0$
- Second episode: $1.0 + 1 \times 0.5 = 1.5$
- Fifth episode: $1.0 + 4 \times 0.5 = 3.0$

**Example Calculation:**
```
Vehicle V1 with 2 active episodes:
- Episode 1: P0128, severity=3, duration=1 day, count=1
  Risk = 7.0 × 1.1 × 1.0 = 7.7
  
- Episode 2: P0171, severity=2, duration=0.5 day, count=2
  Risk = 3.0 × 1.05 × 1.5 = 4.725

Total Risk = 7.7 + 4.725 = 12.425
Health Score = 100 - min(12.425 × 0.2, 100) = 100 - 2.485 = 97.515 ≈ 97.5
```

### 8.3 Maintenance Priority Score

$$\text{priority\_score} = S \times D \times R_{\text{month}}$$

Where:
- $S$ = Severity multiplier (same as above)
- $D$ = Duration factor (same as above)
- $R_{\text{month}}$ = $1.0 + \text{episodes\_last\_30\_days}$

**Higher score** = Higher priority for immediate attention

### 8.4 Active Fault Trend

**7-Day Trend Analysis:**
```
recent_count   = countIf(is_resolved = 0 AND event_date >= today() - 7)
prior_count    = countIf(is_resolved = 0 AND event_date >= today() - 14 AND event_date < today() - 7)

Trend = case
    when recent_count > prior_count × 1.1 + 1     then 'increasing'
    when prior_count > 0 AND recent_count < prior_count × 0.9
                                                   then 'decreasing'
    else                                                'stable'
```

**Thresholds:**
- Increasing: New faults > 10% + 1 of prior week
- Decreasing: New faults < 90% of prior week (only if prior_count > 0)
- Stable: Everything else

---

## 9. Data Flow Diagram

```
┌─────────────────────────────┐
│  EXTERNAL DATA SOURCES      │
├─────────────────────────────┤
│  aimm.dtc_data_history      │
│  (DTC records with arrays)  │
│                             │
│  aimm.engineoncycles        │
│  (Engine on/off times)      │
└──────────────┬──────────────┘
               │
               ├──→ [Populate DTC Master]
               │    ↓
               │    dtc_master_ravi_v2
               │
               ├──→ [Populate Vehicle Master]
               │    (with solutionType filter)
               │    ↓
               │    vehicle_master_ravi_v2
               │
               ├──→ [DTC Events Exploded]
               │    (ARRAY JOIN + Join to vehicles)
               │    ↓
               │    dtc_events_exploded_ravi_v2
               │
               └──→ [EPISODE DETECTION PIPELINE]
                    │
                    ├─→ _tmp_engine_ts_ravi
                    │   (engine cycle timestamps)
                    │
                    ├─→ _tmp_events_lag_ravi
                    │   (previous event timestamps)
                    │
                    ├─→ _tmp_events_break_ravi
                    │   (break flags: time gap + engine boundary)
                    │
                    ├─→ _tmp_events_episode_ravi
                    │   (cumulative episode IDs)
                    │
                    ├─→ _tmp_episode_agg_ravi
                    │   (episodes aggregated: first_ts, last_ts, count)
                    │
                    ├─→ _tmp_episode_resolved_ravi
                    │   (resolution flags + gaps)
                    │
                    ├─→ _tmp_vehicle_risk_ravi
                    │   (per-vehicle risk scores)
                    │
                    └─→ [INSERT INTO VEHICLE FAULT MASTER]
                         (join vehicle detail, DTC details, health scores)
                         ↓
                         vehicle_fault_master_ravi_v2
                         │
                         │ (Central fact table - source for all analytics)
                         │
                         ├──→ [Fleet Health Summary]
                         │    ↓
                         │    fleet_health_summary_ravi_v2
                         │    (1 row per fleet)
                         │
                         ├──→ [Fleet DTC Distribution]
                         │    ↓
                         │    fleet_dtc_distribution_ravi_v2
                         │    (1 row per fleet + DTC code)
                         │
                         ├──→ [Fleet System Health]
                         │    ↓
                         │    fleet_system_health_ravi_v2
                         │    (1 row per fleet + system)
                         │
                         ├──→ [Fleet Fault Trends]
                         │    ↓
                         │    fleet_fault_trends_ravi_v2
                         │    (1 row per fleet + date)
                         │
                         ├──→ [Vehicle Health Summary]
                         │    ↓
                         │    vehicle_health_summary_ravi_v2
                         │    (1 row per vehicle)
                         │
                         ├──→ [DTC Fleet Impact]
                         │    ↓
                         │    dtc_fleet_impact_ravi_v2
                         │    (1 row per fleet + DTC code)
                         │
                         ├──→ [Maintenance Priority]
                         │    ↓
                         │    maintenance_priority_ravi_v2
                         │    (1 row per vehicle + DTC)
                         │
                         └──→ [DTC Co-occurrence]
                              (array collection + pair generation)
                              ↓
                              dtc_cooccurrence_ravi_v2
                              (1 row per DTC pair)

┌─────────────────────────────────┐
│  8 ANALYTICS OUTPUT TABLES      │
├─────────────────────────────────┤
│ All queryable from API/Dashboard│
│ Used for reporting & insights   │
└─────────────────────────────────┘
```

---

## 10. Configuration Parameters

### Pipeline Configuration

All parameters read from environment variables at pipeline startup:

| Parameter | Env Variable | Default | Type | Impact |
|-----------|--------------|---------|------|--------|
| `episode_gap_seconds` | `EPISODE_GAP_SECONDS` | 1800 | int | Time threshold (sec) to split episodes |
| `final_day_cutoff_seconds` | `FINAL_DAY_CUTOFF_SECONDS` | 86400 | int | How far back to mark "resolved" |
| `analytics_window_days` | `ANALYTICS_WINDOW_DAYS` | 90 | int | Historical days to analyze |
| `cdc_bootstrap_days` | `CDC_BOOTSTRAP_DAYS` | 90 | int | Initial catchup on first run |
| `cdc_enabled` | `CDC_ENABLED` | true | bool | Enable incremental processing |
| `health_scaling_constant` | `HEALTH_SCALING_CONSTANT` | 0.2 | float | Risk multiplier for health score |
| `fetch_window_days` | `FETCH_WINDOW_DAYS` | 30 | int | Recent data to ingest |
| `cdc_replay_hours` | `CDC_REPLAY_HOURS` | 1 | int | CDC replay buffer (hours) |

### Data Sources Configuration

| Source | Env Variable | Default | Purpose |
|--------|--------------|---------|---------|
| `DTC_HISTORY_TABLE` | `DTC_HISTORY_TABLE` | `aimm.dtc_data_history` | Raw DTC records |
| `ENGINE_CYCLES_TABLE` | `ENGINE_CYCLES_TABLE` | `aimm.engineoncycles` | Engine boundaries |

### ClickHouse Settings

Default SQL execution settings:
```python
sql_settings = {
    'max_execution_time': 900,           # 15 minutes timeout
    'max_memory_usage': 20_000_000_000   # 20 GB max memory
}

# For co-occurrence (more intensive):
sql_settings = {
    'max_execution_time': 1800,          # 30 minutes
    'max_memory_usage': 20_000_000_000
}
```

### Vehicle Filtering

Only vehicles with `solutionType` IN:
- `'obd_solution'`
- `'obd_analog_solution'`
- `'obd_fuel+fuel_solution'`

---

## 11. `is_resolved` - All Possible Combinations (Practical Examples)

### Setup: Current Pipeline Run
```
Current Date/Time: 2026-03-24 10:00 AM
Cutoff Threshold: 2026-03-23 10:00 AM (1 day before)
```

---

### **Scenario 1: RESOLVED ✅** (`is_resolved = 1`)

**Condition 1:** ✅ Last event is old (before cutoff)  
**Condition 2:** ✅ Engine restarted after fault

**Example:**
```
Vehicle: Truck-001 (Volvo)
DTC Code: P0128 (Coolant Thermostat)

Timeline:
├─ 2026-03-20 14:30 → First P0128 detected
├─ 2026-03-20 15:45 → Last P0128 detected (last_ts)
├─ 2026-03-21 06:15 → ENGINE RESTARTED (engine cycle recorded)
└─ 2026-03-21 onwards → No more P0128 detected

Cutoff Check: 2026-03-20 15:45 < 2026-03-23 10:00 ? ✅ YES (3 days old)
Engine Check: 2026-03-21 06:15 > 2026-03-20 15:45 ? ✅ YES (restarted)

Result: is_resolved = 1 ✅ (RESOLVED - Problem fixed itself)
Interpretation: "The fault resolved naturally; engine restarted successfully"
```

---

### **Scenario 2: ACTIVE ❌** (`is_resolved = 0`)

**Condition 1:** ❌ Last event is TOO RECENT (after cutoff)  
**Condition 2:** ✅ Engine restarted after fault

**Example:**
```
Vehicle: Truck-002 (Volvo)
DTC Code: P0171 (System Too Lean)

Timeline:
├─ 2026-03-24 08:00 → First P0171 detected
├─ 2026-03-24 08:45 → Last P0171 detected (last_ts)
├─ 2026-03-24 09:00 → ENGINE RESTARTED (engine cycle recorded)
└─ No more P0171 after restart

Cutoff Check: 2026-03-24 08:45 < 2026-03-23 10:00 ? ❌ NO (too recent - only 1 hour old)
Engine Check: 2026-03-24 09:00 > 2026-03-24 08:45 ? ✅ YES (restarted)

Result: is_resolved = 0 ❌ (ACTIVE - Wait 1 day to confirm)
Interpretation: "Engine restarted, but not enough time passed to confirm resolution"
```

---

### **Scenario 3: ACTIVE ❌** (`is_resolved = 0`)

**Condition 1:** ✅ Last event is old (before cutoff)  
**Condition 2:** ❌ Engine NEVER restarted after fault

**Example:**
```
Vehicle: Truck-003 (Mercedes)
DTC Code: P0300 (Random Misfire)

Timeline:
├─ 2026-02-15 12:00 → First P0300 detected
├─ 2026-02-15 14:30 → Last P0300 detected (last_ts)
├─ 2026-02-16 → No P0300, but engine NEVER shut down and restarted
├─ 2026-03-10 → Still running, no engine restart cycle
└─ 2026-03-24 → Still no engine restart detected

Cutoff Check: 2026-02-15 14:30 < 2026-03-23 10:00 ? ✅ YES (37+ days old)
Engine Check: max_engine_cycle_ts > 2026-02-15 14:30 ? ❌ NO (no restart after fault)

Result: is_resolved = 0 ❌ (ACTIVE - Never properly cleared)
Interpretation: "Fault is old but was never cleared by engine restart. Vehicle may still have issue"
```

---

### **Scenario 4: ACTIVE ❌** (`is_resolved = 0`)

**Condition 1:** ❌ Last event is TOO RECENT (after cutoff)  
**Condition 2:** ❌ Engine NEVER restarted after fault

**Example:**
```
Vehicle: Truck-004 (Iveco)
DTC Code: P0335 (Crankshaft Position Sensor)

Timeline:
├─ 2026-03-24 09:30 → First P0335 detected
├─ 2026-03-24 09:45 → Last P0335 detected (last_ts)
├─ 2026-03-24 onwards → Engine running continuously
└─ No restart detected yet

Cutoff Check: 2026-03-24 09:45 < 2026-03-23 10:00 ? ❌ NO (too recent - 15 min ago)
Engine Check: max_engine_cycle_ts > 2026-03-24 09:45 ? ❌ NO (no restart)

Result: is_resolved = 0 ❌ (ACTIVE - Ongoing problem)
Interpretation: "Fresh fault, engine hasn't restarted. Immediate attention needed"
```

---

### **Summary: All 4 Combinations**

| Scenario | Condition 1<br/>(Time Old?) | Condition 2<br/>(Engine Restarted?) | is_resolved | Status | Meaning |
|----------|:---:|:---:|:---:|:---|:---|
| **Scenario 1** | ✅ YES | ✅ YES | **1** | ✅ RESOLVED | Fault naturally resolved; confirmed by engine restart |
| **Scenario 2** | ❌ NO | ✅ YES | **0** | ❌ ACTIVE | Engine restarted but too recent; needs more time |
| **Scenario 3** | ✅ YES | ❌ NO | **0** | ❌ ACTIVE | Old fault but never confirmed cleared; still suspicious |
| **Scenario 4** | ❌ NO | ❌ NO | **0** | ❌ ACTIVE | Fresh ongoing fault; needs immediate investigation |

---

### **Simple Visual: Decision Tree**

```
Does fault last_ts occur before cutoff?
│
├─ NO (too recent)
│  └─→ is_resolved = 0 ❌ (Wait longer)
│
└─ YES (old enough)
   │
   Did engine restart AFTER the last fault?
   │
   ├─ NO
   │  └─→ is_resolved = 0 ❌ (Still suspicious)
   │
   └─ YES
      └─→ is_resolved = 1 ✅ (CONFIRMED RESOLVED)
```

---

### **Real-World Impact: How analytics change**

**Example Fleet with 4 vehicles (the scenarios above):**

```
Vehicle     | Fault Date  | is_resolved | Include in Risk? | Health Score Impact
────────────┼─────────────┼─────────────┼─────────────────┼────────────────────
Truck-001   | 2026-02-20  | 1           | ❌ NO           | Not counted in risk
Truck-002   | 2026-03-24  | 0           | ✅ YES          | Counted in risk
Truck-003   | 2026-02-15  | 0           | ✅ YES          | Counted in risk (old & unconfirmed)
Truck-004   | 2026-03-24  | 0           | ✅ YES          | Counted in risk (fresh)

Fleet Metrics:
├─ vehicles_with_active_faults = 3 (Truck-002, 003, 004)
├─ vehicles_with_resolved_faults = 1 (Truck-001)
├─ active_fault_count = 3 (risk calculated)
└─ fleet_health_score = 100 - (avg_active_risk × 0.2)
   [Result: Lower score due to 3 active faults]
```

---

### **When Does Each Status Transition?**

```
Timeline for a single fault over time:

Day 0 (Fault detected):
   └─→ is_resolved = 0 ❌ (Fresh, engine hasn't restarted)

Day 1 evening (Engine starts):
   ├─ If engine restarted: Still is_resolved = 0 (too recent, waiting for cutoff)
   └─ If engine hasn't restarted: is_resolved = 0 (waiting for restart)

Day 2-3 (Waiting period):
   └─→ is_resolved = 0 ❌ (Engine may have restarted, but not 24 hours yet)

Day 4 morning:
   ├─ If engine restarted on Day 1: is_resolved = 1 ✅ (24+ hours passed, restart confirmed)
   └─ If no engine restart yet: is_resolved = 0 ❌ (Stays unresolved)

Day 30+ (No restart ever):
   └─→ is_resolved = 0 ❌ (Old but still unresolved - vehicle may have issue)
```

---

### **Configuration Impact**

How changing `FINAL_DAY_CUTOFF_SECONDS` affects scenarios:

```yaml
# Default (1 day = 86400 seconds)
FINAL_DAY_CUTOFF_SECONDS: 86400
└─→ Scenario 2: is_resolved=0, waits 1 day, then becomes 1

# Stricter (7 days = 604800 seconds)
FINAL_DAY_CUTOFF_SECONDS: 604800
└─→ Scenario 2: is_resolved=0, waits 7 days, then becomes 1
└─→ More faults stay "active" longer

# Lenient (6 hours = 21600 seconds)
FINAL_DAY_CUTOFF_SECONDS: 21600
└─→ Scenario 2: is_resolved=1 after just 6 hours + restart
└─→ Faults marked "resolved" faster
```

---

## Summary of All Tables

### Input Tables
- `aimm.dtc_data_history` - Raw DTC events
- `aimm.engineoncycles` - Engine cycle boundaries
- `vehicle_profile_ss` - Vehicle reference (via populate functions)

### Dimension Tables (Reference)
- `dtc_master_ravi_v2` - DTC metadata & severity
- `vehicle_master_ravi_v2` - Vehicle fleet roster

### Processing Table
- `dtc_events_exploded_ravi_v2` - Raw events with context

### Temporary Tables (Episode Detection)
- `_tmp_engine_ts_ravi`
- `_tmp_events_lag_ravi`
- `_tmp_events_break_ravi`
- `_tmp_events_episode_ravi`
- `_tmp_episode_agg_ravi`
- `_tmp_episode_resolved_ravi`
- `_tmp_vehicle_risk_ravi`

### Operational Master Table
- `vehicle_fault_master_ravi_v2` - Central fact table

### Analytics Output Tables (8 Total)
1. `fleet_health_summary_ravi_v2`
2. `fleet_dtc_distribution_ravi_v2`
3. `fleet_system_health_ravi_v2`
4. `fleet_fault_trends_ravi_v2`
5. `vehicle_health_summary_ravi_v2`
6. `dtc_fleet_impact_ravi_v2`
7. `maintenance_priority_ravi_v2`
8. `dtc_cooccurrence_ravi_v2`

---

**Document Version:** 1.0  
**Last Updated:** March 24, 2026  
**System:** DTC Analytics V2 (SQL-First, ClickHouse)
