# Data Dictionary (RAG-ready, detailed)

This document defines the operational meaning, grain, join paths, and query usage of all major tables used by the AI Analyst.

## Global Query Rules

1. Prefer aggregated analytical tables first for user-facing answers.
2. Use raw fact tables only when aggregated tables cannot answer the question.
3. Apply customer_name filter whenever customer context is available.
4. Use limited projection and avoid select star.
5. For trend questions, prefer date-grain tables over reconstructing time series from raw episodes.

## Master and Reference Tables

### dtc_master_ravi_v2
- Purpose: Canonical DTC knowledge base for definitions and repair guidance.
- Grain: One row per dtc_code.
- Primary key: dtc_code.
- Important columns:
	- dtc_code
	- system, subsystem
	- description
	- primary_cause, secondary_causes
	- symptoms
	- impact_if_unresolved
	- severity_level, safety_risk_level
	- action_required, repair_complexity
	- fleet_management_action, recommended_preventive_action
	- driver_related, driver_behaviour_category, driver_training_required
- Typical joins:
	- vehicle_fault_master_ravi_v2.dtc_code = dtc_master_ravi_v2.dtc_code
	- fleet_dtc_distribution_ravi_v2.dtc_code = dtc_master_ravi_v2.dtc_code
- Best use:
	- DTC meaning, cause, risk, action explanations.

### vehicle_master_ravi_v2
- Purpose: Vehicle dimension and profile attributes.
- Grain: One row per uniqueid or per active vehicle identity record.
- Primary identifiers: uniqueid, vehicle_number.
- Important columns:
	- clientLoginId
	- uniqueid
	- vehicle_number
	- customer_name
	- model, manufacturing_year
	- vehicle_type, solutionType
- Typical joins:
	- vehicle_health_summary_ravi_v2.uniqueid = vehicle_master_ravi_v2.uniqueid
	- vehicle_fault_master_ravi_v2.uniqueid = vehicle_master_ravi_v2.uniqueid
- Best use:
	- Vehicle metadata enrichment and fleet segmentation.

## Raw and Event-Level Fact Tables

### vehicle_fault_master_ravi_v2
- Purpose: Core fault episode fact table.
- Grain: One row per fault episode.
- Important columns:
	- episode_id
	- clientLoginId, uniqueid, vehicle_number, customer_name
	- dtc_code, system, subsystem, description
	- severity_level
	- first_ts, last_ts, event_date
	- occurrence_count
	- resolution_time_sec
	- is_resolved
	- driver_related
	- vehicle_health_score
	- has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue
- Best use:
	- Deep investigations, root-cause patterns, customer-specific drilldowns.
- Cautions:
	- Potentially large; always use filters and/or group by.

### dtc_events_exploded_ravi_v2
- Purpose: Atomic DTC event stream.
- Grain: One row per DTC event occurrence.
- Important columns:
	- clientLoginId, uniqueid, vehicle_number
	- ts
	- dtc_code
	- lat, lng
	- dtc_pgn
- Best use:
	- Event chronology and geo-temporal investigation.
- Cautions:
	- High cardinality; avoid broad scans for regular user Q and A.

## Fleet-Level Aggregated Analytical Tables

### fleet_health_summary_ravi_v2
- Purpose: Fleet KPI snapshot.
- Grain: Fleet snapshot record.
- Important columns:
	- total_vehicles
	- vehicles_with_active_faults
	- vehicles_with_critical_faults
	- driver_related_faults
	- fleet_health_score
	- most_common_dtc
	- most_common_system
	- active_fault_trend
- Best use:
	- Fleet overview and executive KPI answers.

### fleet_dtc_distribution_ravi_v2
- Purpose: Fleet-wide DTC distribution.
- Grain: One row per dtc_code in fleet aggregate.
- Important columns:
	- dtc_code, description, system, subsystem
	- severity_level
	- vehicles_affected, active_vehicles
	- total_occurrences, total_episodes
	- avg_resolution_time
	- driver_related_count
- Best use:
	- Top DTC ranking and impact distributions.

### fleet_system_health_ravi_v2
- Purpose: Risk and fault concentration by system.
- Grain: One row per vehicle system.
- Important columns:
	- system
	- vehicles_affected
	- active_faults
	- critical_faults
	- risk_score
	- trend
- Best use:
	- System-level risk prioritization.

### fleet_fault_trends_ravi_v2
- Purpose: Daily fleet trend series.
- Grain: One row per day.
- Important columns:
	- date
	- active_faults
	- new_faults
	- resolved_faults
	- driver_related_faults
	- fleet_health_score
- Best use:
	- Trend questions and period-over-period analysis.

## Vehicle and Maintenance Analytical Tables

### vehicle_health_summary_ravi_v2
- Purpose: Vehicle-level health summary and risk flags.
- Grain: One row per vehicle.
- Important columns:
	- uniqueid, vehicle_number, customer_name
	- vehicle_health_score
	- active_fault_count, critical_fault_count
	- total_episodes, episodes_last_30_days
	- avg_resolution_time
	- last_fault_ts
	- driver_related_faults
	- most_common_dtc
	- has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue
- Best use:
	- Vehicle status checks and maintenance triage context.

### maintenance_priority_ravi_v2
- Purpose: Pre-ranked maintenance urgency.
- Grain: Vehicle and dtc_code priority item.
- Important columns:
	- uniqueid, vehicle_number
	- dtc_code, description, severity_level
	- fault_duration_sec
	- episodes_last_30_days
	- maintenance_priority_score
	- recommended_action
- Best use:
	- Action planning and urgency prioritization.

## DTC Impact and Correlation Analytical Tables

### dtc_fleet_impact_ravi_v2
- Purpose: Fleet impact ranking by DTC.
- Grain: One row per dtc_code.
- Important columns:
	- dtc_code, system, subsystem
	- vehicles_affected, active_vehicles
	- avg_resolution_time
	- driver_related_ratio
	- fleet_risk_score
- Best use:
	- Fleet risk concentration and top-impact codes.

### dtc_cooccurrence_ravi_v2
- Purpose: DTC pair co-occurrence analytics.
- Grain: One row per dtc_code_a and dtc_code_b pair.
- Important columns:
	- dtc_code_a, dtc_code_b
	- cooccurrence_count
	- vehicles_affected
	- avg_time_gap_sec
	- last_seen_ts
- Best use:
	- Fault correlation and causal hypothesis generation.

## AI Observability Tables (ai_obs_*)

These tables store runtime and evaluation telemetry for model quality monitoring.

### ai_obs_requests
- One row per request.
- Key fields:
	- request_id, ts, query, intent
	- customer_name, mode
	- final_score and judge dimensions
	- root_cause, tokens, versions
	- planner_events_count and planner_error_count

### ai_obs_nodes
- One row per executed node per request.
- Key fields:
	- request_id, node, status, duration_sec
	- customer_name, mode
	- metrics_json, input_summary_json, output_summary_json

### ai_obs_sql_events
- One row per SQL execution event.
- Key fields:
	- request_id, node, tool
	- customer_name, mode
	- success, row_count, duration_sec, query, error

### ai_obs_sql_planner_events
- One row per planner-generated SQL event.
- Key fields:
	- request_id, tool_call_id, tool
	- customer_name, mode
	- generated_by, fix_attempts, success
	- selected_tables_json, generated_query, error

### ai_obs_node_scores
- One row per request and node score.
- Key fields:
	- request_id, node, customer_name, mode, score

### ai_obs_prompt_scores
- One row per request-node mapped prompt score.
- Key fields:
	- request_id, prompt_key, prompt_version, node
	- customer_name, mode
	- node_score, final_score, root_cause

### ai_obs_prompt_versions
- One row per request and prompt version record.
- Key fields:
	- request_id, prompt_key, prompt_version
	- customer_name, mode, intent
	- env and release metadata

### ai_obs_prompt_inventory
- Prompt inventory reference table.
- Key fields:
	- prompt_key
	- prompt
	- prompt_version

## Recommended Query Strategy for AI Analyst

1. Fleet KPI or trends:
	- Start with fleet_health_summary_ravi_v2 and fleet_fault_trends_ravi_v2.
2. Vehicle health question:
	- Start with vehicle_health_summary_ravi_v2.
	- Use vehicle_fault_master_ravi_v2 only for deep drilldown.
3. DTC meaning and recommendations:
	- Join analytical DTC result with dtc_master_ravi_v2.
4. Root-cause patterns:
	- Use dtc_cooccurrence_ravi_v2 plus impacted fleet metrics.
5. Quality debugging:
	- Use ai_obs_requests, ai_obs_nodes, ai_obs_sql_events, ai_obs_prompt_scores.