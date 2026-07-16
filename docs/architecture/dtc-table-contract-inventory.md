# DTC table contract inventory

Source of truth for this snapshot: live read-only ClickHouse `DESCRIBE TABLE` output captured on 2026-07-15, reconciled with `src/clickhouse_utils_v2.py`, `src/schema_registry.py`, `docs/rag/data-dictionary.md`, and query projections.

| Logical table | Physical table | Grain / confirmed key columns | Customer key and isolation consequence |
|---|---|---|---|
| DTC master/intelligence | `dtc_master_ravi_v2` | One row per `dtc_code`; `system`, `subsystem`, `description`, causes, symptoms, severity/safety, action/repair/prevention/driver fields | Global reference data; no customer key. Safe only when joined to scoped facts or used for a requested DTC definition. |
| Vehicle master | `vehicle_master_ravi_v2` | Vehicle identity/profile: `clientLoginId`, `uniqueid`, `vehicle_number`, `customer_name`, model/year/type/solution | `customer_name` is the human/customer scope; `clientLoginId` is the mapping key used by some aggregates. Authorization still requires trusted identity. |
| DTC event fact | `dtc_events_exploded_ravi_v2` | Atomic event: `clientLoginId`, `uniqueid`, `vehicle_number`, `ts`, `dtc_code`, geo/PGN | Native `clientLoginId`; customer scope requires a verified vehicle mapping/join. |
| Vehicle fault master | `vehicle_fault_master_ravi_v2` | Episode fact: `episode_id`, `clientLoginId`, `uniqueid`, `vehicle_number`, `customer_name`, DTC/system/severity/timestamps/count/resolution/flags | Native `customer_name` and `clientLoginId`; preferred customer-scoped fact source, but every query must enforce scope. |
| Fleet health summary | `fleet_health_summary_ravi_v2` | Fleet snapshot: totals, active/critical/driver-related counts, score, common DTC/system, trend | Native `clientLoginId`, verified live. Map authenticated tenant/customer to allowed client IDs. |
| Fleet DTC distribution | `fleet_dtc_distribution_ravi_v2` | DTC aggregate: DTC/description/system/severity, vehicles/active/occurrences/episodes/resolution/driver counts | Native `clientLoginId`, verified live. Current AI recomputation is not required merely for tenancy. |
| Fleet system health | `fleet_system_health_ravi_v2` | Per-system vehicles, active/critical faults, risk score, trend | Native `clientLoginId`, verified live. |
| Fleet fault trends | `fleet_fault_trends_ravi_v2` | Daily `date`, active/new/resolved/driver-related faults, health score | Native `clientLoginId`, verified live. |
| Vehicle health summary | `vehicle_health_summary_ravi_v2` | One row per `uniqueid`; vehicle/customer, score/counts/episodes/resolution/last fault/common DTC/system flags | Native `customer_name`; main vehicle/customer mapping for repository queries. |
| Maintenance priority | `maintenance_priority_ravi_v2` | Vehicle+DTC priority: `uniqueid`, `vehicle_number`, DTC/description/severity/duration/episodes/score/action | Native `clientLoginId`, verified live; ownership joins remain useful as defense in depth. |
| DTC fleet impact | `dtc_fleet_impact_ravi_v2` | One row per client+DTC: system/subsystem, affected/active vehicles, resolution, driver ratio, risk | Native `clientLoginId`, verified live. Current GraphQL resolver fails to apply it. |
| DTC co-occurrence | `dtc_cooccurrence_ravi_v2` | One row per client+DTC pair, count, affected vehicles, time gap, last seen | Native `clientLoginId`, verified live. Current GraphQL resolver fails to apply it. |

## Tenant key conclusion

The deployed repository-level tenant discriminators are `customer_name` on vehicle/fault/vehicle-health tables and `clientLoginId` on every tenant-bearing fact and aggregate table. Neither value is trustworthy when supplied by the frontend or model. Production identity must map an authenticated immutable tenant/customer ID to permitted `customer_name` and `clientLoginId` values on the server.

## Live deployed columns

- `vehicle_fault_master_ravi_v2`: `episode_id`, `clientLoginId`, `uniqueid`, `vehicle_number`, `customer_name`, `model`, `manufacturing_year`, `dtc_code`, `system`, `subsystem`, `description`, `severity_level`, `first_ts`, `last_ts`, `event_date`, `event_date_ist`, `occurrence_count`, `resolution_time_sec`, `is_resolved`, `resolution_reason`, `gap_from_previous_episode`, `engine_cycles_during`, `driver_related`, `has_engine_issue`, `has_coolant_issue`, `has_safety_issue`, `has_emission_issue`, `has_electrical_issue`, `vehicle_health_score`, `created_at`, `updated_at`, `vp_created_at`, `vp_updated_at`, `engine_created_at`, `engine_updated_at`, `dtc_created_at`, `dtc_updated_at`.
- `fleet_health_summary_ravi_v2`: `clientLoginId`, `total_vehicles`, `vehicles_with_active_faults`, `vehicles_with_critical_faults`, `driver_related_faults`, `fleet_health_score`, `most_common_dtc`, `most_common_system`, `active_fault_trend`, and lineage timestamps.
- `fleet_fault_trends_ravi_v2`: `clientLoginId`, `date`, `active_faults`, `critical_faults`, `new_faults`, `resolved_faults`, `driver_related_faults`, `fleet_health_score`, and lineage timestamps.
- `maintenance_priority_ravi_v2`: `clientLoginId`, `uniqueid`, `vehicle_number`, `dtc_code`, `description`, `severity_level`, `fault_duration_sec`, `episodes_last_30_days`, `maintenance_priority_score`, `recommended_action`, and lineage timestamps.
- `dtc_cooccurrence_ravi_v2`: `clientLoginId`, `dtc_code_a`, `dtc_code_b`, `cooccurrence_count`, `vehicles_affected`, `avg_time_gap_sec`, `last_seen_ts`, and lineage timestamps.
- `dtc_fleet_impact_ravi_v2`: `clientLoginId`, `dtc_code`, `system`, `subsystem`, `vehicles_affected`, `active_vehicles`, `avg_resolution_time`, `driver_related_ratio`, `fleet_risk_score`, and lineage timestamps.
- `dtc_master_ravi_v2`: `dtc_code`, `system`, `subsystem`, `description`, `primary_cause`, `secondary_causes`, `symptoms`, `impact_if_unresolved`, `fuel_mileage_impact`, `vehicle_health_impact`, `severity_level`, `safety_risk_level`, `action_required`, `repair_complexity`, `estimated_repair_hours`, `driver_related`, `driver_behaviour_category`, `driver_behaviour_trigger`, `driver_training_required`, `fleet_management_action`, `recommended_preventive_action`, `oem_specific`, `manufacturer_notes`, and lineage timestamps.
- `vehicle_master_ravi_v2`: `clientLoginId`, `uniqueid`, `vehicle_number`, `customer_name`, `model`, `manufacturing_year`, `vehicle_type`, `solutionType`, and lineage timestamps.
- `vehicle_health_summary_ravi_v2`: `clientLoginId`, `uniqueid`, `vehicle_number`, `customer_name`, `active_fault_count`, `critical_fault_count`, `total_episodes`, `episodes_last_30_days`, `avg_resolution_time`, `last_fault_ts`, `vehicle_health_score`, `driver_related_faults`, `most_common_dtc`, `has_engine_issue`, `has_emission_issue`, `has_safety_issue`, `has_electrical_issue`, and lineage timestamps.

## Contract discrepancies to resolve before implementation

- `src/schema_registry.py` and the RAG data dictionary mark several aggregate customer filters as unavailable, but live deployed tables consistently contain `clientLoginId`; these documentation sources must be corrected during repository implementation.
- GraphQL uses `clientLoginId` for some aggregates but omits it for deployed `dtc_fleet_impact` and `dtc_cooccurrence` tables.
- The Airflow DAG imports `src.analytics_pipeline_v2_sql`, which is absent from this repository snapshot, so producer DDL/versioning logic cannot be inspected here.
- Repository implementation must retain a non-secret startup/health compatibility check so future producer schema drift fails safely.
