# Data Dictionary (summary)

This document describes the key precomputed analytics tables available to the AI Analyst.

- `fleet_health_summary_ravi_v2`: Fleet-level KPIs (total_vehicles, vehicles_with_active_faults, fleet_health_score, most_common_dtc, active_fault_trend).
- `fleet_dtc_distribution_ravi_v2`: Top DTCs with `dtc_code`, `description`, `system`, `subsystem`, `vehicles_affected`, `total_occurrences`, `avg_resolution_time`.
- `fleet_system_health_ravi_v2`: Per-system summary with `system`, `vehicles_affected`, `active_faults`, `critical_faults`, `risk_score`, `trend`.
- `vehicle_health_summary_ravi_v2`: Per-vehicle snapshot with `uniqueid`, `vehicle_number`, `vehicle_health_score`, `active_fault_count`, `critical_fault_count`, `most_common_dtc`.
- `vehicle_fault_master_ravi_v2`: Operational fault episodes (episode_id, uniqueid, dtc_code, event_date, severity_level, is_resolved, occurrence_count, resolution_time_sec).
- `dtc_master_ravi_v2`: Knowledge base for DTC codes (`dtc_code`, `system`, `subsystem`, `description`, `severity_level`, `primary_cause`).

Notes:
- Prefer aggregated `fleet_*` and `vehicle_health_summary_ravi_v2` for fast answers.
- `vehicle_fault_master_ravi_v2` is allowed for advanced queries but may be large; queries will be limited and validated.