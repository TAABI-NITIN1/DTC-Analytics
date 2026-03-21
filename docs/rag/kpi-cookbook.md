# KPI Cookbook

Short guidance for the AI on what KPIs mean and when to use them.

- `fleet_health_score`: Aggregate health metric (0-100). Use for top-line summary.
- `vehicles_with_active_faults`: Count of vehicles currently with active unresolved faults. Use to prioritise maintenance.
- `critical_faults`: Faults with severity >= 3. Use for safety/urgent alerts.
- `most_common_dtc`: The DTC code most frequent in the fleet or vehicle; useful for root-cause hints.
- `avg_resolution_time`: Average time it takes to resolve episodes for a DTC; higher means longer downtime.

Recommendation:
- When asked for summaries, return `fleet_health_score` and `vehicles_with_active_faults` first.
- Only drill into `vehicle_fault_master_ravi_v2` when the user asks for episode-level detail or when `most_common_dtc` needs verification.
- Always mention if data is sampled or limited by `MAX_RESULT_ROWS`.