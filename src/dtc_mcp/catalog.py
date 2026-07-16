from __future__ import annotations

from src.clickhouse_utils_v2 import V2_TABLES


LINEAGE = ("created_at", "updated_at", "vp_created_at", "vp_updated_at", "engine_created_at", "engine_updated_at", "dtc_created_at", "dtc_updated_at")

APPROVED_COLUMNS: dict[str, tuple[str, ...]] = {
    V2_TABLES["dtc_master"]: ("dtc_code", "system", "subsystem", "description", "primary_cause", "secondary_causes", "symptoms", "impact_if_unresolved", "fuel_mileage_impact", "vehicle_health_impact", "severity_level", "safety_risk_level", "action_required", "repair_complexity", "estimated_repair_hours", "driver_related", "driver_behaviour_category", "driver_behaviour_trigger", "driver_training_required", "fleet_management_action", "recommended_preventive_action", "oem_specific", "manufacturer_notes") + LINEAGE,
    V2_TABLES["vehicle_master"]: ("clientLoginId", "uniqueid", "vehicle_number", "customer_name", "model", "manufacturing_year", "vehicle_type", "solutionType") + LINEAGE,
    V2_TABLES["dtc_events_exploded"]: ("clientLoginId", "uniqueid", "vehicle_number", "ts", "dtc_code", "lat", "lng", "dtc_pgn", "created_at", "vp_created_at", "vp_updated_at", "engine_created_at", "engine_updated_at", "dtc_created_at", "dtc_updated_at"),
    V2_TABLES["vehicle_fault_master"]: ("episode_id", "clientLoginId", "uniqueid", "vehicle_number", "customer_name", "model", "manufacturing_year", "dtc_code", "system", "subsystem", "description", "severity_level", "first_ts", "last_ts", "event_date", "event_date_ist", "occurrence_count", "resolution_time_sec", "is_resolved", "resolution_reason", "gap_from_previous_episode", "engine_cycles_during", "driver_related", "has_engine_issue", "has_coolant_issue", "has_safety_issue", "has_emission_issue", "has_electrical_issue", "vehicle_health_score") + LINEAGE,
    V2_TABLES["fleet_health_summary"]: ("clientLoginId", "total_vehicles", "vehicles_with_active_faults", "vehicles_with_critical_faults", "driver_related_faults", "fleet_health_score", "most_common_dtc", "most_common_system", "active_fault_trend") + LINEAGE,
    V2_TABLES["fleet_dtc_distribution"]: ("clientLoginId", "dtc_code", "description", "system", "subsystem", "severity_level", "vehicles_affected", "active_vehicles", "total_occurrences", "total_episodes", "avg_resolution_time", "driver_related_count") + LINEAGE,
    V2_TABLES["fleet_system_health"]: ("clientLoginId", "system", "vehicles_affected", "active_faults", "critical_faults", "risk_score", "trend") + LINEAGE,
    V2_TABLES["fleet_fault_trends"]: ("clientLoginId", "date", "active_faults", "critical_faults", "new_faults", "resolved_faults", "driver_related_faults", "fleet_health_score") + LINEAGE,
    V2_TABLES["vehicle_health_summary"]: ("clientLoginId", "uniqueid", "vehicle_number", "customer_name", "active_fault_count", "critical_fault_count", "total_episodes", "episodes_last_30_days", "avg_resolution_time", "last_fault_ts", "vehicle_health_score", "driver_related_faults", "most_common_dtc", "has_engine_issue", "has_emission_issue", "has_safety_issue", "has_electrical_issue") + LINEAGE,
    V2_TABLES["dtc_fleet_impact"]: ("clientLoginId", "dtc_code", "system", "subsystem", "vehicles_affected", "active_vehicles", "avg_resolution_time", "driver_related_ratio", "fleet_risk_score") + LINEAGE,
    V2_TABLES["maintenance_priority"]: ("clientLoginId", "uniqueid", "vehicle_number", "dtc_code", "description", "severity_level", "fault_duration_sec", "episodes_last_30_days", "maintenance_priority_score", "recommended_action") + LINEAGE,
    V2_TABLES["dtc_cooccurrence"]: ("clientLoginId", "dtc_code_a", "dtc_code_b", "cooccurrence_count", "vehicles_affected", "avg_time_gap_sec", "last_seen_ts") + LINEAGE,
}

METRIC_DEFINITIONS = {
    "fleet_health_score": {"business_meaning": "Aggregate fleet health score where higher is healthier.", "calculation_source": "Airflow analytics pipeline", "source_table": V2_TABLES["fleet_health_summary"], "freshness": "producer checkpoint", "limitations": "Depends on the latest completed analytics run."},
    "fleet_risk_score": {"business_meaning": "Relative DTC fleet impact risk.", "calculation_source": "Airflow severity-weighted aggregation", "source_table": V2_TABLES["dtc_fleet_impact"], "freshness": "producer checkpoint", "limitations": "A prioritization score, not a probability of failure."},
    "maintenance_priority_score": {"business_meaning": "Relative ordering for maintenance attention.", "calculation_source": "Airflow maintenance ranking", "source_table": V2_TABLES["maintenance_priority"], "freshness": "producer checkpoint", "limitations": "Requires operational review before action."},
}
