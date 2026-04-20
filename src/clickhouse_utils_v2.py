V2_TABLES = {
    "dtc_master": "dtc_master_ravi_v2",
    "vehicle_master": "vehicle_master_ravi_v2",
    "dtc_events_exploded": "dtc_events_exploded_ravi_v2",
    "vehicle_fault_master": "vehicle_fault_master_ravi_v2",
    "fleet_health_summary": "fleet_health_summary_ravi_v2",
    "fleet_dtc_distribution": "fleet_dtc_distribution_ravi_v2",
    "fleet_system_health": "fleet_system_health_ravi_v2",
    "fleet_fault_trends": "fleet_fault_trends_ravi_v2",
    "vehicle_health_summary": "vehicle_health_summary_ravi_v2",
    "dtc_fleet_impact": "dtc_fleet_impact_ravi_v2",
    "maintenance_priority": "maintenance_priority_ravi_v2",
    "dtc_cooccurrence": "dtc_cooccurrence_ravi_v2",
}


def ensure_v2_tables(_client=None):
    """No-op in API-only mode.

    Airflow owns lifecycle/DDL for these tables.
    """
    return None
