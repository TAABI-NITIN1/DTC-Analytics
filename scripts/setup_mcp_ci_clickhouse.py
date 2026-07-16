"""Create disposable, schema-compatible MCP fixtures in the CI ClickHouse service."""

import os

import clickhouse_connect


def main() -> None:
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_ADMIN_HOST", "127.0.0.1"),
        port=int(os.getenv("CLICKHOUSE_ADMIN_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_ADMIN_USER", "default"),
        password=os.getenv("CLICKHOUSE_ADMIN_PASSWORD", ""),
        database="default",
    )
    statements = [
        "DROP TABLE IF EXISTS fleet_health_summary_ravi_v2",
        "DROP TABLE IF EXISTS fleet_dtc_distribution_ravi_v2",
        "DROP TABLE IF EXISTS vehicle_health_summary_ravi_v2",
        """CREATE TABLE fleet_health_summary_ravi_v2 (
            clientLoginId String, total_vehicles UInt32, vehicles_with_active_faults UInt32,
            vehicles_with_critical_faults UInt32, driver_related_faults UInt32,
            fleet_health_score Float64, most_common_dtc String, most_common_system String,
            active_fault_trend String
        ) ENGINE = Memory""",
        """CREATE TABLE fleet_dtc_distribution_ravi_v2 (
            clientLoginId String, dtc_code String, description String, system String, subsystem String,
            severity_level UInt8, vehicles_affected UInt32, active_vehicles UInt32,
            total_occurrences UInt32, total_episodes UInt32, avg_resolution_time Float64,
            driver_related_count UInt32
        ) ENGINE = Memory""",
        """CREATE TABLE vehicle_health_summary_ravi_v2 (
            clientLoginId String, uniqueid String, vehicle_number String, customer_name String,
            active_fault_count UInt32, critical_fault_count UInt32, total_episodes UInt32,
            episodes_last_30_days UInt32, avg_resolution_time Float64, last_fault_ts DateTime,
            vehicle_health_score Float64, driver_related_faults UInt32, most_common_dtc String,
            has_engine_issue UInt8, has_emission_issue UInt8, has_safety_issue UInt8,
            has_electrical_issue UInt8
        ) ENGINE = Memory""",
        """INSERT INTO fleet_health_summary_ravi_v2 VALUES
            ('101', 4, 2, 1, 1, 80, 'P0123', 'engine', 'stable'),
            ('202', 9, 8, 7, 6, 20, 'P9999', 'brakes', 'worsening')""",
        """INSERT INTO fleet_dtc_distribution_ravi_v2 VALUES
            ('101', 'P0123', 'Sensor fault', 'engine', 'fuel', 3, 2, 1, 5, 2, 60, 1),
            ('202', 'P9999', 'Other tenant fault', 'brakes', 'abs', 5, 9, 8, 99, 20, 900, 8)""",
        """INSERT INTO vehicle_health_summary_ravi_v2 VALUES
            ('101', 'vehicle-101', 'TRUCK-101', 'Fixture A', 2, 1, 4, 2, 30, now(), 75, 1, 'P0123', 1, 0, 0, 0),
            ('202', 'vehicle-202', 'TRUCK-202', 'Fixture B', 8, 7, 20, 10, 900, now(), 10, 7, 'P9999', 0, 0, 1, 0)""",
        "CREATE USER IF NOT EXISTS dtc_ci IDENTIFIED WITH plaintext_password BY 'test-only'",
        "GRANT SELECT ON default.* TO dtc_ci",
    ]
    for statement in statements:
        client.command(statement)


if __name__ == "__main__":
    main()
