from datetime import datetime

import pytest

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.dtc_repository import DTCRepository
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.maintenance_repository import MaintenanceRepository
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor
from src.dtc_mcp.security import context_from_verified_claims
from src.dtc_mcp.vehicle_repository import VehicleRepository


class FakeClient:
    def __init__(self, rows=(), error=None):
        self.rows = list(rows)
        self.error = error
        self.calls = []

    def execute(self, query, params, settings=None):
        self.calls.append((query, params, settings))
        if self.error:
            raise self.error
        return self.rows


def tenant(ids=("101",)):
    return context_from_verified_claims({"user_id": "u", "tenant_id": "tenant", "customer_id": "customer", "allowed_customer_ids": ids, "scopes": ["dtc:fleet:read"], "request_id": "request", "trace_id": "trace"})


def executor(client, max_rows=2):
    return RepositoryExecutor(lambda: client, DTCSettings(max_result_rows=max_rows))


def test_parameters_are_bound_tenant_filter_and_limits_are_enforced():
    client = FakeClient([("P1", "desc", "engine", "fuel", 3, 2, 1, 9, 4, 1.5, 0), ("P2", "desc", "engine", "fuel", 2, 1, 0, 8, 2, 1.0, 0), ("P3", "desc", "engine", "fuel", 1, 1, 0, 7, 1, 1.0, 0)])
    result = FleetRepository(executor(client)).get_top_dtcs(tenant(), limit=99)
    query, params, settings = client.calls[0]
    assert "clientLoginId IN {tenant_ids:Array(String)}" in query
    assert params["tenant_ids"] == ["101"] and "101" not in query
    assert params["query_limit"] == 3 and settings["max_result_rows"] == 3
    assert result.metadata.row_count == 2 and result.metadata.truncated


def test_empty_results_are_successful_repository_results():
    result = FleetRepository(executor(FakeClient())).get_fleet_health_summary(tenant())
    assert result.rows == [] and result.metadata.row_count == 0 and not result.metadata.truncated


def test_clickhouse_errors_are_typed_and_do_not_log_credentials(caplog):
    client = FakeClient(error=RuntimeError("password=super-secret host=internal"))
    with pytest.raises(RepositoryError) as exc:
        FleetRepository(executor(client)).get_fleet_health_summary(tenant())
    assert exc.value.code == ErrorCode.UPSTREAM_UNAVAILABLE
    assert "super-secret" not in str(exc.value) and "super-secret" not in caplog.text


def test_vehicle_lookups_and_faults_are_tenant_scoped():
    client = FakeClient()
    repo = VehicleRepository(executor(client))
    repo.get_vehicle_health(tenant(("101", "102")), uniqueid="vehicle-x")
    repo.get_vehicle_faults(tenant(), vehicle_number="REG-1")
    for query, params, _ in client.calls:
        assert "clientLoginId IN {tenant_ids:Array(String)}" in query
        assert params["vehicle_id"] in {"vehicle-x", "REG-1"}
        assert params["vehicle_id"] not in query


def test_customer_selector_and_selected_scope_use_only_authorized_ids():
    client = FakeClient([("Acme",)])
    repo = VehicleRepository(executor(client, max_rows=500))
    names = repo.list_authorized_customer_names(tenant(("101", "202")))
    query, params, _ = client.calls[-1]
    assert names.rows == [{"customer_name": "Acme"}]
    assert params["tenant_ids"] == ["101", "202"] and "customer_name != ''" in query
    assert "vehicle_fault_master_ravi_v2" in query and "solutionType IN" in query
    assert params["obd_solution_types"] == ["obd_solution", "obd_analog_solution", "obd_fuel+fuel_solution"]

    client.rows = [("202",)]
    ids = repo.get_authorized_customer_ids(tenant(("101", "202")), "Beta")
    query, params, _ = client.calls[-1]
    assert ids.rows == [{"clientLoginId": "202"}]
    assert params["tenant_ids"] == ["101", "202"] and params["customer_name"] == "Beta"
    assert "vehicle_fault_master_ravi_v2" in query and "solutionType IN" in query
    assert "Beta" not in query


def test_customer_overview_groups_real_rows_with_server_tenant_scope():
    client = FakeClient([("Acme", 4, 2, 1, 87.5)])
    result = FleetRepository(executor(client, max_rows=200)).get_customer_overview(tenant(("101", "202")), limit=200)
    query, params, _ = client.calls[-1]
    assert result.rows == [{"customer_name": "Acme", "vehicle_count": 4, "active_fault_vehicles": 2, "critical_fault_vehicles": 1, "avg_health_score": 87.5}]
    assert "GROUP BY" in query and "clientLoginId IN {tenant_ids:Array(String)}" in query
    assert "vehicle_fault_master_ravi_v2" in query and "solutionType IN" in query
    assert params["tenant_ids"] == ["101", "202"] and params["query_limit"] == 201


def test_system_health_orders_by_grouped_alias():
    client = FakeClient([("engine", 2, 3, 1, 8.5, "stable")])
    result = FleetRepository(executor(client)).get_system_health(tenant())
    query, _, _ = client.calls[-1]
    assert result.rows == [{"system": "engine", "vehicles_affected": 2, "active_faults": 3, "critical_faults": 1, "risk_score": 8.5, "trend": "stable"}]
    assert "AS sys" in query and "GROUP BY sys" in query and "ORDER BY avg(risk_score) DESC, sys ASC" in query


def test_vehicle_fault_summary_declares_order_aliases():
    client = FakeClient([("P1", 3, 2, 4, 9)])
    result = VehicleRepository(executor(client)).get_vehicle_fault_summary(tenant(), uniqueid="vehicle-x")
    query, params, _ = client.calls[-1]
    assert result.rows == [{"dtc_code": "P1", "episode_count": 3, "active_episodes": 2, "max_severity": 4, "days_persistence": 9}]
    assert "count() AS episode_count" in query and "countIf(is_resolved = 0) AS active_episodes" in query
    assert "max(severity_level) AS max_severity" in query and "AS days_persistence" in query
    assert params["vehicle_id"] == "vehicle-x"


def test_cross_tenant_dtc_aggregation_and_maintenance_are_scoped():
    client = FakeClient()
    dtc = DTCRepository(executor(client))
    dtc.get_dtc_fleet_impact(tenant(), dtc_code="P100")
    dtc.get_dtc_cooccurrence(tenant(), dtc_code="P100")
    MaintenanceRepository(executor(client)).get_maintenance_priority(tenant())
    assert len(client.calls) == 3
    assert all("clientLoginId IN {tenant_ids:Array(String)}" in query for query, _, _ in client.calls)
    assert all(params["tenant_ids"] == ["101"] for _, params, _ in client.calls)


def test_normalizes_datetime_and_nullable_values():
    client = FakeClient([{"date": datetime(2026, 1, 2, 3, 4), "active_faults": None}])
    result = FleetRepository(executor(client)).get_fault_trends(tenant())
    assert result.rows == [{"date": "2026-01-02T03:04:00", "active_faults": None}]


@pytest.mark.integration
def test_live_repository_contract_requires_database():
    pytest.skip("Run explicitly with an approved read-only ClickHouse test identity")
