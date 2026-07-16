import os

import pytest

import src.ai_analyst as analyst
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor
from src.dtc_mcp.security import context_from_verified_claims
from src.dtc_mcp.vehicle_repository import VehicleRepository


pytestmark = pytest.mark.integration


def tenant(customer_id="101", scopes=("dtc:fleet:read", "dtc:vehicle:read")):
    return context_from_verified_claims({
        "user_id": "ci-user", "tenant_id": f"tenant-{customer_id}", "customer_id": customer_id,
        "allowed_customer_ids": [customer_id], "roles": ["analyst"], "scopes": list(scopes),
        "request_id": "ci-request", "trace_id": "ci-trace",
    })


@pytest.fixture(scope="module")
def executor():
    if os.getenv("DTC_MCP_INTEGRATION") != "1":
        pytest.skip("Set DTC_MCP_INTEGRATION=1 for the disposable ClickHouse suite")
    return RepositoryExecutor(settings=DTCSettings(cache_ttl_seconds=1))


def test_real_clickhouse_queries_are_tenant_scoped_and_empty_safe(executor):
    fleet = FleetRepository(executor)
    vehicles = VehicleRepository(executor)
    health = fleet.get_fleet_health_summary(tenant("101"))
    top = fleet.get_top_dtcs(tenant("101"), limit=10)
    vehicle = vehicles.get_vehicle_health(tenant("101"), uniqueid="vehicle-101")
    empty = vehicles.get_vehicle_health(tenant("101"), uniqueid="vehicle-202")
    assert health.rows[0]["total_vehicles"] == 4
    assert [row["dtc_code"] for row in top.rows] == ["P0123"]
    assert vehicle.rows[0]["vehicle_number"] == "TRUCK-101" and empty.rows == []
    assert "P9999" not in str(health.rows + top.rows + vehicle.rows)


def test_real_transport_timeout_and_database_unavailable_are_bounded(executor):
    timeout_executor = RepositoryExecutor(settings=DTCSettings(query_timeout_seconds=1))
    with pytest.raises(RepositoryError) as timeout:
        timeout_executor.execute(
            "SELECT sleep(2), clientLoginId FROM fleet_health_summary_ravi_v2 WHERE clientLoginId IN {tenant_ids:Array(String)} LIMIT {query_limit:UInt32}",
            parameters={"tenant_ids": ["101"]}, columns=("wait", "clientLoginId"),
            tables=("fleet_health_summary_ravi_v2",), context=tenant("101"), limit=1,
            query_type="integration_timeout", filters_applied={"tenant_scope": "server"},
        )
    assert timeout.value.code in {ErrorCode.TIMEOUT, ErrorCode.UPSTREAM_UNAVAILABLE} and timeout.value.retryable

    offline = RepositoryExecutor(lambda: (_ for _ in ()).throw(OSError("offline")), DTCSettings())
    with pytest.raises(RepositoryError) as unavailable:
        FleetRepository(offline).get_fleet_health_summary(tenant("101"))
    assert unavailable.value.code == ErrorCode.UPSTREAM_UNAVAILABLE and unavailable.value.retryable


def test_question_to_ai_to_stdio_mcp_to_clickhouse_to_evidence_to_answer(executor):
    context = tenant("101")
    response = analyst._fast_path_response(
        [{"role": "user", "content": "fleet summary"}],
        {"_dtc_tenant_context": context, "_dtc_data_access_mode": "mcp", "customer_name": "Fixture A"},
    )
    assert response["text"].startswith("Fleet health score is 80")
    assert response["tool_results"]["fast_path"]["evidence"]["query_hash"]
    assert response["token_usage"] == {"prompt": 0, "completion": 0}
