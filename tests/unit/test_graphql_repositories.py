import base64
import hashlib
import hmac
import json
from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.api_server import app
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError
from src.dtc_mcp.security import context_from_verified_claims
from src.graphql_schema import schema


def result(rows):
    return SimpleNamespace(rows=rows)


class Fleet:
    def get_customer_overview(self, tenant, limit=50):
        return result([{"customer_name": "Acme", "vehicle_count": 4, "active_fault_vehicles": 2, "critical_fault_vehicles": 1, "avg_health_score": 87.5}])

    def get_fleet_health_summary(self, tenant):
        return result([{"total_vehicles": 4, "vehicles_with_active_faults": 2, "vehicles_with_critical_faults": 1, "driver_related_faults": 1, "fleet_health_score": 87.5, "most_common_dtc": "P1", "most_common_system": "engine", "active_fault_trend": "stable"}])

    def get_fleet_kpis(self, tenant, days=30):
        return result([{"total_vehicles": 4, "vehicles_with_dtcs": 2, "critical_vehicles": 1, "avg_resolution_days": 1.5, "fleet_health_score": 87.5, "maintenance_due": 1, "total_dtc_alerts": 8, "critical_alerts": 2}])

    def get_fault_trends(self, tenant, days=30):
        return result([{"date": "2026-07-15", "active_faults": 2, "new_faults": 1, "resolved_faults": 1, "driver_related_faults": 0, "fleet_health_score": 87.5}])

    def get_system_health(self, tenant):
        return result([])


class Vehicles:
    def list_authorized_customer_names(self, tenant):
        return result([{"customer_name": "Acme"}])

    def get_authorized_customer_ids(self, tenant, customer_name):
        return result([{"clientLoginId": "101"}]) if customer_name == "Acme" else result([])

    def get_vehicle_health(self, tenant, uniqueid=None, vehicle_number=None):
        return result([{"uniqueid": uniqueid, "vehicle_number": "TRUCK-1", "customer_name": "Acme", "vehicle_health_score": 80, "active_fault_count": 2, "critical_fault_count": 1, "total_episodes": 4, "episodes_last_30_days": 3, "avg_resolution_time": 60, "driver_related_faults": 0, "most_common_dtc": "P1", "has_engine_issue": True, "has_emission_issue": False, "has_safety_issue": False, "has_electrical_issue": False}])

    def get_vehicle_overview(self, tenant, uniqueid):
        return result([])

    def get_vehicle_fault_summary(self, tenant, uniqueid, days=90, limit=200):
        return result([])

    def get_vehicle_timeline_summary(self, tenant, uniqueid, days=90):
        return result([])

    def list_customer_vehicles(self, tenant):
        return result([{"uniqueid": "v1", "vehicle_number": "TRUCK-1"}])


class DTCs:
    def get_dtc_fleet_impact(self, tenant, limit=20):
        return result([{"dtc_code": "P1", "system": "engine", "subsystem": "fuel", "vehicles_affected": 2, "active_vehicles": 1, "avg_resolution_time": 60, "driver_related_ratio": 0.25, "fleet_risk_score": 7.5}])

    def get_dtc_cooccurrence(self, tenant, limit=20):
        return result([{"dtc_code_a": "P1", "dtc_code_b": "P2", "cooccurrence_count": 3, "vehicles_affected": 2, "avg_time_gap_sec": 30}])


class Maintenance:
    def get_maintenance_priority(self, tenant, limit=30):
        return result([{"uniqueid": "v1", "vehicle_number": "TRUCK-1", "dtc_code": "P1", "description": "Fault", "severity_level": 3, "fault_duration_sec": 120, "episodes_last_30_days": 2, "maintenance_priority_score": 9.0, "recommended_action": "Inspect"}])

    def get_maintenance_recommendations(self, tenant, limit=20):
        return result([])


def tenant():
    return context_from_verified_claims({"user_id": "u", "tenant_id": "tenant", "customer_id": "Acme", "allowed_customer_ids": ["101"], "scopes": ["dtc:fleet:read", "dtc:vehicle:read", "dtc:maintenance:read", "dtc:schema:read"], "request_id": "r", "trace_id": "t"})


def context(**overrides):
    repositories = {"fleet": Fleet(), "vehicles": Vehicles(), "dtcs": DTCs(), "maintenance": Maintenance()}
    repositories.update(overrides)
    return {"tenant_context": tenant(), "dtc_repositories": repositories}


def test_graphql_repository_contract_shapes():
    response = schema.execute_sync("""{
      customerOverview { customerName vehicleCount activeFaultVehicles criticalFaultVehicles avgHealthScore }
      fleetHealthSnap { totalVehicles vehiclesWithActiveFaults fleetHealthScore }
      vehicleHealthDetail(uniqueid: "v1") { uniqueid vehicleNumber customerName activeFaultCount }
      dtcFleetImpact { dtcCode vehiclesAffected fleetRiskScore }
      dtcCooccurrence { dtcCodeA dtcCodeB cooccurrenceCount }
      enhancedMaintenance { uniqueid dtcCode maintenancePriorityScore recommendedAction }
    }""", context_value=context())
    assert response.errors is None
    assert set(response.data) == {"customerOverview", "fleetHealthSnap", "vehicleHealthDetail", "dtcFleetImpact", "dtcCooccurrence", "enhancedMaintenance"}
    assert response.data["customerOverview"] == [{"customerName": "Acme", "vehicleCount": 4, "activeFaultVehicles": 2, "criticalFaultVehicles": 1, "avgHealthScore": 87.5}]
    assert response.data["fleetHealthSnap"] == {"totalVehicles": 4, "vehiclesWithActiveFaults": 2, "fleetHealthScore": 87.5}
    assert response.data["vehicleHealthDetail"]["customerName"] == "Acme"


def test_graphql_tenant_filter_and_customer_enumeration_are_server_controlled():
    denied = schema.execute_sync('{ fleetHealthSnap(customerName: "Other") { totalVehicles } }', context_value=context())
    assert denied.errors and "FORBIDDEN" in denied.errors[0].message
    names = schema.execute_sync('{ customerNames customerVehicles(customerName: "Acme") { uniqueid } }', context_value=context())
    assert names.errors is None and names.data == {"customerNames": ["Acme"], "customerVehicles": [{"uniqueid": "v1"}]}


def test_multi_customer_identity_lists_names_and_narrows_selected_repository_scope():
    seen = []

    class MultiFleet(Fleet):
        def get_fleet_health_summary(self, scoped_tenant):
            seen.append(scoped_tenant)
            return super().get_fleet_health_summary(scoped_tenant)

    class MultiVehicles(Vehicles):
        def list_authorized_customer_names(self, scoped_tenant):
            return result([{"customer_name": "Acme"}, {"customer_name": "Beta"}])

        def get_authorized_customer_ids(self, scoped_tenant, customer_name):
            mapping = {"Acme": "101", "Beta": "202"}
            return result([{"clientLoginId": mapping[customer_name]}]) if customer_name in mapping else result([])

    multi = tenant().model_copy(update={"customer_id": "portal", "allowed_customer_ids": ("101", "202")})
    repositories = {"fleet": MultiFleet(), "vehicles": MultiVehicles(), "dtcs": DTCs(), "maintenance": Maintenance()}
    response = schema.execute_sync(
        '{ customerNames fleetHealthSnap(customerName: "Beta") { totalVehicles } }',
        context_value={"tenant_context": multi, "dtc_repositories": repositories},
    )
    assert response.errors is None
    assert response.data["customerNames"] == ["Acme", "Beta"]
    assert seen[-1].customer_id == "Beta" and seen[-1].allowed_customer_ids == ("202",)


def test_graphql_empty_results_preserve_nullable_and_list_contracts():
    class EmptyFleet(Fleet):
        def get_fleet_health_summary(self, tenant):
            return result([])

    class EmptyDTCs(DTCs):
        def get_dtc_fleet_impact(self, tenant, limit=20):
            return result([])

    response = schema.execute_sync('{ fleetHealthSnap { totalVehicles } dtcFleetImpact { dtcCode } }', context_value=context(fleet=EmptyFleet(), dtcs=EmptyDTCs()))
    assert response.errors is None and response.data == {"fleetHealthSnap": None, "dtcFleetImpact": []}


def test_graphql_repository_failure_is_sanitized():
    class FailedFleet(Fleet):
        def get_fleet_health_summary(self, tenant):
            raise RepositoryError(ErrorCode.UPSTREAM_UNAVAILABLE, "password=secret")

    response = schema.execute_sync('{ fleetHealthSnap { totalVehicles } }', context_value=context(fleet=FailedFleet()))
    assert response.errors and response.errors[0].message == "UPSTREAM_UNAVAILABLE"
    assert "secret" not in str(response.errors)


def test_graphql_http_uses_signed_identity_and_rejects_scope_override(monkeypatch):
    secret = "test-graphql-identity"
    monkeypatch.setenv("DTC_MCP_IDENTITY_HMAC_SECRET", secret)
    monkeypatch.setattr("src.graphql_schema._repositories", lambda info: context()["dtc_repositories"])
    claims = {"user_id": "u", "tenant_id": "tenant", "customer_id": "Acme", "allowed_customer_ids": ["101"], "scopes": ["dtc:fleet:read"], "request_id": "r", "trace_id": "t"}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    client = TestClient(app)
    headers = {"X-DTC-Identity": payload, "X-DTC-Identity-Signature": signature}
    allowed = client.post("/graphql", json={"query": "{ customerNames }"}, headers=headers)
    assert allowed.status_code == 200 and allowed.json()["data"] == {"customerNames": ["Acme"]}
    denied = client.post("/graphql", json={"query": "{ fleetHealthSnap(customerName: \"Other\") { totalVehicles } }"}, headers=headers)
    assert denied.status_code == 200 and denied.json()["errors"][0]["message"] == "FORBIDDEN"
