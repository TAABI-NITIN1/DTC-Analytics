import logging
import json

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.dtc_repository import DTCRepository
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.maintenance_repository import MaintenanceRepository
import src.dtc_mcp.observability as observability
from src.dtc_mcp.observability import METRICS, metrics_snapshot, sanitize_parameters
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor
from src.dtc_mcp.schema_repository import SchemaRepository
from src.dtc_mcp.security import ContextProvider, context_from_verified_claims
from src.dtc_mcp.tools import ToolService
from src.dtc_mcp.vehicle_repository import VehicleRepository


class Client:
    def __init__(self, error=None):
        self.error = error
        self.settings = None

    def execute(self, query, params, settings=None):
        self.settings = settings
        if self.error:
            raise self.error
        return []


def context(scopes=("dtc:fleet:read",)):
    return context_from_verified_claims({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "roles": ["analyst"], "scopes": scopes, "request_id": "request", "trace_id": "trace"})


def service(client, scopes=("dtc:fleet:read",)):
    executor = RepositoryExecutor(lambda: client, DTCSettings())
    return ToolService(ContextProvider(context(scopes)), FleetRepository(executor), VehicleRepository(executor), DTCRepository(executor), MaintenanceRepository(executor), SchemaRepository(), "stdio")


def test_success_rejection_timeout_and_failure_are_traced(caplog):
    caplog.set_level(logging.INFO, logger="dtc_mcp.audit")
    METRICS.clear()
    client = Client()
    success = service(client).get_fleet_health_summary()
    rejected = service(Client(), scopes=()).get_fleet_health_summary()
    timeout = service(Client(TimeoutError())).get_fleet_health_summary()
    failure = service(Client(RuntimeError("password=secret"))).get_fleet_health_summary()
    assert success.ok and rejected.error.code.value == "FORBIDDEN"
    assert timeout.error.code.value == "TIMEOUT" and failure.error.code.value == "UPSTREAM_UNAVAILABLE"
    assert "trace" in caplog.text and "FORBIDDEN" in caplog.text and "TIMEOUT" in caplog.text
    assert "secret" not in caplog.text
    assert client.settings["log_comment"] == "dtc_mcp trace=trace"
    snapshot = metrics_snapshot()
    assert snapshot["calls_total"] == 4 and snapshot["errors_total"] == 3
    assert {"latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "success_rate", "failure_rate"}.issubset(snapshot)


def test_audit_event_is_complete_redacted_and_correlated(caplog):
    caplog.set_level(logging.INFO, logger="dtc_mcp.audit")
    response = service(Client(), scopes=("dtc:vehicle:read",)).get_vehicle_health(type("Input", (), {"uniqueid": "secret-vehicle", "vehicle_number": None, "model_dump": lambda self: {"uniqueid": "secret-vehicle", "password": "secret"}})())
    assert response.ok
    event = json.loads(next(record.message for record in reversed(caplog.records) if record.name == "dtc_mcp.audit"))
    assert event["request_id"] == "request" and event["trace_id"] == "trace"
    assert event["tool_name"] == "get_vehicle_health" and event["database_latency_ms"] >= 0
    assert event["sanitized_parameters"]["password"] == "[REDACTED]"
    assert "secret-vehicle" not in json.dumps(event)


def test_audit_persistence_failure_does_not_fail_tool(monkeypatch):
    monkeypatch.setattr(observability._PERSISTENCE, "submit", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("logger offline")))
    assert service(Client()).get_fleet_health_summary().ok


def test_configurable_redaction(monkeypatch):
    monkeypatch.setenv("DTC_MCP_REDACT_FIELDS", "dtc_code")
    assert sanitize_parameters({"dtc_code": "P1", "limit": 10}) == {"dtc_code": "[REDACTED]", "limit": 10}


def test_rejected_sql_and_tenant_denial_emit_redacted_audit_events(caplog):
    caplog.set_level(logging.INFO, logger="dtc_mcp.audit")
    sql = "SELECT password FROM forbidden"
    result = service(Client(), scopes=("dtc:sql:execute",))._call(
        "run_validated_dtc_sql",
        "dtc:sql:execute",
        lambda tenant: (_ for _ in ()).throw(RepositoryError(ErrorCode.QUERY_REJECTED, "SQL rejected")),
        {"question_or_reason": "unusual analysis", "sql": sql},
    )
    denied = service(Client(), scopes=()).get_fleet_health_summary()
    events = [json.loads(record.message) for record in caplog.records if record.name == "dtc_mcp.audit"]
    assert result.error.code == ErrorCode.QUERY_REJECTED and denied.error.code == ErrorCode.FORBIDDEN
    assert any(event.get("error_code") == "QUERY_REJECTED" for event in events)
    assert any(event.get("error_code") == "FORBIDDEN" for event in events)
    assert sql not in json.dumps(events) and any(event["sanitized_parameters"].get("sql") == "[REDACTED]" for event in events)
