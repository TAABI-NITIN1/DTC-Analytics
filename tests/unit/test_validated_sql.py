import pytest

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError, RepositoryExecutor
from src.dtc_mcp.security import context_from_verified_claims
from src.dtc_mcp.validated_sql import SQLPolicy, ValidatedSQLInput, ValidatedSQLService


def tenant():
    return context_from_verified_claims({"user_id": "u", "tenant_id": "t", "customer_id": "c", "allowed_customer_ids": ["101"], "scopes": ["dtc:sql:execute"], "request_id": "r", "trace_id": "trace"})


def policy(enabled=True):
    return SQLPolicy(DTCSettings(dynamic_sql_enabled=enabled, max_result_rows=25, max_lookback_days=90))


def values(sql, reason="custom severity cohort analysis", maximum_rows=None):
    return ValidatedSQLInput(question_or_reason=reason, sql=sql, maximum_rows=maximum_rows)


def test_disabled_by_default_and_domain_tool_first():
    with pytest.raises(RepositoryError) as disabled:
        policy(False).validate(values("SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level = 3 ORDER BY dtc_code"), tenant())
    assert disabled.value.code == ErrorCode.FORBIDDEN
    with pytest.raises(RepositoryError) as domain:
        policy().validate(values("SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level = 3 ORDER BY dtc_code", reason="show top dtc ranking"), tenant())
    assert domain.value.code == ErrorCode.QUERY_REJECTED


def test_ast_injects_tenant_scope_and_enforces_lower_limit():
    query = policy().validate(values("SELECT dtc_code, count() AS n FROM vehicle_fault_master_ravi_v2 WHERE severity_level >= 3 GROUP BY dtc_code ORDER BY n DESC LIMIT 99999", maximum_rows=10), tenant())
    assert "clientLoginId IN {tenant_ids: Array(String)}" in query.sql
    assert "101" not in query.sql and query.parameters == {"tenant_ids": ["101"]}
    assert query.sql.endswith("LIMIT 11") and query.limit == 10


def test_safe_cte_is_scoped_at_the_physical_source():
    query = policy().validate(values("WITH x AS (SELECT dtc_code, count() AS n FROM vehicle_fault_master_ravi_v2 WHERE severity_level >= 2 GROUP BY dtc_code) SELECT dtc_code,n FROM x WHERE n > 1 ORDER BY n DESC"), tenant())
    assert query.sql.count("clientLoginId") == 1


@pytest.mark.parametrize("sql", [
    "SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=1; SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=2",
    "SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=1 -- hidden\n ORDER BY dtc_code",
    "SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=1 UNION ALL SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=2",
    "SELECT dtc_code FROM system.tables WHERE name='x' ORDER BY dtc_code",
    "SELECT dtc_code FROM url('https://attacker') WHERE dtc_code='x' ORDER BY dtc_code",
    "SELECT clientLoginId,dtc_code FROM vehicle_fault_master_ravi_v2 WHERE clientLoginId='other' ORDER BY dtc_code",
    "SELECT dtc_code FROM vehicle_fault_master_ravi_v2 WHERE clientLoginId='other' OR severity_level=3 ORDER BY dtc_code",
    "SELECT * FROM vehicle_fault_master_ravi_v2 WHERE severity_level=3 ORDER BY dtc_code",
    "SELECT dtc_code FROM vehicle_fault_master_ravi_v2 WHERE severity_level=3",
    "SELECT dtc_code FROM vehicle_fault_master_ravi_v2 WHERE event_date >= today()-9999 ORDER BY dtc_code",
    "SELECT a.dtc_code FROM vehicle_fault_master_ravi_v2 a CROSS JOIN dtc_master_ravi_v2 b WHERE a.severity_level=3 ORDER BY a.dtc_code",
    "WITH x AS (DELETE FROM vehicle_fault_master_ravi_v2) SELECT dtc_code FROM x WHERE dtc_code='P1' ORDER BY dtc_code",
    "SELECT unknown_column FROM vehicle_fault_master_ravi_v2 WHERE severity_level=3 ORDER BY unknown_column",
    "SELECT dtc_code FROM `SYSTEM`.tables WHERE name='x' ORDER BY dtc_code",
    "SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=3 ORDER BY dtc_code SETTINGS max_threads=100",
    "SELECT dictGet('secret','value',dtc_code) FROM dtc_master_ravi_v2 WHERE severity_level=3 ORDER BY dtc_code",
    "SELECT dtc_code FROM (SELECT dtc_code FROM (SELECT dtc_code FROM (SELECT dtc_code FROM (SELECT dtc_code FROM dtc_master_ravi_v2 WHERE severity_level=3) a WHERE dtc_code='P1') b WHERE dtc_code='P1') c WHERE dtc_code='P1') d WHERE dtc_code='P1' ORDER BY dtc_code",
])
def test_adversarial_queries_fail_closed(sql):
    with pytest.raises(RepositoryError) as exc:
        policy().validate(values(sql), tenant())
    assert exc.value.code in {ErrorCode.QUERY_REJECTED, ErrorCode.SCOPE_VIOLATION}


class DynamicClient:
    def __init__(self):
        self.call = None

    def query_df(self, query, params, settings=None):
        self.call = (query, params, settings)
        return ["dtc_code", "n"], [("P1", 4), ("P2", 2), ("P3", 1)]


def test_dynamic_output_is_bounded_and_returns_only_hash_evidence():
    client = DynamicClient()
    settings = DTCSettings(dynamic_sql_enabled=True, max_result_rows=2)
    service = ValidatedSQLService(RepositoryExecutor(lambda: client, settings), SQLPolicy(settings))
    result = service.run(values("SELECT dtc_code, count() AS n FROM vehicle_fault_master_ravi_v2 WHERE severity_level >= 3 GROUP BY dtc_code ORDER BY n DESC"), tenant())
    assert result.metadata.truncated and result.metadata.row_count == 2
    assert result.rows == [{"dtc_code": "P1", "n": 4}, {"dtc_code": "P2", "n": 2}]
    assert "sql" not in result.evidence.model_dump() and len(result.evidence.query_hash) == 64
