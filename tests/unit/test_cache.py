from src.dtc_mcp.cache import ResultCache
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.fleet_repository import FleetRepository
from src.dtc_mcp.repository import RepositoryExecutor
from src.dtc_mcp.security import context_from_verified_claims


ROW = (4, 2, 1, 1, 87.5, "P1", "engine", "stable")


class Redis:
    def __init__(self, fail=False):
        self.values = {}
        self.ttls = {}
        self.fail = fail

    def get(self, key):
        if self.fail:
            raise ConnectionError("redis unavailable")
        return self.values.get(key)

    def setex(self, key, ttl, value):
        if self.fail:
            raise ConnectionError("redis unavailable")
        self.values[key] = value
        self.ttls[key] = ttl


class ClickHouse:
    def __init__(self):
        self.calls = 0

    def execute(self, query, params, settings=None):
        self.calls += 1
        return [ROW]


def tenant(name="tenant-a", ids=("101",)):
    return context_from_verified_claims({"user_id": "u", "tenant_id": name, "customer_id": name, "allowed_customer_ids": ids, "scopes": ["dtc:fleet:read"], "request_id": "r", "trace_id": "t"})


def repository(redis, clickhouse, *, checkpoint="cp-1", ttl=30):
    cache = ResultCache(redis, ttl_seconds=ttl, checkpoint=checkpoint)
    return FleetRepository(RepositoryExecutor(lambda: clickhouse, DTCSettings(), cache=cache))


def test_cache_hit_ttl_and_stale_evidence():
    redis, clickhouse = Redis(), ClickHouse()
    repo = repository(redis, clickhouse, ttl=17)
    miss = repo.get_fleet_health_summary(tenant())
    hit = repo.get_fleet_health_summary(tenant())
    assert clickhouse.calls == 1
    assert miss.evidence.cache_status == "miss" and hit.evidence.cache_status == "hit"
    assert next(iter(redis.ttls.values())) == 17
    assert hit.evidence.cache_age_seconds is not None and "cached checkpoint=cp-1" in hit.evidence.data_freshness
    assert hit.evidence.cache_latency_saved_ms is not None


def test_cache_separates_tenants_and_checkpoint_versions_without_plain_ids_in_keys():
    redis, clickhouse = Redis(), ClickHouse()
    repo = repository(redis, clickhouse, checkpoint="cp-1")
    repo.get_fleet_health_summary(tenant("tenant-secret-a", ("customer-secret-a",)))
    repo.get_fleet_health_summary(tenant("tenant-secret-b", ("customer-secret-b",)))
    repository(redis, clickhouse, checkpoint="cp-2").get_fleet_health_summary(tenant("tenant-secret-a", ("customer-secret-a",)))
    assert clickhouse.calls == 3 and len(redis.values) == 3
    assert all("secret" not in key and key.startswith("dtc:v1:") for key in redis.values)


def test_redis_failure_is_not_data_failure():
    clickhouse = ClickHouse()
    response = repository(Redis(fail=True), clickhouse).get_fleet_health_summary(tenant())
    assert response.rows and clickhouse.calls == 1
    assert response.evidence.cache_status == "error" and response.evidence.cache_error == "cache_unavailable"
