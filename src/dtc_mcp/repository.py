from __future__ import annotations

import hashlib
import math
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Iterable

from src.clickhouse_utils import get_dtc_mcp_clickhouse_client
from src.dtc_mcp.cache import ResultCache
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import ErrorCode, EvidenceMetadata, QueryMetadata, RepositoryResult, TenantContext, TimeRange


class RepositoryError(Exception):
    def __init__(self, code: ErrorCode, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _normalize(item) for key, item in value.items()}
    return str(value)


class RepositoryExecutor:
    CACHEABLE_QUERIES = {"fleet_health_summary", "fleet_kpis", "top_dtcs", "fault_trends", "vehicle_health", "dtc_code_info", "dtc_fleet_impact", "maintenance_priority"}

    def __init__(self, client_factory: Callable[[], Any] = get_dtc_mcp_clickhouse_client, settings: DTCSettings | None = None, cache: ResultCache | None = None):
        self.client_factory = client_factory
        self.settings = settings or DTCSettings.from_env()
        self.cache = cache or ResultCache.from_settings(self.settings)

    def ready(self) -> bool:
        try:
            rows = self.client_factory().execute("SELECT 1", {}, settings={"readonly": 2, "max_execution_time": min(self.settings.query_timeout_seconds, 5)})
            return bool(rows and rows[0][0] == 1)
        except Exception:
            return False

    def execute(
        self,
        query: str,
        *,
        parameters: dict[str, Any],
        columns: Iterable[str] | None,
        tables: Iterable[str],
        context: TenantContext,
        limit: int,
        query_type: str,
        filters_applied: dict[str, Any],
        query_window: TimeRange | None = None,
    ) -> RepositoryResult:
        if not context.allowed_customer_ids:
            raise RepositoryError(ErrorCode.UNAUTHENTICATED, "Authenticated customer mapping is required")
        tables_tuple = tuple(tables)
        if not tables_tuple or set(tables_tuple) - set(self.settings.allowed_tables):
            raise RepositoryError(ErrorCode.QUERY_REJECTED, "Query references an unapproved analytics table")
        effective_limit = min(max(1, int(limit)), self.settings.max_result_rows)
        bound = dict(parameters)
        bound["query_limit"] = effective_limit + 1
        query_hash = hashlib.sha256(" ".join(query.split()).encode("utf-8")).hexdigest()
        cache_key = cache_key_hash = None
        cache_lookup = None
        if query_type in self.CACHEABLE_QUERIES:
            cache_key, cache_key_hash = self.cache.key(tenant_id=context.tenant_id, customer_ids=context.allowed_customer_ids, operation=query_type, parameters=parameters, query_hash=query_hash, limit=effective_limit)
            cache_lookup = self.cache.get(cache_key, cache_key_hash)
            if cache_lookup.value:
                payload = cache_lookup.value
                result = RepositoryResult.model_validate(payload["result"])
                age = max(0.0, time.time() - float(payload.get("cached_at") or time.time()))
                result.evidence.cache_status = "hit"
                result.evidence.cache_key_hash = cache_key_hash
                result.evidence.cache_invalidation_version = self.cache.checkpoint
                result.evidence.cache_age_seconds = round(age, 3)
                result.evidence.cache_latency_saved_ms = round(max(0.0, result.evidence.duration_ms - cache_lookup.latency_ms), 3)
                result.evidence.duration_ms = cache_lookup.latency_ms
                result.evidence.data_freshness = f"cached checkpoint={self.cache.checkpoint} age_seconds={round(age, 3)}"
                return result
        settings = {
            "readonly": 2,
            "max_execution_time": self.settings.query_timeout_seconds,
            "max_result_rows": effective_limit + 1,
            "result_overflow_mode": "break",
            "max_result_bytes": self.settings.max_result_bytes,
            "log_comment": f"dtc_mcp trace={context.trace_id}",
        }
        started = time.perf_counter()
        try:
            client = self.client_factory()
            if columns is None:
                names, raw_rows = client.query_df(query, bound, settings=settings)
            else:
                raw_rows = client.execute(query, bound, settings=settings)
        except TimeoutError as exc:
            raise RepositoryError(ErrorCode.TIMEOUT, "Analytics query timed out", retryable=True) from exc
        except Exception as exc:
            raise RepositoryError(ErrorCode.UPSTREAM_UNAVAILABLE, "Analytics query failed", retryable=True) from exc
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        names = tuple(names if columns is None else columns)
        normalized: list[dict[str, Any]] = []
        for row in raw_rows or []:
            mapping = row if isinstance(row, dict) else dict(zip(names, row))
            normalized.append({str(key): _normalize(value) for key, value in mapping.items()})
        truncated = len(normalized) > effective_limit
        rows = normalized[:effective_limit]
        now = datetime.now(timezone.utc)
        metadata = QueryMetadata(
            query_hash=query_hash,
            tables=tables_tuple,
            row_count=len(rows),
            truncated=truncated,
            effective_limit=effective_limit,
            execution_latency_ms=duration_ms,
        )
        evidence = EvidenceMetadata(
            tables=tables_tuple,
            query_type=query_type,
            query_hash=query_hash,
            filters_applied=filters_applied,
            data_freshness="producer_checkpoint_unknown",
            scope_ref=hashlib.sha256(context.tenant_id.encode("utf-8")).hexdigest()[:16],
            query_window=query_window,
            as_of=now,
            row_count=len(rows),
            truncated=truncated,
            effective_limit=effective_limit,
            duration_ms=duration_ms,
            trace_id=context.trace_id,
        )
        result = RepositoryResult(rows=rows, metadata=metadata, evidence=evidence)
        if cache_lookup is not None:
            evidence.cache_status = cache_lookup.status
            evidence.cache_key_hash = cache_key_hash
            evidence.cache_invalidation_version = self.cache.checkpoint
            evidence.cache_error = cache_lookup.error
            if cache_key and cache_lookup.status in {"miss", "error"}:
                evidence.cache_error = evidence.cache_error or self.cache.set(cache_key, result.model_dump(mode="json"))
        return result
