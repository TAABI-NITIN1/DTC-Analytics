from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator


audit_log = logging.getLogger("dtc_mcp.audit")
METRICS: Counter = Counter()
_LATENCIES: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=2000))
_PERSISTENCE = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dtc-mcp-audit")
_SENSITIVE_KEYS = {"password", "secret", "token", "authorization", "connection", "sql", "query"}


def _ref(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:16] if value else ""


def sanitize_parameters(parameters: dict[str, Any] | None) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    extra = {item.strip().lower() for item in os.getenv("DTC_MCP_REDACT_FIELDS", "").split(",") if item.strip()}
    for key, value in (parameters or {}).items():
        lowered = str(key).lower()
        if any(part in lowered for part in _SENSITIVE_KEYS | extra):
            redacted[str(key)] = "[REDACTED]"
        elif lowered in {"uniqueid", "vehicle_number", "customer_id", "tenant_id", "user_id"}:
            redacted[str(key)] = f"sha256:{_ref(str(value))}"
        elif isinstance(value, (str, int, float, bool)) or value is None:
            redacted[str(key)] = value
        else:
            redacted[str(key)] = "[COMPLEX]"
    return redacted


def _persist(event: dict[str, Any]) -> None:
    if os.getenv("DTC_MCP_AUDIT_PERSIST_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        return
    try:
        from src.observability_store import try_persist_mcp_audit_event

        try_persist_mcp_audit_event(event)
    except Exception:
        METRICS["audit_persistence_errors_total"] += 1


def record_metric(name: str, value: int = 1) -> None:
    METRICS[name] += value


@contextmanager
def audited_call(tool_name: str, *, request_id: str, trace_id: str, scope_ref: str, user_id: str = "", tenant_id: str = "", session_id: str = "", ai_run_id: str = "", parameters: dict[str, Any] | None = None, roles: tuple[str, ...] = (), scopes: tuple[str, ...] = (), transport: str = "internal", mode: str = "mcp") -> Iterator[dict[str, Any]]:
    started_at = datetime.now(timezone.utc)
    started = time.perf_counter()
    outcome: dict[str, Any] = {"status": "ok", "row_count": 0, "truncated": False}
    try:
        yield outcome
    except Exception as exc:
        outcome["status"] = "error"
        code = getattr(exc, "code", None)
        outcome["error_code"] = getattr(code, "value", None) or "INTERNAL"
        raise
    finally:
        ended_at = datetime.now(timezone.utc)
        duration_ms = round((time.perf_counter() - started) * 1000, 3)
        event = {
            "event": "dtc_mcp_tool_call", "tool_name": tool_name, "tool_version": "1.0",
            "request_id": request_id, "trace_id": trace_id, "session_id": session_id,
            "user_id": _ref(user_id), "tenant_id": _ref(tenant_id), "scope_ref": scope_ref,
            "ai_run_id": ai_run_id, "sanitized_parameters": sanitize_parameters(parameters),
            "start_time": started_at.isoformat(), "end_time": ended_at.isoformat(),
            "latency_ms": duration_ms, "transport": transport, "mode": mode,
            "roles": roles, "scopes": scopes, "policy": "tenant_scope_required", **outcome,
        }
        METRICS["mcp_requests_total"] += 1
        METRICS["calls_total"] += 1
        METRICS["rows_returned_total"] += int(outcome.get("row_count") or 0)
        METRICS["rows_total"] += int(outcome.get("row_count") or 0)
        METRICS["mcp_failures_total"] += int(outcome.get("status") != "ok")
        METRICS["errors_total"] += int(outcome.get("status") != "ok")
        METRICS["mcp_success_total"] += int(outcome.get("status") == "ok")
        METRICS["truncated_total"] += int(bool(outcome.get("truncated")))
        METRICS["cache_hits_total"] += int(outcome.get("cache_status") == "hit")
        METRICS["cache_misses_total"] += int(outcome.get("cache_status") == "miss")
        METRICS["validation_rejections_total"] += int(outcome.get("error_code") == "QUERY_REJECTED")
        METRICS["tenant_authorization_rejections_total"] += int(outcome.get("error_code") in {"FORBIDDEN", "SCOPE_VIOLATION", "UNAUTHENTICATED"})
        METRICS["dynamic_sql_calls_total"] += int(tool_name == "run_validated_dtc_sql")
        METRICS["duration_ms_total"] += duration_ms
        METRICS["database_latency_ms_total"] += float(outcome.get("database_latency_ms") or 0)
        _LATENCIES[tool_name].append(duration_ms)
        audit_log.info(json.dumps(event, sort_keys=True, separators=(",", ":")))
        try:
            _PERSISTENCE.submit(_persist, event)
        except Exception:
            METRICS["audit_persistence_errors_total"] += 1


def _percentile(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(percentile * len(ordered)) - 1))
    return round(ordered[index], 3)


def metrics_snapshot() -> dict[str, float]:
    result: dict[str, float] = {key: float(value) for key, value in METRICS.items()}
    all_latencies = [value for values in _LATENCIES.values() for value in values]
    total = max(1.0, result.get("mcp_requests_total", 0.0))
    cache_total = max(1.0, result.get("cache_hits_total", 0.0) + result.get("cache_misses_total", 0.0))
    result.update({
        "success_rate": result.get("mcp_success_total", 0.0) / total,
        "failure_rate": result.get("mcp_failures_total", 0.0) / total,
        "cache_hit_rate": result.get("cache_hits_total", 0.0) / cache_total,
        "latency_p50_ms": _percentile(all_latencies, 0.50),
        "latency_p95_ms": _percentile(all_latencies, 0.95),
        "latency_p99_ms": _percentile(all_latencies, 0.99),
        "dynamic_sql_usage_rate": result.get("dynamic_sql_calls_total", 0.0) / total,
        "mcp_fallback_rate": result.get("mcp_fallback_total", 0.0) / total,
        "shadow_mismatch_rate": result.get("shadow_mismatches_total", 0.0) / max(1.0, result.get("shadow_comparisons_total", 0.0)),
    })
    for tool, values in _LATENCIES.items():
        result[f"tool.{tool}.latency_p95_ms"] = _percentile(list(values), 0.95)
    return result
