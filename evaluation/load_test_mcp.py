"""Non-destructive MCP load probe for an approved non-production environment."""

import concurrent.futures
import json
import os
import statistics
import sys
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.dtc_mcp.client import invoke_tool
from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.security import context_from_verified_claims


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, int(len(ordered) * fraction + 0.999) - 1))] if ordered else 0


def main():
    calls = max(1, int(os.getenv("DTC_LOAD_CALLS", "100")))
    concurrency = max(1, int(os.getenv("DTC_LOAD_CONCURRENCY", "10")))
    settings = DTCSettings.from_env()
    context = context_from_verified_claims({
        "user_id": "load-probe", "tenant_id": "approved-load-tenant", "customer_id": "approved-load-customer",
        "allowed_customer_ids": [value for value in os.environ["DTC_LOAD_CUSTOMER_IDS"].split(",") if value],
        "roles": ["analyst"], "scopes": ["dtc:fleet:read"], "request_id": "load-probe", "trace_id": "load-probe",
    })

    def one(_):
        started = time.perf_counter()
        try:
            response = invoke_tool(settings, context, "get_fleet_health_summary", {})
            return {"ok": response.ok, "latency_ms": (time.perf_counter() - started) * 1000, "cache": response.evidence.cache_status if response.evidence else "disabled"}
        except Exception as exc:
            return {"ok": False, "latency_ms": (time.perf_counter() - started) * 1000, "error": type(exc).__name__}

    tracemalloc.start()
    cpu_started = time.process_time()
    wall_started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        results = list(pool.map(one, range(calls)))
    wall_seconds = time.perf_counter() - wall_started
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    latencies = [item["latency_ms"] for item in results]
    report = {
        "scope": "Approved non-production MCP load probe", "calls": calls, "concurrency": concurrency,
        "wall_seconds": wall_seconds, "requests_per_second": calls / wall_seconds,
        "latency_ms": {"p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95), "p99": percentile(latencies, 0.99), "average": statistics.mean(latencies)},
        "success_rate": sum(item["ok"] for item in results) / calls,
        "error_rate": sum(not item["ok"] for item in results) / calls,
        "cache_hit_rate": sum(item.get("cache") == "hit" for item in results) / calls,
        "client_cpu_seconds": time.process_time() - cpu_started, "client_peak_tracemalloc_bytes": peak,
        "note": "Collect server/container CPU, memory, ClickHouse query/pool metrics alongside this client report. Never target production without approval."
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
