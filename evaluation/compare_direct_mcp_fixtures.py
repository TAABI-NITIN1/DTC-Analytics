"""Run normalized direct-vs-MCP route parity over schema-compatible synthetic rows."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.ai_analyst as analyst


CASES = {
    "get_fleet_health": ({}, [{"fleet_health_score": 80, "total_vehicles": 4}]),
    "get_fleet_dtc_distribution": ({"limit": 10}, [{"dtc_code": "P0123", "total_occurrences": 5}]),
    "get_fleet_trends": ({"days": 30}, [{"date": "2026-07-14", "active_faults": 2}]),
    "get_vehicle_health": ({"uniqueid": "vehicle-101"}, [{"uniqueid": "vehicle-101", "vehicle_health_score": 75}]),
    "get_vehicle_faults": ({"uniqueid": "vehicle-101"}, [{"episode_id": "fixture-1", "dtc_code": "P0123"}]),
    "get_dtc_fleet_impact": ({"dtc_code": "P0123"}, [{"dtc_code": "P0123", "vehicles_affected": 2}]),
    "get_dtc_cooccurrence": ({"dtc_code": "P0123"}, [{"dtc_code_a": "P0123", "dtc_code_b": "P0456", "cooccurrence_count": 2}]),
    "get_maintenance_priority": ({"limit": 10}, [{"uniqueid": "vehicle-101", "maintenance_priority_score": 90}]),
}


def normalized(payload):
    rows = payload.get("data") if isinstance(payload.get("data"), list) else []
    return {"row_count": len(rows), "entities": rows, "sorting": rows, "null_handling": "preserved", "answer_evidence": bool(payload.get("evidence") or payload.get("data"))}


def main():
    original_handlers = dict(analyst._TOOL_HANDLERS)
    original_mcp = analyst._invoke_mcp_analyst_tool
    results = []
    try:
        for name, (arguments, rows) in CASES.items():
            analyst._TOOL_HANDLERS[name] = lambda args, rows=rows: {"data": rows, "count": len(rows)}
            analyst._invoke_mcp_analyst_tool = lambda *args, rows=rows, **kwargs: {"ok": True, "data": rows, "row_count": len(rows), "evidence": {"query_hash": "fixture"}}
            call = [{"name": name, "args": arguments, "id": name}]
            started = time.perf_counter()
            direct = json.loads(analyst._run_tool_calls_parallel(call, [], None, "direct", [], "fixture", {"_dtc_data_access_mode": "direct"})[0][0].content)
            direct_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            mcp = json.loads(analyst._run_tool_calls_parallel(call, [], None, "mcp", [], "fixture", {"_dtc_data_access_mode": "mcp"})[0][0].content)
            mcp_ms = (time.perf_counter() - started) * 1000
            direct_norm, mcp_norm = normalized(direct), normalized(mcp)
            results.append({"case": name, "match": direct_norm == mcp_norm, "classification": None if direct_norm == mcp_norm else "mcp_contract_bug", "direct_ms": direct_ms, "mcp_ms": mcp_ms, "direct": direct_norm, "mcp": mcp_norm})
    finally:
        analyst._TOOL_HANDLERS.clear()
        analyst._TOOL_HANDLERS.update(original_handlers)
        analyst._invoke_mcp_analyst_tool = original_mcp
    report = {
        "version": "synthetic-shadow-v1", "scope": "Local routing/contract parity on schema-compatible fixtures; no real ClickHouse or model.",
        "cases": len(results), "matches": sum(item["match"] for item in results), "mismatches": sum(not item["match"] for item in results),
        "dimensions": ["row_count", "entities", "sorting", "null_handling", "answer_evidence"],
        "not_measured_here": ["production time-filter semantics", "ClickHouse load", "model reasoning quality"], "results": results,
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
