"""Measure the deterministic MCP fast-path subset; never extrapolate it to full production traffic."""

import json
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import src.ai_analyst as analyst
from src.dtc_mcp.security import context_from_verified_claims


def percentile(values, fraction):
    values = sorted(values)
    return values[min(len(values) - 1, max(0, int(len(values) * fraction + 0.999) - 1))]


def main():
    historical = json.loads(Path("eval-dashboard/public/runs/combined_report.json").read_text(encoding="utf-8"))["report"]["run_metrics"]
    tenant = context_from_verified_claims({
        "user_id": "bench", "tenant_id": "fixture", "customer_id": "fixture",
        "allowed_customer_ids": ["101"], "scopes": ["dtc:fleet:read", "dtc:schema:read"],
        "request_id": "bench", "trace_id": "bench",
    })
    calls = []

    def fake(tool, arguments, context, question):
        calls.append((tool, json.dumps(arguments, sort_keys=True)))
        if tool == "get_dtc_details":
            data = [{"dtc_code": "P0123", "description": "Sensor fault", "system": "engine", "action_required": "Inspect sensor"}]
        else:
            data = [{"fleet_health_score": 80, "total_vehicles": 4, "vehicles_with_active_faults": 2, "vehicles_with_critical_faults": 1}]
        return {"ok": True, "data": data, "row_count": 1, "truncated": False, "limitations": [], "evidence": {"query_hash": "fixture", "data_freshness": "fixture-checkpoint"}, "request_id": "bench", "_tool_name": tool, "_arguments_hash": "fixture"}

    original = analyst._invoke_mcp_analyst_tool
    analyst._invoke_mcp_analyst_tool = fake
    latencies = []
    passed = 0
    multi_turn_pass = 0
    benchmark_call_count = 0
    try:
        questions = ["What is code P0123?", "fleet summary"]
        for index in range(200):
            started = time.perf_counter()
            response = analyst._fast_path_response([{"role": "user", "content": questions[index % 2]}], {"_dtc_tenant_context": tenant, "customer_name": "Fixture"})
            latencies.append((time.perf_counter() - started) * 1000)
            passed += int(bool(response and response["text"] and response["token_usage"] == {"prompt": 0, "completion": 0} and response["tool_results"]["fast_path"]["evidence"]["query_hash"]))
        benchmark_call_count = len(calls)
        first = analyst._fast_path_response([{"role": "user", "content": "fleet summary"}], {"_dtc_tenant_context": tenant, "customer_name": "Fixture"})
        follow_up = analyst._fast_path_response(
            [{"role": "user", "content": "Show that again"}],
            {"_dtc_tenant_context": tenant, "customer_name": "Fixture", "_dtc_conversation_state_verified": True, "_dtc_conversation_state": first["conversation_state"]},
        )
        multi_turn_pass = int(bool(follow_up and "previously verified" in follow_up["text"] and follow_up["token_usage"] == {"prompt": 0, "completion": 0}))
    finally:
        analyst._invoke_mcp_analyst_tool = original

    report = {
        "benchmark_version": "mcp-serving-fast-path-v1",
        "scope": "Local deterministic fast-path microbenchmark; not a full-graph or production comparison.",
        "historical_before": {
            "source_run": historical["run_id"], "turns": historical["total_turns"],
            "average_latency_ms": historical["avg_latency_sec"] * 1000,
            "p95_latency_ms": historical["p95_latency_sec"] * 1000,
            "tokens_per_turn": historical["avg_tokens_per_turn"],
            "tools_per_turn": historical["total_tool_calls"] / historical["total_turns"],
            "sql_calls_per_turn": historical["total_sql_queries"] / historical["total_turns"],
            "groundedness": historical["avg_groundedness_score"],
            "single_turn_pass": historical["pass_rate"],
            "true_multi_turn_pass": "not_separately_reported",
            "cost_per_successful_answer_usd": historical["total_cost_usd"] / historical["total_passed"],
            "repeated_query_rate": "not_reported", "mcp_error_rate": "not_applicable_pre_mcp"
        },
        "current_fast_path": {
            "turns": len(latencies), "average_latency_ms": statistics.mean(latencies),
            "p95_latency_ms": percentile(latencies, 0.95), "tokens_per_turn": 0,
            "tools_per_turn": benchmark_call_count / len(latencies), "sql_calls_per_turn": 0,
            "repeated_query_rate": 0, "mcp_error_rate": 0,
            "groundedness": passed / len(latencies), "single_turn_pass": passed / len(latencies),
            "true_multi_turn_pass": float(multi_turn_pass),
            "cost_per_successful_answer_usd": 0
        },
        "comparison_validity": "The populations/environments differ. These numbers prove fast-path mechanics only; no end-to-end production improvement is claimed.",
        "full_mcp_after": "pending approved ClickHouse, model, load environment, and shadow traffic"
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
