# AI Analyst - Complete Technical Documentation

## 1. Scope

This document explains the AI Analyst end-to-end:

- how a request is processed from start to finish
- graph flow and node logic
- tools and SQL behavior
- which analytics tables are used
- what data is read and why
- observability, tracing, evaluation, and persistence
- fallback and failure handling
- worked examples

This is intentionally focused on the AI Analyst runtime and reasoning pipeline only.

## 2. What the AI Analyst Is

The AI Analyst is a LangGraph-based diagnostic intelligence agent implemented in `src/ai_analyst.py`.

It combines:

- intent classification
- scoped context enrichment
- structured tool calls over ClickHouse analytics tables
- optional dynamic SQL generation and auto-fix
- multi-stage reasoning for diagnostics
- final plain-language response synthesis
- deep observability (node-level, SQL-level, planner-level, token-level)

At runtime, it behaves as an orchestrated graph, not a single monolithic prompt call.

## 3. High-Level Architecture

The graph uses these 8 nodes:

1. `detect_intent`
2. `build_context`
3. `investigate_data`
4. `analyze_faults`
5. `reason_root_cause`
6. `assess_maintenance`
7. `generate_recommendation`
8. `explain`

Routing behavior:

- casual conversation: `detect_intent -> build_context -> explain`
- summary intents: `detect_intent -> build_context -> investigate_data -> explain`
- diagnostic intents: `detect_intent -> build_context -> investigate_data -> analyze_faults -> reason_root_cause -> assess_maintenance -> generate_recommendation -> explain`

## 4. Core Data Model (AgentState)

The graph state carries all intermediate artifacts. Important fields:

- `messages`: conversation history (plus final AI response)
- `context`: scope filters (`customer_name`, `vehicle_number`, `dtc_code`, `mode`)
- `intent`: intent label from Node 1
- `tool_results`: tool call outputs keyed by tool-call id
- `chart_config`: optional chart object
- `last_tool_data`: latest data list from tool outputs
- `fault_analysis`, `root_cause`, `maintenance_priority_assessment`, `recommendation`: staged reasoning outputs
- `nodes_executed`: deterministic execution trace
- `failure_reasons`: accumulated failure flags
- `token_usage`: accumulated prompt/completion tokens
- `tools_used`: tool names called in Node 3
- `trace_log`: structured per-node trace events
- `request_id`: per-request correlation id
- `sql_events`: SQL execution events (query, status, row count, duration)
- `metrics`: per-node deterministic metrics
- `version`: release/service/model/prompt version metadata

## 5. Entry Point and Lifecycle

Main entry point is `chat(messages, context=None)`.

### 5.1 Startup checks

- Requires `OPENAI_API_KEY`
- Converts external messages to LangChain message classes
- Prepends `SYSTEM_PERSONA + SYSTEM_PROMPT`
- Builds version metadata (`release`, `service`, `git commit`, `env`, model, dataset, prompt versions)
- Initializes `AgentState`

### 5.2 Graph invocation

Runs `_agent_graph.invoke(initial_state, config=...)` with:

- recursion limit
- run name
- tags for observability
- metadata including customer/mode/query/request_id/version fields

### 5.3 Post-run processing

- extracts final answer text
- enforces persona phrase replacement (`_enforce_persona`)
- runs trace evaluation via `evaluate_trace(...)`
- aggregates node metrics from trace log
- derives `sql_planner_events` from tool results
- optionally persists observability payload using `try_persist_observability_run(...)`
- returns final response payload (text, chart, metrics, traces, version)

## 6. Intent Taxonomy and Routing

### 6.1 Intent labels

Diagnostic intents:

- `vehicle_investigation`
- `dtc_investigation`
- `fault_correlation`

Summary intents:

- `fleet_health`
- `trend_analysis`
- `maintenance_prioritization`
- `unknown`

Special:

- `casual_conversation`

### 6.2 Routing rules

- After `build_context`:
  - if `casual_conversation`: route directly to `explain`
  - else: go to `investigate_data`

- After `investigate_data`:
  - diagnostic intents: continue through reasoning stack (`analyze_faults` onward)
  - summary intents: route directly to `explain`

## 7. Node-by-Node Logic

## 7.1 Node 1: detect_intent

Function: `_node_detect_intent`

Input:

- latest user message

Logic:

- uses `_INTENT_PROMPT`
- single LLM call (`_reasoning_llm`)
- normalizes output to lowercase and underscore format
- if unknown label, defaults to `unknown`
- updates token usage and node history

Output:

- `intent`
- updated `nodes_executed`
- updated `token_usage`

Example:

- User: "show top DTCs in last 30 days"
- Intent: `fleet_health` or `trend_analysis` (depending on phrasing)

## 7.2 Node 2: build_context

Function: `_node_build_context`

Input:

- current context and intent

Logic:

- copies context
- writes `_intent` into context
- builds `_scope_hints` from available filters:
  - customer
  - vehicle
  - DTC
  - mode

Output:

- enriched `context`
- updated `nodes_executed`

Example scope hints:

- `Filter all data to customer: Acme Logistics`
- `Focus on vehicle: MH12AB1234`

## 7.3 Node 3: investigate_data

Function: `_node_investigate_data`

This is the retrieval engine.

Input:

- user query
- context scope hints
- tools bound LLM (`_tools_llm`)

Logic:

- creates retrieval prompt with optional RAG snippets from `docs/rag/*.md`
- runs up to 4 rounds of tool-calling mini-loop
- each round:
  - LLM emits tool calls
  - calls executed in parallel via `_run_tool_calls_parallel`
  - tool results converted to `ToolMessage` and fed back into loop
- tracks `tools_used`, `tool_results`, token usage, chart config, SQL events

Fallback behavior:

- if no tool data returned:
  - deterministic fallback to `_tool_get_fleet_health`
  - marks `heuristic_fallback_used`
- if fallback also fails:
  - marks `no_tool_data_returned`

Error marker:

- if any tool result contains `error`: adds `sql_error`

Output:

- `tool_results`
- `chart_config`
- `last_tool_data`
- `sql_events`
- `tools_used`
- updated `failure_reasons`

## 7.4 Node 4: analyze_faults

Function: `_node_analyze_faults`

Input:

- summarized `tool_results`

Logic:

- prompts LLM to group faults by:
  - system
  - severity
  - recurring vs one-time
- asks for top 3 concern areas with data support

Output:

- `fault_analysis`
- updates failures if output too short (`fault_analysis_empty`)

## 7.5 Node 5: reason_root_cause

Function: `_node_reason_root_cause`

Input:

- `fault_analysis`
- optional co-occurrence data extracted from tool outputs

Logic:

- prompts LLM to infer likely root causes
- includes confidence ranking (High/Medium/Low)
- looks for cascade and systemic patterns

Output:

- `root_cause`

## 7.6 Node 6: assess_maintenance

Function: `_node_assess_maintenance`

Input:

- fault analysis
- root cause
- truncated raw tool data

Logic:

- prompts LLM to assign urgency buckets:
  - CRITICAL
  - HIGH
  - MEDIUM
  - LOW
- factors include severity, persistence, recurrence, safety systems, driver-related flags

Output:

- `maintenance_priority_assessment`

## 7.7 Node 7: generate_recommendation

Function: `_node_generate_recommendation`

Input:

- `fault_analysis`
- `root_cause`
- `maintenance_priority_assessment`

Logic:

- enforces structured response format:
  - Problem
  - Cause
  - Impact
  - Action

Output:

- `recommendation`

## 7.8 Node 8: explain

Function: `_node_explain`

Special behavior for casual messages:

- if identity question, returns `TAABI_IDENTITY_REPLY`
- else returns short greeting
- no tool/analysis usage

Normal behavior:

- assembles prompt context from user question + tool summary + intermediate outputs
- evidence guard:
  - if non-casual and no retrieval evidence, returns explicit insufficient-data message
- chooses response style:
  - detailed mode for summary intents or `force_detailed_response`
  - concise mode otherwise
- applies persona phrase enforcement

Output:

- final AI message (`messages`)
- updated failures/tokens/nodes

## 8. Structured Tools and Data Access

The AI Analyst defines 12 callable functions (11 data/chart functions + `run_sql` fallback).

## 8.1 Tool list

- `get_fleet_health`
- `get_fleet_dtc_distribution`
- `get_fleet_trends`
- `get_vehicle_health`
- `get_vehicle_faults`
- `get_dtc_details`
- `get_dtc_cooccurrence`
- `get_maintenance_priority`
- `get_fleet_system_health`
- `get_dtc_fleet_impact`
- `run_sql`
- `generate_chart`

## 8.2 Tool execution strategy

Inside `_run_tool_calls_parallel`:

- executes each tool call in thread pool
- tags SQL events with request/node/tool context using thread-local `_SQL_TRACE_CONTEXT`
- merges chart and latest data payloads for downstream usage

For non-`run_sql` tools:

- primary behavior controlled by env flags:
  - `AI_ANALYST_TOOL_FIRST` (default on)
  - `AI_ANALYST_DYNAMIC_SQL_FALLBACK` (default on)

Mode behavior:

- tool-first mode:
  - call deterministic tool handler first
  - if it returns error, fallback to LLM SQL planner
- planner-first mode:
  - directly use LLM SQL planner for structured intents

## 9. Tables Used by AI Analyst

This section lists tables referenced by tool SQL and prompt/schema context.

## 9.1 Primary analytics tables used in executed SQL

- `vehicle_fault_master_ravi_v2`
- `vehicle_health_summary_ravi_v2`
- `fleet_health_summary_ravi_v2`
- `fleet_fault_trends_ravi_v2`
- `fleet_dtc_distribution_ravi_v2`
- `fleet_system_health_ravi_v2`
- `dtc_fleet_impact_ravi_v2`
- `dtc_cooccurrence_ravi_v2`

## 10. Tool Schemas, Parameters, and Example Outputs

This section documents the callable tools, their input parameters, typical outputs, and example payloads that the AI Analyst expects and produces.

### 10.1 `get_fleet_health`
- Parameters: `customer_name` (optional string), `days` (optional int, default 30)
- Returns: JSON object with `{ total_vehicles, vehicles_with_active_faults, vehicles_with_critical_faults, fleet_health_score, most_common_dtc, most_common_system, active_fault_trend }`

Example output:

```
{
  "total_vehicles": 2042,
  "vehicles_with_active_faults": 1446,
  "vehicles_with_critical_faults": 287,
  "fleet_health_score": 94.5,
  "most_common_dtc": "P0420",
  "most_common_system": "Powertrain",
  "active_fault_trend": "increasing"
}
```

### 10.2 `get_vehicle_health`
- Parameters: `uniqueid` (string), `customer_name` (optional), `limit` (optional)
- Returns: vehicle-level summary rows matching `vehicle_health_summary_ravi_v2` schema

Example output (truncated):

```
{
  "uniqueid": "MH12AB1234",
  "vehicle_number": "MH12-1234",
  "active_fault_count": 3,
  "critical_fault_count": 1,
  "vehicle_health_score": 82.3,
  "most_common_dtc": "P0128"
}
```

### 10.3 `get_vehicle_faults`
- Parameters: `uniqueid`, `limit`, `since_ts`/`until_ts`
- Returns: List of episode objects (one row per episode) from `vehicle_fault_master_ravi_v2`.

Example output (one episode):

```
{
  "episode_id": "MH12AB1234_P0128_3",
  "dtc_code": "P0128",
  "first_ts": 1679875200,
  "last_ts": 1679961600,
  "occurrence_count": 12,
  "resolution_time_sec": 86400,
  "is_resolved": 0,
  "severity_level": 3
}
```

### 10.4 `get_dtc_details`
- Parameters: `dtc_code`
- Returns: Entry from `dtc_master_ravi_v2` with severity, description, symptoms, recommended actions.

### 10.5 `run_sql`
- Parameters: `sql` (string), `max_rows` (int)
- Behavior: Only allowed tables from `_ALLOWED_TABLES` are permitted. The tool performs read-only `SELECT` statements and returns `{ rows: [...], row_count, query }` or an `error` field.

Example `run_sql` output:

```
{
  "query": "SELECT uniqExactIf(uniqueid, is_resolved = 0) FROM vehicle_fault_master_ravi_v2 WHERE clientLoginId = 'VRL'",
  "row_count": 1,
  "rows": [[1446]]
}
```

### 10.6 `generate_chart`
- Parameters: `type` (bar/line/pie), `data` (array of objects), `title`, `x`, `y`
- Returns: `chart_config` object used by frontend to render visualizations

Example chart_config (bar):

```
{ "type": "bar", "title": "Top DTCs", "data": [{"dtc":"P0420","count":120},{"dtc":"P0128","count":88}], "x":"dtc","y":"count" }
```

## 11. SQL Patterns and Safe Usage

The AI Analyst uses deterministic SQL patterns for common information requests and only allows read-only queries in `run_sql`.

Recommended patterns:

- Distinct active vehicles for a customer:

```
SELECT uniqExactIf(uniqueid, is_resolved = 0) AS active_vehicles
FROM vehicle_fault_master_ravi_v2
WHERE customer_name = 'VRL LOGISTICS LIMITED'
```

- Per-vehicle recent episodes (last 30 days):

```
SELECT episode_id, dtc_code, first_ts, last_ts, occurrence_count, is_resolved
FROM vehicle_fault_master_ravi_v2
WHERE uniqueid = 'MH12AB1234' AND first_ts >= {thirty_days_ago_ts}
ORDER BY first_ts DESC
LIMIT 100
```

Security enforcement:

- `run_sql` rejects queries that contain `INSERT`, `UPDATE`, `DELETE`, `ALTER`, `DROP`, `TRUNCATE`, or reference non-allowed tables.
- All parameter values must be sanitized and/or bound by the runtime; never format user input directly into SQL.

## 12. Observability, Tracing, and Persisted Payloads

The AI Analyst produces rich observability payloads for every request. Key pieces:

- `trace_log`: List of node-level events produced by the `trace_node` decorator. Each entry contains `node`, `duration`, `metrics`, `sql_events`, `output_summary`.
- `sql_events`: Array of SQL execution events with `{ query, duration, row_count, success, error }`.
- `sql_planner_events`: Events describing generated SQL from planner and fix attempts.
- `request_id`: Correlation id used across logs, traces, and persisted payloads.

Persistence flow:

- At the end of a run, `try_persist_observability_run(state)` attempts to write a compact JSON payload to the observability store (clickhouse/managed store). Payload includes `version`, `trace_log`, `sql_planner_events`, `metrics`, `final_answer_preview`, and `token_usage`.
- MLflow metrics (if enabled) are emitted per-run with tags for `intent`, `customer_name`, `model_name`, and `release_version`.

Retention and size:

- Trace payloads are truncated to avoid storing raw messages longer than 4k characters. Full SQL queries are preserved but large result sets are summarized with `row_count` only.

## 13. Evaluation, Testing, and Benchmarks

### 13.1 `evaluate_trace` and unit tests

- `evaluate_trace(trace_log)` computes readiness signals such as `final_answer_non_empty`, `sql_error_rate`, `tool_error_rate`, and `intent_consistency`.
- Unit tests should assert expected behavior for each node using mocked LLM and tool responses.

Example pytest snippets:

```python
def test_detect_intent_simple():
    state = {'messages': [HumanMessage(content='Show top DTCs last 7 days')], 'request_id': 'test'}
    result = _node_detect_intent(state)
    assert result['intent'] in ('fleet_health', 'trend_analysis')

def test_investigate_data_tool_fallback(monkeypatch):
    # Mock tools to return empty, ensure fallback to get_fleet_health
    monkeypatch.setattr(tools_module, 'get_fleet_health', lambda **k: {'total_vehicles': 1})
    state = {'messages': [HumanMessage(content='Status for VRL')], 'context': {'customer_name': 'VRL'}}
    res = _node_investigate_data(state)
    assert 'tool_results' in res
```

### 13.2 Integration tests

- Use a small ClickHouse test dataset (subset of V2 tables) in CI.
- Validate end-to-end flows: sample user message → graph invoke → persisted trace → expected keys in response.

### 13.3 Performance benchmarks

- Latency targets:
  - Intent detection: < 200ms
  - Investigate data (tools only): < 1s for typical tool calls
  - Full diagnostic flow with 2 SQL calls: < 2–3s

- Concurrency: Runtime should support N parallel requests where N is tuned per CPU and connection pool; recommended starting point 8 workers with 5 threads each.

## 14. Deployment and Runtime Configuration

### 14.1 Environment variables (summary)

- `OPENAI_API_KEY` — LLM key (required)
- `AI_ANALYST_MODEL_NAME` — default model name
- `AI_ANALYST_TOOL_FIRST` — boolean
- `AI_ANALYST_DYNAMIC_SQL_FALLBACK` — boolean
- `MLFLOW_TRACKING_URI` — optional MLflow host
- `HEALTH_SCALING_CONSTANT` — pipeline constant
- `CLICKHOUSE_HTTP_URL` — ClickHouse HTTP endpoint (default port 8123)

### 14.2 Container and scaling guidance

- CPU: 2–4 vCPUs per replica for moderate load
- Memory: 4–8 GB depending on model and LLM client libraries
- Autoscaling: scale replicas by request latency and queue depth; prefer horizontal scaling with shared ClickHouse connection pool

### 14.3 Observability and alerting

- Instrument metrics: request_count, request_latency, error_rate, sql_error_rate, tool_error_rate
- Alerts: request_latency > 5s, sql_error_rate > 0.05, tool_error_rate > 0.1

## 15. Security, Privacy, and Data Governance

### 15.1 PII and sensitive data handling

- Do not log full VINs or driver PII in persisted traces. Use hashed or truncated identifiers for persisted payloads.
- Redact text fields that match configured PII regexes prior to persistence.

### 15.2 Access control

- Only backend services with appropriate vault-scoped credentials should access `OPENAI_API_KEY` and `CLICKHOUSE_HTTP_URL`.
- API endpoints should be authenticated and authorized by customer scope.

### 15.3 Prompt and tool safety

- Avoid passing raw diagnostic text to LLM without structured context.
- Enforce the `NEVER` rules (no hallucination of DTCs, no write SQL) in prompts and at runtime.

## 16. Failure Handling and Retries

- Transient LLM failures: retry up to 2 times with exponential backoff (200ms → 600ms).
- Transient ClickHouse HTTP errors: retry up to 3 times with backoff; log full SQL on final failure.
- Permanent tool errors: mark `failure_reasons` and surface concise error message to user.

## 17. Maintenance, Changelog, and Versioning

- Bump `PROMPT_VERSIONS` when editing any prompt text.
- Record `AI_ANALYST_RELEASE_VERSION` for each deployment and include in `version` metadata persisted with traces.
- Changelog should list prompt, model, and SQL changes.

## 18. Troubleshooting Guide

- Symptom: Empty final answer
  - Check `trace_log` for `investigate_data` and `tool_results` entries.
  - If no SQL events, ensure `AI_ANALYST_TOOL_FIRST` is enabled or planner fallback is active.

- Symptom: SQL errors in run
  - Inspect `sql_planner_events` for generated query and `error` messages.
  - Re-run query in ClickHouse client with bound parameters to reproduce.

- Symptom: Slow responses
  - Measure per-node durations in `trace_log` to find bottleneck (LLM vs SQL vs chart generation).

## 19. Example Conversations and Walkthroughs

### 19.1 Fleet-level query

User: "Show me the top 5 DTC codes for VRL Logistics in the last 30 days and suggest top maintenance actions."

Flow:
- `detect_intent` → classifies `trend_analysis`
- `build_context` → scope `{ customer_name: 'VRL Logistics', days: 30 }`
- `investigate_data` → calls `get_fleet_dtc_distribution` and `get_maintenance_priority`
- `analyze_faults` → groups codes by severity and recurrence
- `generate_recommendation` → recommends maintenance actions for top 3 codes
- `explain` → returns structured report + `chart_config` for top DTCs

### 19.2 Vehicle-level deep-dive

User: "Why is vehicle MH12AB1234 health score low?"

Flow:
- `detect_intent` → `vehicle_investigation`
- `build_context` → scope `{ uniqueid: 'MH12AB1234' }`
- `investigate_data` → `get_vehicle_health` + `get_vehicle_faults` + `get_dtc_details`
- `analyze_faults` → identifies recurring P0128 episodes with long durations
- `reason_root_cause` → hypothesizes coolant thermostat failure or sensor fault
- `assess_maintenance` → assigns CRITICAL and suggests immediate inspection
- `generate_recommendation` → step-by-step checks and parts to inspect
- `explain` → user-facing actionable summary with estimated downtime risk

## 20. Appendix: Useful Files and References

- Implementation: `src/ai_analyst.py`
- Observability helpers: `src/observability_store.py`
- Evaluation harness: `src/evaluation_agent.py`
- Prompt versions: `PROMPT_VERSIONS` at top of `src/ai_analyst.py`
- RAG docs: `docs/rag/*.md`

---

If you'd like, I can:

- generate a downloadable Word/PDF version of this completed AI Analyst documentation,
- create a condensed one-page executive summary for stakeholders,
- or produce a test harness (pytest) with mocked tools and sample ClickHouse data to validate the full graph in CI.

Tell me which of these you prefer next and I'll proceed.

- `maintenance_priority_ravi_v2`
- `dtc_master_ravi_v2`

## 9.2 Additional catalog tables present in system prompt/schema context

- `dtc_events_exploded_ravi_v2`
- `vehicle_master_ravi_v2`

These may be used by dynamic planner paths depending on schema allowlist and table selection.

## 9.3 Table-to-tool mapping

### get_fleet_health

Reads:

- `vehicle_health_summary_ravi_v2` (summary aggregation)
- `vehicle_fault_master_ravi_v2` (customer-specific snapshot/trend path)
- `fleet_health_summary_ravi_v2` (global snapshot path)
- `fleet_fault_trends_ravi_v2` (global trend path)

### get_fleet_dtc_distribution

Reads:

- `vehicle_fault_master_ravi_v2` (customer path)
- `fleet_dtc_distribution_ravi_v2` (global path)

### get_fleet_trends

Reads:

- `vehicle_fault_master_ravi_v2` (customer path)
- `fleet_fault_trends_ravi_v2` (global path)

### get_vehicle_health

Reads:

- `vehicle_health_summary_ravi_v2`
- `vehicle_fault_master_ravi_v2`

### get_vehicle_faults

Reads:

- `vehicle_fault_master_ravi_v2`

### get_dtc_details

Reads:

- `dtc_master_ravi_v2`

### get_dtc_cooccurrence

Reads:

- `vehicle_fault_master_ravi_v2` (customer path)
- `dtc_cooccurrence_ravi_v2` (global path)

### get_maintenance_priority

Reads:

- `maintenance_priority_ravi_v2`
- `vehicle_health_summary_ravi_v2` (join enrichment)

### get_fleet_system_health

Reads:

- `vehicle_fault_master_ravi_v2` (customer path)
- `fleet_system_health_ravi_v2` (global path)

### get_dtc_fleet_impact

Reads:

- `vehicle_fault_master_ravi_v2` (customer path)
- `dtc_fleet_impact_ravi_v2` (global path)

### run_sql

Reads:

- any table allowed by `V2_TABLES` (enforced allowlist)

### generate_chart

Reads:

- no DB access; uses prior tool data payload

## 10. SQL Safety and Planner Logic

The SQL layer is strongly guarded.

## 10.1 Read-only validation

`_validate_sql(query)` enforces:

- query must start with `SELECT` or `WITH`
- blocks mutating statements (insert, update, delete, drop, create, alter, truncate, etc.)
- checks all `FROM`/`JOIN` tables against `_ALLOWED_TABLES`

## 10.2 Scoped context enforcement for run_sql

`_validate_run_sql_scope(query, context)` ensures:

- if `customer_name` exists in context:
  - query must include customer_name filter
  - literal must match scoped customer
- if vehicle context exists:
  - query must include scoped vehicle_number or uniqueid literal
- if DTC context exists:
  - query must include scoped DTC literal

## 10.3 SQL parameter rendering

- `_apply_sql_params` performs literal substitution for `%(name)s`
- `_format_sql_literal` escapes and formats values safely for ClickHouse SQL strings/arrays

## 10.4 Dynamic SQL generation and repair

Planner flow (`_execute_structured_tool_with_llm_sql`):

1. choose relevant tables from schema registry (`select_relevant_tables`)
2. generate SQL (`_generate_sql_for_tool`)
3. validate for safety
4. validate semantics (`_validate_sql_semantics`)
5. check efficiency (`_check_query_efficiency`)
6. execute query (`_safe_exec`)
7. validate result shape (`_validate_result_shape`)
8. if any step fails, auto-fix using `_fix_sql_if_needed`

Retry policy:

- up to 3 attempts
- tracks `fix_attempts` and `retry_reasons`
- stores successful SQL in `QUERY_CACHE`

## 11. Observability and Telemetry

Observability is implemented at multiple layers.

## 11.1 Node-level tracing (`trace_node` decorator)

For every node execution, it records:

- start/end timings
- input summary and output summary
- token deltas
- tools-used delta
- failure delta
- SQL event deltas and SQL error counts
- node-specific derived metrics (`_node_stage_metrics`)

It appends structured events into `trace_log`.

## 11.2 SQL-level tracing

`_safe_exec` records SQL events with:

- request id
- node name
- tool name
- rendered query
- success/failure
- row_count
- duration_sec
- timestamp

These events are collected in `sql_events` and included in final payload.

## 11.3 SQL planner event extraction

`_collect_sql_planner_events_from_tool_results(...)` derives planner telemetry from tool results:

- planner-generated query
- selected tables
- fix attempts
- retry reasons
- success/error

## 11.4 Runtime version metadata

`_build_version_metadata()` injects:

- release/service versions
- git commit
- environment name
- model name
- dataset version
- prompt versions

This metadata is copied into traces and final payload.

## 11.5 MLflow integration

If `MLFLOW_TRACKING_URI` is configured and reachable:

- starts an MLflow run
- logs params (intent/mode/customer/failure/version/prompt versions)
- logs metrics:
  - `latency_sec`
  - `success`
  - `tools_called`
  - `nodes_executed_count`
  - prompt/completion token totals
  - `failure_count`
  - `sql_success_rate`

If unavailable, MLflow logging is disabled gracefully.

## 11.6 LLM quality evaluation

After final response, `evaluate_trace(...)` is called with:

- user query
- trace log
- final answer
- evaluator model and timeout

Returned `evaluation` object is attached to the response payload.

## 11.7 Optional persistence

If `AI_ANALYST_PERSIST_OBSERVABILITY` is enabled:

- full payload is persisted through `try_persist_observability_run(...)`
- persistence status is returned as `persisted`

## 12. Failure Handling and Resilience

Built-in resilience layers:

- strict SQL safety guard
- context scope enforcement for ad-hoc SQL
- dynamic planner retry and auto-fix
- deterministic fallback in data retrieval node
- evidence threshold guard before final explanation
- node-level exception tracing with status=`error`
- graceful MLflow disable on connectivity/import failures

Common failure reason flags:

- `heuristic_fallback_used`
- `no_tool_data_returned`
- `sql_error`
- `fault_analysis_empty`
- `insufficient_evidence`
- `explain_empty`

## 13. End-to-End Worked Examples

## 13.1 Example A: Fleet summary question

User input:

- "Give me fleet health overview for Acme Logistics in last 30 days"

Likely flow:

1. `detect_intent` -> `fleet_health`
2. `build_context` -> scope hint includes customer filter
3. `investigate_data` -> tools such as `get_fleet_health`, `get_fleet_dtc_distribution`, `get_fleet_trends`
4. route to `explain` (summary path)
5. final concise manager-facing answer with key numbers and next steps

Data likely read:

- `vehicle_health_summary_ravi_v2`
- `vehicle_fault_master_ravi_v2`
- `fleet_fault_trends_ravi_v2`

## 13.2 Example B: Vehicle diagnostic investigation

User input:

- "Why is vehicle MH12AB1234 repeatedly failing with severe faults?"

Likely flow:

1. `detect_intent` -> `vehicle_investigation`
2. `build_context` -> vehicle scope hint
3. `investigate_data` -> `get_vehicle_health`, `get_vehicle_faults`, possibly `get_dtc_details`/`get_dtc_cooccurrence`
4. `analyze_faults`
5. `reason_root_cause`
6. `assess_maintenance`
7. `generate_recommendation`
8. `explain`

Final output includes structured sections:

- Problem
- Cause
- Impact
- Action

## 13.3 Example C: Casual conversation

User input:

- "Hi"

Flow:

1. `detect_intent` -> `casual_conversation`
2. `build_context`
3. route to `explain`
4. returns short conversational response

No tool calls, no SQL.

## 14. Final Response Payload Contract

`chat(...)` returns a dict with keys such as:

- `text`: final answer
- `chart`: chart config or null
- `intent`
- `customer_name`
- `mode`
- `tools_called`
- `tool_results`
- `metrics` (aggregated node metrics)
- `token_usage`
- `nodes_executed`
- `failure_reasons`
- `trace_log`
- `sql_events`
- `sql_planner_events`
- `evaluation`
- `request_id`
- `version`
- `persisted` (when enabled)

## 15. Environment Variables and Runtime Knobs

Critical:

- `OPENAI_API_KEY`

Model and planner:

- `AI_ANALYST_MODEL_NAME` (default observed in metadata)
- `AI_ANALYST_SQL_PLANNER_MODEL`
- `AI_ANALYST_EVAL_MODEL`
- `AI_ANALYST_EVAL_TIMEOUT_SEC`

Execution behavior:

- `AI_ANALYST_TOOL_FIRST` (tool-first vs planner-first)
- `AI_ANALYST_DYNAMIC_SQL_FALLBACK` (fallback enable/disable)
- `AI_ANALYST_PERSIST_OBSERVABILITY`

Version metadata:

- `AI_ANALYST_RELEASE_VERSION`
- `AI_ANALYST_SERVICE_VERSION`
- `GIT_COMMIT_SHA`
- `DEPLOYMENT_ENV` / `ENV_NAME`
- `AI_ANALYST_DATASET_VERSION`

MLflow:

- `MLFLOW_TRACKING_URI`

## 16. Design Strengths

- clear stage separation (retrieval vs reasoning vs explanation)
- explicit safety guardrails for SQL
- context-aware scope enforcement
- planner self-healing with retry reasons
- rich traceability for debugging and quality monitoring
- deterministic fallback when tools fail

## 17. Known Constraints and Practical Notes

- retrieval mini-loop max rounds is fixed (currently 4)
- planner retries capped (currently 3)
- result rows capped by `MAX_RESULT_ROWS` (500)
- large tool payloads are truncated before reasoning prompts to control token usage
- structured tools should be preferred over `run_sql` for reliability and consistency

## 18. Quick Troubleshooting Checklist

If responses are weak or incorrect:

1. Verify `OPENAI_API_KEY` is set.
2. Check `failure_reasons` in payload.
3. Inspect `sql_events` for failed queries.
4. Inspect `trace_log` for node-level failures or empty outputs.
5. Confirm context scope values (`customer_name`, `vehicle_number`, `dtc_code`) are valid.
6. If planner is overused, enable/confirm tool-first mode.
7. Check MLflow connectivity only if tracking is required.

---

This document is aligned with the current AI Analyst implementation in `src/ai_analyst.py` and is intended to be a full operational reference for development, debugging, and analytics governance.
