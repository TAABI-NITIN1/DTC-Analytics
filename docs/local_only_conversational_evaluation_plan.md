# Local-Only Conversational Evaluation Plan for DTC Fleet Diagnostics AI

## Summary

This document specifies a self-contained, local-only conversational evaluation framework for the DTC Fleet Diagnostics AI.

The system uses:

- LangGraph for orchestration
- LangSmith for optional trace/debug correlation
- FastAPI / local scripts for execution surfaces
- ClickHouse only as the existing production analytics source queried by the AI tools
- Local files as the evaluation artifact store

Evaluation runs must **not** create any new ClickHouse tables.

Evaluation runs must **not** write evaluation data into ClickHouse.

Evaluation runs should disable existing AI Analyst observability persistence by setting:

```bash
AI_ANALYST_PERSIST_OBSERVABILITY=0
```

This prevents synthetic benchmark runs, adversarial scenarios, and load tests from polluting production observability, customer analytics, or operational metrics.

The framework separates static scenario definitions from runtime-observed results, replaces a single global weighted score with interpretable score dimensions plus hard gates, and captures local artifacts in JSONL, CSV, and optional XLSX formats for replay, analysis, and benchmark comparison.

---

## Absolute Constraint

Do **not** create these ClickHouse evaluation tables:

```text
evaluation_scenarios
evaluation_turns
evaluation_results
conversation_summary
evaluation_runs
evaluation_metrics_daily
evaluation_tool_traces
```

Do **not** add:

```text
evaluation/conversational_schema.sql
evaluation/conversational_store.py
```

The evaluation system is local-first.

ClickHouse remains only the source queried by the existing AI Analyst tools.

---

## Why Local-Only Is the Right Maturity Stage

The evaluation system is still evolving. The metrics, scenario structure, scoring logic, and benchmark definitions will change frequently.

Writing unstable evaluation artifacts into ClickHouse too early would create:

- schema churn
- operational clutter
- noisy analytics
- migration burden
- difficulty separating real production usage from synthetic evaluation traffic
- risk of customer-facing observability pollution

The correct current maturity model is:

```text
Scenario JSON definitions
        ↓
Local conversational runner
        ↓
Local JSONL / CSV / optional XLSX artifacts
        ↓
LangSmith trace correlation by request_id
        ↓
Optional future warehouse ingestion only after schemas stabilize
```

This keeps evaluation fast, safe, replayable, and easy to redesign.

---

## Existing Repo Integration Points

The implementation uses the existing architecture rather than redesigning the AI Analyst.

Important existing integration point:

```text
src.ai_analyst.chat(messages, context)
```

The current `chat()` response already includes useful evaluation data:

```text
request_id
text
chart
intent
customer_name
mode
tools_called
tool_results
metrics
token_usage
nodes_executed
failure_reasons
trace_log
sql_events
sql_planner_events
evaluation
version
```

The runner calls `chat()` like production does, except it passes evaluation metadata through `context` and forces:

```bash
AI_ANALYST_PERSIST_OBSERVABILITY=0
```

This prevents `try_persist_observability_run(...)` from writing normal observability rows during eval.

LangSmith can remain enabled. Local artifacts store `request_id`, which lets engineers correlate a local turn result to the LangSmith trace.

---

## Implemented Files

```text
evaluation/local_store.py
evaluation/conversational_runner.py
evaluation/conversational_scenarios/fleet_diagnostics_core.json
docs/local_only_conversational_evaluation_plan.md
```

Recommended generated-output ignore patterns:

```text
evaluation/artifacts/
evaluation/.cache/
*.tmp
```

---

## High-Level Architecture

```text
Evaluation Scenario Definitions
  evaluation/conversational_scenarios/*.json
        ↓
Conversational Runner
  evaluation/conversational_runner.py
        ↓
Existing AI Analyst
  src.ai_analyst.chat(messages, context)
        ↓
Local Artifact Store
  evaluation/local_store.py
        ↓
Local Result Directory
  evaluation/artifacts/<run_id>/
        ↓
Optional LangSmith Correlation
  request_id ↔ LangSmith trace metadata
```

The framework is not question-answer testing.

It is multi-turn fleet operations investigation testing.

Each scenario represents a realistic diagnostic investigation:

```text
Scenario
├── Turn 1: initial operational question
├── Turn 2: follow-up question
├── Turn 3: narrowed diagnostic investigation
├── Turn 4: maintenance or executive decision
└── Expected memory, evidence, tools, and safety behavior
```

---

## Separation of Ground Truth and Observed Metrics

Do **not** mix benchmark definitions with runtime results.

### Scenario Definition Files

Scenario definitions live under:

```text
evaluation/conversational_scenarios/
```

They contain:

- expected behavior
- expected intent
- expected tool usage
- expected evidence anchors
- expected memory behavior
- safety and scope rules
- difficulty tier
- scenario metadata

They should **not** contain runtime scores such as `tool_f1`, `latency_ms`, or `memory_score`.

### Runtime Result Files

Runtime results are generated per evaluation run under:

```text
evaluation/artifacts/<run_id>/
```

They contain:

- actual output
- actual intent
- actual tools
- SQL hashes/previews
- token usage
- latency
- failures
- score dimensions
- hard gate results
- benchmark deltas

This separation is required for replayability, reproducibility, and future benchmark comparison.

---

## Local Artifact Design

Each evaluation run creates one directory:

```text
evaluation/artifacts/eval_YYYYMMDD_HHMMSS_xxxxxx/
```

Artifacts:

```text
run_manifest.json
scenario_catalog_snapshot.json
turns.jsonl
scenarios_rollup.jsonl
sql_events.jsonl
sql_events.csv
metrics.csv
tokens.csv
scenarios_rollup.csv
summary.json
benchmark_delta.json
benchmark_delta.csv
optional_xlsx/benchmark_summary.xlsx
replay/manifest.json
```

### `run_manifest.json`

The replay manifest captures exactly what was evaluated and under what conditions.

It includes:

- run id and label
- scenario files
- dataset hash
- model and prompt version
- git commit
- graph version
- tool registry version
- evaluation code version
- LangSmith project/enabled flag
- environment snapshot
- `AI_ANALYST_PERSIST_OBSERVABILITY=0`

### `turns.jsonl`

One JSON object per scenario turn.

It includes:

- request_id
- scenario_id
- difficulty tier
- category
- elapsed_ms
- actual/expected intent
- actual/expected tools
- gates
- dimensions
- memory metrics
- efficiency metrics
- token usage
- lineage
- LangSmith correlation fields

### `sql_events.jsonl`

One JSON object per SQL/tool event.

The implementation persists only:

- `sql_hash`
- masked `sql_preview`
- success flag
- row count
- duration
- node
- tool
- request_id

It does **not** persist full raw SQL.

---

## Scenario Categories

The initial suite covers:

1. Fleet Investigation
2. Vehicle Diagnostics
3. DTC Investigation
4. Maintenance Prioritization
5. Driver Behavior Correlation
6. Co-occurrence Investigation
7. Executive Summary
8. Multi-Customer Isolation
9. Long Conversational Memory
10. Adversarial / Edge Cases

Each scenario also has a difficulty tier:

```text
Easy        = 1-step retrieval
Medium      = multi-tool but direct synthesis
Hard        = investigative continuity and multi-turn context
Expert      = ambiguous/root-cause reasoning with uncertainty
Adversarial = safety, scope, or stability challenge
```

---

## Version Lineage Metadata

Every run and every turn should capture:

| Field | Why |
|---|---|
| `git_commit` | Reproduce backend code state |
| `graph_version` | Detect LangGraph structure/prompt graph changes |
| `tool_registry_version` | Detect tool schema or SQL policy changes |
| `dataset_hash` | Ensure benchmark scenario consistency |
| `evaluation_code_version` | Detect scoring logic changes |
| `model_name` | Compare model behavior |
| `prompt_version` | Compare prompt behavior |
| `release_version` | Tie to deployment |
| `service_version` | Tie to backend service version |

Without this lineage, benchmark score changes become impossible to diagnose.

---

## Scoring Philosophy

Do **not** rely on one global `weighted_score`.

A single score hides dangerous failures.

Instead, use:

1. hard gates
2. score dimensions
3. optional summary labels

### Hard Gates

Required gates:

```json
{
  "safety_ok": true,
  "evidence_ok": true,
  "sql_ok": true,
  "scope_ok": true,
  "contradictions_ok": true,
  "repetition_ok": true
}
```

Gate definitions:

| Gate | Meaning | Failure Example |
|---|---|---|
| `safety_ok` | no unsafe advice, no tenant leakage | shows another customer's data |
| `evidence_ok` | claims grounded in tool/SQL evidence | severity stated without DTC lookup |
| `sql_ok` | read-only, allowed SQL only | INSERT, CREATE, DELETE, unsafe query |
| `scope_ok` | customer/vehicle/DTC filters preserved | query omits customer filter |
| `contradictions_ok` | no severe self-conflicts | says 0 faults then 5 faults |
| `repetition_ok` | no excessive repeated SQL/tools | same SQL hash called 5 times |

### Score Dimensions

Dimensions are reported separately:

```json
{
  "task_fulfillment": 0.91,
  "factual_grounding": 0.88,
  "safety": 1.0,
  "tool_use": 0.95,
  "sql_hygiene": 0.93,
  "memory": 0.89,
  "efficiency": 0.77
}
```

Optional labels:

```text
PASS
PASS_WITH_WARNINGS
FAIL_GATE
FAIL_RUNTIME
FAIL_EVIDENCE
FAIL_SCOPE
```

---

## Evidence-Grounded Validation

Hallucination detection cannot rely only on keyword checks.

Fleet diagnostics requires evidence-grounded validation.

For diagnostic intents, require:

- at least one relevant tool result
- non-empty result data unless the correct answer is “not found”
- final answer references at least one evidence anchor
- severity claims align with DTC metadata when available
- recommendation urgency aligns with severity / active fault count

Initial deterministic checks include:

- numeric anchor matching
- expected field presence
- expected tool execution
- non-empty tool data
- answer mentions expected DTC/vehicle/customer

Future validators can assert:

```python
assert_claim_matches_tool_result("vehicles_affected")
assert_dtc_exists(dtc_code)
assert_severity_matches_kb(dtc_code, answer)
assert_priority_matches_fault_count(vehicle_number)
```

---

## Memory Metrics

Memory is split into actionable components:

| Metric | Meaning |
|---|---|
| `entity_retention` | remembers customer, vehicle, uniqueid, DTC, subsystem, timeframe |
| `scope_retention` | preserves customer/vehicle/DTC filters in tools and answers |
| `reasoning_continuity` | maintains investigation flow across turns |
| `contradiction_rate` | detects conflicting claims across turns |
| `redundant_analysis_rate` | detects repeated boilerplate or unnecessary repeated analysis |

---

## Efficiency Metrics

Cost explosion is a major production risk in multi-turn AI.

The runner tracks:

| Metric | Meaning |
|---|---|
| `repeated_tool_calls` | identical repeated tool invocations |
| `repeated_sql_queries` | repeated normalized SQL hashes |
| `redundant_reasoning` | repeated answer/reasoning text |
| `context_bloat_rate` | prompt context growth per turn |
| `token_growth_per_turn` | absolute token growth |

---

## SQL Privacy and Hashing

Do not store full SQL in long-term local artifacts.

The implementation stores:

```json
{
  "sql_hash": "sha256:...",
  "sql_preview": "select dtc_code, count(?) from ... where customer_name = ? ...",
  "success": true,
  "row_count": 20,
  "duration_sec": 1.42
}
```

Before hashing, SQL is normalized by:

- lowercasing
- stripping comments
- collapsing whitespace
- replacing string literals with `?`
- replacing numeric literals with `?`
- removing trailing semicolons

SQL gate checks flag:

```text
insert
update
delete
create
alter
drop
truncate
optimize
system
attach
detach
```

and inefficiencies like:

- `SELECT *`
- no `LIMIT` on detail queries
- missing customer filter when customer context exists
- repeated identical SQL hash

---

## Benchmark-Centric Evaluation

The framework supports benchmark-centric comparison:

```text
benchmark
├── baseline run
├── candidate run
├── prompt A run
├── prompt B run
└── deltas by scenario, tier, category, and gate
```

For valid comparison:

- same `dataset_hash`
- same scenario IDs
- same or explicitly different `evaluation_code_version`
- same customer simulation context unless intentionally varied
- lineage captured for both runs

`benchmark_delta.json` reports:

- gate pass rate delta
- dimension deltas
- regressions
- win/loss/draw-style signals

---

## How to Run

Dry run without calling OpenAI:

```powershell
$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_DRY_RUN="1"
python evaluation/conversational_runner.py --limit-scenarios 2
```

Real local-only run:

```powershell
$env:AI_ANALYST_PERSIST_OBSERVABILITY="0"
$env:EVAL_LOCAL_ONLY="1"
python evaluation/conversational_runner.py --limit-scenarios 3 --max-turns 2
```

Optional Excel export:

```powershell
$env:EVAL_WRITE_EXCEL="1"
python evaluation/conversational_runner.py --limit-scenarios 3
```

Compare with a previous baseline:

```powershell
python evaluation/conversational_runner.py --baseline-dir evaluation/artifacts/eval_YYYYMMDD_HHMMSS_xxxxxx
```

---

## Operational Dashboards Without ClickHouse

For now, dashboards should be local-file based.

Options:

1. pandas notebook
2. Streamlit local app
3. Excel workbook
4. simple CLI summary

Example local views:

- gate pass rate by difficulty
- factual grounding by category
- memory score by turn index
- token growth by scenario
- repeated SQL by scenario
- regressions vs baseline
- top violated gates
- adversarial pass/fail summary

No ClickHouse dashboard tables until the evaluation schema stabilizes.

---

## Load and Scale Testing Position

Do not mix load testing artifacts into production observability tables.

For load testing:

- keep local artifacts
- use separate run labels
- disable observability persistence unless explicitly testing observability ingestion
- sample LangSmith traces only if needed

Load scenarios:

```text
100 concurrent users
500 concurrent users
1000 concurrent users
```

Metrics:

- p50 / p95 / p99 latency
- timeout rate
- OpenAI failures
- ClickHouse read query failures
- repeated SQL/query amplification
- token growth
- memory/context bloat
- tool bottlenecks
- graph node bottlenecks

---

## Critical Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Heuristic scoring can lie | use hard gates, evidence validation, manual review |
| One global score hides unsafe behavior | report dimensions and gates separately |
| Memory evaluation can be vague | split into entity, scope, continuity, contradiction, redundancy |
| SQL artifacts can leak sensitive information | store only masked preview + hash |
| Evaluation artifacts become too large | JSONL append-only, previews by default, no raw SQL |
| Benchmark comparisons become invalid | require dataset_hash and evaluation_code_version |
| Eval traffic pollutes observability | force `AI_ANALYST_PERSIST_OBSERVABILITY=0` |

---

## Acceptance Criteria

The implementation is acceptable when:

1. No new ClickHouse tables are created.
2. Evaluation runs set `AI_ANALYST_PERSIST_OBSERVABILITY=0`.
3. All evaluation artifacts are stored locally.
4. Scenario definitions and runtime results are separated.
5. Runtime artifacts include version lineage.
6. SQL is stored only as masked preview + hash.
7. Results include hard gates and score dimensions.
8. Memory metrics are split into actionable submetrics.
9. Efficiency metrics detect repeated tools, repeated SQL, and token growth.
10. Difficulty tiers are present and reported.
11. Replay manifests are written.
12. Baseline/candidate comparisons are supported locally.
13. LangSmith correlation works through `request_id` when enabled.
14. The system works even when LangSmith is disabled.

---

## Final Direction

This is the correct maturity path:

```text
Local scenario definitions
        ↓
Local conversational runner
        ↓
Existing AI Analyst / LangGraph
        ↓
Local JSONL + CSV + optional XLSX artifacts
        ↓
LangSmith trace correlation
        ↓
Benchmark-centric regression analysis
```

Do **not** create new ClickHouse tables.

Do **not** persist evaluation runs into ClickHouse.

Do **not** let synthetic evaluation traffic pollute production observability.

Keep the framework local, transparent, replayable, and easy to evolve until the evaluation schema and metrics are stable enough to justify a warehouse-backed analytics layer later.
