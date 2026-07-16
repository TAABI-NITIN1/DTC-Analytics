# Taabi AI Analyst — Evaluation Analytics Blueprint

## Goal

Build a complete analytics layer for all AI evaluation runs — not only quality checking, but **operational intelligence** for a production AI system.

The analytics system answers:

- Is the AI trustworthy, scalable, operationally stable, and economically viable?
- Where and why does it fail?
- Which workflows are expensive or slow?
- Which customers/workflows are risky?

## Five layers

| Layer | Purpose |
|-------|---------|
| **Run** | Overall benchmark execution, leadership reporting |
| **Session** | Full conversation behavior, memory, efficiency |
| **Turn** | Single-response debugging and inspection |
| **Trace** | LangGraph/tool/SQL internal flow |
| **System** | Infrastructure readiness (CPU, CH, API limits) — *future* |

## Dashboards (static Vite app)

1. **Executive AI Health** — health score, pass rate, cost, latency, safety
2. **Cost** — USD/tokens by category, customer, expensive sessions
3. **Speed** — latency percentiles, node/SQL timing
4. **Quality** — judges, grounding, gates
5. **Conversation** — multi-turn depth, follow-up sources, coverage
6. **Tool & SQL** — tool frequency, SQL success, repeated queries
7. **Failure** — taxonomy, high-risk sessions
8. **Experiment** — baseline vs candidate deltas, regression alerts

## Data sources (local artifacts)

```
evaluation/artifacts/<run_id>/
  session_turns.jsonl | turns.jsonl
  session_rollups.jsonl | scenarios_rollup.jsonl
  validation/           # Phase 1 rich findings
  sql_events.jsonl
  trace_events.jsonl
  analytics_summary.json      # python -m evaluation.analytics summarize
  dashboard_bundle.json       # python -m evaluation.analytics export-dashboard
```

## Workflow

```powershell
# From project root with venv active
python -m evaluation.analytics summarize --run-id eval_20260520_085108_190cf6
python -m evaluation.analytics export-dashboard --run-id eval_20260520_085108_190cf6 --baseline eval_20260520_065502_5e18bb --copy-to-eval-dashboard

cd eval-dashboard
npm install
npm run dev    # http://localhost:5174
```

Notebook: [`ai_fleet_evaluation_analytics.ipynb`](../ai_fleet_evaluation_analytics.ipynb) — exploratory charts via Plotly.

## Known gaps (honest)

| Area | Status |
|------|--------|
| Phase 2 production run | Judges + rollups; limited validation findings |
| Memory/efficiency dimensions | Rich on Phase 1 `turns.jsonl` only |
| Trace node timing | Requires `trace_events.jsonl` from runner |
| System/infra metrics | Not in eval artifacts — dashboard shows data-gap banner |

## Philosophy

We are evolving from “AI chatbot” to **reliable industrial AI system**. This analytics layer is the **operational nervous system** of the platform.
