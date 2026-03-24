import threading
import functools
import uuid

import json

# ── Observability: Node-level tracing decorator ────────────────
def trace_node(name):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(state):
            start = time.time()
            req_id = state.get('request_id', str(uuid.uuid4()))
            customer_name = state.get('context', {}).get('customer_name', None)
            thread_name = threading.current_thread().name
            log.info(f"[{req_id}] [NODE START] {name}")
            try:
                result = func(state)
                duration = round(time.time() - start, 3)
                log.info(f"[{req_id}] [NODE END] {name} | time={duration}s")
                # Structured trace
                if 'trace_log' in state:
                    state['trace_log'].append({
                        'type': 'node',
                        'node': name,
                        'time': time.time(),
                        'duration': duration,
                        'input': dict(state),
                        'output': dict(result),
                        'customer_name': customer_name,
                        'thread': thread_name,
                    })
                return result
            except Exception as e:
                log.error(f"[{req_id}] [NODE ERROR] {name}: {e}")
                if 'trace_log' in state:
                    state['trace_log'].append({
                        'type': 'node',
                        'node': name,
                        'time': time.time(),
                        'error': str(e),
                        'customer_name': customer_name,
                        'thread': thread_name,
                    })
                raise
        return wrapper
    return decorator

# ── Observability: LLM call tracing ───────────────────────────
def trace_llm_call(llm, messages, label="LLM", req_id=None):
    start = time.time()
    log.info(f"[{req_id}] [{label} START]")
    log.info(f"[{label} PROMPT]\n{messages}")
    response = llm.invoke(messages)
    duration = round(time.time() - start, 3)
    log.info(f"[{req_id}] [{label} END] time={duration}s")
    log.info(f"[{label} RESPONSE]\n{getattr(response, 'content', response)}")
    return response
"""
AI Analyst — Fleet intelligence copilot powered by GPT.

Architecture:
  Intent detection → Structured tool calls → DTC knowledge retrieval →
  Diagnostic reasoning → Structured response with optional chart.

Uses OpenAI function-calling with domain-specific analytics tools:
  1. get_fleet_health       — fleet-wide health snapshot
  2. get_fleet_dtc_distribution — top DTC codes across fleet
  3. get_fleet_trends       — fleet health trends over time
  4. get_vehicle_health     — per-vehicle health details
  5. get_vehicle_faults     — vehicle fault episodes
  6. get_dtc_details        — DTC knowledge base lookup
  7. get_dtc_cooccurrence   — fault combination patterns
  8. get_maintenance_priority — vehicles needing urgent maintenance
  9. run_sql                — fallback raw SQL for advanced queries
  10. generate_chart        — structured chart config for frontend

Env: OPENAI_API_KEY must be set.
"""
import concurrent.futures
import json
import logging
import os
import re
import time
from typing import Annotated, TypedDict

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages

from src.clickhouse_utils import get_clickhouse_client
from src.clickhouse_utils_v2 import V2_TABLES
from pathlib import Path

log = logging.getLogger(__name__)

# ── Observability: MLflow (experiment metrics) ──────────────────
try:
    import mlflow
    _mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', '')
    if _mlflow_uri:
        mlflow.set_tracking_uri(_mlflow_uri)
        mlflow.set_experiment('taabi_ai_analyst')
        log.info('MLflow tracking enabled → %s', _mlflow_uri)
except ImportError:
    mlflow = None  # type: ignore[assignment]
    log.info('mlflow not installed — metrics disabled')

MAX_RESULT_ROWS = 500

# Allowed tables for run_sql (lowercase)
_ALLOWED_TABLES = {t.lower() for t in V2_TABLES.values()}

# RAG docs location
_RAG_PATH = Path(__file__).resolve().parents[1] / 'docs' / 'rag'

def _get_rag_snippets(context: dict | None = None, max_chars: int = 1000) -> str:
    """Load small helpful snippets from docs/rag for the LLM to ground on.
    Returns a short concatenated string (may be empty).
    """
    try:
        if not _RAG_PATH.exists():
            return ''
        parts = []
        for p in sorted(_RAG_PATH.glob('*.md')):
            try:
                text = p.read_text(encoding='utf-8').strip()
                if not text:
                    continue
                # Keep only the first chunk of each doc
                parts.append(f"### {p.name}\n" + text[:max_chars])
            except Exception:
                continue
        joined = '\n\n'.join(parts)
        return joined[:max_chars]
    except Exception:
        return ''

# ── Prompt version registry ─────────────────────────────────────
# Bump a version string whenever you edit a prompt to track performance
# changes in MLflow. Format: "v<major>" (e.g. "v2" after a major rewrite).
PROMPT_VERSIONS = {
    "intent":         "v1",
    "investigate":    "v1",
    "fault_analysis": "v1",
    "root_cause":     "v1",
    "maintenance":    "v1",
    "recommendation": "v1",
    "explain":        "v1",
}


def _build_version_metadata() -> dict:
    """Build canonical release/version metadata for observability and eval tracking."""
    return {
        'release_version': os.getenv('AI_ANALYST_RELEASE_VERSION', ''),
        'service_version': os.getenv('AI_ANALYST_SERVICE_VERSION', ''),
        'git_commit': os.getenv('GIT_COMMIT_SHA', ''),
        'env_name': os.getenv('DEPLOYMENT_ENV', os.getenv('ENV_NAME', 'dev')),
        'model_name': os.getenv('AI_ANALYST_MODEL_NAME', 'gpt-5.4-mini'),
        'dataset_version': os.getenv('AI_ANALYST_DATASET_VERSION', ''),
        'prompt_versions': PROMPT_VERSIONS,
    }

# ── System prompt — Taabi AI Analyst ────────────────────────────

SYSTEM_PROMPT = """\
You behave like a helpful assistant.

If the user message is casual (greeting, thanks, small talk), respond naturally and briefly like a normal conversation.

Do NOT perform any analytics or tool calls for casual messages.

Only perform deep analysis when the user asks a data-related question.

You are **Taabi AI Analyst**, a highly experienced **Fleet Diagnostics and Vehicle Health Intelligence Specialist**.

Your job is to analyze fleet telemetry, diagnostic trouble codes (DTC), vehicle health analytics, and fleet operational patterns to help fleet owners understand vehicle issues, reduce downtime, improve maintenance planning, and improve driver behavior.

You are not a generic chatbot.
You behave like a **senior fleet engineer and vehicle diagnostics expert** who works with telematics, OBD/CAN data, and heavy commercial fleets.

You analyze data from the Taabi Fleet Intelligence Platform and explain insights clearly for fleet owners, maintenance teams, and operations managers.

Your analysis is based on three major information sources:

1. Fleet analytics tables generated from vehicle telemetry
2. Vehicle diagnostic knowledge stored in the DTC knowledge base
3. Context from the current dashboard page (fleet level, vehicle level, or DTC level)

You always combine these sources to produce accurate diagnostic insights.

---

## ROLE AND EXPERTISE

You have deep expertise in:

- OBD / CAN bus diagnostics
- Commercial vehicle maintenance and fleet management
- Diagnostic Trouble Codes (SAE and manufacturer specific)
- Heavy vehicle subsystems (powertrain, braking, emissions, electrical, sensors)
- Telematics and vehicle health monitoring
- Driver behavior and operational efficiency
- Predictive maintenance and fleet risk analysis

You think and respond like an **experienced fleet operations analyst**. and whenever you are answering you should answering like you are explaining to non technical person.

---

## PLATFORM CONTEXT

The Taabi platform collects telemetry from vehicles every few seconds using OBD and CAN systems.

From this data, Taabi calculates:

- Active DTC faults
- Fault episodes (start time → end time)
- Vehicle health score
- Fleet fault distribution
- Maintenance priority
- System health trends
- DTC co-occurrence patterns
- Resolution time of faults
- Fault frequency per vehicle and fleet

Your responses should interpret these analytics for the fleet owner.

---

## DATA SOURCES YOU MAY USE

The system may provide data from these analytics tables:

Fleet analytics tables:
- fleet_health_summary
- fleet_dtc_distribution
- fleet_system_health
- fleet_fault_trends

Vehicle analytics tables:
- vehicle_health_summary
- vehicle_fault_master

Maintenance analytics tables:
- maintenance_priority

Diagnostic analysis tables:
- dtc_fleet_impact
- dtc_cooccurrence

Reference knowledge tables:
- dtc_master (description, causes, symptoms, severity, repair guidance)
- vehicle_master (vehicle metadata)

Always rely on these analytics when answering questions.

---

## HOW TO ANALYZE QUESTIONS

When a user asks a question, follow this reasoning structure:

**Step 1 — Identify the context**
Determine whether the user is asking about:
- Fleet level
- Vehicle level
- Specific DTC code
- Maintenance planning
- Fault patterns
- Driver behavior impact

**Step 2 — Interpret the analytics**
Analyze the relevant metrics such as:
- number of affected vehicles
- fault frequency
- episode duration
- severity level
- subsystem impact
- recurrence patterns
- fault co-occurrence

**Step 3 — Explain the technical meaning**
Translate the analytics into understandable insights.

**Step 4 — Provide operational impact**
Explain what this means for the fleet owner.

Examples:
- safety risk
- fuel efficiency impact
- downtime risk
- emissions compliance
- potential mechanical damage

**Step 5 — Provide recommended actions**
Suggest clear next steps such as:
- inspection procedures
- component checks
- sensor diagnostics
- driver training recommendations
- maintenance scheduling
- fleet policy adjustments

---

## HOW TO STRUCTURE YOUR ANSWERS

Your responses should follow this format when possible:

1. Summary of the issue
2. Key analytics insight
3. Technical explanation of the fault
4. Impact on vehicle or fleet operations
5. Recommended maintenance actions
6. Preventive recommendations

Always prioritize **clear and actionable insights** rather than raw data.

---

## WHEN ANALYZING DTC CODES

When discussing a specific DTC code:

Explain:
- what the code means
- which vehicle system is affected
- possible root causes
- symptoms drivers may experience
- risks if the issue continues
- recommended inspection or repair steps

If multiple vehicles have the same DTC, explain the **fleet impact and possible systemic causes**.

---

## WHEN ANALYZING FLEET DATA

If analyzing fleet-level analytics:

Focus on:
- which faults affect the most vehicles
- which systems are most problematic
- recurring faults across the fleet
- trends over time
- maintenance priorities

Provide insights that help fleet managers **reduce downtime and prevent failures**.

---

## WHEN ANALYZING VEHICLE HEALTH

If analyzing a specific vehicle:

Focus on:
- active DTC faults
- historical fault episodes
- subsystem risks
- predicted maintenance needs
- potential root causes

Recommend specific inspection steps for that vehicle.

---

## CHART GENERATION

If charts are requested or helpful, you may generate visual insights such as:
- fleet DTC distribution
- system fault distribution
- fault trends over time
- vehicle health comparisons
- maintenance priority ranking

Charts should help users understand patterns quickly.
When generating charts, you MUST pass the actual data objects array from tool results, not empty arrays.

---

## SAFETY AND RELIABILITY

Always prioritize vehicle safety and operational reliability.

If a DTC indicates a critical system (braking, emissions, engine, CAN network), clearly highlight the risk.

Do not guess when data is missing.
If insufficient data is available, state that clearly.

---

## COMMUNICATION STYLE

Your tone should be:
- professional
- analytical
- technical but understandable
- concise but insightful

Avoid generic language.

You are a **diagnostic expert explaining vehicle health analytics to a fleet manager**.

Format numbers with commas (e.g. 1,234 not 1234).

---

## PRIMARY OBJECTIVE

Your main goal is to help fleet owners:
- detect problems early
- reduce breakdowns
- prioritize maintenance
- improve fleet reliability
- improve driver behavior
- reduce operational costs

---

## AVAILABLE TOOLS

Use these structured tools instead of raw SQL when possible:
- **get_fleet_health**: Fleet-wide health snapshot (scores, fault counts, common DTCs, trends)
- **get_fleet_dtc_distribution**: Top DTC codes across the fleet with severity, vehicles affected
- **get_fleet_trends**: Fleet health trends over time (daily metrics, new/resolved faults)
- **get_fleet_system_health**: Health breakdown by vehicle system (engine, emission, safety, etc.)
- **get_vehicle_health**: Per-vehicle health details with system flags and fault summary
- **get_vehicle_faults**: Detailed fault episodes for a specific vehicle
- **get_dtc_details**: DTC knowledge base — description, causes, symptoms, repair actions
- **get_dtc_cooccurrence**: Fault combination patterns (which DTCs appear together)
- **get_dtc_fleet_impact**: DTC codes ranked by fleet-wide impact and risk score
- **get_maintenance_priority**: Vehicles/faults ranked by maintenance urgency with priority scores
- **run_sql**: Fallback for complex queries not covered by structured tools
- **generate_chart**: Create charts for visual analytics

---

## DATA TABLES (V2 — all analytics)

All data is in V2 analytics tables. DTC code '0' has been excluded from all tables.

Fact tables:
- vehicle_fault_master_ravi_v2: episode_id, clientLoginId, uniqueid, vehicle_number, customer_name, model, manufacturing_year, dtc_code, system, subsystem, description, severity_level (1-5), first_ts (unix), last_ts (unix), event_date, occurrence_count, resolution_time_sec, is_resolved (0/1), gap_from_previous_episode, engine_cycles_during, driver_related (0/1), has_engine_issue, has_coolant_issue, has_safety_issue, has_emission_issue, has_electrical_issue, vehicle_health_score
- dtc_events_exploded_ravi_v2: clientLoginId, uniqueid, vehicle_number, ts (unix), dtc_code, lat, lng, dtc_pgn

Fleet-level summaries:
- fleet_health_summary_ravi_v2: clientLoginId, total_vehicles, vehicles_with_active_faults, vehicles_with_critical_faults, driver_related_faults, fleet_health_score, most_common_dtc, most_common_system, active_fault_trend
- fleet_dtc_distribution_ravi_v2: clientLoginId, dtc_code, description, system, subsystem, severity_level, vehicles_affected, active_vehicles, total_occurrences, total_episodes, avg_resolution_time, driver_related_count
- fleet_system_health_ravi_v2: clientLoginId, system, vehicles_affected, active_faults, critical_faults, risk_score, trend
- fleet_fault_trends_ravi_v2: clientLoginId, date, active_faults, new_faults, resolved_faults, driver_related_faults, fleet_health_score

Vehicle-level summaries:
- vehicle_health_summary_ravi_v2: clientLoginId, uniqueid, vehicle_number, customer_name, active_fault_count, critical_fault_count, total_episodes, episodes_last_30_days, avg_resolution_time, last_fault_ts, vehicle_health_score, driver_related_faults, most_common_dtc, has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue

DTC-level analytics:
- dtc_fleet_impact_ravi_v2: clientLoginId, dtc_code, system, subsystem, vehicles_affected, active_vehicles, avg_resolution_time, driver_related_ratio, fleet_risk_score
- dtc_cooccurrence_ravi_v2: clientLoginId, dtc_code_a, dtc_code_b, cooccurrence_count, vehicles_affected, avg_time_gap_sec, last_seen_ts

Maintenance:
- maintenance_priority_ravi_v2: clientLoginId, uniqueid, vehicle_number, dtc_code, description, severity_level, fault_duration_sec, episodes_last_30_days, maintenance_priority_score, recommended_action

Dimension tables:
- dtc_master_ravi_v2: dtc_code, system, subsystem, description, primary_cause, secondary_causes, symptoms, impact_if_unresolved, severity_level, safety_risk_level, action_required, repair_complexity, fleet_management_action, recommended_preventive_action, driver_related, driver_behaviour_category, driver_behaviour_trigger, driver_training_required
- vehicle_master_ravi_v2: clientLoginId, uniqueid, vehicle_number, customer_name, model, manufacturing_year, vehicle_type, solutionType

---

## RULES
- NEVER hallucinate DTC information. Only use data returned by tools.
- NEVER invent vehicle faults or vehicle numbers.
- Only write SELECT queries if using run_sql. Never modify data.
- When a customer_name filter is provided in the context, always apply it.
"""

# ── Structured tool definitions ─────────────────────────────────

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_fleet_health",
            "description": (
                "Get fleet-wide health snapshot: total vehicles, vehicles with active/critical faults, "
                "fleet health score. Optionally filter by customer_name."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name to filter by.",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of recent days to consider (default 30).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_fleet_dtc_distribution",
            "description": (
                "Get top DTC codes across the fleet ranked by occurrence count. "
                "Returns dtc_code, total occurrences, vehicles affected."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of top DTCs to return (default 10).",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of recent days (default 30).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_fleet_trends",
            "description": (
                "Get daily fleet health trends: health score, active faults, critical faults over time."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of recent days (default 30).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_vehicle_health",
            "description": (
                "Get health details for a specific vehicle: health score, active faults, "
                "critical faults, fault history. Can look up by vehicle_number or uniqueid."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "vehicle_number": {
                        "type": "string",
                        "description": "Vehicle registration number.",
                    },
                    "uniqueid": {
                        "type": "string",
                        "description": "Vehicle unique device ID.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_vehicle_faults",
            "description": (
                "Get fault episodes for a specific vehicle. Returns DTC codes, severity, "
                "resolution status, first/last seen dates."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "vehicle_number": {
                        "type": "string",
                        "description": "Vehicle registration number.",
                    },
                    "uniqueid": {
                        "type": "string",
                        "description": "Vehicle unique device ID.",
                    },
                    "unresolved_only": {
                        "type": "boolean",
                        "description": "If true, only return unresolved faults (default false).",
                    },
                    "days": {
                        "type": "integer",
                        "description": "Number of recent days (default 90).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dtc_details",
            "description": (
                "Look up DTC knowledge base for one or more DTC codes. Returns description, "
                "system, causes, symptoms, repair actions, severity, safety risk, driver-related info."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dtc_codes": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of DTC codes to look up (e.g. ['791', '792']).",
                    },
                },
                "required": ["dtc_codes"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dtc_cooccurrence",
            "description": (
                "Get DTC fault co-occurrence patterns: which DTC codes frequently appear together "
                "on the same vehicle. Helps identify root cause correlations."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "dtc_code": {
                        "type": "string",
                        "description": "DTC code to find co-occurring faults for.",
                    },
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                },
                "required": ["dtc_code"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_maintenance_priority",
            "description": (
                "Get vehicles ranked by maintenance urgency. Returns vehicle number, "
                "active fault count, critical fault count, health score, worst DTC."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of vehicles to return (default 10).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_fleet_system_health",
            "description": (
                "Get health breakdown by vehicle system (engine, emission, safety, electrical etc). "
                "Returns vehicles affected, active/critical faults, risk score per system."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_dtc_fleet_impact",
            "description": (
                "Get DTC codes ranked by fleet impact: vehicles affected, active vehicles, "
                "risk score, avg resolution time, driver-related ratio."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_name": {
                        "type": "string",
                        "description": "Optional customer name filter.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of top DTC codes to return (default 15).",
                    },
                },
                "required": [],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "run_sql",
            "description": (
                "Fallback: Execute a read-only SQL SELECT query against ClickHouse "
                "for complex analytics not covered by other tools. "
                "Prefer structured tools when possible."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "A ClickHouse-compatible SELECT query.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "generate_chart",
            "description": (
                "Generate a chart to visualize data for the user. "
                "CRITICAL: The 'data' parameter MUST contain the actual data objects from previous tool results. "
                "Example: data=[{\"dtc_code\":\"810\",\"total_occurrences\":835243},{\"dtc_code\":\"791\",\"total_occurrences\":811201}]. "
                "NEVER pass an empty array. If you don't have data yet, fetch it first with another tool."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "chart_type": {
                        "type": "string",
                        "enum": ["bar", "line", "pie", "area"],
                        "description": "Type of chart to render.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Chart title.",
                    },
                    "data": {
                        "type": "array",
                        "items": {"type": "object"},
                        "description": "Array of data objects for the chart. Must contain actual values.",
                    },
                    "x_key": {
                        "type": "string",
                        "description": "Key in data objects for X axis.",
                    },
                    "y_keys": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Keys in data objects for Y axis series.",
                    },
                },
                "required": ["chart_type", "title", "data", "x_key", "y_keys"],
            },
        },
    },
]

# ── LangGraph Agent State ───────────────────────────────────────

class AgentState(TypedDict):
    # Conversation history — only user messages + final AI reply live here.
    # All intermediate reasoning is stored in dedicated fields below.
    messages: Annotated[list, add_messages]
    context: dict
    chart_config: dict | None
    last_tool_data: list
    # Diagnostic pipeline fields
    intent: str                          # detected by Node 1
    tool_results: dict                   # raw data fetched by Node 3
    fault_analysis: str                  # summary from Node 4
    root_cause: str                      # reasoning from Node 5
    maintenance_priority_assessment: str  # urgency from Node 6
    recommendation: str                  # structured P/C/I/A from Node 7
    # Observability fields (populated incrementally as nodes execute)
    nodes_executed: list                 # node names appended on entry
    failure_reasons: list                # failure codes appended on detection
    token_usage: dict                    # {"prompt": int, "completion": int} accumulated
    tools_used: list                     # tool names called (populated by Node 3)


# ── Read-only SQL guard ─────────────────────────────────────────

_FORBIDDEN_PATTERN = re.compile(
    r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|RENAME|ATTACH|DETACH|GRANT|REVOKE|KILL)\b',
    re.IGNORECASE,
)


def _validate_sql(query: str) -> str | None:
    """Return an error message if the query is unsafe, else None."""
    stripped = query.strip()
    upper = stripped.lstrip('(').upper()
    if not (upper.startswith('SELECT') or upper.startswith('WITH')):
        return "Only SELECT / WITH queries are allowed."
    if _FORBIDDEN_PATTERN.search(stripped):
        return "Query contains a forbidden keyword. Only read-only queries are allowed."
    # Check referenced tables against allowlist
    # Find FROM and JOIN table names
    found = []
    for m in re.finditer(r"\b(?:FROM|JOIN)\s+([^\s,;()]+)", query, re.IGNORECASE):
        tbl = m.group(1).strip()
        # remove surrounding quotes/backticks and db prefix
        tbl = tbl.strip('`"')
        if '.' in tbl:
            tbl = tbl.split('.')[-1]
        found.append(tbl.lower())
    for tbl in found:
        if tbl and tbl not in _ALLOWED_TABLES:
            return f"Query references disallowed table: {tbl}. Allowed tables: {_ALLOWED_TABLES}"
    return None


# ── Helper: execute SQL safely ──────────────────────────────────

def _exec_sql(query: str) -> list[dict]:
    """Execute SQL and return list of dicts with column names as keys."""
    q = query.rstrip().rstrip(';')
    if 'LIMIT' not in q.upper():
        q += f' LIMIT {MAX_RESULT_ROWS}'
    client = get_clickhouse_client()
    columns, data_rows = client.query_df(q)
    rows = []
    for row in data_rows[:MAX_RESULT_ROWS]:
        d = {}
        for i, col in enumerate(columns):
            val = row[i] if i < len(row) else None
            d[col] = val if isinstance(val, (int, float, str, bool, type(None))) else str(val)
        rows.append(d)
    return rows

def _safe_exec(query: str) -> dict:
    req_id = str(uuid.uuid4())

    try:
        log.info(f"[{req_id}] [SQL QUERY]\n{query}")
        rows = _exec_sql(query)
        return {"data": rows, "count": len(rows)}

    except Exception as exc:
        log.error(f"[{req_id}] [SQL ERROR] {exc}")
        return {"error": str(exc)}
# def _safe_exec(query: str) -> dict:
#     """Execute SQL safely returning {data, count} or {error}."""
#     req_id = str(uuid.uuid4())
#     try:
#         log.info(f"[{req_id}] [SQL QUERY]\n{query}")
#         rows = _exec_sql(query)
#         log.info(f"[{req_id}] [SQL RESULT] rows={len(rows)}")
#         return {"data": rows, "count": len(rows)}
#     except Exception as exc:
#         log.error(f"[{req_id}] [SQL ERROR] {exc} — query: {query[:200]}")
#         return {"error": f"Query failed: {exc}"}


# ── Structured tool implementations ────────────────────────────

def _tool_get_fleet_health(args: dict) -> dict:
    customer = args.get("customer_name", "")
    days = args.get("days", 30)
    cust_filter = f"AND customer_name = '{customer}'" if customer else ""
    summary = _safe_exec(f"""
        SELECT
            count() as total_vehicles,
            sumIf(1, active_fault_count > 0) as vehicles_with_active_faults,
            sumIf(1, critical_fault_count > 0) as vehicles_with_critical_faults,
            round(avg(vehicle_health_score), 1) as avg_health_score,
            sum(active_fault_count) as total_active_faults,
            sum(critical_fault_count) as total_critical_faults,
            sum(driver_related_faults) as total_driver_related_faults
        FROM vehicle_health_summary_ravi_v2
        WHERE 1=1 {cust_filter}
    """)
    if customer:
        snap = _safe_exec(f"""
            SELECT
                uniqExact(uniqueid) as total_vehicles,
                uniqExactIf(uniqueid, is_resolved = 0) as vehicles_with_active_faults,
                uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3) as vehicles_with_critical_faults,
                countIf(driver_related = 1) as driver_related_faults,
                topKWeighted(1)(dtc_code, occurrence_count)[1] as most_common_dtc,
                topKWeighted(1)(system, occurrence_count)[1] as most_common_system
            FROM vehicle_fault_master_ravi_v2
            WHERE customer_name = '{customer}'
        """)
    else:
        snap = _safe_exec("""
            SELECT total_vehicles, vehicles_with_active_faults,
                   vehicles_with_critical_faults, driver_related_faults,
                   fleet_health_score, most_common_dtc, most_common_system,
                   active_fault_trend
            FROM fleet_health_summary_ravi_v2
            LIMIT 1
        """)
    if customer:
        trend = _safe_exec(f"""
            SELECT
                event_date as date,
                count() as active_faults,
                countIf(is_resolved = 1) as resolved_faults,
                countIf(driver_related = 1) as driver_related_faults
            FROM vehicle_fault_master_ravi_v2
            WHERE customer_name = '{customer}'
            GROUP BY event_date
            ORDER BY event_date DESC
            LIMIT 7
        """)
    else:
        trend = _safe_exec("""
            SELECT date, fleet_health_score, active_faults, resolved_faults,
                   driver_related_faults
            FROM fleet_fault_trends_ravi_v2
            ORDER BY date DESC
            LIMIT 7
        """)
    return {"summary": summary, "fleet_snapshot": snap, "recent_trend": trend}


def _tool_get_fleet_dtc_distribution(args: dict) -> dict:
    customer = args.get("customer_name", "")
    limit = args.get("limit", 10)
    if customer:
        return _safe_exec(f"""
            SELECT
                f.dtc_code,
                any(f.description) as description,
                any(f.system) as system,
                any(f.subsystem) as subsystem,
                any(f.severity_level) as severity_level,
                uniqExact(f.uniqueid) as vehicles_affected,
                uniqExactIf(f.uniqueid, f.is_resolved = 0) as active_vehicles,
                sum(f.occurrence_count) as total_occurrences,
                count() as total_episodes,
                if(countIf(f.resolution_time_sec > 0) > 0,
                   round(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                         / countIf(f.resolution_time_sec > 0), 1), 0) as avg_resolution_time,
                countIf(f.driver_related = 1) as driver_related_count
            FROM vehicle_fault_master_ravi_v2 f
            WHERE f.customer_name = '{customer}'
            GROUP BY f.dtc_code
            ORDER BY total_occurrences DESC
            LIMIT {int(limit)}
        """)
    return _safe_exec(f"""
        SELECT
            dtc_code, description, system, subsystem, severity_level,
            vehicles_affected, active_vehicles, total_occurrences,
            total_episodes, round(avg_resolution_time, 1) as avg_resolution_time,
            driver_related_count
        FROM fleet_dtc_distribution_ravi_v2
        ORDER BY total_occurrences DESC
        LIMIT {int(limit)}
    """)


def _tool_get_fleet_trends(args: dict) -> dict:
    customer = args.get("customer_name", "")
    days = args.get("days", 30)
    if customer:
        return _safe_exec(f"""
            SELECT
                event_date as date,
                count() as active_faults,
                count() as new_faults,
                countIf(is_resolved = 1) as resolved_faults,
                countIf(driver_related = 1) as driver_related_faults
            FROM vehicle_fault_master_ravi_v2
            WHERE customer_name = '{customer}'
            GROUP BY event_date
            ORDER BY event_date ASC
        """)
    return _safe_exec("""
        SELECT
            date,
            fleet_health_score,
            active_faults,
            new_faults,
            resolved_faults,
            driver_related_faults
        FROM fleet_fault_trends_ravi_v2
        ORDER BY date ASC
    """)


def _tool_get_vehicle_health(args: dict) -> dict:
    v_num = args.get("vehicle_number", "")
    uid = args.get("uniqueid", "")
    if v_num:
        where = f"vehicle_number = '{v_num}'"
    elif uid:
        where = f"uniqueid = '{uid}'"
    else:
        return {"error": "Provide vehicle_number or uniqueid."}
    status = _safe_exec(f"""
        SELECT uniqueid, vehicle_number, customer_name,
               vehicle_health_score, active_fault_count, critical_fault_count,
               total_episodes, episodes_last_30_days,
               round(avg_resolution_time, 1) as avg_resolution_time,
               driver_related_faults, most_common_dtc,
               has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue
        FROM vehicle_health_summary_ravi_v2
        WHERE {where}
        LIMIT 1
    """)
    # Also fetch fault detail from vehicle_fault_master
    faults = _safe_exec(f"""
        SELECT dtc_code, system, description, count() as episodes,
               max(severity_level) as max_severity,
               sumIf(1, is_resolved = 0) as unresolved,
               min(first_ts) as earliest_ts, max(last_ts) as latest_ts,
               round(avg(resolution_time_sec), 0) as avg_resolution_sec
        FROM vehicle_fault_master_ravi_v2
        WHERE {where}
        GROUP BY dtc_code, system, description
        ORDER BY unresolved DESC, episodes DESC
        LIMIT 15
    """)
    return {"status": status, "fault_summary": faults}


def _tool_get_vehicle_faults(args: dict) -> dict:
    v_num = args.get("vehicle_number", "")
    uid = args.get("uniqueid", "")
    unresolved_only = args.get("unresolved_only", False)
    days = args.get("days", 90)
    if v_num:
        where = f"vehicle_number = '{v_num}'"
    elif uid:
        where = f"uniqueid = '{uid}'"
    else:
        return {"error": "Provide vehicle_number or uniqueid."}
    resolved_filter = "AND is_resolved = 0" if unresolved_only else ""
    return _safe_exec(f"""
        SELECT episode_id, dtc_code, system, subsystem, description,
               severity_level, is_resolved, event_date,
               first_ts, last_ts, occurrence_count,
               resolution_time_sec, driver_related,
               vehicle_health_score
        FROM vehicle_fault_master_ravi_v2
        WHERE {where}
          AND event_date >= today() - {days}
          {resolved_filter}
        ORDER BY event_date DESC, severity_level DESC
        LIMIT 50
    """)


def _tool_get_dtc_details(args: dict) -> dict:
    codes = args.get("dtc_codes", [])
    if not codes:
        return {"error": "Provide at least one DTC code."}
    codes_str = ", ".join(f"'{c}'" for c in codes[:20])
    return _safe_exec(f"""
        SELECT dtc_code, system, subsystem, description, primary_cause,
               secondary_causes, symptoms, impact_if_unresolved,
               severity_level, safety_risk_level, action_required,
               repair_complexity, fleet_management_action,
               recommended_preventive_action, driver_related,
               driver_behaviour_category, driver_behaviour_trigger,
               driver_training_required
        FROM dtc_master_ravi_v2
        WHERE dtc_code IN ({codes_str})
    """)


def _tool_get_dtc_cooccurrence(args: dict) -> dict:
    dtc_code = args.get("dtc_code", "")
    customer = args.get("customer_name", "")
    if not dtc_code:
        return {"error": "Provide a dtc_code."}
    if customer:
        return _safe_exec(f"""
            WITH target_vehicles AS (
                SELECT DISTINCT uniqueid
                FROM vehicle_fault_master_ravi_v2
                WHERE dtc_code = '{dtc_code}' AND customer_name = '{customer}'
            )
            SELECT
                f.dtc_code as cooccurring_dtc,
                count() as cooccurrence_count,
                uniqExact(f.uniqueid) as vehicles_affected
            FROM vehicle_fault_master_ravi_v2 f
            INNER JOIN target_vehicles tv ON f.uniqueid = tv.uniqueid
            WHERE f.dtc_code != '{dtc_code}'
              AND f.customer_name = '{customer}'
            GROUP BY f.dtc_code
            ORDER BY vehicles_affected DESC
            LIMIT 15
        """)
    return _safe_exec(f"""
        SELECT
            if(dtc_code_a = '{dtc_code}', dtc_code_b, dtc_code_a) as cooccurring_dtc,
            cooccurrence_count,
            vehicles_affected,
            round(avg_time_gap_sec, 0) as avg_time_gap_sec
        FROM dtc_cooccurrence_ravi_v2
        WHERE dtc_code_a = '{dtc_code}' OR dtc_code_b = '{dtc_code}'
        ORDER BY vehicles_affected DESC
        LIMIT 15
    """)


def _tool_get_maintenance_priority(args: dict) -> dict:
    customer = args.get("customer_name", "")
    limit = args.get("limit", 10)
    cust_filter = f"AND v.customer_name = '{customer}'" if customer else ""
    return _safe_exec(f"""
        SELECT
            m.vehicle_number, m.uniqueid, v.customer_name,
            m.dtc_code, m.description, m.severity_level,
            m.maintenance_priority_score,
            m.fault_duration_sec, m.episodes_last_30_days,
            m.recommended_action,
            v.vehicle_health_score, v.active_fault_count, v.critical_fault_count
        FROM maintenance_priority_ravi_v2 m
        LEFT JOIN vehicle_health_summary_ravi_v2 v ON m.uniqueid = v.uniqueid
        WHERE 1=1 {cust_filter}
        ORDER BY m.maintenance_priority_score DESC
        LIMIT {int(limit)}
    """)


def _tool_get_fleet_system_health(args: dict) -> dict:
    customer = args.get("customer_name", "")
    if customer:
        return _safe_exec(f"""
            SELECT
                f.system,
                uniqExactIf(f.uniqueid, f.is_resolved = 0) as vehicles_affected,
                countIf(f.is_resolved = 0) as active_faults,
                countIf(f.is_resolved = 0 AND f.severity_level >= 3) as critical_faults,
                round(sumIf(
                    multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                            f.severity_level=3,7.0, 12.0),
                    f.is_resolved = 0), 1) as risk_score
            FROM vehicle_fault_master_ravi_v2 f
            WHERE f.system != '' AND f.customer_name = '{customer}'
            GROUP BY f.system
            ORDER BY risk_score DESC
        """)
    return _safe_exec("""
        SELECT system, vehicles_affected, active_faults, critical_faults,
               round(risk_score, 1) as risk_score, trend
        FROM fleet_system_health_ravi_v2
        ORDER BY risk_score DESC
    """)


def _tool_get_dtc_fleet_impact(args: dict) -> dict:
    limit = args.get("limit", 15)
    customer = args.get("customer_name", "")
    if customer:
        return _safe_exec(f"""
            SELECT
                f.dtc_code,
                any(f.system) as system,
                any(f.subsystem) as subsystem,
                uniqExact(f.uniqueid) as vehicles_affected,
                uniqExactIf(f.uniqueid, f.is_resolved = 0) as active_vehicles,
                if(countIf(f.resolution_time_sec > 0) > 0,
                   round(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
                         / countIf(f.resolution_time_sec > 0), 0), 0) as avg_resolution_time,
                if(count() > 0,
                   round(countIf(f.driver_related = 1) / count(), 3), 0) as driver_related_ratio,
                round(sumIf(
                    multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
                            f.severity_level=3,7.0, 12.0),
                    f.is_resolved = 0), 1) as fleet_risk_score
            FROM vehicle_fault_master_ravi_v2 f
            WHERE f.customer_name = '{customer}'
            GROUP BY f.dtc_code
            ORDER BY fleet_risk_score DESC
            LIMIT {int(limit)}
        """)
    return _safe_exec(f"""
        SELECT dtc_code, system, subsystem,
               vehicles_affected, active_vehicles,
               round(avg_resolution_time, 0) as avg_resolution_time,
               round(driver_related_ratio, 3) as driver_related_ratio,
               round(fleet_risk_score, 1) as fleet_risk_score
        FROM dtc_fleet_impact_ravi_v2
        ORDER BY fleet_risk_score DESC
        LIMIT {int(limit)}
    """)


# ── Tool dispatcher ─────────────────────────────────────────────

_TOOL_HANDLERS = {
    "get_fleet_health": _tool_get_fleet_health,
    "get_fleet_dtc_distribution": _tool_get_fleet_dtc_distribution,
    "get_fleet_trends": _tool_get_fleet_trends,
    "get_vehicle_health": _tool_get_vehicle_health,
    "get_vehicle_faults": _tool_get_vehicle_faults,
    "get_dtc_details": _tool_get_dtc_details,
    "get_dtc_cooccurrence": _tool_get_dtc_cooccurrence,
    "get_maintenance_priority": _tool_get_maintenance_priority,
    "get_fleet_system_health": _tool_get_fleet_system_health,
    "get_dtc_fleet_impact": _tool_get_dtc_fleet_impact,
}


# ── Shared LLM factory ───────────────────────────────────────────

def _reasoning_llm() -> ChatOpenAI:
    """LLM for reasoning nodes — no tools bound."""
    return ChatOpenAI(
        model='gpt-5.4-mini',
        api_key=os.getenv('OPENAI_API_KEY', ''),
        temperature=0.2,
        timeout=60,
        model_kwargs={
            'max_completion_tokens': 800,
        },
    )


def _tools_llm() -> ChatOpenAI:
    """LLM for data-investigation node — tools bound."""
    return ChatOpenAI(
        model='gpt-5.4-mini',
        api_key=os.getenv('OPENAI_API_KEY', ''),
        temperature=0.1,
        timeout=60,
        model_kwargs={
            'max_completion_tokens': 800,
        },
    ).bind_tools(TOOLS)


# ── Intent labels ────────────────────────────────────────────────

_DIAGNOSTIC_INTENTS = {'vehicle_investigation', 'dtc_investigation', 'fault_correlation'}
_SUMMARY_INTENTS    = {'fleet_health', 'trend_analysis', 'maintenance_prioritization', 'unknown'}

# ─────────────────────────────────────────────────────────────────
# Node 1 — Detect Intent
# ─────────────────────────────────────────────────────────────────


_INTENT_PROMPT = """\
You are an intent classifier for a fleet diagnostics AI platform.

Classify the user's message into EXACTLY ONE of these intents:

- casual_conversation → greetings, small talk, thanks, general chat
- vehicle_investigation → asking about a specific vehicle
- dtc_investigation → asking about a DTC code
- fault_correlation → asking about patterns/root causes
- fleet_health → fleet overview/KPIs
- trend_analysis → trends over time
- maintenance_prioritization → maintenance planning
- unknown → anything else

Respond with ONLY the intent label. No explanation.
"""
# _INTENT_PROMPT = """\
# You are an intent classifier for a fleet diagnostics AI platform.

# Classify the user's question into EXACTLY ONE of these intents:
# - vehicle_investigation   → asking about a specific vehicle's health, faults, or history
# - dtc_investigation       → asking about a specific DTC code's meaning, impact, or trend
# - fault_correlation       → asking about patterns, relationships between faults, root causes
# - fleet_health            → asking about overall fleet status, KPIs, or health score
# - trend_analysis          → asking about trends, changes over time, comparisons
# - maintenance_prioritization → asking which vehicles or faults need attention / scheduling
# - unknown                 → does not fit any category above

# Respond with ONLY the intent label, nothing else. No punctuation, no explanation.
# """


def _node_detect_intent(state: AgentState) -> dict:
    """Node 1 — Classify the user's question into a diagnostic intent."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['detect_intent']

    # Find the most recent human message
    user_text = ''
    for msg in reversed(state['messages']):
        if isinstance(msg, HumanMessage):
            user_text = msg.content
            break

    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=_INTENT_PROMPT),
        HumanMessage(content=user_text),
    ])
    raw = (resp.content or '').strip().lower().replace('-', '_')

    known = _DIAGNOSTIC_INTENTS | _SUMMARY_INTENTS
    intent = raw if raw in known else 'unknown'
    log.info('Intent detected: %s', intent)

    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)
    return {'intent': intent, 'nodes_executed': nodes_exec, 'token_usage': tok}


# ─────────────────────────────────────────────────────────────────
# Node 2 — Build Context (deterministic, no LLM)
# ─────────────────────────────────────────────────────────────────

def _node_build_context(state: AgentState) -> dict:
    """Node 2 — Enrich state context with intent and scope hints. No LLM call."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['build_context']
    ctx = dict(state.get('context') or {})
    ctx['_intent'] = state.get('intent', 'unknown')
    # Derive scope hints for the data investigation node
    hints = []
    if ctx.get('customer_name'):
        hints.append(f"Filter all data to customer: {ctx['customer_name']}")
    if ctx.get('vehicle_number'):
        hints.append(f"Focus on vehicle: {ctx['vehicle_number']}")
    if ctx.get('dtc_code'):
        hints.append(f"Focus on DTC code: {ctx['dtc_code']}")
    if ctx.get('mode'):
        hints.append(f"Dashboard page context: {ctx['mode']}")
    ctx['_scope_hints'] = hints
    return {'context': ctx, 'nodes_executed': nodes_exec}


# ─────────────────────────────────────────────────────────────────
# Node 3 — Investigate Data (internal tool-calling mini-loop)
# ─────────────────────────────────────────────────────────────────

def _run_tool_calls_parallel(tool_calls: list, last_data: list, chart: dict | None):
    """Execute a batch of tool calls in parallel. Returns (tool_messages, new_chart, new_data)."""
    def _run_one(tc):
        fn_name = tc['name']
        fn_args = tc['args'] if isinstance(tc['args'], dict) else {}
        tool_call_id = tc['id']
        local_chart = None
        local_data = None
        req_id = tc.get('request_id', str(uuid.uuid4()))
        customer_name = fn_args.get('customer_name', None)
        thread_name = threading.current_thread().name
        log.info(f"[{req_id}] [TOOL CALL] {fn_name} | args={fn_args} | customer={customer_name} | thread={thread_name}")
        try:
            if fn_name == 'generate_chart':
                chart_data = fn_args.get('data', [])
                if not chart_data and last_data:
                    chart_data = last_data
                local_chart = {
                    'type': fn_args.get('chart_type', 'bar'),
                    'title': fn_args.get('title', ''),
                    'data': chart_data,
                    'xKey': fn_args.get('x_key', ''),
                    'yKeys': fn_args.get('y_keys', []),
                }
                result = {'status': 'chart_created', 'note': 'Chart will render in the UI.'}
            elif fn_name == 'run_sql':
                query = fn_args.get('query', '')
                err = _validate_sql(query)
                result = {'error': err} if err else _safe_exec(query)
                if isinstance(result, dict) and 'data' in result and result['data']:
                    local_data = result['data']
            elif fn_name in _TOOL_HANDLERS:
                result = _TOOL_HANDLERS[fn_name](fn_args)
                log.info(f"[{req_id}] [TOOL RESULT] {fn_name} | rows={len(result.get('data', [])) if isinstance(result, dict) and 'data' in result else 'NA'} | customer={customer_name} | thread={thread_name}")
                if isinstance(result, dict) and 'data' in result and result['data']:
                    local_data = result['data']
                elif isinstance(result, list) and result:
                    local_data = result
            else:
                result = {'error': f'Unknown function: {fn_name}'}
        except Exception as exc:
            log.error(f"[{req_id}] [TOOL ERROR] {fn_name}: {exc} | customer={customer_name} | thread={thread_name}")
            result = {'error': str(exc)}
        return (
            ToolMessage(content=json.dumps(result, default=str), tool_call_id=tool_call_id),
            local_chart,
            local_data,
        )

    with concurrent.futures.ThreadPoolExecutor() as executor:
        outcomes = list(executor.map(_run_one, tool_calls))

    msgs, new_chart, new_data = [], chart, last_data
    for msg, lc, ld in outcomes:
        msgs.append(msg)
        if lc is not None:
            new_chart = lc
        if ld is not None:
            new_data = ld
    return msgs, new_chart, new_data



_INVESTIGATE_PROMPT = """\
You are the data retrieval agent for a fleet diagnostics system.

Your job is to call analytics tools ONLY when required.

If the user message is casual (greeting, thanks, small talk), DO NOT call any tools.

Scope hints for this request:
{scope_hints}

Call tools only if needed to answer the question.
"""
# _INVESTIGATE_PROMPT = """\
# You are the data retrieval agent for a fleet diagnostics system.

# Your ONLY job is to call the right analytics tools to gather all data needed to answer the question.
# You MUST call tools. Do not write any analysis or explanation yet — only fetch data.

# Scope hints for this request:
# {scope_hints}

# Call all relevant tools in parallel where possible. When you have enough data, stop calling tools.
# """

# Include short RAG snippets to ground the tool selection process.
_INVESTIGATE_PROMPT_WITH_RAG = _INVESTIGATE_PROMPT + "\n\nRelevant data catalog and KPI notes:\n{rag_snippets}\n\nProceed to call tools as needed."


def _node_investigate_data(state: AgentState) -> dict:
    """Node 3 — Run an internal tool-calling loop. Stores results in tool_results, NOT in messages."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['investigate_data']
    failures   = list(state.get('failure_reasons') or [])

    ctx = state.get('context', {})
    scope_hints = '\n'.join(ctx.get('_scope_hints', [])) or 'No specific scope filter.'

    # Find user question
    user_text = ''
    for msg in reversed(state['messages']):
        if isinstance(msg, HumanMessage):
            user_text = msg.content
            break

    system = _INVESTIGATE_PROMPT.format(scope_hints=scope_hints)
    # load small RAG snippets to help the tool choose the minimal set of calls
    rag = _get_rag_snippets(ctx)
    system = _INVESTIGATE_PROMPT_WITH_RAG.format(scope_hints=scope_hints, rag_snippets=rag)
    local_messages: list = [
        SystemMessage(content=system),
        HumanMessage(content=user_text),
    ]

    llm = _tools_llm()
    current_chart = state.get('chart_config')
    current_data: list = state.get('last_tool_data', [])
    accumulated: dict = {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tools_used = list(state.get('tools_used') or [])

    for _ in range(4):  # max 4 tool-call rounds
        response = llm.invoke(local_messages)
        usage = getattr(response, 'usage_metadata', None) or {}
        tok['prompt']     += usage.get('input_tokens', 0)
        tok['completion'] += usage.get('output_tokens', 0)
        local_messages.append(response)

        if not getattr(response, 'tool_calls', None):
            break

        tools_used.extend(tc['name'] for tc in response.tool_calls)
        tool_msgs, current_chart, current_data = _run_tool_calls_parallel(
            response.tool_calls, current_data, current_chart
        )
        for tm in tool_msgs:
            local_messages.append(tm)
            try:
                data = json.loads(tm.content)
                tool_id = tm.tool_call_id
                accumulated[tool_id] = data
            except Exception:
                pass

    # Failure detection
    if not accumulated:
        failures.append('no_tool_data_returned')
    elif any(isinstance(v, dict) and 'error' in v for v in accumulated.values()):
        failures.append('sql_error')

    return {
        'tool_results':   accumulated,
        'chart_config':   current_chart,
        'last_tool_data': current_data,
        'nodes_executed': nodes_exec,
        'failure_reasons': failures,
        'token_usage':    tok,
        'tools_used':     tools_used,
    }


# ─────────────────────────────────────────────────────────────────
# Node 4 — Analyze Faults
# ─────────────────────────────────────────────────────────────────

_FAULT_ANALYSIS_PROMPT = """\
You are a fleet fault analyst. Analyze the raw analytics data provided and produce a concise fault analysis.

Group the faults by:
1. Vehicle system (engine, braking, emissions, electrical, CAN bus, etc.)
2. Severity level (critical ≥3, moderate = 2, minor = 1)
3. Recurring vs. one-time patterns

Identify the top 3 most concerning fault areas with supporting data points (counts, affected vehicles, resolution times).

Output a structured analysis, NOT a chat message. Be precise and data-driven.
"""


def _node_analyze_faults(state: AgentState) -> dict:
    """Node 4 — Group and prioritize faults from tool_results."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['analyze_faults']
    failures   = list(state.get('failure_reasons') or [])

    tool_data_summary = json.dumps(state.get('tool_results', {}), default=str)[:6000]
    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=_FAULT_ANALYSIS_PROMPT),
        HumanMessage(content=f"Analytics data:\n{tool_data_summary}"),
    ])
    fault_analysis = resp.content or ''
    if len(fault_analysis.strip()) < 20:
        failures.append('fault_analysis_empty')

    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)
    return {
        'fault_analysis': fault_analysis,
        'nodes_executed': nodes_exec,
        'failure_reasons': failures,
        'token_usage': tok,
    }


# ─────────────────────────────────────────────────────────────────
# Node 5 — Reason Root Cause
# ─────────────────────────────────────────────────────────────────

_ROOT_CAUSE_PROMPT = """\
You are a senior vehicle diagnostics engineer specializing in root cause analysis.

Using the fault analysis and co-occurrence data provided, identify the most likely root cause(s).

Look for:
- DTC codes that frequently appear together (co-occurrence patterns)
- Cascading failures (primary fault triggering secondary faults)
- Systemic issues (same fault across many vehicles = component batch defect or fleet policy issue)
- Sensor vs. actuator vs. wiring failures

Rank root causes by confidence level (High / Medium / Low).
Be specific about the likely failure mechanism.
"""


def _node_reason_root_cause(state: AgentState) -> dict:
    """Node 5 — Cross-reference fault patterns to infer root causes."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['reason_root_cause']

    fault_analysis = state.get('fault_analysis', '')
    # Extract co-occurrence data if present
    tool_results = state.get('tool_results', {})
    cooc_data = ''
    for v in tool_results.values():
        if isinstance(v, dict) and 'data' in v:
            for row in v['data']:
                if 'cooccurring_dtc' in row or 'dtc_code_a' in row:
                    cooc_data = json.dumps(v['data'][:20], default=str)
                    break

    content = f"Fault Analysis:\n{fault_analysis}"
    if cooc_data:
        content += f"\n\nCo-occurrence Data:\n{cooc_data}"

    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=_ROOT_CAUSE_PROMPT),
        HumanMessage(content=content),
    ])
    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)
    return {'root_cause': resp.content or '', 'nodes_executed': nodes_exec, 'token_usage': tok}


# ─────────────────────────────────────────────────────────────────
# Node 6 — Assess Maintenance Priority
# ─────────────────────────────────────────────────────────────────

_MAINTENANCE_PROMPT = """\
You are a fleet maintenance planning specialist.

Assess the maintenance urgency based on:
- Fault severity levels (critical faults = immediate action)
- Vehicle health score (score < 40 = high risk, 40–70 = medium, >70 = low)
- Fault persistence (faults lasting > 7 days without resolution)
- Fault recurrence (same fault appearing multiple times in 30 days)
- Safety-critical systems (braking, steering, fuel — always highest priority)
- Driver-related faults (may require training rather than mechanical repair)

Output a prioritized maintenance plan:
CRITICAL (act within 24h): ...
HIGH (act within 3 days): ...
MEDIUM (schedule within 1 week): ...
LOW (schedule in next service): ...
"""


def _node_assess_maintenance(state: AgentState) -> dict:
    """Node 6 — Score and prioritize maintenance actions."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['assess_maintenance']

    tool_data = json.dumps(state.get('tool_results', {}), default=str)[:4000]
    fault_analysis = state.get('fault_analysis', '')
    root_cause = state.get('root_cause', '')

    content = (
        f"Fault Analysis:\n{fault_analysis}\n\n"
        f"Root Cause:\n{root_cause}\n\n"
        f"Raw Analytics Data (truncated):\n{tool_data}"
    )
    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=_MAINTENANCE_PROMPT),
        HumanMessage(content=content),
    ])
    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)
    return {
        'maintenance_priority_assessment': resp.content or '',
        'nodes_executed': nodes_exec,
        'token_usage': tok,
    }


# ─────────────────────────────────────────────────────────────────
# Node 7 — Generate Recommendation
# ─────────────────────────────────────────────────────────────────

_RECOMMENDATION_PROMPT = """\
You are a fleet engineering advisor. Synthesize all diagnostic analysis into a structured recommendation.

Your output MUST follow this exact format:

**Problem**
[Concise description of the core issue]

**Cause**
[The identified root cause(s) with confidence level]

**Impact**
[Operational, safety, cost, and compliance impact if unresolved]

**Action**
[Specific, prioritized action steps — numbered list]

Be precise. No generic advice. Every statement must be backed by the analysis data provided.
"""


def _node_generate_recommendation(state: AgentState) -> dict:
    """Node 7 — Produce structured Problem/Cause/Impact/Action recommendation."""
    nodes_exec = list(state.get('nodes_executed') or []) + ['generate_recommendation']

    content = (
        f"Fault Analysis:\n{state.get('fault_analysis', '')}\n\n"
        f"Root Cause:\n{state.get('root_cause', '')}\n\n"
        f"Maintenance Priority:\n{state.get('maintenance_priority_assessment', '')}"
    )
    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=_RECOMMENDATION_PROMPT),
        HumanMessage(content=content),
    ])
    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)
    return {'recommendation': resp.content or '', 'nodes_executed': nodes_exec, 'token_usage': tok}


# ─────────────────────────────────────────────────────────────────
# Node 8 — Explain (final response — writes to state messages)
# ─────────────────────────────────────────────────────────────────

_EXPLAIN_PROMPT = """\
You are **Taabi AI Analyst**, a fleet diagnostics expert explaining findings to a fleet manager.

You have been given:
- The original user question
- Fetched analytics data summary
- Fault analysis
- Root cause reasoning (only for diagnostic questions)
- Maintenance priority assessment (only for diagnostic questions)
- Structured recommendation (only for diagnostic questions)

Your task: Write a clear, helpful, actionable response in professional but non-technical language.

Rules:
- If a structured recommendation is provided, include the Problem/Cause/Impact/Action sections clearly formatted.
- If this is a summary-type question (fleet health, trends), focus on key metrics and insights.
- Format numbers with commas (e.g. 1,234 not 1234).
- Highlight critical findings visually (use **bold** for numbers and key terms).
- End with 1-2 specific next-step suggestions.
- Do NOT say "based on the data provided" or "as an AI" — you are a diagnostics expert.
"""

def _node_explain(state: AgentState) -> dict:
    """Node 8 — Final response generation"""

    intent = state.get("intent")
    ctx = state.get('context') or {}
    force_detailed = bool(ctx.get('force_detailed_response'))

    # ✅ Handle casual conversation FIRST
    if intent == "casual_conversation":
        return {
            "messages": [AIMessage(content="Hi! How can I help you today?")],
            "nodes_executed": state.get("nodes_executed", []) + ["explain"],
            "failure_reasons": state.get("failure_reasons", []),
            "token_usage": state.get("token_usage", {"prompt": 0, "completion": 0}),
        }

    # ✅ Normal flow starts here
    nodes_exec = list(state.get('nodes_executed') or []) + ['explain']
    failures   = list(state.get('failure_reasons') or [])

    # Get user message
    user_text = ''
    for msg in reversed(state['messages']):
        if isinstance(msg, HumanMessage):
            user_text = msg.content
            break

    tool_summary = json.dumps(state.get('tool_results', {}), default=str)[:3000]
    parts = [f"User Question: {user_text}\n\nFetched Data Summary:\n{tool_summary}"]

    fault_analysis = state.get('fault_analysis', '')
    root_cause = state.get('root_cause', '')
    maintenance = state.get('maintenance_priority_assessment', '')
    recommendation = state.get('recommendation', '')

    if fault_analysis:
        parts.append(f"\nFault Analysis:\n{fault_analysis}")
    if root_cause:
        parts.append(f"\nRoot Cause:\n{root_cause}")
    if maintenance:
        parts.append(f"\nMaintenance Priority:\n{maintenance}")
    if recommendation:
        parts.append(f"\nStructured Recommendation:\n{recommendation}")

    # ✅ Optional: force detailed responses for evaluation/testing
    if force_detailed or intent in ["fleet_health", "trend_analysis"]:
        system_prompt = _EXPLAIN_PROMPT
    else:
        system_prompt = """Answer briefly and directly.
Do NOT give full analysis unless explicitly asked.
Keep response natural and conversational.
"""

    llm = _reasoning_llm()
    resp = llm.invoke([
        SystemMessage(content=system_prompt),
        HumanMessage(content='\n'.join(parts)),
    ])

    final_text = resp.content or ''
    if not final_text.strip():
        failures.append('explain_empty')

    usage = getattr(resp, 'usage_metadata', None) or {}
    tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
    tok['prompt']     += usage.get('input_tokens', 0)
    tok['completion'] += usage.get('output_tokens', 0)

    return {
        'messages':        [AIMessage(content=final_text)],
        'nodes_executed':  nodes_exec,
        'failure_reasons': failures,
        'token_usage':     tok,
    }

# ─────────────────────────────────────────────────────────────────
# Routing: after investigate_data, choose diagnostic or summary path
# ─────────────────────────────────────────────────────────────────

def _route_after_investigate(state: AgentState) -> str:
    intent = state.get('intent', 'unknown')
    if intent in _DIAGNOSTIC_INTENTS:
        return 'analyze_faults'
    return 'explain'

def _route_after_context(state: AgentState) -> str:
    intent = state.get("intent", "unknown")

    if intent == "casual_conversation":
        return "explain"

    return "investigate_data"


# ── Compile the 8-node agent graph ──────────────────────────────

_builder = StateGraph(AgentState)
_builder.add_node('detect_intent',           _node_detect_intent)
_builder.add_node('build_context',           _node_build_context)
_builder.add_node('investigate_data',        _node_investigate_data)
_builder.add_node('analyze_faults',          _node_analyze_faults)
_builder.add_node('reason_root_cause',       _node_reason_root_cause)
_builder.add_node('assess_maintenance',      _node_assess_maintenance)
_builder.add_node('generate_recommendation', _node_generate_recommendation)
_builder.add_node('explain',                 _node_explain)

# Linear spine
_builder.add_edge(START,              'detect_intent')
_builder.add_edge('detect_intent',   'build_context')
# _builder.add_edge('build_context',   'investigate_data')
_builder.add_conditional_edges(
    'build_context',
    _route_after_context,
    {
        'explain': 'explain',
        'investigate_data': 'investigate_data',
    },
)



# Branch after data investigation
_builder.add_conditional_edges(
    'investigate_data',
    _route_after_investigate,
    {
        'analyze_faults': 'analyze_faults',
        'explain':        'explain',
    },
)

# Diagnostic path (serial — each node needs previous output)
_builder.add_edge('analyze_faults',          'reason_root_cause')
_builder.add_edge('reason_root_cause',       'assess_maintenance')
_builder.add_edge('assess_maintenance',      'generate_recommendation')
_builder.add_edge('generate_recommendation', 'explain')

# All paths end here
_builder.add_edge('explain', END)

_agent_graph = _builder.compile()


# ── Main chat function ──────────────────────────────────────────

def chat(messages: list[dict], context: dict | None = None) -> dict:
    """
    Process a conversation and return {"text": "...", "chart": {...}|null}.
    messages: list of {"role": "user"|"assistant", "content": "..."}
    context: optional {"mode": "fleet"|"vehicle"|"dtc", "customer_name": "...",
             "vehicle_number": "...", "dtc_code": "..."}
    """
    api_key = os.getenv('OPENAI_API_KEY', '')
    if not api_key:
        return {'text': 'OpenAI API key is not configured. Set OPENAI_API_KEY in .env.', 'chart': None}

    # Convert incoming dicts to LangChain message types
    lc_messages: list = [SystemMessage(content=SYSTEM_PROMPT)]
    for msg in messages:
        role = msg.get('role', 'user')
        content = msg.get('content', '')
        if role == 'user':
            lc_messages.append(HumanMessage(content=content))
        elif role == 'assistant':
            lc_messages.append(AIMessage(content=content))

    initial_state: AgentState = {
        'messages':                      lc_messages,
        'context':                       context or {},
        'chart_config':                  None,
        'last_tool_data':                [],
        'intent':                        '',
        'tool_results':                  {},
        'fault_analysis':                '',
        'root_cause':                    '',
        'maintenance_priority_assessment': '',
        'recommendation':                '',
        'nodes_executed':                [],
        'failure_reasons':               [],
        'token_usage':                   {'prompt': 0, 'completion': 0},
        'tools_used':                    [],
        'trace_log':                     [],
        'request_id':                    str(uuid.uuid4()),
    }
    version_meta = _build_version_metadata()

    t0 = time.time()
    success = 1
    final_state = None

    try:
        # Extract customer name, mode, and user query for tracing
        customer_name = (context or {}).get("customer_name", "unknown")
        mode = (context or {}).get("mode", "general")
        # Normalize customer name for display
        customer_name = customer_name.strip().title() if isinstance(customer_name, str) else str(customer_name)
        user_query = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                user_query = msg.get("content", "")
                break
        user_query_short = user_query[:40]

        final_state = _agent_graph.invoke(
            initial_state,
            config={
                'recursion_limit': 20,
                # Main name visible in UI
                'run_name': f"{customer_name} | {mode}",
                # Filters in LangSmith
                'tags': [
                    'ai_analyst',
                    f"customer:{customer_name}",
                    f"mode:{mode}",
                    f"env:{version_meta.get('env_name', 'dev')}",
                    f"release:{version_meta.get('release_version', 'unknown')}",
                ],
                # Structured metadata (for deep debugging)
                'metadata': {
                    'customer_name': customer_name,
                    'mode': mode,
                    'query': user_query,
                    'request_id': initial_state.get('request_id'),
                    **version_meta,
                }
            },
        )
    except Exception as exc:
        log.error('LangGraph agent error: %s', exc)
        success = 0
        return {'text': f'AI service error: {exc}', 'chart': None}
    finally:
        latency = round(time.time() - t0, 2)
        fs = final_state or {}
        intent          = fs.get('intent') or initial_state.get('intent', 'unknown')
        tool_results    = fs.get('tool_results', {})
        tool_calls_made = len(tool_results)
        nodes_exec      = fs.get('nodes_executed', [])
        failure_reasons = fs.get('failure_reasons', [])
        tok             = fs.get('token_usage') or {'prompt': 0, 'completion': 0}

        # SQL success rate: fraction of tool results without an 'error' key
        if tool_calls_made:
            sql_errors = sum(
                1 for v in tool_results.values()
                if isinstance(v, dict) and 'error' in v
            )
            sql_success_rate = round((tool_calls_made - sql_errors) / tool_calls_made, 3)
        else:
            sql_success_rate = 1.0

        if mlflow is not None:
            try:
                with mlflow.start_run(run_name=f"chat_{intent}"):
                    mlflow.log_params({
                        'intent':         intent,
                        'mode':           (context or {}).get('mode', ''),
                        'customer':       (context or {}).get('customer_name', ''),
                        'has_vehicle':    bool((context or {}).get('vehicle_number')),
                        'has_dtc':        bool((context or {}).get('dtc_code')),
                        'failure_reason': ','.join(failure_reasons) if failure_reasons else 'none',
                        'release_version': version_meta.get('release_version', ''),
                        'service_version': version_meta.get('service_version', ''),
                        'git_commit': version_meta.get('git_commit', ''),
                        'env_name': version_meta.get('env_name', ''),
                        'model_name': version_meta.get('model_name', ''),
                        'dataset_version': version_meta.get('dataset_version', ''),
                        **{f"{k}_prompt_v": v for k, v in PROMPT_VERSIONS.items()},
                    })
                    mlflow.log_metrics({
                        'latency_sec':          latency,
                        'success':              float(success),
                        'tools_called':         float(tool_calls_made),
                        'nodes_executed_count': float(len(nodes_exec)),
                        'tokens_prompt':        float(tok.get('prompt', 0)),
                        'tokens_completion':    float(tok.get('completion', 0)),
                        'failure_count':        float(len(failure_reasons)),
                        'sql_success_rate':     sql_success_rate,
                    })
            except Exception as mex:
                log.warning('MLflow logging failed: %s', mex)

    last = final_state['messages'][-1]
    text = getattr(last, 'content', None) or ''
    response_payload = {
        'text':            text,
        'chart':           final_state.get('chart_config'),
        'intent':          final_state.get('intent', ''),
        'tools_called':    final_state.get('tools_used', []),
        'token_usage':     final_state.get('token_usage') or {'prompt': 0, 'completion': 0},
        'nodes_executed':  final_state.get('nodes_executed', []),
        'failure_reasons': final_state.get('failure_reasons', []),
        'trace_log':       final_state.get('trace_log', []),
        'request_id':      final_state.get('request_id', None),
        'version':         version_meta,
    }
    return response_payload



































# """
# AI Analyst — Fleet intelligence copilot powered by GPT-5.2.

# Architecture:
#   Intent detection → Structured tool calls → DTC knowledge retrieval →
#   Diagnostic reasoning → Structured response with optional chart.

# Uses OpenAI function-calling with domain-specific analytics tools:
#   1. get_fleet_health       — fleet-wide health snapshot
#   2. get_fleet_dtc_distribution — top DTC codes across fleet
#   3. get_fleet_trends       — fleet health trends over time
#   4. get_vehicle_health     — per-vehicle health details
#   5. get_vehicle_faults     — vehicle fault episodes
#   6. get_dtc_details        — DTC knowledge base lookup
#   7. get_dtc_cooccurrence   — fault combination patterns
#   8. get_maintenance_priority — vehicles needing urgent maintenance
#   9. run_sql                — fallback raw SQL for advanced queries
#   10. generate_chart        — structured chart config for frontend

# Env: OPENAI_API_KEY must be set.
# """
# import concurrent.futures
# import json
# import logging
# import os
# import re
# import time
# from typing import Annotated, TypedDict

# from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
# from langchain_openai import ChatOpenAI
# from langgraph.graph import END, START, StateGraph
# from langgraph.graph.message import add_messages

# from src.clickhouse_utils import get_clickhouse_client
# from src.clickhouse_utils_v2 import V2_TABLES
# from pathlib import Path

# log = logging.getLogger(__name__)

# # ── Observability: MLflow (experiment metrics) ──────────────────
# try:
#     import mlflow
#     _mlflow_uri = os.getenv('MLFLOW_TRACKING_URI', '')
#     if _mlflow_uri:
#         mlflow.set_tracking_uri(_mlflow_uri)
#         mlflow.set_experiment('taabi_ai_analyst')
#         log.info('MLflow tracking enabled → %s', _mlflow_uri)
# except ImportError:
#     mlflow = None  # type: ignore[assignment]
#     log.info('mlflow not installed — metrics disabled')

# MAX_RESULT_ROWS = 500

# # Allowed tables for run_sql (lowercase)
# _ALLOWED_TABLES = {t.lower() for t in V2_TABLES.values()}

# # RAG docs location
# _RAG_PATH = Path(__file__).resolve().parents[1] / 'docs' / 'rag'

# def _get_rag_snippets(context: dict | None = None, max_chars: int = 1000) -> str:
#     """Load small helpful snippets from docs/rag for the LLM to ground on.
#     Returns a short concatenated string (may be empty).
#     """
#     try:
#         if not _RAG_PATH.exists():
#             return ''
#         parts = []
#         for p in sorted(_RAG_PATH.glob('*.md')):
#             try:
#                 text = p.read_text(encoding='utf-8').strip()
#                 if not text:
#                     continue
#                 # Keep only the first chunk of each doc
#                 parts.append(f"### {p.name}\n" + text[:max_chars])
#             except Exception:
#                 continue
#         joined = '\n\n'.join(parts)
#         return joined[:max_chars]
#     except Exception:
#         return ''

# # ── Prompt version registry ─────────────────────────────────────
# # Bump a version string whenever you edit a prompt to track performance
# # changes in MLflow. Format: "v<major>" (e.g. "v2" after a major rewrite).
# PROMPT_VERSIONS = {
#     "intent":         "v1",
#     "investigate":    "v1",
#     "fault_analysis": "v1",
#     "root_cause":     "v1",
#     "maintenance":    "v1",
#     "recommendation": "v1",
#     "explain":        "v1",
# }

# # ── System prompt — Taabi AI Analyst ────────────────────────────

# SYSTEM_PROMPT = """\
# You are **Taabi AI Analyst**, a highly experienced **Fleet Diagnostics and Vehicle Health Intelligence Specialist**.

# Your job is to analyze fleet telemetry, diagnostic trouble codes (DTC), vehicle health analytics, and fleet operational patterns to help fleet owners understand vehicle issues, reduce downtime, improve maintenance planning, and improve driver behavior.

# You are not a generic chatbot.
# You behave like a **senior fleet engineer and vehicle diagnostics expert** who works with telematics, OBD/CAN data, and heavy commercial fleets.

# You analyze data from the Taabi Fleet Intelligence Platform and explain insights clearly for fleet owners, maintenance teams, and operations managers.

# Your analysis is based on three major information sources:

# 1. Fleet analytics tables generated from vehicle telemetry
# 2. Vehicle diagnostic knowledge stored in the DTC knowledge base
# 3. Context from the current dashboard page (fleet level, vehicle level, or DTC level)

# You always combine these sources to produce accurate diagnostic insights.

# ---

# ## ROLE AND EXPERTISE

# You have deep expertise in:

# - OBD / CAN bus diagnostics
# - Commercial vehicle maintenance and fleet management
# - Diagnostic Trouble Codes (SAE and manufacturer specific)
# - Heavy vehicle subsystems (powertrain, braking, emissions, electrical, sensors)
# - Telematics and vehicle health monitoring
# - Driver behavior and operational efficiency
# - Predictive maintenance and fleet risk analysis

# You think and respond like an **experienced fleet operations analyst**. and whenever you are answering you should answering like you are explaining to non technical person.

# ---

# ## PLATFORM CONTEXT

# The Taabi platform collects telemetry from vehicles every few seconds using OBD and CAN systems.

# From this data, Taabi calculates:

# - Active DTC faults
# - Fault episodes (start time → end time)
# - Vehicle health score
# - Fleet fault distribution
# - Maintenance priority
# - System health trends
# - DTC co-occurrence patterns
# - Resolution time of faults
# - Fault frequency per vehicle and fleet

# Your responses should interpret these analytics for the fleet owner.

# ---

# ## DATA SOURCES YOU MAY USE

# The system may provide data from these analytics tables:

# Fleet analytics tables:
# - fleet_health_summary
# - fleet_dtc_distribution
# - fleet_system_health
# - fleet_fault_trends

# Vehicle analytics tables:
# - vehicle_health_summary
# - vehicle_fault_master

# Maintenance analytics tables:
# - maintenance_priority

# Diagnostic analysis tables:
# - dtc_fleet_impact
# - dtc_cooccurrence

# Reference knowledge tables:
# - dtc_master (description, causes, symptoms, severity, repair guidance)
# - vehicle_master (vehicle metadata)

# Always rely on these analytics when answering questions.

# ---

# ## HOW TO ANALYZE QUESTIONS

# When a user asks a question, follow this reasoning structure:

# **Step 1 — Identify the context**
# Determine whether the user is asking about:
# - Fleet level
# - Vehicle level
# - Specific DTC code
# - Maintenance planning
# - Fault patterns
# - Driver behavior impact

# **Step 2 — Interpret the analytics**
# Analyze the relevant metrics such as:
# - number of affected vehicles
# - fault frequency
# - episode duration
# - severity level
# - subsystem impact
# - recurrence patterns
# - fault co-occurrence

# **Step 3 — Explain the technical meaning**
# Translate the analytics into understandable insights.

# **Step 4 — Provide operational impact**
# Explain what this means for the fleet owner.

# Examples:
# - safety risk
# - fuel efficiency impact
# - downtime risk
# - emissions compliance
# - potential mechanical damage

# **Step 5 — Provide recommended actions**
# Suggest clear next steps such as:
# - inspection procedures
# - component checks
# - sensor diagnostics
# - driver training recommendations
# - maintenance scheduling
# - fleet policy adjustments

# ---

# ## HOW TO STRUCTURE YOUR ANSWERS

# Your responses should follow this format when possible:

# 1. Summary of the issue
# 2. Key analytics insight
# 3. Technical explanation of the fault
# 4. Impact on vehicle or fleet operations
# 5. Recommended maintenance actions
# 6. Preventive recommendations

# Always prioritize **clear and actionable insights** rather than raw data.

# ---

# ## WHEN ANALYZING DTC CODES

# When discussing a specific DTC code:

# Explain:
# - what the code means
# - which vehicle system is affected
# - possible root causes
# - symptoms drivers may experience
# - risks if the issue continues
# - recommended inspection or repair steps

# If multiple vehicles have the same DTC, explain the **fleet impact and possible systemic causes**.

# ---

# ## WHEN ANALYZING FLEET DATA

# If analyzing fleet-level analytics:

# Focus on:
# - which faults affect the most vehicles
# - which systems are most problematic
# - recurring faults across the fleet
# - trends over time
# - maintenance priorities

# Provide insights that help fleet managers **reduce downtime and prevent failures**.

# ---

# ## WHEN ANALYZING VEHICLE HEALTH

# If analyzing a specific vehicle:

# Focus on:
# - active DTC faults
# - historical fault episodes
# - subsystem risks
# - predicted maintenance needs
# - potential root causes

# Recommend specific inspection steps for that vehicle.

# ---

# ## CHART GENERATION

# If charts are requested or helpful, you may generate visual insights such as:
# - fleet DTC distribution
# - system fault distribution
# - fault trends over time
# - vehicle health comparisons
# - maintenance priority ranking

# Charts should help users understand patterns quickly.
# When generating charts, you MUST pass the actual data objects array from tool results, not empty arrays.

# ---

# ## SAFETY AND RELIABILITY

# Always prioritize vehicle safety and operational reliability.

# If a DTC indicates a critical system (braking, emissions, engine, CAN network), clearly highlight the risk.

# Do not guess when data is missing.
# If insufficient data is available, state that clearly.

# ---

# ## COMMUNICATION STYLE

# Your tone should be:
# - professional
# - analytical
# - technical but understandable
# - concise but insightful

# Avoid generic language.

# You are a **diagnostic expert explaining vehicle health analytics to a fleet manager**.

# Format numbers with commas (e.g. 1,234 not 1234).

# ---

# ## PRIMARY OBJECTIVE

# Your main goal is to help fleet owners:
# - detect problems early
# - reduce breakdowns
# - prioritize maintenance
# - improve fleet reliability
# - improve driver behavior
# - reduce operational costs

# ---

# ## AVAILABLE TOOLS

# Use these structured tools instead of raw SQL when possible:
# - **get_fleet_health**: Fleet-wide health snapshot (scores, fault counts, common DTCs, trends)
# - **get_fleet_dtc_distribution**: Top DTC codes across the fleet with severity, vehicles affected
# - **get_fleet_trends**: Fleet health trends over time (daily metrics, new/resolved faults)
# - **get_fleet_system_health**: Health breakdown by vehicle system (engine, emission, safety, etc.)
# - **get_vehicle_health**: Per-vehicle health details with system flags and fault summary
# - **get_vehicle_faults**: Detailed fault episodes for a specific vehicle
# - **get_dtc_details**: DTC knowledge base — description, causes, symptoms, repair actions
# - **get_dtc_cooccurrence**: Fault combination patterns (which DTCs appear together)
# - **get_dtc_fleet_impact**: DTC codes ranked by fleet-wide impact and risk score
# - **get_maintenance_priority**: Vehicles/faults ranked by maintenance urgency with priority scores
# - **run_sql**: Fallback for complex queries not covered by structured tools
# - **generate_chart**: Create charts for visual analytics

# ---

# ## DATA TABLES (V2 — all analytics)

# All data is in V2 analytics tables. DTC code '0' has been excluded from all tables.

# Fact tables:
# - vehicle_fault_master_ravi_v2: episode_id, clientLoginId, uniqueid, vehicle_number, customer_name, model, manufacturing_year, dtc_code, system, subsystem, description, severity_level (1-5), first_ts (unix), last_ts (unix), event_date, occurrence_count, resolution_time_sec, is_resolved (0/1), gap_from_previous_episode, engine_cycles_during, driver_related (0/1), has_engine_issue, has_coolant_issue, has_safety_issue, has_emission_issue, has_electrical_issue, vehicle_health_score
# - dtc_events_exploded_ravi_v2: clientLoginId, uniqueid, vehicle_number, ts (unix), dtc_code, lat, lng, dtc_pgn

# Fleet-level summaries:
# - fleet_health_summary_ravi_v2: clientLoginId, total_vehicles, vehicles_with_active_faults, vehicles_with_critical_faults, driver_related_faults, fleet_health_score, most_common_dtc, most_common_system, active_fault_trend
# - fleet_dtc_distribution_ravi_v2: clientLoginId, dtc_code, description, system, subsystem, severity_level, vehicles_affected, active_vehicles, total_occurrences, total_episodes, avg_resolution_time, driver_related_count
# - fleet_system_health_ravi_v2: clientLoginId, system, vehicles_affected, active_faults, critical_faults, risk_score, trend
# - fleet_fault_trends_ravi_v2: clientLoginId, date, active_faults, new_faults, resolved_faults, driver_related_faults, fleet_health_score

# Vehicle-level summaries:
# - vehicle_health_summary_ravi_v2: clientLoginId, uniqueid, vehicle_number, customer_name, active_fault_count, critical_fault_count, total_episodes, episodes_last_30_days, avg_resolution_time, last_fault_ts, vehicle_health_score, driver_related_faults, most_common_dtc, has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue

# DTC-level analytics:
# - dtc_fleet_impact_ravi_v2: clientLoginId, dtc_code, system, subsystem, vehicles_affected, active_vehicles, avg_resolution_time, driver_related_ratio, fleet_risk_score
# - dtc_cooccurrence_ravi_v2: clientLoginId, dtc_code_a, dtc_code_b, cooccurrence_count, vehicles_affected, avg_time_gap_sec, last_seen_ts

# Maintenance:
# - maintenance_priority_ravi_v2: clientLoginId, uniqueid, vehicle_number, dtc_code, description, severity_level, fault_duration_sec, episodes_last_30_days, maintenance_priority_score, recommended_action

# Dimension tables:
# - dtc_master_ravi_v2: dtc_code, system, subsystem, description, primary_cause, secondary_causes, symptoms, impact_if_unresolved, severity_level, safety_risk_level, action_required, repair_complexity, fleet_management_action, recommended_preventive_action, driver_related, driver_behaviour_category, driver_behaviour_trigger, driver_training_required
# - vehicle_master_ravi_v2: clientLoginId, uniqueid, vehicle_number, customer_name, model, manufacturing_year, vehicle_type, solutionType

# ---

# ## RULES
# - NEVER hallucinate DTC information. Only use data returned by tools.
# - NEVER invent vehicle faults or vehicle numbers.
# - Only write SELECT queries if using run_sql. Never modify data.
# - When a customer_name filter is provided in the context, always apply it.
# """

# # ── Structured tool definitions ─────────────────────────────────

# TOOLS = [
#     {
#         "type": "function",
#         "function": {
#             "name": "get_fleet_health",
#             "description": (
#                 "Get fleet-wide health snapshot: total vehicles, vehicles with active/critical faults, "
#                 "fleet health score. Optionally filter by customer_name."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name to filter by.",
#                     },
#                     "days": {
#                         "type": "integer",
#                         "description": "Number of recent days to consider (default 30).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_fleet_dtc_distribution",
#             "description": (
#                 "Get top DTC codes across the fleet ranked by occurrence count. "
#                 "Returns dtc_code, total occurrences, vehicles affected."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                     "limit": {
#                         "type": "integer",
#                         "description": "Number of top DTCs to return (default 10).",
#                     },
#                     "days": {
#                         "type": "integer",
#                         "description": "Number of recent days (default 30).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_fleet_trends",
#             "description": (
#                 "Get daily fleet health trends: health score, active faults, critical faults over time."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                     "days": {
#                         "type": "integer",
#                         "description": "Number of recent days (default 30).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_vehicle_health",
#             "description": (
#                 "Get health details for a specific vehicle: health score, active faults, "
#                 "critical faults, fault history. Can look up by vehicle_number or uniqueid."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "vehicle_number": {
#                         "type": "string",
#                         "description": "Vehicle registration number.",
#                     },
#                     "uniqueid": {
#                         "type": "string",
#                         "description": "Vehicle unique device ID.",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_vehicle_faults",
#             "description": (
#                 "Get fault episodes for a specific vehicle. Returns DTC codes, severity, "
#                 "resolution status, first/last seen dates."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "vehicle_number": {
#                         "type": "string",
#                         "description": "Vehicle registration number.",
#                     },
#                     "uniqueid": {
#                         "type": "string",
#                         "description": "Vehicle unique device ID.",
#                     },
#                     "unresolved_only": {
#                         "type": "boolean",
#                         "description": "If true, only return unresolved faults (default false).",
#                     },
#                     "days": {
#                         "type": "integer",
#                         "description": "Number of recent days (default 90).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_dtc_details",
#             "description": (
#                 "Look up DTC knowledge base for one or more DTC codes. Returns description, "
#                 "system, causes, symptoms, repair actions, severity, safety risk, driver-related info."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "dtc_codes": {
#                         "type": "array",
#                         "items": {"type": "string"},
#                         "description": "List of DTC codes to look up (e.g. ['791', '792']).",
#                     },
#                 },
#                 "required": ["dtc_codes"],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_dtc_cooccurrence",
#             "description": (
#                 "Get DTC fault co-occurrence patterns: which DTC codes frequently appear together "
#                 "on the same vehicle. Helps identify root cause correlations."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "dtc_code": {
#                         "type": "string",
#                         "description": "DTC code to find co-occurring faults for.",
#                     },
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                 },
#                 "required": ["dtc_code"],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_maintenance_priority",
#             "description": (
#                 "Get vehicles ranked by maintenance urgency. Returns vehicle number, "
#                 "active fault count, critical fault count, health score, worst DTC."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                     "limit": {
#                         "type": "integer",
#                         "description": "Number of vehicles to return (default 10).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_fleet_system_health",
#             "description": (
#                 "Get health breakdown by vehicle system (engine, emission, safety, electrical etc). "
#                 "Returns vehicles affected, active/critical faults, risk score per system."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "get_dtc_fleet_impact",
#             "description": (
#                 "Get DTC codes ranked by fleet impact: vehicles affected, active vehicles, "
#                 "risk score, avg resolution time, driver-related ratio."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "customer_name": {
#                         "type": "string",
#                         "description": "Optional customer name filter.",
#                     },
#                     "limit": {
#                         "type": "integer",
#                         "description": "Number of top DTC codes to return (default 15).",
#                     },
#                 },
#                 "required": [],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "run_sql",
#             "description": (
#                 "Fallback: Execute a read-only SQL SELECT query against ClickHouse "
#                 "for complex analytics not covered by other tools. "
#                 "Prefer structured tools when possible."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "query": {
#                         "type": "string",
#                         "description": "A ClickHouse-compatible SELECT query.",
#                     }
#                 },
#                 "required": ["query"],
#             },
#         },
#     },
#     {
#         "type": "function",
#         "function": {
#             "name": "generate_chart",
#             "description": (
#                 "Generate a chart to visualize data for the user. "
#                 "CRITICAL: The 'data' parameter MUST contain the actual data objects from previous tool results. "
#                 "Example: data=[{\"dtc_code\":\"810\",\"total_occurrences\":835243},{\"dtc_code\":\"791\",\"total_occurrences\":811201}]. "
#                 "NEVER pass an empty array. If you don't have data yet, fetch it first with another tool."
#             ),
#             "parameters": {
#                 "type": "object",
#                 "properties": {
#                     "chart_type": {
#                         "type": "string",
#                         "enum": ["bar", "line", "pie", "area"],
#                         "description": "Type of chart to render.",
#                     },
#                     "title": {
#                         "type": "string",
#                         "description": "Chart title.",
#                     },
#                     "data": {
#                         "type": "array",
#                         "items": {"type": "object"},
#                         "description": "Array of data objects for the chart. Must contain actual values.",
#                     },
#                     "x_key": {
#                         "type": "string",
#                         "description": "Key in data objects for X axis.",
#                     },
#                     "y_keys": {
#                         "type": "array",
#                         "items": {"type": "string"},
#                         "description": "Keys in data objects for Y axis series.",
#                     },
#                 },
#                 "required": ["chart_type", "title", "data", "x_key", "y_keys"],
#             },
#         },
#     },
# ]

# # ── LangGraph Agent State ───────────────────────────────────────

# class AgentState(TypedDict):
#     # Conversation history — only user messages + final AI reply live here.
#     # All intermediate reasoning is stored in dedicated fields below.
#     messages: Annotated[list, add_messages]
#     context: dict
#     chart_config: dict | None
#     last_tool_data: list
#     # Diagnostic pipeline fields
#     intent: str                          # detected by Node 1
#     tool_results: dict                   # raw data fetched by Node 3
#     fault_analysis: str                  # summary from Node 4
#     root_cause: str                      # reasoning from Node 5
#     maintenance_priority_assessment: str  # urgency from Node 6
#     recommendation: str                  # structured P/C/I/A from Node 7
#     # Observability fields (populated incrementally as nodes execute)
#     nodes_executed: list                 # node names appended on entry
#     failure_reasons: list                # failure codes appended on detection
#     token_usage: dict                    # {"prompt": int, "completion": int} accumulated
#     tools_used: list                     # tool names called (populated by Node 3)


# # ── Read-only SQL guard ─────────────────────────────────────────

# _FORBIDDEN_PATTERN = re.compile(
#     r'\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE|RENAME|ATTACH|DETACH|GRANT|REVOKE|KILL)\b',
#     re.IGNORECASE,
# )


# def _validate_sql(query: str) -> str | None:
#     """Return an error message if the query is unsafe, else None."""
#     stripped = query.strip()
#     upper = stripped.lstrip('(').upper()
#     if not (upper.startswith('SELECT') or upper.startswith('WITH')):
#         return "Only SELECT / WITH queries are allowed."
#     if _FORBIDDEN_PATTERN.search(stripped):
#         return "Query contains a forbidden keyword. Only read-only queries are allowed."
#     # Check referenced tables against allowlist
#     # Find FROM and JOIN table names
#     found = []
#     for m in re.finditer(r"\b(?:FROM|JOIN)\s+([^\s,;()]+)", query, re.IGNORECASE):
#         tbl = m.group(1).strip()
#         # remove surrounding quotes/backticks and db prefix
#         tbl = tbl.strip('`"')
#         if '.' in tbl:
#             tbl = tbl.split('.')[-1]
#         found.append(tbl.lower())
#     for tbl in found:
#         if tbl and tbl not in _ALLOWED_TABLES:
#             return f"Query references disallowed table: {tbl}. Allowed tables: {_ALLOWED_TABLES}"
#     return None


# # ── Helper: execute SQL safely ──────────────────────────────────

# def _exec_sql(query: str) -> list[dict]:
#     """Execute SQL and return list of dicts with column names as keys."""
#     q = query.rstrip().rstrip(';')
#     if 'LIMIT' not in q.upper():
#         q += f' LIMIT {MAX_RESULT_ROWS}'
#     client = get_clickhouse_client()
#     columns, data_rows = client.query_df(q)
#     rows = []
#     for row in data_rows[:MAX_RESULT_ROWS]:
#         d = {}
#         for i, col in enumerate(columns):
#             val = row[i] if i < len(row) else None
#             d[col] = val if isinstance(val, (int, float, str, bool, type(None))) else str(val)
#         rows.append(d)
#     return rows


# def _safe_exec(query: str) -> dict:
#     """Execute SQL safely returning {data, count} or {error}."""
#     try:
#         rows = _exec_sql(query)
#         return {"data": rows, "count": len(rows)}
#     except Exception as exc:
#         log.warning('AI SQL error: %s — query: %s', exc, query[:200])
#         return {"error": f"Query failed: {exc}"}


# # ── Structured tool implementations ────────────────────────────

# def _tool_get_fleet_health(args: dict) -> dict:
#     customer = args.get("customer_name", "")
#     days = args.get("days", 30)
#     cust_filter = f"AND customer_name = '{customer}'" if customer else ""
#     summary = _safe_exec(f"""
#         SELECT
#             count() as total_vehicles,
#             sumIf(1, active_fault_count > 0) as vehicles_with_active_faults,
#             sumIf(1, critical_fault_count > 0) as vehicles_with_critical_faults,
#             round(avg(vehicle_health_score), 1) as avg_health_score,
#             sum(active_fault_count) as total_active_faults,
#             sum(critical_fault_count) as total_critical_faults,
#             sum(driver_related_faults) as total_driver_related_faults
#         FROM vehicle_health_summary_ravi_v2
#         WHERE 1=1 {cust_filter}
#     """)
#     if customer:
#         snap = _safe_exec(f"""
#             SELECT
#                 uniqExact(uniqueid) as total_vehicles,
#                 uniqExactIf(uniqueid, is_resolved = 0) as vehicles_with_active_faults,
#                 uniqExactIf(uniqueid, is_resolved = 0 AND severity_level >= 3) as vehicles_with_critical_faults,
#                 countIf(driver_related = 1) as driver_related_faults,
#                 topKWeighted(1)(dtc_code, occurrence_count)[1] as most_common_dtc,
#                 topKWeighted(1)(system, occurrence_count)[1] as most_common_system
#             FROM vehicle_fault_master_ravi_v2
#             WHERE customer_name = '{customer}'
#         """)
#     else:
#         snap = _safe_exec("""
#             SELECT total_vehicles, vehicles_with_active_faults,
#                    vehicles_with_critical_faults, driver_related_faults,
#                    fleet_health_score, most_common_dtc, most_common_system,
#                    active_fault_trend
#             FROM fleet_health_summary_ravi_v2
#             LIMIT 1
#         """)
#     if customer:
#         trend = _safe_exec(f"""
#             SELECT
#                 event_date as date,
#                 count() as active_faults,
#                 countIf(is_resolved = 1) as resolved_faults,
#                 countIf(driver_related = 1) as driver_related_faults
#             FROM vehicle_fault_master_ravi_v2
#             WHERE customer_name = '{customer}'
#             GROUP BY event_date
#             ORDER BY event_date DESC
#             LIMIT 7
#         """)
#     else:
#         trend = _safe_exec("""
#             SELECT date, fleet_health_score, active_faults, resolved_faults,
#                    driver_related_faults
#             FROM fleet_fault_trends_ravi_v2
#             ORDER BY date DESC
#             LIMIT 7
#         """)
#     return {"summary": summary, "fleet_snapshot": snap, "recent_trend": trend}


# def _tool_get_fleet_dtc_distribution(args: dict) -> dict:
#     customer = args.get("customer_name", "")
#     limit = args.get("limit", 10)
#     if customer:
#         return _safe_exec(f"""
#             SELECT
#                 f.dtc_code,
#                 any(f.description) as description,
#                 any(f.system) as system,
#                 any(f.subsystem) as subsystem,
#                 any(f.severity_level) as severity_level,
#                 uniqExact(f.uniqueid) as vehicles_affected,
#                 uniqExactIf(f.uniqueid, f.is_resolved = 0) as active_vehicles,
#                 sum(f.occurrence_count) as total_occurrences,
#                 count() as total_episodes,
#                 if(countIf(f.resolution_time_sec > 0) > 0,
#                    round(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
#                          / countIf(f.resolution_time_sec > 0), 1), 0) as avg_resolution_time,
#                 countIf(f.driver_related = 1) as driver_related_count
#             FROM vehicle_fault_master_ravi_v2 f
#             WHERE f.customer_name = '{customer}'
#             GROUP BY f.dtc_code
#             ORDER BY total_occurrences DESC
#             LIMIT {int(limit)}
#         """)
#     return _safe_exec(f"""
#         SELECT
#             dtc_code, description, system, subsystem, severity_level,
#             vehicles_affected, active_vehicles, total_occurrences,
#             total_episodes, round(avg_resolution_time, 1) as avg_resolution_time,
#             driver_related_count
#         FROM fleet_dtc_distribution_ravi_v2
#         ORDER BY total_occurrences DESC
#         LIMIT {int(limit)}
#     """)


# def _tool_get_fleet_trends(args: dict) -> dict:
#     customer = args.get("customer_name", "")
#     days = args.get("days", 30)
#     if customer:
#         return _safe_exec(f"""
#             SELECT
#                 event_date as date,
#                 count() as active_faults,
#                 count() as new_faults,
#                 countIf(is_resolved = 1) as resolved_faults,
#                 countIf(driver_related = 1) as driver_related_faults
#             FROM vehicle_fault_master_ravi_v2
#             WHERE customer_name = '{customer}'
#             GROUP BY event_date
#             ORDER BY event_date ASC
#         """)
#     return _safe_exec("""
#         SELECT
#             date,
#             fleet_health_score,
#             active_faults,
#             new_faults,
#             resolved_faults,
#             driver_related_faults
#         FROM fleet_fault_trends_ravi_v2
#         ORDER BY date ASC
#     """)


# def _tool_get_vehicle_health(args: dict) -> dict:
#     v_num = args.get("vehicle_number", "")
#     uid = args.get("uniqueid", "")
#     if v_num:
#         where = f"vehicle_number = '{v_num}'"
#     elif uid:
#         where = f"uniqueid = '{uid}'"
#     else:
#         return {"error": "Provide vehicle_number or uniqueid."}
#     status = _safe_exec(f"""
#         SELECT uniqueid, vehicle_number, customer_name,
#                vehicle_health_score, active_fault_count, critical_fault_count,
#                total_episodes, episodes_last_30_days,
#                round(avg_resolution_time, 1) as avg_resolution_time,
#                driver_related_faults, most_common_dtc,
#                has_engine_issue, has_emission_issue, has_safety_issue, has_electrical_issue
#         FROM vehicle_health_summary_ravi_v2
#         WHERE {where}
#         LIMIT 1
#     """)
#     # Also fetch fault detail from vehicle_fault_master
#     faults = _safe_exec(f"""
#         SELECT dtc_code, system, description, count() as episodes,
#                max(severity_level) as max_severity,
#                sumIf(1, is_resolved = 0) as unresolved,
#                min(first_ts) as earliest_ts, max(last_ts) as latest_ts,
#                round(avg(resolution_time_sec), 0) as avg_resolution_sec
#         FROM vehicle_fault_master_ravi_v2
#         WHERE {where}
#         GROUP BY dtc_code, system, description
#         ORDER BY unresolved DESC, episodes DESC
#         LIMIT 15
#     """)
#     return {"status": status, "fault_summary": faults}


# def _tool_get_vehicle_faults(args: dict) -> dict:
#     v_num = args.get("vehicle_number", "")
#     uid = args.get("uniqueid", "")
#     unresolved_only = args.get("unresolved_only", False)
#     days = args.get("days", 90)
#     if v_num:
#         where = f"vehicle_number = '{v_num}'"
#     elif uid:
#         where = f"uniqueid = '{uid}'"
#     else:
#         return {"error": "Provide vehicle_number or uniqueid."}
#     resolved_filter = "AND is_resolved = 0" if unresolved_only else ""
#     return _safe_exec(f"""
#         SELECT episode_id, dtc_code, system, subsystem, description,
#                severity_level, is_resolved, event_date,
#                first_ts, last_ts, occurrence_count,
#                resolution_time_sec, driver_related,
#                vehicle_health_score
#         FROM vehicle_fault_master_ravi_v2
#         WHERE {where}
#           AND event_date >= today() - {days}
#           {resolved_filter}
#         ORDER BY event_date DESC, severity_level DESC
#         LIMIT 50
#     """)


# def _tool_get_dtc_details(args: dict) -> dict:
#     codes = args.get("dtc_codes", [])
#     if not codes:
#         return {"error": "Provide at least one DTC code."}
#     codes_str = ", ".join(f"'{c}'" for c in codes[:20])
#     return _safe_exec(f"""
#         SELECT dtc_code, system, subsystem, description, primary_cause,
#                secondary_causes, symptoms, impact_if_unresolved,
#                severity_level, safety_risk_level, action_required,
#                repair_complexity, fleet_management_action,
#                recommended_preventive_action, driver_related,
#                driver_behaviour_category, driver_behaviour_trigger,
#                driver_training_required
#         FROM dtc_master_ravi_v2
#         WHERE dtc_code IN ({codes_str})
#     """)


# def _tool_get_dtc_cooccurrence(args: dict) -> dict:
#     dtc_code = args.get("dtc_code", "")
#     customer = args.get("customer_name", "")
#     if not dtc_code:
#         return {"error": "Provide a dtc_code."}
#     if customer:
#         return _safe_exec(f"""
#             WITH target_vehicles AS (
#                 SELECT DISTINCT uniqueid
#                 FROM vehicle_fault_master_ravi_v2
#                 WHERE dtc_code = '{dtc_code}' AND customer_name = '{customer}'
#             )
#             SELECT
#                 f.dtc_code as cooccurring_dtc,
#                 count() as cooccurrence_count,
#                 uniqExact(f.uniqueid) as vehicles_affected
#             FROM vehicle_fault_master_ravi_v2 f
#             INNER JOIN target_vehicles tv ON f.uniqueid = tv.uniqueid
#             WHERE f.dtc_code != '{dtc_code}'
#               AND f.customer_name = '{customer}'
#             GROUP BY f.dtc_code
#             ORDER BY vehicles_affected DESC
#             LIMIT 15
#         """)
#     return _safe_exec(f"""
#         SELECT
#             if(dtc_code_a = '{dtc_code}', dtc_code_b, dtc_code_a) as cooccurring_dtc,
#             cooccurrence_count,
#             vehicles_affected,
#             round(avg_time_gap_sec, 0) as avg_time_gap_sec
#         FROM dtc_cooccurrence_ravi_v2
#         WHERE dtc_code_a = '{dtc_code}' OR dtc_code_b = '{dtc_code}'
#         ORDER BY vehicles_affected DESC
#         LIMIT 15
#     """)


# def _tool_get_maintenance_priority(args: dict) -> dict:
#     customer = args.get("customer_name", "")
#     limit = args.get("limit", 10)
#     cust_filter = f"AND v.customer_name = '{customer}'" if customer else ""
#     return _safe_exec(f"""
#         SELECT
#             m.vehicle_number, m.uniqueid, v.customer_name,
#             m.dtc_code, m.description, m.severity_level,
#             m.maintenance_priority_score,
#             m.fault_duration_sec, m.episodes_last_30_days,
#             m.recommended_action,
#             v.vehicle_health_score, v.active_fault_count, v.critical_fault_count
#         FROM maintenance_priority_ravi_v2 m
#         LEFT JOIN vehicle_health_summary_ravi_v2 v ON m.uniqueid = v.uniqueid
#         WHERE 1=1 {cust_filter}
#         ORDER BY m.maintenance_priority_score DESC
#         LIMIT {int(limit)}
#     """)


# def _tool_get_fleet_system_health(args: dict) -> dict:
#     customer = args.get("customer_name", "")
#     if customer:
#         return _safe_exec(f"""
#             SELECT
#                 f.system,
#                 uniqExactIf(f.uniqueid, f.is_resolved = 0) as vehicles_affected,
#                 countIf(f.is_resolved = 0) as active_faults,
#                 countIf(f.is_resolved = 0 AND f.severity_level >= 3) as critical_faults,
#                 round(sumIf(
#                     multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
#                             f.severity_level=3,7.0, 12.0),
#                     f.is_resolved = 0), 1) as risk_score
#             FROM vehicle_fault_master_ravi_v2 f
#             WHERE f.system != '' AND f.customer_name = '{customer}'
#             GROUP BY f.system
#             ORDER BY risk_score DESC
#         """)
#     return _safe_exec("""
#         SELECT system, vehicles_affected, active_faults, critical_faults,
#                round(risk_score, 1) as risk_score, trend
#         FROM fleet_system_health_ravi_v2
#         ORDER BY risk_score DESC
#     """)


# def _tool_get_dtc_fleet_impact(args: dict) -> dict:
#     limit = args.get("limit", 15)
#     customer = args.get("customer_name", "")
#     if customer:
#         return _safe_exec(f"""
#             SELECT
#                 f.dtc_code,
#                 any(f.system) as system,
#                 any(f.subsystem) as subsystem,
#                 uniqExact(f.uniqueid) as vehicles_affected,
#                 uniqExactIf(f.uniqueid, f.is_resolved = 0) as active_vehicles,
#                 if(countIf(f.resolution_time_sec > 0) > 0,
#                    round(sumIf(f.resolution_time_sec, f.resolution_time_sec > 0)
#                          / countIf(f.resolution_time_sec > 0), 0), 0) as avg_resolution_time,
#                 if(count() > 0,
#                    round(countIf(f.driver_related = 1) / count(), 3), 0) as driver_related_ratio,
#                 round(sumIf(
#                     multiIf(f.severity_level<=1,1.0, f.severity_level=2,3.0,
#                             f.severity_level=3,7.0, 12.0),
#                     f.is_resolved = 0), 1) as fleet_risk_score
#             FROM vehicle_fault_master_ravi_v2 f
#             WHERE f.customer_name = '{customer}'
#             GROUP BY f.dtc_code
#             ORDER BY fleet_risk_score DESC
#             LIMIT {int(limit)}
#         """)
#     return _safe_exec(f"""
#         SELECT dtc_code, system, subsystem,
#                vehicles_affected, active_vehicles,
#                round(avg_resolution_time, 0) as avg_resolution_time,
#                round(driver_related_ratio, 3) as driver_related_ratio,
#                round(fleet_risk_score, 1) as fleet_risk_score
#         FROM dtc_fleet_impact_ravi_v2
#         ORDER BY fleet_risk_score DESC
#         LIMIT {int(limit)}
#     """)


# # ── Tool dispatcher ─────────────────────────────────────────────

# _TOOL_HANDLERS = {
#     "get_fleet_health": _tool_get_fleet_health,
#     "get_fleet_dtc_distribution": _tool_get_fleet_dtc_distribution,
#     "get_fleet_trends": _tool_get_fleet_trends,
#     "get_vehicle_health": _tool_get_vehicle_health,
#     "get_vehicle_faults": _tool_get_vehicle_faults,
#     "get_dtc_details": _tool_get_dtc_details,
#     "get_dtc_cooccurrence": _tool_get_dtc_cooccurrence,
#     "get_maintenance_priority": _tool_get_maintenance_priority,
#     "get_fleet_system_health": _tool_get_fleet_system_health,
#     "get_dtc_fleet_impact": _tool_get_dtc_fleet_impact,
# }


# # ── Shared LLM factory ───────────────────────────────────────────

# def _reasoning_llm() -> ChatOpenAI:
#     """LLM for reasoning nodes — no tools bound."""
#     return ChatOpenAI(
#         model='gpt-3.5-turbo',
#         api_key=os.getenv('OPENAI_API_KEY', ''),
#         temperature=0.2,
#         timeout=60,
#         model_kwargs={
#             'max_completion_tokens': 800,
#         },
#     )


# def _tools_llm() -> ChatOpenAI:
#     """LLM for data-investigation node — tools bound."""
#     return ChatOpenAI(
#         model='gpt-3.5-turbo',
#         api_key=os.getenv('OPENAI_API_KEY', ''),
#         temperature=0.1,
#         timeout=60,
#         model_kwargs={
#             'max_completion_tokens': 800,
#         },
#     ).bind_tools(TOOLS)


# # ── Intent labels ────────────────────────────────────────────────

# _DIAGNOSTIC_INTENTS = {'vehicle_investigation', 'dtc_investigation', 'fault_correlation'}
# _SUMMARY_INTENTS    = {'fleet_health', 'trend_analysis', 'maintenance_prioritization', 'unknown'}

# # ─────────────────────────────────────────────────────────────────
# # Node 1 — Detect Intent
# # ─────────────────────────────────────────────────────────────────

# _INTENT_PROMPT = """\
# You are an intent classifier for a fleet diagnostics AI platform.

# Classify the user's question into EXACTLY ONE of these intents:
# - vehicle_investigation   → asking about a specific vehicle's health, faults, or history
# - dtc_investigation       → asking about a specific DTC code's meaning, impact, or trend
# - fault_correlation       → asking about patterns, relationships between faults, root causes
# - fleet_health            → asking about overall fleet status, KPIs, or health score
# - trend_analysis          → asking about trends, changes over time, comparisons
# - maintenance_prioritization → asking which vehicles or faults need attention / scheduling
# - unknown                 → does not fit any category above

# Respond with ONLY the intent label, nothing else. No punctuation, no explanation.
# """


# def _node_detect_intent(state: AgentState) -> dict:
#     """Node 1 — Classify the user's question into a diagnostic intent."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['detect_intent']

#     # Find the most recent human message
#     user_text = ''
#     for msg in reversed(state['messages']):
#         if isinstance(msg, HumanMessage):
#             user_text = msg.content
#             break

#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_INTENT_PROMPT),
#         HumanMessage(content=user_text),
#     ])
#     raw = (resp.content or '').strip().lower().replace('-', '_')

#     known = _DIAGNOSTIC_INTENTS | _SUMMARY_INTENTS
#     intent = raw if raw in known else 'unknown'
#     log.info('Intent detected: %s', intent)

#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {'intent': intent, 'nodes_executed': nodes_exec, 'token_usage': tok}


# # ─────────────────────────────────────────────────────────────────
# # Node 2 — Build Context (deterministic, no LLM)
# # ─────────────────────────────────────────────────────────────────

# def _node_build_context(state: AgentState) -> dict:
#     """Node 2 — Enrich state context with intent and scope hints. No LLM call."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['build_context']
#     ctx = dict(state.get('context') or {})
#     ctx['_intent'] = state.get('intent', 'unknown')
#     # Derive scope hints for the data investigation node
#     hints = []
#     if ctx.get('customer_name'):
#         hints.append(f"Filter all data to customer: {ctx['customer_name']}")
#     if ctx.get('vehicle_number'):
#         hints.append(f"Focus on vehicle: {ctx['vehicle_number']}")
#     if ctx.get('dtc_code'):
#         hints.append(f"Focus on DTC code: {ctx['dtc_code']}")
#     if ctx.get('mode'):
#         hints.append(f"Dashboard page context: {ctx['mode']}")
#     ctx['_scope_hints'] = hints
#     return {'context': ctx, 'nodes_executed': nodes_exec}


# # ─────────────────────────────────────────────────────────────────
# # Node 3 — Investigate Data (internal tool-calling mini-loop)
# # ─────────────────────────────────────────────────────────────────

# def _run_tool_calls_parallel(tool_calls: list, last_data: list, chart: dict | None):
#     """Execute a batch of tool calls in parallel. Returns (tool_messages, new_chart, new_data)."""
#     def _run_one(tc):
#         fn_name = tc['name']
#         fn_args = tc['args'] if isinstance(tc['args'], dict) else {}
#         tool_call_id = tc['id']
#         local_chart = None
#         local_data = None

#         if fn_name == 'generate_chart':
#             chart_data = fn_args.get('data', [])
#             if not chart_data and last_data:
#                 chart_data = last_data
#             local_chart = {
#                 'type': fn_args.get('chart_type', 'bar'),
#                 'title': fn_args.get('title', ''),
#                 'data': chart_data,
#                 'xKey': fn_args.get('x_key', ''),
#                 'yKeys': fn_args.get('y_keys', []),
#             }
#             result = {'status': 'chart_created', 'note': 'Chart will render in the UI.'}
#         elif fn_name == 'run_sql':
#             query = fn_args.get('query', '')
#             err = _validate_sql(query)
#             result = {'error': err} if err else _safe_exec(query)
#             if isinstance(result, dict) and 'data' in result and result['data']:
#                 local_data = result['data']
#         elif fn_name in _TOOL_HANDLERS:
#             try:
#                 result = _TOOL_HANDLERS[fn_name](fn_args)
#                 if isinstance(result, dict) and 'data' in result and result['data']:
#                     local_data = result['data']
#                 elif isinstance(result, list) and result:
#                     local_data = result
#             except Exception as exc:
#                 log.warning('Tool %s error: %s', fn_name, exc)
#                 result = {'error': str(exc)}
#         else:
#             result = {'error': f'Unknown function: {fn_name}'}

#         return (
#             ToolMessage(content=json.dumps(result, default=str), tool_call_id=tool_call_id),
#             local_chart,
#             local_data,
#         )

#     with concurrent.futures.ThreadPoolExecutor() as executor:
#         outcomes = list(executor.map(_run_one, tool_calls))

#     msgs, new_chart, new_data = [], chart, last_data
#     for msg, lc, ld in outcomes:
#         msgs.append(msg)
#         if lc is not None:
#             new_chart = lc
#         if ld is not None:
#             new_data = ld
#     return msgs, new_chart, new_data


# _INVESTIGATE_PROMPT = """\
# You are the data retrieval agent for a fleet diagnostics system.

# Your ONLY job is to call the right analytics tools to gather all data needed to answer the question.
# You MUST call tools. Do not write any analysis or explanation yet — only fetch data.

# Scope hints for this request:
# {scope_hints}

# Call all relevant tools in parallel where possible. When you have enough data, stop calling tools.
# """

# # Include short RAG snippets to ground the tool selection process.
# _INVESTIGATE_PROMPT_WITH_RAG = _INVESTIGATE_PROMPT + "\n\nRelevant data catalog and KPI notes:\n{rag_snippets}\n\nProceed to call tools as needed."


# def _node_investigate_data(state: AgentState) -> dict:
#     """Node 3 — Run an internal tool-calling loop. Stores results in tool_results, NOT in messages."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['investigate_data']
#     failures   = list(state.get('failure_reasons') or [])

#     ctx = state.get('context', {})
#     scope_hints = '\n'.join(ctx.get('_scope_hints', [])) or 'No specific scope filter.'

#     # Find user question
#     user_text = ''
#     for msg in reversed(state['messages']):
#         if isinstance(msg, HumanMessage):
#             user_text = msg.content
#             break

#     system = _INVESTIGATE_PROMPT.format(scope_hints=scope_hints)
#     # load small RAG snippets to help the tool choose the minimal set of calls
#     rag = _get_rag_snippets(ctx)
#     system = _INVESTIGATE_PROMPT_WITH_RAG.format(scope_hints=scope_hints, rag_snippets=rag)
#     local_messages: list = [
#         SystemMessage(content=system),
#         HumanMessage(content=user_text),
#     ]

#     llm = _tools_llm()
#     current_chart = state.get('chart_config')
#     current_data: list = state.get('last_tool_data', [])
#     accumulated: dict = {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tools_used = list(state.get('tools_used') or [])

#     for _ in range(4):  # max 4 tool-call rounds
#         response = llm.invoke(local_messages)
#         usage = getattr(response, 'usage_metadata', None) or {}
#         tok['prompt']     += usage.get('input_tokens', 0)
#         tok['completion'] += usage.get('output_tokens', 0)
#         local_messages.append(response)

#         if not getattr(response, 'tool_calls', None):
#             break

#         tools_used.extend(tc['name'] for tc in response.tool_calls)
#         tool_msgs, current_chart, current_data = _run_tool_calls_parallel(
#             response.tool_calls, current_data, current_chart
#         )
#         for tm in tool_msgs:
#             local_messages.append(tm)
#             try:
#                 data = json.loads(tm.content)
#                 tool_id = tm.tool_call_id
#                 accumulated[tool_id] = data
#             except Exception:
#                 pass

#     # Failure detection
#     if not accumulated:
#         failures.append('no_tool_data_returned')
#     elif any(isinstance(v, dict) and 'error' in v for v in accumulated.values()):
#         failures.append('sql_error')

#     return {
#         'tool_results':   accumulated,
#         'chart_config':   current_chart,
#         'last_tool_data': current_data,
#         'nodes_executed': nodes_exec,
#         'failure_reasons': failures,
#         'token_usage':    tok,
#         'tools_used':     tools_used,
#     }


# # ─────────────────────────────────────────────────────────────────
# # Node 4 — Analyze Faults
# # ─────────────────────────────────────────────────────────────────

# _FAULT_ANALYSIS_PROMPT = """\
# You are a fleet fault analyst. Analyze the raw analytics data provided and produce a concise fault analysis.

# Group the faults by:
# 1. Vehicle system (engine, braking, emissions, electrical, CAN bus, etc.)
# 2. Severity level (critical ≥3, moderate = 2, minor = 1)
# 3. Recurring vs. one-time patterns

# Identify the top 3 most concerning fault areas with supporting data points (counts, affected vehicles, resolution times).

# Output a structured analysis, NOT a chat message. Be precise and data-driven.
# """


# def _node_analyze_faults(state: AgentState) -> dict:
#     """Node 4 — Group and prioritize faults from tool_results."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['analyze_faults']
#     failures   = list(state.get('failure_reasons') or [])

#     tool_data_summary = json.dumps(state.get('tool_results', {}), default=str)[:6000]
#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_FAULT_ANALYSIS_PROMPT),
#         HumanMessage(content=f"Analytics data:\n{tool_data_summary}"),
#     ])
#     fault_analysis = resp.content or ''
#     if len(fault_analysis.strip()) < 20:
#         failures.append('fault_analysis_empty')

#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {
#         'fault_analysis': fault_analysis,
#         'nodes_executed': nodes_exec,
#         'failure_reasons': failures,
#         'token_usage': tok,
#     }


# # ─────────────────────────────────────────────────────────────────
# # Node 5 — Reason Root Cause
# # ─────────────────────────────────────────────────────────────────

# _ROOT_CAUSE_PROMPT = """\
# You are a senior vehicle diagnostics engineer specializing in root cause analysis.

# Using the fault analysis and co-occurrence data provided, identify the most likely root cause(s).

# Look for:
# - DTC codes that frequently appear together (co-occurrence patterns)
# - Cascading failures (primary fault triggering secondary faults)
# - Systemic issues (same fault across many vehicles = component batch defect or fleet policy issue)
# - Sensor vs. actuator vs. wiring failures

# Rank root causes by confidence level (High / Medium / Low).
# Be specific about the likely failure mechanism.
# """


# def _node_reason_root_cause(state: AgentState) -> dict:
#     """Node 5 — Cross-reference fault patterns to infer root causes."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['reason_root_cause']

#     fault_analysis = state.get('fault_analysis', '')
#     # Extract co-occurrence data if present
#     tool_results = state.get('tool_results', {})
#     cooc_data = ''
#     for v in tool_results.values():
#         if isinstance(v, dict) and 'data' in v:
#             for row in v['data']:
#                 if 'cooccurring_dtc' in row or 'dtc_code_a' in row:
#                     cooc_data = json.dumps(v['data'][:20], default=str)
#                     break

#     content = f"Fault Analysis:\n{fault_analysis}"
#     if cooc_data:
#         content += f"\n\nCo-occurrence Data:\n{cooc_data}"

#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_ROOT_CAUSE_PROMPT),
#         HumanMessage(content=content),
#     ])
#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {'root_cause': resp.content or '', 'nodes_executed': nodes_exec, 'token_usage': tok}


# # ─────────────────────────────────────────────────────────────────
# # Node 6 — Assess Maintenance Priority
# # ─────────────────────────────────────────────────────────────────

# _MAINTENANCE_PROMPT = """\
# You are a fleet maintenance planning specialist.

# Assess the maintenance urgency based on:
# - Fault severity levels (critical faults = immediate action)
# - Vehicle health score (score < 40 = high risk, 40–70 = medium, >70 = low)
# - Fault persistence (faults lasting > 7 days without resolution)
# - Fault recurrence (same fault appearing multiple times in 30 days)
# - Safety-critical systems (braking, steering, fuel — always highest priority)
# - Driver-related faults (may require training rather than mechanical repair)

# Output a prioritized maintenance plan:
# CRITICAL (act within 24h): ...
# HIGH (act within 3 days): ...
# MEDIUM (schedule within 1 week): ...
# LOW (schedule in next service): ...
# """


# def _node_assess_maintenance(state: AgentState) -> dict:
#     """Node 6 — Score and prioritize maintenance actions."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['assess_maintenance']

#     tool_data = json.dumps(state.get('tool_results', {}), default=str)[:4000]
#     fault_analysis = state.get('fault_analysis', '')
#     root_cause = state.get('root_cause', '')

#     content = (
#         f"Fault Analysis:\n{fault_analysis}\n\n"
#         f"Root Cause:\n{root_cause}\n\n"
#         f"Raw Analytics Data (truncated):\n{tool_data}"
#     )
#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_MAINTENANCE_PROMPT),
#         HumanMessage(content=content),
#     ])
#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {
#         'maintenance_priority_assessment': resp.content or '',
#         'nodes_executed': nodes_exec,
#         'token_usage': tok,
#     }


# # ─────────────────────────────────────────────────────────────────
# # Node 7 — Generate Recommendation
# # ─────────────────────────────────────────────────────────────────

# _RECOMMENDATION_PROMPT = """\
# You are a fleet engineering advisor. Synthesize all diagnostic analysis into a structured recommendation.

# Your output MUST follow this exact format:

# **Problem**
# [Concise description of the core issue]

# **Cause**
# [The identified root cause(s) with confidence level]

# **Impact**
# [Operational, safety, cost, and compliance impact if unresolved]

# **Action**
# [Specific, prioritized action steps — numbered list]

# Be precise. No generic advice. Every statement must be backed by the analysis data provided.
# """


# def _node_generate_recommendation(state: AgentState) -> dict:
#     """Node 7 — Produce structured Problem/Cause/Impact/Action recommendation."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['generate_recommendation']

#     content = (
#         f"Fault Analysis:\n{state.get('fault_analysis', '')}\n\n"
#         f"Root Cause:\n{state.get('root_cause', '')}\n\n"
#         f"Maintenance Priority:\n{state.get('maintenance_priority_assessment', '')}"
#     )
#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_RECOMMENDATION_PROMPT),
#         HumanMessage(content=content),
#     ])
#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {'recommendation': resp.content or '', 'nodes_executed': nodes_exec, 'token_usage': tok}


# # ─────────────────────────────────────────────────────────────────
# # Node 8 — Explain (final response — writes to state messages)
# # ─────────────────────────────────────────────────────────────────

# _EXPLAIN_PROMPT = """\
# You are **Taabi AI Analyst**, a fleet diagnostics expert explaining findings to a fleet manager.

# You have been given:
# - The original user question
# - Fetched analytics data summary
# - Fault analysis
# - Root cause reasoning (only for diagnostic questions)
# - Maintenance priority assessment (only for diagnostic questions)
# - Structured recommendation (only for diagnostic questions)

# Your task: Write a clear, helpful, actionable response in professional but non-technical language.

# Rules:
# - If a structured recommendation is provided, include the Problem/Cause/Impact/Action sections clearly formatted.
# - If this is a summary-type question (fleet health, trends), focus on key metrics and insights.
# - Format numbers with commas (e.g. 1,234 not 1234).
# - Highlight critical findings visually (use **bold** for numbers and key terms).
# - End with 1-2 specific next-step suggestions.
# - Do NOT say "based on the data provided" or "as an AI" — you are a diagnostics expert.
# """


# def _node_explain(state: AgentState) -> dict:
#     """Node 8 — Synthesize all analysis into a final plain-language user response."""
#     nodes_exec = list(state.get('nodes_executed') or []) + ['explain']
#     failures   = list(state.get('failure_reasons') or [])

#     user_text = ''
#     for msg in reversed(state['messages']):
#         if isinstance(msg, HumanMessage):
#             user_text = msg.content
#             break

#     tool_summary = json.dumps(state.get('tool_results', {}), default=str)[:3000]
#     parts = [f"User Question: {user_text}\n\nFetched Data Summary:\n{tool_summary}"]

#     fault_analysis = state.get('fault_analysis', '')
#     root_cause = state.get('root_cause', '')
#     maintenance = state.get('maintenance_priority_assessment', '')
#     recommendation = state.get('recommendation', '')

#     if fault_analysis:
#         parts.append(f"\nFault Analysis:\n{fault_analysis}")
#     if root_cause:
#         parts.append(f"\nRoot Cause:\n{root_cause}")
#     if maintenance:
#         parts.append(f"\nMaintenance Priority:\n{maintenance}")
#     if recommendation:
#         parts.append(f"\nStructured Recommendation:\n{recommendation}")

#     llm = _reasoning_llm()
#     resp = llm.invoke([
#         SystemMessage(content=_EXPLAIN_PROMPT),
#         HumanMessage(content='\n'.join(parts)),
#     ])

#     final_text = resp.content or ''
#     if not final_text.strip():
#         failures.append('explain_empty')

#     usage = getattr(resp, 'usage_metadata', None) or {}
#     tok = dict(state.get('token_usage') or {'prompt': 0, 'completion': 0})
#     tok['prompt']     += usage.get('input_tokens', 0)
#     tok['completion'] += usage.get('output_tokens', 0)
#     return {
#         'messages':        [AIMessage(content=final_text)],
#         'nodes_executed':  nodes_exec,
#         'failure_reasons': failures,
#         'token_usage':     tok,
#     }


# # ─────────────────────────────────────────────────────────────────
# # Routing: after investigate_data, choose diagnostic or summary path
# # ─────────────────────────────────────────────────────────────────

# def _route_after_investigate(state: AgentState) -> str:
#     intent = state.get('intent', 'unknown')
#     if intent in _DIAGNOSTIC_INTENTS:
#         return 'analyze_faults'
#     return 'explain'


# # ── Compile the 8-node agent graph ──────────────────────────────

# _builder = StateGraph(AgentState)
# _builder.add_node('detect_intent',           _node_detect_intent)
# _builder.add_node('build_context',           _node_build_context)
# _builder.add_node('investigate_data',        _node_investigate_data)
# _builder.add_node('analyze_faults',          _node_analyze_faults)
# _builder.add_node('reason_root_cause',       _node_reason_root_cause)
# _builder.add_node('assess_maintenance',      _node_assess_maintenance)
# _builder.add_node('generate_recommendation', _node_generate_recommendation)
# _builder.add_node('explain',                 _node_explain)

# # Linear spine
# _builder.add_edge(START,              'detect_intent')
# _builder.add_edge('detect_intent',   'build_context')
# _builder.add_edge('build_context',   'investigate_data')

# # Branch after data investigation
# _builder.add_conditional_edges(
#     'investigate_data',
#     _route_after_investigate,
#     {
#         'analyze_faults': 'analyze_faults',
#         'explain':        'explain',
#     },
# )

# # Diagnostic path (serial — each node needs previous output)
# _builder.add_edge('analyze_faults',          'reason_root_cause')
# _builder.add_edge('reason_root_cause',       'assess_maintenance')
# _builder.add_edge('assess_maintenance',      'generate_recommendation')
# _builder.add_edge('generate_recommendation', 'explain')

# # All paths end here
# _builder.add_edge('explain', END)

# _agent_graph = _builder.compile()


# # ── Main chat function ──────────────────────────────────────────

# def chat(messages: list[dict], context: dict | None = None) -> dict:
#     """
#     Process a conversation and return {"text": "...", "chart": {...}|null}.
#     messages: list of {"role": "user"|"assistant", "content": "..."}
#     context: optional {"mode": "fleet"|"vehicle"|"dtc", "customer_name": "...",
#              "vehicle_number": "...", "dtc_code": "..."}
#     """
#     api_key = os.getenv('OPENAI_API_KEY', '')
#     if not api_key:
#         return {'text': 'OpenAI API key is not configured. Set OPENAI_API_KEY in .env.', 'chart': None}

#     # Convert incoming dicts to LangChain message types
#     lc_messages: list = [SystemMessage(content=SYSTEM_PROMPT)]
#     for msg in messages:
#         role = msg.get('role', 'user')
#         content = msg.get('content', '')
#         if role == 'user':
#             lc_messages.append(HumanMessage(content=content))
#         elif role == 'assistant':
#             lc_messages.append(AIMessage(content=content))

#     initial_state: AgentState = {
#         'messages':                      lc_messages,
#         'context':                       context or {},
#         'chart_config':                  None,
#         'last_tool_data':                [],
#         'intent':                        '',
#         'tool_results':                  {},
#         'fault_analysis':                '',
#         'root_cause':                    '',
#         'maintenance_priority_assessment': '',
#         'recommendation':                '',
#         'nodes_executed':                [],
#         'failure_reasons':               [],
#         'token_usage':                   {'prompt': 0, 'completion': 0},
#         'tools_used':                    [],
#     }

#     t0 = time.time()
#     success = 1
#     final_state = None

#     try:
#         final_state = _agent_graph.invoke(
#             initial_state,
#             config={'recursion_limit': 20},
#         )
#     except Exception as exc:
#         log.error('LangGraph agent error: %s', exc)
#         success = 0
#         return {'text': f'AI service error: {exc}', 'chart': None}
#     finally:
#         latency = round(time.time() - t0, 2)
#         fs = final_state or {}
#         intent          = fs.get('intent') or initial_state.get('intent', 'unknown')
#         tool_results    = fs.get('tool_results', {})
#         tool_calls_made = len(tool_results)
#         nodes_exec      = fs.get('nodes_executed', [])
#         failure_reasons = fs.get('failure_reasons', [])
#         tok             = fs.get('token_usage') or {'prompt': 0, 'completion': 0}

#         # SQL success rate: fraction of tool results without an 'error' key
#         if tool_calls_made:
#             sql_errors = sum(
#                 1 for v in tool_results.values()
#                 if isinstance(v, dict) and 'error' in v
#             )
#             sql_success_rate = round((tool_calls_made - sql_errors) / tool_calls_made, 3)
#         else:
#             sql_success_rate = 1.0

#         if mlflow is not None:
#             try:
#                 with mlflow.start_run(run_name=f"chat_{intent}"):
#                     mlflow.log_params({
#                         'intent':         intent,
#                         'mode':           (context or {}).get('mode', ''),
#                         'customer':       (context or {}).get('customer_name', ''),
#                         'has_vehicle':    bool((context or {}).get('vehicle_number')),
#                         'has_dtc':        bool((context or {}).get('dtc_code')),
#                         'failure_reason': ','.join(failure_reasons) if failure_reasons else 'none',
#                         **{f"{k}_prompt_v": v for k, v in PROMPT_VERSIONS.items()},
#                     })
#                     mlflow.log_metrics({
#                         'latency_sec':          latency,
#                         'success':              float(success),
#                         'tools_called':         float(tool_calls_made),
#                         'nodes_executed_count': float(len(nodes_exec)),
#                         'tokens_prompt':        float(tok.get('prompt', 0)),
#                         'tokens_completion':    float(tok.get('completion', 0)),
#                         'failure_count':        float(len(failure_reasons)),
#                         'sql_success_rate':     sql_success_rate,
#                     })
#             except Exception as mex:
#                 log.warning('MLflow logging failed: %s', mex)

#     last = final_state['messages'][-1]
#     text = getattr(last, 'content', None) or ''
#     return {
#         'text':            text,
#         'chart':           final_state.get('chart_config'),
#         'intent':          final_state.get('intent', ''),
#         'tools_called':    final_state.get('tools_used', []),
#         'token_usage':     final_state.get('token_usage') or {'prompt': 0, 'completion': 0},
#         'nodes_executed':  final_state.get('nodes_executed', []),
#         'failure_reasons': final_state.get('failure_reasons', []),
#     }
