from __future__ import annotations

# IMPORTANT: this must be set before importing src.ai_analyst so evaluation traffic
# does not write into existing ClickHouse observability tables.
import os
os.environ['AI_ANALYST_PERSIST_OBSERVABILITY'] = '0'
os.environ['EVAL_PERSIST_CLICKHOUSE'] = '0'

import argparse
import json
import math
import re
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import httpx

# Add project root to sys.path so `src` package is importable when run directly.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except Exception:
    pass

from evaluation.local_store import (  # noqa: E402
    DEFAULT_OUTPUT_DIR,
    LocalEvaluationStore,
    compute_dataset_hash,
    normalize_sql,
    stable_hash_json,
    utc_now_iso,
    write_json,
)
from evaluation.excel_export import flatten_trace_judge, load_jsonl_rows, write_bulk_evaluation_workbook  # noqa: E402
from evaluation.validators import run_mvp_validation  # noqa: E402

EVALUATION_CODE_VERSION = 'eval_v1.0.0-local-only'
DEFAULT_SCENARIO_FILE = Path(__file__).resolve().parent / 'conversational_scenarios' / 'fleet_diagnostics_core.json'


def truthy(value: str | None, default: bool = False) -> bool:
    if value is None or value == '':
        return default
    return str(value).strip().lower() in {'1', 'true', 'yes', 'on'}


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def safe_div(num: float, den: float, default: float = 0.0) -> float:
    return num / den if den else default


def git_commit() -> str:
    env_sha = os.getenv('GIT_COMMIT_SHA', '').strip()
    if env_sha:
        return env_sha
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=PROJECT_ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return ''


def load_scenario_files(path_value: str) -> tuple[list[Path], dict[str, Any]]:
    candidate = Path(path_value)
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate

    if any(ch in str(candidate) for ch in ['*', '?', '[']):
        paths = sorted(Path(PROJECT_ROOT).glob(path_value)) if not Path(path_value).is_absolute() else sorted(Path().glob(str(candidate)))
    elif candidate.is_dir():
        paths = sorted(candidate.glob('*.json'))
    else:
        paths = [candidate]

    paths = [p for p in paths if p.exists() and p.suffix.lower() == '.json']
    if not paths:
        raise FileNotFoundError(f'No scenario JSON files found for {path_value}')

    merged: dict[str, Any] = {'suite_id': '', 'suite_version': '', 'dataset_version': '', 'scenarios': []}
    for path in paths:
        data = json.loads(path.read_text(encoding='utf-8'))
        if not merged.get('suite_id'):
            merged['suite_id'] = data.get('suite_id', path.stem)
        if not merged.get('suite_version'):
            merged['suite_version'] = data.get('suite_version', os.getenv('EVAL_SUITE_VERSION', 'v1'))
        if not merged.get('dataset_version'):
            merged['dataset_version'] = data.get('dataset_version', os.getenv('AI_ANALYST_DATASET_VERSION', ''))
        scenarios = data.get('scenarios') if isinstance(data.get('scenarios'), list) else []
        merged['scenarios'].extend(scenarios)
    merged['scenario_files'] = [str(p.relative_to(PROJECT_ROOT)) if str(p).startswith(str(PROJECT_ROOT)) else str(p) for p in paths]
    return paths, merged


def collect_lineage(dataset_hash: str) -> dict[str, Any]:
    lineage = {
        'git_commit': git_commit(),
        'graph_version': '',
        'tool_registry_version': '',
        'dataset_hash': dataset_hash,
        'evaluation_code_version': EVALUATION_CODE_VERSION,
        'model_name': os.getenv('EVAL_MODEL_NAME') or os.getenv('AI_ANALYST_MODEL_NAME', ''),
        'prompt_version': os.getenv('EVAL_PROMPT_VERSION', ''),
        'release_version': os.getenv('AI_ANALYST_RELEASE_VERSION', ''),
        'service_version': os.getenv('AI_ANALYST_SERVICE_VERSION', ''),
    }
    try:
        from src import ai_analyst
        prompt_versions = getattr(ai_analyst, 'PROMPT_VERSIONS', {})
        graph_nodes = [
            'detect_intent', 'build_context', 'investigate_data', 'analyze_faults',
            'reason_root_cause', 'assess_maintenance', 'generate_recommendation', 'explain',
        ]
        try:
            prompt_catalog = ai_analyst._build_prompt_catalog()  # type: ignore[attr-defined]
        except Exception:
            prompt_catalog = {}
        tools = getattr(ai_analyst, 'TOOLS', [])
        lineage['graph_version'] = stable_hash_json({'nodes': graph_nodes, 'prompt_versions': prompt_versions, 'prompt_catalog': prompt_catalog})
        lineage['tool_registry_version'] = stable_hash_json({'tools': tools, 'sql_policy': 'read_only_select_with_no_ddl_v1'})
        if not lineage['prompt_version']:
            lineage['prompt_version'] = stable_hash_json(prompt_versions)[:20]
    except Exception:
        lineage['graph_version'] = 'unavailable'
        lineage['tool_registry_version'] = 'unavailable'
    return lineage


def tokenize(text: str) -> set[str]:
    return {t for t in re.findall(r'[a-zA-Z0-9_]+', str(text or '').lower()) if len(t) > 2}


def ngram_set(text: str, n: int = 3) -> set[tuple[str, ...]]:
    toks = list(tokenize(text))
    if len(toks) < n:
        return set()
    return {tuple(toks[i:i + n]) for i in range(len(toks) - n + 1)}


def jaccard(a: set[Any], b: set[Any]) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b) if (a | b) else 0.0


def expected_from_turn(turn: dict[str, Any]) -> dict[str, Any]:
    exp = turn.get('expectations') if isinstance(turn.get('expectations'), dict) else {}
    # Backward-compatible keys if someone authors simpler scenarios.
    if not exp:
        exp = {
            'target_intent': turn.get('expected_intent', ''),
            'required_tools_any': turn.get('expected_tools', []),
            'evidence_anchors': turn.get('expected_facts', []),
            'required_memory_entities': turn.get('memory_assertions', []),
        }
    return exp


def normalize_tool_call(tool: str, args: Any = None) -> str:
    return json.dumps({'tool': tool, 'args': args or {}}, sort_keys=True, default=str)


def extract_tools(result: dict[str, Any]) -> list[str]:
    tools = result.get('tools_called') or result.get('tools_used') or []
    if not isinstance(tools, list):
        return []
    return [str(t) for t in tools if str(t).strip()]


def summarize_tool_results(tool_results: Any) -> dict[str, Any]:
    if not isinstance(tool_results, dict):
        return {'tool_result_count': 0, 'non_empty_tool_results': 0, 'selected_tables': []}
    selected = []
    non_empty = 0
    for value in tool_results.values():
        if isinstance(value, dict):
            if any(k in value for k in ['rows', 'data', 'result', 'results', 'summary']) or len(value) > 0:
                non_empty += 1
            tables = value.get('selected_tables')
            if isinstance(tables, list):
                selected.extend(str(t) for t in tables)
    return {
        'tool_result_count': len(tool_results),
        'non_empty_tool_results': non_empty,
        'selected_tables': sorted(set(selected)),
    }


def sanitize_sql_events(sql_events: Any, base: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    sanitized: list[dict[str, Any]] = []
    raw_for_checks: list[dict[str, Any]] = []
    if not isinstance(sql_events, list):
        return sanitized, raw_for_checks
    for idx, event in enumerate(sql_events):
        if not isinstance(event, dict):
            continue
        raw_query = str(event.get('query') or event.get('sql_query') or '')
        norm = normalize_sql(raw_query)
        row = {
            **base,
            'sql_event_index': idx,
            'node': str(event.get('node') or ''),
            'tool': str(event.get('tool') or ''),
            'success': bool(event.get('success')),
            'duration_sec': safe_float(event.get('duration_sec', event.get('duration', 0.0))),
            'row_count': int(safe_float(event.get('row_count', 0), 0)),
            'sql_hash': norm['sql_hash'],
            'sql_preview': norm['sql_preview'],
            'sql_forbidden_words': norm['forbidden_words'],
            'sql_is_read_only_shape': norm['is_read_only_shape'],
            'sql_has_select_star': norm['has_select_star'],
            'error': str(event.get('error') or ''),
        }
        sanitized.append(row)
        raw_for_checks.append({**row, 'raw_query_lower': raw_query.lower()})
    return sanitized, raw_for_checks


def compute_tool_scores(expected: dict[str, Any], actual_tools: list[str]) -> dict[str, float]:
    required_all = set(str(x) for x in expected.get('required_tools_all', []) if x)
    required_any = set(str(x) for x in expected.get('required_tools_any', []) if x)
    expected_set = required_all | required_any
    actual_set = set(actual_tools)
    if not expected_set:
        return {'tool_precision': 1.0, 'tool_recall': 1.0, 'tool_f1': 1.0}
    recall_hits = len(required_all & actual_set)
    if required_any:
        recall_hits += 1 if required_any & actual_set else 0
        recall_den = len(required_all) + 1
    else:
        recall_den = len(required_all)
    recall = safe_div(recall_hits, recall_den, 0.0)
    precision = safe_div(len(expected_set & actual_set), len(actual_set), 1.0 if not actual_set else 0.0)
    f1 = safe_div(2 * precision * recall, precision + recall, 0.0)
    return {'tool_precision': round(precision, 3), 'tool_recall': round(recall, 3), 'tool_f1': round(f1, 3)}


def evidence_anchor_score(anchors: list[Any], text: str, tool_summary: dict[str, Any]) -> float:
    clean = [str(a).strip().lower() for a in anchors if str(a).strip()]
    if not clean:
        return 1.0 if tool_summary.get('non_empty_tool_results', 0) else 0.6
    text_l = (text or '').lower()
    hits = sum(1 for anchor in clean if anchor in text_l or anchor.replace('_', ' ') in text_l)
    return round(hits / len(clean), 3)


def required_entities(scenario: dict[str, Any], expected: dict[str, Any]) -> list[str]:
    entities: list[str] = []
    context = scenario.get('context') if isinstance(scenario.get('context'), dict) else {}
    for key in ['customer_name', 'vehicle_number', 'uniqueid', 'dtc_code', 'mode']:
        value = context.get(key)
        if value:
            entities.append(str(value))
    for item in expected.get('required_memory_entities', []) or []:
        if isinstance(item, str) and item.strip():
            entities.append(item.strip())
    return list(dict.fromkeys(entities))


def compute_memory_metrics(
    *,
    scenario: dict[str, Any],
    expected: dict[str, Any],
    answer: str,
    raw_sql_events: list[dict[str, Any]],
    previous_answers: list[str],
) -> dict[str, float]:
    entities = required_entities(scenario, expected)
    answer_l = (answer or '').lower()
    retained = 0
    for entity in entities:
        ent_l = entity.lower()
        if ent_l in answer_l or ent_l.replace('_', ' ') in answer_l:
            retained += 1
    entity_retention = safe_div(retained, len(entities), 1.0)

    context = scenario.get('context') if isinstance(scenario.get('context'), dict) else {}
    required_scope = [str(context.get(k)).lower() for k in ['customer_name', 'vehicle_number', 'uniqueid', 'dtc_code'] if context.get(k)]
    scoped_count = 0
    checked = 0
    for event in raw_sql_events:
        q = str(event.get('raw_query_lower') or '')
        if not q:
            continue
        checked += 1
        if all(scope in q for scope in required_scope):
            scoped_count += 1
    scope_retention = safe_div(scoped_count, checked, 1.0)

    if previous_answers:
        continuity = max(jaccard(tokenize(answer), tokenize(prev)) for prev in previous_answers)
    else:
        continuity = 1.0

    contradiction_terms = [
        ('no active faults', 'active faults'),
        ('low severity', 'critical'),
        ('not severe', 'urgent'),
        ('no issue', 'critical'),
    ]
    contradictions = 0
    for prev in previous_answers:
        prev_l = prev.lower()
        for a, b in contradiction_terms:
            if a in prev_l and b in answer_l and a not in answer_l:
                contradictions += 1
    contradiction_rate = safe_div(contradictions, max(len(previous_answers), 1), 0.0)

    ans_ngrams = ngram_set(answer)
    redundant = 0
    for prev in previous_answers:
        if jaccard(ans_ngrams, ngram_set(prev)) > 0.70:
            redundant += 1
    redundant_analysis_rate = safe_div(redundant, max(len(previous_answers), 1), 0.0)

    return {
        'entity_retention': round(entity_retention, 3),
        'scope_retention': round(scope_retention, 3),
        'reasoning_continuity': round(min(1.0, continuity), 3),
        'contradiction_rate': round(min(1.0, contradiction_rate), 3),
        'redundant_analysis_rate': round(min(1.0, redundant_analysis_rate), 3),
    }


def compute_efficiency_metrics(
    *,
    actual_tools: list[str],
    sanitized_sql_events: list[dict[str, Any]],
    previous_tool_calls: Counter[str],
    previous_sql_hashes: Counter[str],
    previous_prompt_tokens: int,
    current_prompt_tokens: int,
    previous_total_tokens: int,
    current_total_tokens: int,
    answer: str,
    previous_answers: list[str],
) -> dict[str, Any]:
    repeated_tool_calls = 0
    for tool in actual_tools:
        key = normalize_tool_call(tool)
        if previous_tool_calls[key] > 0:
            repeated_tool_calls += 1
        previous_tool_calls[key] += 1

    repeated_sql_queries = 0
    for event in sanitized_sql_events:
        h = str(event.get('sql_hash') or '')
        if not h:
            continue
        if previous_sql_hashes[h] > 0:
            repeated_sql_queries += 1
        previous_sql_hashes[h] += 1

    redundant_reasoning = 0.0
    if previous_answers:
        redundant_reasoning = max(jaccard(ngram_set(answer), ngram_set(prev)) for prev in previous_answers)

    context_bloat_rate = safe_div(current_prompt_tokens - previous_prompt_tokens, max(previous_prompt_tokens, 1), 0.0) if previous_prompt_tokens else 0.0
    token_growth_per_turn = max(current_total_tokens - previous_total_tokens, 0) if previous_total_tokens else current_total_tokens

    return {
        'repeated_tool_calls': repeated_tool_calls,
        'repeated_sql_queries': repeated_sql_queries,
        'redundant_reasoning': round(redundant_reasoning, 3),
        'context_bloat_rate': round(context_bloat_rate, 3),
        'token_growth_per_turn': int(token_growth_per_turn),
    }


def compute_gates(
    *,
    expected: dict[str, Any],
    scenario: dict[str, Any],
    answer: str,
    tool_summary: dict[str, Any],
    sanitized_sql_events: list[dict[str, Any]],
    raw_sql_events: list[dict[str, Any]],
    memory: dict[str, float],
    efficiency: dict[str, Any],
) -> tuple[dict[str, bool], list[str]]:
    diagnostic_expected = bool(expected.get('required_tools_any') or expected.get('required_tools_all') or expected.get('evidence_anchors'))
    evidence_ok = True
    if diagnostic_expected:
        anchor_score = evidence_anchor_score(expected.get('evidence_anchors', []) or [], answer, tool_summary)
        evidence_ok = bool(tool_summary.get('non_empty_tool_results', 0) > 0 or anchor_score >= 0.5)

    sql_ok = all(bool(e.get('sql_is_read_only_shape')) for e in sanitized_sql_events) if sanitized_sql_events else True
    sql_ok = sql_ok and not any(e.get('sql_forbidden_words') for e in sanitized_sql_events)

    context = scenario.get('context') if isinstance(scenario.get('context'), dict) else {}
    policies = {}
    for turn_policy in scenario.get('policies', []) if isinstance(scenario.get('policies'), list) else []:
        if isinstance(turn_policy, dict):
            policies.update(turn_policy)
    must_scope = bool(context.get('customer_name') or context.get('vehicle_number') or context.get('dtc_code'))
    scope_ok = memory.get('scope_retention', 1.0) >= (0.95 if must_scope else 0.5)

    answer_l = answer.lower()
    customer = str(context.get('customer_name') or '').lower()
    safety_ok = True
    # Simple tenant-leakage heuristic: if a scenario has explicit forbidden customers, fail if mentioned.
    for forbidden in scenario.get('forbidden_customer_names', []) or []:
        if str(forbidden).lower() in answer_l:
            safety_ok = False
    if 'another customer' in answer_l and customer:
        safety_ok = False

    gates = {
        'safety_ok': safety_ok,
        'evidence_ok': evidence_ok,
        'sql_ok': sql_ok,
        'scope_ok': scope_ok,
        'contradictions_ok': memory.get('contradiction_rate', 0.0) < 0.05,
        'repetition_ok': int(efficiency.get('repeated_sql_queries', 0) or 0) <= 2,
    }
    violated = [name for name, ok in gates.items() if not ok]
    return gates, violated


def compute_dimensions(
    *,
    expected: dict[str, Any],
    actual_intent: str,
    answer: str,
    tool_scores: dict[str, float],
    tool_summary: dict[str, Any],
    sanitized_sql_events: list[dict[str, Any]],
    gates: dict[str, bool],
    memory: dict[str, float],
    efficiency: dict[str, Any],
) -> dict[str, float]:
    target_intent = str(expected.get('target_intent') or '')
    intent_match = 1.0 if not target_intent or target_intent == actual_intent else 0.0
    anchor = evidence_anchor_score(expected.get('evidence_anchors', []) or [], answer, tool_summary)
    task_fulfillment = round(0.45 * intent_match + 0.35 * anchor + 0.20 * min(len(answer or '') / 500.0, 1.0), 3)
    factual_grounding = round(0.55 * anchor + 0.45 * min(float(tool_summary.get('non_empty_tool_results', 0)), 1.0), 3)
    safety = 1.0 if gates.get('safety_ok') else 0.0
    tool_use = round(tool_scores.get('tool_f1', 0.0), 3)
    sql_hygiene = 1.0
    if sanitized_sql_events:
        ok_count = sum(1 for e in sanitized_sql_events if e.get('sql_is_read_only_shape') and not e.get('sql_has_select_star'))
        sql_hygiene = safe_div(ok_count, len(sanitized_sql_events), 0.0)
    memory_score = round(
        0.30 * memory.get('entity_retention', 0.0)
        + 0.30 * memory.get('scope_retention', 0.0)
        + 0.20 * memory.get('reasoning_continuity', 0.0)
        + 0.10 * (1.0 - memory.get('contradiction_rate', 0.0))
        + 0.10 * (1.0 - memory.get('redundant_analysis_rate', 0.0)),
        3,
    )
    repeated_penalty = min(0.5, 0.10 * int(efficiency.get('repeated_tool_calls', 0) or 0) + 0.15 * int(efficiency.get('repeated_sql_queries', 0) or 0))
    bloat_penalty = min(0.3, max(float(efficiency.get('context_bloat_rate', 0.0) or 0.0), 0.0) * 0.2)
    redundant_penalty = min(0.2, float(efficiency.get('redundant_reasoning', 0.0) or 0.0) * 0.2)
    efficiency_score = round(max(0.0, 1.0 - repeated_penalty - bloat_penalty - redundant_penalty), 3)
    return {
        'task_fulfillment': task_fulfillment,
        'factual_grounding': factual_grounding,
        'safety': round(safety, 3),
        'tool_use': tool_use,
        'sql_hygiene': round(sql_hygiene, 3),
        'memory': memory_score,
        'efficiency': efficiency_score,
    }


def status_from_gates(violated: list[str], failures: list[Any]) -> str:
    if failures:
        return 'FAIL_RUNTIME'
    if not violated:
        return 'PASS'
    if 'scope_ok' in violated:
        return 'FAIL_SCOPE'
    if 'evidence_ok' in violated:
        return 'FAIL_EVIDENCE'
    return 'FAIL_GATE'


def dry_run_result(turn: dict[str, Any], scenario: dict[str, Any]) -> dict[str, Any]:
    expected = expected_from_turn(turn)
    tools = expected.get('required_tools_all') or expected.get('required_tools_any') or []
    return {
        'request_id': f"dry_{scenario.get('scenario_id')}_{turn.get('turn_id')}",
        'text': f"[DRY RUN] Would answer: {turn.get('user_message', '')}",
        'intent': expected.get('target_intent', ''),
        'tools_called': list(tools)[:1],
        'tool_results': {'dry_tool': {'rows': [{'sample': 1}], 'selected_tables': []}} if tools else {},
        'token_usage': {'prompt': 100, 'completion': 40},
        'trace_log': [],
        'sql_events': [],
        'failure_reasons': [],
        'evaluation': {},
        'version': {},
    }


def check_api_health(api_base_url: str) -> None:
    url = api_base_url.rstrip('/') + '/health'
    timeout_sec = float(os.getenv('EVAL_HEALTH_TIMEOUT_SEC', '30') or '30')
    retries = max(1, int(os.getenv('EVAL_HEALTH_RETRIES', '3') or '3'))
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = httpx.get(url, timeout=timeout_sec)
            resp.raise_for_status()
            data = resp.json()
            if str(data.get('status', '')).lower() != 'ok':
                raise RuntimeError(f'Unexpected health response: {data}')
            return
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(min(5 * attempt, 15))
    raise RuntimeError(
        f'Backend not reachable at {api_base_url} after {retries} attempts '
        f'(health timeout {timeout_sec}s each). '
        f'Start it with: python -m uvicorn src.api_server:app --host 127.0.0.1 --port 8001'
    ) from last_exc


def call_ai_api(
    *,
    api_base_url: str,
    messages: list[dict[str, str]],
    context: dict[str, Any],
    conversation_id: str,
) -> dict[str, Any]:
    timeout_sec = float(os.getenv('AI_ANALYST_EVAL_TIMEOUT_SEC', '180') or '180')
    payload: dict[str, Any] = {
        'messages': messages,
        'context': context,
    }
    if conversation_id:
        payload['conversation_id'] = conversation_id

    resp = httpx.post(
        api_base_url.rstrip('/') + '/api/ai/chat',
        json=payload,
        timeout=timeout_sec,
    )
    if resp.status_code >= 400:
        raise RuntimeError(f'API error {resp.status_code}: {resp.text[:500]}')
    data = resp.json()
    if not isinstance(data, dict):
        raise RuntimeError(f'Unexpected API response type: {type(data)}')
    return data


def call_ai(
    messages: list[dict[str, str]],
    context: dict[str, Any],
    dry_run: bool,
    turn: dict[str, Any],
    scenario: dict[str, Any],
    *,
    api_base_url: str = '',
    conversation_id: str = '',
) -> tuple[dict[str, Any], str]:
    if dry_run:
        return dry_run_result(turn, scenario), conversation_id
    if api_base_url:
        result = call_ai_api(
            api_base_url=api_base_url,
            messages=messages,
            context=context,
            conversation_id=conversation_id,
        )
        new_conversation_id = str(result.get('conversation_id') or conversation_id or '')
        return result, new_conversation_id
    from src.ai_analyst import chat
    return chat(messages=messages, context=context), conversation_id


def run_scenario(
    *,
    scenario: dict[str, Any],
    suite: dict[str, Any],
    store: LocalEvaluationStore,
    lineage: dict[str, Any],
    dry_run: bool,
    max_turns: int,
    default_customer_name: str,
    api_base_url: str = '',
    store_full_answer: bool = False,
) -> dict[str, Any]:
    scenario_id = str(scenario.get('scenario_id') or scenario.get('id') or 'unknown_scenario')
    category = str(scenario.get('category') or 'Uncategorized')
    difficulty_tier = str(scenario.get('difficulty_tier') or 'Medium')
    base_context = dict(scenario.get('context') or {})
    if default_customer_name and not base_context.get('customer_name'):
        base_context['customer_name'] = default_customer_name
    if not base_context.get('mode'):
        base_context['mode'] = 'general'

    messages: list[dict[str, str]] = []
    previous_answers: list[str] = []
    previous_tool_calls: Counter[str] = Counter()
    previous_sql_hashes: Counter[str] = Counter()
    previous_prompt_tokens = 0
    previous_total_tokens = 0

    turns = scenario.get('turns') if isinstance(scenario.get('turns'), list) else []
    if max_turns > 0:
        turns = turns[:max_turns]

    turn_records: list[dict[str, Any]] = []
    violated_all: list[str] = []
    started = time.time()
    conversation_id = ''

    for index, turn in enumerate(turns, 1):
        user_message = str(turn.get('user_message') or turn.get('message') or '')
        if not user_message:
            continue
        expected = expected_from_turn(turn)
        context = dict(base_context)
        context.update({
            'evaluation_run_id': store.run_id,
            'evaluation_suite_id': suite.get('suite_id'),
            'evaluation_suite_version': suite.get('suite_version'),
            'evaluation_code_version': EVALUATION_CODE_VERSION,
            'scenario_id': scenario_id,
            'turn_id': turn.get('turn_id', index),
            'difficulty_tier': difficulty_tier,
            'force_detailed_response': True,
        })
        messages.append({'role': 'user', 'content': user_message})

        t0 = time.time()
        try:
            result, conversation_id = call_ai(
                messages,
                context,
                dry_run,
                turn,
                scenario,
                api_base_url=api_base_url,
                conversation_id=conversation_id,
            )
        except Exception as exc:
            result = {
                'request_id': '',
                'text': '',
                'intent': '',
                'tools_called': [],
                'tool_results': {},
                'token_usage': {'prompt': 0, 'completion': 0},
                'trace_log': [],
                'sql_events': [],
                'failure_reasons': [f'exception:{exc}'],
                'evaluation': {},
                'version': {},
            }
        elapsed_ms = int((time.time() - t0) * 1000)
        answer = str(result.get('text') or '')
        messages.append({'role': 'assistant', 'content': answer})

        request_id = str(result.get('request_id') or '')
        token_usage = result.get('token_usage') if isinstance(result.get('token_usage'), dict) else {}
        prompt_tokens = int(safe_float(token_usage.get('prompt', 0), 0))
        completion_tokens = int(safe_float(token_usage.get('completion', 0), 0))
        total_tokens = prompt_tokens + completion_tokens
        actual_tools = extract_tools(result)
        tool_scores = compute_tool_scores(expected, actual_tools)
        tool_summary = summarize_tool_results(result.get('tool_results'))

        base_sql = {
            'run_id': store.run_id,
            'scenario_id': scenario_id,
            'turn_index': index,
            'request_id': request_id,
            'category': category,
            'difficulty_tier': difficulty_tier,
        }
        sanitized_sql_events, raw_sql_events = sanitize_sql_events(result.get('sql_events'), base_sql)
        for sql_row in sanitized_sql_events:
            store.append_sql_event(sql_row)

        memory = compute_memory_metrics(
            scenario=scenario,
            expected=expected,
            answer=answer,
            raw_sql_events=raw_sql_events,
            previous_answers=previous_answers,
        )
        efficiency = compute_efficiency_metrics(
            actual_tools=actual_tools,
            sanitized_sql_events=sanitized_sql_events,
            previous_tool_calls=previous_tool_calls,
            previous_sql_hashes=previous_sql_hashes,
            previous_prompt_tokens=previous_prompt_tokens,
            current_prompt_tokens=prompt_tokens,
            previous_total_tokens=previous_total_tokens,
            current_total_tokens=total_tokens,
            answer=answer,
            previous_answers=previous_answers,
        )
        gates, violated = compute_gates(
            expected=expected,
            scenario=scenario,
            answer=answer,
            tool_summary=tool_summary,
            sanitized_sql_events=sanitized_sql_events,
            raw_sql_events=raw_sql_events,
            memory=memory,
            efficiency=efficiency,
        )
        dimensions = compute_dimensions(
            expected=expected,
            actual_intent=str(result.get('intent') or ''),
            answer=answer,
            tool_scores=tool_scores,
            tool_summary=tool_summary,
            sanitized_sql_events=sanitized_sql_events,
            gates=gates,
            memory=memory,
            efficiency=efficiency,
        )
        failures = result.get('failure_reasons') if isinstance(result.get('failure_reasons'), list) else []
        status_label = status_from_gates(violated, failures)

        record = {
            'run_id': store.run_id,
            'scenario_id': scenario_id,
            'category': category,
            'difficulty_tier': difficulty_tier,
            'turn_index': index,
            'turn_id': turn.get('turn_id', index),
            'timestamp': utc_now_iso(),
            'elapsed_ms': elapsed_ms,
            'request_id': request_id,
            'customer_name': base_context.get('customer_name', ''),
            'mode': base_context.get('mode', ''),
            'user_message': user_message,
            'answer_text': answer if store_full_answer else '',
            'final_answer_preview': answer[:500],
            'store_full_answer': store_full_answer,
            'final_answer': answer if store_full_answer else '',
            'expected_intent': expected.get('target_intent', ''),
            'actual_intent': result.get('intent', ''),
            **flatten_trace_judge(result.get('evaluation')),
            'expected_tools_any': expected.get('required_tools_any', []),
            'expected_tools_all': expected.get('required_tools_all', []),
            'actual_tools': actual_tools,
            **tool_scores,
            'tool_summary': tool_summary,
            'gates': gates,
            'gate_passed': all(gates.values()),
            'violated_gates': violated,
            'dimensions': dimensions,
            'memory': memory,
            'efficiency': efficiency,
            'token_usage': {'prompt': prompt_tokens, 'completion': completion_tokens, 'total': total_tokens},
            'sql_event_count': len(sanitized_sql_events),
            'sql_hashes': [e.get('sql_hash') for e in sanitized_sql_events],
            'failure_count': len(failures),
            'failure_reasons': failures,
            'status_label': status_label,
            'conversation_id': conversation_id,
            'lineage': lineage,
            'langsmith': {
                'request_id': request_id,
                'project': os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or '',
                'enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False),
            },
        }

        validation = run_mvp_validation(scenario=scenario, turn=turn, result=result, turn_record=record)
        validation_score_record = validation.turn_score_record()
        record['validation'] = validation_score_record
        record['validation_status'] = validation_score_record.get('status')
        record['validation_finding_count'] = validation_score_record.get('finding_count', 0)
        record['validation_critical_finding_count'] = validation_score_record.get('critical_finding_count', 0)

        for claim in validation.claims:
            store.append_validation_claim(claim.to_dict())
        for evidence_item in validation.evidence_items:
            store.append_validation_evidence(evidence_item.to_dict())
        for claim_map in validation.claim_evidence_map:
            store.append_claim_evidence_map(claim_map)
        for finding in validation.findings:
            store.append_validation_finding(finding.to_dict())
        store.append_validation_turn_score(validation_score_record)
        for review_item in validation.human_review_items:
            store.append_human_review_item(review_item)

        store.append_turn(record)
        store.append_token_row({
            'run_id': store.run_id,
            'scenario_id': scenario_id,
            'turn_index': index,
            'request_id': request_id,
            'prompt_tokens': prompt_tokens,
            'completion_tokens': completion_tokens,
            'total_tokens': total_tokens,
            'token_growth_per_turn': efficiency.get('token_growth_per_turn'),
            'context_bloat_rate': efficiency.get('context_bloat_rate'),
        })

        previous_answers.append(answer)
        previous_prompt_tokens = prompt_tokens
        previous_total_tokens = total_tokens
        violated_all.extend(violated)
        turn_records.append(record)

        print(f"  - {scenario_id} turn {index}: {status_label} | {elapsed_ms}ms | req={request_id[:8]}")

    elapsed_total_ms = int((time.time() - started) * 1000)
    rollup = rollup_scenario(store.run_id, scenario, turn_records, violated_all, elapsed_total_ms)
    store.append_scenario_rollup(rollup)
    return rollup


def avg(values: list[float]) -> float:
    return round(sum(values) / len(values), 3) if values else 0.0


def rollup_scenario(run_id: str, scenario: dict[str, Any], turn_records: list[dict[str, Any]], violated_all: list[str], elapsed_total_ms: int) -> dict[str, Any]:
    dim_keys = ['task_fulfillment', 'factual_grounding', 'safety', 'tool_use', 'sql_hygiene', 'memory', 'efficiency']
    mem_keys = ['entity_retention', 'scope_retention', 'reasoning_continuity', 'contradiction_rate', 'redundant_analysis_rate']
    eff_keys = ['repeated_tool_calls', 'repeated_sql_queries', 'redundant_reasoning', 'context_bloat_rate', 'token_growth_per_turn']
    return {
        'run_id': run_id,
        'scenario_id': scenario.get('scenario_id') or scenario.get('id'),
        'category': scenario.get('category', 'Uncategorized'),
        'difficulty_tier': scenario.get('difficulty_tier', 'Medium'),
        'turns_count': len(turn_records),
        'successful_turns': sum(1 for r in turn_records if r.get('gate_passed') and not r.get('failure_count')),
        'failed_turns': sum(1 for r in turn_records if not r.get('gate_passed') or r.get('failure_count')),
        'gates_passed': all(r.get('gate_passed') for r in turn_records) if turn_records else False,
        'violated_gates': sorted(set(violated_all)),
        'violation_counts': dict(Counter(violated_all)),
        'mean_dimensions': {k: avg([safe_float((r.get('dimensions') or {}).get(k)) for r in turn_records]) for k in dim_keys},
        'mean_memory': {k: avg([safe_float((r.get('memory') or {}).get(k)) for r in turn_records]) for k in mem_keys},
        'mean_efficiency': {k: avg([safe_float((r.get('efficiency') or {}).get(k)) for r in turn_records]) for k in eff_keys},
        'total_prompt_tokens': sum(int((r.get('token_usage') or {}).get('prompt', 0)) for r in turn_records),
        'total_completion_tokens': sum(int((r.get('token_usage') or {}).get('completion', 0)) for r in turn_records),
        'total_tokens': sum(int((r.get('token_usage') or {}).get('total', 0)) for r in turn_records),
        'total_sql_calls': sum(int(r.get('sql_event_count', 0) or 0) for r in turn_records),
        'elapsed_ms_total': elapsed_total_ms,
    }


def build_summary(store: LocalEvaluationStore, suite: dict[str, Any], lineage: dict[str, Any], rollups: list[dict[str, Any]], started_at: str) -> dict[str, Any]:
    finished_at = utc_now_iso()
    all_violations = Counter()
    for r in rollups:
        all_violations.update(r.get('violation_counts') or {})
    dim_keys = ['task_fulfillment', 'factual_grounding', 'safety', 'tool_use', 'sql_hygiene', 'memory', 'efficiency']
    mem_keys = ['entity_retention', 'scope_retention', 'reasoning_continuity', 'contradiction_rate', 'redundant_analysis_rate']
    eff_keys = ['repeated_tool_calls', 'repeated_sql_queries', 'redundant_reasoning', 'context_bloat_rate', 'token_growth_per_turn']

    tier_agg: dict[str, dict[str, Any]] = {}
    for tier in sorted({str(r.get('difficulty_tier', 'Medium')) for r in rollups}):
        rows = [r for r in rollups if str(r.get('difficulty_tier', 'Medium')) == tier]
        tier_agg[tier] = {
            'scenarios': len(rows),
            'pass_rate': round(safe_div(sum(1 for r in rows if r.get('gates_passed')), len(rows), 0.0), 3),
            'avg_dimensions': {k: avg([safe_float((r.get('mean_dimensions') or {}).get(k)) for r in rows]) for k in dim_keys},
        }

    category_agg: dict[str, dict[str, Any]] = {}
    for cat in sorted({str(r.get('category', 'Uncategorized')) for r in rollups}):
        rows = [r for r in rollups if str(r.get('category', 'Uncategorized')) == cat]
        category_agg[cat] = {
            'scenarios': len(rows),
            'pass_rate': round(safe_div(sum(1 for r in rows if r.get('gates_passed')), len(rows), 0.0), 3),
            'avg_dimensions': {k: avg([safe_float((r.get('mean_dimensions') or {}).get(k)) for r in rows]) for k in dim_keys},
        }

    return {
        'run_id': store.run_id,
        'suite_id': suite.get('suite_id'),
        'suite_version': suite.get('suite_version'),
        'dataset_version': suite.get('dataset_version'),
        'started_at': started_at,
        'finished_at': finished_at,
        'status': 'completed',
        'local_only': True,
        'clickhouse_evaluation_tables': False,
        'ai_analyst_persist_observability': os.getenv('AI_ANALYST_PERSIST_OBSERVABILITY') not in {'0', 'false', 'False', 'no', 'NO'},
        'ai_analyst_observability_disabled': os.getenv('AI_ANALYST_PERSIST_OBSERVABILITY') in {'0', 'false', 'False', 'no', 'NO'},
        'scenarios_evaluated': len(rollups),
        'turns_evaluated': sum(int(r.get('turns_count', 0)) for r in rollups),
        'gate_pass_rate': round(safe_div(sum(1 for r in rollups if r.get('gates_passed')), len(rollups), 0.0), 3),
        'dimension_averages': {k: avg([safe_float((r.get('mean_dimensions') or {}).get(k)) for r in rollups]) for k in dim_keys},
        'memory_averages': {k: avg([safe_float((r.get('mean_memory') or {}).get(k)) for r in rollups]) for k in mem_keys},
        'efficiency_averages': {k: avg([safe_float((r.get('mean_efficiency') or {}).get(k)) for r in rollups]) for k in eff_keys},
        'tier_aggregates': tier_agg,
        'category_aggregates': category_agg,
        'top_violations': all_violations.most_common(20),
        'total_tokens': sum(int(r.get('total_tokens', 0)) for r in rollups),
        'total_sql_calls': sum(int(r.get('total_sql_calls', 0)) for r in rollups),
        'lineage': lineage,
    }


def build_validation_summary(store: LocalEvaluationStore) -> dict[str, Any]:
    finding_counts = Counter(str(r.get('failure_type') or 'unknown') for r in store.validation_finding_rows)
    severity_counts = Counter(str(r.get('severity') or 'unknown') for r in store.validation_finding_rows)
    status_counts = Counter(str(r.get('status') or 'unknown') for r in store.validation_score_rows)
    gate_failures: Counter[str] = Counter()
    for row in store.validation_score_rows:
        gates = row.get('gates') if isinstance(row.get('gates'), dict) else {}
        for gate, ok in gates.items():
            if not ok:
                gate_failures[str(gate)] += 1
    return {
        'run_id': store.run_id,
        'validation_layer': 'mvp_deterministic_heuristic',
        'claims_count': len(store.validation_claim_rows),
        'evidence_items_count': len(store.validation_evidence_rows),
        'claim_evidence_links_count': len(store.validation_map_rows),
        'findings_count': len(store.validation_finding_rows),
        'human_review_items_count': len(store.human_review_rows),
        'status_counts': dict(status_counts),
        'severity_counts': dict(severity_counts),
        'top_failure_types': finding_counts.most_common(25),
        'gate_failures': dict(gate_failures),
        'local_only': True,
        'clickhouse_evaluation_tables': False,
    }


def load_summary(run_dir: Path) -> dict[str, Any] | None:
    path = run_dir / 'summary.json'
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def compare_with_baseline(current_summary: dict[str, Any], baseline_dir: str | None) -> dict[str, Any] | None:
    if not baseline_dir:
        return None
    baseline = load_summary(Path(baseline_dir))
    if not baseline:
        return {'error': f'No readable baseline summary found at {baseline_dir}'}
    current_dims = current_summary.get('dimension_averages') or {}
    base_dims = baseline.get('dimension_averages') or {}
    keys = sorted(set(current_dims) | set(base_dims))
    dim_deltas = {k: round(safe_float(current_dims.get(k)) - safe_float(base_dims.get(k)), 3) for k in keys}
    return {
        'baseline_run_id': baseline.get('run_id'),
        'candidate_run_id': current_summary.get('run_id'),
        'dataset_hash_match': (baseline.get('lineage') or {}).get('dataset_hash') == (current_summary.get('lineage') or {}).get('dataset_hash'),
        'gate_pass_rate_delta': round(safe_float(current_summary.get('gate_pass_rate')) - safe_float(baseline.get('gate_pass_rate')), 3),
        'dimension_deltas': dim_deltas,
        'regressions': [
            {'metric': k, 'baseline': safe_float(base_dims.get(k)), 'candidate': safe_float(current_dims.get(k)), 'delta': v, 'severity': 'high' if v < -0.1 else 'medium'}
            for k, v in dim_deltas.items()
            if v < -0.03
        ],
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Local-only multi-turn conversational evaluation runner for the DTC AI Analyst.')
    parser.add_argument('--scenario-file', default=os.getenv('EVAL_SCENARIO_FILE', str(DEFAULT_SCENARIO_FILE)))
    parser.add_argument('--output-dir', default=os.getenv('EVAL_OUTPUT_DIR', str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument('--run-label', default=os.getenv('EVAL_RUN_LABEL', 'local-conversational-eval'))
    parser.add_argument('--limit-scenarios', type=int, default=int(os.getenv('EVAL_LIMIT_SCENARIOS', '0') or '0'))
    parser.add_argument('--max-turns', type=int, default=int(os.getenv('EVAL_MAX_TURNS', '0') or '0'))
    parser.add_argument('--baseline-dir', default=os.getenv('EVAL_BASELINE_DIR', ''))
    parser.add_argument('--dry-run', action='store_true', default=truthy(os.getenv('EVAL_DRY_RUN'), False))
    parser.add_argument('--write-excel', action='store_true', default=truthy(os.getenv('EVAL_WRITE_EXCEL'), False))
    parser.add_argument('--api-base-url', default=os.getenv('EVAL_API_BASE_URL', 'http://127.0.0.1:8001'))
    parser.add_argument('--run-id', default=os.getenv('EVAL_RUN_ID', ''))
    parser.add_argument('--store-full-answer', action='store_true', default=truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False))
    parser.add_argument('--skip-unified-excel', action='store_true', help='Skip unified Excel export (orchestrator writes combined workbook).')
    return parser.parse_args(argv)


def run_conversational_eval(args: argparse.Namespace) -> dict[str, Any]:
    os.environ['AI_ANALYST_PERSIST_OBSERVABILITY'] = '0'
    os.environ['EVAL_PERSIST_CLICKHOUSE'] = '0'

    scenario_paths, suite = load_scenario_files(args.scenario_file)
    dataset_hash = compute_dataset_hash(scenario_paths)
    lineage = collect_lineage(dataset_hash)

    scenarios = suite.get('scenarios') if isinstance(suite.get('scenarios'), list) else []
    if args.limit_scenarios > 0:
        scenarios = scenarios[:args.limit_scenarios]
    if not scenarios:
        raise ValueError('No scenarios found.')

    api_base_url = '' if args.dry_run else str(args.api_base_url or '').strip()
    if api_base_url:
        print(f'Using backend API: {api_base_url}')
        check_api_health(api_base_url)

    run_id = str(args.run_id or '').strip() or None
    store = LocalEvaluationStore(args.output_dir, run_id=run_id)
    store_full_answer = args.store_full_answer or truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False)
    started_at = utc_now_iso()
    manifest = {
        'run_id': store.run_id,
        'run_label': args.run_label,
        'started_at': started_at,
        'status': 'running',
        'local_only': True,
        'clickhouse_evaluation_tables': False,
        'ai_analyst_persist_observability': os.getenv('AI_ANALYST_PERSIST_OBSERVABILITY') not in {'0', 'false', 'False', 'no', 'NO'},
        'ai_analyst_observability_disabled': os.getenv('AI_ANALYST_PERSIST_OBSERVABILITY') in {'0', 'false', 'False', 'no', 'NO'},
        'scenario_files': suite.get('scenario_files', []),
        'suite_id': suite.get('suite_id'),
        'suite_version': suite.get('suite_version'),
        'dataset_version': suite.get('dataset_version'),
        'lineage': lineage,
        'model_name': lineage.get('model_name'),
        'prompt_version': lineage.get('prompt_version'),
        'langsmith_enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False),
        'langsmith_project': os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or '',
        'api_base_url': api_base_url or None,
        'env_snapshot': {
            'AI_ANALYST_PERSIST_OBSERVABILITY': os.getenv('AI_ANALYST_PERSIST_OBSERVABILITY'),
            'EVAL_PERSIST_CLICKHOUSE': os.getenv('EVAL_PERSIST_CLICKHOUSE'),
            'DEPLOYMENT_ENV': os.getenv('DEPLOYMENT_ENV', os.getenv('ENV_NAME', 'dev')),
            'AI_ANALYST_MODEL_NAME': os.getenv('AI_ANALYST_MODEL_NAME', ''),
            'AI_ANALYST_EVAL_TIMEOUT_SEC': os.getenv('AI_ANALYST_EVAL_TIMEOUT_SEC', ''),
        },
    }
    store.write_manifest(manifest)
    store.write_scenario_snapshot({**suite, 'scenarios': scenarios})

    print(f'Local-only conversational evaluation run: {store.run_id}')
    print(f'Artifacts: {store.run_dir}')
    print('ClickHouse evaluation persistence: disabled')

    default_customer = os.getenv('EVAL_CUSTOMER_NAME', '').strip()
    rollups = []
    for scenario in scenarios:
        print(f"\nScenario: {scenario.get('scenario_id') or scenario.get('id')} [{scenario.get('difficulty_tier', 'Medium')}]")
        rollups.append(
            run_scenario(
                scenario=scenario,
                suite=suite,
                store=store,
                lineage=lineage,
                dry_run=args.dry_run,
                max_turns=args.max_turns,
                default_customer_name=default_customer,
                api_base_url=api_base_url,
                store_full_answer=store_full_answer,
            )
        )

    summary = build_summary(store, suite, lineage, rollups, started_at)
    store.write_summary(summary)
    store.write_validation_summary(build_validation_summary(store))
    delta = compare_with_baseline(summary, args.baseline_dir or None)
    store.write_benchmark_delta(delta)
    store.finalize_tables()
    if args.write_excel and not args.skip_unified_excel:
        xlsx_path = store.path('full_evaluation_report.xlsx')
        combined_summary = {
            **summary,
            'single_turn_questions': len(load_jsonl_rows(store.path('single_turn_results.jsonl'))),
            'multi_turn_turns': summary.get('turns_evaluated', len(store.turn_rows)),
        }
        wrote = write_bulk_evaluation_workbook(
            output_path=xlsx_path,
            summary=combined_summary,
            single_turn_rows=load_jsonl_rows(store.path('single_turn_results.jsonl')),
            multi_turn_rows=store.turn_rows,
        )
        if wrote:
            print(f'Unified Excel written: {xlsx_path}')
        else:
            print('Unified Excel export skipped: install openpyxl')
    elif args.write_excel:
        wrote = store.write_excel_workbook_if_available()
        if not wrote:
            print('Excel export skipped: openpyxl is not installed.')
    store.write_latest_pointer()

    manifest['status'] = 'completed'
    manifest['finished_at'] = summary.get('finished_at')
    store.write_manifest(manifest)

    print('\nCompleted local-only conversational evaluation.')
    print(f"Scenarios: {summary['scenarios_evaluated']} | Turns: {summary['turns_evaluated']} | Gate pass rate: {summary['gate_pass_rate']}")
    print(f'Artifacts written to: {store.run_dir}')
    return {
        'run_id': store.run_id,
        'run_dir': str(store.run_dir),
        'summary': summary,
        'turn_rows': store.turn_rows,
    }


def main() -> int:
    args = parse_args()
    try:
        run_conversational_eval(args)
    except Exception as exc:
        print(f'Conversational evaluation failed: {exc}')
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
