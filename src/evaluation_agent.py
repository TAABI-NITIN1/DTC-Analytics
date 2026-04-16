import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
import os
from src.persona import SYSTEM_PERSONA

api_key = os.getenv('OPENAI_API_KEY')

def _safe_score(value: Any, default: float = 0.5) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = default
    return max(0.0, min(1.0, round(parsed, 3)))


def _extract_root_cause_fallback(trace_log: list[dict]) -> str:
    for event in trace_log:
        if not isinstance(event, dict) or event.get('type') != 'node':
            continue
        metrics = event.get('metrics') or {}
        if int(metrics.get('sql_errors_delta', 0) or 0) > 0:
            return str(event.get('node') or 'investigate_data')
        if int(metrics.get('tool_called', 1) or 0) == 0:
            return str(event.get('node') or 'investigate_data')
        if int(metrics.get('intent_is_unknown', 0) or 0) == 1:
            return str(event.get('node') or 'detect_intent')
    return 'unknown'


def _build_eval_input(query: str, trace_log: list[dict], final_answer: str) -> dict:
    node_events = [e for e in trace_log if isinstance(e, dict) and e.get('type') == 'node']
    nodes = []
    for event in node_events:
        nodes.append(
            {
                'node': event.get('node'),
                'duration': event.get('duration'),
                'status': event.get('status'),
                'metrics': event.get('metrics') or {},
                'sql_query': event.get('sql_query'),
                'row_count': event.get('row_count'),
                'failure_reasons': event.get('failure_reasons') or [],
            }
        )
    return {
        'query': query,
        'nodes': nodes,
        'final_answer': final_answer,
    }


def _compute_final_score(result: dict) -> float:
    correctness = _safe_score(result.get('correctness'), 0.5)
    completeness = _safe_score(result.get('completeness'), 0.5)
    relevance = _safe_score(result.get('relevance'), 0.5)
    groundedness = _safe_score(result.get('groundedness'), 0.5)
    return round(
        (0.30 * correctness)
        + (0.25 * groundedness)
        + (0.20 * relevance)
        + (0.25 * completeness),
        3,
    )


def evaluate_trace(
    *,
    api_key: str,
    query: str,
    trace_log: list[dict],
    final_answer: str,
    model_name: str = 'gpt-4o-mini',
    timeout: int = 45,
) -> dict:
    eval_input = _build_eval_input(query=query, trace_log=trace_log, final_answer=final_answer)

    if not api_key:
        fallback = {
            'correctness': 0.5,
            'completeness': 0.5,
            'relevance': 0.5,
            'groundedness': 0.5,
            'node_scores': {},
            'root_cause': _extract_root_cause_fallback(trace_log),
            'explanation': 'Trace evaluator skipped: missing API key.',
            'judge_mode': 'fallback',
        }
        fallback['final_score'] = _compute_final_score(fallback)
        return fallback

    system_prompt = (
        f'{SYSTEM_PERSONA}\n\n'
        'You are an AI system evaluator. Evaluate execution trace quality for a fleet diagnostics agent. '
        'Use the provided node metrics and SQL evidence as grounding signals. Return STRICT JSON with keys: '
        'correctness, completeness, relevance, groundedness, node_scores, root_cause, explanation. '
        'Scores must be numbers between 0 and 1. root_cause should be a node name if identifiable.'
    )

    llm = ChatOpenAI(
        model=model_name,
        api_key=api_key,
        temperature=0,
        timeout=timeout,
        model_kwargs={'response_format': {'type': 'json_object'}},
    )

    try:
        response = llm.invoke(
            [
                SystemMessage(content=system_prompt),
                HumanMessage(content=json.dumps(eval_input, default=str)),
            ]
        )
        raw_content = response.content if hasattr(response, 'content') else response
        parsed = json.loads(raw_content)
        result = {
            'correctness': _safe_score(parsed.get('correctness'), 0.5),
            'completeness': _safe_score(parsed.get('completeness'), 0.5),
            'relevance': _safe_score(parsed.get('relevance'), 0.5),
            'groundedness': _safe_score(parsed.get('groundedness'), 0.5),
            'node_scores': parsed.get('node_scores') if isinstance(parsed.get('node_scores'), dict) else {},
            'root_cause': str(parsed.get('root_cause') or _extract_root_cause_fallback(trace_log)),
            'explanation': str(parsed.get('explanation') or ''),
            'judge_mode': 'llm',
        }
        result['final_score'] = _compute_final_score(result)
        return result
    except Exception:
        fallback = {
            'correctness': 0.5,
            'completeness': 0.5,
            'relevance': 0.5,
            'groundedness': 0.5,
            'node_scores': {},
            'root_cause': _extract_root_cause_fallback(trace_log),
            'explanation': 'Trace evaluator fallback due to parsing or runtime error.',
            'judge_mode': 'fallback',
        }
        fallback['final_score'] = _compute_final_score(fallback)
        return fallback
