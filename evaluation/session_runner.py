"""Unified session runner for single, static multi, and dynamic multi conversations."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

os.environ.setdefault('AI_ANALYST_PERSIST_OBSERVABILITY', '0')
os.environ.setdefault('EVAL_PERSIST_CLICKHOUSE', '0')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from evaluation.langsmith_eval_env import configure_eval_langsmith  # noqa: E402

configure_eval_langsmith(tracing_on=True)

from evaluation.conversational_runner import (  # noqa: E402
    call_ai_api,
    check_api_health,
    compute_dimensions,
    compute_efficiency_metrics,
    compute_gates,
    compute_memory_metrics,
    compute_tool_scores,
    expected_from_turn,
    extract_tools,
    sanitize_sql_events,
    summarize_tool_results,
    truthy,
)
from evaluation.eval_target import target_fields  # noqa: E402
from evaluation.analytics.compute import flatten_trace_log_to_events, write_analytics_artifacts  # noqa: E402
from evaluation.analytics.registry import register_run  # noqa: E402
from evaluation.excel_export import flatten_trace_judge, load_jsonl_rows  # noqa: E402
from evaluation.local_store import DEFAULT_OUTPUT_DIR, LocalEvaluationStore, append_jsonl, utc_now_iso, write_json  # noqa: E402
from evaluation.run_evaluation import _build_eval_version_metadata, _build_llm_judge, _score_question  # noqa: E402
from evaluation.simulated_user import generate_follow_up  # noqa: E402
from evaluation.validators.mvp import run_mvp_validation  # noqa: E402

EVAL_DIR = Path(__file__).resolve().parent


def langsmith_fields(request_id: str | None) -> dict[str, Any]:
    project = os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or ''
    rid = str(request_id or '')
    return {
        'request_id': rid,
        'langsmith_project': project,
        'langsmith_enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False) or bool(rid),
        'langsmith_trace_hint': f'{project} | request_id={rid}' if rid and project else rid,
    }


def load_catalog(path_value: str) -> dict[str, Any]:
    path = Path(path_value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        raise FileNotFoundError(f'Sessions catalog not found: {path}')
    data = json.loads(path.read_text(encoding='utf-8'))
    if isinstance(data, list):
        return {'sessions': data}
    return data


def load_completed_session_ids(path: Path) -> set[str]:
    return {str(r.get('session_id', '')).strip() for r in load_jsonl_rows(path) if r.get('session_id')}


def session_turn_plan(session: dict[str, Any]) -> list[dict[str, Any]]:
    session_type = str(session.get('session_type') or 'single')
    turns: list[dict[str, Any]] = [{
        'turn_index': 1,
        'user_message': str(session.get('seed_message') or ''),
        'follow_up_source': 'seed',
        'expectations': session.get('expectations') if isinstance(session.get('expectations'), dict) else {},
        'policies': {},
    }]
    if session_type == 'static_multi':
        for idx, turn in enumerate(session.get('static_turns') or [], start=2):
            if not isinstance(turn, dict):
                continue
            turns.append({
                'turn_index': idx,
                'user_message': str(turn.get('user_message') or ''),
                'follow_up_source': 'scripted',
                'expectations': turn.get('expectations') if isinstance(turn.get('expectations'), dict) else {},
                'policies': turn.get('policies') if isinstance(turn.get('policies'), dict) else {},
            })
    elif session_type == 'dynamic_multi':
        max_turns = int(session.get('max_turns') or 3)
        for idx in range(2, max_turns + 1):
            turns.append({
                'turn_index': idx,
                'user_message': '',
                'follow_up_source': 'simulated',
                'expectations': session.get('expectations') if isinstance(session.get('expectations'), dict) else {},
                'policies': {},
            })
    max_turns = int(session.get('max_turns') or len(turns))
    return turns[:max_turns]


def build_turn_record(
    *,
    session: dict[str, Any],
    turn: dict[str, Any],
    result: dict[str, Any],
    scored: dict[str, Any] | None,
    eval_context: dict[str, Any],
    store_full_answer: bool,
    elapsed_ms: int,
    conversation_id: str,
    simulator_meta: dict[str, Any] | None,
    run_id: str,
    eval_target: dict[str, Any],
    gate_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    answer = str(result.get('text') or '')
    token_usage = result.get('token_usage') if isinstance(result.get('token_usage'), dict) else {}
    prompt_tokens = int(token_usage.get('prompt') or 0)
    completion_tokens = int(token_usage.get('completion') or 0)
    row: dict[str, Any] = {
        'run_id': run_id,
        'timestamp': utc_now_iso(),
        **eval_target,
        'session_id': session.get('session_id'),
        'session_type': session.get('session_type'),
        'category': session.get('category'),
        'difficulty_tier': session.get('difficulty_tier'),
        'dynamic_policy': session.get('dynamic_policy'),
        'turn_index': turn.get('turn_index'),
        'follow_up_source': turn.get('follow_up_source'),
        'user_message': turn.get('user_message'),
        'answer_text': answer if store_full_answer else '',
        'answer_preview': answer[:500],
        'conversation_id': conversation_id,
        'customer_name': eval_context.get('customer_name', ''),
        'mode': eval_context.get('mode', ''),
        'latency_sec': round(elapsed_ms / 1000.0, 2),
        'elapsed_ms': elapsed_ms,
        'tokens_prompt': prompt_tokens,
        'tokens_completion': completion_tokens,
        'tokens_total': prompt_tokens + completion_tokens,
        'actual_intent': result.get('intent', ''),
        'actual_tools': extract_tools(result),
        'failure_reasons': result.get('failure_reasons', []),
        'status': 'error' if result.get('failure_reasons') else 'ok',
        **flatten_trace_judge(result.get('evaluation')),
        **langsmith_fields(result.get('request_id')),
    }
    if scored:
        row.update({
            'expected_intent': scored.get('expected_intent', ''),
            'intent_match': scored.get('intent_match'),
            'tool_recall': scored.get('tool_recall'),
            'tool_precision': scored.get('tool_precision'),
            'tool_f1': scored.get('tool_f1'),
            'keyword_hits': scored.get('keyword_hits'),
            'sql_query_count': scored.get('sql_query_count'),
            'sql_success_rate': scored.get('sql_success_rate'),
            'batch_judge_correctness': scored.get('correctness'),
            'batch_judge_relevance': scored.get('relevance'),
            'batch_judge_completeness': scored.get('completeness'),
            'batch_judge_hallucination_risk': scored.get('hallucination_risk'),
            'batch_judge_mode': scored.get('judge_mode'),
            'batch_judge_rationale': scored.get('judge_rationale'),
        })
    if simulator_meta:
        row['simulator_mode'] = simulator_meta.get('simulator_mode')
        row['simulator_follow_up_intent'] = simulator_meta.get('follow_up_intent')
    if gate_info:
        row.update(gate_info)
    return row


def _version_fields(result: dict[str, Any]) -> dict[str, Any]:
    version = result.get('version') if isinstance(result.get('version'), dict) else {}
    return {
        'model_version': version.get('model_name', ''),
        'graph_version': version.get('release_version') or version.get('service_version', ''),
        'prompt_version': version.get('prompt_version', ''),
        'git_commit': version.get('git_commit', ''),
    }


def _append_validation_artifacts(
    store: LocalEvaluationStore,
    validation: Any,
) -> None:
    findings_path = store.run_dir / 'validation' / 'findings.jsonl'
    findings_path.parent.mkdir(parents=True, exist_ok=True)
    for finding in validation.findings:
        append_jsonl(findings_path, finding.to_dict())
    scores_path = store.run_dir / 'validation' / 'scores.jsonl'
    append_jsonl(scores_path, validation.turn_score_record())


def rollup_session(
    session: dict[str, Any],
    turn_rows: list[dict[str, Any]],
    run_id: str,
    eval_target: dict[str, Any],
    *,
    model_name: str = '',
) -> dict[str, Any]:
    def avg(field: str) -> float:
        vals = [float(r[field]) for r in turn_rows if r.get(field) is not None and str(r.get(field)) != '']
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    from evaluation.analytics.pricing import estimate_cost_usd

    prompt_t = sum(int(r.get('tokens_prompt') or 0) for r in turn_rows)
    comp_t = sum(int(r.get('tokens_completion') or 0) for r in turn_rows)
    session_cost = estimate_cost_usd(prompt_tokens=prompt_t, completion_tokens=comp_t, model_name=model_name)

    gate_passed = all(r.get('gate_passed', True) for r in turn_rows) if turn_rows else False
    val_fail = any(str(r.get('validation_status', '')).startswith('FAIL') for r in turn_rows)
    runtime_fail = any(r.get('failure_reasons') for r in turn_rows)
    hall_count = sum(int(r.get('hallucination_finding_count') or 0) for r in turn_rows)
    safety_count = sum(int(r.get('safety_finding_count') or 0) for r in turn_rows)

    if runtime_fail or val_fail or not gate_passed or safety_count > 0:
        session_pass_fail = 'fail'
    else:
        session_pass_fail = 'pass'

    scores = []
    for r in turn_rows:
        if r.get('trace_judge_final_score') is not None:
            scores.append(float(r['trace_judge_final_score']))
        elif r.get('batch_judge_correctness') is not None:
            scores.append(float(r['batch_judge_correctness']))
    session_score = round(sum(scores) / len(scores), 4) if scores else (1.0 if session_pass_fail == 'pass' else 0.0)
    if hall_count > 0:
        session_score = max(0.0, session_score - 0.05 * hall_count)

    repeated_sql = sum(int((r.get('efficiency') or {}).get('repeated_sql_queries', 0) or 0) for r in turn_rows if isinstance(r.get('efficiency'), dict))
    repeated_tools = sum(int((r.get('efficiency') or {}).get('repeated_tool_calls', 0) or 0) for r in turn_rows if isinstance(r.get('efficiency'), dict))

    version_meta = {}
    if turn_rows:
        version_meta = {
            k: turn_rows[-1].get(k, '')
            for k in ('model_version', 'graph_version', 'prompt_version', 'git_commit')
        }

    return {
        'run_id': run_id,
        **eval_target,
        'session_id': session.get('session_id'),
        'session_type': session.get('session_type'),
        'category': session.get('category'),
        'difficulty_tier': session.get('difficulty_tier'),
        'dynamic_policy': session.get('dynamic_policy'),
        'turns_count': len(turn_rows),
        'total_tokens': sum(int(r.get('tokens_total') or 0) for r in turn_rows),
        'total_session_cost_usd': session_cost,
        'total_session_latency_sec': round(sum(float(r.get('latency_sec') or 0) for r in turn_rows), 2),
        'avg_trace_judge_final_score': avg('trace_judge_final_score'),
        'avg_batch_judge_correctness': avg('batch_judge_correctness'),
        'avg_latency_sec': avg('latency_sec'),
        'gate_passed': gate_passed,
        'session_pass_fail': session_pass_fail,
        'session_score': session_score,
        'failure_count': sum(1 for r in turn_rows if r.get('failure_reasons')),
        'hallucination_count': hall_count,
        'safety_violation_count': safety_count,
        'repeated_sql_count': repeated_sql,
        'repeated_tool_count': repeated_tools,
        'follow_up_sources': sorted({str(r.get('follow_up_source') or '') for r in turn_rows}),
        **version_meta,
    }


def run_session(
    *,
    session: dict[str, Any],
    api_base_url: str,
    store: LocalEvaluationStore,
    judge_llm: Any,
    store_full_answer: bool,
    eval_customer_name: str,
    eval_target: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    session_id = str(session.get('session_id') or 'unknown')
    context = dict(session.get('context') or {})
    if not context.get('customer_name'):
        context['customer_name'] = eval_customer_name
    context['force_detailed_response'] = True
    context['evaluation_run_id'] = store.run_id
    context['session_id'] = session_id

    messages: list[dict[str, str]] = []
    conversation_id = ''
    turn_rows: list[dict[str, Any]] = []
    previous_answers: list[str] = []
    previous_tool_calls: dict[str, int] = {}
    previous_sql_hashes: dict[str, int] = {}
    previous_prompt_tokens = 0
    previous_total_tokens = 0

    for turn in session_turn_plan(session):
        user_message = str(turn.get('user_message') or '')
        simulator_meta = None
        if turn.get('follow_up_source') == 'simulated':
            simulator_meta = generate_follow_up(
                session=session,
                messages=messages,
                last_answer=previous_answers[-1] if previous_answers else '',
                policy=str(session.get('dynamic_policy') or ''),
            )
            user_message = str(simulator_meta.get('user_message') or '')
            turn['user_message'] = user_message

        if not user_message:
            continue

        messages.append({'role': 'user', 'content': user_message})
        t0 = time.time()
        try:
            result = call_ai_api(
                api_base_url=api_base_url,
                messages=messages,
                context=context,
                conversation_id=conversation_id,
            )
            conversation_id = str(result.get('conversation_id') or conversation_id or '')
        except Exception as exc:
            result = {
                'text': '',
                'intent': '',
                'tools_called': [],
                'token_usage': {'prompt': 0, 'completion': 0},
                'failure_reasons': [f'exception:{exc}'],
                'evaluation': {},
            }
        elapsed_ms = int((time.time() - t0) * 1000)
        answer = str(result.get('text') or '')
        messages.append({'role': 'assistant', 'content': answer})

        q_like = {
            'id': session_id,
            'question': user_message,
            'expected_intent': (turn.get('expectations') or {}).get('target_intent', session.get('expectations', {}).get('target_intent', '')),
            'expected_tools': (turn.get('expectations') or {}).get('required_tools_all') or (turn.get('expectations') or {}).get('required_tools_any') or [],
            'expected_output_contains': (turn.get('expectations') or {}).get('evidence_anchors', []),
            'reference_answer': '',
        }
        scored = _score_question(q_like, result, elapsed_ms / 1000.0, judge_llm=judge_llm)

        expected = expected_from_turn({
            'expectations': turn.get('expectations', {}) or session.get('expectations', {}),
            **turn,
        })
        actual_tools = extract_tools(result)
        tool_scores = compute_tool_scores(expected, actual_tools)
        tool_summary = summarize_tool_results(result.get('tool_results'))
        base_sql = {
            'run_id': store.run_id,
            'session_id': session_id,
            'turn_index': turn.get('turn_index'),
            'request_id': str(result.get('request_id') or ''),
        }
        sanitized_sql, raw_sql = sanitize_sql_events(result.get('sql_events'), base_sql)
        for sql_row in sanitized_sql:
            append_jsonl(store.path('sql_events.jsonl'), sql_row)

        memory = compute_memory_metrics(
            scenario={'context': context, 'category': session.get('category')},
            expected=expected,
            answer=answer,
            raw_sql_events=raw_sql,
            previous_answers=previous_answers,
        )
        efficiency = compute_efficiency_metrics(
            actual_tools=actual_tools,
            sanitized_sql_events=sanitized_sql,
            previous_tool_calls=previous_tool_calls,  # type: ignore[arg-type]
            previous_sql_hashes=previous_sql_hashes,  # type: ignore[arg-type]
            previous_prompt_tokens=previous_prompt_tokens,
            current_prompt_tokens=int((result.get('token_usage') or {}).get('prompt') or 0),
            previous_total_tokens=previous_total_tokens,
            current_total_tokens=int((result.get('token_usage') or {}).get('prompt') or 0) + int((result.get('token_usage') or {}).get('completion') or 0),
            answer=answer,
            previous_answers=previous_answers,
        )
        gates, violated = compute_gates(
            expected=expected,
            scenario={'context': context, 'category': session.get('category')},
            answer=answer,
            tool_summary=tool_summary,
            sanitized_sql_events=sanitized_sql,
            raw_sql_events=raw_sql,
            memory=memory,
            efficiency=efficiency,
        )
        dimensions = compute_dimensions(
            expected=expected,
            actual_intent=str(result.get('intent') or ''),
            answer=answer,
            tool_scores=tool_scores,
            tool_summary=tool_summary,
            sanitized_sql_events=sanitized_sql,
            gates=gates,
            memory=memory,
            efficiency=efficiency,
        )
        gate_info = {
            'gate_passed': all(gates.values()),
            'violated_gates': violated,
            'gates': gates,
            'dimensions': dimensions,
            'memory': memory,
            'efficiency': efficiency,
        }

        for trace_ev in flatten_trace_log_to_events(
            result.get('trace_log'),
            run_id=store.run_id,
            session_id=session_id,
            turn_index=int(turn.get('turn_index') or 0),
            request_id=str(result.get('request_id') or ''),
        ):
            append_jsonl(store.path('trace_events.jsonl'), trace_ev)

        row = build_turn_record(
            session=session,
            turn=turn,
            result=result,
            scored=scored,
            eval_context=context,
            store_full_answer=store_full_answer,
            elapsed_ms=elapsed_ms,
            conversation_id=conversation_id,
            simulator_meta=simulator_meta,
            run_id=store.run_id,
            eval_target=eval_target,
            gate_info=gate_info,
        )
        row.update(_version_fields(result))
        row['sql_event_count'] = len(sanitized_sql)

        scenario_for_val = {
            'scenario_id': session_id,
            'session_id': session_id,
            'context': context,
            'category': session.get('category'),
        }
        turn_for_val = {
            'turn_index': turn.get('turn_index'),
            'expectations': turn.get('expectations', {}) or session.get('expectations', {}),
        }
        validation = run_mvp_validation(
            scenario=scenario_for_val,
            turn=turn_for_val,
            result=result,
            turn_record=row,
        )
        vrec = validation.turn_score_record()
        row['validation'] = vrec
        row['validation_status'] = vrec.get('status')
        row['validation_finding_count'] = vrec.get('finding_count', 0)
        row['validation_critical_finding_count'] = vrec.get('critical_finding_count', 0)
        row['claims_detected'] = len(validation.claims)
        row['unsupported_claims'] = sum(
            1 for f in validation.findings
            if str(f.failure_type).startswith(('evidence.', 'hallucination.'))
        )
        row['hallucination_finding_count'] = sum(
            1 for f in validation.findings if str(f.failure_type).startswith('hallucination.')
        )
        row['safety_finding_count'] = sum(
            1 for f in validation.findings if str(f.failure_type).startswith('safety.')
        )
        _append_validation_artifacts(store, validation)

        append_jsonl(store.path('session_turns.jsonl'), row)
        turn_rows.append(row)
        previous_answers.append(answer)
        for tool_name in actual_tools:
            previous_tool_calls[tool_name] = previous_tool_calls.get(tool_name, 0) + 1
        for sql_ev in sanitized_sql:
            h = str(sql_ev.get('sql_hash') or '')
            if h:
                previous_sql_hashes[h] = previous_sql_hashes.get(h, 0) + 1
        previous_prompt_tokens = row.get('tokens_prompt') or 0
        previous_total_tokens = row.get('tokens_total') or 0

    version_meta = _build_eval_version_metadata()
    model_name = str(version_meta.get('model_name') or '')
    rollup = rollup_session(
        session, turn_rows, store.run_id, eval_target, model_name=model_name,
    )
    append_jsonl(store.path('session_rollups.jsonl'), rollup)
    return turn_rows, rollup


def run_sessions(args: argparse.Namespace) -> dict[str, Any]:
    if args.use_batch_judge:
        os.environ['EVAL_USE_LLM_JUDGE'] = '1'
    else:
        os.environ['EVAL_USE_LLM_JUDGE'] = '0'

    api_base_url = str(args.api_base_url or '').strip()
    eval_target = target_fields(api_base_url)
    check_api_health(api_base_url)

    catalog = load_catalog(args.sessions_file)
    sessions = catalog.get('sessions') if isinstance(catalog.get('sessions'), list) else []
    if not sessions:
        raise ValueError('No sessions in catalog')

    run_id = args.run_id or LocalEvaluationStore._new_run_id()
    store = LocalEvaluationStore(args.output_dir, run_id=run_id)
    write_json(store.path('eval_target.json'), eval_target)
    checkpoint = store.path('session_rollups.jsonl')
    completed = load_completed_session_ids(checkpoint) if args.resume else set()
    pending = [s for s in sessions if str(s.get('session_id') or '') not in completed]
    if args.limit_sessions > 0:
        pending = pending[: args.limit_sessions]

    judge_llm = _build_llm_judge() if args.use_batch_judge else None
    eval_customer_name = os.getenv('EVAL_CUSTOMER_NAME', 'VRL LOGISTICS LIMITED').strip() or 'VRL LOGISTICS LIMITED'
    store_full_answer = args.store_full_answer or truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False)

    print(f'\nSession evaluation run: {store.run_id}')
    print(f'  Environment: {eval_target["eval_environment"]} ({eval_target["backend_host"]})')
    print(f'  Pending (this run): {len(pending)} (catalog size {len(sessions)}, already done {len(completed)})')
    print(f'  API: {api_base_url}')

    for i, session in enumerate(pending, 1):
        sid = session.get('session_id')
        print(f"  [{i:>4}/{len(pending)}] {sid} ({session.get('session_type')})")
        try:
            run_session(
                session=session,
                api_base_url=api_base_url,
                store=store,
                judge_llm=judge_llm,
                store_full_answer=store_full_answer,
                eval_customer_name=eval_customer_name,
                eval_target=eval_target,
            )
        except Exception as exc:
            append_jsonl(checkpoint, {
                'run_id': store.run_id,
                **eval_target,
                'session_id': sid,
                'session_type': session.get('session_type'),
                'turns_count': 0,
                'failure_count': 1,
                'error': str(exc),
            })

    turn_rows = load_jsonl_rows(store.path('session_turns.jsonl'))
    rollups_raw = load_jsonl_rows(checkpoint)
    rollups = list({str(r.get('session_id', '')): r for r in rollups_raw if r.get('session_id')}.values())
    version_meta = _build_eval_version_metadata()
    summary = {
        'run_id': store.run_id,
        'sessions_evaluated': len(rollups),
        'turns_evaluated': len(turn_rows),
        **eval_target,
        'catalog_id': catalog.get('catalog_id'),
        'session_counts': catalog.get('session_counts'),
        'version': version_meta,
        'finished_at': utc_now_iso(),
    }
    write_json(store.path('session_summary.json'), summary)

    from evaluation.analytics.loader import load_run

    bundle = load_run(store.run_id, artifacts_dir=store.output_dir, use_parquet_cache=False)
    bundle.collection_summary = summary
    write_analytics_artifacts(bundle)
    analytics_path = store.path('analytics_summary.json')
    analytics_summary = json.loads(analytics_path.read_text(encoding='utf-8')) if analytics_path.exists() else {}
    register_run(
        run_id=store.run_id,
        run_dir=store.run_dir,
        collection_summary=summary,
        analytics_summary=analytics_summary,
        artifacts_dir=store.output_dir,
    )

    return {
        'run_id': store.run_id,
        'run_dir': str(store.run_dir),
        'summary': summary,
        'turn_rows': turn_rows,
        'session_rollups': rollups,
        'analytics_summary': analytics_summary,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    default_catalog = EVAL_DIR / 'conversational_scenarios' / 'sessions_1000.json'
    parser = argparse.ArgumentParser(description='Run unified session evaluation catalog.')
    parser.add_argument('--api-base-url', default=os.getenv('EVAL_API_BASE_URL', 'http://127.0.0.1:8001'))
    parser.add_argument('--output-dir', default=os.getenv('EVAL_OUTPUT_DIR', str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument('--run-id', default=os.getenv('EVAL_RUN_ID', ''))
    parser.add_argument('--sessions-file', default=os.getenv('EVAL_SESSIONS_FILE', str(default_catalog)))
    parser.add_argument('--limit-sessions', type=int, default=int(os.getenv('EVAL_LIMIT_SESSIONS', '0') or '0'))
    parser.add_argument('--resume', action='store_true', default=truthy(os.getenv('EVAL_RESUME'), False))
    parser.add_argument('--store-full-answer', action='store_true', default=truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False))
    parser.add_argument('--use-batch-judge', action='store_true', default=truthy(os.getenv('EVAL_USE_LLM_JUDGE', '1'), True))
    parser.add_argument('--no-batch-judge', dest='use_batch_judge', action='store_false')
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    try:
        result = run_sessions(args)
    except Exception as exc:
        print(f'Session evaluation failed: {exc}')
        return 1
    print(f"\nCompleted session evaluation: {result['summary']['sessions_evaluated']} sessions, {result['summary']['turns_evaluated']} turns")
    print(f"Artifacts: {result['run_dir']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
