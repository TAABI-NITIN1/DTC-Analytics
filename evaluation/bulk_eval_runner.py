"""API-based single-turn bulk evaluation with checkpoint, judges, and Excel export."""
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

from evaluation.conversational_runner import call_ai_api, check_api_health, truthy  # noqa: E402
from evaluation.excel_export import flatten_trace_judge, load_jsonl_rows, write_bulk_evaluation_workbook  # noqa: E402
from evaluation.local_store import DEFAULT_OUTPUT_DIR, LocalEvaluationStore, append_jsonl, utc_now_iso, write_json  # noqa: E402
from evaluation.run_evaluation import (  # noqa: E402
    _build_eval_version_metadata,
    _build_llm_judge,
    _load_datasets,
    _score_question,
)


def langsmith_fields(request_id: str | None) -> dict[str, Any]:
    project = os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or ''
    rid = str(request_id or '')
    return {
        'request_id': rid,
        'langsmith_project': project,
        'langsmith_enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False),
        'langsmith_trace_hint': f'{project} | request_id={rid}' if rid and project else rid,
    }


def load_questions(questions_file: str | None, limit: int) -> list[dict[str, Any]]:
    if questions_file:
        path = Path(questions_file)
        if not path.is_absolute():
            path = PROJECT_ROOT / path
        if not path.exists():
            raise FileNotFoundError(f'Questions file not found: {path}')
        with path.open('r', encoding='utf-8') as handle:
            loaded = json.load(handle)
        if not isinstance(loaded, list):
            raise ValueError(f'Questions file must contain a JSON array: {path}')
        questions = loaded
        print(f'  Loaded {len(questions)} questions from {path.name}')
    else:
        questions = _load_datasets()
    if limit > 0:
        questions = questions[:limit]
    return questions


def load_completed_ids(checkpoint_path: Path) -> set[str]:
    return {str(r.get('id', '')).strip() for r in load_jsonl_rows(checkpoint_path) if r.get('id')}


def build_result_row(
    *,
    q: dict[str, Any],
    result: dict[str, Any],
    scored: dict[str, Any],
    eval_context: dict[str, Any],
    store_full_answer: bool,
) -> dict[str, Any]:
    answer = str(result.get('text') or '')
    row = {
        'run_id': '',
        'timestamp': utc_now_iso(),
        'id': q.get('id', ''),
        'question': q.get('question', ''),
        'customer_name': eval_context.get('customer_name', ''),
        'mode': eval_context.get('mode', ''),
        'answer_text': answer if store_full_answer else '',
        'answer_preview': answer[:500],
        'expected_intent': scored.get('expected_intent', ''),
        'actual_intent': scored.get('actual_intent', ''),
        'intent_match': scored.get('intent_match'),
        'expected_tools': scored.get('expected_tools', []),
        'actual_tools': scored.get('actual_tools', []),
        'tool_recall': scored.get('tool_recall'),
        'tool_precision': scored.get('tool_precision'),
        'tool_f1': scored.get('tool_f1'),
        'keyword_hits': scored.get('keyword_hits'),
        'sql_query_count': scored.get('sql_query_count'),
        'sql_success_rate': scored.get('sql_success_rate'),
        'sql_relevance_score': scored.get('sql_relevance_score'),
        'latency_sec': scored.get('latency_sec'),
        'tokens_prompt': scored.get('tokens_prompt'),
        'tokens_completion': scored.get('tokens_completion'),
        'tokens_total': scored.get('tokens_total'),
        'batch_judge_correctness': scored.get('correctness'),
        'batch_judge_relevance': scored.get('relevance'),
        'batch_judge_completeness': scored.get('completeness'),
        'batch_judge_hallucination_risk': scored.get('hallucination_risk'),
        'batch_judge_groundedness_score': scored.get('groundedness_score'),
        'batch_judge_mode': scored.get('judge_mode'),
        'batch_judge_rationale': scored.get('judge_rationale'),
        'failure_reasons': scored.get('failure_reasons', []),
        'status': 'error' if scored.get('failure_reasons') else 'ok',
        **flatten_trace_judge(result.get('evaluation')),
        **langsmith_fields(scored.get('request_id')),
    }
    return row


def build_summary(rows: list[dict[str, Any]], version_meta: dict[str, Any], *, api_base_url: str) -> dict[str, Any]:
    n = len(rows)
    if not n:
        return {
            'questions_evaluated': 0,
            'api_base_url': api_base_url,
            'version': version_meta,
        }

    def avg(field: str) -> float:
        vals = [float(r[field]) for r in rows if r.get(field) is not None and str(r.get(field)) != '']
        return round(sum(vals) / len(vals), 4) if vals else 0.0

    return {
        'questions_evaluated': n,
        'api_base_url': api_base_url,
        'langsmith_project': os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or '',
        'batch_judge_enabled': truthy(os.getenv('EVAL_USE_LLM_JUDGE'), False),
        'intent_accuracy': round(sum(int(r.get('intent_match') or 0) for r in rows) / n, 4),
        'avg_tool_f1': avg('tool_f1'),
        'avg_keyword_hits': avg('keyword_hits'),
        'avg_batch_judge_correctness': avg('batch_judge_correctness'),
        'avg_trace_judge_final_score': avg('trace_judge_final_score'),
        'avg_latency_sec': avg('latency_sec'),
        'total_tokens': sum(int(r.get('tokens_total') or 0) for r in rows),
        'failure_count': sum(1 for r in rows if r.get('failure_reasons')),
        'version': version_meta,
    }


def run_bulk_eval(args: argparse.Namespace) -> dict[str, Any]:
    if args.use_batch_judge:
        os.environ['EVAL_USE_LLM_JUDGE'] = '1'
    else:
        os.environ['EVAL_USE_LLM_JUDGE'] = '0'

    api_base_url = str(args.api_base_url or '').strip()
    if not api_base_url:
        raise ValueError('--api-base-url is required for bulk evaluation')
    check_api_health(api_base_url)

    questions = load_questions(args.questions_file, args.limit)
    if not questions:
        raise ValueError('No questions to evaluate')

    run_id = args.run_id or LocalEvaluationStore._new_run_id()
    store = LocalEvaluationStore(args.output_dir, run_id=run_id)
    checkpoint_path = store.path('single_turn_results.jsonl')

    completed_ids: set[str] = set()
    if args.resume:
        completed_ids = load_completed_ids(checkpoint_path)
        if completed_ids:
            print(f'Resuming: skipping {len(completed_ids)} completed question(s)')

    version_meta = _build_eval_version_metadata()
    judge_llm = _build_llm_judge() if args.use_batch_judge else None
    eval_customer_name = os.getenv('EVAL_CUSTOMER_NAME', 'VRL LOGISTICS LIMITED').strip() or 'VRL LOGISTICS LIMITED'
    store_full_answer = args.store_full_answer or truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False)

    pending = [q for q in questions if str(q.get('id', '')).strip() not in completed_ids]
    n_total = len(pending)
    print(f'\nBulk single-turn evaluation: {store.run_id}')
    print(f'  Questions pending: {n_total} / {len(questions)}')
    print(f'  API: {api_base_url}')
    print(f'  Batch judge: {"on" if args.use_batch_judge else "off"}')
    print(f'  Artifacts: {store.run_dir}\n')

    new_rows: list[dict[str, Any]] = []
    for i, q in enumerate(pending, 1):
        t0 = time.time()
        eval_context = dict(q.get('context') or {})
        eval_context['customer_name'] = eval_customer_name
        eval_context['force_detailed_response'] = True
        eval_context['evaluation_run_id'] = store.run_id
        eval_context['question_id'] = q.get('id', '')

        try:
            result = call_ai_api(
                api_base_url=api_base_url,
                messages=[{'role': 'user', 'content': q['question']}],
                context=eval_context,
                conversation_id='',
            )
        except Exception as exc:
            result = {
                'text': '',
                'intent': '',
                'tools_called': [],
                'token_usage': {'prompt': 0, 'completion': 0},
                'failure_reasons': [f'exception:{exc}'],
                'evaluation': {},
            }

        latency = round(time.time() - t0, 2)
        scored = _score_question(q, result, latency, judge_llm=judge_llm)
        row = build_result_row(
            q=q,
            result=result,
            scored=scored,
            eval_context=eval_context,
            store_full_answer=store_full_answer,
        )
        row['run_id'] = store.run_id
        append_jsonl(checkpoint_path, row)
        new_rows.append(row)

        intent_icon = 'Y' if row.get('intent_match') else 'N'
        print(
            f'  [{i:>3}/{n_total}] {row["id"]:<12} {intent_icon} '
            f'tool_f1={row.get("tool_f1", 0):.2f} '
            f'trace={row.get("trace_judge_final_score", "-")} '
            f'batch={row.get("batch_judge_correctness", "-")} '
            f'{latency:>5.1f}s'
        )

    all_rows = load_jsonl_rows(checkpoint_path)
    summary = build_summary(all_rows, version_meta, api_base_url=api_base_url)
    summary['run_id'] = store.run_id
    summary['finished_at'] = utc_now_iso()
    write_json(store.path('single_turn_summary.json'), summary)

    if args.write_excel:
        xlsx_path = store.path('full_evaluation_report.xlsx')
        wrote = write_bulk_evaluation_workbook(
            output_path=xlsx_path,
            summary=summary,
            single_turn_rows=all_rows,
            multi_turn_rows=load_jsonl_rows(store.path('turns.jsonl')),
        )
        if wrote:
            print(f'\nExcel written: {xlsx_path}')
        else:
            print('\nExcel export skipped: install openpyxl')

    return {
        'run_id': store.run_id,
        'run_dir': str(store.run_dir),
        'questions_evaluated': len(all_rows),
        'new_questions': len(new_rows),
        'summary': summary,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Bulk single-turn evaluation via backend API.')
    parser.add_argument('--api-base-url', default=os.getenv('EVAL_API_BASE_URL', 'http://127.0.0.1:8005'))
    parser.add_argument('--output-dir', default=os.getenv('EVAL_OUTPUT_DIR', str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument('--run-id', default=os.getenv('EVAL_RUN_ID', ''))
    parser.add_argument('--limit', type=int, default=int(os.getenv('EVAL_LIMIT', '0') or '0'))
    parser.add_argument('--questions-file', default=os.getenv('EVAL_QUESTIONS_FILE', ''))
    parser.add_argument('--write-excel', action='store_true', default=truthy(os.getenv('EVAL_WRITE_EXCEL'), False))
    parser.add_argument('--resume', action='store_true', default=truthy(os.getenv('EVAL_RESUME'), False))
    parser.add_argument('--store-full-answer', action='store_true', default=truthy(os.getenv('EVAL_STORE_FULL_ANSWER'), False))
    parser.add_argument('--use-batch-judge', action='store_true', default=truthy(os.getenv('EVAL_USE_LLM_JUDGE', '1'), True))
    parser.add_argument('--no-batch-judge', dest='use_batch_judge', action='store_false')
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    try:
        result = run_bulk_eval(args)
    except Exception as exc:
        print(f'Bulk evaluation failed: {exc}')
        return 1
    print(
        f"\nCompleted bulk evaluation: {result['questions_evaluated']} question(s) "
        f"in {result['run_dir']}"
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
