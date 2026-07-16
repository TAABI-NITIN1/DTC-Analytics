"""Orchestrate single-turn bulk + multi-turn conversational data collection."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault('AI_ANALYST_PERSIST_OBSERVABILITY', '0')
os.environ.setdefault('EVAL_PERSIST_CLICKHOUSE', '0')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from evaluation.bulk_eval_runner import parse_args as bulk_parse_args, run_bulk_eval  # noqa: E402
from evaluation.conversational_runner import DEFAULT_SCENARIO_FILE, parse_args as conv_parse_args, run_conversational_eval, truthy  # noqa: E402
from evaluation.excel_export import load_jsonl_rows, write_bulk_evaluation_workbook  # noqa: E402
from evaluation.local_store import DEFAULT_OUTPUT_DIR, LocalEvaluationStore, utc_now_iso, write_json  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Unified evaluation data collection (single-turn + multi-turn).')
    parser.add_argument('--api-base-url', default=os.getenv('EVAL_API_BASE_URL', 'http://127.0.0.1:8005'))
    parser.add_argument('--output-dir', default=os.getenv('EVAL_OUTPUT_DIR', str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument('--run-id', default=os.getenv('EVAL_RUN_ID', ''))
    parser.add_argument('--single-turn-limit', type=int, default=int(os.getenv('EVAL_SINGLE_TURN_LIMIT', '0') or '0'))
    parser.add_argument('--questions-file', default=os.getenv('EVAL_QUESTIONS_FILE', ''))
    parser.add_argument('--multi-turn-scenarios', default=os.getenv('EVAL_SCENARIO_FILE', str(DEFAULT_SCENARIO_FILE)))
    parser.add_argument('--limit-scenarios', type=int, default=int(os.getenv('EVAL_LIMIT_SCENARIOS', '0') or '0'))
    parser.add_argument('--max-turns', type=int, default=int(os.getenv('EVAL_MAX_TURNS', '0') or '0'))
    parser.add_argument('--write-excel', action='store_true', default=truthy(os.getenv('EVAL_WRITE_EXCEL', '1'), True))
    parser.add_argument('--store-full-answer', action='store_true', default=truthy(os.getenv('EVAL_STORE_FULL_ANSWER', '1'), True))
    parser.add_argument('--use-batch-judge', action='store_true', default=truthy(os.getenv('EVAL_USE_LLM_JUDGE', '1'), True))
    parser.add_argument('--no-batch-judge', dest='use_batch_judge', action='store_false')
    parser.add_argument('--resume', action='store_true', default=truthy(os.getenv('EVAL_RESUME'), False))
    parser.add_argument('--skip-single-turn', action='store_true')
    parser.add_argument('--skip-multi-turn', action='store_true')
    return parser.parse_args()


def run_data_collection(args: argparse.Namespace) -> dict:
    run_id = str(args.run_id or '').strip() or LocalEvaluationStore._new_run_id()
    started_at = utc_now_iso()

    bulk_result: dict | None = None
    conv_result: dict | None = None

    if not args.skip_single_turn:
        bulk_args = bulk_parse_args([
            '--api-base-url', args.api_base_url,
            '--output-dir', args.output_dir,
            '--run-id', run_id,
            '--limit', str(args.single_turn_limit or 0),
            *(['--questions-file', args.questions_file] if args.questions_file else []),
            *(['--resume'] if args.resume else []),
            *(['--store-full-answer'] if args.store_full_answer else []),
            *(['--use-batch-judge'] if args.use_batch_judge else ['--no-batch-judge']),
        ])
        bulk_result = run_bulk_eval(bulk_args)

    if not args.skip_multi_turn:
        conv_args = conv_parse_args([
            '--api-base-url', args.api_base_url,
            '--output-dir', args.output_dir,
            '--run-id', run_id,
            '--scenario-file', args.multi_turn_scenarios,
            '--limit-scenarios', str(args.limit_scenarios or 0),
            '--max-turns', str(args.max_turns or 0),
            '--skip-unified-excel',
            *(['--store-full-answer'] if args.store_full_answer else []),
        ])
        conv_result = run_conversational_eval(conv_args)

    run_dir = Path(args.output_dir) / run_id
    single_turn_rows = load_jsonl_rows(run_dir / 'single_turn_results.jsonl')
    multi_turn_rows = conv_result.get('turn_rows', []) if conv_result else load_jsonl_rows(run_dir / 'turns.jsonl')

    collection_summary = {
        'run_id': run_id,
        'started_at': started_at,
        'finished_at': utc_now_iso(),
        'api_base_url': args.api_base_url,
        'langsmith_project': os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or '',
        'langsmith_enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False),
        'batch_judge_enabled': args.use_batch_judge,
        'single_turn_questions': len(single_turn_rows),
        'multi_turn_turns': len(multi_turn_rows),
        'single_turn_summary': (bulk_result or {}).get('summary', {}),
        'multi_turn_summary': (conv_result or {}).get('summary', {}),
    }
    write_json(run_dir / 'collection_summary.json', collection_summary)

    if args.write_excel:
        xlsx_path = run_dir / 'full_evaluation_report.xlsx'
        wrote = write_bulk_evaluation_workbook(
            output_path=xlsx_path,
            summary=collection_summary,
            single_turn_rows=single_turn_rows,
            multi_turn_rows=multi_turn_rows,
        )
        if wrote:
            print(f'\nUnified collection Excel: {xlsx_path}')
        else:
            print('\nExcel export skipped: install openpyxl (`pip install openpyxl`)')

    return {
        'run_id': run_id,
        'run_dir': str(run_dir),
        'collection_summary': collection_summary,
        'excel_path': str(run_dir / 'full_evaluation_report.xlsx') if args.write_excel else '',
    }


def main() -> int:
    args = parse_args()
    os.environ['AI_ANALYST_PERSIST_OBSERVABILITY'] = '0'
    if args.use_batch_judge:
        os.environ['EVAL_USE_LLM_JUDGE'] = '1'
    else:
        os.environ['EVAL_USE_LLM_JUDGE'] = '0'
    if args.store_full_answer:
        os.environ['EVAL_STORE_FULL_ANSWER'] = '1'

    print('Unified evaluation data collection')
    print(f'  API: {args.api_base_url}')
    print(f'  Single-turn limit: {args.single_turn_limit or "all"}')
    print(f'  Multi-turn scenarios: {args.multi_turn_scenarios}')
    print(f'  Batch judge: {"on" if args.use_batch_judge else "off"}')
    print(f'  Store full answers: {args.store_full_answer}')

    try:
        result = run_data_collection(args)
    except Exception as exc:
        print(f'Data collection failed: {exc}')
        return 1

    summary = result['collection_summary']
    print(
        f"\nCompleted data collection run {summary['run_id']}: "
        f"{summary['single_turn_questions']} single-turn, "
        f"{summary['multi_turn_turns']} multi-turn rows"
    )
    if result.get('excel_path'):
        print(f"Excel: {result['excel_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
