"""Orchestrate Phase 2: 1000-session evaluation with dynamic follow-ups."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

os.environ.setdefault('AI_ANALYST_PERSIST_OBSERVABILITY', '0')
os.environ.setdefault('EVAL_PERSIST_CLICKHOUSE', '0')

PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVAL_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / '.env')
except ImportError:
    pass

from evaluation.langsmith_eval_env import configure_eval_langsmith  # noqa: E402

configure_eval_langsmith(tracing_on=True)

from evaluation.conversational_runner import check_api_health, truthy  # noqa: E402
from evaluation.eval_target import target_fields  # noqa: E402
from evaluation.excel_export import load_jsonl_rows, write_session_evaluation_workbook  # noqa: E402
from evaluation.generate_session_catalog import generate_catalog, git_commit  # noqa: E402
from evaluation.local_store import DEFAULT_OUTPUT_DIR, LocalEvaluationStore, utc_now_iso, write_json  # noqa: E402
from evaluation.session_runner import load_catalog, parse_args as session_parse_args, run_sessions  # noqa: E402

DEFAULT_SESSIONS_FILE = EVAL_DIR / 'conversational_scenarios' / 'sessions_1000.json'


def ensure_catalog(args: argparse.Namespace) -> Path:
    sessions_path = Path(args.sessions_file)
    if not sessions_path.is_absolute():
        sessions_path = PROJECT_ROOT / sessions_path
    if sessions_path.exists() and not args.generate_catalog:
        return sessions_path

    print('Generating session catalog...')
    catalog = generate_catalog(
        single_count=args.single_count,
        static_multi_count=args.static_multi_count,
        dynamic_multi_count=args.dynamic_multi_count,
        customer_name=args.customer_name,
        ground_in_clickhouse=args.ground_in_clickhouse,
        seed=args.catalog_seed,
        vrl_fraction=float(os.getenv('EVAL_VRL_FRACTION', '0.7') or '0.7'),
    )
    sessions_path.parent.mkdir(parents=True, exist_ok=True)
    import json
    sessions_path.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding='utf-8')
    manifest_path = sessions_path.with_name('catalog_manifest.json')
    manifest = {
        'catalog_id': catalog['catalog_id'],
        'catalog_version': catalog['catalog_version'],
        'output': str(sessions_path),
        'session_counts': catalog['session_counts'],
        'git_commit': git_commit(),
        'generator_model': os.getenv('EVAL_GENERATOR_MODEL', 'template-only'),
        'seed': args.catalog_seed,
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print(f"  Catalog: {sessions_path} ({catalog['session_counts']['total']} sessions)")
    return sessions_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Phase 2: 1000-session evaluation collection.')
    parser.add_argument('--api-base-url', default=os.getenv('EVAL_API_BASE_URL', 'http://127.0.0.1:8005'))
    parser.add_argument('--output-dir', default=os.getenv('EVAL_OUTPUT_DIR', str(DEFAULT_OUTPUT_DIR)))
    parser.add_argument('--run-id', default=os.getenv('EVAL_RUN_ID', ''))
    parser.add_argument('--sessions-file', default=os.getenv('EVAL_SESSIONS_FILE', str(DEFAULT_SESSIONS_FILE)))
    parser.add_argument('--limit-sessions', type=int, default=int(os.getenv('EVAL_LIMIT_SESSIONS', '0') or '0'))
    parser.add_argument('--resume', action='store_true', default=truthy(os.getenv('EVAL_RESUME'), False))
    parser.add_argument('--write-excel', action='store_true', default=truthy(os.getenv('EVAL_WRITE_EXCEL', '1'), True))
    parser.add_argument('--no-write-excel', dest='write_excel', action='store_false')
    parser.add_argument('--store-full-answer', action='store_true', default=truthy(os.getenv('EVAL_STORE_FULL_ANSWER', '1'), True))
    parser.add_argument('--use-batch-judge', action='store_true', default=truthy(os.getenv('EVAL_USE_LLM_JUDGE', '1'), True))
    parser.add_argument('--no-batch-judge', dest='use_batch_judge', action='store_false')
    parser.add_argument('--generate-catalog', action='store_true', default=truthy(os.getenv('EVAL_GENERATE_CATALOG'), False))
    parser.add_argument('--single-count', type=int, default=int(os.getenv('EVAL_SINGLE_COUNT', '650') or '650'))
    parser.add_argument('--static-multi-count', type=int, default=int(os.getenv('EVAL_STATIC_MULTI_COUNT', '250') or '250'))
    parser.add_argument('--dynamic-multi-count', type=int, default=int(os.getenv('EVAL_DYNAMIC_MULTI_COUNT', '100') or '100'))
    parser.add_argument('--customer-name', default=os.getenv('EVAL_CUSTOMER_NAME', 'VRL LOGISTICS LIMITED'))
    parser.add_argument('--ground-in-clickhouse', action='store_true', default=True)
    parser.add_argument('--no-ground-in-clickhouse', dest='ground_in_clickhouse', action='store_false')
    parser.add_argument('--catalog-seed', type=int, default=int(os.getenv('EVAL_CATALOG_SEED', '42') or '42'))
    parser.add_argument('--baseline-dir', default=os.getenv('EVAL_BASELINE_DIR', ''))
    parser.add_argument('--skip-run', action='store_true', help='Generate catalog and/or Excel only.')
    return parser.parse_args()


def run_1000_collection(args: argparse.Namespace) -> dict:
    started_at = utc_now_iso()
    sessions_path = ensure_catalog(args)
    args.sessions_file = str(sessions_path)

    if args.skip_run:
        catalog = load_catalog(str(sessions_path))
        run_id = args.run_id or LocalEvaluationStore._new_run_id()
        store = LocalEvaluationStore(args.output_dir, run_id=run_id)
        return {
            'run_id': run_id,
            'run_dir': str(store.run_dir),
            'catalog_only': True,
            'session_counts': catalog.get('session_counts'),
        }

    check_api_health(args.api_base_url)

    session_argv = [
        '--api-base-url', args.api_base_url,
        '--output-dir', args.output_dir,
        '--sessions-file', str(sessions_path),
        '--limit-sessions', str(args.limit_sessions or 0),
        *(['--run-id', args.run_id] if args.run_id else []),
        *(['--resume'] if args.resume else []),
        *(['--store-full-answer'] if args.store_full_answer else []),
        *(['--use-batch-judge'] if args.use_batch_judge else ['--no-batch-judge']),
    ]
    result = run_sessions(session_parse_args(session_argv))
    run_dir = Path(result['run_dir'])
    catalog = load_catalog(str(sessions_path))

    eval_target = target_fields(args.api_base_url)
    collection_summary = {
        'run_id': result['run_id'],
        'started_at': started_at,
        'finished_at': utc_now_iso(),
        'phase': '1000_session_benchmark',
        **eval_target,
        'sessions_file': str(sessions_path),
        'catalog_id': catalog.get('catalog_id'),
        'session_counts': catalog.get('session_counts'),
        'sessions_evaluated': result['summary'].get('sessions_evaluated', 0),
        'turns_evaluated': result['summary'].get('turns_evaluated', 0),
        'limit_sessions': args.limit_sessions or None,
        'batch_judge_enabled': args.use_batch_judge,
        'langsmith_project': os.getenv('LANGSMITH_PROJECT') or os.getenv('LANGCHAIN_PROJECT') or '',
        'langsmith_enabled': truthy(os.getenv('LANGSMITH_TRACING') or os.getenv('LANGCHAIN_TRACING_V2'), False),
        'baseline_dir': args.baseline_dir or None,
        'version': result['summary'].get('version'),
    }
    write_json(run_dir / 'collection_summary.json', collection_summary)

    from evaluation.analytics.registry import register_run

    analytics_summary = result.get('analytics_summary') or {}
    if not analytics_summary:
        from evaluation.analytics.compute import build_analytics_summary, write_analytics_artifacts
        from evaluation.analytics.loader import load_run

        bundle = load_run(result['run_id'], artifacts_dir=Path(args.output_dir), use_parquet_cache=False)
        bundle.collection_summary = collection_summary
        write_analytics_artifacts(bundle)
        import json as _json
        ap = run_dir / 'analytics_summary.json'
        analytics_summary = _json.loads(ap.read_text(encoding='utf-8')) if ap.exists() else build_analytics_summary(bundle)
    register_run(
        run_id=result['run_id'],
        run_dir=run_dir,
        collection_summary=collection_summary,
        analytics_summary=analytics_summary,
        artifacts_dir=Path(args.output_dir),
    )

    excel_path = ''
    if args.write_excel:
        turn_rows = load_jsonl_rows(run_dir / 'session_turns.jsonl')
        rollups = load_jsonl_rows(run_dir / 'session_rollups.jsonl')
        xlsx_path = run_dir / 'full_evaluation_report.xlsx'
        wrote = write_session_evaluation_workbook(
            output_path=xlsx_path,
            summary=collection_summary,
            session_rollups=rollups,
            turn_rows=turn_rows,
            analytics_summary=analytics_summary,
        )
        if wrote:
            excel_path = str(xlsx_path)
            print(f'\nPhase 2 Excel: {xlsx_path}')
        else:
            print('\nExcel export skipped: install openpyxl (`pip install openpyxl`)')

    return {
        'run_id': result['run_id'],
        'run_dir': str(run_dir),
        'collection_summary': collection_summary,
        'excel_path': excel_path,
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

    print('Phase 2: 1000-session evaluation collection')
    target = target_fields(args.api_base_url)
    print(f'  Environment: {target["eval_environment"]} (production VM backend)' if target['is_production'] else f'  Environment: {target["eval_environment"]} (local dev backend)')
    print(f'  API: {args.api_base_url}')
    print(f'  Sessions file: {args.sessions_file}')
    print(f'  Limit: {args.limit_sessions or "all"}')
    print(f'  Resume: {args.resume}')
    print(f'  Batch judge: {"on" if args.use_batch_judge else "off"}')

    try:
        result = run_1000_collection(args)
    except Exception as exc:
        print(f'1000-session collection failed: {exc}')
        return 1

    if result.get('catalog_only'):
        print(f"Catalog ready: {result.get('session_counts')}")
        return 0

    summary = result['collection_summary']
    print(
        f"\nCompleted Phase 2 run {summary['run_id']}: "
        f"{summary['sessions_evaluated']} sessions, "
        f"{summary['turns_evaluated']} turns"
    )
    if result.get('excel_path'):
        print(f"Excel: {result['excel_path']}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
