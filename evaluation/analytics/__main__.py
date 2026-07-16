"""CLI: python -m evaluation.analytics summarize --run-id <id>"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from evaluation.analytics.compute import build_analytics_summary, write_analytics_artifacts
from evaluation.analytics.experiments import compare_runs, register_run_from_dir
from evaluation.analytics.export_dashboard import export_all_to_eval_dashboard, write_dashboard_bundle
from evaluation.analytics.loader import load_run
from evaluation.backfill_validation import backfill_validation
from evaluation.local_store import DEFAULT_OUTPUT_DIR


def cmd_summarize(args: argparse.Namespace) -> int:
    bundle = load_run(args.run_id, artifacts_dir=Path(args.artifacts_dir))
    path = write_analytics_artifacts(bundle, write_parquet=not args.no_parquet)
    summary = build_analytics_summary(bundle)
    register_run_from_dir(bundle.run_dir, collection_summary=bundle.collection_summary)
    print(json.dumps(summary.get('run_metrics', {}), indent=2))
    print(f'Wrote {path}')
    return 0


def cmd_compare(args: argparse.Namespace) -> int:
    result = compare_runs(args.baseline, args.candidate, artifacts_dir=Path(args.artifacts_dir))
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_backfill_validation(args: argparse.Namespace) -> int:
    summary = backfill_validation(
        args.run_id,
        artifacts_dir=Path(args.artifacts_dir),
        overwrite=not args.no_overwrite,
        catalog_path=Path(args.catalog) if args.catalog else None,
    )
    print(json.dumps(summary, indent=2))
    if args.summarize_after:
        bundle = load_run(args.run_id, artifacts_dir=Path(args.artifacts_dir))
        path = write_analytics_artifacts(bundle)
        print(f'Wrote analytics {path}')
    if args.export_after:
        path = write_dashboard_bundle(
            args.run_id,
            artifacts_dir=Path(args.artifacts_dir),
            baseline_run_id=args.baseline or None,
            copy_to_eval_dashboard=args.copy_to_eval_dashboard,
            eval_dashboard_dir=Path(args.eval_dashboard_dir) if args.eval_dashboard_dir else None,
        )
        print(f'Wrote dashboard {path}')
    return 0


def cmd_export_dashboard(args: argparse.Namespace) -> int:
    artifacts = Path(args.artifacts_dir)
    run_ids = list(args.run_id or [])
    if not run_ids:
        print('Provide at least one --run-id')
        return 1
    dash_root = Path(args.eval_dashboard_dir) if args.eval_dashboard_dir else None
    for rid in run_ids:
        path = write_dashboard_bundle(
            rid,
            artifacts_dir=artifacts,
            baseline_run_id=args.baseline or None,
            copy_to_eval_dashboard=args.copy_to_eval_dashboard,
            eval_dashboard_dir=dash_root,
        )
        print(f'Wrote {path}')
    if args.copy_to_eval_dashboard and len(run_ids) > 1:
        from evaluation.analytics.export_dashboard import _update_manifest

        runs_dir = (dash_root or Path.cwd() / 'eval-dashboard') / 'public' / 'runs'
        for rid in run_ids[1:]:
            bundle = load_run(rid, artifacts_dir=artifacts)
            cs = bundle.collection_summary or {}
            _update_manifest(
                runs_dir,
                rid,
                {'generated_at': cs.get('finished_at'), 'eval_environment': cs.get('eval_environment', 'local')},
            )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description='Evaluation analytics utilities')
    parser.add_argument('--artifacts-dir', default=str(DEFAULT_OUTPUT_DIR))
    sub = parser.add_subparsers(dest='command', required=True)

    p_sum = sub.add_parser('summarize', help='Build analytics_summary.json for a run')
    p_sum.add_argument('--run-id', required=True)
    p_sum.add_argument('--no-parquet', action='store_true')
    p_sum.set_defaults(func=cmd_summarize)

    p_cmp = sub.add_parser('compare', help='Compare baseline vs candidate run')
    p_cmp.add_argument('--baseline', required=True)
    p_cmp.add_argument('--candidate', required=True)
    p_cmp.set_defaults(func=cmd_compare)

    p_bf = sub.add_parser('backfill-validation', help='Generate validation/findings.jsonl from session turns')
    p_bf.add_argument('--run-id', required=True)
    p_bf.add_argument('--catalog', default='')
    p_bf.add_argument('--no-overwrite', action='store_true')
    p_bf.add_argument('--summarize-after', action='store_true')
    p_bf.add_argument('--export-after', action='store_true')
    p_bf.add_argument('--baseline', default='')
    p_bf.add_argument('--copy-to-eval-dashboard', action='store_true')
    p_bf.add_argument('--eval-dashboard-dir', default='')
    p_bf.set_defaults(func=cmd_backfill_validation)

    p_exp = sub.add_parser('export-dashboard', help='Build dashboard_bundle.json for Vite dashboard')
    p_exp.add_argument('--run-id', action='append', required=True, help='Run id (repeatable)')
    p_exp.add_argument('--baseline', default='', help='Baseline run for experiment_compare in bundle')
    p_exp.add_argument('--copy-to-eval-dashboard', action='store_true')
    p_exp.add_argument('--eval-dashboard-dir', default='')
    p_exp.set_defaults(func=cmd_export_dashboard)

    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
