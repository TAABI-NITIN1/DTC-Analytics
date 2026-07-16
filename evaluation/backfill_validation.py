"""Backfill MVP validation artifacts from existing session_turns.jsonl."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

from evaluation.analytics.loader import load_run
from evaluation.excel_export import load_jsonl_rows
from evaluation.local_store import DEFAULT_OUTPUT_DIR, append_jsonl, utc_now_iso, write_json
from evaluation.session_runner import load_catalog, session_turn_plan
from evaluation.validators.base import ValidationFinding
from evaluation.validators.mvp import run_mvp_validation
from evaluation.validators.taxonomy import severity_for

PROJECT_ROOT = Path(__file__).resolve().parent.parent

_RUNTIME_PREFIX = re.compile(r'^exception:', re.I)

_REASON_TO_FAILURE: dict[str, str] = {
    'sql_error': 'sql.error',
    'insufficient_evidence': 'evidence.missing',
    'heuristic_fallback_used': 'runtime.empty_response',
}


def _failure_type_for_reason(reason: str) -> str:
    r = str(reason or '').strip()
    if r in _REASON_TO_FAILURE:
        return _REASON_TO_FAILURE[r]
    if _RUNTIME_PREFIX.match(r):
        if 'timed out' in r.lower() or 'timeout' in r.lower():
            return 'runtime.timeout'
        return 'runtime.exception'
    return 'runtime.exception'


def _runtime_findings(row: dict[str, Any]) -> list[ValidationFinding]:
    run_id = str(row.get('run_id') or '')
    sid = str(row.get('session_id') or row.get('scenario_id') or '')
    turn_index = int(row.get('turn_index') or 0)
    findings: list[ValidationFinding] = []
    for reason in row.get('failure_reasons') or []:
        ft = _failure_type_for_reason(str(reason))
        findings.append(
            ValidationFinding(
                failure_type=ft,
                severity=severity_for(ft),
                confidence=0.95,
                message=f'Runtime failure: {reason}',
                run_id=run_id,
                scenario_id=sid,
                turn_index=turn_index,
                validator_name='runtime_backfill',
                metadata={'source': 'failure_reasons', 'reason': str(reason)},
            )
        )
    return findings


def _trace_evaluation_from_row(row: dict[str, Any]) -> dict[str, Any]:
    keys = (
        'trace_judge_groundedness', 'trace_judge_correctness', 'trace_judge_relevance',
        'trace_judge_completeness', 'trace_judge_root_cause', 'trace_judge_final_score',
        'trace_judge_mode', 'trace_judge_explanation',
    )
    ev: dict[str, Any] = {}
    for k in keys:
        if row.get(k) is not None:
            ev[k.replace('trace_judge_', '') if k.startswith('trace_judge_') else k] = row[k]
    # normalize to nested shape used elsewhere
    return {
        'groundedness': row.get('trace_judge_groundedness'),
        'correctness': row.get('trace_judge_correctness'),
        'relevance': row.get('trace_judge_relevance'),
        'completeness': row.get('trace_judge_completeness'),
        'root_cause': row.get('trace_judge_root_cause'),
        'final_score': row.get('trace_judge_final_score'),
        'mode': row.get('trace_judge_mode'),
        'explanation': row.get('trace_judge_explanation'),
    }


def _result_from_turn(row: dict[str, Any]) -> dict[str, Any]:
    return {
        'text': str(row.get('answer_text') or row.get('answer_preview') or ''),
        'intent': row.get('actual_intent', ''),
        'tool_results': {},
        'sql_events': [],
        'failure_reasons': row.get('failure_reasons') or [],
        'request_id': str(row.get('request_id') or ''),
        'token_usage': {
            'prompt': int(row.get('tokens_prompt') or 0),
            'completion': int(row.get('tokens_completion') or 0),
        },
        'evaluation': _trace_evaluation_from_row(row),
    }


def _expectations_for_turn(session: dict[str, Any], turn_index: int) -> dict[str, Any]:
    for turn in session_turn_plan(session):
        if int(turn.get('turn_index') or 0) == turn_index:
            exp = turn.get('expectations')
            return exp if isinstance(exp, dict) else {}
    exp = session.get('expectations')
    return exp if isinstance(exp, dict) else {}


def backfill_validation(
    run_id: str,
    *,
    artifacts_dir: Path | None = None,
    overwrite: bool = True,
    catalog_path: Path | None = None,
) -> dict[str, Any]:
    bundle = load_run(run_id, artifacts_dir=artifacts_dir, use_parquet_cache=False)
    run_dir = bundle.run_dir
    turns_path = run_dir / 'session_turns.jsonl'
    if not turns_path.exists():
        raise FileNotFoundError(f'No session_turns.jsonl in {run_dir}')

    cat_path = catalog_path
    if cat_path is None:
        sf = bundle.collection_summary.get('sessions_file')
        if sf:
            cat_path = Path(str(sf))
            if not cat_path.is_absolute():
                cat_path = PROJECT_ROOT / cat_path
    if cat_path is None or not cat_path.exists():
        cat_path = PROJECT_ROOT / 'evaluation' / 'conversational_scenarios' / 'sessions_1000.json'
    catalog = load_catalog(str(cat_path))
    sessions_by_id = {
        str(s.get('session_id') or ''): s
        for s in (catalog.get('sessions') or [])
        if s.get('session_id')
    }

    val_dir = run_dir / 'validation'
    val_dir.mkdir(parents=True, exist_ok=True)
    findings_path = val_dir / 'findings.jsonl'
    scores_path = val_dir / 'scores.jsonl'
    if overwrite:
        for p in (findings_path, scores_path):
            if p.exists():
                p.unlink()

    turns = load_jsonl_rows(turns_path)
    mvp_count = 0
    runtime_count = 0
    total_findings = 0

    for row in turns:
        sid = str(row.get('session_id') or '')
        session = sessions_by_id.get(sid, {})
        turn_index = int(row.get('turn_index') or 0)
        context = session.get('context') if isinstance(session.get('context'), dict) else {}
        if not context.get('customer_name') and row.get('customer_name'):
            context = {**context, 'customer_name': row.get('customer_name'), 'mode': row.get('mode', '')}

        scenario = {
            'scenario_id': sid,
            'session_id': sid,
            'context': context,
            'category': session.get('category') or row.get('category'),
        }
        turn = {
            'turn_index': turn_index,
            'expectations': _expectations_for_turn(session, turn_index),
        }
        result = _result_from_turn(row)
        validation = run_mvp_validation(
            scenario=scenario,
            turn=turn,
            result=result,
            turn_record=row,
        )
        mvp_n = len(validation.findings)
        extra = _runtime_findings(row)
        if extra:
            validation.findings.extend(extra)
            runtime_count += len(extra)
        mvp_count += mvp_n

        for finding in validation.findings:
            append_jsonl(findings_path, finding.to_dict())
            total_findings += 1
        append_jsonl(scores_path, validation.turn_score_record())

    summary = {
        'run_id': run_id,
        'backfilled_at': utc_now_iso(),
        'turns_processed': len(turns),
        'findings_written': total_findings,
        'mvp_derived_findings': mvp_count,
        'runtime_mapped_findings': runtime_count,
        'findings_path': str(findings_path),
        'scores_path': str(scores_path),
        'catalog_path': str(cat_path),
    }
    write_json(val_dir / 'backfill_summary.json', summary)
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Backfill validation/findings.jsonl from session turns.')
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--artifacts-dir', default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument('--catalog', default='', help='Override sessions catalog JSON path')
    parser.add_argument('--no-overwrite', dest='overwrite', action='store_false', default=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    catalog = Path(args.catalog) if args.catalog else None
    summary = backfill_validation(
        args.run_id,
        artifacts_dir=Path(args.artifacts_dir),
        overwrite=args.overwrite,
        catalog_path=catalog,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
