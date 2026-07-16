"""Build static dashboard_bundle.json for the Vite eval-dashboard."""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path
from typing import Any

from evaluation.analytics.compute import (
    build_analytics_summary,
    compute_session_metrics,
    compute_trace_metrics,
    write_analytics_artifacts,
)
from evaluation.analytics.experiments import compare_runs
from evaluation.analytics.failure_analytics import aggregate_failures
from evaluation.analytics.loader import RunBundle, enrich_turns_with_validation, load_run
from evaluation.analytics.pricing import turn_cost_from_row, version_model_name
from evaluation.local_store import write_json, utc_now_iso


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return round(s[int(k)], 4)
    return round(s[f] + (s[c] - s[f]) * (k - f), 4)


def _numeric_histogram(values: list[float], bins: int = 20, prefix: str = '') -> list[dict[str, Any]]:
    if not values:
        return []
    lo, hi = min(values), max(values)
    if hi <= lo:
        return [{'bin': f'{prefix}{lo:.2f}', 'count': len(values)}]
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in values:
        idx = min(bins - 1, int((v - lo) / width) if width > 0 else 0)
        counts[idx] += 1
    out = []
    for i, c in enumerate(counts):
        if c == 0:
            continue
        b0 = lo + i * width
        b1 = b0 + width
        out.append({'bin': f'{prefix}{b0:.2f}-{b1:.2f}', 'count': c})
    return out


def _latency_histogram(latencies: list[float], bins: int = 20) -> list[dict[str, Any]]:
    if not latencies:
        return []
    lo, hi = min(latencies), max(latencies)
    if hi <= lo:
        return [{'bin': f'{lo:.1f}', 'count': len(latencies)}]
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in latencies:
        idx = min(bins - 1, int((v - lo) / width) if width > 0 else 0)
        counts[idx] += 1
    out = []
    for i, c in enumerate(counts):
        if c == 0:
            continue
        b0 = lo + i * width
        b1 = b0 + width
        out.append({'bin': f'{b0:.0f}-{b1:.0f}s', 'count': c})
    return out


def _detect_data_gaps(bundle: RunBundle) -> list[str]:
    gaps: list[str] = []
    if not bundle.trace_events:
        gaps.append('trace_events_empty')
    if not bundle.findings:
        gaps.append('validation_findings_empty')
    elif not bundle.sql_events:
        tool_empty = sum(
            1 for f in bundle.findings
            if str(f.get('failure_type') or '') == 'tool.empty_result'
        )
        if tool_empty > 50:
            gaps.append('validation_without_tool_evidence')
    if not bundle.sql_events:
        gaps.append('sql_events_empty')
    gaps.append('system_metrics_not_collected')
    return gaps


def _coverage_stats(bundle: RunBundle, turns: list[dict[str, Any]]) -> dict[str, Any]:
    sessions = bundle.sessions
    catalog_sessions = []
    if isinstance(bundle.catalog.get('sessions'), list):
        catalog_sessions = bundle.catalog['sessions']
    missing = 0
    if catalog_sessions:
        by_sid = Counter(str(t.get('session_id') or t.get('scenario_id') or '') for t in turns)
        for s in catalog_sessions:
            sid = str(s.get('session_id') or '')
            expected = int(s.get('max_turns') or 1)
            actual = by_sid.get(sid, 0)
            if actual < expected:
                missing += 1
    return {
        'unique_sessions': len(sessions),
        'turn_rows': len(turns),
        'catalog_sessions': len(catalog_sessions),
        'missing_turns_vs_catalog': missing,
    }


def build_chart_series(bundle: RunBundle, turns: list[dict[str, Any]], sessions: list[Any]) -> dict[str, Any]:
    import pandas as pd

    df = pd.DataFrame(turns) if turns else pd.DataFrame()
    model = version_model_name(bundle.version)
    charts: dict[str, Any] = {}

    if df.empty:
        return charts

    lat = df['latency_sec'].apply(_safe_float) if 'latency_sec' in df.columns else pd.Series(dtype=float)
    lat = lat[lat > 0]
    charts['latency_histogram'] = _latency_histogram(lat.tolist())

    if 'session_type' in df.columns and 'latency_sec' in df.columns:
        rows = []
        for st, g in df.groupby('session_type'):
            vals = g['latency_sec'].apply(_safe_float)
            vals = vals[vals > 0]
            if len(vals):
                rows.append({
                    'session_type': st,
                    'mean': round(vals.mean(), 2),
                    'p50': _percentile(vals.tolist(), 50),
                    'p95': _percentile(vals.tolist(), 95),
                })
        charts['latency_by_session_type'] = rows

    if 'category' in df.columns:
        cost_rows = []
        for cat, g in df.groupby('category'):
            cost = sum(turn_cost_from_row(r, model) for r in g.to_dict('records'))
            cost_rows.append({'category': cat, 'usd': round(cost, 4)})
        charts['cost_by_category'] = sorted(cost_rows, key=lambda x: x['usd'], reverse=True)[:20]

    if 'customer_name' in df.columns:
        cust_rows = []
        for cust, g in df.groupby('customer_name'):
            cost = sum(turn_cost_from_row(r, model) for r in g.to_dict('records'))
            cust_rows.append({'customer': str(cust)[:40], 'usd': round(cost, 4)})
        charts['cost_by_customer'] = sorted(cust_rows, key=lambda x: x['usd'], reverse=True)[:15]

    if 'category' in df.columns and 'trace_judge_final_score' in df.columns:
        def _safe_mean(series: Any) -> float | None:
            vals = series.apply(_safe_float).dropna()
            if vals.empty:
                return None
            m = float(vals.mean())
            return round(m, 3) if not math.isnan(m) else None

        jdf = (
            df.groupby('category')['trace_judge_final_score']
            .apply(_safe_mean)
            .reset_index(name='avg_trace_judge')
        )
        jdf = jdf.dropna(subset=['avg_trace_judge'])
        charts['judge_scores_by_category'] = jdf.sort_values('avg_trace_judge').to_dict('records')

    if 'follow_up_source' in df.columns:
        charts['follow_up_source_mix'] = [
            {'source': k, 'count': int(v)} for k, v in df['follow_up_source'].value_counts().items()
        ]

    if 'turn_index' in df.columns and 'trace_judge_final_score' in df.columns:
        def _turn_mean(series: Any) -> float | None:
            vals = series.apply(_safe_float).dropna()
            if vals.empty:
                return None
            m = float(vals.mean())
            return round(m, 3) if not math.isnan(m) else None

        tdf = (
            df.groupby('turn_index')['trace_judge_final_score']
            .apply(_turn_mean)
            .reset_index(name='avg_score')
        )
        tdf = tdf.dropna(subset=['avg_score'])
        charts['turn_score_by_turn_index'] = tdf.to_dict('records')

    tool_counts: Counter[str] = Counter()
    for t in turns:
        tools = t.get('actual_tools')
        if isinstance(tools, list):
            for tool in tools:
                tool_counts[str(tool)] += 1
    charts['tool_usage'] = [{'tool': k, 'count': v} for k, v in tool_counts.most_common(25)]

    if 'sql_success_rate' in df.columns:
        charts['sql_success_distribution'] = [
            {'bucket': str(b), 'count': int(c)}
            for b, c in pd.cut(df['sql_success_rate'].apply(_safe_float), bins=5).value_counts().items()
        ]

    if 'session_id' in df.columns and 'tokens_total' in df.columns:
        tok = df.groupby('session_id').agg(
            turns=('turn_index', 'count'),
            tokens=('tokens_total', lambda s: int(s.apply(lambda x: int(_safe_float(x))).sum())),
        ).reset_index()
        charts['token_growth_scatter'] = tok.head(200).to_dict('records')

    for col, key in [
        ('trace_judge_groundedness', 'groundedness_histogram'),
        ('trace_judge_correctness', 'correctness_histogram'),
        ('trace_judge_final_score', 'final_judge_histogram'),
    ]:
        if col in df.columns:
            vals = df[col].apply(_safe_float).dropna().tolist()
            if vals:
                charts[key] = _numeric_histogram(vals, bins=15)

    if lat.tolist():
        sorted_lat = sorted(lat.tolist())
        step = max(1, len(sorted_lat) // 100)
        charts['latency_ecdf'] = [
            {'latency_sec': round(sorted_lat[i], 2), 'pct': round(100 * i / len(sorted_lat), 1)}
            for i in range(0, len(sorted_lat), step)
        ]

    if 'session_type' in df.columns and 'gate_passed' in df.columns:
        gp = df.groupby('session_type')['gate_passed'].mean().reset_index()
        gp.columns = ['session_type', 'gate_pass_rate']
        gp['gate_pass_rate'] = gp['gate_pass_rate'].apply(
            lambda v: None if (isinstance(v, float) and math.isnan(v)) else round(float(v), 4)
        )
        charts['gate_pass_by_session_type'] = gp.to_dict('records')

    # Session-level cost distribution
    session_dicts = [s.to_dict() if hasattr(s, 'to_dict') else s for s in sessions]
    if session_dicts:
        sdf = pd.DataFrame(session_dicts)
        if 'total_session_cost_usd' in sdf.columns:
            costs = sdf['total_session_cost_usd'].apply(_safe_float).tolist()
            charts['cost_per_session_histogram'] = _numeric_histogram(costs, bins=25, prefix='$')
        if 'scenario_category' in sdf.columns and 'total_session_cost_usd' in sdf.columns:
            charts['avg_cost_by_scenario'] = (
                sdf.groupby('scenario_category')['total_session_cost_usd']
                .mean()
                .reset_index(name='usd')
                .sort_values('usd', ascending=False)
                .head(20)
                .to_dict('records')
            )
        if 'session_pass_fail' in sdf.columns:
            charts['pass_fail_counts'] = [
                {'status': k, 'count': int(v)}
                for k, v in sdf['session_pass_fail'].value_counts().items()
            ]

    return charts


def build_tables(bundle: RunBundle, sessions: list[Any], turns: list[dict[str, Any]]) -> dict[str, Any]:
    session_dicts = [s.to_dict() if hasattr(s, 'to_dict') else s for s in sessions]
    worst = sorted(session_dicts, key=lambda x: _safe_float(x.get('session_score'), 1.0))[:15]
    expensive = sorted(session_dicts, key=lambda x: _safe_float(x.get('total_session_cost_usd'), 0), reverse=True)[:15]
    high_risk = [
        s for s in session_dicts
        if s.get('session_pass_fail') == 'fail' or int(s.get('safety_violation_count') or 0) > 0
    ][:20]
    return {
        'worst_sessions': worst,
        'most_expensive_sessions': expensive,
        'high_risk_sessions': high_risk,
        'run_comparison': [],
    }


def export_dashboard_bundle(
    run_id: str,
    *,
    artifacts_dir: Path | None = None,
    baseline_run_id: str | None = None,
    ensure_summary: bool = True,
) -> dict[str, Any]:
    bundle = load_run(run_id, artifacts_dir=artifacts_dir)
    if ensure_summary and not bundle.analytics_summary:
        write_analytics_artifacts(bundle)
        bundle = load_run(run_id, artifacts_dir=artifacts_dir)

    summary = bundle.analytics_summary or build_analytics_summary(bundle)
    turns = enrich_turns_with_validation(bundle.turns, bundle.findings)
    sessions = compute_session_metrics(bundle)
    trace_m = summary.get('trace_metrics') or compute_trace_metrics(bundle)
    fail_m = aggregate_failures(
        bundle.findings,
        total_turns=len(turns),
        total_sessions=len(sessions),
    )
    fail_m['runtime_failure_turns'] = sum(1 for t in turns if t.get('failure_reasons'))

    cs = bundle.collection_summary or bundle.session_summary
    meta = {
        'run_id': run_id,
        'generated_at': utc_now_iso(),
        'api_base_url': cs.get('api_base_url', ''),
        'eval_environment': cs.get('eval_environment', cs.get('local_only') and 'local' or ''),
        'layout': bundle.layout,
        'version': bundle.version,
    }

    experiment_compare = None
    if baseline_run_id and baseline_run_id != run_id:
        experiment_compare = compare_runs(baseline_run_id, run_id, artifacts_dir=artifacts_dir)

    charts = build_chart_series(bundle, turns, sessions)  # sessions: list[SessionMetrics]
    by_cat_turns = fail_m.get('by_category_turns') or {}
    if by_cat_turns:
        charts['failure_by_category'] = [
            {'category': k, 'turns': int(v), 'count': int(v)} for k, v in by_cat_turns.items()
        ]
    elif fail_m.get('by_category'):
        charts['failure_by_category'] = [
            {'category': k, 'turns': int(v), 'count': int(v)} for k, v in fail_m['by_category'].items()
        ]
    by_type_turns = fail_m.get('by_failure_type_turns') or {}
    if by_type_turns:
        charts['failure_by_type'] = [
            {'failure_type': k, 'turns': int(v), 'count': int(v)}
            for k, v in list(by_type_turns.items())[:25]
        ]
    elif fail_m.get('by_failure_type'):
        charts['failure_by_type'] = [
            {'failure_type': k, 'turns': int(v), 'count': int(v)}
            for k, v in list(fail_m['by_failure_type'].items())[:25]
        ]

    bundle_out: dict[str, Any] = {
        'meta': meta,
        'run_metrics': summary.get('run_metrics', {}),
        'trace_metrics': trace_m,
        'failure_analytics': fail_m,
        'experiment_compare': experiment_compare,
        'charts': charts,
        'tables': build_tables(bundle, sessions, turns),
        'coverage': _coverage_stats(bundle, turns),
        'data_gaps': _detect_data_gaps(bundle),
    }
    return bundle_out


def write_dashboard_bundle(
    run_id: str,
    *,
    artifacts_dir: Path | None = None,
    baseline_run_id: str | None = None,
    copy_to_eval_dashboard: bool = False,
    eval_dashboard_dir: Path | None = None,
) -> Path:
    from evaluation.local_store import DEFAULT_OUTPUT_DIR

    base = Path(artifacts_dir or DEFAULT_OUTPUT_DIR)
    data = export_dashboard_bundle(
        run_id,
        artifacts_dir=artifacts_dir,
        baseline_run_id=baseline_run_id,
    )
    out_path = base / run_id / 'dashboard_bundle.json'
    write_json(out_path, data)

    if copy_to_eval_dashboard:
        dash = Path(eval_dashboard_dir or Path.cwd() / 'eval-dashboard')
        runs_dir = dash / 'public' / 'runs'
        runs_dir.mkdir(parents=True, exist_ok=True)
        dest = runs_dir / f'{run_id}.json'
        write_json(dest, data)
        _update_manifest(runs_dir, run_id, data.get('meta', {}))

    return out_path


def _update_manifest(runs_dir: Path, run_id: str, meta: dict[str, Any]) -> None:
    manifest_path = runs_dir / 'manifest.json'
    entries: list[dict[str, Any]] = []
    if manifest_path.exists():
        try:
            entries = json.loads(manifest_path.read_text(encoding='utf-8'))
            if not isinstance(entries, list):
                entries = []
        except json.JSONDecodeError:
            entries = []
    entries = [e for e in entries if e.get('run_id') != run_id]
    entries.insert(0, {
        'run_id': run_id,
        'label': f"{run_id} ({meta.get('eval_environment') or 'unknown'})",
        'path': f'./runs/{run_id}.json',
        'generated_at': meta.get('generated_at'),
    })
    manifest_path.write_text(json.dumps(entries, indent=2), encoding='utf-8')


def export_all_to_eval_dashboard(
    run_ids: list[str],
    *,
    artifacts_dir: Path | None = None,
    baseline_run_id: str | None = None,
    eval_dashboard_dir: Path | None = None,
) -> list[Path]:
    paths = []
    for rid in run_ids:
        p = write_dashboard_bundle(
            rid,
            artifacts_dir=artifacts_dir,
            baseline_run_id=baseline_run_id if rid != baseline_run_id else None,
            copy_to_eval_dashboard=True,
            eval_dashboard_dir=eval_dashboard_dir,
        )
        paths.append(p)
    return paths
