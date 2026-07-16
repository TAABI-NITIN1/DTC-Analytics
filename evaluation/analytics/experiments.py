"""Compare evaluation runs and detect regressions."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from evaluation.analytics.compute import build_analytics_summary
from evaluation.analytics.loader import load_run
from evaluation.analytics.registry import register_run

DEFAULT_REGRESSION_THRESHOLDS: dict[str, float] = {
    'hallucination_rate': 0.05,
    'p95_latency_sec': 0.20,
    'pass_rate': -0.05,
    'ai_health_score': -5.0,
    'total_cost_usd': 0.15,
    'avg_groundedness_score': -0.05,
}


def _metric_delta(baseline: dict[str, Any], candidate: dict[str, Any], key: str) -> float | None:
    b = baseline.get(key)
    c = candidate.get(key)
    if b is None or c is None:
        return None
    try:
        return round(float(c) - float(b), 6)
    except (TypeError, ValueError):
        return None


def compare_runs(
    baseline_run_id: str,
    candidate_run_id: str,
    *,
    artifacts_dir: Path | None = None,
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Compare two local eval runs; negative pass_rate delta means regression."""
    thresholds = thresholds or DEFAULT_REGRESSION_THRESHOLDS
    base_bundle = load_run(baseline_run_id, artifacts_dir=artifacts_dir)
    cand_bundle = load_run(candidate_run_id, artifacts_dir=artifacts_dir)

    base_summary = base_bundle.analytics_summary or build_analytics_summary(base_bundle)
    cand_summary = cand_bundle.analytics_summary or build_analytics_summary(cand_bundle)
    base_m = base_summary.get('run_metrics') or {}
    cand_m = cand_summary.get('run_metrics') or {}

    deltas: dict[str, float | None] = {}
    for key in (
        'pass_rate', 'ai_health_score', 'hallucination_rate', 'p95_latency_sec',
        'avg_latency_sec', 'total_cost_usd', 'avg_groundedness_score',
        'avg_correctness_score', 'gate_pass_rate', 'safety_violations',
    ):
        deltas[key] = _metric_delta(base_m, cand_m, key)

    alerts: list[dict[str, Any]] = []
    if deltas.get('hallucination_rate') is not None and deltas['hallucination_rate'] > thresholds['hallucination_rate']:
        alerts.append({'metric': 'hallucination_rate', 'delta': deltas['hallucination_rate'], 'severity': 'high'})
    if deltas.get('p95_latency_sec') is not None and base_m.get('p95_latency_sec'):
        rel = deltas['p95_latency_sec'] / float(base_m['p95_latency_sec'])
        if rel > thresholds['p95_latency_sec']:
            alerts.append({'metric': 'p95_latency_sec', 'delta': deltas['p95_latency_sec'], 'relative': rel, 'severity': 'medium'})
    if deltas.get('pass_rate') is not None and deltas['pass_rate'] < thresholds['pass_rate']:
        alerts.append({'metric': 'pass_rate', 'delta': deltas['pass_rate'], 'severity': 'high'})
    if deltas.get('ai_health_score') is not None and deltas['ai_health_score'] < thresholds['ai_health_score']:
        alerts.append({'metric': 'ai_health_score', 'delta': deltas['ai_health_score'], 'severity': 'high'})
    if deltas.get('total_cost_usd') is not None and base_m.get('total_cost_usd'):
        rel_cost = deltas['total_cost_usd'] / float(base_m['total_cost_usd'])
        if rel_cost > thresholds['total_cost_usd']:
            alerts.append({'metric': 'total_cost_usd', 'delta': deltas['total_cost_usd'], 'relative': rel_cost, 'severity': 'medium'})

    base_fail = (base_summary.get('failure_analytics') or {}).get('by_category', {})
    cand_fail = (cand_summary.get('failure_analytics') or {}).get('by_category', {})
    failure_category_deltas = {
        cat: int(cand_fail.get(cat, 0)) - int(base_fail.get(cat, 0))
        for cat in set(base_fail) | set(cand_fail)
    }

    return {
        'baseline_run_id': baseline_run_id,
        'candidate_run_id': candidate_run_id,
        'baseline_metrics': base_m,
        'candidate_metrics': cand_m,
        'deltas': deltas,
        'failure_category_deltas': failure_category_deltas,
        'regression_alerts': alerts,
        'improved': (
            (deltas.get('pass_rate') or 0) > 0
            and (deltas.get('hallucination_rate') or 0) <= 0
        ),
    }


def register_run_from_dir(
    run_dir: Path,
    *,
    collection_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from evaluation.analytics.compute import build_analytics_summary, write_analytics_artifacts
    from evaluation.analytics.loader import load_run

    run_id = run_dir.name
    bundle = load_run(run_id, artifacts_dir=run_dir.parent, use_parquet_cache=False)
    write_analytics_artifacts(bundle)
    summary = build_analytics_summary(bundle)
    return register_run(
        run_id=run_id,
        run_dir=run_dir,
        collection_summary=collection_summary or bundle.collection_summary,
        analytics_summary=summary,
        artifacts_dir=run_dir.parent,
    )
