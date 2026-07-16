"""Local evaluation analytics: load artifacts, compute metrics, compare runs."""

from evaluation.analytics.compute import (
    build_analytics_summary,
    compute_ai_health_score,
    compute_run_metrics,
    compute_session_metrics,
    compute_trace_metrics,
)
from evaluation.analytics.experiments import compare_runs, register_run
from evaluation.analytics.failure_analytics import aggregate_failures
from evaluation.analytics.export_dashboard import export_dashboard_bundle, write_dashboard_bundle
from evaluation.analytics.loader import RunBundle, load_run, list_runs
from evaluation.analytics.pricing import estimate_cost_usd

__all__ = [
    'export_dashboard_bundle',
    'write_dashboard_bundle',
    'RunBundle',
    'aggregate_failures',
    'build_analytics_summary',
    'compare_runs',
    'compute_ai_health_score',
    'compute_run_metrics',
    'compute_session_metrics',
    'compute_trace_metrics',
    'estimate_cost_usd',
    'list_runs',
    'load_run',
    'register_run',
]
