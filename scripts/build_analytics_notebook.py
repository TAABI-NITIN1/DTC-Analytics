"""Generate dtc_bot_analytics_ravi.ipynb from template cells."""
from __future__ import annotations

import json
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / 'dtc_bot_analytics_ravi.ipynb'

SETUP = r'''
import os
import sys
from pathlib import Path

ROOT = Path.cwd()
if not (ROOT / 'evaluation').exists():
    ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from IPython.display import display

# Non-blocking charts in Jupyter and batch runs
pio.renderers.default = os.getenv('PLOTLY_RENDERER', 'plotly_mimetype+notebook')

from evaluation.analytics.loader import load_run, list_runs, enrich_turns_with_validation
from evaluation.analytics.compute import build_analytics_summary, compute_session_metrics, compute_trace_metrics
from evaluation.analytics.experiments import compare_runs
from evaluation.analytics.failure_analytics import aggregate_failures

def show_fig(fig):
    """Display Plotly figure without hanging on browser open."""
    try:
        display(fig)
    except Exception:
        fig.show(renderer='json')

# --- Select run ---
RUN_ID = os.getenv('EVAL_ANALYTICS_RUN_ID', 'eval_20260520_085108_190cf6')
BASELINE_RUN_ID = os.getenv('EVAL_BASELINE_RUN_ID', '')

runs = list_runs()
print('Available runs:', [r.get('run_id') for r in runs[:10]])

bundle = load_run(RUN_ID)
if not bundle.analytics_summary:
    from evaluation.analytics.compute import write_analytics_artifacts
    write_analytics_artifacts(bundle)
    bundle = load_run(RUN_ID)

summary = bundle.analytics_summary or build_analytics_summary(bundle)
run_m = summary.get('run_metrics', {})
turns_df = pd.DataFrame(enrich_turns_with_validation(bundle.turns, bundle.findings))
sessions_df = pd.DataFrame([s.to_dict() for s in compute_session_metrics(bundle)])
trace_m = summary.get('trace_metrics') or compute_trace_metrics(bundle)
fail_m = summary.get('failure_analytics') or aggregate_failures(bundle.findings)
print(f'Loaded {RUN_ID}: {len(turns_df)} turns, {len(sessions_df)} sessions')
'''


def cell(cell_type: str, source: str) -> dict:
    cid = uuid.uuid4().hex[:8]
    base: dict = {
        'id': cid,
        'cell_type': cell_type,
        'metadata': {},
        'source': [line + '\n' for line in source.strip().split('\n')],
    }
    if cell_type == 'code':
        base['execution_count'] = None
        base['outputs'] = []
    return base


cells = [
    cell('markdown', '# DTC Bot Evaluation Analytics\n\nOperational intelligence for Taabi AI Analyst benchmark runs. Set `EVAL_ANALYTICS_RUN_ID` to switch runs.'),
    cell('code', SETUP),
    cell('markdown', '## 1 — Executive AI Health Dashboard'),
    cell('code', '''
kpis = {
    'AI Health Score': run_m.get('ai_health_score'),
    'Pass Rate': run_m.get('pass_rate'),
    'Avg Cost / Session (USD)': run_m.get('avg_cost_per_session_usd'),
    'Avg Latency (sec)': run_m.get('avg_latency_sec'),
    'Hallucination Rate': run_m.get('hallucination_rate'),
    'P95 Latency (sec)': run_m.get('p95_latency_sec'),
    'Safety Violations': run_m.get('safety_violations'),
}
display(pd.DataFrame([{'KPI': k, 'Value': v} for k, v in kpis.items()]))
if len(runs) > 1:
    trend = pd.DataFrame([
        {'run_id': r.get('run_id'), 'ai_health_score': r.get('ai_health_score'), 'pass_rate': r.get('pass_rate')}
        for r in runs[:15] if r.get('ai_health_score') is not None
    ])
    if not trend.empty:
        show_fig(px.line(trend, x='run_id', y=['ai_health_score', 'pass_rate'], title='Trend Over Recent Runs').update_layout(xaxis_tickangle=-45))
'''),
    cell('markdown', '## 2 — Cost Analytics Dashboard'),
    cell('code', '''
if not sessions_df.empty and 'total_session_cost_usd' in sessions_df.columns:
    show_fig(px.histogram(sessions_df, x='total_session_cost_usd', nbins=40, title='Cost Per Conversation'))
    by_cat = sessions_df.groupby('scenario_category', as_index=False)['total_session_cost_usd'].mean()
    show_fig(px.bar(by_cat.sort_values('total_session_cost_usd', ascending=False).head(20),
                  x='scenario_category', y='total_session_cost_usd', title='Avg Cost By Scenario Category').update_layout(xaxis_tickangle=-45))
    top = sessions_df.nlargest(15, 'total_session_cost_usd')[['session_id', 'scenario_category', 'total_session_cost_usd', 'total_session_tokens']]
    display(top)
if not turns_df.empty and 'turn_index' in turns_df.columns:
    tg = turns_df.groupby('turn_index')['tokens_total'].mean().reset_index()
    show_fig(px.line(tg, x='turn_index', y='tokens_total', title='Token Growth By Turn Index'))
'''),
    cell('markdown', '## 3 — Speed Analytics Dashboard'),
    cell('code', '''
if not turns_df.empty:
    show_fig(px.ecdf(turns_df, x='latency_sec', title='Latency Distribution'))
    print(f"P95 latency: {run_m.get('p95_latency_sec')} sec")
slow_nodes = trace_m.get('slowest_nodes', [])
if slow_nodes:
    ndf = pd.DataFrame(slow_nodes)
    show_fig(px.bar(ndf, x='node', y='avg_duration_sec', title='Slowest Graph Nodes'))
slow_sql = trace_m.get('slowest_sql', [])
if slow_sql:
    sdf = pd.DataFrame(slow_sql).head(20)
    show_fig(px.bar(sdf, x='sql_hash', y='duration_sec', title='Slowest SQL Queries').update_layout(xaxis_tickangle=-45))
'''),
    cell('markdown', '## 4 — Quality Analytics Dashboard'),
    cell('code', '''
if not turns_df.empty:
    for col, title in [
        ('trace_judge_groundedness', 'Groundedness Distribution'),
        ('trace_judge_correctness', 'Correctness Distribution'),
        ('trace_judge_final_score', 'Final Judge Score'),
    ]:
        if col in turns_df.columns and turns_df[col].notna().any():
            show_fig(px.histogram(turns_df, x=col, nbins=30, title=title))
fail_types = fail_m.get('by_failure_type', {})
if fail_types:
    fdf = pd.DataFrame([{'failure_type': k, 'count': v} for k, v in fail_types.items()])
    fdf = fdf.sort_values('count', ascending=False).head(25)
    show_fig(px.bar(fdf, x='failure_type', y='count', title='Failure Breakdown').update_layout(xaxis_tickangle=-45))
if 'category' in turns_df.columns and 'trace_judge_final_score' in turns_df.columns:
    heat = turns_df.groupby('category')['trace_judge_final_score'].mean().reset_index()
    show_fig(px.bar(heat.sort_values('trace_judge_final_score'), x='category', y='trace_judge_final_score', title='Avg Quality By Category').update_layout(xaxis_tickangle=-45))
'''),
    cell('markdown', '## 5 — Conversation Analytics Dashboard'),
    cell('code', '''
multi = turns_df[turns_df['session_type'].isin(['static_multi', 'dynamic_multi'])] if 'session_type' in turns_df.columns else pd.DataFrame()
if not multi.empty:
    if 'memory' in multi.columns:
        drift = multi['memory'].apply(lambda m: (m or {}).get('context_drift', 0) if isinstance(m, dict) else 0)
        print(f"Context drift turns (rate>0.3): {(drift > 0.3).sum()}")
    if 'gate_passed' in multi.columns:
        gp = multi.copy()
        gp['gate_passed'] = gp['gate_passed'].astype(bool)
        gp = gp.groupby('turn_index')['gate_passed'].mean().reset_index()
        show_fig(px.line(gp, x='turn_index', y='gate_passed', title='Gate Pass Rate By Turn Index'))
    if not sessions_df.empty and 'session_type' in sessions_df.columns:
        completion = sessions_df[sessions_df['session_type'].isin(['static_multi', 'dynamic_multi'])]['session_pass_fail'].value_counts()
        print('Multi-turn completion:', completion.to_dict())
else:
    print('No multi-turn sessions in this run.')
'''),
    cell('markdown', '## 6 — Tool & SQL Analytics Dashboard'),
    cell('code', '''
if not turns_df.empty and 'actual_tools' in turns_df.columns:
    tool_rows = []
    for _, row in turns_df.iterrows():
        tools = row.get('actual_tools') or []
        if isinstance(tools, list):
            for t in tools:
                tool_rows.append({'tool': str(t), 'session_id': row.get('session_id')})
    if tool_rows:
        tdf = pd.DataFrame(tool_rows)
        vc = tdf['tool'].value_counts().reset_index()
        vc.columns = ['tool', 'count']
        show_fig(px.bar(vc, x='tool', y='count', title='Tool Usage Frequency'))
if not bundle.sql_events:
    print('No sql_events.jsonl — re-run collection with enriched session_runner.')
else:
    sql_df = pd.DataFrame(bundle.sql_events)
    if not sql_df.empty and 'sql_hash' in sql_df.columns:
        rep = sql_df['sql_hash'].value_counts()
        repeated = rep[rep > 1]
        print(f"Repeated SQL patterns: {len(repeated)}")
    if not sql_df.empty and 'duration_sec' in sql_df.columns and 'turn_index' in sql_df.columns:
        show_fig(px.scatter(sql_df, x='turn_index', y='duration_sec', title='SQL Duration By Turn', opacity=0.5))
'''),
    cell('markdown', '## 7 — Failure Analytics Dashboard'),
    cell('code', '''
cat_fail = fail_m.get('by_category', {})
if cat_fail:
    cdf = pd.DataFrame([{'category': k, 'count': v} for k, v in cat_fail.items()])
    show_fig(px.bar(cdf, x='category', y='count', title='Failure Category Distribution'))
critical = fail_m.get('critical_sessions', [])
if critical:
    print('High-risk sessions (sample):', critical[:10])
if BASELINE_RUN_ID and BASELINE_RUN_ID != RUN_ID:
    cmp = compare_runs(BASELINE_RUN_ID, RUN_ID)
    print('Regression alerts:', cmp.get('regression_alerts'))
'''),
    cell('markdown', '## 8 — Experiment Comparison Dashboard'),
    cell('code', '''
baseline_id = BASELINE_RUN_ID or (runs[1]['run_id'] if len(runs) > 1 else '')
if baseline_id and baseline_id != RUN_ID:
    cmp = compare_runs(baseline_id, RUN_ID)
    deltas = cmp.get('deltas', {})
    ddf = pd.DataFrame([{'metric': k, 'delta': v} for k, v in deltas.items() if v is not None])
    display(ddf)
    if cmp.get('regression_alerts'):
        print('REGRESSION ALERTS:', cmp['regression_alerts'])
    b = cmp['baseline_metrics']
    c = cmp['candidate_metrics']
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[b.get('total_cost_usd')], y=[b.get('avg_groundedness_score') or b.get('avg_correctness_score')],
                             mode='markers+text', name='baseline', text=[baseline_id]))
    fig.add_trace(go.Scatter(x=[c.get('total_cost_usd')], y=[c.get('avg_groundedness_score') or c.get('avg_correctness_score')],
                             mode='markers+text', name='candidate', text=[RUN_ID]))
    fig.update_layout(title='Cost vs Quality', xaxis_title='Total Cost USD', yaxis_title='Avg Groundedness/Correctness')
    show_fig(fig)
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=[b.get('p95_latency_sec')], y=[b.get('pass_rate')], mode='markers+text', name='baseline', text=[baseline_id]))
    fig2.add_trace(go.Scatter(x=[c.get('p95_latency_sec')], y=[c.get('pass_rate')], mode='markers+text', name='candidate', text=[RUN_ID]))
    fig2.update_layout(title='Latency vs Quality Tradeoff', xaxis_title='P95 Latency sec', yaxis_title='Pass Rate')
    show_fig(fig2)
else:
    print('Set EVAL_BASELINE_RUN_ID or ensure index.json has 2+ runs for experiment comparison.')
'''),
    cell('markdown', '## Appendix — Production Ops (ClickHouse)\n\nEval runs do not write to `ai_obs_*`. Uncomment below for production observability only.'),
    cell('code', '# from src.observability_store import ObservabilityStore\n# store = ObservabilityStore()\n# print("Production observability — separate from eval benchmarks")'),
]

nb = {
    'nbformat': 4,
    'nbformat_minor': 5,
    'metadata': {
        'kernelspec': {'display_name': 'Python 3', 'language': 'python', 'name': 'python3'},
        'language_info': {'name': 'python', 'pygments_lexer': 'ipython3'},
    },
    'cells': cells,
}
OUT.write_text(json.dumps(nb, indent=1), encoding='utf-8')
print(f'Wrote {OUT}')
