import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import { fmtNum, fmtPct } from '../hooks/useDashboardBundle';

export default function ExperimentDashboard({ bundle, allBundles = [] }) {
  const cmp = bundle?.experiment_compare;
  const deltas = cmp?.deltas || {};
  const alerts = cmp?.regression_alerts || [];

  const scatter = allBundles.map((b) => ({
    run: b.meta?.run_id?.slice(-8) || '?',
    cost: b.run_metrics?.total_cost_usd || 0,
    quality: b.run_metrics?.ai_health_score || 0,
    latency: b.run_metrics?.avg_latency_sec || 0,
  }));

  return (
    <>
      <h2 className="page-title">Experiment Comparison</h2>
      <p className="page-desc">Regression tracking across eval runs</p>
      {cmp ? (
        <>
          <p className="page-desc">
            Baseline: {cmp.baseline_run_id} → Candidate: {cmp.candidate_run_id}
            {cmp.improved ? ' (improved)' : ''}
          </p>
          <div className="kpi-grid">
            <KpiCard title="Δ Pass Rate" value={fmtNum(deltas.pass_rate, 4)} />
            <KpiCard title="Δ AI Health" value={fmtNum(deltas.ai_health_score, 2)} />
            <KpiCard title="Δ Hallucination" value={fmtNum(deltas.hallucination_rate, 4)} />
            <KpiCard title="Δ P95 Latency" value={fmtNum(deltas.p95_latency_sec, 2)} />
            <KpiCard title="Δ Total Cost" value={fmtNum(deltas.total_cost_usd, 4)} />
          </div>
          {alerts.length > 0 && (
            <div className="gaps-banner">
              <strong>Regression alerts:</strong>{' '}
              {alerts.map((a) => `${a.metric} (${a.severity})`).join(', ')}
            </div>
          )}
        </>
      ) : (
        <p className="page-desc">Export with --baseline to populate experiment_compare in bundle.</p>
      )}
      {scatter.length > 1 && (
        <ChartCard title="AI Health vs total cost (all loaded runs)">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={scatter}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="run" tick={{ fill: '#8b9cb3' }} />
              <YAxis yAxisId="left" tick={{ fill: '#8b9cb3' }} />
              <YAxis yAxisId="right" orientation="right" tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar yAxisId="left" dataKey="quality" fill="#22c55e" name="Health" />
              <Bar yAxisId="right" dataKey="cost" fill="#3b82f6" name="Cost USD" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}
    </>
  );
}
