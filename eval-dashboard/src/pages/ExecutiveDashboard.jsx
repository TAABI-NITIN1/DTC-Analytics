import {
  Bar, BarChart, CartesianGrid, Cell, Legend, Line, LineChart,
  Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataGapsBanner from '../components/DataGapsBanner';
import { fmtNum, fmtPct, fmtUsd } from '../hooks/useDashboardBundle';

const PIE_COLORS = ['#22c55e', '#ef4444', '#f59e0b'];

export default function ExecutiveDashboard({ bundle }) {
  const m = bundle?.run_metrics || {};
  const cov = bundle?.coverage || {};
  const charts = bundle?.charts || {};

  const kpiRows = [
    ['AI Health Score', fmtNum(m.ai_health_score, 1)],
    ['Pass Rate', fmtPct(m.pass_rate)],
    ['Avg Cost / Session (USD)', fmtUsd(m.avg_cost_per_session_usd)],
    ['Avg Latency (sec)', fmtNum(m.avg_latency_sec)],
    ['Turns w/ hallucination.* flags', fmtPct(m.hallucination_rate)],
    ['P95 Latency (sec)', fmtNum(m.p95_latency_sec)],
    ['Safety Violations', String(m.safety_violations ?? 0)],
  ];

  return (
    <>
      <h2 className="page-title">Executive AI Health</h2>
      <p className="page-desc">Leadership overview — trust, cost, latency, safety (matches notebook §1)</p>
      <DataGapsBanner gaps={bundle?.data_gaps} />

      <table className="kpi-table">
        <thead>
          <tr><th>KPI</th><th>Value</th></tr>
        </thead>
        <tbody>
          {kpiRows.map(([k, v]) => (
            <tr key={k}><td>{k}</td><td>{v}</td></tr>
          ))}
        </tbody>
      </table>

      <div className="kpi-grid">
        <KpiCard title="AI Health Score" value={fmtNum(m.ai_health_score, 1)} />
        <KpiCard title="Pass Rate" value={fmtPct(m.pass_rate)} />
        <KpiCard title="Total Cost" value={fmtUsd(m.total_cost_usd)} />
        <KpiCard title="Avg Latency" value={`${fmtNum(m.avg_latency_sec)}s`} />
        <KpiCard title="Sessions" value={cov.unique_sessions ?? m.total_sessions} subtitle={`${cov.turn_rows ?? m.total_turns} turns`} />
        <KpiCard title="Gate Pass Rate" value={fmtPct(m.gate_pass_rate)} />
      </div>

      <div className="chart-grid">
        <ChartCard title="Pass / fail sessions">
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie
                data={charts.pass_fail_counts || [
                  { status: 'pass', count: m.total_passed },
                  { status: 'fail', count: m.total_failed },
                ]}
                dataKey="count"
                nameKey="status"
                cx="50%"
                cy="50%"
                outerRadius={80}
                label
              >
                {(charts.pass_fail_counts || []).map((_, i) => (
                  <Cell key={i} fill={PIE_COLORS[i % PIE_COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Latency distribution">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.latency_histogram || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Latency by session type">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.latency_by_session_type || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="session_type" tick={{ fill: '#8b9cb3', fontSize: 10 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="p95" fill="#f59e0b" name="P95" />
              <Bar dataKey="mean" fill="#22c55e" name="Mean" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Gate pass rate by session type">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.gate_pass_by_session_type || []}>
              <XAxis dataKey="session_type" tick={{ fill: '#8b9cb3', fontSize: 10 }} />
              <YAxis domain={[0, 1]} tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="gate_pass_rate" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}
