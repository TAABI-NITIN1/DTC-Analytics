import {
  Bar, BarChart, CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataGapsBanner from '../components/DataGapsBanner';
import { fmtNum } from '../hooks/useDashboardBundle';

export default function SpeedDashboard({ bundle }) {
  const m = bundle?.run_metrics || {};
  const trace = bundle?.trace_metrics || {};
  const charts = bundle?.charts || {};

  return (
    <>
      <h2 className="page-title">Speed Analytics</h2>
      <p className="page-desc">Latency ECDF, P95, graph nodes, SQL timing (notebook §3)</p>
      <DataGapsBanner gaps={bundle?.data_gaps?.filter((g) => g.includes('trace') || g.includes('sql'))} />
      <div className="kpi-grid">
        <KpiCard title="Avg Latency" value={`${fmtNum(m.avg_latency_sec)}s`} />
        <KpiCard title="P50" value={`${fmtNum(m.p50_latency_sec)}s`} />
        <KpiCard title="P95" value={`${fmtNum(m.p95_latency_sec)}s`} />
        <KpiCard title="P99" value={`${fmtNum(m.p99_latency_sec)}s`} />
        <KpiCard title="Avg SQL Latency" value={`${fmtNum(m.avg_sql_latency_sec || trace.avg_sql_latency_sec)}s`} />
        <KpiCard title="P95 SQL" value={`${fmtNum(trace.p95_sql_latency_sec)}s`} />
        <KpiCard title="Timeout Rate" value={`${fmtNum((m.timeout_rate || 0) * 100, 2)}%`} />
      </div>
      <div className="chart-grid">
        <ChartCard title="Latency ECDF">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={charts.latency_ecdf || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="latency_sec" tick={{ fill: '#8b9cb3' }} name="sec" />
              <YAxis tick={{ fill: '#8b9cb3' }} unit="%" />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Line type="monotone" dataKey="pct" stroke="#3b82f6" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Latency histogram">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={charts.latency_histogram || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="P95 by session type">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={charts.latency_by_session_type || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="session_type" tick={{ fill: '#8b9cb3' }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Line type="monotone" dataKey="p95" stroke="#f59e0b" strokeWidth={2} />
              <Line type="monotone" dataKey="mean" stroke="#22c55e" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
      {trace.slowest_nodes?.length > 0 && (
        <ChartCard title="Slowest graph nodes">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={trace.slowest_nodes}>
              <XAxis dataKey="node" tick={{ fill: '#8b9cb3', fontSize: 10 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="avg_duration_sec" fill="#ef4444" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}
      {trace.slowest_sql?.length > 0 && (
        <ChartCard title="Slowest SQL (sample)">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={(trace.slowest_sql || []).slice(0, 15)}>
              <XAxis dataKey="sql_hash" tick={{ fill: '#8b9cb3', fontSize: 8 }} hide />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="duration_sec" fill="#f97316" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      )}
    </>
  );
}
