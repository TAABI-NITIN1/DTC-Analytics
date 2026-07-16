import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Scatter, ScatterChart, Tooltip, XAxis, YAxis, ZAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataTable from '../components/DataTable';
import { fmtNum, fmtUsd } from '../hooks/useDashboardBundle';

export default function CostDashboard({ bundle }) {
  const m = bundle?.run_metrics || {};
  const charts = bundle?.charts || {};
  const scatter = (charts.token_growth_scatter || []).map((r) => ({
    ...r,
    tokens: Number(r.tokens) || 0,
    turns: Number(r.turns) || 0,
  }));

  return (
    <>
      <h2 className="page-title">Cost Analytics</h2>
      <p className="page-desc">AI economics — tokens and USD by segment (notebook §2)</p>
      <div className="kpi-grid">
        <KpiCard title="Total Cost" value={fmtUsd(m.total_cost_usd)} />
        <KpiCard title="Cost / Turn" value={fmtUsd(m.avg_cost_per_turn_usd)} />
        <KpiCard title="Cost / Session" value={fmtUsd(m.avg_cost_per_session_usd)} />
        <KpiCard title="Total Tokens" value={fmtNum(m.total_tokens, 0)} />
        <KpiCard title="Tokens / Turn" value={fmtNum(m.avg_tokens_per_turn, 0)} />
      </div>
      <div className="chart-grid">
        <ChartCard title="Cost per conversation (histogram)">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={charts.cost_per_session_histogram || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Avg cost by scenario category">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={charts.avg_cost_by_scenario || []} layout="vertical">
              <XAxis type="number" tick={{ fill: '#8b9cb3' }} />
              <YAxis type="category" dataKey="scenario_category" width={140} tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="usd" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Cost by category (turns)">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={(charts.cost_by_category || []).slice(0, 12)} layout="vertical">
              <XAxis type="number" tick={{ fill: '#8b9cb3' }} />
              <YAxis type="category" dataKey="category" width={120} tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="usd" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Cost by customer">
          <ResponsiveContainer width="100%" height={240}>
            <BarChart data={charts.cost_by_customer || []} layout="vertical">
              <XAxis type="number" tick={{ fill: '#8b9cb3' }} />
              <YAxis type="category" dataKey="customer" width={140} tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="usd" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Tokens vs turns per session">
          <ResponsiveContainer width="100%" height={240}>
            <ScatterChart>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis type="number" dataKey="turns" tick={{ fill: '#8b9cb3' }} />
              <YAxis type="number" dataKey="tokens" tick={{ fill: '#8b9cb3' }} />
              <ZAxis range={[40, 40]} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Scatter data={scatter} fill="#3b82f6" fillOpacity={0.6} />
            </ScatterChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
      <ChartCard title="Most expensive sessions">
        <DataTable
          columns={[
            { key: 'session_id', label: 'Session' },
            { key: 'scenario_category', label: 'Category' },
            { key: 'total_session_cost_usd', label: 'Cost', render: (r) => fmtUsd(r.total_session_cost_usd) },
            { key: 'total_session_tokens', label: 'Tokens' },
          ]}
          rows={bundle?.tables?.most_expensive_sessions}
        />
      </ChartCard>
    </>
  );
}
