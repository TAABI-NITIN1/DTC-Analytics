import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataGapsBanner from '../components/DataGapsBanner';
import { fmtNum } from '../hooks/useDashboardBundle';

export default function ToolSqlDashboard({ bundle }) {
  const m = bundle?.run_metrics || {};
  const trace = bundle?.trace_metrics || {};

  return (
    <>
      <h2 className="page-title">Tool & SQL Analytics</h2>
      <p className="page-desc">Orchestration — tools, queries, efficiency</p>
      <DataGapsBanner gaps={bundle?.data_gaps?.filter((g) => g.includes('sql'))} />
      <div className="kpi-grid">
        <KpiCard title="Tool Calls" value={m.total_tool_calls} />
        <KpiCard title="SQL Queries" value={m.total_sql_queries} />
        <KpiCard title="Repeated SQL hashes" value={trace.repeated_sql_hash_count ?? 0} />
        <KpiCard title="Graph Failure Rate" value={`${fmtNum((m.graph_failure_rate || 0) * 100, 2)}%`} />
      </div>
      <div className="chart-grid">
        <ChartCard title="Tool usage frequency">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={(bundle?.charts?.tool_usage || []).slice(0, 15)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis type="number" tick={{ fill: '#8b9cb3' }} />
              <YAxis type="category" dataKey="tool" width={160} tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="SQL success distribution">
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={bundle?.charts?.sql_success_distribution || []}>
              <XAxis dataKey="bucket" tick={{ fill: '#8b9cb3', fontSize: 10 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}
