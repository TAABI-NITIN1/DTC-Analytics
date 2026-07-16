import {
  Bar, BarChart, CartesianGrid, Line, LineChart, Pie, PieChart, Cell, ResponsiveContainer, Tooltip, XAxis, YAxis, Legend,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import { fmtNum } from '../hooks/useDashboardBundle';

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#8b5cf6'];

export default function ConversationDashboard({ bundle }) {
  const cov = bundle?.coverage || {};

  return (
    <>
      <h2 className="page-title">Conversation Analytics</h2>
      <p className="page-desc">Multi-turn reliability and follow-up behavior</p>
      <div className="kpi-grid">
        <KpiCard title="Sessions" value={cov.unique_sessions} />
        <KpiCard title="Turn rows" value={cov.turn_rows} />
        <KpiCard title="Missing turns vs catalog" value={cov.missing_turns_vs_catalog} subtitle="coverage gap" />
      </div>
      <div className="chart-grid">
        <ChartCard title="Score vs turn index">
          <ResponsiveContainer width="100%" height={240}>
            <LineChart data={bundle?.charts?.turn_score_by_turn_index || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="turn_index" tick={{ fill: '#8b9cb3' }} />
              <YAxis domain={[0, 1]} tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Line type="monotone" dataKey="avg_score" stroke="#3b82f6" strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Follow-up source mix">
          <ResponsiveContainer width="100%" height={240}>
            <PieChart>
              <Pie data={bundle?.charts?.follow_up_source_mix || []} dataKey="count" nameKey="source" cx="50%" cy="50%" outerRadius={80} label>
                {(bundle?.charts?.follow_up_source_mix || []).map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </ChartCard>
      </div>
    </>
  );
}
