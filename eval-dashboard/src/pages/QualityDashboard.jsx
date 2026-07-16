import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataGapsBanner from '../components/DataGapsBanner';
import { fmtNum, fmtPct } from '../hooks/useDashboardBundle';

export default function QualityDashboard({ bundle }) {
  const m = bundle?.run_metrics || {};
  const charts = bundle?.charts || {};

  return (
    <>
      <h2 className="page-title">Quality Analytics</h2>
      <p className="page-desc">Groundedness, correctness, judge distributions (notebook §4)</p>
      <DataGapsBanner gaps={bundle?.data_gaps?.filter((g) => g.includes('validation'))} />
      <div className="kpi-grid">
        <KpiCard title="Avg Trace Judge" value={fmtNum(m.avg_trace_judge_final_score)} />
        <KpiCard title="Avg Batch Correctness" value={fmtNum(m.avg_batch_judge_correctness)} />
        <KpiCard title="Groundedness" value={fmtNum(m.avg_groundedness_score)} />
        <KpiCard title="Correctness" value={fmtNum(m.avg_correctness_score)} />
        <KpiCard title="Gate Pass Rate" value={fmtPct(m.gate_pass_rate)} />
        <KpiCard title="Unsupported Claims" value={fmtPct(m.unsupported_claim_rate)} />
      </div>
      <div className="chart-grid">
        <ChartCard title="Groundedness distribution">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.groundedness_histogram || []}>
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Correctness distribution">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.correctness_histogram || []}>
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#3b82f6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Final judge score distribution">
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={charts.final_judge_histogram || []}>
              <XAxis dataKey="bin" tick={{ fill: '#8b9cb3', fontSize: 9 }} />
              <YAxis tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="count" fill="#8b5cf6" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        <ChartCard title="Judge score by category">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={charts.judge_scores_by_category || []}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="category" tick={{ fill: '#8b9cb3', fontSize: 9 }} angle={-20} textAnchor="end" height={70} />
              <YAxis domain={[0, 1]} tick={{ fill: '#8b9cb3' }} />
              <Tooltip contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }} />
              <Bar dataKey="avg_trace_judge" fill="#22c55e" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        {(charts.failure_by_type || []).length > 0 && (
          <ChartCard title="Validator failure types (unique turns)">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={charts.failure_by_type} layout="vertical">
                <XAxis type="number" tick={{ fill: '#8b9cb3' }} allowDecimals={false} />
                <YAxis type="category" dataKey="failure_type" width={180} tick={{ fill: '#8b9cb3', fontSize: 8 }} />
                <Tooltip
                  contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }}
                  formatter={(v) => [`${v} turns`, 'Affected']}
                />
                <Bar dataKey="turns" fill="#ef4444" />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        )}
      </div>
    </>
  );
}
