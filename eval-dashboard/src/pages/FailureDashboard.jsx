import {
  Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import ChartCard from '../components/ChartCard';
import DataTable from '../components/DataTable';
import DataGapsBanner from '../components/DataGapsBanner';

function fmtPct(n, total) {
  if (!total) return '0%';
  return `${((n / total) * 100).toFixed(1)}%`;
}

export default function FailureDashboard({ bundle }) {
  const fail = bundle?.failure_analytics || {};
  const m = bundle?.run_metrics || {};
  const charts = bundle?.charts || {};
  const totalTurns = m.total_turns || 0;
  const totalSessions = m.total_sessions || 0;

  const runtimeTurns = fail.runtime_failure_turns ?? m.total_failures ?? 0;
  const turnsAny = fail.turns_with_any_flag ?? 0;
  const turnsHall = fail.turns_with_hallucination_flag ?? 0;
  const sessionsFailed = m.total_failed ?? 0;

  const byCat = charts.failure_by_category?.length
    ? charts.failure_by_category
    : Object.entries(fail.by_category_turns || fail.by_category || {}).map(([category, count]) => ({
      category,
      turns: count,
      count,
    }));

  const byType = charts.failure_by_type || [];

  return (
    <>
      <h2 className="page-title">Failure Analytics</h2>
      <p className="page-desc">Runtime issues and validator flags by turn and session (notebook §7)</p>
      <DataGapsBanner gaps={bundle?.data_gaps?.filter((g) => g.includes('validation') || g === 'sql_events_empty')} />

      <div className="kpi-grid">
        <KpiCard
          title="Runtime issue turns"
          value={runtimeTurns}
          subtitle={`${fmtPct(runtimeTurns, totalTurns)} of ${totalTurns} turns`}
        />
        <KpiCard
          title="Failed sessions"
          value={sessionsFailed}
          subtitle={`${fmtPct(sessionsFailed, totalSessions)} of ${totalSessions} sessions`}
        />
        <KpiCard
          title="Turns with validator flags"
          value={turnsAny}
          subtitle={`${fmtPct(turnsAny, totalTurns)} of turns`}
        />
        <KpiCard
          title="Turns with hallucination flags"
          value={turnsHall}
          subtitle={`${fmtPct(turnsHall, totalTurns)} of turns`}
        />
        <KpiCard
          title="Sessions flagged (validator)"
          value={fail.critical_session_count ?? 0}
          subtitle="Critical or human-review flag"
        />
        <KpiCard
          title="Safety-flag turns"
          value={fail.turns_with_safety_flag ?? 0}
          subtitle={`${fmtPct(fail.turns_with_safety_flag ?? 0, totalTurns)} of turns`}
        />
      </div>

      <div className="chart-grid">
        <ChartCard title="Turns affected by category">
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={byCat}>
              <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
              <XAxis dataKey="category" tick={{ fill: '#8b9cb3' }} />
              <YAxis tick={{ fill: '#8b9cb3' }} allowDecimals={false} />
              <Tooltip
                contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }}
                formatter={(v) => [`${v} turns`, 'Affected']}
              />
              <Bar dataKey="turns" fill="#ef4444" name="turns" />
            </BarChart>
          </ResponsiveContainer>
        </ChartCard>
        {byType.length > 0 && (
          <ChartCard title="Top failure types">
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={byType} layout="vertical">
                <XAxis type="number" tick={{ fill: '#8b9cb3' }} allowDecimals={false} />
                <YAxis type="category" dataKey="failure_type" width={200} tick={{ fill: '#8b9cb3', fontSize: 8 }} />
                <Tooltip
                  contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }}
                  formatter={(v) => [`${v} turns`, 'Affected']}
                />
                <Bar dataKey="turns" fill="#f97316" name="turns" />
              </BarChart>
            </ResponsiveContainer>
          </ChartCard>
        )}
      </div>
      <ChartCard title="High-risk sessions">
        <DataTable
          columns={[
            { key: 'session_id', label: 'Session' },
            { key: 'session_pass_fail', label: 'Status' },
            { key: 'session_score', label: 'Score' },
            { key: 'scenario_category', label: 'Category' },
          ]}
          rows={bundle?.tables?.high_risk_sessions?.length
            ? bundle.tables.high_risk_sessions
            : (fail.critical_sessions || []).map((id) => ({ session_id: id }))}
        />
      </ChartCard>
      {bundle?.experiment_compare?.regression_alerts?.length > 0 && (
        <div className="gaps-banner">
          <strong>Regression alerts vs baseline:</strong>{' '}
          {bundle.experiment_compare.regression_alerts.map((a) => a.metric).join(', ')}
        </div>
      )}
    </>
  );
}
