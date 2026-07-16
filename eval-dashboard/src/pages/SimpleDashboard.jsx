import { useState } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, PieChart, Pie, Cell,
} from 'recharts';

function grade(score) {
  if (score >= 90) return { letter: 'A', label: 'Excellent', color: '#22c55e', emoji: '🌟' };
  if (score >= 80) return { letter: 'B', label: 'Good', color: '#3b82f6', emoji: '👍' };
  if (score >= 70) return { letter: 'C', label: 'Fair', color: '#f59e0b', emoji: '📋' };
  if (score >= 60) return { letter: 'D', label: 'Needs work', color: '#f97316', emoji: '⚠️' };
  return { letter: 'F', label: 'Poor', color: '#ef4444', emoji: '❌' };
}

function pct(v) {
  if (v == null || Number.isNaN(v)) return '—';
  return `${(v * 100).toFixed(0)}%`;
}

function SimpleCard({ emoji, title, big, small, hint, color }) {
  return (
    <div className="sd-metric-card">
      <div className="sd-metric-icon">{emoji}</div>
      <div className="sd-metric-body">
        <div className="sd-metric-title">{title}</div>
        <div className="sd-metric-value" style={{ color: color || '#e8edf4' }}>{big}</div>
        <div className="sd-metric-sub">{small}</div>
        {hint && <div className="sd-metric-explain">{hint}</div>}
      </div>
    </div>
  );
}

export default function SimpleDashboard({ combined }) {
  const [showEngineer, setShowEngineer] = useState(false);
  const bundle = combined?.report || combined;
  const m = bundle?.run_metrics || {};
  const charts = bundle?.charts || {};
  const narrative = combined?.narrative || {};
  const earlier = combined?.earlier_checks || [];
  const cov = bundle?.coverage || {};
  const dataGaps = bundle?.data_gaps || [];
  const updated = combined?.generated_at
    ? new Date(combined.generated_at).toLocaleString()
    : null;

  const g = grade(m.ai_health_score || 0);
  const passPct = m.pass_rate || 0;
  const passColor = passPct >= 0.7 ? '#22c55e' : passPct >= 0.5 ? '#f59e0b' : '#ef4444';
  const totalQ = m.total_sessions || 0;
  const passed = m.total_passed || 0;
  const failed = m.total_failed || 0;

  const donutData = [
    { name: 'Answered well', value: passed },
    { name: 'Needs improvement', value: failed },
  ];

  const categoryChart = (charts.judge_scores_by_category || []).slice(0, 8).map((row) => ({
    name: (row.category || '').replace('Fleet ', '').slice(0, 18),
    score: Math.round((row.avg_trace_judge || 0) * 100),
  }));

  const good = [];
  const watch = [];

  if ((m.hallucination_rate || 0) === 0) good.push('The AI did not invent fake numbers or facts.');
  if ((m.safety_violations || 0) === 0) good.push('Every answer was safe — nothing harmful or inappropriate.');
  if (passPct >= 0.5) good.push(`About ${pct(passPct)} of questions got a good enough answer.`);
  if ((m.avg_correctness_score || 0) >= 0.85) good.push('Answers were accurate and on-topic most of the time.');
  if ((m.avg_latency_sec || 0) > 0 && m.avg_latency_sec < 45) {
    good.push(`Typical wait time is around ${Math.round(m.avg_latency_sec)} seconds per question.`);
  }
  if (!good.length) good.push('The system completed all tests without crashing.');

  if (passPct < 0.7) watch.push(`We want at least 70% of questions passed — currently ${pct(passPct)}.`);
  if ((m.ai_health_score || 0) < 80) watch.push('Overall health score can be improved with better answers and fewer errors.');
  if ((m.avg_latency_sec || 0) > 40) watch.push('Some questions take a long time to answer — users may get impatient.');
  if (cov.missing_turns_vs_catalog > 0) {
    watch.push(`${cov.missing_turns_vs_catalog} conversations did not finish all planned follow-up questions.`);
  }

  return (
    <div className="sd-wrapper">
      <section className="sd-how-to-read">
        <strong>How to read this page:</strong> One overall grade (like a report card), then how many questions
        the AI answered well, what went right, and what to fix. Everything below is from our{' '}
        <strong>full live test</strong> — not mixed with smaller practice runs.
        {updated && <span className="sd-updated"> · Updated {updated}</span>}
      </section>

      <header className="sd-header">
        <div>
          <h1 className="sd-title">How is our Fleet AI doing?</h1>
          <p className="sd-subtitle">
            One simple report · {totalQ.toLocaleString()} real questions · Live production system
          </p>
        </div>
        <div className="sd-overall-pill" style={{ borderColor: g.color }}>
          <span className="sd-pill-grade" style={{ color: g.color }}>{g.emoji} {g.letter}</span>
          <span className="sd-pill-text">{g.label}</span>
        </div>
      </header>

      <section className="sd-story-box">
        <h2>What is this report?</h2>
        <p>{narrative.headline || 'We tested the fleet AI with many real questions.'}</p>
        <p>{narrative.what_we_did || 'Each answer was checked for being correct, safe, and helpful.'}</p>
        <p className="sd-story-verdict">
          <strong>Bottom line:</strong> The AI is <span style={{ color: g.color }}>{narrative.overall_verdict || g.label}</span>
          {' '}(score {Math.round(m.ai_health_score || 0)} out of 100).
        </p>
      </section>

      <section className="sd-hero">
        <div className="sd-hero-left">
          <div className="sd-big-grade" style={{ color: g.color }}>{g.letter}</div>
          <div className="sd-big-score">
            {Math.round(m.ai_health_score || 0)}
            <span className="sd-big-score-max"> / 100</span>
          </div>
          <div className="sd-big-label" style={{ color: g.color }}>Overall grade — like a school report card</div>
          <p className="sd-big-desc">
            Higher is better. Above 80 is good for production use. This score mixes: Did it answer correctly?
            Was it safe? Did the system stay reliable?
          </p>
        </div>
        <div className="sd-hero-right">
          <ResponsiveContainer width="100%" height={200}>
            <PieChart>
              <Pie data={donutData} cx="50%" cy="50%" innerRadius={55} outerRadius={80} paddingAngle={4} dataKey="value">
                <Cell fill="#22c55e" />
                <Cell fill="#ef4444" />
              </Pie>
              <Tooltip
                contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d', borderRadius: 8 }}
                formatter={(v, name) => [`${v} questions`, name]}
              />
            </PieChart>
          </ResponsiveContainer>
          <div className="sd-donut-legend">
            <span style={{ color: '#22c55e' }}>● {passed.toLocaleString()} good</span>
            <span style={{ color: '#ef4444' }}>● {failed.toLocaleString()} weak</span>
          </div>
        </div>
      </section>

      <h2 className="sd-section-title">The four numbers that matter</h2>
      <div className="sd-metrics-grid">
        <SimpleCard
          emoji="📋"
          title="Questions we asked"
          big={totalQ.toLocaleString()}
          small="Real fleet scenarios (health, faults, maintenance)"
          hint="This was the full benchmark — not a tiny demo."
          color="#3b82f6"
        />
        <SimpleCard
          emoji={passPct >= 0.7 ? '✅' : '⚠️'}
          title="Got it right"
          big={pct(passPct)}
          small={`${passed.toLocaleString()} out of ${totalQ.toLocaleString()} questions`}
          hint="The #1 number leadership should look at."
          color={passColor}
        />
        <SimpleCard
          emoji="🎯"
          title="Answer quality"
          big={pct(m.avg_correctness_score)}
          small="How helpful and accurate answers were"
          hint="Graded by an automatic checker, like a teacher marking homework."
        />
        <SimpleCard
          emoji="🔒"
          title="Safety"
          big={(m.safety_violations || 0) === 0 ? 'All clear' : `${m.safety_violations} issues`}
          small={(m.safety_violations || 0) === 0 ? 'No dangerous advice' : 'Review needed'}
          color={(m.safety_violations || 0) === 0 ? '#22c55e' : '#ef4444'}
        />
      </div>

      <div className="sd-two-col">
        <div className="sd-insight-card sd-insight-good">
          <div className="sd-insight-heading">✅ What went well</div>
          <ul className="sd-insight-list">
            {good.map((p, i) => (
              <li key={i}>{p}</li>
            ))}
          </ul>
        </div>
        <div className="sd-insight-card sd-insight-warn">
          <div className="sd-insight-heading">📌 What to improve</div>
          {watch.length === 0 ? (
            <p style={{ color: '#8b9cb3' }}>Nothing major flagged.</p>
          ) : (
            <ul className="sd-insight-list">
              {watch.map((p, i) => (
                <li key={i}>{p}</li>
              ))}
            </ul>
          )}
        </div>
      </div>

      {categoryChart.length > 0 && (
        <>
          <h2 className="sd-section-title">Which topics did best?</h2>
          <p className="sd-section-desc">
            Score out of 100 by type of question (higher bar = AI handled that topic better).
          </p>
          <div className="sd-chart-card">
            <ResponsiveContainer width="100%" height={260}>
              <BarChart data={categoryChart} margin={{ top: 8, right: 16, left: 0, bottom: 40 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#2d3a4d" />
                <XAxis dataKey="name" tick={{ fill: '#8b9cb3', fontSize: 11 }} angle={-20} textAnchor="end" height={50} />
                <YAxis domain={[0, 100]} tick={{ fill: '#8b9cb3' }} unit="%" />
                <Tooltip
                  contentStyle={{ background: '#1a2332', border: '1px solid #2d3a4d' }}
                  formatter={(v) => [`${v}%`, 'Score']}
                />
                <Bar dataKey="score" fill="#3b82f6" radius={[6, 6, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {(m.avg_latency_sec || 0) > 0 && (
        <>
          <h2 className="sd-section-title">How long do users wait?</h2>
          <p className="sd-section-desc">Time to get an answer after asking a question.</p>
          <div className="sd-speed-grid">
            <div className="sd-speed-card sd-speed-highlight">
              <div className="sd-speed-big">{Math.round(m.avg_latency_sec)}s</div>
              <div className="sd-speed-label">Average wait</div>
              <p className="sd-speed-note">
                {m.avg_latency_sec < 25
                  ? 'Usually feels quick for analytics questions.'
                  : 'Some complex questions take longer — worth optimizing.'}
              </p>
            </div>
            <div className="sd-speed-card">
              <div className="sd-stat-row">
                <span className="sd-stat-label">Typical (half of questions)</span>
                <span className="sd-stat-value">{Math.round(m.p50_latency_sec || 0)}s</span>
              </div>
              <div className="sd-stat-row">
                <span className="sd-stat-label">Slowest 5%</span>
                <span className="sd-stat-value">{Math.round(m.p95_latency_sec || 0)}s</span>
              </div>
            </div>
          </div>
        </>
      )}

      {(m.total_cost_usd || 0) > 0 && (
        <>
          <h2 className="sd-section-title">What did testing cost?</h2>
          <div className="sd-metrics-grid">
            <SimpleCard
              emoji="💵"
              title="Total test cost"
              big={`$${(m.total_cost_usd || 0).toFixed(2)}`}
              small="For all 1,000 questions (AI usage fees)"
              hint="Usually pennies per question — scales with volume in production."
              color="#f59e0b"
            />
            <SimpleCard
              emoji="🧮"
              title="Cost per question"
              big={`$${(m.avg_cost_per_session_usd || 0).toFixed(4)}`}
              small="Average per conversation"
            />
          </div>
        </>
      )}

      {earlier.length > 0 && (
        <section className="sd-earlier-box">
          <h2 className="sd-section-title" style={{ marginTop: 0 }}>Earlier small checks (optional)</h2>
          <p className="sd-section-desc">
            Before the big 1,000-question test, we ran smaller checks on a developer machine. Those are
            <strong> not</strong> added into the numbers above — they are just for context.
          </p>
          <ul className="sd-earlier-list">
            {earlier.map((e, i) => (
              <li key={i}>
                <strong>{e.label}</strong> — {e.questions} questions, {e.pass_rate_pct}% passed,
                health {e.health_score}/100. <span className="sd-muted">{e.note}</span>
              </li>
            ))}
          </ul>
        </section>
      )}

      <h2 className="sd-section-title">Words explained simply</h2>
      <div className="sd-glossary-grid">
        <div className="sd-glossary-item">
          <div className="sd-glossary-term">Pass rate</div>
          <div className="sd-glossary-def">How often the AI gave an answer good enough to use. Like a test score.</div>
        </div>
        <div className="sd-glossary-item">
          <div className="sd-glossary-term">Health score</div>
          <div className="sd-glossary-def">One overall grade from 0–100 combining quality, safety, and reliability.</div>
        </div>
        <div className="sd-glossary-item">
          <div className="sd-glossary-term">Hallucination</div>
          <div className="sd-glossary-def">When the AI makes up facts. We want zero of these.</div>
        </div>
        <div className="sd-glossary-item">
          <div className="sd-glossary-term">Response time</div>
          <div className="sd-glossary-def">Seconds from asking a question to getting the answer.</div>
        </div>
      </div>

      {dataGaps.length > 0 && (
        <section className="sd-engineer-fold">
          <button
            type="button"
            className="sd-engineer-toggle"
            onClick={() => setShowEngineer((v) => !v)}
          >
            {showEngineer ? '▾' : '▸'} For engineers (technical notes)
          </button>
          {showEngineer && (
            <ul className="sd-engineer-list">
              {dataGaps.map((g) => (
                <li key={g}>{g.replace(/_/g, ' ')}</li>
              ))}
              {combined?.runs_included?.length > 0 && (
                <li>Runs scanned: {combined.runs_included.join(', ')}</li>
              )}
            </ul>
          )}
        </section>
      )}

      <footer className="sd-footer">
        Taabi Fleet AI · Full production benchmark · Easy report for the whole team
      </footer>
    </div>
  );
}
