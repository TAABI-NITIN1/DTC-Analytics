import { useEffect, useMemo, useState } from 'react';
import { useDashboardBundle, useManifest } from './hooks/useDashboardBundle';
import ExecutiveDashboard from './pages/ExecutiveDashboard';
import CostDashboard from './pages/CostDashboard';
import SpeedDashboard from './pages/SpeedDashboard';
import QualityDashboard from './pages/QualityDashboard';
import ConversationDashboard from './pages/ConversationDashboard';
import ToolSqlDashboard from './pages/ToolSqlDashboard';
import FailureDashboard from './pages/FailureDashboard';
import ExperimentDashboard from './pages/ExperimentDashboard';
import SimpleDashboard from './pages/SimpleDashboard';

const PAGES = [
  { id: 'executive', label: '1 — Executive Health' },
  { id: 'cost', label: '2 — Cost' },
  { id: 'speed', label: '3 — Speed' },
  { id: 'quality', label: '4 — Quality' },
  { id: 'conversation', label: '5 — Conversation' },
  { id: 'toolsql', label: '6 — Tool & SQL' },
  { id: 'failure', label: '7 — Failure' },
  { id: 'experiment', label: '8 — Experiment' },
];

function PageContent({ page, bundle, allBundles }) {
  switch (page) {
    case 'executive': return <ExecutiveDashboard bundle={bundle} />;
    case 'cost': return <CostDashboard bundle={bundle} />;
    case 'speed': return <SpeedDashboard bundle={bundle} />;
    case 'quality': return <QualityDashboard bundle={bundle} />;
    case 'conversation': return <ConversationDashboard bundle={bundle} />;
    case 'toolsql': return <ToolSqlDashboard bundle={bundle} />;
    case 'failure': return <FailureDashboard bundle={bundle} />;
    case 'experiment': return <ExperimentDashboard bundle={bundle} allBundles={allBundles} />;
    default: return null;
  }
}

export default function App() {
  const { manifest } = useManifest();
  const [viewMode, setViewMode] = useState('detailed');
  const [runId, setRunId] = useState('');
  const [page, setPage] = useState('executive');
  const { bundle, loading, error } = useDashboardBundle(runId);
  const [allBundles, setAllBundles] = useState([]);
  const [combined, setCombined] = useState(null);

  useEffect(() => {
    if (manifest.length && !runId) {
      const primary = manifest.find((m) => m.run_id?.includes('085108')) || manifest[0];
      setRunId(primary.run_id);
    }
  }, [manifest, runId]);

  useEffect(() => {
    if (!manifest.length) return;
    Promise.all(
      manifest.map((m) =>
        fetch(`./runs/${m.run_id}.json`).then((r) => (r.ok ? r.json() : null)),
      ),
    ).then((rows) => setAllBundles(rows.filter(Boolean)));
    fetch('./runs/combined_report.json')
      .then((r) => (r.ok ? r.json() : null))
      .then(setCombined)
      .catch(() => setCombined(null));
  }, [manifest]);

  const activeLabel = useMemo(
    () => manifest.find((m) => m.run_id === runId)?.label || runId,
    [manifest, runId],
  );

  if (viewMode === 'simple') {
    return (
      <div>
        <div className="view-mode-bar">
          <button type="button" className="view-mode-btn active" onClick={() => setViewMode('simple')}>
            Executive summary
          </button>
          <button type="button" className="view-mode-btn" onClick={() => setViewMode('detailed')}>
            Detailed dashboards (8 views)
          </button>
        </div>
        <SimpleDashboard combined={combined} />
      </div>
    );
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <h1>Taabi AI Eval Analytics</h1>
        <div className="view-mode-inline">
          <button type="button" className="view-mode-btn-sm" onClick={() => setViewMode('simple')}>
            Simple summary
          </button>
        </div>
        <label htmlFor="run-select">Evaluation run</label>
        <select
          id="run-select"
          value={runId}
          onChange={(e) => setRunId(e.target.value)}
        >
          {manifest.map((m) => (
            <option key={m.run_id} value={m.run_id}>{m.label || m.run_id}</option>
          ))}
        </select>
        <nav>
          {PAGES.map((p) => (
            <button
              key={p.id}
              type="button"
              className={`nav-btn ${page === p.id ? 'active' : ''}`}
              onClick={() => setPage(p.id)}
            >
              {p.label}
            </button>
          ))}
        </nav>
        <p className="sidebar-foot">
          {activeLabel}
        </p>
      </aside>
      <main className="main">
        {loading && <div className="loading">Loading dashboard bundle…</div>}
        {error && <div className="error">{error}</div>}
        {!loading && !error && bundle && (
          <PageContent page={page} bundle={bundle} allBundles={allBundles} />
        )}
        {!loading && !manifest.length && (
          <div className="error">
            No runs in public/runs/manifest.json. Export first:
            <pre className="cmd-hint">
              python -m evaluation.analytics export-dashboard --run-id eval_20260520_085108_190cf6 --copy-to-eval-dashboard
            </pre>
          </div>
        )}
      </main>
    </div>
  );
}
