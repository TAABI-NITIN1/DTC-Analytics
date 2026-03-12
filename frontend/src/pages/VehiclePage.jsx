import { gql, useQuery } from '@apollo/client';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
  BarChart,
  Bar,
} from 'recharts';
import DataTable from '../components/DataTable';

const VEHICLE_QUERY = gql`
  query VehicleDashboard($uniqueid: String!, $days: Int!, $customerName: String) {
    vehicleOverview(uniqueid: $uniqueid) {
      uniqueid
      vehicleNumber
      vehicleModel
      vehicleType
      customerName
      healthScore
      activeFaultCount
      criticalFaultCount
    }
    vehicleFaults(uniqueid: $uniqueid, days: $days, limit: 30, customerName: $customerName) {
      dtcCode
      episodeCount
      activeEpisodes
      maxSeverity
      daysPersistence
    }
    vehicleTimeline(uniqueid: $uniqueid, days: $days, customerName: $customerName) {
      eventDate
      activeEpisodes
      criticalEpisodes
    }
    vehicleHealthDetail(uniqueid: $uniqueid) {
      uniqueid
      vehicleHealthScore
      activeFaultCount
      totalEpisodes
      episodesLast30Days
      driverRelatedFaults
      mostCommonDtc
      hasEngineIssue
      hasEmissionIssue
      hasSafetyIssue
      hasElectricalIssue
    }
  }
`;

const severityLabel = (level) => {
  if (level >= 4) return 'Critical';
  if (level === 3) return 'High';
  if (level === 2) return 'Moderate';
  return 'Minor';
};

const severityBadge = (level) => {
  const cls = level >= 3 ? 'badge-critical' : level === 2 ? 'badge-moderate' : 'badge-minor';
  return <span className={`badge ${cls}`}>{severityLabel(level)}</span>;
};

const healthColor = (score) => {
  if (score >= 95) return '#16a34a';
  if (score >= 80) return '#d97706';
  return '#dc2626';
};

const healthBg = (score) => {
  if (score >= 95) return '#dcfce7';
  if (score >= 80) return '#fef3c7';
  return '#fee2e2';
};

function VehiclePage({ uniqueid, days, customerName }) {
  const effectiveUniqueid = uniqueid || 'UNKNOWN';
  const { data, loading } = useQuery(VEHICLE_QUERY, {
    variables: { uniqueid: effectiveUniqueid, days, customerName: customerName || null },
    skip: !uniqueid,
  });

  if (!uniqueid) {
    return (
      <div className="card" style={{ textAlign: 'center', padding: 48, color: '#64748b' }}>
        Select a vehicle from the dropdown in the toolbar to view vehicle details.
      </div>
    );
  }

  const overview = data?.vehicleOverview;
  const faults = data?.vehicleFaults ?? [];
  const timeline = data?.vehicleTimeline ?? [];
  const healthDetail = data?.vehicleHealthDetail;
  const score = overview?.healthScore ?? 0;

  const systemFlags = [
    { key: 'hasEngineIssue', label: 'Engine', icon: '🔧', color: '#dc2626' },
    { key: 'hasEmissionIssue', label: 'Emission', icon: '💨', color: '#d97706' },
    { key: 'hasSafetyIssue', label: 'Safety', icon: '⚠️', color: '#dc2626' },
    { key: 'hasElectricalIssue', label: 'Electrical', icon: '⚡', color: '#6366f1' },
  ];

  return (
    <div className="page">
      {loading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading...</div>}

      <div className="card">
        <div className="vehicle-header">
          <div className="vehicle-info">
            <h2 style={{ margin: '0 0 6px' }}>{overview?.vehicleNumber || uniqueid}</h2>
            <div className="vehicle-meta">
              <span>👤 Customer: <strong>{overview?.customerName || '—'}</strong></span>
              <span>📋 Model: <strong>{overview?.vehicleModel || '—'}</strong></span>
              <span>🔧 Type: <strong>{overview?.vehicleType || '—'}</strong></span>
              <span>⚡ Active DTCs: <strong>{overview?.activeFaultCount ?? 0}</strong></span>
              <span>🔴 Critical: <strong>{overview?.criticalFaultCount ?? 0}</strong></span>
            </div>
            {healthDetail && (
              <div style={{ display: 'flex', gap: 8, marginTop: 8, flexWrap: 'wrap' }}>
                {systemFlags.map((f) =>
                  healthDetail[f.key] ? (
                    <span key={f.key} style={{ display: 'inline-flex', alignItems: 'center', gap: 4, padding: '2px 10px', borderRadius: 12, fontSize: 12, fontWeight: 600, background: `${f.color}18`, color: f.color, border: `1px solid ${f.color}40` }}>
                      {f.icon} {f.label}
                    </span>
                  ) : null
                )}
              </div>
            )}
            {healthDetail && (
              <div style={{ display: 'flex', gap: 16, marginTop: 8, fontSize: 13, color: '#475569' }}>
                <span>📊 Episodes (30d): <strong>{healthDetail.episodesLast30Days ?? 0}</strong></span>
                <span>🔁 Total Episodes: <strong>{healthDetail.totalEpisodes ?? 0}</strong></span>
                {healthDetail.mostCommonDtc && <span>🏷️ Top DTC: <strong>{healthDetail.mostCommonDtc}</strong></span>}
              </div>
            )}
          </div>
          <div className="health-badge" style={{ background: healthBg(score) }}>
            <div className="health-badge-value" style={{ color: healthColor(score) }}>{score.toFixed(1)}%</div>
            <div className="health-badge-label" style={{ color: healthColor(score) }}>Health Score</div>
          </div>
        </div>
      </div>

      <div className="chart-grid">
        <div className="card chart-card">
          <h3>DTC Timeline</h3>
          <ResponsiveContainer width="100%" height={260}>
            <LineChart data={timeline}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="eventDate" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="activeEpisodes" name="Active Episodes" stroke="#3b82f6" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="criticalEpisodes" name="Critical Episodes" stroke="#dc2626" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="card chart-card">
          <h3>DTC Distribution</h3>
          <ResponsiveContainer width="100%" height={260}>
            <BarChart data={faults.slice(0, 8)} layout="vertical">
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis type="number" tick={{ fontSize: 11 }} />
              <YAxis dataKey="dtcCode" type="category" tick={{ fontSize: 11 }} width={70} />
              <Tooltip />
              <Legend />
              <Bar dataKey="episodeCount" name="Total Episodes" fill="#3b82f6" radius={[0, 4, 4, 0]} />
              <Bar dataKey="activeEpisodes" name="Active" fill="#dc2626" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      <DataTable
        title="Top Problematic DTC Codes"
        columns={[
          { key: 'dtcCode', label: 'DTC Code' },
          { key: 'episodeCount', label: 'Total Episodes' },
          { key: 'activeEpisodes', label: 'Active' },
          {
            key: 'maxSeverity',
            label: 'Severity',
            render: (v) => {
              const pct = Math.min((v / 5) * 100, 100);
              const color = v >= 3 ? '#dc2626' : v === 2 ? '#d97706' : '#16a34a';
              return (
                <div className="severity-bar-container">
                  <div className="severity-bar" style={{ maxWidth: 80 }}>
                    <div style={{ height: '100%', width: `${pct}%`, background: color, borderRadius: 4 }} />
                  </div>
                  {severityBadge(v)}
                </div>
              );
            },
          },
          {
            key: 'daysPersistence',
            label: 'Days Persistence',
            render: (v) => (
              <span style={{ fontWeight: 600, color: v > 30 ? '#dc2626' : v > 7 ? '#d97706' : '#334155' }}>{v} days</span>
            ),
          },
        ]}
        rows={faults}
      />

      <div className="card">
        <h3>Maintenance Insights</h3>
        <ul className="checklist">
          {faults.filter((f) => f.activeEpisodes > 0).slice(0, 5).map((f, i) => {
            const bg = f.maxSeverity >= 3 ? '#fee2e2' : f.maxSeverity === 2 ? '#fef3c7' : '#dbeafe';
            const color = f.maxSeverity >= 3 ? '#dc2626' : f.maxSeverity === 2 ? '#d97706' : '#3b82f6';
            return (
              <li className="checklist-item" key={i}>
                <div className="checklist-icon" style={{ background: bg, color }}>{f.maxSeverity >= 3 ? '!' : '✓'}</div>
                <div className="checklist-text">
                  <strong>{f.dtcCode} — {f.activeEpisodes} active episode{f.activeEpisodes > 1 ? 's' : ''}</strong>
                  <span>Persisting for {f.daysPersistence} days · Severity: {severityLabel(f.maxSeverity)}{f.maxSeverity >= 3 ? ' — Schedule service immediately' : ' — Monitor and plan maintenance'}</span>
                </div>
              </li>
            );
          })}
          {faults.filter((f) => f.activeEpisodes > 0).length === 0 && (
            <li className="checklist-item" style={{ color: '#64748b' }}>No active faults — vehicle is healthy.</li>
          )}
        </ul>
      </div>
    </div>
  );
}

export default VehiclePage;