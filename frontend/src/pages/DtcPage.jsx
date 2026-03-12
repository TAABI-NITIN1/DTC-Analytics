import { gql, useQuery } from '@apollo/client';
import {
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
  Tooltip,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Legend,
  BarChart,
  Bar,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import DataTable from '../components/DataTable';

const DTC_QUERY = gql`
  query DtcDashboard($days: Int!, $limit: Int!, $customerName: String) {
    fleetKpis(days: $days, customerName: $customerName) {
      totalVehicles
      vehiclesWithDtcs
      criticalAlerts
      maintenanceDue
      totalDtcAlerts
    }
    topDtcCodes(days: $days, limit: $limit, customerName: $customerName) {
      dtcCode
      description
      occurrences
      vehiclesAffected
    }
    severityBreakdown(days: $days, customerName: $customerName) {
      level
      count
      percentage
    }
    dtcAlertsTrend(days: $days, customerName: $customerName) {
      eventDate
      totalAlerts
      criticalAlerts
    }
    vehiclesWithDtcs(limit: 30, customerName: $customerName) {
      uniqueid
      vehicleNumber
      customerName
      dtcCodes
      status
      lastReported
      activeCount
      criticalCount
    }
    maintenanceRecommendations(limit: 8, customerName: $customerName) {
      uniqueid
      dtcCode
      severityLevel
      subsystem
      recommendation
    }
    dtcFleetImpact(limit: 15, customerName: $customerName) {
      dtcCode
      system
      vehiclesAffected
      activeVehicles
      avgResolutionTime
      driverRelatedRatio
      fleetRiskScore
    }
    dtcCooccurrence(limit: 15, customerName: $customerName) {
      dtcCodeA
      dtcCodeB
      cooccurrenceCount
      vehiclesAffected
      avgTimeGapSec
    }
  }
`;

const SEVERITY_COLORS = {
  Critical: '#dc2626',
  Moderate: '#d97706',
  Minor: '#3b82f6',
};

const BAR_GRADIENTS = [
  '#3b82f6', '#2563eb', '#1d4ed8', '#1e40af', '#1e3a8a',
  '#6366f1', '#4f46e5', '#4338ca', '#3730a3', '#312e81',
];

function DtcPage({ days, dtcCode, customerName }) {
  const { data, loading } = useQuery(DTC_QUERY, {
    variables: { days, limit: 10, customerName: customerName || null },
  });

  const kpis = data?.fleetKpis;
  const topCodes = data?.topDtcCodes ?? [];
  const severity = data?.severityBreakdown ?? [];
  const alertsTrend = data?.dtcAlertsTrend ?? [];
  const vehicles = data?.vehiclesWithDtcs ?? [];
  const maintenance = data?.maintenanceRecommendations ?? [];
  const fleetImpact = data?.dtcFleetImpact ?? [];
  const cooccurrence = data?.dtcCooccurrence ?? [];
  const maxOcc = Math.max(...topCodes.map((d) => d.occurrences), 1);

  return (
    <div className="page">
      <h2>DTC Level Analytics</h2>
      {loading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading...</div>}

      <div className="kpi-grid">
        <KpiCard title="Total DTC Alerts" value={kpis?.totalDtcAlerts ?? '—'} icon="⚡" color="blue" />
        <KpiCard title="Vehicles with DTCs" value={kpis?.vehiclesWithDtcs ?? '—'} subtitle={`out of ${kpis?.totalVehicles ?? 0}`} icon="🚛" color="orange" />
        <KpiCard title="Critical Alerts" value={kpis?.criticalAlerts ?? '—'} icon="🔴" color="red" />
        <KpiCard title="Maintenance Due" value={kpis?.maintenanceDue ?? '—'} icon="🛠️" color="info" />
      </div>

      <div className="chart-grid">
        <div className="card">
          <h3>Top DTC Codes</h3>
          <div className="hbar-list">
            {topCodes.map((d, i) => (
              <div className="hbar-row" key={d.dtcCode}>
                <span className="hbar-label">{d.dtcCode}</span>
                <span className="hbar-desc">{d.description}</span>
                <div className="hbar-track">
                  <div className="hbar-fill" style={{ width: `${(d.occurrences / maxOcc) * 100}%`, background: BAR_GRADIENTS[i % BAR_GRADIENTS.length] }} />
                </div>
                <span className="hbar-count">{d.occurrences}</span>
              </div>
            ))}
          </div>
        </div>

        <div className="card chart-card">
          <h3>Fault Code Breakdown</h3>
          <div style={{ display: 'flex', alignItems: 'center', gap: 24 }}>
            <ResponsiveContainer width={200} height={200}>
              <PieChart>
                <Pie data={severity} dataKey="count" nameKey="level" cx="50%" cy="50%" innerRadius={55} outerRadius={80} stroke="none">
                  {severity.map((entry) => (
                    <Cell key={entry.level} fill={SEVERITY_COLORS[entry.level] || '#94a3b8'} />
                  ))}
                </Pie>
                <Tooltip formatter={(val, name) => [`${val} alerts`, name]} />
              </PieChart>
            </ResponsiveContainer>
            <div className="donut-legend">
              {severity.map((s) => (
                <div className="donut-legend-item" key={s.level}>
                  <div className="donut-legend-dot" style={{ background: SEVERITY_COLORS[s.level] || '#94a3b8' }} />
                  <span><strong>{s.level}</strong> — {s.percentage}%</span>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="card chart-card">
        <h3>DTC Alerts Trend</h3>
        <ResponsiveContainer width="100%" height={260}>
          <LineChart data={alertsTrend}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis dataKey="eventDate" tick={{ fontSize: 11 }} />
            <YAxis tick={{ fontSize: 11 }} />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="totalAlerts" name="Total Alerts" stroke="#3b82f6" strokeWidth={2} dot={false} />
            <Line type="monotone" dataKey="criticalAlerts" name="Critical Alerts" stroke="#dc2626" strokeWidth={2} dot={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <DataTable
        title="Vehicles with DTCs"
        columns={[
          { key: 'vehicleNumber', label: 'Vehicle' },
          { key: 'customerName', label: 'Customer' },
          {
            key: 'dtcCodes',
            label: 'DTC Codes',
            render: (codes) => (
              <div className="pill-row">
                {(codes || []).slice(0, 5).map((c) => (
                  <span key={c} className="dtc-pill minor">{c}</span>
                ))}
                {(codes || []).length > 5 && <span className="dtc-pill minor">+{codes.length - 5}</span>}
              </div>
            ),
          },
          {
            key: 'status',
            label: 'Status',
            render: (v) => (
              <span className={`badge ${v === 'Critical' ? 'badge-critical' : v === 'Active' ? 'badge-active' : 'badge-normal'}`}>{v}</span>
            ),
          },
          { key: 'lastReported', label: 'Last Reported' },
        ]}
        rows={vehicles}
      />

      <div className="chart-grid">
        <DataTable
          title="DTC Fleet Impact Ranking"
          columns={[
            { key: 'dtcCode', label: 'DTC Code' },
            { key: 'system', label: 'System' },
            { key: 'vehiclesAffected', label: 'Vehicles' },
            { key: 'activeVehicles', label: 'Active' },
            {
              key: 'fleetRiskScore',
              label: 'Risk Score',
              render: (v) => {
                const color = v >= 70 ? '#dc2626' : v >= 40 ? '#d97706' : '#16a34a';
                return (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <div style={{ width: 48, height: 6, background: '#e2e8f0', borderRadius: 3 }}>
                      <div style={{ width: `${Math.min(v, 100)}%`, height: '100%', background: color, borderRadius: 3 }} />
                    </div>
                    <span style={{ fontWeight: 600, color, fontSize: 12 }}>{v?.toFixed(1)}</span>
                  </div>
                );
              },
            },
            {
              key: 'avgResolutionTime',
              label: 'Avg Resolution',
              render: (v) => v != null ? `${(v / 3600).toFixed(1)}h` : '—',
            },
          ]}
          rows={fleetImpact}
        />

        <div className="card">
          <h3>DTC Co-occurrence Patterns</h3>
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
              <thead>
                <tr style={{ borderBottom: '2px solid #e2e8f0' }}>
                  <th style={{ textAlign: 'left', padding: '8px 12px' }}>DTC A</th>
                  <th style={{ textAlign: 'left', padding: '8px 12px' }}>DTC B</th>
                  <th style={{ textAlign: 'right', padding: '8px 12px' }}>Co-occurrences</th>
                  <th style={{ textAlign: 'right', padding: '8px 12px' }}>Vehicles</th>
                  <th style={{ textAlign: 'right', padding: '8px 12px' }}>Avg Gap</th>
                </tr>
              </thead>
              <tbody>
                {cooccurrence.map((row, i) => (
                  <tr key={i} style={{ borderBottom: '1px solid #f1f5f9' }}>
                    <td style={{ padding: '8px 12px' }}><span className="dtc-pill minor">{row.dtcCodeA}</span></td>
                    <td style={{ padding: '8px 12px' }}><span className="dtc-pill minor">{row.dtcCodeB}</span></td>
                    <td style={{ padding: '8px 12px', textAlign: 'right', fontWeight: 600 }}>{row.cooccurrenceCount}</td>
                    <td style={{ padding: '8px 12px', textAlign: 'right' }}>{row.vehiclesAffected}</td>
                    <td style={{ padding: '8px 12px', textAlign: 'right' }}>{row.avgTimeGapSec != null ? `${(row.avgTimeGapSec / 60).toFixed(0)}m` : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <h3>Maintenance Recommendations</h3>
        <ul className="checklist">
          {maintenance.map((m, i) => {
            const bg = m.severityLevel >= 3 ? '#fee2e2' : m.severityLevel === 2 ? '#fef3c7' : '#dbeafe';
            const color = m.severityLevel >= 3 ? '#dc2626' : m.severityLevel === 2 ? '#d97706' : '#3b82f6';
            return (
              <li className="checklist-item" key={i}>
                <div className="checklist-icon" style={{ background: bg, color }}>{m.severityLevel >= 3 ? '!' : '✓'}</div>
                <div className="checklist-text">
                  <strong>{m.dtcCode} — {m.subsystem || 'General'}</strong>
                  <span>Vehicle {m.uniqueid} · {m.recommendation}</span>
                </div>
              </li>
            );
          })}
        </ul>
      </div>
    </div>
  );
}

export default DtcPage;