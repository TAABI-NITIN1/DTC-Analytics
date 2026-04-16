import { gql, useQuery } from '@apollo/client';
import {
  ResponsiveContainer,
  AreaChart,
  Area,
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

const DTC_FLEET_QUERY = gql`
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

const DTC_SELECTED_QUERY = gql`
  query SelectedDtcDashboard($dtcCode: String!, $days: Int!, $limit: Int!, $customerName: String) {
    selectedDtcKpis(dtcCode: $dtcCode, days: $days, customerName: $customerName) {
      dtcCode
      system
      subsystem
      affectedVehicles
      activeEpisodes
      criticalEpisodes
      avgResolutionDays
      driverRelatedRatio
      lastSeenDate
    }
    selectedDtcWeeklyTrend(dtcCode: $dtcCode, days: $days, customerName: $customerName) {
      weekStart
      occurrences
      affectedVehicles
      activeEpisodes
      criticalEpisodes
    }
    selectedDtcCooccurrence(dtcCode: $dtcCode, days: $days, limit: $limit, customerName: $customerName) {
      coDtcCode
      cooccurrenceCount
      vehiclesAffected
    }
    selectedDtcVehicles(dtcCode: $dtcCode, days: $days, limit: 100, customerName: $customerName) {
      uniqueid
      vehicleNumber
      episodeCount
      activeEpisodes
      lastReportedDate
      healthScore
    }
    dtcDetail(dtcCode: $dtcCode) {
      dtcCode
      system
      subsystem
      description
      severityLevel
      primaryCause
      symptoms
      impactIfUnresolved
      actionRequired
      recommendedPreventiveAction
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

function severityTypeFromLevel(level) {
  const severity = Number(level);
  if (!Number.isFinite(severity)) return 'Unknown';
  if (severity >= 3) return 'Critical';
  if (severity === 2) return 'Major';
  if (severity === 1) return 'Minor';
  if (severity === 0) return 'Info';
  return 'Unknown';
}

function severityTypeColor(type) {
  if (type === 'Critical') return 'red';
  if (type === 'Major') return 'orange';
  if (type === 'Minor') return 'blue';
  if (type === 'Info') return 'info';
  return 'info';
}

function DtcPage({ days, dtcCode, customerName, onSelectVehicle, onSelectDtc }) {
  const selectedCode = String(dtcCode || '').trim().toUpperCase();
  const isSingleCodeMode = Boolean(selectedCode);

  const { data: fleetData, loading: fleetLoading } = useQuery(DTC_FLEET_QUERY, {
    variables: { days, limit: 10, customerName: customerName || null },
    skip: isSingleCodeMode,
  });

  const { data: selectedData, loading: selectedLoading } = useQuery(DTC_SELECTED_QUERY, {
    variables: { dtcCode: selectedCode, days, limit: 12, customerName: customerName || null },
    skip: !isSingleCodeMode,
  });

  const data = fleetData;
  const loading = fleetLoading;

  const kpis = data?.fleetKpis;
  const topCodes = data?.topDtcCodes ?? [];
  const severity = data?.severityBreakdown ?? [];
  const alertsTrend = data?.dtcAlertsTrend ?? [];
  const vehicles = data?.vehiclesWithDtcs ?? [];
  const maintenance = data?.maintenanceRecommendations ?? [];
  const fleetImpact = data?.dtcFleetImpact ?? [];
  const cooccurrence = data?.dtcCooccurrence ?? [];
  const maxOcc = Math.max(...topCodes.map((d) => d.occurrences), 1);

  const selectedKpis = selectedData?.selectedDtcKpis;
  const selectedTrend = selectedData?.selectedDtcWeeklyTrend ?? [];
  const selectedCooccurrence = selectedData?.selectedDtcCooccurrence ?? [];
  const selectedVehicles = selectedData?.selectedDtcVehicles ?? [];
  const selectedDetail = selectedData?.dtcDetail;
  const selectedDtcSeverityType = severityTypeFromLevel(selectedDetail?.severityLevel);

  if (isSingleCodeMode) {
    return (
      <div className="page">
        <h2>DTC Level Analytics - {selectedCode}</h2>
        {selectedLoading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading selected DTC analytics...</div>}

        {!selectedLoading && !selectedKpis && (
          <div className="card" style={{ padding: 20 }}>
            <h3 style={{ marginTop: 0 }}>No Data Found</h3>
            <p style={{ marginBottom: 0, color: '#64748b' }}>
              No records found for DTC <strong>{selectedCode}</strong> in the selected customer scope.
            </p>
          </div>
        )}

        {selectedKpis && (
          <>
            <div className="kpi-grid">
              <KpiCard title="Affected Vehicles" value={selectedKpis.affectedVehicles ?? 0} icon="🚛" color="blue" />
              <KpiCard title="Active Episodes" value={selectedKpis.activeEpisodes ?? 0} icon="⚠️" color="orange" />
              <KpiCard title="DTC Severity Type" value={selectedDtcSeverityType} icon="🏷️" color={severityTypeColor(selectedDtcSeverityType)} />
              <KpiCard title="Avg Resolution" value={`${selectedKpis.avgResolutionDays ?? 0} days`} icon="⏱️" color="info" />
              <KpiCard title="Driver-Related Ratio" value={`${((selectedKpis.driverRelatedRatio ?? 0) * 100).toFixed(1)}%`} icon="👤" color="orange" />
              <KpiCard title="Last Seen" value={selectedKpis.lastSeenDate || '—'} icon="🗓️" color="info" />
              <KpiCard title="System / Subsystem" value={`${selectedKpis.system || '—'}${selectedKpis.subsystem ? ` / ${selectedKpis.subsystem}` : ''}`} icon="⚙️" color="blue" />
            </div>

            <div className="card chart-card">
              <h3>Weekly Trend (Selected DTC)</h3>
              <ResponsiveContainer width="100%" height={280}>
                <AreaChart data={selectedTrend}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                  <XAxis dataKey="weekStart" tick={{ fontSize: 11 }} />
                  <YAxis tick={{ fontSize: 11 }} />
                  <Tooltip />
                  <Legend />
                  <Area type="monotone" dataKey="occurrences" name="Occurrences" stroke="#2563eb" fill="#93c5fd" fillOpacity={0.35} />
                  <Area type="monotone" dataKey="activeEpisodes" name="Active Episodes" stroke="#d97706" fill="#fde68a" fillOpacity={0.25} />
                  <Area type="monotone" dataKey="criticalEpisodes" name="Critical Episodes" stroke="#dc2626" fill="#fecaca" fillOpacity={0.25} />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-grid">
              <div className="card">
                <h3>Co-occurrence with {selectedCode}</h3>
                <div style={{ overflowX: 'auto' }}>
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: 13 }}>
                    <thead>
                      <tr style={{ borderBottom: '2px solid #e2e8f0' }}>
                        <th style={{ textAlign: 'left', padding: '8px 12px' }}>Co DTC</th>
                        <th style={{ textAlign: 'right', padding: '8px 12px' }}>Co-occurrences</th>
                        <th style={{ textAlign: 'right', padding: '8px 12px' }}>Vehicles</th>
                      </tr>
                    </thead>
                    <tbody>
                      {selectedCooccurrence.map((row, i) => (
                        <tr key={i} style={{ borderBottom: '1px solid #f1f5f9' }}>
                          <td style={{ padding: '8px 12px' }}>
                            <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(row.coDtcCode)}>
                              <span className="dtc-pill moderate">{row.coDtcCode}</span>
                            </button>
                          </td>
                          <td style={{ padding: '8px 12px', textAlign: 'right', fontWeight: 600 }}>{row.cooccurrenceCount}</td>
                          <td style={{ padding: '8px 12px', textAlign: 'right' }}>{row.vehiclesAffected}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              <div className="card">
                <h3>DTC Knowledge</h3>
                <details>
                  <summary style={{ cursor: 'pointer', fontWeight: 600, marginBottom: 10 }}>Expand Details</summary>
                  {selectedDetail ? (
                    <div style={{ fontSize: 13, color: '#334155', lineHeight: 1.55 }}>
                      <p><strong>Description:</strong> {selectedDetail.description || '—'}</p>
                      <p><strong>Severity:</strong> {selectedDetail.severityLevel ?? '—'}</p>
                      <p><strong>Cause:</strong> {selectedDetail.primaryCause || '—'}</p>
                      <p><strong>Symptoms:</strong> {selectedDetail.symptoms || '—'}</p>
                      <p><strong>Impact:</strong> {selectedDetail.impactIfUnresolved || '—'}</p>
                      <p><strong>Action:</strong> {selectedDetail.actionRequired || '—'}</p>
                      <p><strong>Preventive:</strong> {selectedDetail.recommendedPreventiveAction || '—'}</p>
                    </div>
                  ) : (
                    <p style={{ margin: 0, color: '#64748b' }}>No DTC master details found for this code.</p>
                  )}
                </details>
              </div>
            </div>

            <DataTable
              title="Vehicles Affected by Selected DTC"
              columns={[
                { key: 'vehicleNumber', label: 'Vehicle Number' },
                { key: 'episodeCount', label: 'Episode Count' },
                { key: 'activeEpisodes', label: 'Active Episodes' },
                { key: 'lastReportedDate', label: 'Last Reported' },
                {
                  key: 'healthScore',
                  label: 'Health Score',
                  render: (v) => (
                    <span className="badge" style={{ background: Number(v) >= 95 ? '#dcfce7' : Number(v) >= 80 ? '#fef3c7' : '#fee2e2', color: Number(v) >= 95 ? '#16a34a' : Number(v) >= 80 ? '#d97706' : '#dc2626' }}>
                      {Number(v || 0).toFixed(1)}%
                    </span>
                  ),
                },
              ]}
              rows={selectedVehicles}
              onRowClick={(row) => onSelectVehicle && onSelectVehicle(row.uniqueid)}
            />
          </>
        )}
      </div>
    );
  }

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
              <button
                type="button"
                className="hbar-row hbar-row-button"
                key={d.dtcCode}
                onClick={() => onSelectDtc && onSelectDtc(d.dtcCode)}
              >
                <span className="hbar-label">{d.dtcCode}</span>
                <span className="hbar-desc">{d.description}</span>
                <div className="hbar-track">
                  <div className="hbar-fill" style={{ width: `${(d.occurrences / maxOcc) * 100}%`, background: BAR_GRADIENTS[i % BAR_GRADIENTS.length] }} />
                </div>
                <span className="hbar-count">{d.occurrences}</span>
              </button>
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
                  <button key={c} type="button" className="link-button" onClick={(event) => {
                    event.stopPropagation();
                    onSelectDtc && onSelectDtc(c);
                  }}>
                    <span className="dtc-pill minor">{c}</span>
                  </button>
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
        onRowClick={(row) => onSelectVehicle && onSelectVehicle(row.uniqueid)}
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
          onCellClick={({ event, column, row }) => {
            if (column.key === 'dtcCode') {
              event.stopPropagation();
              onSelectDtc && onSelectDtc(row.dtcCode);
            }
          }}
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
                    <td style={{ padding: '8px 12px' }}>
                      <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(row.dtcCodeA)}>
                        <span className="dtc-pill minor">{row.dtcCodeA}</span>
                      </button>
                    </td>
                    <td style={{ padding: '8px 12px' }}>
                      <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(row.dtcCodeB)}>
                        <span className="dtc-pill minor">{row.dtcCodeB}</span>
                      </button>
                    </td>
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