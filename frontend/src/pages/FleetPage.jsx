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
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import DataTable from '../components/DataTable';

const FLEET_QUERY = gql`
  query FleetDashboard($days: Int!, $limit: Int!, $customerName: String) {
    fleetKpis(days: $days, customerName: $customerName) {
      totalVehicles
      vehiclesWithDtcs
      criticalVehicles
      avgResolutionDays
      fleetHealthScore
      maintenanceDue
      totalDtcAlerts
      criticalAlerts
    }
    fleetTrend(days: $days, customerName: $customerName) {
      eventDate
      activeFaultVehicles
      criticalFaultVehicles
      fleetHealthScore
    }
    topRiskVehicles(limit: $limit, customerName: $customerName) {
      uniqueid
      vehicleNumber
      vehicleModel
      customerName
      healthScore
      activeFaultCount
      criticalFaultCount
      longestActiveDays
    }
    topDtcCodes(days: $days, limit: 8, customerName: $customerName) {
      dtcCode
      description
      occurrences
      vehiclesAffected
    }
    fleetHealthSnap(customerName: $customerName) {
      driverRelatedFaults
      mostCommonDtc
      mostCommonSystem
      activeFaultTrend
    }
    fleetSystemHealth(customerName: $customerName) {
      system
      vehiclesAffected
      activeFaults
      criticalFaults
      riskScore
      trend
    }
    fleetFaultTrends(days: $days, customerName: $customerName) {
      eventDate
      activeFaults
      newFaults
      resolvedFaults
      driverRelatedFaults
      fleetHealthScore
    }
  }
`;

const SeverityBar = ({ level }) => {
  const cls = level >= 3 ? 'level-4' : level === 2 ? 'level-2' : 'level-1';
  const label = level >= 3 ? 'Critical' : level === 2 ? 'Moderate' : 'Minor';
  const badgeCls = level >= 3 ? 'badge-critical' : level === 2 ? 'badge-moderate' : 'badge-minor';
  return (
    <div className="severity-bar-container">
      <div className="severity-bar">
        <div className={`severity-bar-fill ${cls}`} />
      </div>
      <span className={`badge ${badgeCls}`}>{label}</span>
    </div>
  );
};

const healthColor = (score) => {
  if (score >= 95) return '#16a34a';
  if (score >= 80) return '#d97706';
  return '#dc2626';
};

const healthLabel = (score) => {
  if (score >= 95) return 'Excellent';
  if (score >= 80) return 'Good';
  if (score >= 60) return 'Fair';
  return 'Poor';
};

function FleetPage({ days, customerName, onSelectVehicle, onSelectDtc }) {
  const { data, loading } = useQuery(FLEET_QUERY, {
    variables: { days, limit: 10, customerName: customerName || null },
  });

  const kpis = data?.fleetKpis;
  const trend = data?.fleetTrend ?? [];
  const topRisk = data?.topRiskVehicles ?? [];
  const topDtc = data?.topDtcCodes ?? [];
  const maxOcc = Math.max(...topDtc.map((d) => d.occurrences), 1);
  const snap = data?.fleetHealthSnap;
  const systemHealth = data?.fleetSystemHealth ?? [];
  const faultTrends = data?.fleetFaultTrends ?? [];

  const score = kpis?.fleetHealthScore ?? 0;
  const gaugeData = [
    { name: 'Score', value: score },
    { name: 'Remaining', value: 100 - score },
  ];
  const GAUGE_COLORS = [healthColor(score), '#e2e8f0'];
  const trendIcon = snap?.activeFaultTrend === 'increasing' ? '📈' : snap?.activeFaultTrend === 'decreasing' ? '📉' : '➡️';

  const SYSTEM_COLORS = ['#dc2626', '#d97706', '#3b82f6', '#16a34a', '#8b5cf6', '#ec4899', '#06b6d4', '#f59e0b'];

  return (
    <div className="page">
      <h2>Fleet Overview</h2>
      {loading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading dashboard...</div>}

      <div className="kpi-grid">
        <KpiCard title="Total Vehicles" value={kpis?.totalVehicles ?? '—'} icon="🚛" color="blue" />
        <KpiCard
          title="Vehicles with Active DTCs"
          value={kpis?.vehiclesWithDtcs ?? '—'}
          subtitle={`out of ${kpis?.totalVehicles ?? 0} vehicles`}
          icon="⚠️"
          color="orange"
        />
        <KpiCard title="Critical Vehicles" value={kpis?.criticalVehicles ?? '—'} icon="🔴" color="red" />
        <KpiCard
          title="Avg Resolution Time"
          value={`${kpis?.avgResolutionDays ?? '—'} Days`}
          icon="⏱️"
          color="info"
        />
      </div>

      {snap && (
        <div className="kpi-grid" style={{ marginTop: 0 }}>
          <KpiCard title="Driver-Related Faults" value={snap.driverRelatedFaults ?? 0} icon="👤" color="orange" />
          <KpiCard title="Most Common DTC" value={snap.mostCommonDtc || '—'} icon="🔧" color="blue" />
          <KpiCard title="Most Common System" value={snap.mostCommonSystem || '—'} icon="⚙️" color="info" />
          <KpiCard title="Fault Trend" value={snap.activeFaultTrend || 'stable'} icon={trendIcon} color={snap.activeFaultTrend === 'increasing' ? 'red' : 'blue'} />
        </div>
      )}

      <div className="chart-grid">
        <div className="card chart-card">
          <h3>Active Vehicles Trend</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={trend}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="eventDate" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="activeFaultVehicles" name="Active Vehicles" stroke="#16a34a" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="criticalFaultVehicles" name="Critical Vehicles" stroke="#dc2626" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>

        <div className="card chart-card">
          <h3>Fleet Health Score</h3>
          <div className="gauge-wrapper">
            <ResponsiveContainer width={200} height={200}>
              <PieChart>
                <Pie data={gaugeData} cx="50%" cy="50%" innerRadius={65} outerRadius={85} startAngle={90} endAngle={-270} dataKey="value" stroke="none">
                  {gaugeData.map((_, i) => (
                    <Cell key={i} fill={GAUGE_COLORS[i]} />
                  ))}
                </Pie>
              </PieChart>
            </ResponsiveContainer>
            <div style={{ marginTop: -120, textAlign: 'center' }}>
              <div className="gauge-value">{score.toFixed(1)}</div>
              <div className="gauge-label" style={{ color: healthColor(score) }}>
                {healthLabel(score)}
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="chart-grid">
        <DataTable
          title="Top Risk Vehicles"
          columns={[
            { key: 'vehicleNumber', label: 'Vehicle' },
            { key: 'customerName', label: 'Customer' },
            { key: 'activeFaultCount', label: 'Active DTCs' },
            { key: 'criticalFaultCount', label: 'Critical' },
            { key: 'longestActiveDays', label: 'Longest Active Issue', render: (v) => `${v} days` },
            {
              key: 'criticalFaultCount',
              label: 'Severity',
              render: (v) => <SeverityBar level={v >= 1 ? 3 : 1} />,
            },
            {
              key: 'healthScore',
              label: 'Health Score',
              render: (v) => (
                <span className="badge" style={{ background: v >= 95 ? '#dcfce7' : v >= 80 ? '#fef3c7' : '#fee2e2', color: v >= 95 ? '#16a34a' : v >= 80 ? '#d97706' : '#dc2626' }}>
                  {Number(v).toFixed(1)}%
                </span>
              ),
            },
          ]}
          rows={topRisk}
          onRowClick={(row) => onSelectVehicle && onSelectVehicle(row.uniqueid)}
        />

        <div className="card">
          <h3>Top Problematic DTC Codes</h3>
          <div className="hbar-list">
            {topDtc.map((d) => (
              <button
                type="button"
                className="hbar-row hbar-row-button"
                key={d.dtcCode}
                onClick={() => onSelectDtc && onSelectDtc(d.dtcCode)}
              >
                <span className="hbar-label">{d.dtcCode}</span>
                <span className="hbar-desc">{d.description}</span>
                <div className="hbar-track">
                  <div className="hbar-fill" style={{ width: `${(d.occurrences / maxOcc) * 100}%`, background: 'linear-gradient(90deg, #3b82f6, #1e40af)' }} />
                </div>
                <span className="hbar-count">{d.occurrences}</span>
              </button>
            ))}
          </div>
        </div>
      </div>

      {/* New vs Resolved Faults Trend */}
      {faultTrends.length > 0 && (
        <div className="card chart-card">
          <h3>New vs Resolved Faults</h3>
          <ResponsiveContainer width="100%" height={280}>
            <LineChart data={faultTrends}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="eventDate" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} />
              <Tooltip />
              <Legend />
              <Line type="monotone" dataKey="newFaults" name="New Faults" stroke="#dc2626" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="resolvedFaults" name="Resolved" stroke="#16a34a" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="driverRelatedFaults" name="Driver-Related" stroke="#d97706" strokeWidth={2} strokeDasharray="5 5" dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}

      {/* System Health Breakdown */}
      {systemHealth.length > 0 && (
        <div className="chart-grid">
          <div className="card chart-card">
            <h3>System Health Breakdown</h3>
            <ResponsiveContainer width="100%" height={280}>
              <BarChart data={systemHealth} layout="vertical">
                <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
                <XAxis type="number" tick={{ fontSize: 11 }} />
                <YAxis dataKey="system" type="category" tick={{ fontSize: 11 }} width={120} />
                <Tooltip />
                <Legend />
                <Bar dataKey="activeFaults" name="Active Faults" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                <Bar dataKey="criticalFaults" name="Critical Faults" fill="#dc2626" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>

          <div className="card">
            <h3>System Risk Scores</h3>
            <div className="hbar-list">
              {systemHealth.map((s, i) => {
                const maxRisk = Math.max(...systemHealth.map((x) => x.riskScore), 1);
                return (
                  <div className="hbar-row" key={s.system}>
                    <span className="hbar-label">{s.system}</span>
                    <span className="hbar-desc">{s.vehiclesAffected} vehicles · {s.trend}</span>
                    <div className="hbar-track">
                      <div className="hbar-fill" style={{ width: `${(s.riskScore / maxRisk) * 100}%`, background: SYSTEM_COLORS[i % SYSTEM_COLORS.length] }} />
                    </div>
                    <span className="hbar-count">{s.riskScore.toFixed(1)}</span>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default FleetPage;