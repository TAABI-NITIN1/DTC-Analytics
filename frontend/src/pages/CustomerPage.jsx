import { gql, useQuery } from '@apollo/client';
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  CartesianGrid,
  Legend,
} from 'recharts';
import KpiCard from '../components/KpiCard';
import { aliasCustomerName } from '../utils/demoMasking';

const CUSTOMER_QUERY = gql`
  query CustomerDashboard($limit: Int!) {
    customerOverview(limit: $limit) {
      customerName
      vehicleCount
      activeFaultVehicles
      criticalFaultVehicles
      avgHealthScore
    }
  }
`;

function CustomerPage({ onSelectCustomer, demoMode }) {
  const { data, loading } = useQuery(CUSTOMER_QUERY, { variables: { limit: 50 } });
  const rows = data?.customerOverview ?? [];

  const totalVehicles = rows.reduce((s, r) => s + r.vehicleCount, 0);
  const totalActive = rows.reduce((s, r) => s + r.activeFaultVehicles, 0);
  const totalCritical = rows.reduce((s, r) => s + r.criticalFaultVehicles, 0);
  const avgScore =
    rows.length > 0 ? rows.reduce((s, r) => s + r.avgHealthScore, 0) / rows.length : 0;

  return (
    <div className="page">
      <h2>Select a Customer</h2>
      <p style={{ color: '#64748b', margin: '-8px 0 16px' }}>
        Click a customer row to view their personalized fleet analytics.
      </p>
      {loading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading...</div>}

      <div className="kpi-grid">
        <KpiCard title="Active Customers" value={rows.length} icon="👤" color="blue" />
        <KpiCard title="Total Vehicles" value={totalVehicles} icon="🚛" color="green" />
        <KpiCard title="Vehicles with Faults" value={totalActive} icon="⚠️" color="orange" />
        <KpiCard title="Avg Health Score" value={`${avgScore.toFixed(1)}%`} icon="❤️" color="info" />
      </div>

      {/* <div className="card chart-card">
        <h3>Customer Fleet Health</h3>
        <ResponsiveContainer width="100%" height={320}>
          <BarChart data={rows.slice(0, 12)} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis type="number" tick={{ fontSize: 11 }} />
            <YAxis dataKey="customerName" type="category" tick={{ fontSize: 11 }} width={140} />
            <Tooltip />
            <Legend />
            <Bar dataKey="vehicleCount" name="Total Vehicles" fill="#3b82f6" radius={[0, 4, 4, 0]} />
            <Bar dataKey="activeFaultVehicles" name="Active Faults" fill="#d97706" radius={[0, 4, 4, 0]} />
            <Bar dataKey="criticalFaultVehicles" name="Critical" fill="#dc2626" radius={[0, 4, 4, 0]} />
          </BarChart>
        </ResponsiveContainer>
      </div> */}

      {/* Clickable customer table */}
      <div className="card">
        <h3>Customer Summary</h3>
        <table className="data-table">
          <thead>
            <tr>
              <th>Customer</th>
              <th>Vehicles</th>
              <th>Active Faults</th>
              <th>Critical</th>
              <th>Avg Health</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r) => {
              const val = Number(r.avgHealthScore || 0);
              const color = val >= 95 ? '#16a34a' : val >= 80 ? '#d97706' : '#dc2626';
              const bg = val >= 95 ? '#dcfce7' : val >= 80 ? '#fef3c7' : '#fee2e2';
              return (
                <tr
                  key={r.customerName}
                  className="clickable-row"
                  onClick={() => onSelectCustomer && onSelectCustomer(r.customerName)}
                  title={`View ${demoMode ? aliasCustomerName(r.customerName) : r.customerName} dashboard`}
                >
                  <td><strong>{demoMode ? aliasCustomerName(r.customerName) : r.customerName}</strong></td>
                  <td>{r.vehicleCount}</td>
                  <td>
                    {r.activeFaultVehicles > 0 ? (
                      <span className="badge badge-active">{r.activeFaultVehicles}</span>
                    ) : (
                      <span className="badge badge-normal">0</span>
                    )}
                  </td>
                  <td>
                    {r.criticalFaultVehicles > 0 ? (
                      <span className="badge badge-critical">{r.criticalFaultVehicles}</span>
                    ) : (
                      <span className="badge badge-normal">0</span>
                    )}
                  </td>
                  <td>
                    <span className="badge" style={{ background: bg, color }}>{val.toFixed(1)}%</span>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default CustomerPage;