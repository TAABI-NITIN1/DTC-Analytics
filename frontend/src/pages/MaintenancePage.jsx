import { gql, useQuery } from '@apollo/client';
import KpiCard from '../components/KpiCard';
import DataTable from '../components/DataTable';

const MAINT_QUERY = gql`
  query MaintenanceDashboard($limit: Int!, $customerName: String) {
    maintenanceRecommendations(limit: $limit, customerName: $customerName) {
      uniqueid
      dtcCode
      severityLevel
      subsystem
      recommendation
    }
    fleetKpis(days: 30, customerName: $customerName) {
      maintenanceDue
      criticalAlerts
      vehiclesWithDtcs
    }
    enhancedMaintenance(limit: $limit, customerName: $customerName) {
      uniqueid
      vehicleNumber
      dtcCode
      description
      severityLevel
      faultDurationSec
      episodesLast30Days
      maintenancePriorityScore
      recommendedAction
    }
  }
`;

const severityLabel = (level) => {
  if (level >= 4) return 'Critical';
  if (level === 3) return 'High';
  if (level === 2) return 'Moderate';
  return 'Low';
};

function MaintenancePage({ customerName, onSelectVehicle, onSelectDtc }) {
  const { data, loading } = useQuery(MAINT_QUERY, {
    variables: { limit: 30, customerName: customerName || null },
  });
  const recs = data?.maintenanceRecommendations ?? [];
  const enhanced = data?.enhancedMaintenance ?? [];
  const kpis = data?.fleetKpis;

  const critCount = recs.filter((r) => r.severityLevel >= 3).length;
  const modCount = recs.filter((r) => r.severityLevel === 2).length;
  const lowCount = recs.filter((r) => r.severityLevel <= 1).length;

  return (
    <div className="page">
      <h2>Maintenance Insights</h2>
      {loading && <div className="card" style={{ textAlign: 'center', padding: 32 }}>Loading...</div>}

      <div className="kpi-grid">
        <KpiCard title="Maintenance Due" value={kpis?.maintenanceDue ?? '—'} icon="🛠️" color="red" />
        <KpiCard title="Critical Actions" value={critCount} icon="🔴" color="red" />
        <KpiCard title="Moderate Actions" value={modCount} icon="🟡" color="orange" />
        <KpiCard title="Low Priority" value={lowCount} icon="🔵" color="blue" />
      </div>

      {critCount > 0 && (
        <div className="card">
          <h3 style={{ color: '#dc2626' }}>⚠️ Critical — Immediate Action Required</h3>
          <ul className="checklist">
            {recs.filter((m) => m.severityLevel >= 3).map((m, i) => (
              <li className="checklist-item" key={`c-${i}`}>
                <div className="checklist-icon" style={{ background: '#fee2e2', color: '#dc2626' }}>!</div>
                <div className="checklist-text">
                  <strong>
                    <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(m.dtcCode)}>{m.dtcCode}</button>
                    {' '}· {m.subsystem || 'General'}
                  </strong>
                  <span>
                    Vehicle <button type="button" className="link-button" onClick={() => onSelectVehicle && onSelectVehicle(m.uniqueid)}>{m.uniqueid}</button> · {m.recommendation}
                  </span>
                </div>
                <span className="badge badge-critical" style={{ marginLeft: 'auto', flexShrink: 0 }}>{severityLabel(m.severityLevel)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {modCount > 0 && (
        <div className="card">
          <h3 style={{ color: '#d97706' }}>🟡 Moderate — Plan Service Soon</h3>
          <ul className="checklist">
            {recs.filter((m) => m.severityLevel === 2).map((m, i) => (
              <li className="checklist-item" key={`m-${i}`}>
                <div className="checklist-icon" style={{ background: '#fef3c7', color: '#d97706' }}>◆</div>
                <div className="checklist-text">
                  <strong>
                    <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(m.dtcCode)}>{m.dtcCode}</button>
                    {' '}· {m.subsystem || 'General'}
                  </strong>
                  <span>
                    Vehicle <button type="button" className="link-button" onClick={() => onSelectVehicle && onSelectVehicle(m.uniqueid)}>{m.uniqueid}</button> · {m.recommendation}
                  </span>
                </div>
                <span className="badge badge-moderate" style={{ marginLeft: 'auto', flexShrink: 0 }}>{severityLabel(m.severityLevel)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {lowCount > 0 && (
        <div className="card">
          <h3 style={{ color: '#3b82f6' }}>🔵 Low Priority — Monitor</h3>
          <ul className="checklist">
            {recs.filter((m) => m.severityLevel <= 1).map((m, i) => (
              <li className="checklist-item" key={`l-${i}`}>
                <div className="checklist-icon" style={{ background: '#dbeafe', color: '#3b82f6' }}>✓</div>
                <div className="checklist-text">
                  <strong>
                    <button type="button" className="link-button" onClick={() => onSelectDtc && onSelectDtc(m.dtcCode)}>{m.dtcCode}</button>
                    {' '}· {m.subsystem || 'General'}
                  </strong>
                  <span>
                    Vehicle <button type="button" className="link-button" onClick={() => onSelectVehicle && onSelectVehicle(m.uniqueid)}>{m.uniqueid}</button> · {m.recommendation}
                  </span>
                </div>
                <span className="badge badge-minor" style={{ marginLeft: 'auto', flexShrink: 0 }}>{severityLabel(m.severityLevel)}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

      {enhanced.length > 0 && (
        <DataTable
          title="Priority-Scored Maintenance Queue"
          columns={[
            { key: 'vehicleNumber', label: 'Vehicle' },
            { key: 'dtcCode', label: 'DTC Code' },
            { key: 'description', label: 'Description', render: (v) => v || '—' },
            {
              key: 'maintenancePriorityScore',
              label: 'Priority',
              render: (v) => {
                const color = v >= 70 ? '#dc2626' : v >= 40 ? '#d97706' : '#16a34a';
                return (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    <div style={{ width: 48, height: 6, background: '#e2e8f0', borderRadius: 3 }}>
                      <div style={{ width: `${Math.min(v, 100)}%`, height: '100%', background: color, borderRadius: 3 }} />
                    </div>
                    <span style={{ fontWeight: 700, color, fontSize: 12 }}>{v?.toFixed(1)}</span>
                  </div>
                );
              },
            },
            {
              key: 'faultDurationSec',
              label: 'Duration',
              render: (v) => {
                if (v == null) return '—';
                const hrs = v / 3600;
                return hrs >= 24 ? `${(hrs / 24).toFixed(1)}d` : `${hrs.toFixed(1)}h`;
              },
            },
            { key: 'episodesLast30Days', label: 'Episodes (30d)' },
            { key: 'recommendedAction', label: 'Action', render: (v) => v || '—' },
          ]}
          rows={enhanced}
          onRowClick={(row) => onSelectVehicle && onSelectVehicle(row.uniqueid)}
          onCellClick={({ event, column, row }) => {
            if (column.key === 'dtcCode') {
              event.stopPropagation();
              onSelectDtc && onSelectDtc(row.dtcCode);
            }
          }}
        />
      )}
    </div>
  );
}

export default MaintenancePage;