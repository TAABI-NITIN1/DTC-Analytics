export default function KpiCard({ title, value, subtitle = '' }) {
  return (
    <div className="kpi-card">
      <div className="kpi-title">{title}</div>
      <div className="kpi-value">{value}</div>
      {subtitle ? <div className="kpi-sub">{subtitle}</div> : null}
    </div>
  );
}
