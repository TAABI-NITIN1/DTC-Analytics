function KpiCard({ title, value, subtitle = '', icon = '', color = 'blue' }) {
  const bgMap = {
    blue: 'bg-blue',
    green: 'bg-green',
    red: 'bg-red',
    orange: 'bg-orange',
    info: 'bg-info',
  };

  return (
    <div className={`card kpi-card kpi-${color}`}>
      <div className="kpi-header">
        <div>
          <div className="kpi-title">{title}</div>
          <div className="kpi-value">{value}</div>
          {subtitle ? <div className="kpi-subtitle">{subtitle}</div> : null}
        </div>
        {icon ? (
          <div className={`kpi-icon ${bgMap[color] || 'bg-blue'}`}>{icon}</div>
        ) : null}
      </div>
    </div>
  );
}

export default KpiCard;