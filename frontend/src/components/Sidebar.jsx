const levels = [
  { key: 'customer', label: 'Customer Select', icon: '👤', gated: false },
  { key: 'fleet', label: 'Fleet Overview', icon: '🚛', gated: true },
  { key: 'dtc', label: 'DTC Analytics', icon: '⚡', gated: true },
  { key: 'vehicle', label: 'Vehicle Details', icon: '🔧', gated: true },
  { key: 'maintenance', label: 'Maintenance', icon: '🛠️', gated: true },
];

function Sidebar({ activeLevel, onChange, selectedCustomer }) {
  return (
    <aside className="sidebar">
      <div className="brand">
        <span>📊</span> DTC Analytics
      </div>
      <div className="brand-sub">Fleet Diagnostic Intelligence</div>
      <nav>
        {levels.map((level) => {
          const disabled = level.gated && !selectedCustomer;
          return (
            <button
              key={level.key}
              type="button"
              className={`nav-item ${activeLevel === level.key ? 'active' : ''} ${disabled ? 'disabled' : ''}`}
              onClick={() => !disabled && onChange(level.key)}
              disabled={disabled}
              title={disabled ? 'Select a customer first' : level.label}
            >
              <span className="nav-icon">{level.icon}</span>
              {level.label}
            </button>
          );
        })}
      </nav>
    </aside>
  );
}

export default Sidebar;