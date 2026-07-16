export default function DataTable({ columns, rows, maxRows = 15 }) {
  const slice = (rows || []).slice(0, maxRows);
  if (!slice.length) return <p style={{ color: 'var(--muted)' }}>No data</p>;
  return (
    <table className="data-table">
      <thead>
        <tr>
          {columns.map((c) => (
            <th key={c.key}>{c.label}</th>
          ))}
        </tr>
      </thead>
      <tbody>
        {slice.map((row, i) => (
          <tr key={i}>
            {columns.map((c) => (
              <td key={c.key}>{c.render ? c.render(row) : row[c.key]}</td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
