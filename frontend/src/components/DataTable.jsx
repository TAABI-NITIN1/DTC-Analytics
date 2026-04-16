function DataTable({ columns, rows, title, onRowClick, onCellClick }) {
  return (
    <div className="card table-card">
      {title ? <h3 style={{ padding: '14px 14px 0', margin: 0 }}>{title}</h3> : null}
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>{column.label}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="empty-row">No data</td>
            </tr>
          ) : (
            rows.map((row, index) => (
              <tr
                key={index}
                className={onRowClick ? 'clickable-row' : ''}
                onClick={onRowClick ? () => onRowClick(row, index) : undefined}
              >
                {columns.map((column) => (
                  <td
                    key={column.key}
                    className={onCellClick ? 'clickable-cell' : ''}
                    onClick={onCellClick ? (event) => onCellClick({ event, row, column, index, value: row[column.key] }) : undefined}
                  >
                    {column.render ? column.render(row[column.key], row) : row[column.key]}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export default DataTable;