export default function DataGapsBanner({ gaps = [] }) {
  if (!gaps?.length) return null;
  const labels = {
    trace_events_empty: 'Trace/node timing not logged for this run',
    validation_findings_empty: 'Validation findings absent (Phase 2 run)',
    validation_without_tool_evidence: 'Validator flags may be inflated — tool/SQL evidence was not stored for this run',
    sql_events_empty: 'SQL event log empty — claim-level validator counts may be inflated',
    system_metrics_not_collected: 'System/infra metrics (CPU, CH, OpenAI limits) not in eval artifacts',
  };
  return (
    <div className="gaps-banner">
      <strong>Data gaps:</strong>{' '}
      {gaps.map((g) => labels[g] || g).join(' · ')}
    </div>
  );
}
