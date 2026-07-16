## ADDED Requirements

### Requirement: Traceable tool calls
Every MCP tool call MUST be traceable across API, client, server, repository, and ClickHouse using a correlation/trace ID.

#### Scenario: Tool call completes
- **WHEN** a call succeeds, fails, times out, or is rejected
- **THEN** audit telemetry records identity/scope references, tool/version, transport, policy outcome, duration, row count, mode, and stable error code where applicable

### Requirement: Operational metrics
The service SHALL emit metrics for calls, latency, errors, policy rejection, timeouts, rows, truncation, cache behavior, and shadow mismatch.

#### Scenario: Shadow comparison differs
- **WHEN** shadow mode observes results outside the configured parity tolerance
- **THEN** a mismatch metric/event is emitted without changing the user-visible direct response

### Requirement: Telemetry safety
Logs, metrics, and traces MUST redact or omit secrets, tokens, and sensitive raw SQL and SHALL be access-controlled and tenant-safe.

#### Scenario: SQL execution fails
- **WHEN** an upstream error contains SQL or connection data
- **THEN** telemetry stores only approved redacted diagnostics and the client receives a sanitized error
