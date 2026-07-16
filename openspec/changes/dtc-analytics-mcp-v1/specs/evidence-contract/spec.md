## ADDED Requirements

### Requirement: Evidence on every result
Every successful MCP tool result MUST contain evidence metadata identifying tool/contract version, approved sources, server-applied scope, query window/as-of time, row count, truncation, cache status, duration, and trace ID.

#### Scenario: Tool succeeds
- **WHEN** a domain or validated-SQL tool returns data
- **THEN** its response validates against the common evidence schema and each mandatory evidence field is present

### Requirement: Safe provenance
Evidence MUST NOT reveal credentials, raw authentication tokens, unsafe raw SQL, or another tenant's identifiers.

#### Scenario: Evidence is serialized
- **WHEN** provenance is returned or logged
- **THEN** sensitive values are absent or policy-redacted while approved source identifiers remain traceable

### Requirement: Explicit freshness and truncation
Evidence SHALL make data freshness/query window and partial-result status machine-readable.

#### Scenario: Result is truncated
- **WHEN** a result reaches a server maximum
- **THEN** evidence sets `truncated=true`, states the effective limit, and does not imply completeness
