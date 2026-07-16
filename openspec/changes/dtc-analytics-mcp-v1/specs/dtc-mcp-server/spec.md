## ADDED Requirements

### Requirement: Read-only governed MCP service
The DTC MCP server SHALL expose only registered read-only DTC tools and resources and MUST use least-privilege ClickHouse access with enforced query limits and timeouts.

#### Scenario: Destructive operation is attempted
- **WHEN** a request attempts INSERT, UPDATE, DELETE, ALTER, DROP, TRUNCATE, multiple statements, or an unregistered operation
- **THEN** the server rejects it before ClickHouse execution and emits an audited `QUERY_REJECTED` outcome

#### Scenario: Query exceeds execution budget
- **WHEN** a repository query exceeds the configured time or row budget
- **THEN** the server cancels or terminates it and returns a bounded `TIMEOUT` or truncated result with evidence

### Requirement: Supported transports
The server SHALL provide stdio for local development/protocol tests and Streamable HTTP for internal production with identical tool contracts.

#### Scenario: Contract runs across transports
- **WHEN** the same authorized tool request is executed over stdio and Streamable HTTP
- **THEN** both responses validate against the same output and evidence schemas

### Requirement: Secret safety
The server MUST load credentials from approved runtime configuration and MUST NOT store or return production secrets in source control, logs, resources, or responses.

#### Scenario: Server error occurs
- **WHEN** a database or transport error contains connection details
- **THEN** the client receives a sanitized error and no credential value is logged or returned
