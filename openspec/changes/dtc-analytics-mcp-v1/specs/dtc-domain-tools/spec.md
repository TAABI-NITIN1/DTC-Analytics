## ADDED Requirements

### Requirement: Typed domain-first tools
The server SHALL initially provide typed tools for fleet health, DTC distribution, fleet trends, vehicle health, vehicle faults, DTC fleet impact, DTC co-occurrence, and maintenance priority. DTC detail lookup SHALL be exposed through the governed catalog capability. Every tool MUST publish input and output JSON schemas.

#### Scenario: Supported business question
- **WHEN** a requested answer is covered by a registered domain tool
- **THEN** the client invokes that tool and does not invoke dynamic SQL

#### Scenario: Invalid tool input
- **WHEN** input violates a tool schema or configured bounds
- **THEN** the server returns `INVALID_ARGUMENT` without executing a query

### Requirement: Server-controlled scope and bounds
Domain tools MUST omit model-settable tenant/customer fields and SHALL enforce server-provided scope, row limit, and timeout on every repository query.

#### Scenario: Model supplies a customer field
- **WHEN** model-generated tool arguments include a tenant or customer override
- **THEN** schema validation rejects the field and the trusted server scope remains unchanged

### Requirement: Stable results
Each domain tool SHALL return a versioned data envelope plus the evidence contract, with deterministic truncation and ordering for bounded lists.

#### Scenario: More rows exist than permitted
- **WHEN** a tool result exceeds its effective maximum rows
- **THEN** only the deterministic bounded subset is returned and evidence marks the result truncated
