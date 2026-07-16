## ADDED Requirements

### Requirement: Disabled-by-default fallback
Validated SQL MUST be disabled by default and SHALL be available only to explicitly authorized roles when no domain tool can answer.

#### Scenario: Feature disabled
- **WHEN** any caller requests dynamic SQL while `DTC_MCP_DYNAMIC_SQL_ENABLED=false`
- **THEN** the server rejects the request without query execution

#### Scenario: Domain tool can answer
- **WHEN** an authorized SQL caller asks a question supported by a domain tool
- **THEN** the request is routed to or rejected in favor of the domain tool

### Requirement: Structural SQL validation
The SQL policy MUST prove a single read-only SELECT/WITH statement over approved tables, columns, functions, and joins; it MUST reject destructive SQL, comments/directives, system/external/table functions, unsafe settings, and ambiguous syntax.

#### Scenario: Destructive or ambiguous SQL
- **WHEN** SQL contains a write/DDL statement, multiple statements, unapproved identifier/function, or cannot be parsed conclusively
- **THEN** validation returns `QUERY_REJECTED` before ClickHouse execution

### Requirement: Server-injected tenant predicate
Validated SQL MUST receive tenant scope from trusted server context and the model MUST NOT provide tenant literals or predicates.

#### Scenario: SQL omits scope
- **WHEN** otherwise valid SQL is submitted for tenant data
- **THEN** the server structurally injects a provable scope predicate or rejects it; unscoped execution is impossible

### Requirement: Bounded execution
All validated SQL SHALL have server-enforced row, byte, and time limits and MUST return the standard evidence contract.

#### Scenario: Caller requests an excessive limit
- **WHEN** SQL requests more rows than permitted
- **THEN** the server applies the lower configured maximum and reports truncation/effective limit in evidence
