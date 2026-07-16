## ADDED Requirements

### Requirement: Trusted tenant identity
Production identity MUST be authenticated outside the model and SHALL provide user ID, tenant/customer ID, roles/scopes, and trace/correlation ID. The LLM MUST NOT choose, supply, or override its tenant.

#### Scenario: Authorized tenant request
- **WHEN** an authenticated user invokes a tool within an allowed scope
- **THEN** the server maps identity to approved customer keys and injects that scope into every repository query

#### Scenario: Tenant override attempted
- **WHEN** client content, model arguments, or SQL names a different customer
- **THEN** the server rejects the request as `SCOPE_VIOLATION` before query execution

### Requirement: No cross-customer queries
Every tenant data query, join, cache entry, fallback, and result MUST remain within the authenticated customer scope.

#### Scenario: Vehicle belongs to another customer
- **WHEN** a caller requests a vehicle identifier not mapped to its tenant
- **THEN** the server returns no data or `FORBIDDEN` according to policy and reveals no existence detail

#### Scenario: Aggregate lacks tenant key
- **WHEN** an aggregate table cannot prove customer isolation
- **THEN** the repository uses a scoped fact/join path or rejects the query; it never returns the global aggregate

### Requirement: Development identity
Stdio development mode SHALL require an explicit configured development principal and MUST NOT infer tenant from model content.

#### Scenario: Development principal missing
- **WHEN** a stdio request starts without configured identity
- **THEN** tenant data tools fail `UNAUTHENTICATED`
