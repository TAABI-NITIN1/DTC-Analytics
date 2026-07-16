## ADDED Requirements

### Requirement: GraphQL compatibility
Existing GraphQL schema, field semantics, and frontend behavior SHALL remain backward compatible while resolvers migrate to shared repositories.

#### Scenario: Resolver migrates
- **WHEN** a GraphQL query family switches from inline SQL to a repository
- **THEN** contract tests confirm the same field names/types, authorized scope behavior, and agreed metric semantics

### Requirement: Shared repositories
GraphQL/FastAPI and the DTC MCP server SHALL use the same repository functions; GraphQL MUST NOT call MCP merely to access backend data.

#### Scenario: Dashboard and MCP request same domain result
- **WHEN** both paths request equivalent inputs under the same trusted scope
- **THEN** they execute the same repository contract and produce semantically equivalent data

### Requirement: Feature modes
The analyst SHALL support `direct`, `shadow`, and `mcp` modes. Shadow mode MUST be disabled in production by default because it doubles queries.

#### Scenario: Shadow mode request
- **WHEN** non-production shadow mode is enabled
- **THEN** the direct result is user-visible, the MCP result is compared asynchronously/boundedly, and both calls share scope/trace metadata

### Requirement: Rollback
Direct mode SHALL remain temporarily available for explicit rollback until MCP security, parity, reliability, and observation exit criteria pass.

#### Scenario: MCP rollout is rolled back
- **WHEN** operators set `DTC_DATA_ACCESS_MODE=direct`
- **THEN** analyst data access returns to the existing path without a frontend/GraphQL contract or data migration rollback

### Requirement: Airflow separation
Airflow SHALL remain the separate analytics producer and its paths, scheduling, and deployment ownership MUST NOT move into the MCP service.

#### Scenario: MCP is deployed
- **WHEN** the new runtime service starts
- **THEN** it reads approved analytics outputs without owning or relocating Airflow DAGs or table-production jobs
