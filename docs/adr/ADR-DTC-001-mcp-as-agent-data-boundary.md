# ADR-DTC-001: MCP as the agent data boundary

Status: Accepted for DTC MCP v1 design

## Context

The AI Analyst directly constructs/executes ClickHouse SQL. That couples model behavior to storage and makes tenant, limits, audit, and evolution inconsistent.

## Decision

The AI Analyst will access DTC data through a read-only DTC MCP client/server boundary. In MCP mode, direct AI SQL is disabled. GraphQL remains the frontend API and does not call MCP for its own backend data.

## Alternatives

- Keep direct tools and add more regex checks: insufficient trust boundary.
- Route every backend caller through MCP: unnecessary latency/coupling for GraphQL.
- Replace GraphQL with MCP: breaks the established frontend contract.

## Consequences

Agent tools become versioned and governed, but introduce a deployable service/client and transport failure mode. Shared repositories prevent duplicate business logic.

## Security implications

Identity, scope, policy, bounds, and audit are enforced server-side outside model control. MCP exposure remains internal and read-only.

## Rollback implications

`DTC_DATA_ACCESS_MODE=direct` temporarily restores the existing analyst path. No dashboard or data migration rollback is needed.
