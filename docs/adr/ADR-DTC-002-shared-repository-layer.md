# ADR-DTC-002: Shared repository layer

Status: Accepted for DTC MCP v1 design

## Context

AI tools and GraphQL resolvers duplicate fleet, vehicle, DTC, trend, co-occurrence, impact, maintenance, filtering, and result-shaping SQL.

## Decision

Extract typed repositories inside the existing Python repository. GraphQL/FastAPI calls them in-process; the MCP server calls the same functions. Repositories require trusted tenant context and own SQL, bounds, mapping, and evidence.

## Alternatives

- Keep duplicate SQL: continued drift and inconsistent security.
- Have GraphQL call MCP: adds network/protocol failure to an in-process path.
- Create a separate repository service/repo immediately: speculative operational overhead.

## Consequences

Metric semantics have one implementation and are testable. Incremental extraction requires parity fixtures because current paths sometimes calculate metrics differently.

## Security implications

Scope injection and ownership checks occur once below both interfaces. Repositories never accept model/client tenant overrides.

## Rollback implications

Resolvers can be migrated family-by-family. A failing migration can revert that resolver to captured legacy behavior while the shared contract is corrected.
