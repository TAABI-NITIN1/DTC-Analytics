# ADR-DTC-004: Tenant context from authentication

Status: Accepted for DTC MCP v1 design

## Context

The frontend and model currently supply `customer_name`. This is a filter, not authorization, and identifier/global aggregate paths can bypass it.

## Decision

Production creates `TenantContext` from verified identity containing user ID, internal tenant ID, server-mapped customer keys, roles/scopes, and trace ID. Tenant fields are absent from model-visible tool schemas. Stdio requires an explicit development principal.

## Alternatives

- Trust `customer_name` request input: permits impersonation.
- Let the model select tenant with allowlist prompting: model instructions are not an authorization boundary.
- Global service account with query-convention filtering: lacks defense in depth.

## Consequences

An identity/mapping contract with the platform is required. Queries, caches, logs, resources, and fallback become consistently scoped.

## Security implications

Cross-customer access is rejected before query execution; unknown identifiers reveal no existence detail. A least-privilege DB principal provides an additional boundary.

## Rollback implications

Rollback to direct mode must preserve authenticated context and cannot restore unauthenticated tenant selection in production.
