# ADR-DTC-003: Domain tools before dynamic SQL

Status: Accepted for DTC MCP v1 design

## Context

The analyst exposes proven business tools and a generic model-generated SQL fallback. Lexical validation cannot establish complete ClickHouse safety or tenant scope.

## Decision

Versioned domain tools are the primary interface. Dynamic SQL is disabled by default, role-gated, used only for uncovered questions, structurally validated, server-scoped, read-only, and bounded.

## Alternatives

- Dynamic SQL only: flexible but unsafe and unstable.
- No fallback ever: safest, but may unnecessarily block legitimate uncovered analysis after policy maturity.
- Regex allowlist: inadequate for aliases, nesting, functions, settings, and parser ambiguity.

## Consequences

Common questions are predictable and observable. Some novel analysis is unavailable until a domain tool or approved SQL path exists.

## Security implications

Destructive/multi-statement/unapproved SQL is rejected before execution; tenant predicates come only from trusted context. Ambiguous SQL fails closed.

## Rollback implications

The SQL fallback can be disabled independently. Domain tools and direct rollback do not depend on it.
