# ADR-DTC-005: Stdio and Streamable HTTP transports

Status: Accepted for DTC MCP v1 design

## Context

Local development benefits from process-local transport, while production requires an internal network service with identity propagation and operations support.

## Decision

Support stdio for local development/protocol tests and Streamable HTTP for internal production. Keep tool/resource contracts transport-independent. Do not use deprecated HTTP+SSE as the primary transport.

## Alternatives

- Stdio only: unsuitable for independently deployed production clients.
- HTTP only: heavier local test/setup and weaker process-isolated protocol tests.
- HTTP+SSE primary: deprecated direction and avoidable migration debt.

## Consequences

Two transport adapters require contract tests, but share server logic and schemas. Production gains normal health, auth, timeout, tracing, and scaling controls.

## Security implications

Stdio requires configured development identity. Streamable HTTP requires verified internal authentication, TLS/network policy as appropriate, request limits, cancellation, and trace propagation.

## Rollback implications

Transport can be switched by configuration. Service rollback uses direct analyst mode without affecting GraphQL.
