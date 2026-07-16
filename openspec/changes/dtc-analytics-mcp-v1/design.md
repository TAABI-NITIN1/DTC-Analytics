## Context

The React dashboards use GraphQL/FastAPI, while the DTC AI Analyst and GraphQL resolvers independently construct ClickHouse SQL. Customer scope is currently an optional client/model value. Some aggregate tables lack a documented customer key, and the generic AI SQL fallback relies on lexical checks. Airflow remains a separate producer deployment and owns analytical table lifecycle.

## Goals / Non-Goals

**Goals:**

- Make MCP the governed AI-facing boundary for read-only DTC data.
- Share repository functions between GraphQL and MCP without changing frontend contracts.
- Enforce trusted tenant scope, bounded execution, evidence, auditing, and traceability.
- Support local stdio and production Streamable HTTP with direct/shadow/MCP migration modes.
- Establish a minimal package pattern reusable by later TAABI domain agents.

**Non-Goals:**

- Moving Airflow, changing producer DDL, replacing GraphQL, or routing GraphQL through MCP.
- Write tools, autonomous maintenance actions, broad arbitrary SQL, or a public internet MCP endpoint.
- New caches/frameworks/service repositories without measured need.

## Decisions

### 1. Current and target architecture

Current: AI Analyst, GraphQL, and evaluation reach direct Python/SQL utilities, then ClickHouse. Target:

```text
React -> GraphQL/FastAPI -> shared DTC repositories -> ClickHouse
DTC AI Analyst -> MCP client -> DTC MCP server -> same repositories -> ClickHouse
```

GraphQL calls repositories directly because an in-process backend call is simpler and avoids needless protocol latency/failure. MCP is the trust/tool boundary for agents, not a universal data proxy.

### 2. Component placement and boundaries

Use an in-repository Python package adapted to the existing flat `src` layout:

```text
src/dtc_mcp/
  server.py
  client.py
  tools.py
  resources.py
  repositories.py
  security.py
  models.py
  observability.py
```

Split further only when a file becomes genuinely unwieldy. Repositories own SQL and result mapping. Security owns trusted context/policy. Tools own MCP schemas/orchestration. The client owns transport and feature-mode behavior. The server owns lifecycle/registration only.

### 3. Shared repository layer

Repositories accept a required `TenantContext` plus typed query inputs. They inject customer predicates/joins and never accept a raw customer override. They return domain rows plus query evidence. Existing AI/GraphQL SQL is consolidated one query family at a time with snapshot/contract tests. Table names reuse `V2_TABLES`; schema information starts from `SCHEMA_REGISTRY` but must be checked against deployed metadata.

### 4. MCP server responsibilities

The server authenticates/receives trusted identity, constructs request context, registers typed tools/resources, applies authorization, bounds/executes repository calls, maps errors, emits audit/metrics/traces, and returns evidence. It is stateless aside from optional bounded tenant-aware result caching.

### 5. MCP client responsibilities

The AI client creates calls from approved tool schemas, propagates identity/trace metadata from the API (never from the model), enforces timeout/cancellation, validates response envelopes, records observability, and implements explicit feature/fallback policy. It does not generate SQL or alter tenant scope.

### 6. Tenant and authentication context

`TenantContext` contains `user_id`, internal `tenant_id`, mapped customer key(s), roles/scopes, and `trace_id`. Local stdio uses an explicit development principal configured outside model input. Production HTTP identity is verified by the hosting/auth layer and propagated in protected headers/token claims. Tool schemas omit tenant/customer fields. Any conflicting client/model field is rejected. Tenant isolation applies to data, cache keys, logs, resources, and fallback.

### 7. Read-only ClickHouse access

Production uses a dedicated read-only ClickHouse principal plus server-side read-only settings. Repository execution permits SELECT/WITH only, one statement, approved tables/columns, maximum rows/bytes/time, and cancellation. Limits are applied structurally/settings-side, not only by searching query text. The general adapter's command/insert capability is not exposed through the MCP server.

### 8. Domain tool contracts

The initial production tools correspond to the eight approved analyst needs: fleet health, DTC distribution, fleet trends, vehicle health, vehicle faults, DTC fleet impact, DTC co-occurrence, and maintenance priority. DTC code information and schema lookup are governed catalog tools rather than core tenant-data tools. System health and high-risk vehicle repository operations remain available for a later explicitly approved tool-contract expansion. Every tool has versioned input/output JSON schemas and bounded filters. Customer is absent from model-visible inputs. Domain tools are selected whenever capable; dynamic SQL is not a shortcut for existing domain behavior.

### 9. Evidence contract

Every success returns `data` and evidence containing tool/contract version, approved source tables, server-applied scope identifier (non-secret), query window/as-of time, row count, truncation, cache status, duration, and trace ID. It does not expose credentials or unsafe raw SQL. Errors retain trace ID and stable code.

### 10. Validated SQL fallback

`DTC_MCP_DYNAMIC_SQL_ENABLED=false` by default. When explicitly enabled for an authorized role, a real parser/AST or an equivalently robust ClickHouse-aware policy must prove: one read-only statement, approved identifiers/functions, mandatory server-injected scope, no comments/settings/external functions/table functions/system tables, and enforced bounds. SQL supplied by the model never contains tenant values. Reject rather than guess when proof fails.

### 11. Schema/catalog resources

Resources expose versioned approved table/column descriptions, grains, join paths, freshness, and scope classification. Tenant callers cannot enumerate unapproved/system objects or discover other customer identities. Deployed-schema compatibility is checked at startup/health in non-secret form; mismatches fail affected tools safely.

### 12. Error model

Stable codes: `INVALID_ARGUMENT`, `UNAUTHENTICATED`, `FORBIDDEN`, `SCOPE_VIOLATION`, `QUERY_REJECTED`, `TIMEOUT`, `UPSTREAM_UNAVAILABLE`, `SCHEMA_MISMATCH`, and `INTERNAL`. Client messages contain no SQL, credentials, stack traces, or cross-tenant facts. Retryability is explicit.

### 13. Caching

Measured high-frequency fleet, DTC, vehicle, and maintenance reads justify an optional bounded Redis result cache for multi-worker deployments. It is disabled when `REDIS_URL` is absent and fails open when Redis is unavailable. Keys are SHA-256 references over tenant/customer scope, operation, normalized inputs, query hash, row limit, schema version, and analytics checkpoint; raw customer identifiers and secrets never appear in Redis keys. A conservative 30-second TTL is the fallback while the producer checkpoint is unknown. Dynamic SQL is never cached. Evidence labels hit/miss/error, key hash, checkpoint, age, saved latency, and freshness.

### 14. Audit logging, metrics, and tracing

Each call records timestamp, tool/version, authenticated user/tenant, roles/scopes, transport, trace/correlation ID, policy decision, duration, row count, truncation/cache flags, error code, and mode. Sensitive inputs and raw SQL are redacted/hashed according to policy. Metrics cover latency, calls, errors, rejection, timeouts, rows, cache, and shadow mismatch. Trace context crosses API, client, server, repository, and ClickHouse.

Heavy audit persistence and AI judging are scheduled after the customer response. The analyst uses deterministic zero-token fast paths for DTC definitions, simple fleet summaries, and verified conversation evidence. Normal requests select the smallest domain-tool set, schema resources are loaded on demand, and only bounded representative evidence is sent to reasoning models.

### 15. Transport

Stdio is used for local development and protocol/contract tests with a configured development identity. Streamable HTTP is used for internal production. Deprecated HTTP+SSE is not the primary transport. Transport does not change tool/resource contracts.

The deployment is a dedicated Python 3.11 non-root container on an internal network. HTTP requires signed identity, validates any supplied Origin, exposes only health/readiness plus MCP, and relies on the existing ingress for TLS. CI validates OpenSpec, syntax, secrets, dependency vulnerabilities, tests with an 80% MCP coverage floor, disposable ClickHouse integration/E2E, frontend build, and container build.

### 16. Feature flags and migration

```text
DTC_DATA_ACCESS_MODE=direct|shadow|mcp
DTC_MCP_TRANSPORT=stdio|streamable_http
DTC_MCP_URL=
DTC_MCP_TIMEOUT_SECONDS=
DTC_MCP_MAX_ROWS=
DTC_MCP_DYNAMIC_SQL_ENABLED=false
```

`direct` preserves the current path temporarily. `shadow` returns direct results and compares a non-user-visible MCP call; it is disabled in production by default because it doubles queries. `mcp` uses only MCP for analyst data. MCP failure falls back to direct only with a separate explicit fallback flag and the same trusted scope.

### 17. Rollback

Rollback changes `DTC_DATA_ACCESS_MODE` to `direct` and disables the MCP deployment; repository-backed GraphQL remains compatible. No destructive data migration is introduced. Direct mode removal occurs only after isolation tests, parity thresholds, SLOs, and an observation window pass.

### 18. Dependency/version policy

Pin `mcp==1.28.1`, the production/stable v1 release verified on 2026-07-15 from the official PyPI package metadata and MCP Python SDK repository. It supports Python 3.11 and both required transports. The v2 line is beta/pre-release and is excluded from production; any v2 experiment requires a separate change and compatibility evaluation. Reassess the pin deliberately after stable v2 is released rather than accepting an automatic major upgrade.

### 19. Rejected alternatives

- GraphQL calling MCP: rejected due to needless network/protocol coupling.
- MCP wrapping existing AI functions unchanged: rejected because model-controlled scope and duplicate SQL remain.
- Dynamic SQL as the primary interface: rejected because domain tools are safer, stable, observable contracts.
- Separate repository/service repo now: rejected because the existing package and deployment can host the minimal boundary; split only on proven ownership/deployment need.
- HTTP+SSE primary transport: rejected in favor of Streamable HTTP.

## Risks / Trade-offs

- Repository documentation has stale customer-key metadata -> use the live-verified `clientLoginId` aggregate scope, correct the catalog, and retain compatibility checks; use scoped facts only when aggregate semantics cannot satisfy a tool.
- Consolidation may change dashboard metrics -> capture GraphQL contract fixtures and migrate resolver families incrementally.
- Shadow mode doubles load -> non-production default, sampling, strict budgets.
- Direct rollback retains legacy risk -> time-box it, gate access, and remove after exit criteria.
- SDK/protocol evolution -> pin stable version and run transport contract tests.
- Evidence/audit metadata can leak identifiers -> redact, minimize, and authorize access.

## Migration Plan

1. Add identity/security models, read-only query policy, and isolation tests.
2. Extract repository families with parity tests; switch GraphQL internally without schema changes.
3. Add MCP server/domain tools/resources over repositories and run stdio contracts.
4. Add Streamable HTTP auth, observability, limits, and failure tests.
5. Add AI client in `direct`; enable sampled non-production `shadow`; resolve parity gaps.
6. Roll out `mcp` by tenant with explicit fallback disabled by default; monitor SLO/security gates.
7. Remove generic/direct AI SQL after observation and rollback criteria pass.

## Open Questions

- Establish the canonical immutable authenticated tenant ID to live-verified `clientLoginId`/`customer_name` mapping with platform owners.
- Select the stable MCP SDK version at implementation time using the project's compatibility check.
- Define production identity provider/header or token claim contract with platform owners.
- Establish parity tolerances and freshness source from the external Airflow producer.
