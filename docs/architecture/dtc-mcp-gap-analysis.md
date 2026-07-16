# DTC MCP gap analysis

## P0 security/correctness blockers

1. **Tenant identity is model/client supplied.** React sends `customer_name`; FastAPI and GraphQL accept it without authenticated identity or role/scope checks. A model can also supply customer arguments to AI tools. The server must derive tenant/customer scope from trusted auth context and reject mismatches.
2. **Aggregate and identifier leakage.** `dtc_fleet_impact` and `dtc_cooccurrence` GraphQL resolvers ignore their customer argument and fail to filter the deployed `clientLoginId` column. Other identifier-only resolvers can fetch a vehicle by `uniqueid` without customer verification.
3. **SQL validation is lexical, not a security boundary.** AI `run_sql` uses regex/table allowlists and literal scope checks. It is not a complete ClickHouse parser/policy engine and cannot reliably prove read-only, single-statement, scoped semantics.
4. **Database credentials are not demonstrably read-only.** Client creation sets neither read-only ClickHouse settings nor a read-only service account policy. The generic adapter can execute commands and inserts.
5. **No deterministic isolation tests.** Existing evaluation heuristics cannot establish that cross-customer reads are impossible.

## P1 required for MCP v1

- Extract typed, reusable repositories for domain queries shared by GraphQL and MCP.
- Define trusted request identity (`user_id`, `customer_id/customer_name`, roles/scopes, trace ID) and server-side scope injection.
- Implement read-only business tools before dynamic SQL, each with input/output JSON schemas, enforced maximum rows and timeout, and evidence metadata.
- Add catalog/schema resources that expose only approved tables/columns and scope semantics.
- Add stdio transport for local protocol tests and Streamable HTTP for internal production.
- Add a narrow AI MCP client and `direct|shadow|mcp` feature mode; never silently fall back unless configured.
- Preserve current GraphQL schema and frontend behavior while changing resolvers to use repositories.
- Create unit/contract/integration tests for validation, repositories, tool schemas, and tenant isolation.

## P2 production hardening

- Dedicated least-privilege ClickHouse user, server query settings, quotas, maximum bytes/rows, cancellation, and pool limits.
- Structured error taxonomy with no SQL/schema/internal detail leakage.
- Audit logs and metrics for tool, tenant, user, trace, duration, row count, cache status, and policy outcome; redact raw SQL/identifiers where needed.
- Bounded tenant-aware caching with versioned keys and invalidation/TTL.
- Shadow comparison dashboards, SLOs, load tests, failure injection, and rollback drills.
- Dependency lock/pin policy and SDK compatibility/protocol tests.

## P3 future optimization

- Reuse the governed MCP/repository pattern for other TAABI domain agents.
- Reconcile and use existing `clientLoginId`-partitioned aggregates safely; precompute different aggregates only if measured query needs remain unmet.
- Semantic catalog discovery and cost-based tool routing after production evidence shows need.
- Cross-service distributed cache only if local/process caching and ClickHouse performance prove insufficient.

## Reusable modules

- `src/clickhouse_utils_v2.py`: canonical logical-to-physical table names.
- `src/schema_registry.py` and `docs/rag/`: approved schema/catalog seed (must be reconciled with deployed metadata).
- AI tool names/descriptions and result/evidence tracing concepts in `src/ai_analyst.py`.
- Observability persistence/event concepts in `src/observability_store.py`.
- Evaluation scope gates and direct-call/API harnesses.

## Modules needing refactoring

- `src/ai_analyst.py`: separate planning/client behavior from SQL construction/execution; remove model-controlled tenant fields in MCP mode.
- `src/graphql_schema.py`: move SQL/result shaping into repositories and add trusted identity to resolver context.
- `src/clickhouse_utils.py`: add read-only connection/query policy and true bounded execution; manual parameter rendering is not sufficient as the policy layer.
- `src/api_server.py`: authenticate and construct trusted tenant context.
- Evaluation configuration: distinguish privileged dataset enumeration from tenant-scoped agent evaluation.

## Migration risk summary

The largest compatibility risk is that current AI and GraphQL implementations compute similar metrics differently even though deployed aggregates already carry `clientLoginId`. Repository contract tests must capture current GraphQL outputs before consolidation. Shadow mode doubles reads and is therefore non-production by default.
