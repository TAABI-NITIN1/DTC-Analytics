## 1. Security and contract foundations

- [x] 1.1 Capture authorized GraphQL/AI contract fixtures for current fleet, vehicle, DTC, trend, co-occurrence, and maintenance results; record accepted parity tolerances.
- [x] 1.2 Verify deployed ClickHouse columns/grains for every `V2_TABLES` entry, especially aggregate `clientLoginId` support, and add a non-secret compatibility check.
- [x] 1.3 Define typed `TenantContext`, tool input/output, evidence, and error models; prove tenant/customer fields are absent from model-visible tool inputs.
- [x] 1.4 Define production identity-to-customer mapping and stdio development principal configuration; add authentication/scope rejection tests.
- [x] 1.5 Configure a dedicated read-only ClickHouse path with statement, row, byte, and timeout limits; test destructive/multi-statement rejection and cancellation.
- [x] 1.6 Add deterministic two-tenant fixtures/tests proving vehicle, aggregate, cache, fallback, and error paths cannot cross customer boundaries.

## 2. Shared repositories

- [x] 2.1 Create the minimal `src/dtc_mcp` package and repository execution boundary using existing `V2_TABLES`/ClickHouse utilities where safe.
- [x] 2.2 Extract fleet health, DTC distribution, fleet trends, and system health repositories with typed results, scope injection, bounds, and parity tests.
- [x] 2.3 Extract vehicle health and vehicle fault repositories with ownership checks and parity tests.
- [x] 2.4 Extract DTC detail, co-occurrence, and fleet-impact repositories; use scoped fact paths whenever aggregates cannot prove tenancy.
- [x] 2.5 Extract maintenance-priority repositories with customer-safe joins and deterministic limits/order.
- [x] 2.6 Return common evidence metadata from all repositories without exposing unsafe raw SQL or secrets.

## 3. GraphQL compatibility migration

- [x] 3.1 Add trusted identity to FastAPI/GraphQL request context and reject customer arguments outside the authenticated scope.
- [x] 3.2 Migrate fleet and trend resolvers to shared repositories; pass existing GraphQL contract fixtures.
- [x] 3.3 Migrate vehicle, DTC, co-occurrence, impact, and maintenance resolvers; secure identifier-only/global leakage paths.
- [x] 3.4 Confirm existing frontend queries/types and `/api/ai/chat` behavior remain backward compatible; run frontend build and API/GraphQL smoke checks.

## 4. MCP server, tools, resources, and evidence

- [x] 4.1 Pin the production/stable MCP Python SDK (`mcp==1.28.1`) compatible with Python 3.11 and document the exclusion of prerelease v2.
- [x] 4.2 Implement server lifecycle and stdio transport with explicit development identity and sanitized error mapping.
- [x] 4.3 Register the eight approved initial typed domain tools over shared repositories and validate every input/output/evidence schema.
- [x] 4.4 Implement governed schema/catalog resources with approved-object allowlists, scope classification, freshness, and schema mismatch handling.
- [x] 4.5 Add protocol tests for discovery, calls, invalid inputs, truncation, timeout, cancellation, destructive attempts, and tenant isolation.

## 5. Restricted validated SQL

- [x] 5.1 Keep dynamic SQL disabled by default and implement role/feature authorization plus domain-tool-before-SQL routing tests.
- [x] 5.2 Implement structural ClickHouse SQL parsing/policy for one SELECT/WITH statement, approved identifiers/functions, forbidden constructs, and server-injected tenant scope.
- [x] 5.3 Add adversarial SQL tests for comments, nesting, aliases, unions, table functions, system/external access, settings, multiple statements, destructive SQL, scope bypass, and excessive limits.
- [x] 5.4 Return bounded validated-SQL results through the common evidence/error/observability contracts.

## 6. Production transport and observability

- [x] 6.1 Implement internal Streamable HTTP transport with verified production identity propagation, role/scope checks, health/readiness, timeout, and cancellation.
- [x] 6.2 Emit redacted audit events and metrics for identity/scope reference, tool/version, trace, policy, duration, rows, truncation, cache, error, transport, and mode.
- [x] 6.3 Propagate trace/correlation context from API through client/server/repository to ClickHouse and test success, rejection, timeout, and failure traces.
- [x] 6.4 Add bounded tenant-aware caching only if measurements justify it; otherwise document the no-cache v1 decision and tests.

## 7. AI client and migration modes

- [x] 7.1 Implement the narrow AI MCP client for stdio/Streamable HTTP with response validation, timeout, cancellation, and trusted out-of-band identity.
- [x] 7.2 Add `DTC_DATA_ACCESS_MODE`, transport URL/timeout/max-row/dynamic-SQL settings with safe defaults and configuration validation.
- [x] 7.3 Implement direct mode unchanged and sampled non-production shadow comparison that never changes the user-visible direct response.
- [x] 7.4 Implement MCP mode so the analyst creates no direct ClickHouse client and uses only registered MCP tools for data.
- [x] 7.5 Implement separately configured fallback; prove failure performs no direct query by default and any enabled fallback preserves scope and audit trace.

## 8. Evaluation, rollout, and operations

- [x] 8.1 Extend evaluation with direct/shadow/MCP parity, evidence, tenant-isolation, tool-choice, latency, and failure metrics.
- [ ] 8.2 Run unit, GraphQL/API, MCP protocol, ClickHouse integration, frontend build, and evaluation baselines; resolve all P0/P1 failures.
- [ ] 8.3 Load/failure-test row/byte/time budgets and Streamable HTTP behavior; establish SLOs and alert thresholds.
- [x] 8.4 Document deployment, identity/secret setup, feature-mode rollout, explicit fallback, rollback drill, and incident diagnosis.
- [ ] 8.5 Roll out MCP mode by approved tenant after parity/security gates; retain direct rollback for the observation window, then create a separate task to remove legacy AI SQL.

## 9. Cache, freshness, and integrated audit

- [x] 9.1 Implement optional tenant/checkpoint/schema-aware Redis caching for approved repository operations; keep dynamic SQL uncached and fail open.
- [x] 9.2 Add TTL, tenant separation, checkpoint invalidation, stale marking, cache evidence, unavailable-Redis, and safe-key tests.
- [x] 9.3 Integrate complete redacted MCP audit events and bounded metrics with the existing observability store using non-blocking persistence.
- [x] 9.4 Test success/failure correlation, configurable redaction, logger failure, rejected SQL, tenant denial, and clean stdio output.

## 10. AI serving optimization

- [x] 10.1 Add deterministic fast paths, domain/deep routing policy, short serving prompt, on-demand metadata, bounded evidence summaries, and tenant-bound conversation reuse.
- [x] 10.2 Remove synchronous judging/persistence from the customer path and add sampled post-response scheduling tests.
- [x] 10.3 Establish and run a versioned before/after benchmark for latency, P95, tokens, tools, SQL, repeats, errors, groundedness, multi-turn quality, and cost per successful answer.

## 11. MCP deployment

- [x] 11.1 Add the pinned, non-root, minimal MCP container and optional Redis/internal-network Compose services without modifying Airflow ownership.
- [x] 11.2 Add signed production auth, Origin validation, safe host/port/worker/concurrency configuration, health/readiness, and environment examples.
- [ ] 11.3 Document TLS ingress, resource limits, secrets, shutdown, deployment, and rollback; verify the image build in CI or an available Docker host.

## 12. Enterprise verification gates

- [x] 12.1 Add a meaningful 80% MCP/security coverage gate and pin test/audit dependencies.
- [x] 12.2 Add protocol, security, GraphQL, cache, AI-mode, and disposable ClickHouse integration/E2E suites including tenant, empty, timeout, and unavailable paths.
- [x] 12.3 Add CI for OpenSpec, syntax/import/diff, tests/coverage, frontend, container, secret scanning, and vulnerability audit without production secrets.
- [ ] 12.4 Run the disposable ClickHouse and container-build jobs on an available Linux/Docker executor.

## 13. Shadow migration and operations

- [x] 13.1 Create the complete schema-compatible golden question set and normalized mismatch classifier.
- [x] 13.2 Run local direct/MCP shadow comparisons and safe performance/cache/failure measurements; publish measured evidence without extrapolating to production.
- [x] 13.3 Publish the seven required MCP architecture, tools, security, runbook, troubleshooting, client, and production-checklist documents.
- [x] 13.4 Define development, QA, limited pilot, customer pilot, GA, stability-window, alert, incident, and direct rollback procedures.

## 14. Final verification

- [x] 14.1 Run the full local suite, OpenSpec strict/status, Taskmaster list, frontend build, dependency/secret scans, and diff checks.
- [x] 14.2 Reconcile every acceptance criterion and issue the enterprise-readiness report with architecture, controls, remaining direct access, tests, benchmarks, limitations, rollback, blockers, and READY/NOT READY recommendation.
