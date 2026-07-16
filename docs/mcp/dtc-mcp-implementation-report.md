# DTC Analytics MCP implementation report

Date: 2026-07-15

OpenSpec: `dtc-analytics-mcp-v1`

Taskmaster tag: `dtc-mcp-v1`
Recommendation: **NOT READY for production/GA; ready for CI and controlled non-production verification**

## Architecture delivered

GraphQL and MCP now share typed, read-only repositories while keeping their request paths separate. GraphQL derives tenant scope in FastAPI context and calls repositories directly. In MCP mode the AI Analyst calls the governed MCP service, which enforces trusted identity, scope, bounded ClickHouse execution, optional Redis caching, evidence, and auditing. Airflow remains unchanged and owns analytics production/table lifecycle.

The service supports local stdio and internal Streamable HTTP, a dedicated non-root container, health/readiness, bounded workers/concurrency/resources, internal-only Compose networking, optional Redis, and existing-ingress TLS termination. Direct AI data access remains as the explicit rollback/shadow path; it is not used silently in MCP mode.

## Tools and resources delivered

Delivered domain tools cover fleet health, top DTCs, trends, vehicle health/faults, DTC impact, co-occurrence, maintenance priority, and DTC reference information. Restricted validated SQL is separately scoped and disabled by default. Governed resources expose approved tool/catalog/schema/metric metadata only.

All successful calls return typed evidence with approved tables, server scope reference, filters/window/as-of, row count/truncation/limit, query hash, latency/trace, freshness, and cache metadata.

## Security and observability

- Signed production identity; explicit stdio development principal; model-visible inputs cannot set tenant/customer scope.
- Server-bound tenant predicates, ownership checks, approved tables, SELECT-only client path, `readonly=2`, row/byte/time limits.
- AST SQL policy rejects destructive, multi-statement, union, system/external/table-function, setting, wildcard, unapproved identifier, and tenant-override attempts.
- Tenant/checkpoint/schema-aware hashed cache keys; dynamic SQL uncached; Redis failures fail open.
- Complete redacted audit events with request/trace/session/run correlation, tool/version, status/latency/database/cache/rows/tables/query hash/error/transport/mode.
- Bounded P50/P95/P99 and success/failure/cache/rejection/fallback/shadow metrics; persistence and deep AI judge work are post-response.
- Origin validation, internal service networking, no new public ClickHouse/MCP port, no embedded secrets.

## Direct database access remaining

Legacy AI direct handlers remain for `DTC_DATA_ACCESS_MODE=direct` rollback and non-production shadow comparison. They are not invoked in normal MCP mode unless the separate explicit direct-fallback flag is approved and enabled. GraphQL intentionally calls shared repositories directly rather than MCP. Existing non-DTC application paths are outside this migration.

## Verification results

| Gate | Result |
|---|---|
| Python full local suite | 117 passed, 4 skipped, 4 deprecation warnings |
| MCP package coverage | 85.34% (80% gate passed) |
| MCP stdio protocol | Initialize/list/call/clean shutdown passed |
| HTTP auth/origin/health | Passed |
| GraphQL/API repository regression | Passed |
| Cache/security/SQL/audit/AI modes | Passed |
| Frontend production build | Passed; existing 824 kB chunk warning |
| OpenSpec strict JSON | Passed, zero issues |
| Python compile/import | Passed |
| Diff whitespace check | Passed |
| Secret scan | Passed |
| MCP dependency audit | No known vulnerabilities after `python-dotenv==1.2.2` update |
| Synthetic shadow comparison | 8/8 normalized fixture cases matched |
| Disposable ClickHouse E2E | Implemented in CI; skipped locally because Docker/ClickHouse is unavailable |
| MCP Docker image build | Implemented in CI; not run locally because Docker CLI is unavailable |

The four skips are the three disposable ClickHouse integration/E2E cases plus the pre-existing approved-real-ClickHouse integration placeholder. CI supplies a pinned ClickHouse LTS service, creates schema-compatible fixtures and a SELECT-only user, and runs the three new cases.

## Benchmark evidence

The historical direct baseline contains 895 turns: 35.272-second average, 73.015-second P95, 11,998 tokens/turn, 1.555 tools/turn, 1.670 SQL calls/turn, 0.9045 groundedness, and 0.529 pass rate.

The current deterministic fast-path microbenchmark ran 200 local turns: 0.0697 ms average, 0.1548 ms P95, zero LLM tokens, one tool/turn, zero analyst SQL calls, 100% fixture grounding/single-turn checks, and a separately exercised zero-token verified multi-turn repeat. This proves only the local fast-path mechanics. The populations and environments differ, so **no full end-to-end or production improvement is claimed**. Full current MCP model/shadow/load/cost comparison remains an external gate.

## Known limitations and production blockers

1. Run Linux CI to prove the pinned container build and disposable ClickHouse integration/E2E job.
2. Independently review production SELECT-only grants and install/rotate secrets in the approved secret manager.
3. Verify TLS ingress, trusted identity-header stripping, internal network policy, and production readiness endpoint behavior.
4. Run approved real-data direct/MCP shadow comparison across the golden set and resolve every P0/P1 mismatch.
5. Run the supplied non-production concurrent load/failure probe while collecting server CPU, memory, ClickHouse load/pool, cache, timeout, P50/P95/P99, and error metrics; approve SLOs.
6. Complete limited/customer pilots and the agreed stability window with zero cross-tenant findings before GA or legacy-path removal.

## Rollback

Set `DTC_DATA_ACCESS_MODE=direct` on the API deployment and roll/restart the API. Verify a known GraphQL query and AI question, stop MCP traffic if required, preserve audit/trace evidence, and diagnose the incident. Do not delete shared GraphQL repositories, the direct path, or rollback controls until the stability-window change is separately approved.

OpenSpec remains unarchived and Taskmaster task 8 remains in progress because the external production, load, pilot, and stability conditions have not passed.
