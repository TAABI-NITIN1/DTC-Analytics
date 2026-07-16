# DTC MCP production checklist

## Locally verified

- [x] Shared repositories and GraphQL compatibility/security tests
- [x] Typed tools/resources/evidence and disabled-by-default validated SQL
- [x] Tenant-safe cache with fail-open behavior and freshness evidence
- [x] Complete redacted audit events, metrics, async persistence, and clean stdio
- [x] MCP-only analyst behavior, evidence reuse, fast path, and async judge scheduling
- [x] Signed HTTP identity, Origin validation, safe defaults, and no public Compose port
- [x] 80% coverage gate (latest local MCP package result: 85%)
- [x] OpenSpec strict validation, syntax, secret scan, and MCP dependency audit
- [x] Golden question set and explicitly scoped fast-path benchmark
- [x] Direct rollback retained; Airflow unchanged

## External promotion blockers

- [ ] CI Linux job proves disposable ClickHouse E2E and Docker image build
- [ ] Production SELECT-only ClickHouse grants independently reviewed
- [ ] Secrets installed/rotated through the approved secret manager
- [ ] Existing ingress TLS and trusted-header stripping verified
- [ ] Approved real-data direct/MCP shadow comparison meets tolerance
- [ ] Non-production concurrent load/failure test establishes CPU, memory, pool, P50/P95/P99, cache, timeout, and error SLOs
- [ ] Limited tenant pilot approved and monitored with zero P0/P1
- [ ] Observation/stability window completed and change approval recorded

Recommendation: **NOT READY for production/GA** until every external blocker is checked. The implementation is ready for CI and controlled non-production verification.
