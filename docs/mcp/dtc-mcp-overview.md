# DTC Analytics MCP overview

The service is the governed, read-only data boundary for the DTC AI Analyst. GraphQL does not call MCP; both paths reuse the same repositories.

```text
React -> FastAPI/GraphQL -> shared repositories -> ClickHouse
AI Analyst -> MCP client -> DTC MCP -> shared repositories -> ClickHouse
                                      -> optional Redis result cache
```

`src/dtc_mcp` owns identity enforcement, tools/resources, repository execution limits, evidence, caching, validated SQL policy, and audit events. Airflow remains the separate analytics producer and table owner.

Data modes are `direct` (rollback/default), `shadow` (non-production comparison), and `mcp`. In MCP mode the analyst creates no ClickHouse client and cannot silently fall back unless `DTC_MCP_DIRECT_FALLBACK_ENABLED=true` is explicitly set. Legacy direct AI functions remain only for rollback/shadow. GraphQL uses repositories directly.

The production transport is internal Streamable HTTP. Local development and protocol tests use stdio. The SDK is pinned to stable MCP v1; dynamic SQL and Redis are disabled by default.

Known boundary: the repository contains local fixtures, CI, and a fast-path microbenchmark. Production credentials, real shadow traffic, approved load testing, container execution on a Docker host, pilot rollout, and the stability window are external gates; readiness therefore remains **NOT READY** for general availability.
