## Why

The DTC AI Analyst currently executes direct Python/ClickHouse tool calls, including model-generated SQL, while customer scope is supplied by the client/model rather than enforced from trusted identity. A governed MCP boundary and shared repository layer are required before this access pattern can be safely operated or reused by other TAABI domain agents.

## What Changes

- Add a read-only DTC Analytics MCP server as the AI-facing data/tool boundary, using domain tools before restricted validated SQL.
- Add a narrow MCP client for the AI Analyst and `direct`, non-production `shadow`, and `mcp` migration modes with explicit fallback policy.
- Extract shared DTC repositories used directly by both GraphQL/FastAPI and the MCP server; GraphQL will not call MCP for backend data.
- Derive customer/tenant scope from authenticated server context; the model cannot provide or override it.
- Attach evidence metadata, bounded execution, audit events, metrics, tracing, and structured errors to every MCP call.
- Support stdio locally and Streamable HTTP for internal production deployment.
- Preserve the React/GraphQL/API contracts and leave Airflow as the separate analytics producer.
- Keep MCP v1 read-only. No production secret is stored in source control.

## Capabilities

### New Capabilities

- `dtc-mcp-server`: Server lifecycle, transport, read-only policy, and tool/resource registration.
- `dtc-domain-tools`: Typed business tools for fleet, vehicle, DTC, trend, correlation, and maintenance queries.
- `dtc-schema-resources`: Governed catalog and schema resources for approved DTC data.
- `tenant-isolation`: Trusted identity propagation and server-enforced customer boundaries.
- `validated-sql`: Disabled-by-default, restricted SQL fallback for questions no domain tool can answer.
- `evidence-contract`: Common provenance, scope, timing, truncation, and freshness metadata on results.
- `dtc-agent-mcp-client`: AI Analyst MCP client, feature modes, fallback, and compatibility behavior.
- `observability`: Auditing, metrics, trace correlation, redaction, and operational health.
- `migration-compatibility`: GraphQL compatibility, shared repositories, shadow comparison, rollout, and rollback.
- `cache-freshness`: Tenant-bound Redis caching, checkpoint invalidation, stale marking, and fail-open behavior.
- `ai-serving-optimization`: Fast/domain/deep paths, compact evidence, and asynchronous sampled evaluation.
- `mcp-deployment`: A dedicated non-root internal container, authenticated HTTP, origin policy, health, and rollback configuration.
- `quality-gates`: Layered tests, an 80% MCP/security coverage floor, disposable ClickHouse integration, CI, dependency audit, and secret scanning.

### Modified Capabilities

None. This repository had no existing OpenSpec capability specifications at change creation.

## Impact

Affected areas are `src/ai_analyst.py`, `src/graphql_schema.py`, `src/api_server.py`, ClickHouse client configuration, evaluation harnesses, dependency manifests, and a new package following the repository's Python conventions. The frontend GraphQL contract remains compatible. Airflow paths and ownership do not change. A stable MCP Python SDK version will be pinned under the existing dependency policy after compatibility verification.
