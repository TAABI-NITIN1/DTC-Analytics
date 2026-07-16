# DTC Analytics MCP v1 PRD

Source of truth: `openspec/changes/dtc-analytics-mcp-v1/` (proposal, design, specifications, and tasks). This file is a Taskmaster ingestion view and must not diverge from OpenSpec.

## Objective

Route DTC AI Analyst data fetching through a governed, read-only MCP tool boundary while preserving existing GraphQL/frontend behavior. GraphQL and MCP share typed repositories; Airflow remains the separate analytics producer.

## Required outcomes

- Authenticated server context supplies user, tenant/customer, roles/scopes, and trace identity; the model cannot choose or override tenant.
- Live verification confirms `clientLoginId` on all tenant-bearing aggregates and `customer_name` on vehicle/fault mappings; repository catalogs must replace stale "not available" metadata and scope both keys from trusted identity.
- No destructive or cross-customer SQL. All queries have row/byte/time bounds.
- Ten domain tools are preferred over a disabled-by-default validated SQL fallback.
- Every tool has input/output schemas and returns safe evidence metadata.
- Stdio supports local protocol tests; Streamable HTTP supports internal production.
- The production dependency is pinned to stable `mcp==1.28.1`; v2 prereleases are excluded.
- The AI supports `direct|shadow|mcp`; direct fallback after MCP failure is separately explicit.
- Existing GraphQL schema/frontend behavior remains compatible and GraphQL calls repositories directly.
- Every call is audited/traced without secrets or unsafe raw SQL.

## Dependency order

1. Security, identity, deployed schema verification, and isolation tests.
2. Shared read-only repositories and evidence.
3. Backward-compatible GraphQL migration.
4. MCP stdio server, domain tools, and schema resources.
5. Restricted validated SQL.
6. Streamable HTTP, production auth, metrics, audit, and tracing.
7. AI client and direct/shadow/MCP feature modes.
8. Evaluation, load/failure testing, rollout, and rollback.

## Acceptance gates

- All OpenSpec scenarios pass, including adversarial SQL and two-tenant isolation tests.
- MCP mode performs no direct AI ClickHouse access.
- GraphQL contract fixtures and frontend build pass.
- No production secret is tracked or returned.
- Shadow mode is non-production by default and explicit fallback is off by default.
- OpenSpec strict validation passes.

## Taskmaster synchronization status

Taskmaster 0.43.1 is initialized with Codex rules and local file storage. The active `dtc-mcp-v1` tag contains eight non-duplicated tasks corresponding to the eight OpenSpec task groups, with a validated dependency chain `1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 8`. Task 1, security and contract foundations, is the first ready implementation task.

Taskmaster's local `parse-prd` model providers could not complete generation: its advertised Codex model IDs are unsupported for the available ChatGPT-backed Codex account, and the Claude Code structured-output invocation stalled. The approved PRD was therefore synchronized deterministically into Taskmaster's documented tagged `tasks.json` schema, followed by successful `tags use`, `generate`, `list --json`, and `validate-dependencies` commands.
