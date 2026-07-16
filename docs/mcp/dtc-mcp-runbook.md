# DTC MCP deployment and rollout runbook

## Deploy

1. Provision a ClickHouse user with SELECT only on the approved DTC tables. Store its password and the identity HMAC secret in the deployment secret manager, never the image or repository.
2. Set `DTC_MCP_CH_DB_*`, `DTC_MCP_AUTH_MODE=signed_hmac`, an exact internal origin allowlist, checkpoint, and optional Redis URL. Keep dynamic SQL and direct fallback false.
3. Build `Dockerfile.dtc-mcp`; CI performs this on Linux. The image runs as UID 10001 on pinned Python 3.11 and exposes port 8001 only to the internal network.
4. Configure the existing TLS ingress to proxy only `/mcp`, `/health`, and `/ready`; do not publish debug routes or trust client-supplied identity headers.
5. Start Redis, MCP, then the API using Compose/deployment readiness conditions. Verify `/health`, `/ready`, signed MCP initialize/list/call, audit events, and no cross-tenant fixture.

The documented starting limits are 1 worker, 100 concurrent requests, 1 CPU, 512 MiB, 15-second query timeout, 200 rows, 10 MB result bytes, and 30-second cache TTL. Tune only from approved load evidence. Uvicorn handles termination signals and drains workers; the orchestrator should allow at least the query timeout plus five seconds before force kill.

## Staged rollout

| Stage | Mode | Exit gate |
|---|---|---|
| Development | `shadow` | Golden parity and security pass |
| Internal QA | `shadow` | Approved real-data parity and SLO baseline |
| Limited pilot | `mcp` with direct rollback | No P0/P1; alerts within thresholds |
| Customer pilot | `mcp` | Tenant approval and stability window |
| General availability | `mcp` | Change approval after observation window |

Initial alert thresholds pending production baseline: success below 99%, P95 above 2 seconds for domain tools, tenant rejection spike, any cross-tenant finding, cache error above 5%, or shadow mismatch above 2%. Any cross-tenant or destructive-policy failure is an immediate stop/rollback regardless of rate.

## Rollback

Set the API deployment to `DTC_DATA_ACCESS_MODE=direct`, restart/roll the API, verify a known GraphQL and AI query, then disable MCP traffic while preserving logs. Do not remove the shared GraphQL repositories or direct path. Record incident request/trace IDs and keep Redis data isolated or flush only the dedicated MCP cache after change approval.
