# DTC Analytics MCP implementation handoff

## Implemented boundary

The governed runtime lives in `src/dtc_mcp`. It reuses the existing ClickHouse client builder and adds a dedicated production configuration prefix (`DTC_MCP_CH_DB_*`) for a SELECT-only principal. Repository parameters use driver-supported binding; customer IDs never enter SQL text.

Repository modules:

- `fleet_repository.py`: fleet snapshot, top DTCs, trends, high-risk vehicles.
- `vehicle_repository.py`: vehicle health, faults, and timeline.
- `dtc_repository.py`: DTC fleet impact, affected vehicles, co-occurrence, and code information.
- `maintenance_repository.py`: maintenance priority.
- `schema_repository.py`: approved catalog, schemas, metrics, and compatibility checks.

The initial production tool set is intentionally the eight tools approved in the implementation prompt. DTC code lookup and schema operations are separate catalog tools. Dynamic SQL is a disabled-by-default fallback validated with SQLGlot's ClickHouse AST.

## Extracted query locations

The shared equivalents were extracted from the inline query families in `src/ai_analyst.py` (`_tool_get_fleet_health` through `_tool_get_dtc_fleet_impact`) and the GraphQL resolver families in `src/graphql_schema.py` (`fleet_kpis`, `fleet_trend`, `top_dtc_codes`, vehicle/DTC/co-occurrence/impact/maintenance resolvers).

## Direct callers not yet migrated

- GraphQL resolvers still use their existing direct SQL to preserve the current schema and response semantics. Their contract baseline is in `tests/fixtures/dtc_contract_baseline.json`.
- AI Analyst `direct` mode retains the legacy direct handlers for rollback compatibility.
- AI Analyst `shadow` compares MCP results but returns the direct result.
- AI Analyst `mcp` mode routes governed data calls through MCP and does not invoke the direct handlers unless the separately configured fallback is explicitly enabled.
- Conversation storage, evaluation storage, ingestion, and Airflow remain separate direct/database paths outside the MCP data-tool boundary.

## Local commands

```powershell
python -m pip install -r requirements.txt
python scripts/check_dtc_mcp_schema.py
python -m src.dtc_mcp
```

For Streamable HTTP:

```powershell
$env:DTC_MCP_TRANSPORT = "streamable_http"
python -m src.dtc_mcp
```

Run tests:

```powershell
python -m pytest tests/unit -q -m "not integration"
python -m pytest -q
```

Use MCP Inspector against the stdio command `python -m src.dtc_mcp` or the configured `/mcp` Streamable HTTP endpoint. Application logs go to stderr in stdio mode.

## Security and operations

- Production HTTP identity is a signed service envelope and is never accepted from model-visible tool arguments.
- Multi-turn evidence state is HMAC-signed and checked against an opaque authenticated tenant-scope fingerprint before reuse; configure `DTC_MCP_CONVERSATION_STATE_HMAC_SECRET` consistently across API workers.
- Stdio development identity requires `DTC_MCP_DEV_CONTEXT_ENABLED=true` plus explicit user, tenant, customer mapping, roles, and scopes. It is rejected in production.
- Runtime limits cover rows, bytes, timeout, lookback, approved database, and approved tables.
- No cache is used in v1. Evidence reports `cache_status=disabled`, avoiding cross-tenant cache risk until measurements justify a bounded tenant-keyed cache.
- Rollback sets `DTC_DATA_ACCESS_MODE=direct` and disables the MCP deployment. Direct fallback remains disabled unless `DTC_MCP_DIRECT_FALLBACK_ENABLED=true`.

## Compatibility risks

- Direct and repository aggregate semantics may differ until each GraphQL resolver is migrated under its contract fixture.
- Production identity-to-`clientLoginId` mapping must be supplied by the trusted platform identity layer.
- The external Airflow producer owns freshness and schema evolution; startup compatibility checks must remain a deployment gate.
- Shadow mode doubles reads and is rejected in production configuration.
