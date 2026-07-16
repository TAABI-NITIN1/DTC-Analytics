# DTC MCP tools and resources

| Tool | Scope | Purpose | Cache |
|---|---|---|---|
| `get_fleet_health_summary` | `dtc:fleet:read` | Fleet health aggregate | Yes |
| `get_top_dtcs` | `dtc:fleet:read` | Ranked DTC distribution | Yes |
| `get_fault_trends` | `dtc:fleet:read` | Bounded dated trend | Yes |
| `get_vehicle_health` | `dtc:vehicle:read` | One owned vehicle | Yes |
| `get_vehicle_faults` | `dtc:vehicle:read` | Bounded fault episodes | No |
| `get_dtc_fleet_impact` | `dtc:fleet:read` | Tenant DTC impact | Yes |
| `get_dtc_cooccurrence` | `dtc:fleet:read` | Correlation pairs, not causation | No |
| `get_maintenance_priority` | `dtc:maintenance:read` | Operational priority ranking | Yes |
| `get_dtc_code_info` | `dtc:schema:read` | Global DTC reference | Yes |
| `run_validated_dtc_sql` | `dtc:sql:execute` | Restricted unusual-query fallback | Never/default off |

Catalog helpers list only approved analytics tables, return approved schemas, and define governed metrics. Resources expose the versioned tool catalog and schema catalog without unrestricted ClickHouse introspection.

Every data result has a typed envelope: status, bounded rows/count, truncation, limitations, stable error, request ID, and evidence. Evidence includes tables, query type/hash/window, server-applied filters, freshness/as-of, non-secret scope reference, effective limit, latency, trace ID, and cache metadata.

Model-visible inputs contain business filters only. Tenant/customer fields are supplied out of band from verified identity.
