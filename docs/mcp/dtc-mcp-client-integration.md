# DTC MCP client integration

The FastAPI layer constructs `TenantContext` from verified identity and passes it out of band to `DTCMCPClient`. Never include customer/tenant identity in model tool arguments.

For local stdio set `DTC_MCP_TRANSPORT=stdio` and explicitly enable a development principal. For deployed service set `streamable_http`, an internal HTTPS URL, and the shared signing secret from the secret manager. The client signs the serialized context into `X-DTC-Identity` and `X-DTC-Identity-Signature` and propagates request/trace IDs.

Select the least-powerful route: reuse fresh signed conversation evidence; otherwise use one domain tool; fetch governed schema/catalog only when metadata is needed; use validated SQL only for an unusual approved question when the feature/scope is enabled. Validate the `ToolResponse` envelope and expose evidence/limitations to the answer.

MCP mode must never create a direct ClickHouse client. An MCP failure returns a safe limitation unless the separately approved `DTC_MCP_DIRECT_FALLBACK_ENABLED=true` rollback control is active. Shadow mode returns the direct result and records the MCP comparison; it is disabled in production.

The API contract remains additive: existing `text`, `chart`, `request_id`, `tool_results`, and other fields remain; conversation state/signature and post-response status are additional MCP metadata.
