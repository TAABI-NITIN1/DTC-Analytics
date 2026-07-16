# Project development instructions

- OpenSpec is the specification source of truth. Codex must read the relevant OpenSpec change before implementing it.
- Taskmaster is the implementation task tracker. Use the active `dtc-mcp-v1` tag for this change.
- Ponytail full mode is used to prevent over-engineering: reuse existing code and avoid speculative abstractions.
- Airflow is a separate deployment unit; do not move its DAGs, files, or runtime responsibilities.
- Preserve existing GraphQL and frontend behavior and contracts.
- All new data access must enforce tenant/customer isolation from trusted server-side identity.
- Taskmaster MCP and DTC Analytics MCP are different systems: Taskmaster MCP is development tooling only; DTC Analytics MCP is the product runtime data/tool server.
