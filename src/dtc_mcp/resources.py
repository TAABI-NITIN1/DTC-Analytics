from __future__ import annotations

import json

from src.dtc_mcp.catalog import METRIC_DEFINITIONS
from src.dtc_mcp.schema_repository import SchemaRepository


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def register_resources(mcp, schema: SchemaRepository, tool_names: list[str]) -> None:
    @mcp.resource("dtc://catalog/tables", description="Approved DTC analytics tables and tenant-scope classification.")
    def catalog_tables() -> str:
        return _json(schema.list_approved_tables())

    @mcp.resource("dtc://catalog/metrics", description="Approved DTC business metric definitions.")
    def catalog_metrics() -> str:
        return _json(sorted(METRIC_DEFINITIONS))

    @mcp.resource("dtc://catalog/tools", description="Registered governed DTC MCP operations.")
    def catalog_tools() -> str:
        return _json(sorted(tool_names))

    @mcp.resource("dtc://schema/{approved_table}", description="Schema for one approved analytics table; sensitive scope columns are omitted.")
    def table_schema(approved_table: str) -> str:
        return _json(schema.get_approved_table_schema(approved_table))

    @mcp.resource("dtc://definitions/{metric_name}", description="Business definition, source, freshness, and limitations for an approved metric.")
    def metric_definition(metric_name: str) -> str:
        return _json(schema.get_metric_definition(metric_name))
