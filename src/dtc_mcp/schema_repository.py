from __future__ import annotations

from typing import Any

from src.dtc_mcp.catalog import APPROVED_COLUMNS, METRIC_DEFINITIONS
from src.dtc_mcp.models import ErrorCode
from src.dtc_mcp.repository import RepositoryError


class SchemaRepository:
    def list_approved_tables(self) -> list[dict[str, Any]]:
        return [{"table": table, "column_count": len(columns), "tenant_scoped": "clientLoginId" in columns} for table, columns in sorted(APPROVED_COLUMNS.items())]

    def get_approved_table_schema(self, table: str) -> dict[str, Any]:
        columns = APPROVED_COLUMNS.get(table)
        if columns is None:
            raise RepositoryError(ErrorCode.FORBIDDEN, "Table is not in the approved analytics catalog")
        sensitive = {"customer_name", "clientLoginId"}
        return {"table": table, "columns": [name for name in columns if name not in sensitive], "tenant_scope": "server_enforced" if "clientLoginId" in columns else "global_reference"}

    def get_metric_definition(self, metric: str) -> dict[str, Any]:
        definition = METRIC_DEFINITIONS.get(metric)
        if definition is None:
            raise RepositoryError(ErrorCode.INVALID_ARGUMENT, "Unknown metric definition")
        return {"name": metric, **definition}

    def compatibility_check(self, deployed: dict[str, set[str]]) -> dict[str, list[str]]:
        missing: dict[str, list[str]] = {}
        for table, approved in APPROVED_COLUMNS.items():
            absent = sorted(set(approved) - deployed.get(table, set()))
            if absent:
                missing[table] = absent
        return missing
