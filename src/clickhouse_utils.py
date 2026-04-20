from __future__ import annotations

from typing import Any

import clickhouse_connect
from clickhouse_driver import Client

from src.config import get_clickhouse_cfg, get_vehicle_clickhouse_cfg


class ClickHouseHTTPAdapter:
    """Small execute-compatible wrapper over clickhouse-connect client.

    This keeps legacy call sites working with `client.execute(sql, params)` while
    using the HTTP client in API/read-service deployments.
    """

    def __init__(self, client: clickhouse_connect.driver.client.Client):
        self._client = client

    def _render_query(self, query: str, params: dict[str, Any] | None) -> str:
        if not params:
            return query

        rendered = query
        for key, value in params.items():
            rendered = rendered.replace(f"%({key})s", self._literal(value))
        return rendered

    @staticmethod
    def _literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, (list, tuple, set)):
            return "(" + ", ".join(ClickHouseHTTPAdapter._literal(v) for v in value) + ")"
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def execute(self, query: str, params: dict[str, Any] | list[tuple] | None = None, settings: dict[str, Any] | None = None):
        upper = query.lstrip().upper()

        if isinstance(params, list):
            # Legacy insert usage: INSERT ... VALUES with list-of-tuples payload
            if "INSERT INTO" not in upper:
                raise ValueError("List payload is supported only for INSERT statements")
            return self._insert_rows(query, params)

        rendered = self._render_query(query, params if isinstance(params, dict) else None)

        if upper.startswith("SELECT") or upper.startswith("SHOW") or upper.startswith("DESCRIBE") or upper.startswith("WITH"):
            result = self._client.query(rendered, settings={k: str(v) for k, v in (settings or {}).items()})
            return result.result_rows

        self._client.command(rendered, settings={k: str(v) for k, v in (settings or {}).items()})
        return []

    def query_df(self, query: str, params: dict[str, Any] | None = None):
        """Return `(column_names, result_rows)` for select-style queries.

        Some existing service paths rely on this helper shape.
        """
        rendered = self._render_query(query, params)
        result = self._client.query(rendered)
        return result.column_names, result.result_rows

    def _insert_rows(self, query: str, rows: list[tuple]):
        prefix = query.split("VALUES", 1)[0]
        # Expected format: INSERT INTO table_name (col1, col2, ...)
        into_part = prefix.split("INTO", 1)[1].strip()
        table_name = into_part.split("(", 1)[0].strip().replace("`", "")
        cols_part = into_part.split("(", 1)[1].rsplit(")", 1)[0]
        column_names = [c.strip().replace("`", "") for c in cols_part.split(",") if c.strip()]
        self._client.insert(table=table_name, data=rows, column_names=column_names)
        return []



def _build_client(cfg: dict[str, Any]):
    host = cfg.get("host")
    port = int(cfg.get("port") or 0)
    user = cfg.get("user")
    password = cfg.get("password")
    database = cfg.get("database")

    if not port:
        port = 8123

    if port == 8123:
        http_client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
            send_receive_timeout=60,
            query_limit=0,
        )
        return ClickHouseHTTPAdapter(http_client)

    return Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )



def get_clickhouse_client(**overrides):
    cfg = get_clickhouse_cfg()
    cfg.update({k: v for k, v in overrides.items() if v is not None})
    return _build_client(cfg)



def get_vehicle_clickhouse_client(**overrides):
    vehicle_cfg = get_vehicle_clickhouse_cfg()
    base_cfg = get_clickhouse_cfg()

    merged = {
        "host": vehicle_cfg.get("host") or base_cfg.get("host"),
        "port": vehicle_cfg.get("port") or base_cfg.get("port"),
        "user": vehicle_cfg.get("user") or base_cfg.get("user"),
        "password": vehicle_cfg.get("password") or base_cfg.get("password"),
        "database": vehicle_cfg.get("database") or base_cfg.get("database"),
    }
    merged.update({k: v for k, v in overrides.items() if v is not None})
    return _build_client(merged)



def ensure_tables(_client):
    """No-op in API-only mode.

    Table creation and heavy analytics table management are owned by Airflow.
    """
    return None
