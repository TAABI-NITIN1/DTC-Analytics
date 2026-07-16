from __future__ import annotations

import os
from dataclasses import dataclass

from src.clickhouse_utils_v2 import V2_TABLES


def _bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int(name: str, default: int, minimum: int = 1) -> int:
    return max(minimum, int(os.getenv(name) or default))


@dataclass(frozen=True)
class DTCSettings:
    environment: str = "development"
    query_timeout_seconds: int = 15
    max_result_rows: int = 200
    max_result_bytes: int = 10_000_000
    max_lookback_days: int = 365
    allowed_database: str = ""
    allowed_tables: tuple[str, ...] = tuple(V2_TABLES.values())
    dynamic_sql_enabled: bool = False
    transport: str = "stdio"
    streamable_http_url: str = "http://127.0.0.1:8001/mcp"
    http_host: str = "127.0.0.1"
    http_port: int = 8001
    data_access_mode: str = "direct"
    direct_fallback_enabled: bool = False
    redis_url: str = ""
    cache_ttl_seconds: int = 30
    analytics_checkpoint: str = "unknown"
    auth_mode: str = "signed_hmac"
    allowed_origins: tuple[str, ...] = ()
    http_workers: int = 1
    http_limit_concurrency: int = 100

    @classmethod
    def from_env(cls) -> "DTCSettings":
        allowed = os.getenv("DTC_MCP_ALLOWED_TABLES", "").strip()
        allowed_tables = tuple(x.strip() for x in allowed.split(",") if x.strip()) or tuple(V2_TABLES.values())
        unknown = set(allowed_tables) - set(V2_TABLES.values())
        if unknown:
            raise ValueError("DTC_MCP_ALLOWED_TABLES contains an unapproved table")
        mode = (os.getenv("DTC_DATA_ACCESS_MODE") or "direct").strip().lower()
        if mode not in {"direct", "shadow", "mcp"}:
            raise ValueError("DTC_DATA_ACCESS_MODE must be direct, shadow, or mcp")
        transport = (os.getenv("DTC_MCP_TRANSPORT") or "stdio").strip().lower().replace("-", "_")
        if transport not in {"stdio", "streamable_http"}:
            raise ValueError("DTC_MCP_TRANSPORT must be stdio or streamable_http")
        environment = (os.getenv("DEPLOYMENT_ENV") or os.getenv("ENV_NAME") or "development").strip().lower()
        auth_mode = (os.getenv("DTC_MCP_AUTH_MODE") or "signed_hmac").strip().lower()
        if auth_mode != "signed_hmac":
            raise ValueError("DTC_MCP_AUTH_MODE must be signed_hmac")
        allowed_origins = tuple(value.strip() for value in os.getenv("DTC_MCP_ALLOWED_ORIGINS", "").split(",") if value.strip())
        if environment in {"prod", "production"} and mode == "shadow":
            raise ValueError("shadow mode is disabled in production")
        return cls(
            environment=environment,
            query_timeout_seconds=_int("DTC_MCP_TIMEOUT_SECONDS", 15),
            max_result_rows=_int("DTC_MCP_MAX_ROWS", 200),
            max_result_bytes=_int("DTC_MCP_MAX_RESULT_BYTES", 10_000_000),
            max_lookback_days=_int("DTC_MCP_MAX_LOOKBACK_DAYS", 365),
            allowed_database=(os.getenv("DTC_MCP_ALLOWED_DATABASE") or "").strip(),
            allowed_tables=allowed_tables,
            dynamic_sql_enabled=_bool("DTC_MCP_DYNAMIC_SQL_ENABLED", False),
            transport=transport,
            streamable_http_url=(os.getenv("DTC_MCP_URL") or "http://127.0.0.1:8001/mcp").strip(),
            http_host=(os.getenv("DTC_MCP_HOST") or os.getenv("DTC_MCP_HTTP_HOST") or "127.0.0.1").strip(),
            http_port=_int("DTC_MCP_PORT", _int("DTC_MCP_HTTP_PORT", 8001)),
            data_access_mode=mode,
            direct_fallback_enabled=_bool("DTC_MCP_DIRECT_FALLBACK_ENABLED", False),
            redis_url=(os.getenv("REDIS_URL") or "").strip(),
            cache_ttl_seconds=_int("DTC_MCP_CACHE_TTL_SECONDS", 30),
            analytics_checkpoint=(os.getenv("DTC_ANALYTICS_CHECKPOINT") or "unknown").strip(),
            auth_mode=auth_mode,
            allowed_origins=allowed_origins,
            http_workers=_int("DTC_MCP_WORKERS", 1),
            http_limit_concurrency=_int("DTC_MCP_CONCURRENCY", 100),
        )
