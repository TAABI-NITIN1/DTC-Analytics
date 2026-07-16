from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

from src.dtc_mcp.config import DTCSettings


@dataclass(frozen=True)
class CacheLookup:
    value: dict[str, Any] | None
    status: str
    key_hash: str | None
    latency_ms: float
    error: str | None = None


class ResultCache:
    def __init__(self, client=None, *, ttl_seconds: int = 30, checkpoint: str = "unknown", error: str | None = None):
        self.client = client
        self.ttl_seconds = ttl_seconds
        self.checkpoint = checkpoint
        self.error = error

    @classmethod
    def from_settings(cls, settings: DTCSettings) -> "ResultCache":
        if not settings.redis_url:
            return cls(ttl_seconds=settings.cache_ttl_seconds, checkpoint=settings.analytics_checkpoint)
        try:
            from redis import Redis

            client = Redis.from_url(settings.redis_url, decode_responses=True, socket_connect_timeout=0.2, socket_timeout=0.2)
        except Exception:
            return cls(ttl_seconds=settings.cache_ttl_seconds, checkpoint=settings.analytics_checkpoint, error="cache_unavailable")
        return cls(client, ttl_seconds=settings.cache_ttl_seconds, checkpoint=settings.analytics_checkpoint)

    def key(self, *, tenant_id: str, customer_ids: tuple[str, ...], operation: str, parameters: dict[str, Any], query_hash: str, limit: int, schema_version: str = "1.0") -> tuple[str, str]:
        payload = json.dumps({
            "tenant": tenant_id,
            "customers": sorted(customer_ids),
            "operation": operation,
            "parameters": parameters,
            "query_hash": query_hash,
            "limit": limit,
            "checkpoint": self.checkpoint,
            "schema": schema_version,
        }, sort_keys=True, separators=(",", ":"), default=str)
        key_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return f"dtc:v1:{key_hash}", key_hash

    def get(self, key: str, key_hash: str) -> CacheLookup:
        started = time.perf_counter()
        if self.client is None:
            return CacheLookup(None, "error" if self.error else "disabled", key_hash, 0.0, self.error)
        try:
            raw = self.client.get(key)
            value = json.loads(raw) if raw else None
            status = "hit" if value is not None else "miss"
            return CacheLookup(value, status, key_hash, round((time.perf_counter() - started) * 1000, 3))
        except Exception:
            return CacheLookup(None, "error", key_hash, round((time.perf_counter() - started) * 1000, 3), "cache_unavailable")

    def set(self, key: str, value: dict[str, Any]) -> str | None:
        if self.client is None:
            return None
        try:
            payload = {"cached_at": time.time(), "result": value}
            self.client.setex(key, self.ttl_seconds, json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str))
            return None
        except Exception:
            return "cache_unavailable"
