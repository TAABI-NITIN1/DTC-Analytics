from __future__ import annotations

import json
from datetime import date, datetime
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def stable_json(self) -> str:
        return json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class ToolStatus(str, Enum):
    SUCCESS = "success"
    EMPTY = "empty"
    ERROR = "error"


class ErrorCode(str, Enum):
    INVALID_ARGUMENT = "INVALID_ARGUMENT"
    UNAUTHENTICATED = "UNAUTHENTICATED"
    FORBIDDEN = "FORBIDDEN"
    SCOPE_VIOLATION = "SCOPE_VIOLATION"
    QUERY_REJECTED = "QUERY_REJECTED"
    TIMEOUT = "TIMEOUT"
    UPSTREAM_UNAVAILABLE = "UPSTREAM_UNAVAILABLE"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    INTERNAL = "INTERNAL"


class ToolError(StrictModel):
    code: ErrorCode
    message: str
    retryable: bool = False


class ToolRequestContext(StrictModel):
    user_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    allowed_customer_ids: tuple[str, ...] = Field(min_length=1)
    roles: frozenset[str] = frozenset()
    scopes: frozenset[str] = frozenset()
    request_id: str = Field(min_length=1)
    trace_id: str = Field(min_length=1)
    session_id: str = ""
    ai_run_id: str = ""
    auth_source: str = Field(min_length=1)


TenantContext = ToolRequestContext


class TimeRange(StrictModel):
    start: date | datetime | None = None
    end: date | datetime | None = None

    @model_validator(mode="after")
    def ordered(self) -> "TimeRange":
        if self.start and self.end and self.start > self.end:
            raise ValueError("time range start must not be after end")
        return self


class PaginationMetadata(StrictModel):
    limit: int = Field(ge=1)
    returned: int = Field(ge=0)
    truncated: bool = False


class QueryMetadata(StrictModel):
    query_hash: str
    tables: tuple[str, ...]
    row_count: int = Field(ge=0)
    truncated: bool
    effective_limit: int = Field(ge=1)
    execution_latency_ms: float = Field(ge=0)


class EvidenceMetadata(StrictModel):
    source: str = "clickhouse"
    tables: tuple[str, ...]
    query_type: str
    query_hash: str
    filters_applied: dict[str, Any]
    data_freshness: str
    analytics_checkpoint: str | None = None
    contract_version: str = "1.0"
    scope_ref: str
    query_window: TimeRange | None = None
    as_of: datetime
    row_count: int = Field(ge=0)
    truncated: bool
    effective_limit: int = Field(ge=1)
    cache_status: str = "disabled"
    cache_key_hash: str | None = None
    cache_invalidation_version: str | None = None
    cache_age_seconds: float | None = Field(default=None, ge=0)
    cache_latency_saved_ms: float | None = None
    cache_error: str | None = None
    duration_ms: float = Field(ge=0)
    trace_id: str


T = TypeVar("T")


class ToolResponse(StrictModel, Generic[T]):
    ok: bool
    tool_name: str
    data: T | None = None
    row_count: int = Field(ge=0)
    truncated: bool = False
    evidence: EvidenceMetadata | None = None
    limitations: tuple[str, ...] = ()
    error: ToolError | None = None
    request_id: str
    status: ToolStatus


class RepositoryResult(StrictModel):
    rows: list[dict[str, Any]]
    metadata: QueryMetadata
    evidence: EvidenceMetadata
