from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import uuid
from contextvars import ContextVar
from typing import Any, Mapping

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from src.dtc_mcp.config import DTCSettings
from src.dtc_mcp.models import ErrorCode, TenantContext


class SecurityError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code


_LOCAL_STATE_SECRET = secrets.token_urlsafe(32)


def require_scope(context: TenantContext, scope: str) -> None:
    if not context:
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Authenticated tenant context is required")
    if scope not in context.scopes:
        raise SecurityError(ErrorCode.FORBIDDEN, "The caller does not have the required scope")


def tenant_scope_fingerprint(context: TenantContext) -> str:
    """Opaque, stable reference for conversation-state scope checks."""
    scope = json.dumps([context.tenant_id, context.customer_id, sorted(context.allowed_customer_ids)], separators=(",", ":"))
    return hashlib.sha256(scope.encode("utf-8")).hexdigest()[:16]


def sign_conversation_state(state: Mapping[str, Any]) -> str:
    secret = os.getenv("DTC_MCP_CONVERSATION_STATE_HMAC_SECRET") or os.getenv("DTC_MCP_IDENTITY_HMAC_SECRET") or _LOCAL_STATE_SECRET
    payload = json.dumps(state, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def verify_conversation_state(state: Mapping[str, Any], signature: str) -> bool:
    return bool(signature) and hmac.compare_digest(sign_conversation_state(state), signature)


def reject_customer_arguments(arguments: Mapping[str, Any]) -> None:
    forbidden = {"tenant", "tenant_id", "customer", "customer_id", "customer_name", "clientLoginId", "allowed_customer_ids"}
    if forbidden.intersection(arguments):
        raise SecurityError(ErrorCode.SCOPE_VIOLATION, "Tenant scope cannot be supplied as a tool argument")


def context_from_verified_claims(claims: Mapping[str, Any], *, auth_source: str = "verified_http") -> TenantContext:
    allowed = claims.get("allowed_customer_ids") or ()
    if isinstance(allowed, str):
        allowed = tuple(x.strip() for x in allowed.split(",") if x.strip())
    required = {"user_id", "tenant_id", "customer_id"}
    if any(not str(claims.get(key) or "").strip() for key in required) or not allowed:
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Verified identity is missing tenant mapping")
    return TenantContext(
        user_id=str(claims["user_id"]),
        tenant_id=str(claims["tenant_id"]),
        customer_id=str(claims["customer_id"]),
        allowed_customer_ids=tuple(str(x) for x in allowed),
        roles=frozenset(claims.get("roles") or ()),
        scopes=frozenset(claims.get("scopes") or ()),
        request_id=str(claims.get("request_id") or uuid.uuid4()),
        trace_id=str(claims.get("trace_id") or uuid.uuid4()),
        session_id=str(claims.get("session_id") or ""),
        ai_run_id=str(claims.get("ai_run_id") or claims.get("request_id") or ""),
        auth_source=auth_source,
    )


def development_context(settings: DTCSettings) -> TenantContext:
    if settings.environment in {"prod", "production"}:
        raise SecurityError(ErrorCode.FORBIDDEN, "Development identity is disabled in production")
    if os.getenv("DTC_MCP_DEV_CONTEXT_ENABLED", "false").strip().lower() not in {"1", "true", "yes", "on"}:
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Explicit development identity is not enabled")
    return context_from_verified_claims(
        {
            "user_id": os.getenv("DTC_MCP_DEV_USER_ID"),
            "tenant_id": os.getenv("DTC_MCP_DEV_TENANT_ID"),
            "customer_id": os.getenv("DTC_MCP_DEV_CUSTOMER_ID"),
            "allowed_customer_ids": os.getenv("DTC_MCP_DEV_ALLOWED_CUSTOMER_IDS", ""),
            "roles": tuple(x.strip() for x in os.getenv("DTC_MCP_DEV_ROLES", "").split(",") if x.strip()),
            "scopes": tuple(x.strip() for x in os.getenv("DTC_MCP_DEV_SCOPES", "").split(",") if x.strip()),
        },
        auth_source="explicit_stdio_development",
    )


_current_context: ContextVar[TenantContext | None] = ContextVar("dtc_mcp_context", default=None)


class ContextProvider:
    def __init__(self, default: TenantContext | None = None):
        self.default = default

    def get(self) -> TenantContext:
        context = _current_context.get() or self.default
        if context is None:
            raise SecurityError(ErrorCode.UNAUTHENTICATED, "Authenticated tenant context is required")
        return context

    def set(self, context: TenantContext):
        return _current_context.set(context)

    def reset(self, token) -> None:
        _current_context.reset(token)


def verify_signed_identity(encoded_claims: str, signature: str, secret: str) -> TenantContext:
    if not secret or not encoded_claims or not signature:
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Signed service identity is required")
    expected = hmac.new(secret.encode("utf-8"), encoded_claims.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Signed service identity is invalid")
    try:
        padded = encoded_claims + "=" * (-len(encoded_claims) % 4)
        claims = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except Exception as exc:
        raise SecurityError(ErrorCode.UNAUTHENTICATED, "Signed service identity is invalid") from exc
    return context_from_verified_claims(claims, auth_source="signed_service_identity")


class VerifiedIdentityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, *, contexts: ContextProvider, environment: str):
        super().__init__(app)
        self.contexts = contexts
        self.environment = environment

    async def dispatch(self, request, call_next):
        if request.url.path in {"/health", "/ready"}:
            return await call_next(request)
        try:
            if self.contexts.default is not None and self.environment not in {"prod", "production"}:
                context = self.contexts.default
            else:
                context = verify_signed_identity(
                    request.headers.get("X-DTC-Identity", ""),
                    request.headers.get("X-DTC-Identity-Signature", ""),
                    os.getenv("DTC_MCP_IDENTITY_HMAC_SECRET", ""),
                )
            token = self.contexts.set(context)
            try:
                return await call_next(request)
            finally:
                self.contexts.reset(token)
        except SecurityError:
            return JSONResponse({"error": "UNAUTHENTICATED"}, status_code=401)


class OriginValidationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, *, allowed_origins: tuple[str, ...]):
        super().__init__(app)
        self.allowed_origins = frozenset(allowed_origins)

    async def dispatch(self, request, call_next):
        origin = request.headers.get("Origin")
        if origin and origin not in self.allowed_origins:
            return JSONResponse({"error": "FORBIDDEN_ORIGIN"}, status_code=403)
        return await call_next(request)
