# DTC MCP security

Production HTTP accepts only HMAC-signed `TenantContext` identity. The context maps a user/tenant to allowed internal customer IDs and scopes. The model cannot add or override tenant scope. Stdio requires an explicitly enabled development principal.

Repositories bind customer IDs as ClickHouse parameters, use an approved-table allowlist, a dedicated SELECT-only identity, `readonly=2`, row/byte/time limits, and trace comments. Vehicle identifiers are checked inside the tenant predicate. Cache keys hash tenant/customer scope plus operation, normalized parameters, query hash, limit, schema version, and checkpoint.

Validated SQL is disabled by default. When enabled for an authorized role it accepts one parsed SELECT/WITH statement, injects tenant scope, restricts tables/columns/functions/joins/subqueries/limits, and rejects DDL/DML, comments, unions, table functions, system/external access, settings, and caller tenant predicates.

Audit logs hash user/tenant/vehicle references and redact passwords, tokens, authorization, connection values, SQL/query text, and configured fields. Full results, credentials, stack traces, and unrestricted SQL are never logged. Persistence is asynchronous and fail-open.

Remote HTTP requires authentication and rejects a supplied Origin outside `DTC_MCP_ALLOWED_ORIGINS`. The Compose service has no host port and ClickHouse is not newly exposed. TLS terminates at the existing internal ingress/proxy; configure its upstream as `dtc-mcp:8001`, require TLS, strip untrusted identity headers, and inject/forward identity only from the trusted API tier.

Security verification commands:

```powershell
python -m pytest tests/unit/test_security.py tests/unit/test_validated_sql.py tests/unit/test_observability.py tests/unit/test_server_protocol.py -q
python scripts/scan_secrets.py
python -m pip_audit -r requirements-mcp.txt --progress-spinner off
```
