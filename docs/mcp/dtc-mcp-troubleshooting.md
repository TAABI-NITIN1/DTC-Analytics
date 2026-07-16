# DTC MCP troubleshooting

| Symptom | Check | Action |
|---|---|---|
| `/ready` is 503 | Read-only ClickHouse connectivity/grants | Restore endpoint or SELECT grants; do not broaden privileges |
| HTTP 401 | HMAC secret, payload, signature, clock/deployment identity | Rotate/fix secret on trusted API and MCP together |
| HTTP 403 | Origin or scope | Add only the exact approved origin or correct identity mapping |
| `SCOPE_VIOLATION` | Requested vehicle/customer and mapped IDs | Correct authorization mapping; never pass a model tenant override |
| `QUERY_REJECTED` | Tool choice and validated-SQL policy | Use a domain tool or revise only to approved SELECT policy |
| `TIMEOUT` | Trace ID, ClickHouse query log, limits | Reduce window/limit; investigate producer/indexing before raising limits |
| `UPSTREAM_UNAVAILABLE` | ClickHouse/transport health | Retry boundedly; direct fallback stays off unless incident approval enables it |
| Cache error | Redis health and evidence `cache_error` | Data should continue from ClickHouse; restore Redis without bypassing scope |
| Stale result | Check checkpoint and cache age | Update producer checkpoint exposure or wait TTL; disclose freshness |
| Shadow mismatch | Compare normalized rows/entities/sort/null/time window | Classify as formatting, repository, contract, legacy, data, or reasoning issue |
| Stdio parse failure | Accidental stdout logging | Keep logs on stderr and rerun the protocol test |

Start diagnosis with `request_id` and `trace_id`; audit rows contain hashes, not customer payloads. Never paste secrets, raw connection strings, or unrestricted SQL into tickets.
