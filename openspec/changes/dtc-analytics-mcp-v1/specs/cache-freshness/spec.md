## ADDED Requirements

### Requirement: Tenant-safe bounded result cache
Cacheable repository operations SHALL use keys derived from authenticated tenant/customer scope, operation, normalized parameters, query hash, result limit, schema version, and analytics checkpoint, without exposing raw identifiers or secrets.

#### Scenario: Two tenants request identical analytics
- **WHEN** two authenticated tenants call the same operation with identical model-visible inputs
- **THEN** their cache key hashes and stored entries differ and neither can receive the other's rows

### Requirement: Cache failure is not data failure
Redis SHALL be optional, use a conservative TTL, mark freshness and stale age in evidence, and fail open to ClickHouse. Dynamic validated SQL MUST NOT be cached.

#### Scenario: Redis is unavailable
- **WHEN** a cache read or write fails
- **THEN** the repository executes its governed ClickHouse query and returns cache-error evidence without failing the data request

#### Scenario: Producer checkpoint changes
- **WHEN** the configured analytics checkpoint/version changes
- **THEN** the derived key changes and the prior result is not reused
