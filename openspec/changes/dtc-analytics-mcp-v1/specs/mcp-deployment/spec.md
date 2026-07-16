## ADDED Requirements

### Requirement: Dedicated internal MCP service
The deployment SHALL provide a pinned, non-root `dtc-mcp` container with health/readiness, graceful shutdown, bounded concurrency/resources, no embedded secrets, stdio development entry point, and authenticated Streamable HTTP service entry point.

#### Scenario: Production container starts
- **WHEN** deployment supplies runtime secrets and a healthy read-only ClickHouse endpoint
- **THEN** readiness succeeds and the service is reachable only from approved internal networks/services

### Requirement: Remote transport boundary
Production HTTP SHALL require signed identity, validate any supplied Origin against the allowlist, bind locally by default outside the container, expose no debug route, and terminate TLS through the existing ingress/proxy.

#### Scenario: Unapproved browser origin calls MCP
- **WHEN** a request supplies an Origin not in the allowlist
- **THEN** the service returns 403 before MCP processing

### Requirement: Reversible rollout
Deployment SHALL retain `DTC_DATA_ACCESS_MODE=direct` as the rollback control until the agreed observation window passes.

#### Scenario: Pilot SLO or parity gate fails
- **WHEN** operations initiates rollback
- **THEN** changing the data access mode to direct restores the legacy AI path without deleting the shared GraphQL repository migration
