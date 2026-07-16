## ADDED Requirements

### Requirement: Layered automated verification
CI SHALL run OpenSpec strict validation, Python import/syntax and diff checks, unit, protocol, security, GraphQL regression, disposable ClickHouse integration, AI end-to-end, frontend build, container build, secret scan, and MCP dependency vulnerability scan without production secrets.

#### Scenario: Pull request changes MCP code
- **WHEN** CI evaluates the change
- **THEN** every gate runs with test-only fixtures and any failed gate blocks promotion

### Requirement: Meaningful MCP/security coverage
The new `src.dtc_mcp` package SHALL maintain at least 80% statement coverage using behavioral assertions.

#### Scenario: Coverage falls below threshold
- **WHEN** the unit/contract/security suite reports less than 80%
- **THEN** CI fails

### Requirement: Controlled migration evidence
A schema-compatible golden set SHALL cover the listed domain, multi-turn, empty, isolation, unusual SQL, and adversarial cases. Shadow comparison and non-production load reports SHALL record normalized parity, latency, resource, cache, timeout, and error metrics without claiming unmeasured production improvement.

#### Scenario: External production inputs are unavailable
- **WHEN** production credentials, traffic, or an approved pilot are not available
- **THEN** local code/fixtures/reports are completed and readiness remains NOT READY with only the external verification blockers listed
