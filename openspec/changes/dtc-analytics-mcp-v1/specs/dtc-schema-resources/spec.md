## ADDED Requirements

### Requirement: Governed catalog resources
The server SHALL expose versioned resources describing only approved DTC tables, columns, grains, joins, freshness, and tenant-scope behavior.

#### Scenario: Client reads approved schema
- **WHEN** an authorized client requests a registered DTC schema resource
- **THEN** it receives the approved versioned contract without credentials, data samples, or other-tenant identifiers

### Requirement: Catalog isolation
Schema resources MUST NOT expose system tables, unapproved objects, secrets, or customer enumeration to tenant callers.

#### Scenario: Caller requests unapproved object
- **WHEN** a tenant caller requests an object outside the catalog allowlist
- **THEN** the server returns `FORBIDDEN` or `INVALID_ARGUMENT` and audits the rejection

### Requirement: Schema compatibility detection
The service SHALL detect incompatible deployed-schema changes for fields used by a tool and fail that tool safely.

#### Scenario: Required column is absent
- **WHEN** compatibility validation finds a missing or incompatible required column
- **THEN** the affected tool returns `SCHEMA_MISMATCH` without issuing its business query
