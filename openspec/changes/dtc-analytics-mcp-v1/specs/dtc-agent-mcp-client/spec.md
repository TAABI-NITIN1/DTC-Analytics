## ADDED Requirements

### Requirement: MCP-mode analyst access
In `mcp` mode the DTC AI Analyst SHALL fetch DTC data only through the MCP client; direct SQL/tool execution from the analyst MUST be disabled.

#### Scenario: Analyst invokes a tool in MCP mode
- **WHEN** the model selects a DTC data tool
- **THEN** the client calls the registered MCP tool and no analyst ClickHouse client is created for that request

### Requirement: Trusted context propagation
The client SHALL propagate authenticated identity and trace context from the API outside model-visible arguments and MUST validate response schemas.

#### Scenario: Model attempts identity modification
- **WHEN** model output includes identity or tenant metadata
- **THEN** the client ignores/rejects it and propagates only the trusted API context

### Requirement: Explicit fallback
MCP failure SHALL fall back to direct access only when a separate explicit fallback configuration is enabled, and fallback MUST preserve the same trusted tenant scope and audit trail.

#### Scenario: MCP unavailable without fallback
- **WHEN** an MCP call fails and fallback is not enabled
- **THEN** the analyst returns a bounded service error and performs no direct database query

### Requirement: Transport configuration
The client SHALL support configured stdio or Streamable HTTP transport with timeout and cancellation.

#### Scenario: Call exceeds client timeout
- **WHEN** an MCP call exceeds `DTC_MCP_TIMEOUT_SECONDS`
- **THEN** the client cancels it, records a timeout, and follows only the explicit fallback policy
