## ADDED Requirements

### Requirement: Least-cost safe execution path
The AI Analyst SHALL use a zero-token fast path for supported definitions, cached/simple summaries, and fresh verified follow-ups; prefer domain tools for normal questions; and reserve multiple tools for genuine investigations.

#### Scenario: Simple DTC definition
- **WHEN** an authenticated MCP-mode user asks for a recognized DTC code definition
- **THEN** one governed DTC tool supplies evidence and the answer uses zero LLM tokens

### Requirement: Evidence-bound serving response
Numeric claims SHALL be grounded in evidence. Empty, truncated, stale, cached, or failed evidence SHALL be disclosed, and correlation MUST NOT be presented as confirmed causation.

#### Scenario: No usable rows are returned
- **WHEN** retrieval returns no usable evidence
- **THEN** the analyst gives a scoped limitation/retry response without inventing a numeric result

### Requirement: Heavy evaluation is post-response
Deep judging and durable observability persistence MUST NOT block the customer response and SHALL run asynchronously and/or by deterministic sampling.

#### Scenario: A response is selected for evaluation
- **WHEN** the sampling policy selects a completed response
- **THEN** the response returns after scheduling evaluation rather than waiting for the judge
