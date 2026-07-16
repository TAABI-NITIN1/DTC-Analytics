# Task ID: 1

**Title:** Establish security and contract foundations

**Status:** done

**Dependencies:** None

**Priority:** high

**Description:** Capture current contracts, verify deployed schemas, define trusted tenant identity, enforce read-only ClickHouse policy, and prove two-tenant isolation before implementing MCP behavior.

**Details:**

Execute OpenSpec tasks 1.1-1.6 in order: capture authorized GraphQL/AI fixtures and parity tolerances; preserve the 2026-07-15 live V2_TABLES DESCRIBE baseline and add a non-secret compatibility check; define TenantContext, tool/evidence/error models; define production identity mapping and stdio principal; configure least-privilege read-only query limits; add deterministic cross-tenant tests for identifiers, aggregates, caches, fallbacks, and errors.

**Test Strategy:**

Contract fixtures are reproducible; deployed schema compatibility is recorded; destructive and multi-statement SQL is rejected; timeout/cancellation works; two-tenant tests prove no cross-customer data or existence leakage.
