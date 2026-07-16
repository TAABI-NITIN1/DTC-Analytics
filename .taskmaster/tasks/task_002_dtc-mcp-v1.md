# Task ID: 2

**Title:** Extract shared read-only DTC repositories

**Status:** done

**Dependencies:** 1 ✓

**Priority:** high

**Description:** Create the minimal shared repository boundary used by GraphQL and MCP for all DTC query families.

**Details:**

Execute OpenSpec tasks 2.1-2.6: add the minimal src/dtc_mcp package; extract fleet health, DTC distribution, fleet/system trends, vehicle health/faults, DTC detail/co-occurrence/impact, and maintenance priority repositories; require trusted tenant context; apply deterministic bounds/order; use scoped facts when aggregates cannot prove tenancy; return the shared evidence contract without unsafe SQL or secrets.

**Test Strategy:**

Repository unit/integration tests pass for authorized and unauthorized tenants, legacy metric parity stays within accepted tolerances, all results are bounded and deterministic, and every result contains valid safe evidence.
