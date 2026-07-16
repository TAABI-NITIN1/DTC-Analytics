# Task ID: 4

**Title:** Implement MCP server, domain tools, resources, and evidence

**Status:** done

**Dependencies:** 3

**Priority:** high

**Description:** Build the read-only local MCP server over shared repositories using the pinned stable SDK and typed contracts.

**Details:**

Execute OpenSpec tasks 4.2-4.5; dependency task 4.1 is already complete with mcp==1.28.1. Implement server lifecycle and stdio with explicit development identity; register the eight approved initial domain tools; validate all input/output/evidence schemas; implement governed catalog resources and schema mismatch handling; add protocol tests for discovery, calls, invalid inputs, truncation, timeout, cancellation, destructive attempts, and isolation.

**Test Strategy:**

Stdio protocol tests pass; every tool/resource is discoverable and schema-valid; customer fields are absent from model inputs; destructive/cross-tenant requests fail before ClickHouse; every success carries evidence.
