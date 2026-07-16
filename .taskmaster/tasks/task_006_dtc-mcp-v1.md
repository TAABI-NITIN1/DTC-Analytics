# Task ID: 6

**Title:** Add production transport and observability

**Status:** done

**Dependencies:** 5 ✓

**Priority:** high

**Description:** Provide authenticated internal Streamable HTTP plus complete redacted audit, metrics, tracing, and measured cache policy.

**Details:**

Execute OpenSpec tasks 6.1-6.4: implement Streamable HTTP identity propagation, authorization, health/readiness, timeout/cancellation; emit redacted audit events and metrics; propagate trace context end-to-end; add a bounded tenant-aware cache only if measurements justify it, otherwise retain/document no-cache v1.

**Test Strategy:**

Transport contract parity with stdio passes; success/rejection/timeout/failure traces correlate end-to-end; telemetry contains required fields but no secrets/raw unsafe SQL; cache isolation tests pass or no-cache decision is evidenced.
