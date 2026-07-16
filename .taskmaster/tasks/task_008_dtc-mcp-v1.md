# Task ID: 8

**Title:** Evaluate, harden, roll out, and document operations

**Status:** in-progress

**Dependencies:** 7 ✓

**Priority:** high

**Description:** Complete parity/security/load evaluation, operational documentation, guarded rollout, rollback drill, and legacy-removal follow-up.

**Details:**

Execute OpenSpec tasks 8.1-8.5 and 9-14: complete cache/freshness, integrated audit, AI serving optimization, container/CI gates, golden/shadow/benchmark evidence, and the seven operations documents. Local gates are implemented and passing. Keep this task in progress until Linux CI proves the disposable ClickHouse E2E/container build and approved production shadow, load, pilot, rollback, and observation-window gates pass.

**Test Strategy:**

Local result: 117 passed, 4 external/integration skips, 85.34% MCP coverage, frontend/OpenSpec/secret/dependency gates passed, 8/8 synthetic parity. External result still required: real-data parity, Docker/ClickHouse CI, load/resource SLOs, production security review, pilot, rollback drill, and stability window. Direct removal remains deferred.
