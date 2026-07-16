# Task ID: 5

**Title:** Implement restricted validated SQL fallback

**Status:** done

**Dependencies:** 4 ✓

**Priority:** high

**Description:** Add a disabled-by-default, role-gated, structurally validated SQL fallback used only when no domain tool can answer.

**Details:**

Execute OpenSpec tasks 5.1-5.4: enforce domain-tool-first routing; implement ClickHouse-aware structural parsing/policy for one SELECT/WITH; allowlist tables/columns/functions/joins; forbid comments, directives, system/external/table functions, unsafe settings and writes; inject trusted tenant scope; enforce row/byte/time limits; return standard evidence/errors/telemetry.

**Test Strategy:**

Adversarial SQL suite covers aliases, nesting, unions, functions, comments, settings, multiple/destructive statements, scope bypass and excessive limits; all unsafe or ambiguous inputs fail closed before execution.
