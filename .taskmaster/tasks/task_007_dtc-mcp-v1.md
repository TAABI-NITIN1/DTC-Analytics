# Task ID: 7

**Title:** Integrate AI MCP client and migration modes

**Status:** done

**Dependencies:** 6 ✓

**Priority:** high

**Description:** Route the DTC AI Analyst through the MCP client under explicit direct, shadow, and MCP modes with safe fallback policy.

**Details:**

Execute OpenSpec tasks 7.1-7.5: implement the narrow dual-transport client with trusted out-of-band identity, schema validation, timeout/cancellation; add all specified environment flags and safe defaults; preserve direct mode; add non-production sampled shadow comparisons; make MCP mode create no direct AI ClickHouse client; add separately explicit fallback preserving scope and audit.

**Test Strategy:**

Mode tests prove direct compatibility, shadow non-interference, MCP-only data access, fallback disabled by default, same-scope explicit fallback, response schema validation, and timeout/cancellation behavior.
