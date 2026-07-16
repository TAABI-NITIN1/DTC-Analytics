# Task ID: 3

**Title:** Migrate GraphQL to shared repositories compatibly

**Status:** pending

**Dependencies:** 2 ✓

**Priority:** high

**Description:** Move resolver SQL to the shared repositories while preserving frontend GraphQL contracts and securing previously global or identifier-only paths.

**Details:**

Execute OpenSpec tasks 3.1-3.4: add trusted identity to FastAPI/GraphQL context; reject customer arguments outside authenticated scope; migrate fleet/trend then vehicle/DTC/co-occurrence/impact/maintenance resolvers; secure identifier-only and global leakage paths; preserve /api/ai/chat and frontend field/type semantics.

**Test Strategy:**

GraphQL contract fixtures, API smoke checks, tenant-isolation checks, and the frontend production build pass with no schema/type or authorized metric regression.
