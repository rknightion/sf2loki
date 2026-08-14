---
id: m-3
title: "Test-coverage backlog"
---

## Description

Close the test-coverage gaps the 2026-07-30 audit measured empirically (coverage run + uncovered-range reading, then adversarial verification that no existing test covers each path). These are not percentage-chasing issues: every child protects a specific failure mode that today could regress silently — CAS surrender, fence absorption, lease recovery, retry classification, drain stall guards, multi-org startup. When this tracker closes, the invariants the last three audit waves fought for are all pinned.

**This issue is the parent tracker** — the checklists below are the authoritative worklist. Read each child IN FULL (body + comments) before working it.

## Phase order

1. Phase 1 — state & coordinate tests
2. Phase 2 — source tests
3. Phase 3 — app & CLI tests

Phases run in order; issues inside a phase are parallel-safe except where a shared file forces one owner. Each task carries its phase as a `phase-N` label.

Imported from GitHub issue #159 (the epic tracker), archived in `archive/issues-dump.json`.
