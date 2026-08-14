---
id: m-0
title: "Correctness & data-integrity hardening"
---

## Description

Close every correctness and data-integrity defect confirmed by the 2026-07-30 full-repo audit (find→adversarial-verify, one verifier per finding; every issue below survived a default-refute review, and the top items were reproduced against the real `Pipeline` on current `main`). When the phases below are done, the connector no longer has a known path that loses events, regresses a checkpoint, wedges a source, or lies to the operator about any of those.

**This issue is the parent tracker.** Progress lives here and on the child issues — no scratch files. A future session with no memory of this one must be able to resume from this issue and its comments alone.

**The phase checklists below are the authoritative worklist.** Read every child issue IN FULL — description AND all comments — before touching it; comments carry binding decisions that override bodies.

## Phase order

1. Phase 1 — pipeline & app seam (data loss first; ONE owner for `app.py`)
2. Phase 2 — polled sources
3. Phase 3 — streaming + auth/clients
4. Phase 4 — state & coordinate
5. Phase 5 — CLI / backfill / doctor / config

Phases run in order; issues inside a phase are parallel-safe except where a shared file forces one owner. Each task carries its phase as a `phase-N` label.

Imported from GitHub issue #156 (the epic tracker), archived in `archive/issues-dump.json`.
