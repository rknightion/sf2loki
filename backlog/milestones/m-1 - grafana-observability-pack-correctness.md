---
id: m-1
title: "Grafana observability pack correctness"
---

## Description

Make the shipped Grafana pack (`deploy/grafana/`) tell the truth. The 2026-07-30 audit confirmed that the pack's three critical alerts can never fire (gauge staleness + `noDataState: Ok`), its ELF-scoped Loki rules query trailing windows EventLogFile timestamps never occupy, its ingest-lag alert fires permanently once ELF is enabled, and two dashboards extract fields Salesforce never populates. When this tracker closes, every rule in the pack can demonstrably fire under its failure condition, every panel queries fields that exist, and a lint test keeps it that way.

**This issue is the parent tracker** — the checklists below are the authoritative worklist; progress lives here and on the children. Read each child IN FULL (body + comments) before working it.

## Phase order

1. Phase 1 — metric emission (Python side first; rules depend on it)
2. Phase 2 — pack query fixes (after Phase 1 lands, so queries target the new shapes)

Phases run in order; issues inside a phase are parallel-safe except where a shared file forces one owner. Each task carries its phase as a `phase-N` label.

Imported from GitHub issue #157 (the epic tracker), archived in `archive/issues-dump.json`.
