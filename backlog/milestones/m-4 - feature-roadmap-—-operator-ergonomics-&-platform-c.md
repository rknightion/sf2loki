---
id: m-4
title: "Feature roadmap — operator ergonomics & platform capabilities"
---

## Description

The post-1.0 feature roadmap from the 2026-07-30 audit: fifteen verified-absent, feasibility-checked capabilities, split into operator quick wins (doctor/CLI gaps that bite in production today), resilience & visibility (dead-letter capture, cardinality guard, /statusz), and platform capabilities (new Salesforce surfaces, multi-tenant routing, new backends). Every child names the concrete API it builds on and why it fits the architecture; each was adversarially reviewed for "does this already exist / is it worth an issue".

**This issue is the parent tracker** — the checklists below are the authoritative worklist. Unlike the companion bug trackers, this one is a MENU with a recommended order, not a mandate: descope freely with a comment; nothing here blocks a release.

## Phase order

1. Phase 1 — operator quick wins
2. Phase 2 — resilience & visibility
3. Phase 3 — platform capabilities (design comment first, rule 1)

Phases run in order; issues inside a phase are parallel-safe except where a shared file forces one owner. Each task carries its phase as a `phase-N` label.

Imported from GitHub issue #160 (the epic tracker), archived in `archive/issues-dump.json`.
