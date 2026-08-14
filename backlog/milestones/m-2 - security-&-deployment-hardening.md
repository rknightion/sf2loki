---
id: m-2
title: "Security & deployment hardening"
---

## Description

Close the security and deployment-hardening gaps confirmed by the 2026-07-30 audit: credential exposure in operator-facing output, inconsistent SOQL interpolation guards, an unhardened compose deployment, Helm RBAC/token-automount slack, a false hardening claim in the docs, and an unpinned release-time build toolchain. None is remotely exploitable on its own — this is defence-in-depth for a security/audit-telemetry connector whose credibility depends on its own posture.

**This issue is the parent tracker** — the checklists below are the authoritative worklist. Read each child IN FULL (body + comments) before working it.

## Phase order

1. Phase 1 — app-level
2. Phase 2 — container / compose
3. Phase 3 — helm / k8s
4. Phase 4 — supply chain

Phases run in order; issues inside a phase are parallel-safe except where a shared file forces one owner. Each task carries its phase as a `phase-N` label.

Imported from GitHub issue #158 (the epic tracker), archived in `archive/issues-dump.json`.
