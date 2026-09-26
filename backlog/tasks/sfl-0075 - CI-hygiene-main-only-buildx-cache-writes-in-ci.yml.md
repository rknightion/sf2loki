---
id: SFL-0075
title: 'CI hygiene: main-only buildx cache writes in ci.yml'
status: To Do
assignee: []
created_date: '2026-09-26 15:55'
labels: []
dependencies: []
priority: high
type: chore
ordinal: 75000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
`ci.yml` (around lines 89-90) builds an image with `cache-from: type=gha` / `cache-to: type=gha,mode=max` on every run, including PRs; about 2.1 GB of PR-ref blob cache is never restorable. Write the cache only on push to main; PRs use cache-from only.

Context: fleet CI hygiene, tracked centrally as GHC-0006 in rknightion/.github. The container-publish.yml buildx cache move to a GHCR registry cache happens there and arrives here through the normal Renovate bump; orphaned PR and tag caches are deleted by the n8n repo-settings aligner. Neither needs work in this repo.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 ci.yml buildx cache-to runs only on push to main
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just check is green (fmt-check + lint + typecheck + test + gen-check + helm-lint + dist-check) — run it, don't assert it
- [ ] #2 just gen run and its output committed, if config.py or proto/ changed (just gen-check inside the gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
