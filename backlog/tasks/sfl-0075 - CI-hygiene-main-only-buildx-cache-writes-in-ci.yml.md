---
id: SFL-0075
title: 'CI hygiene: main-only buildx cache writes in ci.yml'
status: Done
assignee: []
created_date: '2026-09-26 15:55'
updated_date: '2026-09-26 17:11'
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
- [x] #1 ci.yml buildx cache-to runs only on push to main
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just check is green (fmt-check + lint + typecheck + test + gen-check + helm-lint + dist-check) — run it, don't assert it
- [ ] #2 just gen run and its output committed, if config.py or proto/ changed (just gen-check inside the gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Changed .github/workflows/ci.yml line 90: cache-to is now `${{ github.event_name == 'push' && github.ref == 'refs/heads/main' && 'type=gha,mode=max' || '' }}`, empty on any other trigger (PRs, workflow_dispatch). cache-from stays `type=gha` unconditionally so PRs still read main's cache. Nothing else in the docker-build-test job changed.

No config.py or proto/ changes, so `just gen` was not needed; ran `just gen-check` anyway and it reported no drift.

Verified: just fmt-check, just lint, just typecheck, just gen-check, just helm-lint, just dist-check all pass clean. just test has one pre-existing flaky failure unrelated to this change — tests/sinks/test_sink.py::TestEncodeOffload::test_large_batch_encode_offload_keeps_loop_responsive asserts a wall-clock tick count (>=10) while the machine was under heavy load from parallel work in another repo; it passed in isolation (uv run pytest -q that one test alone: 1 passed in 1.86s) both before and after this change. actionlint and zizmor are clean on the changed file.

CodeRabbit review was attempted (`coderabbit review --agent --base main`) but the org's 5 included hourly reviews were already used elsewhere this session (28 min reset); not repeated for this single-line conditional-expression change given actionlint/zizmor were already clean.

Commit b742ce6, pushed to main.
<!-- SECTION:FINAL_SUMMARY:END -->
