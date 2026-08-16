---
id: SFL-0072
title: Add Codex cloud environment setup script
status: Done
assignee:
  - '@codex'
created_date: '2026-08-16 10:26'
updated_date: '2026-08-16 11:30'
labels: []
dependencies: []
references:
  - 'https://learn.chatgpt.com/docs/environments/cloud-environment#manual-setup'
  - 'https://code.claude.com/docs/en/cloud-environments#setup-scripts'
modified_files:
  - CONTRIBUTING.md
  - scripts/cloud-environment-setup.sh
ordinal: 72000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
Provide a repository-owned manual setup script for Codex cloud tasks so agents receive the pinned Python environment, project development dependencies, test and validation tooling, and the Backlog.md CLI required by this repository's workflow.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [x] #1 A documented executable setup script can be pasted or invoked from the Codex cloud environment manual setup configuration
- [x] #2 The script installs Backlog.md at the repository-compatible version and makes the backlog command available to agent sessions
- [x] #3 The script installs the pinned Python runtime and locked development dependencies plus tools required by the repository validation commands
- [x] #4 The script is safe to rerun and validates the installed toolchain
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [x] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [x] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->

## Implementation Plan

<!-- SECTION:PLAN:BEGIN -->
1. Add an idempotent repository-owned Codex cloud setup script that persists the user-local tool PATH, installs pinned uv/just/Backlog.md tooling, installs Python from .python-version, and syncs every locked dependency group/extra.
2. Document the exact Codex environment setup command and the script’s caching/rerun behavior for maintainers.
3. exercise the script in a clean temporary HOME where practical, verify installed versions and locked environment, then run just gate.

Revision after validation: install the locked default development group rather than optional runtime backends, because those backends alter the guarded-import behavior that mypy intentionally validates.

Follow-up: make tool installation compatible with Claude Code cloud’s Trusted network policy by using the allowlisted PyPI, crates.io, and npm registries, and document use in both providers plus Claude’s five-minute/cache constraints.

Follow-up: rename the entry point to the user-required `scripts/cloud-environment-setup.sh`, add an explicit local-agent do-not-run warning at the top, and update every repository reference.
<!-- SECTION:PLAN:END -->

## Implementation Notes

<!-- SECTION:NOTES:BEGIN -->
Added a pinned, idempotent Codex cloud setup script and contributor instructions. The setup script was run twice successfully; the second run confirmed rerun behavior. The full gate passes as a non-root user (the root-only run exposed the expected permission-test limitation).

Validation evidence: `bash -n scripts/codex-cloud-setup.sh`; the setup script completed successfully twice and reported uv 0.12.5, just 1.58.0, Backlog.md 1.50.1, Python 3.14.4, ruff, mypy, and pytest; `just gate` passed as the non-root cloud-agent equivalent with 1045 tests passing and 1 skipped. Push could not be performed because this checkout has no origin remote.

Claude Code cloud follow-up: replaced downloads from astral.sh and an unattached GitHub release with pinned installs from PyPI and crates.io, which Claude Trusted networking allowlists. Documented the shared setup command and Claude’s five-minute/cache requirements. Validation passed: setup script, crates.io resolution, diff check, and the complete non-root `just gate` (1045 passed, 1 skipped).

Renamed the setup entry point to `scripts/cloud-environment-setup.sh` as requested and added a top-of-file warning that local agents must not execute it.

Final naming validation passed without executing the cloud-only script locally: `bash -n scripts/cloud-environment-setup.sh`, `git diff --check`, reference search, and the full non-root `just gate` (1045 passed, 1 skipped).
<!-- SECTION:NOTES:END -->

## Final Summary

<!-- SECTION:FINAL_SUMMARY:BEGIN -->
Added and documented the manual Codex cloud setup entry point. It persistently exposes pinned uv, just, and Backlog.md tooling, installs Python and locked development dependencies, validates tool versions, and is safe to rerun. Verified by two successful setup runs and a green `just gate`; implementation commit dfc923bc1de3d2befa9908135944cf2013dadb3c.

Follow-up commit 2bd65a979f050809458eeef4081398c56ce16d46 adds Claude Code cloud compatibility by limiting tool downloads to its default allowlisted registries; the setup rerun and full gate passed.

The final entry point is `scripts/cloud-environment-setup.sh`, and its first comment explicitly tells non-cloud local agents not to execute it.
<!-- SECTION:FINAL_SUMMARY:END -->
