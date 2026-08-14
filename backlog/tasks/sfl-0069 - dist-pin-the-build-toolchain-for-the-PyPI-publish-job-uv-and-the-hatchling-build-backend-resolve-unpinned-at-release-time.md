---
id: SFL-0069
title: >-
  dist: pin the build toolchain for the PyPI publish job - uv and the hatchling
  build backend resolve unpinned at release time
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-4
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/153'
ordinal: 69000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The `pypi-release` job in `.github/workflows/release-please.yml:51-93` produces the wheel and sdist that ship to PyPI, and two of its inputs are resolved fresh from the network at run time with no version pin and no hash verification.

**Unpinned uv.** `.github/workflows/release-please.yml:76` pins the action by commit SHA (`astral-sh/setup-uv@fac544c07dec837d0ccb6301d7b5580bf5edae39`, v8.2.0) but passes no `version:` input. That action's `version` input defaults to `""`, documented as "Defaults to the version in pyproject.toml or 'latest'". The file-fallback cannot engage in this repo: `pyproject.toml` has no `[tool.uv]` table, so there is no `required-version`; there is no `uv.toml`; and no `version-file:` is supplied. The job therefore installs whichever uv release is latest when it runs. The action's `checksum:` input, which would pin the uv binary's hash, is also unset.

**Unpinned build backend.** `.github/workflows/release-please.yml:85` runs `uv build --out-dir dist` against `pyproject.toml:48-50`:

```toml
[build-system]
requires = ["hatchling>=1.26"]  # >=1.26 for PEP 639 SPDX license expressions
build-backend = "hatchling.build"
```

`uv build` creates an isolated build environment and resolves `build-system.requires` from the configured index at build time. `uv.lock` does not cover it — `grep -c hatchling uv.lock` returns `0`, because build-system requirements are outside the lockfile's dependency graph. Verified empirically with uv 0.12.0 against a throwaway project carrying the same `requires`: with an isolated cache and `--offline`, `uv build` fails with `Because hatchling was not found in the cache and you require hatchling>=1.26 … Packages were unavailable because the network was disabled`. The build backend is fetched over the network on every build.

There are no build constraints anywhere in the repo: no `build-constraints*` file, no `--build-constraints` / `UV_BUILD_CONSTRAINT`, no `[tool.uv] build-constraint-dependencies`, no `--require-hashes` (repo-wide grep for `build-constraint` returns nothing). `.github/workflows/release-please.yml:82` sets `enable-cache: false` — correct for cache-poisoning resistance, but it also guarantees the backend is re-resolved from the index on every publish rather than served from a cache. hatchling 1.31.0 itself requires `packaging`, `pathspec`, `pluggy` and `trove-classifiers`, all resolved unpinned too, so the isolated build environment is five unpinned packages, not one.

**Nothing downstream catches a tampered build.** `scripts/check_dist.py` reads only archive member *paths* — `_wheel_members` / `_sdist_members` return `namelist()` / `getmembers()` names (`scripts/check_dist.py:43-51`) and the gate is membership of `REQUIRED_IN_WHEEL` plus substring matching over those paths (`scripts/check_dist.py:54-55, 68-81`). There is no content or hash check, so code injected inside an existing member passes. `harden-runner` at `.github/workflows/release-please.yml:66-68` uses `egress-policy: audit`, which logs outbound calls and does not block them. `pypa/gh-action-pypi-publish@v1.14.0` (`.github/workflows/release-please.yml:91`) with `id-token: write` (line 63) then publishes via trusted publishing and generates PEP 740 attestations by default — those attest that this workflow produced the artifact, not that the artifact's build inputs were trustworthy.

The CI `package` job has the same shape (`.github/workflows/ci.yml:105-113`), but only the release job's output is published.

This is an inconsistency rather than a uniform policy: the container path digest-pins its uv (`Dockerfile:13`, `ghcr.io/astral-sh/uv:0.11@sha256:3d868e555f8f1dbc324afa005066cd11e1053fc4743b9808ca8025283e65efa5`), every action is SHA-pinned, and the publish job already disables the uv cache specifically to keep a poisoned cache out of the published artifact (`.github/workflows/release-please.yml:80-82`). No doc, code comment or issue records a decision to leave the build toolchain floating (grep over `*.md`/`*.yml`/`*.json` finds no mention of hatchling or supply chain outside `pyproject.toml`).

## Why it matters

A malicious hatchling release (or a malicious uv release, or a compromise of any of hatchling's four transitive deps) published upstream between two sf2loki releases is pulled into the exact build that produces the artifact uploaded to PyPI. A build backend runs arbitrary code with full access to the source tree and the output archives, so it can inject into `src/sf2loki/**` on the way into the wheel. The path-only `check_dist.py` gate passes, trusted publishing signs it, and the PEP 740 attestation states it was genuinely built by `release-please.yml` in `rknightion/sf2loki` — which is true and which is exactly what makes the compromise credible to installers.

Exploitation depends on an upstream compromise, so this is hardening rather than a live defect. It is nonetheless the one supply-chain step the repo's existing controls (OIDC trusted publishing, SHA-pinned actions, digest-pinned container bases, disabled build cache) do not cover, and the fix is a few lines.

## Proposed approach

Pin both inputs. Prefer the `pyproject.toml` route for the backend because it covers `uv build` everywhere — release job, CI `package` job, and local builds — with no workflow duplication.

1. **Pin the build backend by exact version.** Add to `pyproject.toml`:

   ```toml
   [tool.uv]
   # The published artifact must not be built by a toolchain resolved fresh from
   # PyPI at release time; pin the backend so a compromised upstream release
   # cannot enter the build that ships to PyPI.
   build-constraint-dependencies = [
       "hatchling==1.31.0",
       "packaging==<pin>",
       "pathspec==<pin>",
       "pluggy==<pin>",
       "trove-classifiers==<pin>",
   ]
   ```

   Verified with uv 0.12.0 that `[tool.uv] build-constraint-dependencies` is honoured for `build-system.requires`: constraining `hatchling==1.0.0` against `requires = ["hatchling>=1.26"]` fails the build with `Because you require hatchling>=1.26 and hatchling==1.0.0, we can conclude that your requirements are unsatisfiable`. Leave `requires = ["hatchling>=1.26"]` as-is — it is the compatibility floor for consumers building the sdist; the constraint is what pins the build here. Resolve the transitive pins from a local `uv build -v` run.

2. **Optionally hash-pin (stronger tier).** Move the same pins into a committed `build-constraints.txt` with `--hash=sha256:…` entries and run `uv build --build-constraints build-constraints.txt --require-hashes`. Note `--require-hashes` demands an exact version *and* a hash for every package in the build environment including all transitive deps, so this tier only works once step 1's full closure is enumerated. Renovate's `pip_requirements` manager only matches filenames containing `requirements`, so a `build-constraints.txt` needs an explicit `fileMatch` entry added to `renovate.json` or it will silently rot.

3. **Pin uv in both jobs.** Add `version:` to the `setup-uv` steps at `.github/workflows/release-please.yml:75-82` and `.github/workflows/ci.yml:105-110`, with a `# renovate: datasource=github-releases depName=astral-sh/uv` comment so Renovate bumps it — the same pattern already used for the container uv at `Dockerfile:12-13`. Optionally also set `checksum:` on the release job's step to pin the binary hash. Keep the pinned version aligned with `Dockerfile:13` so the two build paths do not diverge.

4. **Pin the pins with a test.** Add `tests/test_build_toolchain_pinning.py` asserting the invariants against the repo files, following the `ROOT = Path(__file__).resolve().parents[1]` + `yaml.safe_load` pattern already used in `tests/test_config_artifacts_drift.py`. Without a test, the next Renovate bump or workflow edit silently drops the pin.

---

Imported from GitHub issue #153 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 153)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `pyproject.toml` carries `[tool.uv] build-constraint-dependencies` pinning `hatchling` and its transitive build deps to exact `==` versions, with a comment stating why.
- [ ] #2 `just gate` is green and `uv build --out-dir dist` still produces a wheel and sdist that pass `python scripts/check_dist.py dist`.
- [ ] #3 `uv build -v` output confirms the pinned hatchling version is the one used to build (no resolution to a newer release).
- [ ] #4 The `setup-uv` steps at `.github/workflows/release-please.yml:75-82` and `.github/workflows/ci.yml:105-110` both pass an explicit `version:`, carrying a `# renovate:` comment so the pin stays maintained.
- [ ] #5 The pinned uv version matches the digest-pinned uv line in `Dockerfile:12-13`, or a comment explains why they differ.
- [ ] #6 New test `tests/test_build_toolchain_pinning.py::test_build_backend_is_exact_pinned` parses `pyproject.toml` and asserts every entry in `[tool.uv] build-constraint-dependencies` uses `==`, and that `hatchling` is present.
- [ ] #7 New test `tests/test_build_toolchain_pinning.py::test_setup_uv_steps_pin_a_version` parses both workflow YAMLs and asserts every step whose `uses` starts with `astral-sh/setup-uv@` has a non-empty `with.version`, so a future edit that drops the pin fails the gate.
- [ ] #8 `just gate` green; `zizmor` and `actionlint` still pass on the edited workflows.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
