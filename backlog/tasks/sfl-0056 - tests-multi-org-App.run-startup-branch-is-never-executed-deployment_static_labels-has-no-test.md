---
id: SFL-0056
title: >-
  tests: multi-org App.run() startup branch is never executed -
  deployment_static_labels has no test
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/140'
ordinal: 56000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The multi-org branch of `App.run()` is dead code as far as the test suite is concerned. `coverage run --source=sf2loki -m pytest tests/` over the full suite (1045 passed, 1 skipped) reports `src/sf2loki/app.py` at 96% with `1152-1156` and `669` among the missed lines:

- `src/sf2loki/app.py:1152` — `await self._probe_orgs()`
- `src/sf2loki/app.py:1156` — `self._pipeline.set_static_labels(deployment_static_labels(self._cfg.sink.loki.labels))`
- `src/sf2loki/app.py:669` — the `return` inside `deployment_static_labels` (defined at `src/sf2loki/app.py:660`)

No test reaches them:

- `rg -n "deployment_static_labels" tests/` returns nothing. The function has zero unit tests and its only call site is never executed.
- `rg -n "\.run\(\)" tests/` returns exactly three sites — `tests/test_app_integration.py:142`, `:456`, `:487` — and all three build a single-org config, so all three take the `if not self._multi_org` branch at `src/sf2loki/app.py:1131`.
- `tests/test_multiorg_app.py` calls `appn._probe_orgs()` directly (`:151`, `:163`) and otherwise only asserts `App.build` wiring. It never drives `App.run()`.
- `tests/test_static_labels.py` covers only the single-org `build_static_labels` (`src/sf2loki/app.py:117`), and both of its tests assert `labels["sf_org_id"]` is present — the opposite of the multi-org invariant.
- `rg -n "set_static_labels" tests/` hits only `tests/test_pipeline.py:132` and `:421`, which pass hand-written dicts to the pipeline setter and exercise neither app-level builder.

The invariant the branch encodes: in multi-org mode `sf_org_id`, `environment` and `org` are injected **per entry** by each org's `OrgSource` (`src/sf2loki/sources/org_adapter.py`), so the shared pipeline must carry only `job`/`service_name` plus the operator's `sink.loki.labels`. Nothing in the suite pins that composition.

## Why it matters

Static labels take precedence over per-entry labels. `src/sf2loki/app.py:305-306` merges them as:

```python
if self._static_labels:
    entry.labels = {**entry.labels, **self._static_labels}
```

Two silent-regression paths follow, both of which keep the suite green:

1. **Cross-org label collapse.** A refactor that made multi-org mode set `sf_org_id`/`environment` statically would override the per-org values `OrgSource` injects, stamping every org's entries with one org's identity. Loki bakes labels into streams at ingest, so the misattribution is permanent and not correctable at query time.
2. **Startup crash on every multi-org deployment.** A `Config` built with `orgs:` leaves `salesforce` as `None` (verified: constructing `tests/test_multiorg_app.py::_multi_cfg()` yields `cfg.salesforce is None`). The `assert self._cfg.salesforce is not None` guard at `src/sf2loki/app.py:1137` sits inside the single-org branch only, so a collapsed branch calling `build_static_labels(environment=self._cfg.salesforce.environment, ...)` raises `AttributeError` before readiness on every multi-org start.

Multi-org ingestion (#31) is a shipped feature whose entire `run()`-level startup sequence — org probe wiring plus deployment-wide label selection — has no end-to-end execution in CI. Any future restructuring of `run()` is validated for single-org only.

## Proposed approach

Add a `run()`-level multi-org test to `tests/test_multiorg_app.py`, reusing the fake-collaborator pattern already established in `tests/test_app_integration.py:420-500` (replace `appn._pipeline`, `appn._coordinator` after `App.build`):

1. Build the app from the existing `_multi_cfg()` helper, with `service={"health_addr": "127.0.0.1:0"}` so the health server binds an ephemeral port, and `sink={"loki": {..., "labels": {"environment": "prod-eu"}}}` in a second case to pin operator-label merging.
2. Replace `appn._orgs` with two `_OrgAuth` entries backed by the existing `_OkTokens` stand-in so the probe succeeds for both, and record which orgs were probed.
3. Replace `appn._pipeline` with a fake that captures `set_static_labels` and whose `run()` returns immediately (sources exhausted), so `_on_pipeline_done` sets the global stop and `run()` returns.
4. Set `appn._coordinator = NoopCoordinator()` (`from sf2loki.coordinate.base import NoopCoordinator`).
5. `await asyncio.wait_for(appn.run(), timeout=5)`, with `await appn._health.stop()` in a `finally`.
6. Assert the captured static labels are exactly `{"job": "sf2loki", "service_name": "sf2loki", **operator_labels}` — with `sf_org_id` and `org` absent, and `environment` absent unless supplied by `sink.loki.labels` — and that both orgs were probed.

Also add direct unit tests for `deployment_static_labels` (`src/sf2loki/app.py:660`) in `tests/test_static_labels.py`, alongside the existing `build_static_labels` tests: the default set, operator-label override of `service_name`, and explicit assertions that `sf_org_id` and `org` are never present.

An assertion that `deployment_static_labels` produces no key in `{"sf_org_id", "org", "environment"}` by default is the guard that catches path 1 above; the `run()`-level test is the guard that catches path 2.

---

Imported from GitHub issue #140 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 140)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `tests/test_static_labels.py` unit-tests `deployment_static_labels`: default set equals `{"job": "sf2loki", "service_name": "sf2loki"}`; operator `sink.loki.labels` merge last and win; `sf_org_id` and `org` are asserted absent from the default set.
- [ ] #2 `tests/test_multiorg_app.py` drives `await App.run()` with a two-org config through the multi-org branch, asserting the pipeline's static labels carry no `sf_org_id`/`org` and that both orgs were probed.
- [ ] #3 A second case pins that operator `sink.loki.labels` (e.g. `environment`) still reach the pipeline in multi-org mode.
- [ ] #4 Coverage of `src/sf2loki/app.py` no longer lists `669` or `1152-1156` as missed (`coverage run --source=sf2loki -m pytest tests/ && coverage report -m --include='*sf2loki/app.py'`).
- [ ] #5 Mutating `src/sf2loki/app.py:1156` to call `build_static_labels` instead fails the new tests rather than raising an unrelated `AttributeError` only at runtime.
- [ ] #6 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
