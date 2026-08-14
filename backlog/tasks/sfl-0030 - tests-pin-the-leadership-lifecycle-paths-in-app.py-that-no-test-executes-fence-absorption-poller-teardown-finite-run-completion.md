---
id: SFL-0030
title: >-
  tests: pin the leadership-lifecycle paths in app.py that no test executes
  (fence absorption, poller teardown, finite-run completion)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/114'
ordinal: 30000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Three branches of the per-acquisition leadership lifecycle in `App.run()` are never executed by the test suite. A coverage run over the whole suite reports `src/sf2loki/app.py` at 96% with these lines missing (among others):

```
src/sf2loki/app.py   588   26   96%   ... 1170, 1182, 1238, 1292-1294
```

Reproduce (coverage is not a project dev dependency, so use an ephemeral env):

```bash
uv run --with coverage --with pytest-cov python -m pytest -q -p no:randomly \
  --cov=sf2loki.app --cov-report=term-missing tests/
```

The three gaps:

1. **`StateFenceError` absorption — src/sf2loki/app.py:1237-1238.** `App._run_pipeline` wraps `_drain_with_grace(self._pipeline.run(run_stop), ...)` in `except StateFenceError: log.warning("checkpoint commit fenced — leadership lost; standing by")`. The docstring at src/sf2loki/app.py:1229-1232 states the contract explicitly: a fence "is a leadership transition, not a fatal crash — the coordinator drives the move back to standby. Any other exception propagates so the pipeline-done callback can crash the process." `grep -rn StateFenceError tests/` shows every occurrence is a unit test of the fence *producers* or of the store (tests/state/test_file_store.py:212, tests/state/test_file_store.py:329, tests/state/test_file_store.py:355, tests/coordinate/test_file_lease.py:470, tests/coordinate/test_file_lease.py:482, tests/coordinate/test_file_lease.py:524, tests/coordinate/test_k8s_lease.py:611, tests/test_statecmd.py:89). Nothing raises `StateFenceError` through a running pipeline, so the `except` clause at app.py:1237 has never run.

2. **Per-acquisition poller teardown — src/sf2loki/app.py:1291-1294.** `App._stop_acquisition` ends with `if poller_tasks: for poller_task in poller_tasks: poller_task.cancel()` then `await asyncio.gather(*poller_tasks, return_exceptions=True)`. `SalesforceLimitsConfig.enabled` defaults to `False` (src/sf2loki/config.py:121-127) and the `_cfg` helper at tests/test_app_integration.py:23 never enables limits, so `self._limits_pollers` (src/sf2loki/app.py:903, populated at src/sf2loki/app.py:1016-1024) is empty in every test that calls `App.run()`. `poller_tasks` is therefore always `[]` and the guard is always false. The only test that builds real pollers, tests/test_multiorg_app.py:72-80, asserts `len(appn._limits_pollers) == 2` and never runs the app.

3. **Finite-run clean completion — src/sf2loki/app.py:1181-1182.** In `_on_pipeline_done`, `if not run_stop.is_set(): stop.set()` encodes "the pipeline finished cleanly while we never asked it to stop, so the sources exhausted on their own (a finite run) — take the whole process down". Only three tests invoke `App.run()`: tests/test_app_integration.py:142 (auth fail-fast, exits before the lifecycle), tests/test_app_integration.py:456 (`_CountingPipeline.run` awaits `stop` forever, tests/test_app_integration.py:418-420, so it never returns on its own), and tests/test_app_integration.py:487 (`_CrashingPipeline` raises `RuntimeError`, covering the *crash* branch at src/sf2loki/app.py:1174-1177). No pipeline double returns cleanly with `run_stop` unset.

## Why it matters

The fence-absorb line is the seam the #47/#48/#49 HA work depends on. Under file-lease or k8s-lease HA, a demotion routinely races an in-flight checkpoint commit: the store's pre-commit fence (src/sf2loki/state/file_store.py:261, src/sf2loki/state/file_store.py:289, src/sf2loki/coordinate/file_lease.py:132, src/sf2loki/coordinate/k8s_lease.py:196) raises `StateFenceError`, which must surface as a quiet transition to standby. A regression that deletes the `except`, narrows its scope, or a store refactor that wraps `StateFenceError` in another exception type converts every fenced commit into `crash.append(exc)` at src/sf2loki/app.py:1175 → `raise crash[0]` at src/sf2loki/app.py:1222 → process exit nonzero. The result is a demote-triggered crash-and-restart loop on the exact flow the HA work was built to make clean, and no test fails. The inverse direction *is* pinned (tests/test_app_integration.py:463 asserts a non-fence `RuntimeError` propagates out of `run()`), so the pair is half-covered: the suite protects "other exceptions crash" but not "a fence does not".

For poller teardown, `LimitsPoller.run` does honour its stop event (src/sf2loki/obs/limits_poller.py:46-66) and `_stop_acquisition` sets `run_stop` at src/sf2loki/app.py:1288 before cancelling, so a dropped `cancel()` would not poll indefinitely. The real exposure is that demotion becomes unbounded: a poller blocked inside `await self._client.fetch()` (src/sf2loki/obs/limits_poller.py:51) is only interrupted by the cancel, so without it `_stop_acquisition` awaits an in-flight Salesforce REST call before the standby can release, and `reset_state()`/`set_not_ready("standby")` at src/sf2loki/app.py:1205-1207 are delayed behind it. It also silently breaks if the poller's stop-honouring contract ever changes, leaving orphaned tasks per acquisition across repeated acquire/lose cycles.

The finite-run line governs whether a one-shot/exhausted-sources deployment exits cleanly (exit 0, `crash` empty) or hangs holding leadership. Nothing distinguishes those outcomes today.

## Proposed approach

Extend the existing scripted-coordinator harness in tests/test_app_integration.py (helpers at tests/test_app_integration.py:395-436: `_FakeTokens`, `_CountingPipeline`, `_CyclingCoordinator`). No production change is required — this issue is test-only.

1. **Fence absorption.** Add a `_FencedPipeline(_CountingPipeline)` whose `run()` raises `StateFenceError("not leader")` (imported from `sf2loki.coordinate.base`). Drive it with a coordinator that acquires, lets the task run, then calls `on_lose()` and sets `stop` — a two-phase variant of `_CyclingCoordinator` — so the demote→standby ordering is what is asserted, not just the bare `except`. Assert: `await asyncio.wait_for(appn.run(), timeout=5)` completes without raising; `appn._metrics.registry.get_sample_value("sf2loki_leader") == 0.0`; the pipeline's `reset_state()` was invoked (record it on the double, mirroring the note at tests/test_app_integration.py:412-415). Keep tests/test_app_integration.py:463 as the paired negative case and reference it in a comment so the pair is obvious.
2. **Poller teardown.** Inject a fake poller into `appn._limits_pollers` after `App.build(cfg)` that records cancellation: `async def run(self, stop): try: await asyncio.sleep(3600) except asyncio.CancelledError: self.cancelled = True; raise`. Deliberately ignore `stop` so the assertion can only pass via `poller_task.cancel()` at src/sf2loki/app.py:1293 — this is what makes the test pin cancellation rather than stop-honouring. Run one acquire/lose cycle and assert `poller.cancelled is True` and that `run()` completes inside a short `asyncio.wait_for` timeout (a dropped cancel makes it hang, so the assertion is real). Add a second assertion that a repeated acquire/lose/acquire cycle leaves no pending poller tasks (`current` empty after `_stop_acquisition`, which pops all three keys at src/sf2loki/app.py:1283-1285).
3. **Finite-run completion.** Add a `_FinitePipeline(_CountingPipeline)` whose `run()` returns immediately, drive it with `NoopCoordinator()` (which awaits `stop` at src/sf2loki/coordinate/base.py:49, so only `stop.set()` at src/sf2loki/app.py:1182 can end the run), and assert `run()` returns without raising and `pipeline.runs == 1`.

Optional hardening: assert the absorbed-fence case does not append to `crash` by asserting `run()` raises nothing — the crash list is local to `run()`, so the observable proxy is a clean return.

---

Imported from GitHub issue #114 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 114)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 A test raises `StateFenceError` out of a running pipeline through `App._run_pipeline` and asserts `App.run()` returns without raising, exercising src/sf2loki/app.py:1237-1238.
- [ ] #2 That test also asserts the demotion completed cleanly: `sf2loki_leader` gauge back to `0.0` and the pipeline's `reset_state()` invoked (src/sf2loki/app.py:1205).
- [ ] #3 A test with a non-empty `App._limits_pollers` asserts each poller task is cancelled during demotion, exercising src/sf2loki/app.py:1291-1294, using a poller that ignores its stop event so only `cancel()` can satisfy it.
- [ ] #4 A test asserts a poller blocked in an in-flight fetch does not stall demotion (the run completes inside a short `asyncio.wait_for` timeout).
- [ ] #5 A test with a pipeline whose `run()` returns immediately while `run_stop` is unset asserts `App.run()` shuts down cleanly, exercising src/sf2loki/app.py:1181-1182.
- [ ] #6 `uv run --with coverage --with pytest-cov python -m pytest -q --cov=sf2loki.app --cov-report=term-missing tests/` no longer lists 1182, 1238 or 1292-1294 as missing.
- [ ] #7 `just gate` green (ruff + `ruff format --check` + `mypy src` + pytest).
- [ ] #8 No production code change in `src/` (this is a coverage gap, not a defect); if the tests uncover a real defect, split that into its own issue.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
