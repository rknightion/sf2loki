---
id: SFL-0048
title: >-
  cli: run/backfill dump a traceback (exit 1) for non-ConfigError wiring
  failures - overlap and label-guard errors bypass the documented exit-2
  contract
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/132'
ordinal: 48000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The run path (no subcommand) guards `App.build` with a single-type handler:

```python
# src/sf2loki/cli.py:261-268
try:
    cfg = load(args.config)
    app = App.build(cfg)
except ConfigError as exc:
    print(f"sf2loki: {exc}", file=sys.stderr)
    return _CONFIG_ERROR_EXIT_CODE
```

`App.build` raises two wiring errors that are **not** `ConfigError` subclasses, so both escape uncaught:

- `OverlapError` — `src/sf2loki/sources/overlap.py:41` (`class OverlapError(Exception)`), raised at `src/sf2loki/sources/overlap.py:117` from `check_overlap`, called at `src/sf2loki/app.py:827`.
- `LabelGuardError` — `src/sf2loki/sinks/loki/labels.py:17` (`class LabelGuardError(ValueError)`), raised at `labels.py:31` / `labels.py:48` from `guard_static_labels`, called in the `LokiSink` constructor at `src/sf2loki/sinks/loki/sink.py:108`, which `App.build` invokes at `src/sf2loki/app.py:922`.

Nothing upstream prevents either state from a plain config file: `sink.loki.labels` is an unvalidated `dict[str, str]` (`src/sf2loki/config.py:959-964`), so a disallowed or reserved label key passes Pydantic validation and only fails at sink construction; the either/or overlap guard runs only inside `App.build`.

`--check` catches bare `Exception` (`src/sf2loki/cli.py:252-257`) and therefore reports both cleanly with exit 2. The run path does not. Reproduced in-process against two configs:

| config | `sf2loki --check --config c.yaml` | `sf2loki --config c.yaml` |
|---|---|---|
| `pubsub.topics: ["/event/LoginEventStream"]` + `eventlog_objects.objects: [{name: LoginEvent}]` | `config check FAILED: ...` / rc 2 | uncaught `sf2loki.sources.overlap.OverlapError` |
| `sink.loki.labels: {user_id: abc}` | `config check FAILED: Disallowed label keys: user_id` / rc 2 | uncaught `sf2loki.sinks.loki.labels.LabelGuardError` |

Because the console script is `sf2loki = "sf2loki.cli:main"` (`pyproject.toml:34`) and `python -m sf2loki` is `sys.exit(main())` (`src/sf2loki/__main__.py:10`), an escaping exception prints a Python traceback and exits **1**.

This contradicts three places that state the opposite:

- `src/sf2loki/cli.py:35-43` — "`--check`, run, and backfill all return this on a config/wiring error (bad secrets, source overlap, org selection, etc.), so scripts can check for one code regardless of which entrypoint they invoke."
- `docs/reference/cli.md:21-30` — "`2` | Config/wiring error — bad secrets, invalid YAML, source overlap, bad org selection."
- `src/sf2loki/cli.py:265-266` — the handler's own comment: "Operator-facing config problems get a clean message, not a traceback (same failure surface `--check` reports; unified config-error exit code)."

`backfill` is exposed the same way for the label guard: `run_backfill` constructs `LokiSink` at `src/sf2loki/backfill.py:740`, which is outside its own `try` (opens at `backfill.py:757`) and outside cli.py's config `try` (the `except (ConfigError, ValueError)` block ends at `src/sf2loki/cli.py:221`, while `uvloop.run(run_backfill(...))` is at `src/sf2loki/cli.py:229-241`). That handler would have caught a `ValueError`, which is precisely why the construction must move inside a guarded region or the call site must be wrapped.

Existing coverage does not catch this: `tests/test_cli.py:110-125` pins "no traceback, exit 2" for the run path but only for a missing-secret `ConfigError`; `tests/test_cli.py:55-80` pins the overlap case for `--check` only.

Related but distinct from #71 item 4, which unified the *value* of the config-error exit code (`--check` 1 -> 2). That change did not broaden the run path's exception type.

## Why it matters

An operator who misconfigures overlapping sources (the either/or-per-category rule is easy to trip when adding a second source for one category) or sets a disallowed `sink.loki.labels` key gets a traceback and exit 1 on service start, instead of the documented one-line message and exit 2. Supervisors and deployment scripts that branch on the documented contract — treat 2 as "invalid config, stop and alert a human", anything else as "crashed, restart" — misclassify a permanent config error as a transient crash and restart-loop the container. It also makes the `--check`/`run` pair inconsistent for byte-identical config, which is the exact property `cli.py:35-43` promises.

## Proposed approach

Preferred: give the wiring guards a common ancestor so every current and future call site is covered by one handler, rather than maintaining an exception tuple that drifts as guards are added.

1. `src/sf2loki/sources/overlap.py:41` -> `class OverlapError(ConfigError)` (import `ConfigError` from `sf2loki.config`; `config.py` imports nothing from `sources/`, so no cycle).
2. `src/sf2loki/sinks/loki/labels.py:17` -> `class LabelGuardError(ConfigError, ValueError)`. Keeping `ValueError` in the bases preserves every existing `pytest.raises(LabelGuardError)` assertion and any caller relying on `ValueError` (notably `src/sf2loki/cli.py:219`). `config.py` does not import `sinks/`, so no cycle.
3. Guard the backfill call: wrap `uvloop.run(run_backfill(...))` (`src/sf2loki/cli.py:229-241`) in `except ConfigError` returning `_CONFIG_ERROR_EXIT_CODE`, or move the `LokiSink` construction at `src/sf2loki/backfill.py:740` inside a region that returns a config-error code. The CLI-side wrapper is simpler and keeps the exit-code constant in one module.
4. Leave `--check`'s bare `except Exception` as-is; do not widen the run path to bare `Exception` — a genuine programming error must still surface as a traceback rather than be reported as invalid config.

Fallback if the base-class change is rejected: `except (ConfigError, OverlapError, LabelGuardError)` on both the run path and the backfill call, with a comment that any new startup guard must be added to the tuple.

---

Imported from GitHub issue #132 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 132)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sf2loki --config <overlapping-config>` prints a single-line message to stderr with no `Traceback` and returns 2.
- [ ] #2 `sf2loki --config <config with sink.loki.labels: {user_id: abc}>` prints a single-line message to stderr with no `Traceback` and returns 2.
- [ ] #3 `sf2loki --config <config with sink.loki.labels: {event_type: x}>` (reserved static key, `labels.py:48`) also returns 2 with no traceback.
- [ ] #4 `sf2loki backfill --since <date> --config <config with a disallowed sink.loki.labels key>` returns 2 with no traceback, failing before any network call.
- [ ] #5 The run path still propagates non-config exceptions (e.g. a deliberately patched `App.build` raising `RuntimeError`) rather than reporting them as a config error.
- [ ] #6 `tests/test_cli.py`: a test that the run path returns 2 and emits no `Traceback` for the overlapping config already used by `test_check_fails_on_source_overlap` (`tests/test_cli.py:55-80`), asserting the same rc as the `--check` invocation on the same file.
- [ ] #7 `tests/test_cli.py`: a test that the run path returns 2 and emits no `Traceback` for a disallowed static label key, and one for a reserved static label key.
- [ ] #8 `tests/test_cli.py`: a test that the `backfill` subcommand returns 2 for a disallowed static label key without making a network call.
- [ ] #9 `tests/test_cli.py`: a test that the run path re-raises a non-config exception from `App.build` (monkeypatched) so the widened handler cannot mask real bugs.
- [ ] #10 Existing `tests/sinks/test_labels.py`, `tests/sinks/test_sink.py`, `tests/sources/test_overlap.py` and `tests/test_app_integration.py` assertions on `LabelGuardError`/`OverlapError` remain green unchanged.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
