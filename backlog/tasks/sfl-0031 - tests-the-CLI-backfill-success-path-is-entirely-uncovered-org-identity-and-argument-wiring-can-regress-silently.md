---
id: SFL-0031
title: >-
  tests: the CLI backfill success path is entirely uncovered - org identity and
  argument wiring can regress silently
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-3
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/115'
ordinal: 31000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The success path of the `sf2loki backfill` subcommand in `src/sf2loki/cli.py` has zero test coverage. Full-suite coverage confirms it:

```
$ uv run --with pytest-cov pytest tests/ --cov=sf2loki.cli --cov-report=term-missing -q
Name                 Stmts   Miss  Cover   Missing
--------------------------------------------------
src/sf2loki/cli.py      98     16    84%   24-25, 198-200, 210-218, 222-229, 263, 269-270
1045 passed, 1 skipped
```

(`pytest-cov` is not a dev dependency; the `--with pytest-cov` form above reproduces the numbers without changing the lockfile.)

The uncovered region `cli.py:210-241` is everything the CLI does after a successful config load:

- `cli.py:210` — `org, note = select_org(cfg, args.org)`
- `cli.py:215-217` — the issue #40 org-identity derivation:
  ```python
  resolved = cfg.resolved_orgs()
  org_name = org.name
  legacy_fallback = bool(resolved) and org.name == resolved[0].name
  ```
- `cli.py:218` — `cfg = as_single_org_view(cfg, org)`
- `cli.py:222-223` — printing the multi-org scoping note to stderr
- `cli.py:224-228` — `--event-types` comma-split and strip
- `cli.py:229-241` — the `run_backfill(...)` call and every argument passed to it (`since`, `until`, `event_types`, `interval`, `ingest_timestamps`, `concurrency`, `org_name`, `legacy_fallback`)

The only CLI-level backfill invocation in the suite is `tests/test_cli.py:304`, inside `test_check_and_run_and_backfill_share_one_config_error_exit_code`. That test's config sets `private_key_file: /does/not/exist.pem`, so `load()` at `cli.py:209` raises `ConfigError` from `_resolve_salesforce_secrets` (`config.py:1514-1521`) and only the `except` arm at `cli.py:219-221` executes. It asserts an exit code, nothing else.

`run_backfill`'s own org semantics *are* pinned, but only with literal arguments handed in by the test: `tests/test_backfill.py:949-959` (`org_name="orgA", legacy_fallback=True`), `tests/test_backfill.py:966-976` (`org_name="orgB", legacy_fallback=False`), `tests/test_backfill.py:1011-1021`. Those tests are indifferent to how the CLI computes those values.

An extra, invisible constraint lives in the ordering. `as_single_org_view` (`config.py:1466-1474`) copies with `update={"salesforce": ..., "sources": ..., "orgs": []}`, so after `cli.py:218` the config's `resolved_orgs()` returns one org whose `name` is `""` — the org identity is unrecoverable. The derivation at `cli.py:215-217` therefore *must* run before line 218, and the comment at `cli.py:211-214` says so, but no test enforces it.

## Why it matters

Every mutation in this region ships green:

- Inverting `cli.py:217` to `org.name != resolved[0].name` gives `legacy_fallback=True` for every non-first org. `sf2loki backfill --org emea` then falls back to the unprefixed `backfill:{interval}:{event_type}` key on a load miss (`backfill.py:576`), inherits the first org's watermark, and silently skips every older EventLogFile for `emea` — the exact silent history loss that #40 fixed.
- Moving the derivation below `cli.py:218`, or passing `args.org` instead of `org.name`, collapses `org_name` to `""` for the default (no `--org`) case, putting every org back on one shared unprefixed key.
- A typo in the `--event-types` split (`cli.py:224-228`) or a swapped/dropped argument in the `run_backfill(...)` call (`cli.py:229-241`) — `since`/`until` transposed, `ingest_timestamps` dropped, `concurrency` not forwarded — is equally undetectable.

The failure mode is silent in production: the backfill exits 0 and reports success while writing less history than requested. This is a multi-org data-loss class that has already shipped once (#40).

## Proposed approach

Add CLI-level tests to `tests/test_cli.py` that monkeypatch `sf2loki.backfill.run_backfill` with a recorder coroutine and assert the derived arguments. This works because `cli.py:203` imports `run_backfill` at call time inside `main`, so patching the module attribute before invoking `main` is intercepted (verified: `main([...])` returned 0 with the recorder capturing `org_name="emea"`, `legacy_fallback=False`, `event_types=["a", "b"]` from `--event-types "a, b,"`).

Add a `_valid_multi_org_config(tmp_path)` helper alongside the existing `_valid_config` (`tests/test_cli.py:12-30`), writing a two-org YAML (verified to load):

```yaml
orgs:
  - name: prod
    salesforce:
      client_id: cid
      username: svc@example.com
      private_key_file: <tmp key.pem>
    sources:
      pubsub:
        enabled: false
  - name: emea
    salesforce:
      client_id: cid2
      username: svc2@example.com
      private_key_file: <tmp key.pem>
    sources:
      pubsub:
        enabled: false
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
```

Recorder shape:

```python
captured: dict[str, object] = {}

async def _recorder(cfg, **kwargs):
    captured["cfg"] = cfg
    captured.update(kwargs)
    return 0

monkeypatch.setattr("sf2loki.backfill.run_backfill", _recorder)
```

Assert on `captured` after `main([...])` returns 0. The single-org case reuses `_valid_config` and expects `org_name == ""`.

---

Imported from GitHub issue #115 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 115)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `tests/test_cli.py` gains a test asserting that `main(["--config", cfg, "backfill", "--since", "2026-01-01", "--org", "emea"])` on a two-org config reaches `run_backfill` with `org_name == "emea"` and `legacy_fallback is False`, and returns 0.
- [ ] #2 Same test file asserts that `--org prod` (the FIRST configured org) yields `org_name == "prod"` and `legacy_fallback is True`.
- [ ] #3 A test asserts that omitting `--org` on the two-org config selects the first org (`org_name == "prod"`, `legacy_fallback is True`) and prints the multi-org scoping note from `select_org` (`config.py:1459-1462`) to stderr (`cli.py:222-223`).
- [ ] #4 A test asserts that a single-org config (no `orgs:` list, e.g. `_valid_config`) yields `org_name == ""`, preserving the legacy unprefixed key path (`backfill.py:478-480`).
- [ ] #5 A test asserts `--event-types "a, b,"` arrives as `event_types == ["a", "b"]`, and that omitting the flag arrives as `event_types is None` (`cli.py:224-228`).
- [ ] #6 A test asserts the remaining arguments are forwarded unchanged: `since` is the parsed `--since`, `until` is `None` when the flag is absent and the parsed value when present, plus `interval`, `ingest_timestamps`, and `concurrency` (`cli.py:229-241`).
- [ ] #7 A test asserts the config handed to `run_backfill` is the single-org view: `captured["cfg"].salesforce.client_id` equals the selected org's `client_id` and `captured["cfg"].orgs == []` (`config.py:1474`). This pins the ordering constraint — the derivation at `cli.py:215-217` running after `cli.py:218` would fail the `org_name` assertions above.
- [ ] #8 Optional, same class of gap surfaced by the same coverage run: `cli.py:198-200` (the `doctor` subcommand dispatch, including `org_name=args.org` pass-through) is also uncovered; a recorder test patching `sf2loki.doctor.run_doctor` closes it.
- [ ] #9 `uv run --with pytest-cov pytest tests/ --cov=sf2loki.cli --cov-report=term-missing` no longer lists 210-218 or 222-229 as missing.
- [ ] #10 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
