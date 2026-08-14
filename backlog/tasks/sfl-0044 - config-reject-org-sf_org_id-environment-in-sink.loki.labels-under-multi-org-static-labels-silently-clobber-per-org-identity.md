---
id: SFL-0044
title: >-
  config: reject org/sf_org_id/environment in sink.loki.labels under multi-org
  (static labels silently clobber per-org identity)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/128'
ordinal: 44000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

In multi-org mode the operator's `sink.loki.labels` are merged over every entry's own labels, so a static `environment`, `org` or `sf_org_id` key silently overwrites the per-org identity labels that `OrgSource` injects. No validation rejects it.

The path:

- `RESERVED_STATIC_LABELS` is `frozenset({"source", "event_type"})` (`src/sf2loki/sinks/loki/labels.py:14`). `guard_static_labels` (`src/sf2loki/sinks/loki/labels.py:34`) rejects only those two plus anything outside `ALLOWED_LABELS`. `ALLOWED_LABELS` (`src/sf2loki/sinks/loki/labels.py:7`) contains `org`, `sf_org_id` and `environment`, so all three are accepted as operator static labels.
- The single call site is `LokiSink.__init__` (`src/sf2loki/sinks/loki/sink.py:108`), which has no knowledge of org topology.
- Multi-org startup passes `sink.loki.labels` through unchanged: `self._pipeline.set_static_labels(deployment_static_labels(self._cfg.sink.loki.labels))` (`src/sf2loki/app.py:1156`), and `deployment_static_labels` returns `{"job": ..., "service_name": ..., **operator_labels}` (`src/sf2loki/app.py:660`).
- `OrgSource.events` sets `org` + `environment` + `sf_org_id` per entry (`src/sf2loki/sources/org_adapter.py:110`), which is the documented multi-org design — those three "move from deployment-wide static labels to per-entry here, since they differ per org" (`src/sf2loki/sources/org_adapter.py:5`).
- `Pipeline._produce` merges the static set LAST, so it wins over the per-entry set: `entry.labels = {**entry.labels, **self._static_labels}` (`src/sf2loki/app.py:306`). Nothing downstream restores the per-org value; the sink does not re-merge `cfg.labels`.
- No config validator covers this. `Config._validate_org_topology` (`src/sf2loki/config.py:1384`) checks topology, duplicate org names and the top-level `sources` clash only. The reserved set that does list these keys, `_RESERVED_LABEL_KEYS` (`src/sf2loki/config.py:552`), applies exclusively to promoted EventLogFile columns (`src/sf2loki/config.py:606`, `src/sf2loki/config.py:772`).

The misconfiguration is actively invited by the docs: the generated config reference gives `{environment: prod}` as the example value for `sink.loki.labels` (`docs/config-reference.md:163`, generated from `examples=[{"environment": "prod"}]` at `src/sf2loki/config.py:964`), and `build_static_labels` names overriding `environment` as a legitimate single-org use (`src/sf2loki/app.py:124`).

## Why it matters

An operator migrating a working single-org config to `orgs:` keeps `sink.loki.labels: {environment: prod}` (the documented example). Startup succeeds with no warning. Every entry from every org — sandbox and production alike — is then pushed with `environment="prod"`, so the two orgs land in the same Loki stream dimension and become indistinguishable. Per-org dashboards and alert rules that slice by `environment` aggregate across environments and under-report.

The `sf_org_id` and `org` variants are worse: all orgs collapse onto one static identity value, so org attribution of security-relevant events (login, audit, permission-set change streams) is silently wrong, and per-org checkpoint namespacing no longer matches what the stream labels claim. Because the streams are self-consistent, the corruption is invisible until someone cross-checks against Salesforce.

Single-org mode is unaffected and must stay unaffected: there `environment`/`sf_org_id` are genuinely deployment-wide values produced by `build_static_labels` (`src/sf2loki/app.py:117`), and overriding them is intentional, pinned by `tests/test_static_labels.py:16`.

## Proposed approach

Reject the per-org identity keys at config-validation time, so `sf2loki --check` and `sf2loki doctor` catch it before any network call and before the sink is constructed.

1. In `src/sf2loki/sinks/loki/labels.py`, add alongside `RESERVED_STATIC_LABELS`:

   ```python
   # Additionally per-entry in MULTI-org mode: OrgSource injects these per org
   # (sources/org_adapter.py), so a static override collapses org separation.
   MULTI_ORG_RESERVED_STATIC_LABELS: frozenset[str] = RESERVED_STATIC_LABELS | frozenset(
       {"org", "sf_org_id", "environment"}
   )
   ```

   `guard_static_labels` already takes `reserved` as a parameter (`src/sf2loki/sinks/loki/labels.py:37`), so no signature change is needed.

2. Add a `model_validator(mode="after")` on `Config` in `src/sf2loki/config.py` (next to `_validate_org_topology` at `src/sf2loki/config.py:1384`, ordered after it) that, when `self.orgs` is non-empty, calls `guard_static_labels(self.sink.loki.labels, reserved=MULTI_ORG_RESERVED_STATIC_LABELS)` and re-raises as a `ValueError` whose message names the offending keys and states the remedy (set `environment` per entry in `orgs[].environment`; `org` comes from `orgs[].name`; `sf_org_id` is resolved from each org's token). Import is cycle-free: both `src/sf2loki/sinks/__init__.py` and `src/sf2loki/sinks/loki/__init__.py` are empty and `labels.py` imports only `collections.abc`.

3. Update the error-adjacent docs: note the multi-org restriction in the `sink.loki.labels` field description (`src/sf2loki/config.py:959`), in `deployment_static_labels`' docstring (`src/sf2loki/app.py:660`), in the label-strategy section (`docs/architecture.md:250`) and in `src/sf2loki/sinks/CLAUDE.md`'s label-guard note. Re-run `just gen-config` after touching the field description (drift gate: `tests/test_config_artifacts_drift.py`).

Alternative rejected: widening `RESERVED_STATIC_LABELS` unconditionally. That would break the documented and tested single-org `environment` override (`tests/test_static_labels.py:16`).

---

Imported from GitHub issue #128 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 128)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `MULTI_ORG_RESERVED_STATIC_LABELS` added in `src/sf2loki/sinks/loki/labels.py`, equal to `RESERVED_STATIC_LABELS | {"org", "sf_org_id", "environment"}`.
- [ ] #2 A `Config` validator raises on a multi-org config (`orgs:` non-empty) whose `sink.loki.labels` contains `org`, `sf_org_id` or `environment`; the message lists every offending key and states where the per-org value comes from.
- [ ] #3 Single-org behaviour byte-identical: `sink.loki.labels: {environment: prod}` with top-level `salesforce:` still loads and still wins over the derived value.
- [ ] #4 `tests/test_multiorg_config.py`: parametrised test over `org`, `sf_org_id`, `environment` asserting config load fails with the key named in the message; plus a test that a benign key (`service_name`) is still accepted under `orgs:`.
- [ ] #5 `tests/test_static_labels.py`: regression test that `build_static_labels(operator_labels={"environment": "prod-eu"})` still yields `environment == "prod-eu"` (single-org override preserved).
- [ ] #6 `tests/test_multiorg_app.py`: end-to-end test that with an accepted multi-org `sink.loki.labels` (e.g. `{"service_name": "salesforce"}`), entries emitted through `Pipeline._produce` retain the per-org `org`/`environment`/`sf_org_id` values injected by `OrgSource`.
- [ ] #7 `tests/sinks/test_labels.py`: test that `guard_static_labels(..., reserved=MULTI_ORG_RESERVED_STATIC_LABELS)` rejects each of the five reserved keys and accepts `job`/`service_name`.
- [ ] #8 Field description, `deployment_static_labels` docstring, `docs/architecture.md` label section and `src/sf2loki/sinks/CLAUDE.md` state the multi-org restriction; `just gen-config` re-run so `config.example.yaml` + `docs/config-reference.md` match and the drift gate passes.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
