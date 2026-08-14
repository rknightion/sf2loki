---
id: SFL-0005
title: >-
  backfill: entries omit the deployment identity labels
  (job/service_name/environment/sf_org_id) and the org label
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/89'
ordinal: 5000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sf2loki backfill` pushes EventLogFile history into Loki with a label set that is missing every identity label the daemon injects, so backfilled streams do not match the label model documented in `docs/architecture.md` ("Label / cardinality strategy") or queried by the shipped dashboards.

`run_backfill` builds the static label set from the operator config alone:

```python
# src/sf2loki/backfill.py:748
static_labels = dict(cfg.sink.loki.labels)
```

That mapping is threaded unchanged through `_process_event_type` (backfill.py:561) → `_process_files` (backfill.py:495) → `_process_file` (backfill.py:411) → `_shape_file_rows`, which is the only place per-entry labels are assembled:

```python
# src/sf2loki/backfill.py:294-299
labels: dict[str, str] = {
    **promote_labels(row, label_fields),
    **static_labels,
    "source": "eventlogfile",
    "event_type": event_type,
}
```

plus `labels["backfill"] = "true"` in default (event-time) mode at backfill.py:307.

Nothing downstream adds labels. `LokiSink` validates operator statics once at construction (`guard_static_labels(cfg.labels)`, `src/sf2loki/sinks/loki/sink.py:108`) and never injects anything per entry.

The daemon, by contrast, always injects a deployment identity set:

- Single-org: `build_static_labels` returns `job=sf2loki`, `service_name=sf2loki`, `environment=<salesforce.environment>`, `sf_org_id=<resolved org id>`, operator labels merged last (`src/sf2loki/app.py:117-138`), applied to the pipeline at `src/sf2loki/app.py:1144-1149`.
- Multi-org: `deployment_static_labels` sets `job`/`service_name` + operator labels (`src/sf2loki/app.py:660-670`, applied at `src/sf2loki/app.py:1155`), and each org's `OrgSource` merges `org` + `environment` + `sf_org_id` into every real entry (`src/sf2loki/sources/org_adapter.py:110-117`).

`sink.loki.labels` defaults to `{}` and its own field description states "job + sf_org_id are added automatically" (`src/sf2loki/config.py:959-965`), so a default deployment's backfilled entries carry exactly `{source="eventlogfile", event_type=<type>, backfill="true"}` — no `job`, no `service_name`, no `environment`, no `sf_org_id`.

The org gap is separate from issue #40. `cli.py:208-217` resolves `--org`, captures `org.name` before `as_single_org_view` drops it, and passes it to `run_backfill`; `org_name` is then used *only* by `_backfill_checkpoint_key` (`src/sf2loki/backfill.py:465-480`, called at backfill.py:574). It never reaches the label set, even though `org` is in `ALLOWED_LABELS` (`src/sf2loki/sinks/loki/labels.py:7-9`).

## Why it matters

Every shipped SF-event dashboard selects on `job`. `$job` is a `CustomVariable` hardcoded to `sf2loki` (e.g. `deploy/grafana/dashboards/sf2loki-security-access.json:42-52`) and every panel selector is `{job="$job", event_type=..., source="eventlogfile"}` (`deploy/grafana/dashboards/sf2loki-security-access.json:142`, `deploy/grafana/dashboards/sf2loki-api-integration.json:161`, `deploy/grafana/dashboards/sf2loki-apex-performance.json:142`, `deploy/grafana/dashboards/sf2loki-overview.json:743`). Backfilled entries have no `job` label at all, so:

- Every dashboard panel and any LogQL selector or alert keyed on `job="sf2loki"`, `sf_org_id`, or `environment` silently returns nothing for backfilled history. The backfill appears to succeed (the run prints a rows_pushed summary) while its output is unreachable through the shipped query surface.
- Missing `service_name` means the streams surface as `unknown_service` in Grafana's service-scoped views rather than under the sf2loki exporter — the exact reason `build_static_labels` sets it (`src/sf2loki/app.py:121-123`).

In multi-org, `sf2loki backfill --org a` followed by `--org b` for the same `--interval`/event types produces entries with byte-identical label sets. Both orgs' history is written into one Loki stream with no label distinguishing them, so backfilled history cannot be sliced or filtered per org the way live data can (`docs/architecture.md`, multi-org section: `org` is the per-entry identity dimension, orthogonal to `source`). Any per-org retention, access, or attribution decision made on labels is wrong for backfilled data.

Note on scope: this is not an out-of-order-rejection bug. Loki accepts unordered per-stream writes by default; the age boundary that does reject backfilled samples is `reject_old_samples_max_age`, which is stream-independent and already warned about at `src/sf2loki/backfill.py:215-222`. The defect here is label identity and cross-org stream merging.

## Proposed approach

In `run_backfill` (`src/sf2loki/backfill.py:707-757`), build the static label set the same way the daemon does, after the `TokenProvider` is constructed (backfill.py:738) and before `_process_event_type` is called:

1. Resolve the org id once: `org_id = cfg.salesforce.org_id or await tokens.org_id()` (mirrors the daemon's startup probe at `src/sf2loki/app.py:1136`; `TokenProvider.org_id` short-circuits on the configured value and otherwise caches a single `userinfo` lookup, `src/sf2loki/auth/jwt_auth.py:102-140`).
2. `static_labels = build_static_labels(environment=cfg.salesforce.environment, org_id=org_id, operator_labels=cfg.sink.loki.labels)`. Import it from `sf2loki.app`, or — preferable, to avoid `backfill` importing the composition root — move `build_static_labels` and `deployment_static_labels` into a small shared module (e.g. `src/sf2loki/sinks/loki/labels.py` or a new `identity.py`) and have `app.py` re-export/import from there. Keep operator labels merged last so `sink.loki.labels` still wins, exactly as today.
3. When `org_name` is non-empty, add `"org": org_name` (single-org resolves to an empty name via `Config.resolved_orgs`, `src/sf2loki/config.py:1420-1435`, so single-org gains no `org` label — consistent with the daemon).
4. Do not add `source`/`event_type`: `_shape_file_rows` sets them after the static merge (backfill.py:296-298), so they cannot be clobbered. `backfill="true"` still applies on top in default mode.
5. `TokenProvider.org_id` can raise `AuthError`. Wrap the resolution: on failure, print/log the error and return exit code 1 rather than propagating an unhandled traceback — pushing history under the wrong (label-less) identity is worse than failing loudly, and the run would fail at the first `list_files` call anyway.

Docs to update in the same change: document the emitted label set in the `sf2loki backfill` section of `docs/reference/cli.md:71-92`, including the migration note that a backfill run completed on an older version wrote to a different stream (no `job`/`sf_org_id`), so its data stays in that old stream after the upgrade and a re-run writes to the new one.

---

Imported from GitHub issue #89 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 89)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `run_backfill` resolves the Salesforce org id once and merges `job`/`service_name`/`environment`/`sf_org_id` (via the same helper the daemon uses) into the static label set before shaping any rows, with `cfg.sink.loki.labels` still merged last and still able to override.
- [ ] #2 `org=<name>` is added when `run_backfill` is called with a non-empty `org_name`, and is absent for single-org configs (empty `org_name`).
- [ ] #3 Failure to resolve the org id aborts the run with a printed error and a non-zero exit code instead of an unhandled `AuthError`.
- [ ] #4 `build_static_labels`/`deployment_static_labels` are importable by `backfill.py` without importing the composition root, or the import is justified in a comment.
- [ ] #5 Test: default (event-time) mode pushes a stream whose label set contains `job="sf2loki"`, `service_name="sf2loki"`, `environment=<configured>`, `sf_org_id=<configured or userinfo-resolved>`, `source="eventlogfile"`, `event_type=<type>`, `backfill="true"`.
- [ ] #6 Test: `--ingest-timestamps` mode carries the same identity labels and still no `backfill` label (extends `tests/test_backfill.py:353-390`).
- [ ] #7 Test: an operator `sink.loki.labels` override (e.g. `{"service_name": "salesforce", "environment": "bf-test"}`) wins over the injected defaults (extends `tests/test_backfill.py:399-424`).
- [ ] #8 Test: two `run_backfill` calls with `org_name="a"` and `org_name="b"` against the same interval/event type produce pushes whose stream label sets differ by `org` (and by `sf_org_id` when the two orgs resolve different ids) — i.e. the two orgs never share one stream.
- [ ] #9 Test: single-org (`org_name=""`) pushes carry no `org` label.
- [ ] #10 `docs/reference/cli.md` backfill section lists the emitted label set and the pre-fix-stream migration note.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
