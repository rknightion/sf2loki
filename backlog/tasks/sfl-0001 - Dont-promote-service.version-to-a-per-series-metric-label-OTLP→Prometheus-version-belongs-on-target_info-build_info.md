---
id: SFL-0001
title: >-
  Don't promote service.version to a per-series metric label (OTLP→Prometheus:
  version belongs on target_info/build_info)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels: []
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/82'
ordinal: 1000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## Summary

`service.version` is set on the **OTLP metrics resource** (the MeterProvider's `Resource`), so it is promoted to a `service_version` **label on every emitted metric series**. Per the OpenTelemetry → Prometheus compatibility spec this is a deviation: version belongs on an **info metric** (`target_info` from the resource, or a `*_build_info` gauge), never as a label on ordinary series.

## The standard

Only `service.name` (+ `service.namespace`) → `job` and `service.instance.id` → `instance` are meant to become labels. **Every other resource attribute — including `service.version` — goes to a `target_info` metric** (OpenMetrics 1.0 convention). Promoting a resource attribute to per-series labels is a documented, non-default opt-in.

Consequence of putting version on every series: **each new build mints a whole new series set.** After a redeploy, the old-version and new-version series coexist for the query-lookback window, so any `sum`-style panel adds both → a transient multiplier; and active-series cardinality grows with the number of versions ever seen. This was found loudly on `graph2otel` (which runs a per-commit `:main` image) — see rknightion/graph2otel#104 for the full spec analysis. Repos on stable release tags hit the cardinality growth but rarely see the doubling.

## Change

- Keep `service.version` on the OTel **resource** (semconv-correct → flows to `target_info`).
- Stop it becoming a per-series label: rely on the existing `*_build_info{version=...}` gauge (and/or `target_info`) for version, joined via `group_left` where a panel needs it.
- Audit dashboards/alerts for any query that aggregates over or filters on `service_version` and repoint them at the info metric.

---

Imported from GitHub issue #82 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 82)' archive/issues-dump.json`).

Cross-repo consistency pass with rknightion/graph2otel#104 (detailed spec citations there). Sibling issues: tailscale2otel#187, opnsense-exporter#270.
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `service_version` no longer appears as a label on ordinary metric series (only on `target_info` / `*_build_info`)
- [ ] #2 Version stays queryable via the build-info gauge (+ a documented `group_left` example)
- [ ] #3 Dashboards/alerts updated if any referenced `service_version`
- [ ] #4 Note the OTLP→Prometheus resource-attribute convention in the telemetry setup docs
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
