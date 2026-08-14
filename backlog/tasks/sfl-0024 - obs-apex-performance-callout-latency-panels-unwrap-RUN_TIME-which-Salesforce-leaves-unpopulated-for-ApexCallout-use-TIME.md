---
id: SFL-0024
title: >-
  obs: apex-performance callout latency panels unwrap RUN_TIME, which Salesforce
  leaves unpopulated for ApexCallout - use TIME
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/108'
ordinal: 24000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Four panels on `deploy/grafana/dashboards/sf2loki-apex-performance.json` compute ApexCallout latency by unwrapping the `RUN_TIME` field, which Salesforce does not populate for that event type:

| Panel key / id | Title | Query line |
| --- | --- | --- |
| `apx-callout-p95` / 404 | Callout run time p95 | `deploy/grafana/dashboards/sf2loki-apex-performance.json:375` |
| `apx-callout-lat` / 406 | Apex callout run time percentiles (p50) | `deploy/grafana/dashboards/sf2loki-apex-performance.json:533` |
| `apx-callout-lat` / 406 | Apex callout run time percentiles (p95) | `deploy/grafana/dashboards/sf2loki-apex-performance.json:554` |
| `apx-callout-lat` / 406 | Apex callout run time percentiles (p99) | `deploy/grafana/dashboards/sf2loki-apex-performance.json:575` |

All four use the same shape (unit `ms`):

```
quantile_over_time(0.95, {job="$job", event_type="ApexCallout", source="eventlogfile"}
  | json rt="RUN_TIME" | rt!="" | unwrap rt [$__range]) by (event_type)
```

Salesforce Object Reference, *Apex Callout Event Type* field table (verified against the API 67.0 / doc version 262.0 PDF, `https://resources.docs.salesforce.com/262/latest/en-us/sfdc/pdf/object_reference.pdf`):

- `RUN_TIME` — "Not used for this event type. Use the TIME field instead."
- `TIME` — "The amount of time that the request took in milliseconds (ms)."
- `CPU_TIME` — "The CPU time in milliseconds used to complete the request." (valid, unaffected)

The ELF ingest path emits the whole CSV row, so the empty column reaches Loki as an empty string rather than being dropped: `src/sf2loki/salesforce/eventlogfile_client.py:256` reads the blob with `csv.DictReader(..., restval="")` and yields every header column, and `src/sf2loki/shaping.py:54` serialises the full row with `json.dumps`. The log line therefore contains `"RUN_TIME": ""` for ApexCallout, so `| json rt="RUN_TIME" | rt!=""` filters out every line. Where an org's blob writes `0` instead of an empty cell, the percentiles resolve to a constant 0. If Salesforce omits the column entirely, `rt` is unset and the `rt!=""` filter still drops the line. All three cases produce a panel with no usable data.

The same RUN_TIME extraction on the neighbouring panels is correct and must not be changed: `RUN_TIME` is documented as a real request duration for URI (`:303`, `:655`), VisualforceRequest, AuraRequest and REST API (`:655`), and for ApexExecution. Only the ApexCallout event type redirects to `TIME`.

There is no test over `deploy/grafana/dashboards/*.json`, so nothing catches a query that references a field the event type does not populate. The only artifact gate in the suite is `tests/test_config_artifacts_drift.py`, which covers generated config docs, not dashboards.

## Why it matters

`sf2loki-apex-performance.json` is the dashboard advertised for callout latency (`deploy/grafana/README.md:13`, `docs/observability/dashboards.md:15`). On every deployment the callout p50/p95/p99 timeseries and the p95 stat render "No data" (or a flat zero line), while the underlying `TIME` value that answers the question is present in the log line. An operator investigating slow external callouts concludes either that no callout events are being ingested or that callout latency is zero, both wrong — the callout count, outcome and status-code panels on the same dashboard populate normally, which makes the empty latency panels look like a source-side gap rather than a query bug.

## Proposed approach

1. Change the four ApexCallout queries to unwrap `TIME`, keeping the `ms` unit, the `by (event_type)` aggregation, the `queryType`/`$__range` vs `$__auto` split and the `p50`/`p95`/`p99` `legendFormat` values already at `:534`, `:555`, `:576`:

   ```
   quantile_over_time(0.95, {job="$job", event_type="ApexCallout", source="eventlogfile"}
     | json t="TIME" | t!="" | unwrap t [$__range]) by (event_type)
   ```

2. Leave `:303` and `:655` (URI / VisualforceRequest / AuraRequest / RestApi) on `RUN_TIME`.

3. Add a guard test asserting that no Loki expression in `deploy/grafana/dashboards/*.json` unwraps `RUN_TIME` while selecting `event_type="ApexCallout"` (extend the deny-list to `ApexTrigger`, whose `RUN_TIME` the same reference documents as "always null", so a future panel cannot reintroduce the class of bug), and that the four callout latency queries unwrap `TIME`. The test walks the dashboard-schema-v2 JSON (`spec.elements[*].spec.data.spec.queries[*].spec.query.spec.expr`) and matches on the expression strings, so it needs no Grafana or Loki instance.

4. Validate and push with `gcx` per `deploy/grafana/README.md` (`gcx resources validate -p ...` then `gcx resources push -p ...`); dashboards are hand-authored, so there is no generator to re-run.

Related, not fixed here: the panel at `deploy/grafana/dashboards/sf2loki-apex-performance.json:738` averages `DB_TOTAL_TIME` across `URI|Report|RestApi|ApexExecution` under the description "DB_TOTAL_TIME is reported in nanoseconds" (`:722`), but the same reference documents `DB_TOTAL_TIME` in nanoseconds for URI/Report/REST API and in milliseconds for ApexExecution — a mixed-unit series worth a separate issue.

---

Imported from GitHub issue #108 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 108)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `deploy/grafana/dashboards/sf2loki-apex-performance.json:375` (`apx-callout-p95`, id 404) unwraps `TIME`, not `RUN_TIME`
- [ ] #2 `deploy/grafana/dashboards/sf2loki-apex-performance.json:533`, `:554`, `:575` (`apx-callout-lat`, id 406) unwrap `TIME`, keeping `p50`/`p95`/`p99` legends and the `ms` unit
- [ ] #3 `:303` and `:655` still unwrap `RUN_TIME` for URI / VisualforceRequest / AuraRequest / RestApi (unchanged)
- [ ] #4 Panel titles/descriptions state that callout duration comes from the `TIME` field in ms
- [ ] #5 New test (e.g. `tests/test_dashboard_queries.py`) parses every `deploy/grafana/dashboards/*.json`, extracts all Loki `expr` strings, and fails if any expression selecting `event_type="ApexCallout"` or `event_type="ApexTrigger"` references `RUN_TIME`
- [ ] #6 Same test asserts the four callout latency expressions unwrap `TIME`
- [ ] #7 Test fails against the current dashboard JSON (watch it fail for the right reason) and passes after the fix
- [ ] #8 `just gate` green
- [ ] #9 `gcx resources validate -p deploy/grafana/dashboards/sf2loki-apex-performance.json` passes
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
