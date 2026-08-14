---
id: SFL-0023
title: >-
  grafana: security dashboard country panels extract COUNTRY_CODE, a field the
  Login EventLogFile row does not have
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-1
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/107'
ordinal: 23000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Two panels in `deploy/grafana/dashboards/sf2loki-security-access.json` extract a `COUNTRY_CODE` field from EventLogFile `Login` streams. That column does not exist on the Salesforce Login event type, so both panels are permanently empty on every deployment.

- `deploy/grafana/dashboards/sf2loki-security-access.json:375` — "Distinct countries" stat (title at :358, layout element `sec-countries` at :1060, one of the four headline stat tiles in the top row):
  ```
  count(sum by (COUNTRY_CODE)(count_over_time({job="$job", event_type="Login", source="eventlogfile"} | json COUNTRY_CODE="COUNTRY_CODE" | COUNTRY_CODE!="" [$__range])))
  ```
- `deploy/grafana/dashboards/sf2loki-security-access.json:610` — "Logins by country" table (title at :593, layout element `sec-country` at :1099):
  ```
  sum by (COUNTRY_CODE)(count_over_time({job="$job", event_type="Login", source="eventlogfile"} | json COUNTRY_CODE="COUNTRY_CODE" | COUNTRY_CODE!="" [$__range]))
  ```

The log line for an EventLogFile row is the raw CSV row and nothing else. `src/sf2loki/sources/eventlogfile_source.py:777` calls `route_fields(row, sm_fields)` on the parsed CSV row dict, and `src/sf2loki/shaping.py:54` serialises exactly that dict (`json.dumps(payload, sort_keys=True, default=str)`). Labels are assigned at `src/sf2loki/sources/eventlogfile_source.py:779-783` (`source="eventlogfile"`, `event_type=<ELF EventType>`), so the stream selector does match, but the line body can only contain real Login ELF columns. The single pre-shaping mutation is `self._transforms.apply(row)`, whose action set at `src/sf2loki/transforms.py:209-219` is `hash` / `mask` / `drop_field` / `regex_replace` / `drop_row` — every action is subtractive or in-place, none adds a field. There is no geo-enrichment step anywhere in `src/sf2loki/`.

The Salesforce Login event type carries 28 columns (EVENT_TYPE, TIMESTAMP, TIMESTAMP_DERIVED, REQUEST_ID, ORGANIZATION_ID, USER_ID, USER_ID_DERIVED, USER_NAME, USER_TYPE, RUN_TIME, CPU_TIME, DB_TOTAL_TIME, URI, URI_ID_DERIVED, REQUEST_STATUS, SESSION_KEY, LOGIN_KEY, LOGIN_TYPE, LOGIN_SUB_TYPE, LOGIN_STATUS, BROWSER_TYPE, API_TYPE, API_VERSION, TLS_PROTOCOL, CIPHER_SUITE, AUTHENTICATION_METHOD_REFERENCE, CLIENT_IP, SOURCE_IP). None is a country or geolocation field. Geography exists only on the real-time side: the `LoginEvent` platform event and `/event/LoginEventStream` carry `City` / `Country` / `CountryIso`. Deriving country from an ELF Login row requires IP-to-geo enrichment of `CLIENT_IP` / `SOURCE_IP`, not a field extraction.

Both panels have an empty `description`, and the row header at `deploy/grafana/dashboards/sf2loki-security-access.json:115` advertises "unfamiliar source IPs / countries", so the intent was real geo data rather than a deliberate placeholder. Nothing catches this: the dashboards are hand-authored with no generator and no drift gate, and no test validates dashboard LogQL field names against the shapes the connector actually emits.

Aggravating factor: the working geo panel is not a universal fallback. `sec-geo` at `deploy/grafana/dashboards/sf2loki-security-access.json:888` queries `{event_type="LoginEventStream", source="pubsub"} | json Country="Country", City="City"`, but the either/or-per-category guard in `src/sf2loki/sources/overlap.py` (documented at `docs/troubleshooting.md:91`) forbids ingesting login activity from both EventLogFile and `/event/LoginEventStream`. A deployment that ingests `Login` from EventLogFile therefore has three empty geo panels and no working one.

## Why it matters

A security dashboard renders a zeroed "Distinct countries" KPI in its headline stat row and a blank "Logins by country" table on every deployment, regardless of data volume. An operator investigating anomalous geographic login activity reads a zero as "no foreign logins" rather than "this panel cannot work", which is a false negative on the exact signal the panel exists to surface. The failure is silent and volume-independent, so it never self-corrects and never looks like a misconfiguration.

## Proposed approach

1. Retarget the "Distinct countries" stat at `deploy/grafana/dashboards/sf2loki-security-access.json:375` at the streaming source, which is the only source that carries geography:
   ```
   count(sum by (Country)(count_over_time({job="$job", event_type="LoginEventStream", source="pubsub"} | json Country="Country" | Country!="" [$__range])))
   ```
2. Repoint the "Logins by country" table at `:610` at the same stream (`sum by (Country)(count_over_time({job="$job", event_type="LoginEventStream", source="pubsub"} | json Country="Country" | Country!="" [$__range]))`), or replace it with a breakdown over a column the ELF Login row actually has — `LOGIN_TYPE` and `BROWSER_TYPE` are the closest useful substitutes and neither is currently plotted anywhere in the pack.
3. Set a non-empty `description` on every geo panel (`sec-countries`, `sec-country`, `sec-geo`) stating that login geography is only available from the real-time stream, that the ELF `Login` row has no country column, and that per `src/sf2loki/sources/overlap.py` an EventLogFile-sourced login deployment will see these panels empty by design. Reword the row header at `:115` so it no longer promises countries from EventLogFile.
4. Document the constraint in `docs/sources/eventlogfile.md` (the ELF Login row has no geo column; use `/event/LoginEventStream` if geography is required) and note in `deploy/grafana/README.md` which panels depend on the streaming source.
5. Add a dashboard field-validity test. Parse every dashboard JSON, pull each LogQL expression, and for expressions whose stream selector pins `source="eventlogfile"` with a literal `event_type="<T>"`, assert every field named in a `| json FIELD="FIELD"` extraction appears in a committed per-event-type column allowlist (a new `tests/data/elf_known_fields.json`, seeded with the 28-column Login set above plus the other ELF types the pack queries: `ApexCallout`, `ApiTotalUsage`, `Report`, `RestApi`, `URI`). This is the missing gate — it turns a silently-empty panel into a failing test.

---

Imported from GitHub issue #107 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 107)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 Neither `deploy/grafana/dashboards/sf2loki-security-access.json:375` nor `:610` references `COUNTRY_CODE`; each either targets `{source="pubsub", event_type="LoginEventStream"} | json Country="Country"` or a column present in the ELF Login allowlist.
- [ ] #2 `rg -n COUNTRY_CODE deploy/grafana/` returns no match.
- [ ] #3 Every geo panel (`sec-countries`, `sec-country`, `sec-geo`) has a non-empty `description` naming the source it requires and stating that EventLogFile-sourced login deployments see it empty by design.
- [ ] #4 The row-header markdown at `deploy/grafana/dashboards/sf2loki-security-access.json:115` no longer attributes country data to EventLogFile.
- [ ] #5 `docs/sources/eventlogfile.md` states that the ELF `Login` row carries no country/geolocation column and points at `/event/LoginEventStream` for geography; `deploy/grafana/README.md` lists the streaming-source-dependent panels.
- [ ] #6 New test `tests/test_grafana_dashboard_fields.py` loads every file in `deploy/grafana/dashboards/`, extracts each `| json FIELD="FIELD"` name from expressions whose selector pins `source="eventlogfile"` and a literal `event_type`, and asserts membership in `tests/data/elf_known_fields.json`.
- [ ] #7 That test is proven to fail against the pre-fix dashboard (reintroducing `COUNTRY_CODE` on an `event_type="Login", source="eventlogfile"` selector makes it red) and passes after.
- [ ] #8 A test case pins that an unknown ELF event type or a non-literal `event_type=~"..."` selector is skipped rather than silently passing, so the allowlist cannot be bypassed by loosening a selector.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
