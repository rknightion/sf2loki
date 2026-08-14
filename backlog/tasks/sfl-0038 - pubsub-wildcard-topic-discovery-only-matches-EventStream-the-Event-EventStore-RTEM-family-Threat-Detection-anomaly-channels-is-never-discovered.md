---
id: SFL-0038
title: >-
  pubsub: wildcard topic discovery only matches *EventStream - the
  Event/EventStore RTEM family (Threat Detection anomaly channels) is never
  discovered
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/122'
ordinal: 38000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`MetadataClient.list_event_stream_topics` (`src/sf2loki/salesforce/metadata_client.py:29-50`) resolves the `topics: ["*"]` wildcard by filtering the describeGlobal response with a single predicate:

```python
names = [
    str(s["name"])
    for s in response.json().get("sobjects", [])
    if str(s.get("name", "")).endswith("EventStream")   # metadata_client.py:48
]
return sorted(f"/event/{name}" for name in names)
```

That matches only the older Real-Time Event Monitoring generation, where the streaming channel is `<Name>EventStream` (`LoginEventStream`, `ApiEventStream`, `ReportEventStream`, …) and the big-object store is `<Name>Event` (`LoginEvent`, `ApiEvent`).

The newer RTEM generation inverts the naming: the streaming channel is `<Name>Event` and the big-object store is `<Name>EventStore`. This covers the Threat Detection anomaly channels (`ApiAnomalyEvent`/`ApiAnomalyEventStore`, `CredentialStuffingEvent`/`CredentialStuffingEventStore`, `ReportAnomalyEvent`/`ReportAnomalyEventStore`, `SessionHijackingEvent`/`SessionHijackingEventStore`) and the `FileEvent`/`FileEventStore`-shaped events. None of these ends in `EventStream`, so the wildcard can never discover them.

The repo already records this two-generation naming split everywhere except the discovery predicate:

- `src/sf2loki/salesforce/CLAUDE.md` — "The stored RTEM event family (`LoginEvent`, `ApiEvent`, `FileEventStore`, Threat-Detection `*EventStore`, ...) are BigObjects".
- `examples/presets/event-log-objects.yaml:3` — same `*EventStore` framing.
- `src/sf2loki/config.py:355` — `/event/ApiAnomalyEvent` is the shipped explicit-topic example.
- `README.md:186-187` — Threat Detection provides "anomaly channels such as `ApiAnomalyEvent`".
- `tests/sources/test_overlap.py:21,26` — `category_of_pubsub("/event/ApiAnomalyEvent") == category_of_stored_object("ApiAnomalyEventStore") == "apianomaly"`.

`tests/salesforce/test_metadata_client.py:42-59` pins the current behaviour and explicitly drops `LoginEvent` with the comment "stored object, not a stream" — correct for the old generation, but it means the `<Name>Event` streaming shape has no code path at all.

The config description overstates what discovery does. `src/sf2loki/config.py:350-357`:

```
'Explicit topics, or "*" to DISCOVER and subscribe to every RTEM stream the '
"org exposes (the *EventStream channels), re-filtered by include/exclude."
```

"every RTEM stream the org exposes" is the headline; the `*EventStream` scope is a parenthetical. The same text is generated into `docs/config-reference.md:68`, `config.example.yaml:207` and `deploy/helm/values.yaml:467`. `docs/sources/pubsub.md:36` is the only place that states the `*EventStream` limit plainly.

Nothing surfaces the shortfall at runtime:

- `src/sf2loki/sources/pubsub_source.py:245-275` (`_discover_with_retry`) only logs when discovery *fails*; a discovery pass that returns a short list is indistinguishable from a complete one.
- `src/sf2loki/doctor.py:252-267` returns a single WARN row for `topics: ["*"]` ("wildcard — topics resolved at runtime, per-topic reachability not probed") and never probes discovered topics.

## Why it matters

An org with the Threat Detection add-on that configures the documented security-posture wildcard:

```yaml
sources:
  pubsub:
    enabled: true
    topics: ["*"]
```

gets every `*EventStream` channel and silently gets none of the anomaly channels. Session hijacking, credential stuffing, report anomaly and API anomaly detections — the highest-signal security events in the RTEM catalogue — are never subscribed. Discovery reports success, `sf2loki doctor` reports a benign wildcard WARN, and the security dashboards in `deploy/grafana/dashboards/` simply have no anomaly data. The operator has no signal distinguishing "no anomalies detected" from "never subscribed".

The data is reachable today by listing `/event/ApiAnomalyEvent` explicitly (`src/sf2loki/config.py:355` shows exactly that), which is why this is a wildcard-completeness gap rather than a missing capability — but the wildcard is the path an operator picks precisely to avoid having to know the channel inventory.

## Proposed approach

Extend `list_event_stream_topics` with a second discriminator derived from the same describeGlobal response, so no extra API call is needed:

1. Collect `names = {sobject["name"]}` once from the response.
2. Keep the existing rule: every name ending `EventStream` → `/event/<name>`.
3. Add: for every name ending `EventStore`, strip the trailing `Store`; if the stripped name is also present in `names`, emit `/event/<stripped>`.
4. Union, de-duplicate, return sorted.

Why the twin guard is the right discriminator:

- It derives the Event/EventStore pairs exactly. `ApiAnomalyEventStore` → `ApiAnomalyEvent`, present as an sObject → `/event/ApiAnomalyEvent`.
- It cannot resurrect old-generation stores. There is no `LoginEventStore`, so `LoginEvent` is never emitted and the `test_metadata_client.py:51` expectation still holds.
- It cannot sweep in CDC (`*ChangeEvent`), custom platform events (`*__e`), or custom big objects (`*__b`) — none of those has an `<X>EventStore` twin.
- A store big object with no streaming twin in the org (for example `IdentityProviderEventStore`-shaped objects) yields nothing, because the stripped name is absent from `names`.

Optional hardening, only if verified live first: describeGlobal entries carry per-object flags, and a platform event is not SOQL-`queryable` while a big object is. Requiring `queryable is False` on the derived candidate would tighten the rule further. Do not implement this on assumption — confirm the flag values against a real describeGlobal response (the DEV org credentials in the gitignored `.env.dev` are the fastest check) before relying on it. The twin-existence guard alone is sufficient and is the required behaviour; the flag check is additive.

Downstream integration needs no change, but state it in the PR description so a reviewer can confirm:

- `src/sf2loki/sources/overlap.py:38` — `_CHANNEL_SUFFIXES = ("EventStream", "EventStore", "Event")`, ordered longest-first, so `/event/ApiAnomalyEvent` normalises to `apianomaly`, the same category as `ApiAnomalyEventStore`. Newly discovered anomaly topics therefore route through the existing either/or guard unchanged.
- `src/sf2loki/sources/pubsub_source.py:197-229` — `_filter_owned` / `_discovered_additions` already apply include/exclude globs and drop discovered topics whose category another enabled source owns, logging each skip at INFO.

Entitlement risk to handle explicitly: an org without the Threat Detection add-on may still expose the `*EventStore` and `*Event` sObjects in describeGlobal, in which case the derived topic is discovered but unsubscribable. `src/sf2loki/sources/pubsub_source.py:641-644` handles a non-INVALID_ARGUMENT subscribe failure by logging a WARN and reconnecting with exponential backoff up to the configured maximum, so the failure mode is bounded log noise, not a crash or a hot loop. This risk already exists for `*EventStream` channels on non-Shield orgs, so it is not a new class of failure. Mitigate the observability side rather than the mechanism:

- Log the resolved discovered topic set at INFO on each discovery pass, separating `*EventStream`-matched from `EventStore`-twin-derived topics, so an operator can see the inventory that was actually subscribed.
- Add a doctor note under the existing wildcard WARN row (`src/sf2loki/doctor.py:252-267`) listing `EventStore` big objects whose derived stream would be excluded by the operator's `include`/`exclude` globs, so a deliberate exclusion is visible and an accidental one is caught before deployment.

Documentation and generated artifacts to update:

- `src/sf2loki/config.py:350-357` — restate the `topics` description to describe both discovered shapes (`<Name>EventStream` channels and `<Name>Event` channels derived from `<Name>EventStore` twins) instead of "the *EventStream channels".
- Run `just gen-config` afterwards. `config.example.yaml`, `docs/config-reference.md` and `deploy/helm/values.yaml` are generated from the Pydantic model and `tests/test_config_artifacts_drift.py` fails on drift.
- `docs/sources/pubsub.md:36` and the surrounding "How it works" prose — document both discovered shapes and name the Threat Detection channels as now covered by `"*"`.
- `src/sf2loki/salesforce/metadata_client.py:1-6,29-35` — module and method docstrings still describe `*EventStream`-only discovery.

---

Imported from GitHub issue #122 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 122)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `MetadataClient.list_event_stream_topics` returns `/event/<Name>` for every describeGlobal sObject named `<Name>EventStore` whose `<Name>` also appears as an sObject, unioned with the existing `*EventStream` results, de-duplicated and sorted.
- [ ] #2 `tests/salesforce/test_metadata_client.py` gains a case whose describeGlobal fixture contains `LoginEventStream`, `LoginEvent`, `ApiAnomalyEvent`, `ApiAnomalyEventStore`, `SessionHijackingEvent`, `SessionHijackingEventStore`, `Account`, `MyCustom__e`, `AccountChangeEvent`, and a store with no twin (for example `IdentityProviderEventStore` with no `IdentityProviderEvent`), asserting the result is exactly `["/event/ApiAnomalyEvent", "/event/LoginEventStream", "/event/SessionHijackingEvent"]` — proving `LoginEvent`, the twinless store, CDC and custom platform events are all excluded.
- [ ] #3 The existing assertion at `tests/salesforce/test_metadata_client.py:59` still passes unmodified (old-generation orgs get an unchanged topic set).
- [ ] #4 A test asserts no duplicate topics when a name would qualify under both rules.
- [ ] #5 A `pubsub_source` test asserts a wildcard-discovered `/event/ApiAnomalyEvent` is dropped by `_filter_owned` when `eventlog_objects` is configured for `ApiAnomalyEventStore` (category `apianomaly` already owned), and is kept when it is not.
- [ ] #6 Each discovery pass logs the resolved topic inventory at INFO, distinguishing `*EventStream` matches from `EventStore`-twin derivations; a test asserts both groups appear.
- [ ] #7 The doctor wildcard row reports `EventStore` big objects whose derived stream is filtered out by `include`/`exclude`; a `tests/test_doctor.py` case pins the message.
- [ ] #8 `src/sf2loki/config.py` `topics` description describes both discovered shapes, `just gen-config` has been run, and `tests/test_config_artifacts_drift.py` is green.
- [ ] #9 `docs/sources/pubsub.md` and the `metadata_client.py` docstrings describe both shapes and name the Threat Detection channels as wildcard-covered.
- [ ] #10 `just gate` green (ruff, ruff format, mypy --strict, pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
