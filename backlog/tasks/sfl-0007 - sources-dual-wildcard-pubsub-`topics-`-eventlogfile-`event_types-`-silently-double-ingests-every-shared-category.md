---
id: SFL-0007
title: >-
  sources: dual wildcard (pubsub `topics: ["*"]` + eventlogfile `event_types:
  ["*"]`) silently double-ingests every shared category
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/91'
ordinal: 7000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The either/or-per-category model is enforced by three cooperating checks: the startup guard `check_overlap` (`src/sf2loki/sources/overlap.py:81-125`), the Pub/Sub runtime filter for wildcard-discovered topics (`src/sf2loki/sources/pubsub_source.py:197-218`, added by #15), and the ELF runtime filter for wildcard-discovered EventTypes (`src/sf2loki/sources/eventlogfile_source.py:437`). All three are fed from **explicitly configured** identifiers only, so a config that wildcards *both* sources defeats all three at once.

Concrete config (single org, `sources.allow_overlap` left at its `false` default):

```yaml
sources:
  pubsub:
    enabled: true
    topics: ["*"]
  eventlogfile:
    enabled: true
    event_types: ["*"]
```

Walk of the current code:

- `src/sf2loki/app.py:739-743` builds `elf_event_types` with `if t.name != EVENT_TYPE_WILDCARD`, so the ELF list is `[]`. `stored_objects` (`src/sf2loki/app.py:734-737`) is `[]` because `eventlog_objects` is disabled.
- `pubsub_owned` (`src/sf2loki/app.py:752-755`) = categories of `stored_objects` + `elf_event_types` = `frozenset()`, passed as `owned_categories=` at `src/sf2loki/app.py:765`. `PubSubSource._filter_owned` short-circuits on an empty set (`src/sf2loki/sources/pubsub_source.py:201-202`), so every discovered topic is kept — at startup and on every `rediscovery_interval` pass, which reuses the same `_discovered_additions` path (`src/sf2loki/sources/pubsub_source.py:220-227`, `:298`).
- `elf_owned` (`src/sf2loki/app.py:792-795`) = `category_of_pubsub(t)` over `pubsub_topics`, which is `pubsub_src.resolve_topics()` (`src/sf2loki/app.py:769`). `resolve_topics` runs `_filter`, and `_filter` skips the `"*"` marker (`src/sf2loki/sources/pubsub_source.py:179`), so `pubsub_topics == []` and `exclude_categories == frozenset()` (`src/sf2loki/app.py:804`). The ELF per-cycle exclusion at `src/sf2loki/sources/eventlogfile_source.py:437` therefore never matches.
- `check_overlap` is called with three empty sequences (`src/sf2loki/app.py:827-832`); `buckets` is empty, no collisions, startup succeeds (`src/sf2loki/sources/overlap.py:95-110`).

Nothing else catches it. `SourcesConfig` (`src/sf2loki/config.py:790-836`) has no `model_validator`. `doctor.py:260-267` emits a single WARN row for `topics == ["*"]` about per-topic reachability not being probed, nothing about overlap. `EventLogFileConfig.discover` (`src/sf2loki/config.py:746-748`) is derived purely from the presence of `"*"` and is never cross-checked against `sources.pubsub.topics`.

At runtime, `MetadataClient.list_event_stream_topics` (`src/sf2loki/salesforce/metadata_client.py:29-50`) returns `/event/<Name>` for every sObject ending in `EventStream`, and ELF discovery returns every EventType the org produces for the interval (`src/sf2loki/sources/eventlogfile_source.py:411-441`). The category normalisers collapse both onto the same key (`src/sf2loki/sources/overlap.py:57-77`): `/event/LoginEventStream` → `login`, ELF `Login` → `login`. Same for `logout`, `api` (`ApiEventStream` / ELF `API`), `report`, `uri`. Every one of those categories is ingested twice.

Docs currently assert the opposite. `docs/sources/index.md:48-56` states "both wildcard sources filter themselves against the categories owned by other configured sources" and enumerates only two remaining gaps (explicit-vs-explicit, and explicitly-listed Pub/Sub topics never being runtime-filtered). This third gap — wildcard-vs-wildcard, where neither source has any explicit categories for the other to own — is neither guarded nor documented.

This is a residual gap left by #15, not a regression: #15's resolution steps specified deriving the owned set from "stored objects + **explicit** ELF types", which is exactly what shipped at `src/sf2loki/app.py:752-755`.

## Why it matters

An operator setting up "give me everything" on an RTEM-entitled org reaches for both wildcards, which is the obvious reading of the two config descriptions (`src/sf2loki/config.py` `topics` / `event_types`, rendered at `docs/config-reference.md:68` and `:113`). Startup passes clean, `--check` passes clean, no INFO skip lines appear (the filters log only when they skip, `src/sf2loki/sources/pubsub_source.py:203-209`), so there is no signal at all that anything is wrong.

Consequences:

- Login/API/Report/Logout/URI activity lands in Loki twice, via two different line shapes and two different timestamps, so Loki's byte-identical dedup cannot collapse it. The `sinks.loki` labels differ by `source` too, so the streams are genuinely distinct.
- Every count, rate and alert threshold over those categories in `deploy/grafana/dashboards/*.json` and `deploy/grafana/rules/` is inflated, silently and non-uniformly (the streaming and ELF feeds have different latency, so the inflation factor drifts with time).
- Duplicate ingest is paid for twice in Loki bytes, and the ELF half also burns Salesforce API calls and download bandwidth for data already streaming.
- `sources.allow_overlap: false` is the default precisely to make this state impossible. A config that lands in it anyway violates the documented contract in `SourcesConfig`'s docstring (`src/sf2loki/config.py:790-802`).

## Proposed approach

Fail fast, matching the existing guard's philosophy, and keep `allow_overlap` as the single escape hatch.

Primary fix — a startup check for the wildcard-vs-wildcard pair:

1. Add a helper to `src/sf2loki/sources/overlap.py`, e.g. `check_wildcard_overlap(*, pubsub_wildcard: bool, elf_discover: bool, allow_overlap: bool) -> None`, raising `OverlapError` when both wildcards are on and `allow_overlap` is false. Keep it in `overlap.py` so the whole either/or policy stays in one module. The message must name the actual fix options: list Pub/Sub topics explicitly, or list ELF `event_types` explicitly, or use `sources.eventlogfile.exclude` to carve out the RTEM-served categories, or set `sources.allow_overlap: true` to accept the duplication.
2. Call it from `_build_org_sources` in `src/sf2loki/app.py`, adjacent to the existing `check_overlap(...)` call at `src/sf2loki/app.py:827-832`, using `TOPIC_WILDCARD in org.sources.pubsub.topics and org.sources.pubsub.enabled` and `org.sources.eventlogfile.enabled and org.sources.eventlogfile.discover` (`src/sf2loki/config.py:746-748`). Per-org, so a multi-org deployment reports the offending org.
3. Do not raise when the ELF side has a non-empty `sources.eventlogfile.exclude` that covers the colliding categories — an operator who wildcards ELF and excludes the RTEM categories by name has resolved the overlap deliberately. Implement this by computing, from `sources.eventlogfile.exclude`, the set of excluded categories via `category_of_elf`, and only raising when the exclusion does not plausibly cover the collision. If that turns out to be unknowable at startup (the discovered RTEM topic set is not known until runtime), downgrade to: raise unless `exclude` is non-empty, and state in the message that a non-empty `exclude` is taken as the operator asserting the carve-out.
4. Surface the same condition as a `doctor` WARN row so `sf2loki --check` reports it before the daemon refuses to start, alongside the existing wildcard row at `src/sf2loki/doctor.py:260-267`.

Follow-up enhancement (optional, separate change): give Pub/Sub deterministic precedence at runtime instead of failing. Replace `EventLogFileSource`'s static `exclude_categories: frozenset[str]` (`src/sf2loki/sources/eventlogfile_source.py:254`, `:271`) with a `Callable[[], frozenset[str]]` supplied by the composition root, backed by a set the Pub/Sub source updates after each discovery pass. ELF discovery re-runs every cycle (`src/sf2loki/sources/eventlogfile_source.py:397-441`), so it would converge. Note the ordering hazard: ELF's first cycle can precede Pub/Sub's first discovery, so the first cycle would still double-ingest unless startup resolves discovery once before the pipeline starts. That hazard is why the fail-fast option above is the primary recommendation.

Docs, in either case: rewrite `docs/sources/index.md:48-56` so the gap list is accurate. The current sentence "both wildcard sources filter themselves against the categories owned by other configured sources" must state that the filter only sees *explicitly configured* identifiers on the other side, and describe what happens when both sources wildcard.

---

Imported from GitHub issue #91 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 91)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sources.pubsub.topics: ["*"]` + `sources.eventlogfile.event_types: ["*"]` with `allow_overlap: false` fails at startup with an `OverlapError` naming both sources and listing the concrete remedies.
- [ ] #2 The same config with `sources.allow_overlap: true` starts normally and ingests both channels (existing bypass semantics unchanged).
- [ ] #3 Single-wildcard configs are unaffected: pubsub `["*"]` + explicit ELF types still starts and still runtime-filters discovered topics; explicit pubsub topics + ELF `["*"]` still starts and still runtime-filters discovered EventTypes.
- [ ] #4 Multi-org: the error names the offending org, and one org's dual wildcard does not block a sibling org's valid config from being reported.
- [ ] #5 `sf2loki --check` / `doctor` reports the condition as a WARN (or FAIL) row rather than only surfacing it when the daemon starts.
- [ ] #6 Test: `tests/sources/test_overlap.py` — the new wildcard check raises with both wildcards, returns cleanly with either one alone, and returns cleanly under `allow_overlap=True`.
- [ ] #7 Test: `tests/test_app_integration.py` — `App.build` (or `_build_org_sources`) raises on the dual-wildcard config and does not raise on each single-wildcard config; add the multi-org variant asserting the org name appears in the message.
- [ ] #8 Test: `tests/test_doctor.py` — the dual-wildcard config produces the new doctor row.
- [ ] #9 Regression guard: a test asserting `pubsub_owned` / `elf_owned` are both empty for the dual-wildcard config, so the reason the runtime filters cannot help is pinned in code rather than only in this issue.
- [ ] #10 `docs/sources/index.md` overlap section states that runtime filters see only explicitly configured identifiers on the other side, and documents the dual-wildcard behaviour (fail fast, plus the `allow_overlap` bypass).
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
