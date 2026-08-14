---
id: SFL-0051
title: >-
  deploy: docker-compose/Helm memory sizing predates the per-lane queue split -
  stated ceiling is 256 MiB below the documented worst case
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/135'
ordinal: 51000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`docker-compose.yml:46-52` sizes `mem_limit: 512m` from an explicit derivation:

> Bounds worst-case memory: the Loki sink queue (`sink.loki.batch.queue_max_bytes`, default 256 MiB) plus the Pub/Sub bridge queue (`sources.pubsub.bridge_max_bytes`, default 128 MiB) can both fill during a sink outage, plus Python/uv runtime overhead. 512m gives headroom above the ~384 MiB queue ceiling [...] raise it if you raise either queue's byte budget.

That math is single-queue math and went stale when the per-lane split landed. `queue_max_bytes` is applied **per lane**, not once globally:

- `src/sf2loki/config.py:870-878` - the field's own description: "applied PER LANE (streaming vs bulk) [...] Worst-case buffered memory during a sink outage is therefore `queue_max_bytes x number-of-lanes` (<= 2x)".
- `src/sf2loki/app.py:359-367` - `_charge` compares `lane.queued_bytes` against the full `self._batch.queue_max_bytes`; each lane gets its own counter and condition (`src/sf2loki/app.py:212-225`).
- `src/sf2loki/app.py:87-92` - lane class is `streaming` for `pubsub`, `bulk` for every other source.

The config that compose actually mounts activates both lane classes: `config.docker.yaml:27-28` (`pubsub.enabled: true`) and `config.docker.yaml:42-43` (`eventlogfile.enabled: true`), and overrides neither byte budget (`config.docker.yaml:72` sets only `max_entries`/`max_bytes`/`flush_interval`/`max_line_bytes`). Accounted ceiling for that deployment is therefore `2 x 256 MiB + 128 MiB = 640 MiB`, against a `512m` cgroup limit whose comment claims 384 MiB.

The repository's canonical docs already state the correct bound, so the deploy artifacts contradict them:

- `README.md:44` - "Worst-case buffered memory is `2 x queue_max_bytes`."
- `docs/architecture.md:192` - "Worst-case buffered memory is bounded at `n_lanes x queue_max_bytes` (at most 2x, never unbounded)".

Same stale derivation is repeated for Kubernetes at `deploy/helm/values.yaml:82-92` (`limits.memory: 512Mi`). The chart's default config enables only `pubsub` (`deploy/helm/values.yaml:463`, with `eventlogfile.enabled: false` at `:528`, `eventlog_objects` at `:487`, `apexlog` at `:567`), so 384 MiB is arithmetically right *for the default values file only* - and silently wrong the moment an operator enables a bulk source in the `config:` block, which the commented example block invites.

Drift, not a decision: the `mem_limit` line was added in c0c9bba (2026-07-02 17:37, issue #71 item 2, when there was one shared queue) and the per-lane split landed in 33b579f (2026-07-02 18:41). Nothing revisited the deploy artifacts. Nothing pins either value - `tests/test_config_artifacts_drift.py:34-39` gates only the generated `config:` region of `values.yaml`, and the hand-written `resources:` block is outside it.

### Scope correction: this is a sizing/guidance defect, not a live OOM

The accounted 640 MiB ceiling is **not reachable with the sources the shipped config enables**, because each lane queue is also count-bounded:

- `src/sf2loki/config.py:861-869` - `queue_maxsize` default 10,000, applied per lane (`src/sf2loki/app.py:221`).
- `src/sf2loki/sources/pubsub_source.py:134,304` - the bridge queue is capped at 1,000 entries.

Hitting a lane's 256 MiB budget needs >= 26.8 KiB average charged cost per entry. Pub/Sub lines are ~1-3 KiB. ELF entries charge ~2 KiB of line plus the carried-ids checkpoint string (capped at 200 pairs, `src/sf2loki/sources/eventlogfile_source.py:98`, ~10 KiB) = ~12 KiB, so the count bound binds first at ~120 MB *charged* - and that checkpoint string is serialized once per file and shared across the file's rows (`src/sf2loki/sources/eventlogfile_source.py:613-627`) while `_entry_cost` charges it per entry (`src/sf2loki/app.py:340-345`), so charged bytes overstate real RSS on the bulk lane. Realistic saturated buffering for `config.docker.yaml` is tens of MB.

The byte budget does become the binding bound when lines are large - `apexlog` carries debug-log bodies in the line up to `max_line_bytes` (256 KiB), so a bulk lane of apexlog entries reaches 256 MiB long before 10,000 entries. That is exactly the configuration where the wrong formula in the comment produces an undersized limit.

## Why it matters

1. The comment states an invariant that does not hold: `512m` does not cover the ceiling the code documents for the two-lane configuration compose ships.
2. The comment is prescriptive guidance ("raise it if you raise either queue's byte budget") built on a formula missing the lane multiplier, so any operator who follows it under-sizes by up to 256 MiB. The Helm comment repeats it.
3. The failure mode when the byte budget genuinely binds (large-line bulk sources such as `apexlog`) is a container OOM kill under sink outage, followed by restart, checkpoint re-read, re-buffer, re-OOM - no data loss (checkpoints advance only on push success) but the deployment is down for the duration of the outage and each cycle re-spends Salesforce API calls re-downloading bodies.

## Proposed approach

1. Rewrite the `docker-compose.yml:46-52` comment to the correct formula: `queue_max_bytes x active lane classes` (streaming = `pubsub`; bulk = `eventlogfile`/`eventlog_objects`/`apexlog`; at most 2) `+ bridge_max_bytes + runtime overhead`. State explicitly that `config.docker.yaml` enables both lane classes, and that the entry-count bound (`queue_maxsize`, 10,000/lane) is the other ceiling - real buffering is `min(count bound x entry size, byte budget)` per lane, so the byte ceiling only binds for large lines.
2. Make the shipped compose deployment self-consistent by either raising `mem_limit` to cover the two-lane ceiling (~896m) or setting `sink.loki.batch.queue_max_bytes: 134217728` (128 MiB) in `config.docker.yaml` so `2 x 128 + 128 = 384 MiB` matches the existing 512m. Prefer the config route: it keeps the small default footprint and leaves the limit unchanged for existing deployments (note it in the compose comment either way).
3. Apply the same lane-count correction to `deploy/helm/values.yaml:82-92`, keeping `512Mi` valid for the chart's pubsub-only default but adding an explicit "if you enable a bulk source (eventlogfile / eventlog_objects / apexlog) in `config:`, the queue ceiling doubles - raise `limits.memory` accordingly" line. Edit only the hand-written `resources:` block; do not touch the generated `config:` region.
4. Add a drift test so the two artifacts cannot desynchronise from the config defaults again.

---

Imported from GitHub issue #135 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 135)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `docker-compose.yml` comment states the ceiling as `queue_max_bytes x lane classes + bridge_max_bytes`, names which lane classes `config.docker.yaml` enables, and notes the `queue_maxsize` count bound as the co-binding limit.
- [ ] #2 `mem_limit` in `docker-compose.yml` and the byte budgets in `config.docker.yaml` are mutually consistent: `lanes x queue_max_bytes + bridge_max_bytes <= mem_limit` with headroom for runtime overhead.
- [ ] #3 `deploy/helm/values.yaml:82-92` carries the lane-count caveat; the generated `config:` region is unchanged (`just gen-config` produces no diff).
- [ ] #4 New test `tests/test_deploy_memory_sizing.py` parses `docker-compose.yml` (`mem_limit`) and `config.docker.yaml`, derives the enabled lane classes with the same rule as `src/sf2loki/app.py:87-92`, computes `lanes x queue_max_bytes + bridge_max_bytes` from the effective config (falling back to the Pydantic defaults when unset), and asserts the compose memory limit is >= that ceiling. The test must fail on the current `main` values before the fix.
- [ ] #5 Same test (or a sibling case) asserts `deploy/helm/values.yaml`'s `resources.limits.memory` covers the ceiling implied by the chart's own default `config:` block.
- [ ] #6 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
