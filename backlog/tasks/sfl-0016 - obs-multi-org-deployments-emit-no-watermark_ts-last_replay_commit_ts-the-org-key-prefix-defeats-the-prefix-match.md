---
id: SFL-0016
title: >-
  obs: multi-org deployments emit no watermark_ts / last_replay_commit_ts (the
  org= key prefix defeats the prefix match)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/100'
ordinal: 16000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`Pipeline._record_commit_metric` (`src/sf2loki/app.py:530-549`) dispatches on the raw checkpoint key:

```python
def _record_commit_metric(self, key: str, value: str) -> None:
    if key.startswith("pubsub:"): ...
    elif key.startswith("eventlog_objects:"): ...
    elif key.startswith("eventlogfile:"): ...
    elif key == "apexlog": ...
```

In multi-org mode every key reaching that function carries an `org=<name>:` prefix, so none of the four branches match and neither gauge is ever set.

The rewrite chain:

- `src/sf2loki/app.py:998-1012` — for every org with a non-empty `name`, each built source is wrapped in `OrgSource`.
- `src/sf2loki/sources/org_adapter.py:115-117` — `OrgSource.events` replaces every yielded entry's token with `CheckpointToken(key=self._prefix + entry.checkpoint.key, value=...)`, `checkpoint_only` tokens included.
- `src/sf2loki/state/org_view.py:36-38` — `org_prefix(name)` returns `f"org={name}:"`.
- `src/sf2loki/app.py:499-515` — `Pipeline._commit` collects `last[entry.checkpoint.key]` (already prefixed) and calls `_record_commit_metric(key, value)` for each.

So a committed key looks like `org=prod:pubsub:/event/LoginEventStream`, `org=prod:eventlog_objects:LoginEvent`, `org=prod:eventlogfile:ApiTotalUsage`, `org=prod:apexlog`. The `legacy_fallback` flag (`app.py:1010`, `state/org_view.py:59-65`) only affects `load`, never the commit key, so this applies to the first configured org too — not just the second and later ones.

Both gauges are set nowhere else: `rg 'watermark_ts|last_replay_commit_ts' src/` yields only the definitions at `src/sf2loki/obs/metrics.py:361` (`sf2loki_last_replay_commit_timestamp_seconds`) and `:411` (`sf2loki_watermark_timestamp_seconds`), and the four setter call-sites at `src/sf2loki/app.py:533,538,543,547`.

Nothing stamps an `org` label either: `Pipeline` is deliberately handed the raw deployment-wide `Metrics`, not a `for_org` view (`src/sf2loki/app.py:1034-1041`; rationale at `src/sf2loki/obs/metrics.py:618-620`).

Reproduced against the real `Pipeline` with a fake source: key `pubsub:/event/LoginEventStream` produces `sf2loki_last_replay_commit_timestamp_seconds{topic="/event/LoginEventStream"}`; key `org=prod:pubsub:/event/LoginEventStream` produces no sample for that instrument under any `topic` value.

Existing coverage does not catch this — every commit-metric test uses an unprefixed key (`tests/test_pipeline.py:247`, `:263`, `:300`), and neither `tests/test_multiorg_app.py` nor `tests/sources/test_org_adapter.py` references either gauge.

## Why it matters

Two documented operator metrics vanish entirely for any deployment using the `orgs:` list, while single-org behaves normally — so the gap only appears after migrating to multi-org, and appears as absence rather than as a wrong value.

`docs/deployment/state.md:37-48` sells `sf2loki_watermark_timestamp_seconds` as the way to identify a stuck checkpoint key ("Its labels map directly to checkpoint keys") and lists the `org=<name>:` prefix a few lines later at `:50-58` without noting that the gauge itself disappears in that mode. `docs/observability/metrics.md:66,73,112-113` documents both gauges with no multi-org caveat. An operator building the obvious staleness alert from those docs (`time() - sf2loki_watermark_timestamp_seconds > N`) gets a rule that can never fire, or an `absent()` rule that fires permanently.

Blast radius is bounded: no shipped Grafana resource queries either gauge (`rg 'watermark|replay_commit' deploy/grafana/` → no hits; the alert pack in `deploy/grafana/rules/alerting/` keys off `last_push_success` and `ingest_lag`). The loss is the documented diagnostic and any operator-authored alert, not data or shipped alerting.

## Proposed approach

1. Add a prefix splitter next to `org_prefix` in `src/sf2loki/state/org_view.py` so the prefix format has exactly one definition:

   ```python
   _ORG_KEY_RE = re.compile(r"^org=([A-Za-z0-9_-]+):")

   def split_org_key(key: str) -> tuple[str, str]:
       """Split ``org=<name>:<rest>`` → ``("<name>", "<rest>")``; ``("", key)`` if unprefixed."""
   ```

   The character class matches `_ORG_NAME_PATTERN` at `src/sf2loki/config.py:1295` (`^[A-Za-z0-9_-]+$`), which is enforced on `OrgConfig.name` (`config.py:1308-1316`), so an org name can never contain `:` and the split is unambiguous. The module docstring at `src/sf2loki/state/org_view.py:1-19` already records that `org=` is deliberately distinct from every other namespace (`pubsub:`, `eventlogfile:`, `eventlog_objects:`, `backfill:`, `egress:`), so it cannot shadow one.

2. In `_record_commit_metric` (`src/sf2loki/app.py:530`), split first and dispatch on the remainder. The `apexlog` branch must become an equality check against the stripped key, not the raw one.

3. Carry the org as a gauge label when non-empty, and omit it entirely when empty so single-org series stay byte-identical (the pipeline holds raw `Metrics` by design, so pass the label explicitly rather than switching the pipeline to a `for_org` view):

   ```python
   org, rest = split_org_key(key)
   extra = {"org": org} if org else {}
   ...
   self._metrics.last_replay_commit_ts.labels(topic=rest, **extra).set(...)
   self._metrics.watermark_ts.labels(source="eventlogfile", object=event_type, **extra).set(...)
   ```

   `_Gauge.labels(**kwargs)` (`src/sf2loki/obs/metrics.py:91-92`) forwards free-form attributes, so a conditional label needs no instrument change. Do not build a `Metrics.for_org(...)` facade per commit — `for_org` (`src/sf2loki/obs/metrics.py:612-624`) constructs an `_OrgMetrics` that iterates `vars(inner)`, and `_record_commit_metric` runs once per key per flush (see #69 on hot-path overheads). If the facade route is preferred instead of an explicit label, cache one view per org name in a `dict[str, Metrics]` on the `Pipeline`.

4. `org` is already an allowed Loki stream label and an existing metric dimension elsewhere (`src/sf2loki/obs/metrics.py:170-193`), so this adds no new cardinality concept: one extra series per org per key, bounded by the configured org count.

5. Update the docs: add the `org` label to the two rows in `docs/observability/metrics.md:66,73`, and extend the key↔label mapping table at `docs/deployment/state.md:39-46` to show that a prefixed key surfaces as `org="<name>"` on the gauge. While there, fix the adjacent inaccuracy at `docs/deployment/state.md:50-52` — it states the prefix applies "for every org except the first configured one", but the first org commits prefixed keys too; only checkpoint *loads* fall back to the unprefixed key.

---

Imported from GitHub issue #100 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 100)' archive/issues-dump.json`).

## Additional evidence (parallel review lanes)

Secondary docs defect from the same root cause: docs/deployment/state.md:53-55 states the org key prefix applies "for every org except the first configured one". Wrong — commit keys are prefixed for every org including the first; `legacy_fallback` (src/sf2loki/app.py:1010) only affects `OrgCheckpointView.load` (src/sf2loki/state/org_view.py:57-63), never the emitted key. Fix the doc line together with the code fix.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `split_org_key` (or equivalent) lives in `src/sf2loki/state/org_view.py` beside `org_prefix`, with the org-name character class derived from / consistent with `_ORG_NAME_PATTERN` (`src/sf2loki/config.py:1295`).
- [ ] #2 `_record_commit_metric` strips a leading `org=<name>:` before dispatch; the `apexlog` branch compares the stripped key.
- [ ] #3 `tests/state/test_org_view.py` (or the module's existing test file): unit tests for `split_org_key` covering `org=prod:pubsub:x` → `("prod", "pubsub:x")`, `pubsub:x` → `("", "pubsub:x")`, `org=a-b_1:apexlog` → `("a-b_1", "apexlog")`, and a key that merely contains `org=` mid-string being left untouched.
- [ ] #4 `tests/test_pipeline.py`: four new tests driving `Pipeline` with prefixed keys and asserting the samples exist with the `org` label — `org=prod:pubsub:/event/LoginEventStream` → `sf2loki_last_replay_commit_timestamp_seconds{topic="/event/LoginEventStream", org="prod"}`; `org=prod:eventlog_objects:LoginEvent`, `org=prod:eventlogfile:Login`, and `org=prod:apexlog` → `sf2loki_watermark_timestamp_seconds{source=..., object=..., org="prod"}` with the parsed watermark value, not the "now" fallback.
- [ ] #5 A test asserting two orgs committing the same inner key (`org=prod:eventlog_objects:LoginEvent` and `org=emea:eventlog_objects:LoginEvent`) yield two distinct series with different values.
- [ ] #6 The existing single-org tests at `tests/test_pipeline.py:247,263,300` pass unmodified (no `org` attribute is added when the key is unprefixed), plus an explicit assertion that no series with `org=""` is emitted in single-org mode.
- [ ] #7 A test that a key matching no branch after stripping (e.g. `org=prod:backfill:Hourly:Login`) still records nothing.
- [ ] #8 `docs/observability/metrics.md:66,73` and `docs/deployment/state.md:39-52` document the `org` label and correct the "except the first configured one" statement about commit-side prefixing.
- [ ] #9 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
