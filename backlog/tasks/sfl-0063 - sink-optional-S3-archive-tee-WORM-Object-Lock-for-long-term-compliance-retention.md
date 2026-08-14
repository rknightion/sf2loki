---
id: SFL-0063
title: >-
  sink: optional S3 archive tee (WORM/Object Lock) for long-term compliance
  retention
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
  - 'https://github.com/rknightion/sf2loki/issues/147'
ordinal: 63000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Events have exactly one destination. `SinkConfig` holds a single field (`src/sf2loki/config.py:983-984`: `loki: LokiConfig`), `Pipeline.__init__` accepts one `sink: Sink` (`src/sf2loki/app.py:175`, stored at `app.py:184`), and the composition root constructs exactly one implementation (`app.py:922` → `app.py:1036`). The `Sink` protocol was written as a seam for additional implementations (`src/sf2loki/sinks/base.py:1`, `:33-36`) but nothing fans out across it.

There is no archival capability anywhere in the tree: a repo-wide search for `archive` / `object lock` / `WORM` / `immutable` across `*.py`, `*.md`, `*.yaml` returns no hits. The only retention handling is Salesforce-side warnings (`src/sf2loki/backfill.py:78,205-210`).

Consequence: the durable lifetime of every ingested event equals the Loki tenant's retention window. Loki is a query store, typically configured for 30-90 days; Salesforce retains EventLogFile for 1 day without the Event Monitoring add-on and 30 days (up to 365) with it (`README.md:189-190`), and Pub/Sub replay for 72h (`src/sf2loki/salesforce/pubsub_client.py:149`). Once the Loki window lapses there is no copy of the data anywhere in the chain, and no configuration makes one.

Scope note for future readers: this is **not** #24 (CLOSED, declined). #24 proposed `sink.type: loki | otlp` — a fork of the primary delivery path to an alternative *query* backend, and explicitly ruled fan-out out of scope. This issue keeps Loki as the sole query destination and adds a write-only raw-retention tee behind the existing `Sink` protocol.

## Why it matters

The categories sf2loki carries — login/logout history, API access, Setup Audit Trail, Threat Detection anomalies, report exports — are the exact records access-audit regimes require retained for years and tamper-evidently. An operator with a 7-year obligation currently has to build and run a second, parallel export pipeline: another connected app, another API budget consumed against the same org, another overlap-guard hazard (`src/sf2loki/sources/overlap.py`), and a duplicate of the decode/shape/redact/batch work sf2loki already performs. Nothing about that second pipeline is Salesforce-specific value; it is the same extraction twice.

Two secondary gains from siting the tee inside the pipeline rather than downstream of Loki:

- The Loki sink truncates oversized lines **in place** at `batch.max_line_bytes` (`src/sf2loki/sinks/loki/sink.py:326-338` mutates `entry.line`). An archive written before the push is the only place the untruncated payload survives.
- A `PermanentSinkError` drop (`app.py:472-490`) discards a poison batch from Loki and advances the checkpoint. Those entries are unrecoverable today; a pre-push archive retains them.

A Loki-side solution does not substitute. Loki compacts and deletes its own chunks per its retention config, chunk objects are not a per-object-lockable or auditor-consumable artifact, and there is no way to hand an auditor "the immutable record" from a Loki chunk store.

## Proposed approach

Opt-in, absent by default, zero effect on any existing deployment when unconfigured.

**Config.** Add `archive: ArchiveConfig | None = None` to `SinkConfig` (`config.py:983-984`). `None` disables the whole feature. Fields:

- `bucket: str` (required when present), `prefix: str = "sf2loki/"`, `region: str | None`, `endpoint_url: str | None` (S3-compatible stores), mirroring `S3StateConfig` (`config.py:994-1013`).
- `compression: Literal["gzip", "none"] = "gzip"`.
- `object_lock_mode: Literal["none", "governance", "compliance"] = "none"` and `retain_for: Duration | None` (required when mode is not `none`).
- `on_error: Literal["retry", "drop"] = "retry"` — see failure policy below.

Adding to the config model requires `just gen-config` (regenerates `config.example.yaml` + `docs/config-reference.md`; the drift gate in `tests/test_config_artifacts_drift.py` fails otherwise) and the same surface in `deploy/helm/values.yaml`, whose config map is generated with its own drift gate (#75).

**Implementation.** New `src/sf2loki/sinks/archive/s3_archive.py` implementing `Sink` (`sinks/base.py:33-36`). Reuse two established patterns verbatim:

- The injectable client factory from `src/sf2loki/state/s3_store.py:161-166` plus lazy `_default_client_factory` at `s3_store.py:204-215`, so `aiobotocore` stays out of module-level imports and unit tests inject a fake without the extra installed.
- The `importlib.util.find_spec("aiobotocore")` guard from `src/sf2loki/state/__init__.py:26-32`, so a missing `sf2loki[s3]` extra fails at startup/`--check` with an actionable message instead of as a raw `ImportError` on the first flush.

**Object format.** One object per flush, gzipped NDJSON, one record per entry: `timestamp` (epoch nanoseconds), `labels`, `structured_metadata`, `line` — sufficient to reconstruct the Loki push. `checkpoint_only` entries carry no payload and are excluded (they are already filtered at `app.py:422-427`). Key layout `{prefix}{sf_org_id}/{source}/{event_type}/{YYYY}/{MM}/{DD}/{epoch_ms}-{seq}.ndjson.gz`, partitioned so an auditor can fetch one org/type/day without a full-bucket scan, and unique per flush so a replayed batch after a crash appears as a distinct object rather than silently interleaving.

**Pipeline wiring.** `Pipeline.__init__` gains `archive: Sink | None = None` (`app.py:175`, `:184`). In `_flush` (`app.py:420`), after egress-governor admission (`app.py:437-451`) and **before** entering the retry loop at `app.py:453`, write the archive object once. Then push to Loki as today, then `_commit` (`app.py:499`) — the commit-after-push invariant becomes commit-after-both.

**The retry-duplication trap — must be handled explicitly.** The retry loop is `while True` around `self._sink.push` (`app.py:453-470`); a `RetryableSinkError` re-enters it. Writing the archive inside that loop re-PUTs the object on every Loki retry. S3 Object Lock requires bucket versioning, so repeat PUTs to the same key create new immutable versions that cannot be deleted before their retain-until date — an unbounded, undeletable duplicate for every Loki 429/5xx. Write the archive exactly once per `_flush` invocation, before the loop. Cross-restart duplication (archive written, process dies before `_commit`) remains possible and is the same at-least-once contract the Loki path already documents; state it in the docs rather than attempting exactly-once.

**Failure policy.** `on_error: retry` (default): bounded backoff, then treat as retryable and stall the lane, so no record reaches Loki without reaching the archive — correct for the compliance use case, at the cost of coupling ingest availability to the archive store's. `on_error: drop`: count and continue to Loki, for operators who prefer ingest availability. Give the archive its own metrics and its own failure mark; do **not** fold it into `_sink_degradation_check` (`app.py:626-640`) in the first cut, so an archive-store outage does not flip readiness on a deployment whose Loki path is healthy.

**Metrics** (OTel-native, suffixes added by the exporter): `sf2loki_archive_objects{outcome}`, `sf2loki_archive_bytes`, `sf2loki_archive_write_duration`, `sf2loki_archive_entries_dropped{reason}`.

**Object Lock mechanics.** Enabling Object Lock on a bucket is a create-time bucket property implying versioning; sf2loki cannot enable it. Two deployment shapes both work: a bucket-level **default retention rule** (nothing client-side needed — plain `put_object` inherits the lock) or per-object `ObjectLockMode` + `ObjectLockRetainUntilDate` on `put_object`. Support both, document the bucket-default path as the simplest correct deployment. Verify during implementation whether botocore requires `ContentMD5` alongside the object-lock parameters.

**Doctor.** Extend `sf2loki doctor` (which already probes the configured state backend, `src/sf2loki/doctor.py:533`) to probe the archive: `HeadBucket`, a test `put_object` under a `_doctor/` key, and `GetObjectLockConfiguration`. Warn loudly when `object_lock_mode` is `none` **and** the bucket has no default retention rule — that is the silent-non-compliance case where the operator believes the archive is immutable and it is an ordinary mutable bucket.

**Redaction fidelity — document, do not change.** Redaction and transforms are applied by source shaping before an entry enters the pipeline (`src/sf2loki/model.py:28-35`), so the archive holds post-redaction lines, not raw Salesforce payloads. That is the correct default for a PII-controlled archive, but it must be stated so operators do not assume raw fidelity. Also note the interaction with the unsalted-hash warning (`app.py:711-726`): a `hash` transform with no `transform_salt` produces table-reversible pseudonyms, and in a WORM archive those cannot be deleted or rewritten.

**Docs.** A page under `docs/` covering the compliance use case, the bucket prerequisites (Object Lock + versioning), the `sf2loki[s3]` extra, the at-least-once/duplicate contract, the post-redaction fidelity caveat, and the `on_error` availability trade-off. Cross-link from `README.md` and `docs/architecture.md`'s sink section.

---

Imported from GitHub issue #147 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 147)' archive/issues-dump.json`).

Tests that pin the behaviour:

- [ ] `test_archive_written_once_across_loki_retries` — fake sink raises `RetryableSinkError` twice then succeeds; assert the fake S3 client received exactly **one** `put_object`. This is the regression test for the undeletable-duplicate-versions trap.
- [ ] `test_no_commit_when_archive_write_fails` — archive `put_object` raises with `on_error: retry`; assert no checkpoint commit and no Loki push for that batch, and that a restart re-delivers it.
- [ ] `test_archive_drop_mode_continues_to_loki` — with `on_error: drop`, an archive failure increments `archive_entries_dropped` and the Loki push plus commit still happen.
- [ ] `test_archive_keeps_untruncated_line` — an entry longer than `batch.max_line_bytes` is archived at full length while Loki receives the capped line, pinning the ordering against `sinks/loki/sink.py:326-338`.
- [ ] `test_archive_retains_permanently_dropped_batch` — sink raises `PermanentSinkError`; the entries are present in the archive object even though Loki dropped them.
- [ ] `test_checkpoint_only_entries_not_archived` — a flush of only `checkpoint_only` tokens writes no object and still commits (`app.py:422-432`).
- [ ] `test_object_lock_params_passed_when_configured` — `object_lock_mode: compliance` + `retain_for: 2555d` produces `put_object` kwargs with `ObjectLockMode="COMPLIANCE"` and an `ObjectLockRetainUntilDate` matching flush time + retention; `object_lock_mode: none` passes neither.
- [ ] `test_archive_object_key_layout` — key contains org id, source, event type and a UTC date path, and two flushes in the same second produce distinct keys.
- [ ] `test_archive_ndjson_roundtrip` — the gzipped object decodes to one JSON record per entry carrying `timestamp`, `labels`, `structured_metadata` and `line`, sufficient to reconstruct the Loki push.
- [ ] `test_disabled_archive_is_inert` — `sink.archive` absent: no S3 client is constructed and no archive metric is registered.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `sink.archive` added to `SinkConfig` (`config.py:983-984`), defaulting to `None`; `config.example.yaml` and `docs/config-reference.md` regenerated via `just gen-config` and the drift gate green.
- [ ] #2 `src/sf2loki/sinks/archive/s3_archive.py` implements `Sink` (`sinks/base.py:33-36`) with an injectable `client_factory` and no module-level `aiobotocore` import.
- [ ] #3 Selecting the archive without the `s3` extra installed raises `ConfigError` at build/`--check` time naming `pip install 'sf2loki[s3]'`, matching `state/__init__.py:26-32`.
- [ ] #4 `Pipeline` accepts `archive: Sink | None = None`; with `None` the flush path is byte-for-byte unchanged in behaviour and no archive code executes.
- [ ] #5 Archive write happens before the Loki push and before the retry loop; checkpoints commit only after both destinations succeed.
- [ ] #6 `sf2loki doctor` probes bucket reachability, write permission, and Object Lock configuration, warning when neither `object_lock_mode` nor a bucket default retention rule is in effect.
- [ ] #7 Docs page published covering prerequisites, duplicate semantics, post-redaction fidelity, and the `on_error` trade-off; linked from `README.md` and `docs/architecture.md`.
- [ ] #8 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
