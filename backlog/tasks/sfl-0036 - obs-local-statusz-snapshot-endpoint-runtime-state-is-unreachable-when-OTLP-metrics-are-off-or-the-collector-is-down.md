---
id: SFL-0036
title: >-
  obs: local /statusz snapshot endpoint - runtime state is unreachable when OTLP
  metrics are off or the collector is down
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-2
  - roadmap
milestone: m-4
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/120'
ordinal: 36000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

The health server exposes exactly two routes and 404s everything else:

- `decide()` (`src/sf2loki/obs/health.py:23-44`) returns `200 "ok"` for `/healthz`, a readiness verdict for `/readyz`, and `404, "not found"` for any other path (`health.py:44`).
- The only extension point is `Health.set_degraded_check` (`health.py:76-82`), which can only change the `/readyz` **body string**.

Metrics are OTLP push-only and **off by default**:

- `src/sf2loki/obs/metrics.py:1-8` — "All metrics are emitted via OTLP/HTTP (push). There is no Prometheus scrape endpoint."
- `TelemetryConfig.enabled` defaults to `False` (`src/sf2loki/config.py:1208`); when disabled, metrics are still recorded in-process but exported nowhere (`config.py:1200-1208`).

The consequence: the process holds a complete picture of its own runtime state and there is no way to read it out of a running container.

| State | Where it lives | Reachable today |
| --- | --- | --- |
| Per-lane queue depth / `queued_bytes` / `failing_since` | `src/sf2loki/app.py:95-113`, `196-199`, `227-230` | no |
| Aggregate sink outage start | `Pipeline.sink_failing_since`, `app.py:201-210` | only as a rounded duration inside the `/readyz` 503 body (`app.py:626-642`) |
| Last checkpoint commit per key + watermarks | `app.py:499-549` (`_commit`, `_record_commit_metric`) | no |
| Per-org auth failure set | `App._degraded_orgs`, `app.py:906`, `1270`; check at `app.py:842-858` | only as the first failing org's name in the `/readyz` body |
| Leadership | written into a gauge only — `app.py:1185`, `1199`, `1215`; never held as a readable attribute | no |
| Egress budget used / paused / day | `src/sf2loki/egress.py:116-129` (`_budget`, `_used`, `_paused`, `_date`) | only as a pause reason in the `/readyz` body (`egress.py:270-275`) |

Out-of-process commands are not substitutes:

- `sf2loki doctor` validates external dependencies (auth, Loki, state backend, OTLP endpoint, coordinator), not live in-process pipeline state.
- `sf2loki state show` reads the configured checkpoint store, but on the file backend the store's exclusive flock is acquired lazily on the first `load`, so **even a read-only `show` fails while the daemon is running** unless `--force` bypasses the lock (`src/sf2loki/statecmd.py:13-24`). Live checkpoint inspection on the default backend is therefore unavailable exactly when it is wanted.

## Why it matters

Failure scenario: Loki pushes stall, or the OTLP collector/gateway is itself the thing that is down or misconfigured — the moment dashboards and alerts go dark. An operator execs into the container and can learn two facts: `ok`, and `ready` / a single degradation string. Which lane is backed up, how many bytes are buffered, how long the outage has actually run, where each source's watermark sits, whether one org out of five is failing auth while the rest are fine, how much of the daily byte budget is spent — all of it exists in memory and is unreachable. Triage falls back to grepping logs for the last emitted lines and inferring state from them.

This is worse for the default deployment than for an instrumented one: `telemetry.enabled` is `False` out of the box (`config.py:1208`), so a fresh install has no metric path at all, and the recommended dashboards/alert pack (`deploy/grafana/`) depend entirely on OTLP egress plus suffix translation (see closed #58). A zero-dependency local snapshot removes the single point of observability failure and is the first thing to paste into an incident channel or attach to a bug report.

## Proposed approach

Add `GET /statusz` to the existing hand-rolled server, in the same zero-dependency style, gated by config.

**1. Snapshot accessors on the state owners** (no new I/O, no blocking calls):

- `Pipeline.snapshot() -> dict[str, object]` in `src/sf2loki/app.py`: per lane, `{queue_depth (lane.queue.qsize()), queued_bytes, n_producers, failing_since_seconds (None | monotonic delta)}`. Add a `dict[str, tuple[str, float]]` of last-committed `key -> (value, unix_ts)` maintained in `_commit` / `_record_commit_metric` (`app.py:499-549`); this is precise and non-blocking, unlike reading the store.
- `EgressGovernor.snapshot() -> dict[str, object]` in `src/sf2loki/egress.py`: `{budget_bytes, used_bytes, paused, action, day}` from `_budget`/`_used`/`_paused`/`_action`/`_date` (`egress.py:116-129`).
- `App`: hold `self._is_leader: bool` alongside the existing `self._metrics.leader.set(...)` calls (`app.py:1185`, `1199`, `1215`) — the flag is currently only observable as a metric.
- Orgs: `{name: auth_ok}` derived from `App._degraded_orgs` (`app.py:906`) against the configured org list.

**2. Compose in `App` and install a provider on `Health`**, mirroring the `set_degraded_check` wiring at `app.py:1058-1059`:

```python
health.set_status_provider(lambda: {...})   # returns a JSON-serialisable dict
```

`Health` renders it with `json.dumps` and serves `200` with `Content-Type: application/json`. Keep `decide()` pure: give it a new keyword (e.g. `status_body: str | None`) so `None` → the existing `404, "not found"` and a string → `200, body`. The provider must be synchronous and I/O-free; it runs inside the request handler on the event loop (`health.py:118-160`).

Suggested shape:

```json
{
  "version": "1.4.0",
  "uptime_seconds": 3612,
  "leader": true,
  "ready": true,
  "degraded_reason": null,
  "orgs": {"prod": true, "sandbox": false},
  "lanes": {
    "streaming": {"queue_depth": 0, "queued_bytes": 0, "n_producers": 1, "failing_since_seconds": null},
    "bulk": {"queue_depth": 812, "queued_bytes": 41943040, "n_producers": 3, "failing_since_seconds": 947}
  },
  "checkpoints": {"pubsub:/event/LoginEventStream": {"value": "…", "committed_ts": 1751500000.0}},
  "egress": {"budget_bytes": 0, "used_bytes": 12938411, "paused": false, "action": "pause", "day": "2026-07-30"}
}
```

**3. Config:** add `service.status_endpoint: bool = True` next to `health_addr` (`config.py:1275`). Adding a config field requires `just gen-config` (regenerates `config.example.yaml` + `docs/config-reference.md`) or the drift gate in `tests/test_config_artifacts_drift.py` fails; the Helm chart's generated values also carry the field (`deploy/helm/values.yaml`).

**4. `sf2loki status [--addr HOST:PORT]` CLI verb** in `src/sf2loki/cli.py` (subparser alongside `doctor` / `state`): fetch `/statusz`, pretty-print a table, exit non-zero when unreachable. Optional but cheap, and it is what an operator reaches for first.

**Security envelope.** The health server binds all interfaces by default (`health.py:99-100`, `health_addr: ":8080"` at `config.py:1275`), so `/statusz` shares the trust boundary of `/readyz` — which already emits org names (`app.py:855`) and budget state (`egress.py:274-275`) to any caller. Checkpoint values are explicitly non-secret (`src/sf2loki/statecmd.py:10`). The snapshot must therefore never include tokens, private-key paths, auth headers, endpoint credentials, or raw config; org names, watermarks, and counters only. `status_endpoint: false` is the escape hatch for deployments that will not expose it.

**Rejected alternative:** serving a dump of the in-memory metric reader. `InMemoryMetricReader.get_metrics_data()` is destructive for gauges — a second collect with no new measurement returns nothing (`src/sf2loki/obs/metrics.py:212-214`) — so a `/statusz` request would perturb the reader that `Metrics.registry` and the test suite read through. Typed accessors, not a metric dump.

---

Imported from GitHub issue #120 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 120)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `service.status_endpoint: bool = True` added to `ServiceConfig` (`src/sf2loki/config.py:1265-1292`), `just gen-config` re-run, `tests/test_config_artifacts_drift.py` green.
- [ ] #2 `Health.set_status_provider()` added; `decide()` stays a pure function and still returns `404, "not found"` for unknown paths and when no provider is installed.
- [ ] #3 `GET /statusz` returns `200` with `Content-Type: application/json` and a body containing `version`, `leader`, `ready`, `orgs`, `lanes`, `checkpoints`, `egress`.
- [ ] #4 `GET /statusz` returns `404` when `service.status_endpoint: false`.
- [ ] #5 `Pipeline.snapshot()` and `EgressGovernor.snapshot()` added; neither performs I/O, awaits, or calls `CheckpointStore.load` (so an s3/gcs backend cannot block the health request).
- [ ] #6 `App` tracks leadership as a readable boolean in addition to the existing `leader` gauge writes (`app.py:1185`, `1199`, `1215`).
- [ ] #7 The in-memory metric reader is not read by the endpoint (guards the destructive-collect behaviour at `src/sf2loki/obs/metrics.py:212-214`).
- [ ] #8 `tests/obs/test_health.py`: `decide()` routing cases for `/statusz` with and without a provider; a socket-level test asserting the JSON content type and parseable body.
- [ ] #9 `tests/test_app.py`: a lane with a forced `failing_since` and non-zero `queued_bytes` shows up in the snapshot with a positive `failing_since_seconds`; a degraded org appears as `false` in `orgs`; a committed checkpoint key appears in `checkpoints` with a `committed_ts`.
- [ ] #10 A test asserts the snapshot contains no secret-bearing keys (no `token`, `secret`, `password`, `private_key`, `auth_token` substrings in the serialised body) for a config that sets all of them.
- [ ] #11 `tests/test_cli.py`: `sf2loki status` prints the fetched snapshot and exits non-zero when the address is unreachable (if the CLI verb is included).
- [ ] #12 Docs: `/statusz` documented in `docs/troubleshooting.md` (as the first mid-incident step when dashboards are dark), `docs/reference/cli.md` (the `status` verb), and the readiness-vs-liveness section it sits next to; the endpoint's non-secret guarantee noted in `docs/security.md`.
- [ ] #13 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
