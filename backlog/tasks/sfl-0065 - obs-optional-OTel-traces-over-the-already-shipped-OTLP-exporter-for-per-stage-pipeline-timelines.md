---
id: SFL-0065
title: >-
  obs: optional OTel traces over the already-shipped OTLP exporter for per-stage
  pipeline timelines
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
  - 'https://github.com/rknightion/sf2loki/issues/149'
ordinal: 65000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

sf2loki is OTLP-push-native but metrics-only. `opentelemetry-sdk` and `opentelemetry-exporter-otlp-proto-http` are hard runtime dependencies (`pyproject.toml:24-25`), yet the only OTel usage in `src/` is the meter wiring in `src/sf2loki/obs/metrics.py`:

- `obs/metrics.py:269-286` builds an `OTLPMetricExporter` + `PeriodicExportingMetricReader` when `service.telemetry.enabled` is set.
- `obs/metrics.py:288-297` builds the `Resource` (`service.name=sf2loki`, `service.version`, plus `telemetry.resource_attributes`) and the `MeterProvider`.
- `obs/metrics.py:626-632` exposes `force_flush()` / `shutdown()`; `app.py:1220` calls `shutdown()` on graceful exit.

No module imports `opentelemetry.trace`. `TelemetryConfig` (`config.py:1200-1255`) has no traces fields, and `config.py:1209-1216` documents `endpoint` as the metrics URL (`https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics`). `src/sf2loki/obs/` contains only `metrics.py`, `logging.py`, `health.py`, `limits_poller.py`. `docs/observability/` contains only `metrics.md`, `dashboards.md`, `alerts.md`.

The same exporter distribution already installed ships `opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter`, and `opentelemetry.sdk.trace` ships `TracerProvider` / `BatchSpanProcessor` (verified importable in the repo venv at opentelemetry 1.43.0). The Grafana Cloud gateway exposes a sibling `/otlp/v1/traces` path that accepts the identical basic-auth header `telemetry_headers()` already computes (`config.py:1571-1589`). So traces are a wiring change with **zero new dependencies**.

Long multi-step operations are currently observable only as aggregates, with no per-operation breakdown:

- EventLogFile poll cycle: `sources/eventlogfile_source.py:283-302` times the whole cycle into the `sf2loki_eventlogfile_cycle_seconds` gauge (`obs/metrics.py:570-575`); the internal stages (`_resolve_event_types` listing at `:397`, `_process_cycle` fan-out at `:314`, `_process_event_type` download + CSV parse at `:490`) have no timing at all.
- Loki push: `sf2loki_loki_push_duration_seconds` (`obs/metrics.py:321-326`) covers the entire tenacity retry envelope at `sinks/loki/sink.py:219-231`, so a 3-attempt push with backoff is indistinguishable from one slow attempt.
- Big-object DESC drain (`sources/eventlog_objects_source.py`) and checkpoint flush (`state/`) are similarly single-number.

## Why it matters

`sf2loki_ingest_lag_seconds` (`obs/metrics.py:341-360`) spikes for one event type. The dashboards show the lag but cannot attribute it: Salesforce list latency, blob download, CSV parse, queue wait, or Loki push retries. Answering that today means reading interleaved JSON debug logs across concurrent workers and reconstructing a timeline by hand. Four coarse spans per cycle turn that into one trace view.

Value is convenience-grade, not correctness-grade — hence low severity. The feature must stay strictly opt-in and default off so existing deployments are byte-identical in behaviour.

## Proposed approach

**Config** (`config.py`, `TelemetryConfig` at `:1200`):

- `traces_enabled: bool = False` — "Push coarse pipeline spans via OTLP/HTTP. Requires `enabled` for credential/resource reuse."
- `traces_endpoint: str = ""` — full OTLP/HTTP traces URL; when blank and `endpoint` ends in `/v1/metrics`, derive it by replacing that suffix with `/v1/traces`; when blank and no such suffix, fail validation with an explicit message rather than guessing.
- `trace_sample_ratio: float = 1.0` — wired to `TraceIdRatioBased`; safe at 1.0 because span volume is bounded by cycles, not events.

Validate in the same place as the existing telemetry credential check (`config.py:1557-1567`): `traces_enabled` without `enabled` is a config error. `config.py` changes require `just gen-config` (regenerates `config.example.yaml` + `docs/config-reference.md`; `tests/test_config_artifacts_drift.py` is the CI gate).

**Wiring** — new module `src/sf2loki/obs/tracing.py` rather than growing `metrics.py`:

- Factor the resource construction out of `obs/metrics.py:288-297` into a shared `build_resource(version, telemetry) -> Resource` so both providers carry byte-identical resource identity.
- `class Tracing`: owns a `TracerProvider` + `BatchSpanProcessor(OTLPSpanExporter(endpoint=..., headers=...))` when `traces_enabled`, otherwise a provider with no processor (spans become cheap no-ops). Expose `tracer(name)`, `force_flush()`, `shutdown()`.
- Do **not** set the global tracer provider. Follow the injected-dependency pattern `Metrics` already uses so tests can build isolated instances; construct it at the composition root next to `Metrics` (`app.py:913-916`, reusing `telemetry_headers(cfg.service.telemetry, cfg.sink.loki)`) and call `shutdown()` alongside `self._metrics.shutdown()` at `app.py:1220`.
- `backfill.py:737` and `doctor.py:362,883` build a bare `Metrics()`; give `Tracing` the same zero-arg disabled default so those paths need no change.

**Spans — coarse only, no per-event spans:**

| span | site |
| --- | --- |
| `elf.poll_cycle` (attrs: event type count, org) | `sources/eventlogfile_source.py:283-302` |
| `elf.event_type` → child `elf.download`, `elf.parse` | `sources/eventlogfile_source.py:490` |
| `eventlog_objects.drain_segment` | `sources/eventlog_objects_source.py` DESC drain loop |
| `loki.push` with one span **event** per retry attempt (attempt number, status, `Retry-After`) | `sinks/loki/sink.py:219-231` |
| `checkpoint.commit_many` | `state/` store flush path |

**Two constraints that must shape the implementation:**

1. **Async-generator context.** `events()` at `sources/eventlogfile_source.py:283` is an async generator that yields entries mid-cycle. Wrapping a `with tracer.start_as_current_span(...)` around a `yield` attaches OTel context that survives the generator's suspension and leaks into the consumer's task. Use explicit `span = tracer.start_span(...)` / `span.end()` in a `try/finally` for any span that spans a `yield`.
2. **The pipeline is decoupled, so there is no end-to-end trace.** Sources hand entries to per-lane queues and a separate worker task pushes them; `Batch` (`model.py:66`) carries no trace context and must not grow one (that would mean per-event context propagation and per-event spans). Source-side spans and `loki.push` spans are therefore **separate traces**, joined by resource + attributes, not by parent/child. Document this explicitly so nobody later "fixes" it by threading context through the queue.

**Span attributes are metadata only** — event type, org id, file id, row/byte/entry counts, attempt numbers, status codes. Never row content, field values, or usernames; the redaction/filter rules apply to event bodies, and spans must not become a bypass.

**Doctor.** Extend `_check_telemetry` (`doctor.py:429`) or add a sibling check that POSTs an empty protobuf body to the resolved traces endpoint when `traces_enabled`, reusing the existing empty-export pattern and the same 401/403 credential guidance.

**Docs.** New `docs/observability/traces.md` (what spans exist, the two constraints above, Grafana Cloud + local Alloy endpoint examples, sampling and cost notes), registered in the `zensical.toml` nav next to `observability/metrics.md` (`zensical.toml:35-37`).

---

Imported from GitHub issue #149 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 149)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `service.telemetry.traces_enabled`, `traces_endpoint`, `trace_sample_ratio` added to `TelemetryConfig` (`config.py:1200`), all defaulting to traces-off.
- [ ] #2 `traces_enabled: true` with `enabled: false` raises a `ConfigError` naming both fields.
- [ ] #3 Blank `traces_endpoint` derives `/v1/traces` from an `endpoint` ending in `/v1/metrics`; a non-matching `endpoint` with blank `traces_endpoint` is a config error, not a silent guess.
- [ ] #4 `just gen-config` re-run; `tests/test_config_artifacts_drift.py` green.
- [ ] #5 `src/sf2loki/obs/tracing.py` provides `Tracing` with `tracer()`, `force_flush()`, `shutdown()`; no global tracer provider is installed.
- [ ] #6 `build_resource()` shared by `MeterProvider` and `TracerProvider`; a test asserts both providers report identical resource attributes.
- [ ] #7 `Tracing` constructed at `app.py:913-916` and shut down beside `self._metrics.shutdown()` at `app.py:1220`; `backfill.py` and `doctor.py` need no signature changes.
- [ ] #8 Spans emitted for the ELF poll cycle, per-event-type download/parse, big-object drain segment, Loki push (retry attempts as span events), and checkpoint flush.
- [ ] #9 `docs/observability/traces.md` added and registered in `zensical.toml` nav.
- [ ] #10 `tests/obs/test_tracing.py`: with traces disabled (default), no span processor is attached and `tracer().start_span(...)` records nothing exportable; with traces enabled against an `InMemorySpanExporter`, the expected span names/attributes appear.
- [ ] #11 `tests/obs/test_tracing.py`: endpoint-derivation table test (`/otlp/v1/metrics` → `/otlp/v1/traces`, explicit `traces_endpoint` wins, non-matching endpoint errors).
- [ ] #12 `tests/sinks/`: a Loki push forced through two transient failures produces one `loki.push` span with three retry-attempt span events carrying attempt number and status.
- [ ] #13 `tests/sources/`: an ELF cycle over a fake client produces `elf.poll_cycle` with `elf.event_type`/`elf.download`/`elf.parse` children, and a regression test asserts no OTel context remains attached in the consumer task after the generator yields (guards the async-generator leak).
- [ ] #14 A test asserts no span attribute carries event-row content (spans built from a fixture row expose only counts/ids/types).
- [ ] #15 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
