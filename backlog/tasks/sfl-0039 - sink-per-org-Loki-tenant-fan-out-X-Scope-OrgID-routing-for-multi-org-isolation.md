---
id: SFL-0039
title: >-
  sink: per-org Loki tenant fan-out (X-Scope-OrgID routing) for multi-org
  isolation
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
  - 'https://github.com/rknightion/sf2loki/issues/123'
ordinal: 39000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

Multi-org ingestion (#31) shares exactly one Loki tenant across every configured org. There is no way to route each org's entries to its own tenant, and no way to express the intent in config.

Current shape:

- `LokiConfig` holds a single `tenant_id` (`src/sf2loki/config.py:931-936`) alongside a single `url`/`auth_token(_file)`; `SinkConfig` exposes exactly one `loki` block (`src/sf2loki/config.py:983-984`).
- `OrgConfig` carries only `name`, `salesforce`, `sources` (`src/sf2loki/config.py:1298-1322`). It is a `StrictModel` (`extra="forbid"`), so an `orgs[].sink` key is rejected at load time rather than ignored.
- `LokiSink` builds one static header set at construction (`src/sf2loki/sinks/loki/sink.py:112` calling `_build_headers`, `src/sf2loki/sinks/loki/sink.py:120-131`): Basic auth with `tenant_id` as the username when `auth_token` is set, otherwise a single `X-Scope-OrgID: <tenant_id>` header. `_post` always posts to `self._cfg.url` with `{**self._headers, **content_headers}` (`src/sf2loki/sinks/loki/sink.py:203-207`), so both tenant and URL are process-wide constants.
- The composition root builds one sink and gives it to one pipeline: `src/sf2loki/app.py:922` and `src/sf2loki/app.py:1033-1041`. `doctor` (`src/sf2loki/doctor.py:362`) and `backfill` (`src/sf2loki/backfill.py:740`) each build their own single `LokiSink` the same way.
- Documented as a limitation: `README.md:342-347` ("the sink, state store, coordinator, and service settings stay shared … one Loki tenant") and `docs/architecture.md:222` ("one shared Loki sink").

Everything needed to partition already exists. `OrgSource` injects an `org` stream label (plus per-org `sf_org_id`/`environment`) into every non-`checkpoint_only` entry (`src/sf2loki/sources/org_adapter.py:110-114`), `org` is in `ALLOWED_LABELS` (`src/sf2loki/sinks/loki/labels.py:7`), and checkpoint keys are already prefixed `org=<name>:` via `OrgCheckpointView`, so per-key commit ordering is unaffected by splitting a batch on org. The Loki push API takes the tenant per request, so per-tenant routing needs no new protocol support — only per-request header selection (multi-tenant Loki) or a per-org client when `url`/token differ (separate Grafana Cloud stacks).

## Why it matters

An MSP or ISV partner ingesting five customer orgs through one process lands all five customers' Salesforce security events (login, API usage, session, Apex) in one Loki tenant. Consequences:

- Tenant-level isolation is unavailable. Anyone with query access to that Loki tenant reads every customer's login and API events. Label-based access control on the `org` label is the only remaining control and it is not available on every Loki deployment.
- Per-customer retention, ingest limits, and per-tenant usage attribution are impossible — those are tenant-scoped in Loki, not label-scoped.
- The operator is forced back to N processes with N configs and N state volumes, which is exactly the cost #31 removed.

## Proposed approach

Add an optional per-org sink override that falls back to the shared sink, and dispatch batches by the `org` label inside the sink seam.

1. Config: new `OrgSinkOverride(StrictModel)` with `tenant_id: str | None`, `url: str | None`, `auth_token: SecretStr | None`, `auth_token_file: Path | None`; add `sink: OrgSinkOverride | None = None` to `OrgConfig` (`src/sf2loki/config.py:1298-1322`). Unset fields inherit from `sink.loki`, so `tenant_id`-only is the common case (one Loki, N tenants) and `url` + token is the separate-stack case. Everything else (encoding, compression, batch, egress, `structured_metadata_fields`, static `labels`) stays deployment-wide — do not fork those, they feed the pipeline and the governor, not the request.
2. Validation, as a model validator on `Config`: reject `orgs[].sink` when `orgs` is empty (single-org), and reject `auth_token` together with `auth_token_file` per override (mirror the existing `LokiConfig` secret-file resolution path so `*_file` reads go through the same loader).
3. Sink: add `src/sf2loki/sinks/loki/fanout.py` with a `FanOutSink` implementing the `Sink` protocol (`src/sf2loki/sinks/base.py:33-36`), holding `{org_name: LokiSink}` plus a default `LokiSink`. `push()` partitions `batch.entries` on `entry.labels.get("org")` (order-preserving, one dict pass), then awaits each sub-batch's `LokiSink.push` sequentially. Entries with no `org` label, or an `org` value with no configured entry, go to the default sink — a mapped-org-without-config situation cannot occur because org names come from the same config list, so assert it at build time rather than at push time. `aclose()` closes every distinct `httpx.AsyncClient`. Reuse one shared client for overrides that do not change `url` (headers are per-request), and create a client per distinct `url`.
4. Error semantics: a `PermanentSinkError` from one sub-batch must not discard the others — catch it per sub-batch, count `loki_entries_dropped` with the existing reason tag, and continue, matching the 413-split behaviour at `src/sf2loki/sinks/loki/sink.py:263-288`. A `RetryableSinkError` from any sub-batch propagates after the remaining sub-batches have been attempted, because the pipeline retries the whole batch (`src/sf2loki/app.py:456-470`). That re-sends already-delivered sub-batches; this is the existing at-least-once contract and Loki drops exact duplicate (stream, timestamp, line) entries, so document it rather than adding cross-call state.
5. Composition root: at `src/sf2loki/app.py:922`, build the fan-out sink when any org declares `sink`, otherwise keep constructing the plain `LokiSink` so single-org and shared-tenant multi-org wiring stay byte-identical. Leave `EgressGovernor` (`src/sf2loki/app.py:1031`) deployment-wide — per-org budgets are a separate change.
6. Observability: `sf2loki_loki_push` / `sf2loki_loki_bytes_pushed` (`src/sf2loki/obs/metrics.py:312-330`) stay unlabelled to avoid breaking the shipped dashboards and rule pack. Instead, include the org name in the auth-failure and drop log lines emitted from the per-org `LokiSink` (`src/sf2loki/sinks/loki/sink.py:290-300`), e.g. by passing an optional `label: str` to `LokiSink` used only in log context.
7. `doctor`: `_check_loki` (`src/sf2loki/doctor.py:358-385`) probes one sink. When per-org sinks are configured, probe the sink for the selected `--org` (doctor already operates on one org) and name it in the result message. `backfill` (`src/sf2loki/backfill.py:740`) likewise builds the selected org's sink.
8. Docs: replace the "one Loki tenant" statements at `README.md:342-347` and `docs/architecture.md:222`, extend `examples/presets/multi-org.yaml` with a commented per-org tenant example, and run `just gen-config` so `config.example.yaml` / `docs/config-reference.md` / the Helm chart's generated config stay drift-gate clean (`tests/test_config_artifacts_drift.py`).

---

Imported from GitHub issue #123 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 123)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `orgs[].sink` accepts `tenant_id` alone and inherits `url`/auth from `sink.loki`; unit test in `tests/test_config.py` asserts the resolved per-org sink settings.
- [ ] #2 Config rejects `orgs[].sink` when no `orgs` are configured, and rejects `auth_token` + `auth_token_file` together in one override; both pinned by `pytest.raises(ValidationError)` tests.
- [ ] #3 `auth_token_file` in an override is read through the same secret-file resolution as `sink.loki.auth_token_file`; test writes a temp token file and asserts the header value.
- [ ] #4 `FanOutSink.push` sends one request per distinct org, each with that org's `X-Scope-OrgID` (or Basic-auth username), verified with an `httpx.MockTransport` capturing requests: a batch of entries labelled `org=a`/`org=b` produces two requests with the two tenant ids and disjoint line sets.
- [ ] #5 Entries with no `org` label route to the default sink; test asserts one request with the shared tenant.
- [ ] #6 A `PermanentSinkError` on one org's sub-batch does not prevent the other orgs' sub-batches from being pushed, and increments `loki_entries_dropped` only for the failed sub-batch's entries.
- [ ] #7 A `RetryableSinkError` on one org's sub-batch propagates to the caller after the other sub-batches have been attempted; test asserts both requests were made and the exception surfaced.
- [ ] #8 Distinct `url` overrides get distinct clients and `aclose()` closes each exactly once (no double-close, no leak); test counts closes.
- [ ] #9 `App.build` produces a plain `LokiSink` when no org declares `sink`, and a `FanOutSink` when one does; test asserts the sink type for both configs.
- [ ] #10 Checkpoint commit still happens once per flush after all sub-pushes succeed; test asserts no checkpoint advances when one org's sub-batch raises `RetryableSinkError`.
- [ ] #11 `doctor` writes its probe line to the selected org's tenant when per-org sinks are configured; test asserts the request header.
- [ ] #12 `README.md` and `docs/architecture.md` no longer claim a single shared Loki tenant is the only option; `examples/presets/multi-org.yaml` shows the per-org tenant form.
- [ ] #13 `just gen-config` re-run and `just gate` green (ruff, mypy --strict, pytest), including the config-artifact drift gate.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
