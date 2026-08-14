---
id: SFL-0026
title: >-
  sources: multi-org org-id resolution re-runs a failing userinfo fetch per
  entry, silently — negative-cache it, log once, and resolve it in the startup
  probe
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/110'
ordinal: 26000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`OrgSource._resolve_org_id` (`src/sf2loki/sources/org_adapter.py:76-91`) caches only successful resolution and swallows every failure without a trace:

```python
if self._org_id:            # org_adapter.py:83-84 — success cache only
    return self._org_id
if self._org_id_provider is None:
    return ""
try:
    self._org_id = await self._org_id_provider()
except Exception:           # org_adapter.py:89-90 — no log, no negative cache, no cap
    return ""
```

It is awaited for **every** non-`checkpoint_only` entry, inside the `async for` over the inner source and before the `yield` (`org_adapter.py:109-118`). So while resolution keeps failing, every single ingested entry pays a fresh call to the provider, which in the app is `TokenProvider.org_id` (`src/sf2loki/app.py:1009`).

`TokenProvider.org_id` (`src/sf2loki/auth/jwt_auth.py:103-145`) also caches only on success — `self._org_id_cached` is assigned at `jwt_auth.py:135-136` and read at `:113-114`. With `salesforce.org_id` unset it performs a real `GET {instance_url}/services/oauth2/userinfo` per call. A 4xx is not retried and raises `AuthError` immediately (`jwt_auth.py:142`; `_should_retry` at `jwt_auth.py:30-36` retries only 5xx/transport; pinned by `tests/auth/test_jwt_auth.py:505-520`, which asserts `call_count == 1` for a 403). The client carries `read=30.0` / `connect=10.0` (`src/sf2loki/app.py:149`).

Nothing surfaces or prevents the failing state in multi-org mode:

- The multi-org startup probe `App._probe_orgs` awaits only `org.tokens.token()` (`src/sf2loki/app.py:1250-1257`). It never resolves the org id.
- The single-org path does the opposite — `org_id = self._cfg.salesforce.org_id or await self._tokens.org_id()` (`src/sf2loki/app.py:1136-1138`) — so a userinfo failure there is fatal at startup and exits nonzero.
- `org_id` is optional per org (`src/sf2loki/config.py:206-212`, `Config.resolved_orgs` at `config.py:1420-1435`); its own description states that leaving it null requires the `openid` scope.
- The `AuthError` never reaches `OrgSource`'s auth supervisor (`org_adapter.py:120-131`) because `_resolve_org_id`'s bare `except Exception` consumes it first, so not even that ERROR log fires.
- The only surface that detects it today is the opt-in `sf2loki doctor` preflight (`src/sf2loki/doctor.py:149-162`), which resolves the org id for one selected org only.

Entries keep flowing while resolution keeps failing, because the REST-polled sources never need the org id: `src/sf2loki/salesforce/soql_client.py:93` and `src/sf2loki/salesforce/eventlogfile_client.py:280` use `tokens.token()` only. EventLogFile yields one entry per CSV row (`src/sf2loki/sources/eventlogfile_source.py:699`), so a file drain of N rows issues N blocking userinfo requests inside the yield path.

**Scope correction — Pub/Sub is not affected.** `PubSubClient._metadata` itself awaits `self._tokens.org_id()` to build the `tenantid` gRPC header (`src/sf2loki/salesforce/pubsub_client.py:498-506`). With userinfo permanently failing the pubsub source cannot stream at all (its `AuthError` is caught and backed off by `org_adapter.py:120-131`), and once it does stream the `TokenProvider` cache is already warm, so the first `OrgSource` entry resolves from cache. The affected sources are the REST/SOQL-based ones: `eventlogfile`, `eventlog_objects`, `apexlog`, and any SOQL-polled object source.

The docstring at `org_adapter.py:77-82` documents the current behaviour as deliberate ("a transient failure just leaves it unresolved ... and retries on the next entry"), resting on the stated premise "an org whose auth is failing yields no entries". That premise does not hold for this failure mode: token minting succeeds on the `api` scope while userinfo resolution fails permanently, so the org is healthy for ingestion and broken for org-id resolution at the same time. The intended design covers a transient blip, not a permanent 4xx.

## Why it matters

Concrete reachable configuration: multi-org (`orgs:`), per-org `salesforce.org_id` left null, External Client App granted `api` but not `openid`.

- Startup passes. `_probe_orgs` mints tokens for every org and logs "authenticated to salesforce org". No warning, no degraded readiness, no ERROR anywhere.
- Every entry from every REST-polled source in every org pays a synchronous userinfo round-trip before it can be yielded. Per-lane throughput collapses to roughly one entry per Salesforce round-trip (order 100-300 ms), so an EventLogFile drain of 100k rows takes hours instead of minutes and hammers the org's OAuth endpoint with one request per row.
- Because emission is that slow, records can age past Loki's accept window during a large drain, turning a silent throughput problem into silent rejection.
- `sf_org_id` is absent from every entry (it is in the label allowlist, `src/sf2loki/config.py:553`), so multi-org dashboards and rules that slice by it silently see nothing.
- No log line anywhere explains either symptom. Diagnosis today requires reading `org_adapter.py` or independently running `doctor`.

The `Exception`-swallow also hides genuinely unexpected programming errors in the provider chain, not just auth failures.

## Proposed approach

1. **Negative-cache with time-based backoff in `OrgSource`.** Add module constants next to `_RETRY_BACKOFF_BASE`/`_RETRY_BACKOFF_MAX` (`org_adapter.py:49-51`), e.g. `_ORG_ID_RETRY_INTERVAL = 300.0`, and instance state `self._org_id_next_attempt: float = 0.0`. Accept an injectable clock on `__init__` (`now: Callable[[], float] = time.monotonic`) so the backoff is testable without sleeping. In `_resolve_org_id`: return `""` immediately when `now() < self._org_id_next_attempt`; on failure set `self._org_id_next_attempt = now() + _ORG_ID_RETRY_INTERVAL`; on success clear it. Resolution stays best-effort and self-healing but stops being a per-entry network call.
2. **Log the failure, once per backoff window,** at WARNING with `org=`, `source=`, `error=str(exc)`, and the fact that `sf_org_id` will be omitted until it resolves. Narrow the `except` to `Exception` but log rather than pass silently; log at INFO when resolution later succeeds so recovery is visible.
3. **Resolve the org id in the multi-org startup probe** so the misconfiguration surfaces at boot, mirroring the single-org path (`app.py:1136-1138`). In `_probe` (`app.py:1250-1257`), after `await org.tokens.token()` succeeds and only when that org's `salesforce.org_id is None`, `await org.tokens.org_id()` and on `AuthError` log ERROR naming the likely cause (userinfo needs the `openid` scope; setting `orgs[].salesforce.org_id` avoids it entirely) — and log the resolved id on success.
   - **Trap to avoid:** do NOT add a userinfo-failing org to `self._degraded_orgs`. The readiness predicate `_org_auth_degraded_check` (`app.py:842-858`) clears only when `tokens.has_token()` is false, and such an org *does* hold a token, so the deployment would be pinned unready forever. Keep the existing token-only semantics for `_degraded_orgs` and all-fail fail-fast (`app.py:1276-1277`); the org-id resolution failure is log-only.
4. Document the `openid`-scope dependency and the "set `org_id` per org to stay on the `api` scope alone" escape hatch in the multi-org docs page alongside the existing `config.py:206-212` field description.

---

Imported from GitHub issue #110 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 110)' archive/issues-dump.json`).

## Scope note

The provider-side half of this failure mode — `org_id()` never invalidating the cached token on an auth failure, and being lock-free — is tracked separately in #109. This issue owns the adapter-side behaviour (per-entry retry storm, silence, and startup-probe resolution); fix both for the full remediation.

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `OrgSource` holds a negative-result cache with a time-based retry interval and an injectable clock; `_resolve_org_id` short-circuits to `""` without calling the provider while inside the backoff window.
- [ ] #2 The first failure in each window logs once at WARNING with `org`, `source`, and the error string; repeated entries inside the window log nothing.
- [ ] #3 A later successful resolution populates `sf_org_id` on subsequent entries and logs the recovery at INFO.
- [ ] #4 `App._probe_orgs` resolves the org id for every org whose `salesforce.org_id` is unset, logs ERROR (with the `openid`-scope hint) on `AuthError`, and does **not** mark that org degraded or abort startup when its token minted.
- [ ] #5 `tests/sources/test_org_adapter.py`: a counting provider that always raises is invoked **once** while draining an inner source of 50 entries, all 50 entries are yielded, and none carries `sf_org_id` (extends the existing `test_sf_org_id_omitted_when_resolution_fails` at `tests/sources/test_org_adapter.py:96-108`).
- [ ] #6 `tests/sources/test_org_adapter.py`: with the injected clock advanced past `_ORG_ID_RETRY_INTERVAL`, the provider is invoked a second time and, on success, later entries carry `sf_org_id` while earlier ones do not.
- [ ] #7 `tests/sources/test_org_adapter.py`: the WARNING is emitted exactly once across a multi-entry drain with a permanently failing provider (assert via captured structlog events).
- [ ] #8 `tests/test_multiorg_app.py`: multi-org startup with a token that mints and a userinfo endpoint returning 403 completes startup successfully, logs the ERROR naming the org, leaves readiness undegraded, and leaves the org's sources running.
- [ ] #9 `tests/test_multiorg_app.py`: multi-org startup with per-org `org_id` set performs no userinfo request at all (assert the mocked userinfo route has zero calls).
- [ ] #10 `just gate` green (ruff + `mypy --strict` + pytest).
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
