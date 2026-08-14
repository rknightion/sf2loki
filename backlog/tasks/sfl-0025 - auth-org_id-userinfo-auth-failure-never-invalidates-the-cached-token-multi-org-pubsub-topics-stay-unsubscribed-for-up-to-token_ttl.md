---
id: SFL-0025
title: >-
  auth: org_id() userinfo auth failure never invalidates the cached token -
  multi-org pubsub topics stay unsubscribed for up to token_ttl
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/109'
ordinal: 25000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`TokenProvider.org_id()` is the only authenticated Salesforce call in the codebase that does not react to a token rejection by invalidating the cached token and retrying with a fresh one.

`src/sf2loki/auth/jwt_auth.py:103-145`:

- The token is captured once, before the fetch: `tok = await self.token()` at `jwt_auth.py:116`, and `_fetch()` closes over `tok.value` (`jwt_auth.py:119-127`).
- The shared retry policy retries transport errors and 5xx only — `_should_retry` at `jwt_auth.py:30-36`, `_retry_policy` at `jwt_auth.py:216-224`. Tenacity re-raises the original exception when the predicate declines, so a 4xx lands in `except _TokenEndpointError` at `jwt_auth.py:140-142` and is converted to `AuthError` with **no** `self.invalidate()`.
- `invalidate()` (`jwt_auth.py:147-149`) is the only way the token cache is cleared reactively. `_is_valid` (`jwt_auth.py:163-168`) judges freshness purely on the locally computed `expires_at`, which `_request_token` sets to `now + token_ttl` because neither the JWT-bearer nor the client_credentials response carries `expires_in` (`jwt_auth.py:205-214`). That code's own comment states the real lifetime is the org session timeout and that correctness "rel[ies] on reactive invalidate()-on-401 for orgs with shorter timeouts" — `token_ttl` default is 1h (`src/sf2loki/config.py:213-221`), org session timeout can be 15m.

Consequence: once a token is dead server-side but still locally "valid", every subsequent `org_id()` call re-presents the same dead token and fails identically until local expiry (`token_ttl` minus the 60s `_REFRESH_SKEW`).

Every other client does the right thing: `src/sf2loki/salesforce/soql_client.py:104-109`, `limits_client.py:44-48`, `metadata_client.py:40-43`, `eventlogfile_client.py:289-292`, `apexlog_client.py:190-193`, and the gRPC path `pubsub_client.py:441-442` (invalidate on `UNAUTHENTICATED`).

`org_id()` is also lock-free. `self._lock` (`jwt_auth.py:78`) guards minting only, so N concurrent first-time callers each issue their own userinfo GET; there is one `_metadata()` call per Pub/Sub topic task (`src/sf2loki/salesforce/pubsub_client.py:498-506`).

**Scope — multi-org only.** Single-org resolves `org_id` eagerly during the startup probe with a token minted milliseconds earlier: `await self._tokens.token()` then `org_id = self._cfg.salesforce.org_id or await self._tokens.org_id()` (`src/sf2loki/app.py:1136-1138`), on the same provider instance the sources hold (`tokens=org_auths[0].tokens`, `app.py:1085`). `_org_id_cached` is then permanent (`jwt_auth.py:113-114`), so no later single-org call touches the network. Multi-org's `_probe_orgs` mints per-org tokens but never resolves `org_id` (`app.py:1240-1265`), leaving resolution lazy for every org.

## Why it matters

Failure walk-through, multi-org config with `sources.pubsub.enabled: true` on an org whose `salesforce.org_id` is unset (the documented default, `docs/config-reference.md:29`):

1. `_probe_orgs` mints that org's token at T0 (`app.py:1250-1258`). No userinfo call.
2. The first `_metadata()` is delayed past the org's session timeout. Two real windows:
   - the pre-subscribe checkpoint-load retry loop in `src/sf2loki/sources/pubsub_source.py:487-500` (a state-store outage backs off and retries before any subscribe);
   - HA active-passive promotion — the pipeline starts only in `on_acquire` (`app.py:1184-1196`), so a standby promoted between session-timeout and `token_ttl - 60s` after process start carries a dead-but-locally-valid token.
3. `PubSubClient.subscribe` awaits `_metadata()` at `pubsub_client.py:311`; `org_id()` GETs userinfo with the dead bearer, gets a 4xx, raises `AuthError`.
4. `AuthError` is not a `grpc.aio.AioRpcError`, so `_handle_rpc_error` / invalidate-on-`UNAUTHENTICATED` (`pubsub_client.py:441-442`) never fires. It escapes into `_run_topic`'s generic handler (`pubsub_source.py:580`), which logs `pubsub stream error` and backs off (`pubsub_source.py:643-648`).
5. Retry re-enters `_metadata()`; `token()` returns the same locally-valid dead token; identical 4xx. The loop persists until the token passes local expiry — up to ~59 min with defaults — during which no topic on that org subscribes. With `replay_preset: LATEST` and no previously stored replay id, events in that window are lost outright; with a stored replay id, Pub/Sub replay retention recovers them and the cost is ingest latency.

Aggravating, same function: `OrgSource._resolve_org_id` (`src/sf2loki/sources/org_adapter.py:84-91`) swallows the exception and returns `""` **without negative caching**, and it is invoked per entry (`org_adapter.py:111`). A persistently failing userinfo (for example the `openid` scope not granted, which `org_id()` requires) therefore costs one extra Salesforce REST call per ingested event, indefinitely, with only a silently missing `sf_org_id` label as the symptom.

## Proposed approach

In `src/sf2loki/auth/jwt_auth.py`:

1. Classify auth rejections on the userinfo response. Treat HTTP 401 as an auth rejection, and also HTTP 403 whose body carries Salesforce's dead-session markers (`Bad_OAuth_Token`, `INVALID_SESSION_ID`) — the userinfo endpoint returns 403 for a bad token in some org configurations. A bare 403 without a marker (a genuinely missing scope) must keep failing fast, otherwise the retry doubles every call for a permanent misconfiguration.
2. On an auth rejection inside `org_id()`: call `self.invalidate()`, re-mint via `await self.token()`, rebuild the `Authorization` header from the new token, and retry the fetch **exactly once**. A second rejection raises `AuthError` as today. Mirror `SoqlClient.query` (`src/sf2loki/salesforce/soql_client.py:104-109`). Note the closure at `jwt_auth.py:119-127` binds `tok` — the retry must re-read the token, so pass it in or rebind.
3. Single-flight the resolution: wrap the cache check plus fetch in a lock (reuse `self._lock` only if the nested `token()` call is restructured to avoid self-deadlock, since `token()` acquires it at `jwt_auth.py:96` — a dedicated `self._org_id_lock` is the simpler, safer option), with the standard double-check inside the lock so concurrent per-topic callers issue one userinfo GET.
4. In `src/sf2loki/sources/org_adapter.py:84-91`, stop retrying per entry on failure: record the last failure time and skip re-resolution until a short backoff has elapsed (reuse the module's `_RETRY_BACKOFF_BASE`/`_RETRY_BACKOFF_MAX` shape), so a permanently failing userinfo costs at most one call per backoff interval rather than one per event.

Existing test `tests/auth/test_jwt_auth.py:506-520` mocks a plain `403 {"error": "forbidden"}` and asserts one call; a marker-scoped fix leaves it green, and it should stay as the regression guard for the missing-scope case.

---

Imported from GitHub issue #109 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 109)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `org_id()` calls `invalidate()`, re-mints, and retries the userinfo fetch exactly once on a 401 (and on a 403 carrying `Bad_OAuth_Token`/`INVALID_SESSION_ID`).
- [ ] #2 A second consecutive auth rejection still raises `AuthError`; no unbounded retry loop is introduced.
- [ ] #3 A bare 403 with no dead-session marker still fails fast with a single call (`tests/auth/test_jwt_auth.py:506-520` unchanged and green).
- [ ] #4 Concurrent first-time `org_id()` callers produce exactly one userinfo GET.
- [ ] #5 `OrgSource._resolve_org_id` does not re-attempt resolution on every entry after a failure; a bounded backoff gates retries.
- [ ] #6 Test: userinfo returns 401 then 200 with a second token minted — `org_id()` returns the resolved id, the token endpoint was hit twice, userinfo was hit twice, and the second userinfo request carried the new bearer value (respx, asserting the `Authorization` header of each recorded request).
- [ ] #7 Test: userinfo returns 401 twice — `AuthError` is raised and the token endpoint was hit exactly twice (one initial mint plus one re-mint).
- [ ] #8 Test: `asyncio.gather` of several `org_id()` calls on one provider with `cfg.org_id` unset yields one userinfo call and identical results.
- [ ] #9 Test: with a mocked always-401 userinfo and a fake clock/patched `datetime`, the second `org_id()` attempt after the token is invalidated uses a freshly minted token rather than the cached one — pins that a dead-token wedge cannot span `token_ttl`.
- [ ] #10 Test: `OrgSource` with an `org_id_provider` that always raises attempts resolution far fewer times than the number of entries yielded (bounded by the backoff, not by the entry count).
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
