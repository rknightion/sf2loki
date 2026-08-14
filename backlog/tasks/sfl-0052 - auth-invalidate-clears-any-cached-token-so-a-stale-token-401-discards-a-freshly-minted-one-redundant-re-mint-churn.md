---
id: SFL-0052
title: >-
  auth: invalidate() clears any cached token, so a stale-token 401 discards a
  freshly minted one (redundant re-mint churn)
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/136'
ordinal: 52000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`TokenProvider.invalidate()` (`src/sf2loki/auth/jwt_auth.py:147-149`) is an unconditional `self._cached = None`:

```python
def invalidate(self) -> None:
    """Clear the cached token (call when a downstream caller receives a 401)."""
    self._cached = None
```

It takes no argument, so it cannot tell whether the token that actually failed is the one currently cached. Every caller invokes it unconditionally on any auth rejection:

- `src/sf2loki/salesforce/soql_client.py:106` (401 on query/pagination)
- `src/sf2loki/salesforce/eventlogfile_client.py:290` (401 on LogFile download)
- `src/sf2loki/salesforce/apexlog_client.py:191` (401 on ApexLog body download)
- `src/sf2loki/salesforce/limits_client.py:45`
- `src/sf2loki/salesforce/metadata_client.py:41`
- `src/sf2loki/salesforce/pubsub_client.py:433-442` (`_handle_rpc_error`, on gRPC `UNAUTHENTICATED`)

One `TokenProvider` is shared per org across all of those clients: it is built at `src/sf2loki/app.py:986` and passed into `_build_org_sources` (`src/sf2loki/app.py:691-696`), which hands the same instance to `PubSubClient`, every `SoqlClient`, the EventLogFile and ApexLog clients, plus `LimitsClient` (`src/sf2loki/app.py:1022`) and `MetadataClient`. A clear from any one caller therefore affects all of them.

The asymmetry that turns this into a defect: a Pub/Sub `Subscribe` stream presents the token it was created with for its entire lifetime. `subscribe()` passes `metadata=await self._metadata()` exactly once at stream creation (`src/sf2loki/salesforce/pubsub_client.py:311`), and `_metadata()` embeds `tok.value` (`src/sf2loki/salesforce/pubsub_client.py:498-506`). gRPC call metadata is fixed for the call's lifetime, so when that stream dies `UNAUTHENTICATED` hours later, `_handle_rpc_error` clears whatever is cached *now* — typically a newer token minted by some other handler and still in use by the REST clients.

Nothing upstream prevents this state:

- The `asyncio.Lock` + in-lock double-check in `token()` (`src/sf2loki/auth/jwt_auth.py:96-101`) only defuses the *simultaneous* case: if every handler calls `invalidate()` before the first mint completes, the extra calls are no-ops on an already-`None` cache and the rest get the fresh token from the double-check at `src/sf2loki/auth/jwt_auth.py:98`. It does nothing for staggered failures, which is the realistic pattern — independent per-topic streams error at different times (each on its own next server write or stall-watchdog fire), and the SOQL/ELF/ApexLog/limits pollers run on independent intervals.
- `_is_valid()` (`src/sf2loki/auth/jwt_auth.py:163-168`) checks only `expires_at`, which is fabricated as `now + token_ttl` (`src/sf2loki/auth/jwt_auth.py:210-214`) because neither the JWT-bearer nor the `client_credentials` response carries `expires_in`. It cannot distinguish "the cache is the dead token" from "the cache is a newer live token".

## Why it matters

Org-wide session expiry is the documented steady state, not an edge case: the access token's real lifetime is the org session timeout, which can be as short as 15 minutes against a default `salesforce.token_ttl` of 1h (`docs/config-reference.md:30`, `docs/troubleshooting.md:14-20`). So every session-timeout cycle produces a wave of auth failures across a shared `TokenProvider`.

Walk-through with three Pub/Sub topics plus one SOQL poller on one org:

1. `t=0` — streams A/B/C created, all carrying token T1 (`src/sf2loki/salesforce/pubsub_client.py:311`).
2. `t=15m` — the server-side session dies. Stream A errors first: `_handle_rpc_error` → `invalidate()` → the source reconnects → `_metadata()` → mint T2.
3. `t=15m+30s` — stream B errors, still holding T1. `invalidate()` clears T2, which is valid and in use. B mints T3. A's already-established stream keeps working on T2 (its metadata is frozen), so nothing breaks — but the cache churned for no reason.
4. Stream C errors → clears T3 → mints T4.
5. The `eventlog_objects` SOQL poll that fetched a token mid-churn gets a 401 → `src/sf2loki/salesforce/soql_client.py:106` → clears T4 → mints T5.

Five token-endpoint POSTs where one suffices, scaling with topic count plus poller count, recurring on every session-timeout cycle. Under `auth_mode: client_credentials` each redundant mint also creates a fresh server-side Salesforce session (`src/sf2loki/auth/jwt_auth.py:189-195`), so the churn consumes session capacity for the Run As user rather than just an API call.

Secondary effect, same root cause: `has_token()` (`src/sf2loki/auth/jwt_auth.py:151-157`) reads the same cache, and `_org_auth_degraded_check` (`src/sf2loki/app.py:842-858`) degrades readiness while a startup-failed org's provider reports no token. A late stale-token `invalidate()` flips an already-recovered degraded org back to not-ready until the next `token()` call, so `/readyz` flaps for reasons unrelated to that org's health.

No data loss and no hot loop: every request path calls `token()`, which mints when the cache is `None`, so no request ever goes out unauthenticated. This is bounded waste plus session churn plus readiness flap.

## Proposed approach

Make invalidation compare-and-clear against the token that actually failed.

1. Change the signature to `invalidate(self, failed: AccessToken | None = None) -> None` in `src/sf2loki/auth/jwt_auth.py:147`. With `failed=None`, keep today's unconditional clear (used by tests and any caller that genuinely has no token in hand). With `failed` supplied, clear only when `self._cached is None or self._cached.value == failed.value`. Keeping the parameter optional avoids touching the fake `TokenProvider` stand-ins across the test suite in one sweep.
2. Pass the failed token at every REST call site — each already holds it in a local `tok`: `src/sf2loki/salesforce/soql_client.py:106`, `src/sf2loki/salesforce/eventlogfile_client.py:290`, `src/sf2loki/salesforce/apexlog_client.py:191`, `src/sf2loki/salesforce/limits_client.py:45`, `src/sf2loki/salesforce/metadata_client.py:41`.
3. Pub/Sub needs the token threaded through, because `_handle_rpc_error` currently has no access to it. Have `_metadata()` (`src/sf2loki/salesforce/pubsub_client.py:498-506`) return the `AccessToken` alongside the metadata list (or add a sibling helper that returns both), keep that token in a local for the lifetime of `subscribe()` / `get_schema()` / `get_topic()`, and give `_handle_rpc_error(exc, failed)` the token the failing RPC was created with. For `subscribe()` this is the token captured at `src/sf2loki/salesforce/pubsub_client.py:311`, held across the whole stream so the hours-later error compares against the right value.
4. Accept one residual, and note it in the docstring: if the cached token is *newer* than the failed one but also already dead (minted shortly before the session was killed), compare-and-clear skips the clear and that caller burns its single retry. It self-heals on the next cycle, because whichever caller holds the newer token will itself see a 401 whose value matches the cache and will clear it. Do not try to fix this with a mint-generation counter — a newer generation is not evidence of liveness, so it changes nothing here.
5. Update the `invalidate()` docstring and the reactive-refresh paragraph in `src/sf2loki/auth/CLAUDE.md` to state the contract: pass the token you were rejected on; the provider clears only if that token is still the cached one.

---

Imported from GitHub issue #136 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 136)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `TokenProvider.invalidate(failed: AccessToken | None = None)` clears the cache only when `failed is None` or `failed.value` equals the cached token's value (`src/sf2loki/auth/jwt_auth.py:147`).
- [ ] #2 All six call sites pass the token they were rejected on: `soql_client.py:106`, `eventlogfile_client.py:290`, `apexlog_client.py:191`, `limits_client.py:45`, `metadata_client.py:41`, `pubsub_client.py` `_handle_rpc_error`.
- [ ] #3 `PubSubClient.subscribe()` retains the `AccessToken` used to create the stream and passes it to `_handle_rpc_error` on `UNAUTHENTICATED`; `get_schema()` and `get_topic()` do the same for their own RPCs.
- [ ] #4 Test: `invalidate(stale_token)` where the cache holds a different, newer token leaves the cache intact — a following `token()` returns the newer token and makes no HTTP request (`respx` route `call_count` unchanged). In `tests/auth/test_jwt_auth.py`.
- [ ] #5 Test: `invalidate(current_token)` where the passed token IS the cached one clears it — the next `token()` re-requests (extends the existing `test_invalidate_forces_re_request`, `tests/auth/test_jwt_auth.py:198-211`).
- [ ] #6 Test: `invalidate()` with no argument still clears unconditionally (back-compat for existing fakes).
- [ ] #7 Test: a Pub/Sub stream created with token T1, where the cache has since moved to T2, does not clear T2 when the stream dies `UNAUTHENTICATED` — assert against a real `TokenProvider` (not the counting fake at `tests/salesforce/test_pubsub_client.py:94-103`) so the cache identity is observable, while the existing `invalidate_calls` assertions at `tests/salesforce/test_pubsub_client.py:488-545` and `:912-942` keep passing.
- [ ] #8 Test: end-to-end churn wave — one shared `TokenProvider`, several staggered stale-token 401s from different clients, exactly one token-endpoint POST after the first re-mint (asserts the redundant-mint fix).
- [ ] #9 Test: a stale-token `invalidate()` does not flip `has_token()` to `False` for an org that has already recovered, so `_org_auth_degraded_check` (`src/sf2loki/app.py:842-858`) stays ready.
- [ ] #10 `src/sf2loki/auth/CLAUDE.md` reactive-refresh section documents the pass-the-failed-token contract and the one-wasted-retry residual.
- [ ] #11 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
