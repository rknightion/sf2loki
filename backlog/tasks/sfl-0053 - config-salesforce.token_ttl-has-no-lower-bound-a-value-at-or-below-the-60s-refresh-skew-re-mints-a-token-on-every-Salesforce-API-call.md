---
id: SFL-0053
title: >-
  config: salesforce.token_ttl has no lower bound - a value at or below the 60s
  refresh skew re-mints a token on every Salesforce API call
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-3
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/137'
ordinal: 53000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`salesforce.token_ttl` (`src/sf2loki/config.py:213-221`) is a plain `Duration` field with a 1h default and no lower bound:

```python
token_ttl: Duration = Field(
    default=timedelta(hours=1),
    description=(...),
)
```

The `Duration` alias (`src/sf2loki/config.py:64-79`) parses Go-style shorthand and otherwise falls through to pydantic's own timedelta parsing, so `60s`, `30s`, `1s` and a bare negative numeric (`token_ttl: -300`) are all accepted. `SalesforceConfig._resolve_login_url_and_validate_mode` (`src/sf2loki/config.py:226-252`) validates only `login_url`/`username`/`auth_mode`; nothing in `src/` other than `src/sf2loki/auth/jwt_auth.py:213` reads the field, and `src/sf2loki/doctor.py` never inspects it.

`TokenProvider` treats the configured TTL as the token's assumed lifetime and applies a fixed proactive-refresh skew:

- `src/sf2loki/auth/jwt_auth.py:23` - `_REFRESH_SKEW: timedelta = timedelta(seconds=60)`
- `src/sf2loki/auth/jwt_auth.py:213` - `expires_at=datetime.now(UTC) + self._cfg.token_ttl`
- `src/sf2loki/auth/jwt_auth.py:164-168` - `return datetime.now(UTC) < token.expires_at - _REFRESH_SKEW`

When `token_ttl <= _REFRESH_SKEW`, `expires_at - _REFRESH_SKEW` is at or before the mint instant, so a token is invalid the moment it is created. Both validity checks in `token()` fail every time - the lock-free fast path (`src/sf2loki/auth/jwt_auth.py:92-95`) and the double-check inside the lock (`src/sf2loki/auth/jwt_auth.py:97-100`) - so every call runs `_mint_token()` and POSTs `/services/oauth2/token`. The cache is effectively disabled and no error or warning is emitted.

`token()` is on every Salesforce request path, so the extra mint is per-request, not per-refresh-interval:

- `src/sf2loki/salesforce/soql_client.py:93`, `:107` - every SOQL page
- `src/sf2loki/salesforce/eventlogfile_client.py:280`, `:291` - every ELF listing and blob download
- `src/sf2loki/salesforce/apexlog_client.py:175`, `:192` - every ApexLog body fetch
- `src/sf2loki/salesforce/limits_client.py:39`, `:46` - every limits poll
- `src/sf2loki/salesforce/metadata_client.py:36`, `:42` - channel discovery
- `src/sf2loki/salesforce/pubsub_client.py:500` - every gRPC metadata build
- `src/sf2loki/auth/jwt_auth.py:116` - `org_id()` resolution

A second consequence: `has_token()` (`src/sf2loki/auth/jwt_auth.py:152-157`) delegates to the same `_is_valid`, so it is permanently `False`. The multi-org readiness predicate `_org_auth_degraded_check` (`src/sf2loki/app.py:842-858`) uses it to decide when a startup-probe-failed org has recovered, so readiness stays pinned at `degraded: org <name> auth failing` forever even once that org is authenticating normally.

## Why it matters

`README.md:246-252` explicitly directs operators at this field: a short org session timeout shows up as reconnect churn, and the documented remedy is to raise the integration user's session timeout and "set `salesforce.token_ttl` to match". That makes a sub-minute value a realistic slip - a units mistake (`60s` intended as 60 minutes, `15` intended as 15 minutes but parsed as 15 seconds by pydantic's numeric-seconds path), or deliberately matching an aggressively short session policy.

The resulting failure mode, with a busy ELF drain or SOQL poll running:

1. Every Salesforce HTTP/gRPC call is preceded by a fresh OAuth token mint, continuously, for the life of the process. Sustained token-endpoint traffic proportional to API call volume risks Salesforce-side throttling or blocking of the External Client App - a connector-wide outage rather than a slow connector.
2. Every mint holds the single `asyncio.Lock` (`src/sf2loki/auth/jwt_auth.py:76`, `:97`), so all concurrent Salesforce work across all sources serializes behind one network round-trip. Bounded-concurrency ELF processing degrades to sequential.
3. `sf2loki_auth_refreshes` (incremented at `src/sf2loki/auth/jwt_auth.py:236`) climbs at request rate, which reads as the documented "short session timeout" symptom in `README.md:248-252` and misdirects diagnosis away from the config value.
4. Under multi-org, readiness never recovers from a transient startup auth failure.

No validation error, no startup warning, no log line distinguishes this from healthy operation. Data correctness is unaffected (at-least-once still holds), which is why this is low severity rather than higher - the cost is API burn, throughput collapse, and a misleading metric signature.

## Proposed approach

Two complementary changes; both are wanted, since the first prevents the misconfiguration and the second keeps the provider well-behaved regardless of what TTL reaches it.

1. **Reject the value at config load.** Add an explicit check in `SalesforceConfig` so the failure is loud and self-explanatory rather than a bare pydantic constraint message. The refresh skew lives in the auth module, so export it rather than duplicating the literal: rename `_REFRESH_SKEW` to a public `REFRESH_SKEW` in `src/sf2loki/auth/jwt_auth.py:23` (keeping a module-private alias is unnecessary - update the three internal uses), or define the bound as a module constant in `config.py` and have `jwt_auth.py` import it. Importing `config` from `auth` already happens (`src/sf2loki/auth/jwt_auth.py:17`), so the constant belongs in `config.py` to avoid an import cycle. Require `token_ttl >= 2 * skew` (2 minutes) in the existing `@model_validator(mode="after")` at `src/sf2loki/config.py:226`, with a message naming the value, the bound, and the reason ("must exceed the 60s proactive-refresh skew, else every request re-mints"). Update the field `description` to state the minimum.

2. **Make `_is_valid` degrade gracefully.** Use an effective skew of `min(REFRESH_SKEW, token_ttl / 4)` so any positive TTL still yields a token that is usable for most of its life. This requires `_is_valid` to stop being a `@staticmethod` (`src/sf2loki/auth/jwt_auth.py:163`) and become an instance method reading `self._cfg.token_ttl`; update the three call sites (`src/sf2loki/auth/jwt_auth.py:93`, `:98`, `:157`). `has_token()` then also behaves correctly, which is what unpins `_org_auth_degraded_check`.

Because the field `description` changes, `just gen-config` must be re-run to refresh `config.example.yaml`, `docs/config-reference.md` and `deploy/helm/values.yaml`, or the drift gate in `tests/test_config_artifacts_drift.py` fails. `README.md:246-252` should gain a sentence naming the 2-minute floor next to the "set `salesforce.token_ttl` to match" advice, so the tuning guidance and the constraint agree.

---

Imported from GitHub issue #137 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 137)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `salesforce.token_ttl` below the floor (`2 * REFRESH_SKEW`, i.e. 2 minutes) fails config validation with a message naming the offending value and the reason, covering shorthand (`60s`, `30s`), bare numeric seconds (`30`), zero, and negative.
- [ ] #2 `token_ttl: 2m` and above load successfully; the 1h default and `"15m"` shorthand still parse (existing `tests/test_config.py:699-706` stay green).
- [ ] #3 `TokenProvider._is_valid` uses an effective skew of `min(REFRESH_SKEW, token_ttl / 4)`; a token minted with any positive TTL is reported valid immediately after minting.
- [ ] #4 `has_token()` returns `True` immediately after a successful mint for any positive `token_ttl`, so `_org_auth_degraded_check` (`src/sf2loki/app.py:842-858`) clears once a previously-failed org authenticates.
- [ ] #5 `tests/test_config.py`: parametrized rejection test over the invalid values above, plus an accepted-boundary case at exactly 2 minutes.
- [ ] #6 `tests/auth/test_jwt_auth.py`: test that two back-to-back `await token()` calls with a short-but-legal TTL issue exactly ONE POST to the token endpoint (assert on the mock transport's request count, not on `auth_refreshes` alone), pinning that the cache fast path is reachable. Add the mirror assertion that `has_token()` is `True` straight after the first mint.
- [ ] #7 `tests/auth/test_jwt_auth.py`: existing expiry test (`:435-460`) still pins `expires_at == mint + token_ttl` - the fix changes the validity window, not the recorded expiry.
- [ ] #8 `just gen-config` re-run; `config.example.yaml`, `docs/config-reference.md` and `deploy/helm/values.yaml` reflect the new description and `tests/test_config_artifacts_drift.py` is green.
- [ ] #9 `README.md:246-252` states the 2-minute minimum alongside the session-timeout tuning advice.
- [ ] #10 `just gate` green.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
