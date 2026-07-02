# CLAUDE.md — src/sf2loki/auth

`jwt_auth.py` — the sole client of Salesforce OAuth, shared by every client in
`../salesforce/`. See docs/getting-started.md for the full setup/scopes walkthrough
(External Client App config, licences, security toggles).

## Two auth modes, one `TokenProvider`
`salesforce.auth_mode` selects between:
- **`jwt_bearer`** (default) — mint an RS256 JWT (`iss`=consumer key,
  `sub`=integration username, `exp`≈+3min), signed with the private key, POST
  it for an access token. **No refresh token is ever issued or used** — on
  expiry or a downstream 401, re-mint a fresh JWT and re-request from scratch.
  Despite that, Salesforce's pre-authorized JWT bearer path still **requires**
  the `refresh_token` (offline_access) OAuth scope on the connected app, or the
  grant fails with `invalid_request: "refresh_token scope is required..."` —
  a Salesforce quirk, not a sign this flow uses refresh tokens.
- **`client_credentials`** — consumer key + secret, no keypair; identity comes
  from the External Client App's **Run As** user. Otherwise flows through the
  same `TokenProvider`/`AccessToken` shape, so every downstream caller
  (Pub/Sub, REST/SOQL, ELF, Tooling) is unaffected by which mode is active.

## Reactive + proactive refresh
`TokenProvider.token()` refreshes proactively before expiry; callers that hit
a downstream 401 should call `TokenProvider.invalidate()` so the next `token()`
re-authenticates rather than replaying a token Salesforce already rejected —
don't just retry with the cached token.

## `org_id()` is a separate, optional resolution
Set `salesforce.org_id` in config to skip the `/services/oauth2/userinfo` call
entirely (and drop the `openid` scope). Leaving it unset triggers a userinfo
round-trip on first use, cached thereafter.
