# src/sf2loki/auth

`salesforce.auth_mode` selects `jwt_bearer` (default) or `client_credentials`. Both resolve through
the same `TokenProvider` / `AccessToken` shape, so no downstream client (Pub/Sub, REST/SOQL,
EventLogFile, Tooling) is affected by which mode is active. `docs/getting-started.md` covers the
External Client App setup, licences and security toggles.

## Traps

- The JWT bearer flow never issues or uses a refresh token: on expiry or a downstream 401 it
  re-mints a fresh JWT and re-requests from scratch. Salesforce's pre-authorized JWT bearer path
  nevertheless **requires** the `refresh_token` (offline_access) scope on the connected app, or the
  grant fails with `invalid_request: "refresh_token scope is required..."`. A Salesforce quirk, not
  a sign this flow uses refresh tokens.
- A caller that hits a downstream 401 calls `TokenProvider.invalidate()` so the next `token()`
  re-authenticates. Retrying with the cached token just replays a token Salesforce already rejected.
- `client_credentials` takes its identity from the External Client App's **Run As** user, not from
  `salesforce.username` (which is the JWT `sub` claim and is required only for `jwt_bearer`).
- Setting `salesforce.org_id` skips the `/services/oauth2/userinfo` round-trip entirely and keeps
  the app on the `api` scope alone. Left unset it resolves on first use, needing `openid`, and
  caches thereafter.
