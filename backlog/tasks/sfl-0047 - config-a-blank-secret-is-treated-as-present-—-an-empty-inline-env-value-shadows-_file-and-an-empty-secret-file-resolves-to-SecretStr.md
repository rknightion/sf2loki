---
id: SFL-0047
title: >-
  config: a blank secret is treated as present — an empty inline/env value
  shadows *_file and an empty secret file resolves to SecretStr('')
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-5
milestone: m-0
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/131'
ordinal: 47000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`_resolve_secret_file` (`src/sf2loki/config.py:1477-1495`) treats a present-but-empty secret as a real secret, in two ways:

1. **An empty inline value shadows the `*_file` path.** The precedence check is `if existing is not None: return existing` (`config.py:1480-1481`). `SecretStr('')` satisfies `is not None`, so the file is never opened. A YAML `client_secret: ${SF_SECRET}` with `SF_SECRET` exported as an empty string interpolates to `''` (`_interpolate_env`, `config.py:91-105`, only raises for an *undefined* variable) and pydantic coerces it to `SecretStr('')`. Same for `SF2LOKI_SALESFORCE__CLIENT_SECRET=""` via the env source.

2. **An empty or whitespace-only secret file resolves to `SecretStr('')`.** `return SecretStr(file.read_text().strip())` (`config.py:1485`) has no emptiness check — a zero-byte or `"\n"`-only file (an empty Kubernetes Secret key, a half-written file, a `printf '' > secret` mistake) becomes an empty secret.

The required-secret gates then pass, because they test identity against `None` only: `if sf.client_secret is None` (`config.py:1508`) and `if sf.private_key is None` (`config.py:1517`).

Reproduced against current `main`:

```python
# SF_SECRET='' in the environment
salesforce:
  login_url: https://example.my.salesforce.com
  client_id: abc
  auth_mode: client_credentials
  client_secret: ${SF_SECRET}
  client_secret_file: /does/not/exist   # never read, never validated
# -> load() returns cfg with salesforce.client_secret == SecretStr('')
```

```python
# private_key_file contains "   \n"
# -> load() returns cfg with salesforce.private_key == SecretStr('')
```

Both contradict the module docstring's stated contract at `config.py:4-5`: *"Secrets come from `*_file` paths or inline; a missing/unreadable secret file is fatal at load time (no silent blanks)."* In case 1 a nonexistent secret file is silently ignored; in case 2 a blank is loaded silently.

Existing coverage does not pin this: `tests/test_config.py:91-94` (`test_missing_secret_file_is_fatal`) and `tests/test_config.py:408-411` (`test_client_credentials_missing_secret_is_fatal`) both set only the `*_file` field with no inline value, so the shadowing path is untested, and no test writes an empty secret file.

### What is NOT broken (verified — do not "fix" these)

* **The telemetry basic-auth fail-fast at `config.py:1558-1567` is correct.** pydantic's `SecretStr` defines `__len__` and no `__bool__`, so `bool(SecretStr(''))` is `False` (pydantic 2.13.4). `token = telemetry.basic_auth_token or cfg.sink.loki.auth_token` (`config.py:1560`) therefore skips an empty token, and a config with `service.telemetry.auth: basic` plus an empty `auth_token_file` already raises `ConfigError: service.telemetry.auth is 'basic' but no credentials resolve` at load time. There is no silently-unauthenticated OTLP path.
* **The unsalted-hash compliance warning is correct.** `app.py:708-709`, `doctor.py:409` and `backfill.py:751` all read the salt through truthiness (`... if org.sources.transform_salt else ""`), so an empty `transform_salt_file` still produces `salt == ""` and still trips `unsalted_hash_warnings` (`transforms.py:161-188`).

## Why it matters

The consequence is a misconfiguration that load-time validation is explicitly designed to catch, surfacing later as an opaque failure instead of an actionable `ConfigError`:

* **Salesforce `client_secret`** — the single-org startup probe mints a token unconditionally (`app.py:1136`), so an empty secret fails there with a raw Salesforce `invalid_client` 400 body rather than a config error naming the field and the ignored `client_secret_file`.
* **Salesforce `private_key`** — an empty key reaches `jwt.encode(payload, "", algorithm="RS256")` (`auth/jwt_auth.py:179-180`), which raises `jwt.InvalidKeyError`. That is not an `httpx.TransportError` or a `_TokenEndpointError`, so `_should_retry` (`auth/jwt_auth.py:30-36`) returns `False` and it is never wrapped in `AuthError` by `_mint_token` (`auth/jwt_auth.py:226-251`). It escapes as an unhandled `InvalidKeyError`, and in multi-org mode `_probe` catches only `AuthError` (`app.py:1259`) — so one org with a blank key propagates out of the `asyncio.gather` and takes down every healthy org, defeating the deliberate some-fail-degraded semantic documented at `app.py:1240-1249`.
* **Loki `auth_token`** — the only path with no fail-fast at all. `_build_headers` gates on `if self._cfg.auth_token is not None` (`sinks/loki/sink.py:124-127`), so an empty token produces `Authorization: Basic base64("<tenant>:")` instead of falling back to `X-Scope-OrgID`. The daemon starts healthy, Salesforce streaming works, and every push is rejected with the "Loki push rejected (auth/config)" retry loop (`sinks/loki/sink.py:297-305`) — no data delivered, process up.

## Proposed approach

Treat an empty/whitespace-only secret as absent throughout secret resolution, in `src/sf2loki/config.py`:

1. In `_resolve_secret_file`, replace the `existing is not None` precedence test with an emptiness-aware one, so a blank inline/env value falls through to the `*_file` path (and thus to the existing missing-file fatal):

```python
def _resolve_secret_file(
    file: Path | None, existing: SecretStr | None, what: str
) -> SecretStr | None:
    if existing is not None and existing.get_secret_value().strip():
        return existing
    if file is None:
        # An explicitly blank inline value with no file is "absent": let the
        # per-field required check produce the actionable error.
        return None
    ...
```

2. After reading the file, raise `ConfigError` when the stripped content is empty, naming the path and the fact that a blank secret is never valid:

```python
    value = file.read_text().strip()   # inside the existing try/except
    if not value:
        raise ConfigError(
            f"{what} file {file} is empty — a blank secret is never valid "
            "(check the mounted Secret key / the file was fully written)"
        )
    return SecretStr(value)
```

Keep the `PermissionError`/`OSError` messages exactly as they are (`config.py:1486-1495`) — the uid-10001 guidance is pinned by `tests/test_config.py:676`.

3. Leave `config.py:1558-1567` and the `transform_salt` consumers untouched: `SecretStr('')` is already falsy, and step 1 additionally normalises blanks to `None` before they get there.

An empty `transform_salt_file` becomes fatal under step 2. That is the desired behaviour (an empty salt file is a mistake, and the unsalted path is reachable deliberately by omitting the field entirely), but note it in `docs/` if the config reference states otherwise.

---

Imported from GitHub issue #131 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 131)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `_resolve_secret_file` returns `None` for an inline/env secret whose stripped value is empty, so a configured `*_file` is still read and a nonexistent one is still fatal.
- [ ] #2 `_resolve_secret_file` raises `ConfigError` naming the path when a secret file is empty or whitespace-only.
- [ ] #3 `load()` on a config with `client_secret: ${VAR}` (`VAR=''`) and no `client_secret_file` raises the existing "salesforce client secret required for auth_mode=client_credentials" `ConfigError` (`config.py:1509-1512`).
- [ ] #4 `load()` on a config with `client_secret: ${VAR}` (`VAR=''`) plus a nonexistent `client_secret_file` raises `ConfigError` mentioning the unreadable file — the empty inline value no longer shadows it.
- [ ] #5 `load()` on a config with `client_secret: ${VAR}` (`VAR=''`) plus a valid `client_secret_file` resolves to the file's contents.
- [ ] #6 `load()` on a `jwt_bearer` config whose `private_key_file` contains only whitespace raises `ConfigError` naming the file.
- [ ] #7 Same empty-file fatal verified for `sink.loki.auth_token_file`, `service.telemetry.basic_auth_token_file` and `sources.transform_salt_file`.
- [ ] #8 Tests added in `tests/test_config.py` alongside `test_missing_secret_file_is_fatal` (`tests/test_config.py:91`), parametrised over the secret fields where practical; `test_secret_file_permission_error_is_actionable` (`tests/test_config.py:676`) still passes unchanged.
- [ ] #9 A regression test asserts the telemetry basic-auth gate still fails fast with an empty token file (guards the already-correct `config.py:1558-1567` behaviour against a future refactor).
- [ ] #10 `just gate` green (ruff + `mypy --strict` + pytest); `just gen-config` re-run if any `Field(description=...)` text changes.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
