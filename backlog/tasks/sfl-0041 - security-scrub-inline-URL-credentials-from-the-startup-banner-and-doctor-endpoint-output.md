---
id: SFL-0041
title: >-
  security: scrub inline URL credentials from the startup banner and doctor
  endpoint output
status: To Do
assignee: []
created_date: '2026-08-14 16:58'
labels:
  - followup
  - phase-1
milestone: m-2
dependencies: []
references:
  - 'https://github.com/rknightion/sf2loki/issues/125'
ordinal: 41000
---

## Description

<!-- SECTION:DESCRIPTION:BEGIN -->
## What

`sink.loki.url` is logged verbatim at INFO on every process start, and inline `user:token@` credentials in that URL are fully functional, so a working configuration can write a Grafana Cloud write-scope token to stdout on every restart.

Chain:

- `App.build` captures the raw URL: `sink_url=cfg.sink.loki.url` at `src/sf2loki/app.py:1098`, stored on `_StartupInfo.sink_url` (`src/sf2loki/app.py:869`).
- `App.run` emits the banner as its first statement (`src/sf2loki/app.py:1120`); `_emit_startup_log` logs `sink=s.sink_url` at INFO (`src/sf2loki/app.py:1112`).
- Default `service.log_level` is `info` (`src/sf2loki/config.py:1266`), and `configure_logging` (`src/sf2loki/app.py:912`, `src/sf2loki/obs/logging.py:48-101`) installs a plain `StreamHandler` with the JSON/logfmt renderer and no redaction processor. The value reaches stdout unmodified.
- `LokiConfig.url` is an unvalidated `str` with `min_length=1` (`src/sf2loki/config.py:930`). The post-load validation block (`src/sf2loki/config.py:1543-1568`) resolves secret files and checks telemetry auth; it never inspects the URL for userinfo.

The inline form works, so nothing signals the mistake. `loki_http` is constructed with no `auth=` (`src/sf2loki/app.py:921`), and httpx 0.28.1 falls back to URL userinfo when no explicit auth object is set (`httpx/_client.py:466-471`: `BasicAuth(username=request.url.username, password=request.url.password)`). `BasicAuth.auth_flow` then assigns `request.headers["Authorization"]`, overwriting the header that `LokiSink._build_headers` built from `tenant_id`/`auth_token` (`src/sf2loki/sinks/loki/sink.py:120-133`) before the POST at `src/sf2loki/sinks/loki/sink.py:203-207`.

Verified against this repo's pinned httpx (0.28.1) with a `MockTransport`: POSTing to `https://123456:glc_TOKEN@logs-prod-006.grafana.net/loki/api/v1/push` while also passing the sink's own `Authorization: Basic ...` header produced `Authorization: Basic MTIzNDU2OmdsY19UT0tFTg==`, i.e. the URL credentials both authenticate and silently win over the configured `tenant_id`/`auth_token`.

Same unscrubbed-endpoint pattern in doctor: `service.telemetry.endpoint` is interpolated into the check detail in every branch — `src/sf2loki/doctor.py:455` (unreachable), `:464` (401/403), `:473` (non-success), `:477` (PASS). `TelemetryConfig.endpoint` is likewise an unvalidated `str` (`src/sf2loki/config.py:1209`). `sf2loki doctor` output is routinely pasted into tickets and chat.

Existing partial mitigation, for context: httpx logs `request.url` at INFO per request (`httpx/_client.py:1740`), which would leak the URL on every push, but the `httpx` logger is floored to WARNING by `_CHATTY_LOGGERS` / `_THIRD_PARTY_LOG_FLOOR` (`src/sf2loki/obs/logging.py:35-44, 105-107`). That floor does not cover sf2loki's own banner call.

Repo docs consistently show the split form (`README.md:290`, `docs/getting-started.md:60`, `docs/configuration/index.md:26`), but the combined `https://<user>:<token>@logs-prod-XX.grafana.net/loki/api/v1/push` form is the shape Grafana Cloud hands out for promtail/alloy push configs, so an operator pasting it is the expected failure mode — and it works, so nothing prompts a correction.

## Why it matters

An operator sets `sink.loki.url` (or `SF2LOKI_SINK__LOKI__URL` / `${GC_LOKI}`) to the combined Grafana Cloud push URL. Pushes authenticate, doctor passes, the deployment is healthy. Every container start then writes `sink=https://123456:glc_...@logs-prod-006.grafana.net/loki/api/v1/push` to stdout at INFO, which container platforms ship into a log aggregation system readable by a much wider audience than the secret store the token was meant to live in. The exposed credential has write scope on the Grafana Cloud stack and must be rotated once discovered. Restart loops multiply the copies.

Second-order: because URL userinfo overrides the `Authorization` header, an operator who sets both sees `tenant_id`/`auth_token` appear to be in effect while the URL credentials are what actually authenticate — auth changes made via the documented fields silently do nothing.

## Proposed approach

1. Add a redaction helper in `src/sf2loki/obs/logging.py` (imported by both `app.py` and `doctor.py`):

   ```python
   def redact_url_userinfo(url: str) -> str:
       """Return *url* with any inline user:password@ stripped (never raises)."""
   ```

   Implementation: `str(httpx.URL(url).copy_with(userinfo=b""))` — verified against httpx 0.28.1, turns `https://123456:glc_TOKEN@logs-prod-006.grafana.net/loki/api/v1/push` into `https://logs-prod-006.grafana.net/loki/api/v1/push`. Wrap in a try/except returning a hard-coded `"<unparseable url>"` so a malformed URL can never crash the banner or doctor.

2. Apply it at both call sites: `_StartupInfo(sink_url=redact_url_userinfo(cfg.sink.loki.url))` (`src/sf2loki/app.py:1098`) and every `telemetry.endpoint` interpolation in `_check_telemetry` (`src/sf2loki/doctor.py:455, 464, 473, 477`). Redacting at capture time (in `_StartupInfo`) is preferable to redacting at log time — the dataclass then never holds the secret.

3. Warn loudly when userinfo is present, mirroring the existing advisory-warning precedent (`unsalted_hash_warnings` in `src/sf2loki/transforms.py:166-190`, surfaced from both the app startup path and doctor). Add `url_userinfo_warnings(cfg) -> list[str]` returning one message per offending field:

   - `sink.loki.url` contains inline credentials; they override `sink.loki.tenant_id`/`auth_token` and are not the supported configuration — move them to `tenant_id` + `auth_token(_file)`.
   - `service.telemetry.endpoint` contains inline credentials; move them to `service.telemetry.basic_auth_user` + `basic_auth_token(_file)`.

   Emit at WARNING from the app startup path and as a doctor `WARN` result. Do not hard-reject in `config.py` load: the form currently works, so raising `ConfigError` breaks running deployments on upgrade. A WARN plus scrubbed logs closes the leak; a future breaking change can escalate to rejection.

4. Docs: add one line to `docs/configuration/index.md` and the Loki sink reference stating that credentials belong in `tenant_id`/`auth_token(_file)`, that inline URL credentials override those fields, and that they are stripped from logs but should not be used. `config.example.yaml`/`docs/config-reference.md` are generated — if any `Field(description=...)` text changes, run `just gen-config` or the drift gate fails (`tests/test_config_artifacts_drift.py`).

---

Imported from GitHub issue #125 on 2026-08-14, when this repo migrated from GitHub Issues to Backlog.md. The original issue has been deleted; its verbatim body, labels and comments are preserved in `archive/issues-dump.json` (`jq '.[] | select(.number == 125)' archive/issues-dump.json`).

<sub>Filed from the 2026-07-30 full-repo audit (11 finder lanes + adversarial verification per finding).</sub>
<!-- SECTION:DESCRIPTION:END -->

## Acceptance Criteria
<!-- AC:BEGIN -->
- [ ] #1 `redact_url_userinfo` exists in `src/sf2loki/obs/logging.py`, strips both username and password, is a no-op for URLs without userinfo, and returns a placeholder instead of raising on an unparseable URL.
- [ ] #2 `_StartupInfo.sink_url` is populated with the redacted URL at `src/sf2loki/app.py:1098`, so the dataclass never stores inline credentials.
- [ ] #3 All four `telemetry.endpoint` interpolations in `_check_telemetry` (`src/sf2loki/doctor.py:455, 464, 473, 477`) print the redacted endpoint.
- [ ] #4 `url_userinfo_warnings` reports one warning per offending field and is surfaced both at app startup (WARNING) and as a doctor `WARN` check result; an empty list produces no output.
- [ ] #5 Test in `tests/test_app_startup_log.py`: with `sink.loki.url = "https://tenant:sekrit@logs-prod-006.grafana.net/loki/api/v1/push"`, the captured `sf2loki starting` entry's `sink` value contains `logs-prod-006.grafana.net/loki/api/v1/push` and does NOT contain `sekrit` or `tenant:`. The existing assertion at `tests/test_app_startup_log.py:50` (`"loki:3100" in entry["sink"]`) must still pass unchanged.
- [ ] #6 Test that no captured log record from the startup path contains the secret substring anywhere in its rendered form (guards against a future field re-leaking it).
- [ ] #7 Doctor test covering all four telemetry branches (transport error, 401/403, non-2xx, success) with a userinfo-bearing endpoint: no `CheckResult.detail` contains the token substring.
- [ ] #8 Test for `url_userinfo_warnings`: warnings raised for a userinfo-bearing `sink.loki.url` and `service.telemetry.endpoint`; none for the clean split-credential configuration.
- [ ] #9 Docs state that credentials go in `tenant_id`/`auth_token(_file)` and `basic_auth_user`/`basic_auth_token(_file)`, and that inline URL credentials override them.
- [ ] #10 `just gate` green (ruff + `ruff format --check` + `mypy src` + pytest), including the config-artifact drift gate if any field description changed.
<!-- AC:END -->

## Definition of Done
<!-- DOD:BEGIN -->
- [ ] #1 just gate is green (ruff check + ruff format --check + mypy src + pytest) — run it, don't assert it
- [ ] #2 just gen-config run and its output committed, if config.py changed (CI drift gate fails otherwise)
- [ ] #3 committed straight to main with a conventional-commit message, and pushed
<!-- DOD:END -->
