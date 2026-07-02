# Security

## Reporting a vulnerability

Report vulnerabilities **privately** via GitHub Security Advisories:
[Report a vulnerability](https://github.com/rknightion/sf2loki/security/advisories/new)
("Security" tab → "Report a vulnerability"). Do **not** open a public issue for anything
security-sensitive.

You should get an acknowledgement within a few days. Include enough detail to reproduce — config
shape, versions, logs with secrets redacted.

## Supported versions

| Version | Supported |
|---|---|
| Latest release | yes |
| `main` (edge container tag, `:main`) | yes — fixes land here first |
| Older releases | no — upgrade to the latest release |

## Data handling

### Credentials and tokens

sf2loki handles Salesforce credentials (a JWT-bearer private key or a client-credentials consumer
secret) and a Grafana Cloud Loki/OTLP token. All of these are secrets in scope for the reporting
process above:

- Credentials are injected from `*_file` paths (mounted read-only, e.g. `salesforce.private_key_file`,
  `sink.loki.auth_token_file`) or `${ENV}` interpolation at config load — never inlined as plain
  config values by convention, and a missing or unreadable secret is fatal at startup rather than
  silently skipped.
- Secrets are never written to logs, error messages, or generated config artifacts
  (`config.example.yaml`, `docs/config-reference.md`, the JSON schema) — those describe shapes and
  keys, never values.
- The container mounts secret files read-only and runs as a non-root user (uid `10001`); the
  secret files themselves must be readable by that uid — see
  [Troubleshooting](troubleshooting.md#the-container-crash-loops-with-a-permission-error-on-startup).
- `.env*`, `*.key`, `*.crt`, and `secrets/` are gitignored **and** `.dockerignore`d so they can
  never be baked into a locally built image.

### Health endpoints

`/healthz` and `/readyz` are **unauthenticated by design** — they carry no Salesforce or Loki
data, only process liveness/readiness state, so they don't need auth to be safely exposed inside a
cluster or behind a load balancer. The shipped `docker-compose.yml` binds them to loopback by
default. Don't expose them to the public internet without a reason to.

### PII in ingested data

sf2loki ships **opt-in** compliance controls for the Salesforce data it forwards to Loki:
declarative PII transforms (hash / mask / drop field / drop row / regex) and deterministic
per-type sampling. These are off by default — enabling them is the operator's responsibility based
on what event types and fields are in scope for their org. See
[PII Redaction & Sampling](sources/pii-and-sampling.md) for the full transform reference and
recipes.

## License obligations

sf2loki is licensed [AGPL-3.0-only](https://github.com/rknightion/sf2loki/blob/main/LICENSE) — free
to use, modify, and self-host. The AGPL's network-use clause applies: if you run a **modified**
version of sf2loki as a network service (including as a hosted/managed offering), you must offer
the modified source to the users of that service. Running an unmodified build (including the
published container image) carries no source-offer obligation beyond the license text itself.
