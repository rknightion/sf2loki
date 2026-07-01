# Security Policy

## Reporting a vulnerability

Please report vulnerabilities **privately** via GitHub Security Advisories:
[Report a vulnerability](https://github.com/rknightion/sf2loki/security/advisories/new)
("Security" tab → "Report a vulnerability"). Do **not** open a public issue for
anything security-sensitive.

You should get an acknowledgement within a few days. Please include enough detail
to reproduce (config shape, versions, logs with secrets redacted).

## Supported versions

| Version | Supported |
| ------- | --------- |
| latest release | yes |
| `main` (edge container tag) | yes — fixes land here first |
| older releases | no — upgrade to the latest release |

## Scope notes

- sf2loki handles Salesforce credentials (private key or consumer secret) and a
  Loki/OTLP token. Anything that could leak those — in logs, error messages,
  generated config artifacts, or the container image — is in scope and treated
  as high severity.
- The health endpoints (`/healthz`, `/readyz`) are unauthenticated by design;
  the compose file binds them to loopback by default.
