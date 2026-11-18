# Salesforce to Loki review

## Streaming contracts

This service ships Salesforce logs to Loki. Review source, sink and checkpoint changes together and preserve the documented delivery semantics in `docs/architecture.md`. Confirm checkpoints advance after sink acceptance. Review checkpoint-only entries against the same batch ordering, so they do not advance past undelivered logs.

## Replay and isolation

For EventLogFile, a partial file flush must retain restart coverage until the final row. Test rejection, retry and restart with deterministic input. Keep per-org checkpoint keys and the org label applied by the source adapter isolated, including when static labels are configured or legacy checkpoints are read.

## Loki labels and diagnostics

Keep label sets bounded and put unbounded dimensions in structured metadata. Check batch shape and retry behavior without relying on a live Salesforce org or Loki tenant. Keep credentials and real org, tenant or user identifiers out of diagnostics and fixtures.

## Configuration and packaging

Review changes to configuration and environment keys against the public contract. Update source definitions and regenerate the configuration reference and protobuf stubs. Review Helm template changes across the supported layouts, including its replica guard and external-secret options.

## Verification

Run `just check`. Use pipeline, EventLogFile, organization adapter and sink tests to demonstrate behavior at the relevant boundary. Compare rendered resources when a chart setting changes.
