# src/sf2loki/sinks

`Sink` protocol and both error types in `base.py`; `loki/` is the only implementation.

## Encoding: protobuf+snappy is the default, JSON+gzip is for debugging

`push.py` carries both `encode_protobuf` (canonical `logproto.PushRequest`) and `encode_json`
(`/loki/api/v1/push` body). Production runs protobuf; JSON exists for human-inspectable payloads.
Do not assume JSON is the normal path when reading a sink log line or writing a new sink test.
`sink.loki.compression` (`snappy` / `gzip` / `none`) only applies on the JSON path: protobuf is
snappy-block-compressed inside `encode_protobuf` whatever that setting says.

## Three HTTP status buckets in `sink.py`, not two

- 429, 5xx and transport failures: bounded tenacity retry honouring `Retry-After`, then
  `RetryableSinkError`.
- 401 / 403 / 404 (`_AUTH_CONFIG_STATUSES`): also `RetryableSinkError`, but this means a rotated
  token or a wrong tenant/URL, not a transient blip. It retries with capped backoff and holds
  checkpoints, so nothing is lost while an operator fixes the config.
- 400 and 413: split the batch and recurse, re-encoding each half. `PermanentSinkError` escapes
  only from a single-entry batch; a parent absorbs it, drops and counts just that half, and keeps
  delivering the rest. One poison payload never stalls the pipeline.
- Any other status retries rather than dropping.

A new failure mode picks one of these buckets deliberately: retry and drop-and-advance have very
different pipeline consequences.

## Per-line truncation cap

`batch.max_line_bytes` (default 262144, Loki's own `max_line_size` default) truncates an oversized
line on a UTF-8 character boundary before push and increments `sf2loki_lines_truncated`, rather
than letting Loki 400 the whole batch over one fat line. Capping is idempotent, so it is safe under
the 400/413 split recursion. Change the default only alongside the Loki server's `max_line_size`.

## Static labels may not carry per-entry identity

`labels.py:RESERVED_STATIC_LABELS` (`source`, `event_type`) are rejected in operator-supplied
`sink.loki.labels`: merged as static they would override every entry's own value and collapse all
stream separation.
