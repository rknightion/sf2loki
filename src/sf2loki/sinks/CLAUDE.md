# CLAUDE.md — src/sf2loki/sinks

`Sink` protocol in `base.py`; `loki/` is the only implementation
(`sink.py` retry/push orchestration, `push.py` wire encoding, `labels.py`
allowlist guard). DESIGN.md §9-10 covers the full design.

## Encoding: protobuf+snappy is the default, JSON+gzip is for debugging
`push.py` implements both `encode_protobuf` (canonical `logproto.PushRequest`,
generated stubs in `loki/_generated/` from `proto/loki_push.proto` — same
`just proto` regen as the Pub/Sub stubs, never hand-edit) and `encode_json`
(`/loki/api/v1/push` body). Protobuf is what production should run; JSON is
for human-inspectable payloads in tests/debugging. Don't assume JSON is the
"normal" path when reading logs or writing new sink tests.

## Retry classification is status-code-driven, not generic
`sink.py`: 429 and 5xx → `RetryableSinkError` (bounded tenacity retry honouring
`Retry-After`); 400 and 413 → `PermanentSinkError` (drop the offending
entry/batch, count it, advance the checkpoint — never stall the whole pipeline
on one poison payload). A 413 first tries splitting the batch before giving up
entry-by-entry. If you add a new failure mode, decide deliberately which
bucket it belongs to — the two error types have very different pipeline
consequences (retry vs. drop-and-advance).

## Per-line truncation cap
`batch.max_line_bytes` (default 262144, Loki's own `max_line_size` default)
truncates an oversized line on a UTF-8 boundary before push and increments
`sf2loki_lines_truncated_total`, rather than letting Loki 400 the whole batch
over one fat line. Keep this in sync with your Loki server's `max_line_size`
if you change it.

## Label allowlist guard
`labels.py:ALLOWED_LABELS` is the single source of truth for permitted Loki
stream labels — update it (and `../../CLAUDE.md`'s copy of the list) together
if a new label is ever justified. `RESERVED_STATIC_LABELS` (`source`,
`event_type`) can never appear in operator-supplied static labels
(`sink.loki.labels`) because they're per-entry identity, set by sources.
