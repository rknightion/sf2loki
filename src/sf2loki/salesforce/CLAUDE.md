# CLAUDE.md — src/sf2loki/salesforce

Thin Salesforce API clients, one per surface: `pubsub_client.py` (gRPC
streaming), `soql_client.py` (REST/Tooling SOQL), `eventlogfile_client.py`
(EventLogFile listing + blob download), `apexlog_client.py` (Tooling API debug
logs), `metadata_client.py` (describeGlobal discovery), `limits_client.py`
(org API-limit polling), `avro_codec.py` (Pub/Sub payload decode). All take a
shared `auth.jwt_auth.TokenProvider` — see `../auth/CLAUDE.md` for auth
selection/refresh. docs/architecture.md covers the full protocol detail per client.

## `_generated/` — never hand-edit
`_generated/pubsub_api_pb2{,_grpc}.py` are committed generated gRPC stubs from
`proto/pubsub_api.proto`. Regenerate with `just proto` (repo root), never edit
directly — a CI drift check compares committed stubs against a fresh
generation. `ruff` excludes `**/_generated/**` entirely.

## `soql_client.py` — Tooling API mode
`SoqlClient(..., tooling=True)` targets `/tooling/query` instead of `/query` —
required for `ApexLog`/`TraceFlag`, which are Tooling-API-only sObjects. Same
client, same auth, just a different query path; don't build a second client
for Tooling queries.

## BigObjects have restrictive SOQL
The stored RTEM event family (`LoginEvent`, `ApiEvent`, `FileEventStore`,
Threat-Detection `*EventStore`, ...) are BigObjects: they reject `ORDER BY ASC`
(the index is DESC-only), expose no `nextRecordsUrl` pagination, and reject
`COUNT()`/aggregates. Set `big_object: true` per object in config —
`eventlog_objects_source._drain_big_object` then pages newest-first
(`ORDER BY <ts> DESC`) with a ratcheting `<=` upper bound, dedups within the
drain and against the checkpoint id-window, and re-sorts each cycle's window
ascending before emitting so the watermark/dedup/checkpoint semantics match the
ASC path (see docs/sources/eventlog-objects.md). `FIELDS(ALL)` itself works on BigObjects; only the
ASC order was the problem. Standard/custom objects (`LoginHistory`,
`MyAudit__c`) leave the flag false and use the ASC path. Historical backfill
beyond the poll window is a deferred follow-up, not this path.

## EventLogFile CSV parsing is schema-agnostic
`eventlogfile_client.py` reads each CSV's header (or `LogFileFieldNames`)
rather than hardcoding the ~70 per-EventType column schemas — there is no
static schema to keep in sync when Salesforce adds/changes EventLogFile
columns.
