# CLAUDE.md — src/sf2loki/salesforce

Thin Salesforce API clients, one per surface: `pubsub_client.py` (gRPC
streaming), `soql_client.py` (REST/Tooling SOQL), `eventlogfile_client.py`
(EventLogFile listing + blob download), `apexlog_client.py` (Tooling API debug
logs), `metadata_client.py` (describeGlobal discovery), `limits_client.py`
(org API-limit polling), `avro_codec.py` (Pub/Sub payload decode). All take a
shared `auth.jwt_auth.TokenProvider` — see `../auth/CLAUDE.md` for auth
selection/refresh. DESIGN.md §5-8b covers the full protocol detail per client.

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
Threat-Detection `*EventStore` objects (queried via `eventlog_objects_source`)
are BigObjects: you may filter only on **indexed fields, in index order**, and
`ORDER BY` is limited. This isn't a generic query-builder problem — it's
handled per-object via a small descriptor of which field is the indexed
watermark and what's queryable (see the object config in `config.py` /
DESIGN.md §7). Don't assume an arbitrary `WHERE`/`ORDER BY` will work against
a BigObject the way it does against a normal sObject.

## EventLogFile CSV parsing is schema-agnostic
`eventlogfile_client.py` reads each CSV's header (or `LogFileFieldNames`)
rather than hardcoding the ~70 per-EventType column schemas — there is no
static schema to keep in sync when Salesforce adds/changes EventLogFile
columns.
