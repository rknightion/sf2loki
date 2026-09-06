# src/sf2loki/salesforce

Thin API clients, one per Salesforce surface, all taking a shared `auth.jwt_auth.TokenProvider`
(see `../auth/AGENTS.md` for mode selection and refresh). `docs/architecture.md` covers the
per-client protocol detail.

## Tooling API is a flag, not a second client

`SoqlClient(..., tooling=True)` targets `/tooling/query` instead of `/query`, which is required for
`ApexLog` and `TraceFlag` - Tooling-API-only sObjects. Same client, same auth, different query path.
Do not build a separate Tooling client.

## BigObjects have restrictive SOQL

The stored RTEM event family (`LoginEvent`, `ApiEvent`, `FileEventStore`, the Threat-Detection
`*EventStore` objects) are BigObjects: they reject `ORDER BY ASC` because the index is DESC-only,
expose no `nextRecordsUrl` pagination, and reject `COUNT()` and aggregates. `FIELDS(ALL)` itself
works - only ascending order is the problem. Set `big_object: true` per object in config so
`eventlog_objects_source._drain_big_object` pages newest-first with a ratcheting upper bound
(`docs/sources/eventlog-objects.md`). Standard and custom objects (`LoginHistory`, `MyAudit__c`)
leave the flag false and take the ASC path.

`FIELDS(ALL)` requires `LIMIT <= 200` on either path - a documented Salesforce constraint, not a
tunable.

Historical backfill beyond the poll window is a deferred follow-up, not this path.

## EventLogFile CSV parsing is schema-agnostic

`eventlogfile_client.py` reads each CSV's own header through `csv.DictReader` rather than
hardcoding the roughly 70 per-EventType column schemas, so there is no static schema to keep in
sync when Salesforce adds or changes EventLogFile columns. Cells beyond the header width land under
the `_extra` overflow key rather than being dropped.
