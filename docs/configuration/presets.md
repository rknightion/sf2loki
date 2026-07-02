# Presets

[`examples/presets/`](https://github.com/rknightion/sf2loki/tree/main/examples/presets)
ships six ready-to-merge config fragments for common setups. Each is **a
fragment, not a runnable standalone file** — merge the shown keys into your
own `config.yaml` alongside the `sink`/`state`/`service` sections (see the
[full reference](../config-reference.md) for the complete schema, and
[Configuration](index.md) for how loading and secrets work).

| File | Purpose |
| --- | --- |
| [`multi-org.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/multi-org.yaml) | Ingest two Salesforce orgs (prod + emea) from one process into one shared sink via the `orgs:` list. |
| [`custom-object-polling.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/custom-object-polling.yaml) | SOQL-poll an arbitrary custom object (e.g. `MyAudit__c`) on a timestamp watermark. |
| [`event-log-objects.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/event-log-objects.yaml) | SOQL-poll Salesforce Big Objects — the stored RTEM `*Event`/`*EventStore` family (`LoginEvent`, `ApiEvent`, ...). |
| [`login-history.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/login-history.yaml) | Ingest login activity via `LoginHistory` polling, with the streamed/polled/ELF alternatives shown as commented blocks. |
| [`setup-audit-trail.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/setup-audit-trail.yaml) | Ingest admin/config change history via `SetupAuditTrail` polling. |
| [`custom-platform-events.yaml`](https://github.com/rknightion/sf2loki/blob/main/examples/presets/custom-platform-events.yaml) | Stream your own custom Platform Events and Change Data Capture channels via the Pub/Sub source. |

## multi-org

Replaces the single-org top-level `salesforce:`/`sources:` with an `orgs:`
list — several Salesforce orgs, one sf2loki process, one shared Loki sink.
Each org keeps its own connection and source selection; `sink`, `state`,
`coordinate`, and `service` stay shared. Every org gets its own `org` stream
label and checkpoint-key prefix (`org=<name>:`), and the overlap guard runs
per org, so the same event category on two *different* orgs is fine.

**How to use:** set either top-level `salesforce:` or `orgs:`, never both.
Org names must be unique (letters, digits, `_`, `-`). See
[Configuration](index.md) for the mutual-exclusion rule.

## custom-object-polling

Demonstrates that `sources.eventlog_objects` isn't limited to Salesforce's
built-in Event Monitoring objects — it polls **any** queryable sObject on a
timestamp watermark, so it doubles as general-purpose custom-object log
collection. The preset polls a custom object `MyAudit__c` on
`LastModifiedDate`.

**How to use:** swap in your own object and field names.
`timestamp_field` must be a monotonically increasing field
(`CreatedDate`/`LastModifiedDate` are the usual picks) — custom objects have
no `EventDate` default, so this must be set explicitly. See
[event-log objects](../sources/eventlog-objects.md) for the query shape and
caveats.

## event-log-objects

SOQL-polls Salesforce **Big Objects** — the stored form of the RTEM event
family (`LoginEvent`, `ApiEvent`, `FileEventStore`, `*EventStore`, ...). Big
Objects reject `ORDER BY ASC`, so each entry sets `big_object: true`; the
source then drains them newest-first and re-sorts each cycle's window
ascending before shipping. Standard/custom objects (`LoginHistory`,
`MyAudit__c`) must leave the flag unset.

**How to use:** requires the Shield Event Monitoring add-on for most of
these objects. These objects are the persisted form of the same records
available via Pub/Sub streaming and, for some types, EventLogFile — don't
also enable those for the same category unless `sources.allow_overlap` is
set. See [event-log objects](../sources/eventlog-objects.md).

## login-history

Login activity is reachable three ways, and the preset shows all three as
one active block plus two commented alternatives — pick exactly **one**:

1. SOQL-poll the standard `LoginHistory` object (the active block) — no
   Shield/Event Monitoring entitlement required.
2. SOQL-poll `LoginEvent`, or stream `/event/LoginEventStream` via
   `sources.pubsub` — typically requires Shield/Event Monitoring.
3. Ingest the `Login` EventLogFile EventType via `sources.eventlogfile`.

All three map to the same overlap-guard category (`login`); enabling more
than one trips the fail-fast startup guard.

**How to use:** uncomment the block matching your entitlements and delete
(or leave commented) the other two. See
[Sources](../sources/index.md) for the either/or-per-category rule and
[EventLogFile](../sources/eventlogfile.md) for option 3.

## setup-audit-trail

SOQL-polls the standard `SetupAuditTrail` object — admin/config change
history (permission changes, metadata edits, admin actions). No
Shield/Event Monitoring entitlement required, just standard API access.
Unlike login data, `SetupAuditTrail` has no ELF/streaming equivalent in the
overlap guard's category map, so there's no either/or choice here.

**How to use:** merge as-is; add a separate `eventlogfile` block for other,
disjoint categories if you also want ELF ingestion alongside it. See
[event-log objects](../sources/eventlog-objects.md).

## custom-platform-events

Streams your own custom Platform Events (`/event/My_Event__e`) and Change
Data Capture channels (`/data/AccountChangeEvent`, custom-object CDC, or a
curated custom channel) via `sources.pubsub`. The Pub/Sub source subscribes
to any explicit topic, not just the RTEM monitoring streams, so this needs
no engine change — only explicit `topics:` entries (never `"*"`, which
discovers RTEM streams, not custom/CDC channels).

**How to use:** unlike RTEM monitoring streams, custom/CDC events count
against your org's event-delivery allocations — subscribe to specific
channels, and use `sample:` if one channel is still too hot. CDC's
`ChangeEventHeader.changedFields`/`nulledFields` bitmap fields ship as raw
encoded strings, unexpanded. These categories are orthogonal to the
RTEM/ELF categories, so the overlap guard never flags them. See
[Pub/Sub](../sources/pubsub.md).
