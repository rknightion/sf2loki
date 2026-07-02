# CLAUDE.md — src/sf2loki/sources

Producers implementing the `Source` protocol (`base.py`): `pubsub_source.py`,
`eventlog_objects_source.py`, `eventlogfile_source.py`, `apexlog_source.py`.
See `DESIGN.md` §4/§6-8b for the frozen `Source` contract and per-source design.

## Either/or overlap guard
`overlap.py` normalises every enabled source's identifiers (Pub/Sub topics,
stored object names, EventLogFile EventTypes) to a canonical *category* and
fails startup if more than one source feeds the same category — Salesforce
exposes the same activity through up to three channels
(`/event/LoginEventStream` / `LoginEvent` / EventLogFile `Login`) and ingesting
more than one double-counts records. `_CATEGORY_ALIASES` holds exceptions
where the normalised stem doesn't already match (e.g. `LoginHistory` aliases
to `login`) — add new aliases here, not a special case elsewhere, if a new
object/topic doesn't auto-normalise to its category. Bypass with
`sources.allow_overlap: true` only when the duplication is deliberate.

## Wildcard discovery
Both `pubsub.topics: ["*"]` and `eventlogfile.event_types: ["*"]` discover
channels at runtime (`MetadataClient.list_event_stream_topics` /
`soql_client` `GROUP BY EventType`) re-checked each poll/reconnect, so new
Salesforce-side channels appear without a config change. Discovery failure is
non-fatal — falls back to explicit entries. **The overlap guard only sees
explicit entries** — discovered ones aren't known at startup — so use
`exclude:` on the wildcard source to keep a discovered category off it when a
higher-priority explicit source already owns that category.

## Multi-org wrapping (`org_adapter.py`)
When the config uses `orgs:` (multi-org), the app wraps each inner source in
`OrgSource`, which merges the `org` label plus per-org `sf_org_id`/
`environment` into every entry and rewrites checkpoint keys with an
`org=<name>:` prefix via `state/org_view.py`. The inner source's own `name`
(`"pubsub"`, `"eventlogfile"`, ...) is preserved unprefixed — `org` is its own
label dimension, kept orthogonal to `source` so dashboards can slice either
way. Single-org deployments never construct an `OrgSource` — behavior there is
bit-identical to pre-multi-org.
