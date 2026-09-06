# src/sf2loki/sources

Producers implementing the `Source` protocol (`base.py`). `docs/sources/index.md` carries the
per-source design.

## Category normalisation lives in `overlap.py`

An identifier normalises to its canonical category by stripping the channel suffix (`EventStream` /
`EventStore` / `Event`, longest first) and lowercasing. `_CATEGORY_ALIASES` holds the exceptions
where the stem does not already match (`LoginHistory` -> `login`). A new object or topic that does
not auto-normalise gets an alias there, never a special case elsewhere: an identifier that
normalises to a category of its own silently bypasses the guard and double-ingests.

## The startup guard and the discovery filter are two separate mechanisms

`check_overlap` sees only EXPLICIT entries. `PubSubSource.resolve_topics()` drops the `"*"` marker,
and wildcard-discovered channels do not exist at startup.

Wildcards (`pubsub.topics: ["*"]`, `eventlogfile.event_types: ["*"]`) discover channels at runtime
(`MetadataClient.list_event_stream_topics`; a `GROUP BY EventType` SOQL for EventLogFile),
re-checked each poll or reconnect, so new Salesforce-side channels appear with no config change.
Discovery failure is non-fatal and falls back to the explicit entries.

`app.py` hands each wildcard source the categories the OTHER enabled sources own
(`owned_categories` on Pub/Sub, `exclude_categories` on EventLogFile), and discovered channels in
those categories are dropped at discovery time. Explicit entries are never filtered this way, since
the startup guard already validated them and the operator was explicit. `sources.allow_overlap:
true` empties both sets as well as bypassing the guard. Pub/Sub logs each skip at INFO with the
owning reason; the EventLogFile filter is silent, so a missing discovered type there is only
visible by working the category out by hand.

## Multi-org wrapping (`org_adapter.py`)

Under `orgs:`, each inner source is wrapped in `OrgSource`, which merges the `org` label plus that
org's `sf_org_id` and `environment` into every entry and rewrites checkpoint keys through
`state/org_view.py`. Two parts are deliberate: the inner source's `name` stays unprefixed so
`source` and `org` remain orthogonal label dimensions for dashboards, and a single-org config never
constructs an `OrgSource` at all, keeping that path bit-identical to pre-multi-org behaviour.
