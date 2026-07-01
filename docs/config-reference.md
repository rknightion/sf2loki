# sf2loki configuration reference

## Config

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `salesforce` | `SalesforceConfig` |  | yes | Salesforce org connection and authentication. |
| `sources` | `SourcesConfig` |  | no | Event source selection and settings. |
| `sink` | `SinkConfig` |  | yes | Log sink settings. |
| `state` | `StateConfig` |  | no | Checkpoint/state store settings. |
| `service` | `ServiceConfig` |  | no | Application-level service settings. |

## SalesforceConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `login_url` | `str` | "" | no | Optional: derived from `environment` when omitted; set to a custom My Domain URL to override. An explicit value always takes precedence over the environment-derived default. |
| `environment` | `Literal['production', 'sandbox']` | production | no | production \| sandbox — derives login_url when login_url is unset; an explicit login_url takes precedence. |
| `auth_mode` | `Literal['jwt_bearer', 'client_credentials']` | jwt_bearer | no | jwt_bearer (private key + cert) \| client_credentials (consumer key + secret). |
| `client_id` | `str` | ${SF_CLIENT_ID} | yes | External Client App consumer key. |
| `client_secret` | `SecretStr` | *(secret)* | no | client_credentials flow secret (injectable from file/env like the key). Required when auth_mode is client_credentials; unused for jwt_bearer. |
| `client_secret_file` | `Path` | *(secret)* | no | File path to the client_credentials secret; required when auth_mode: client_credentials. |
| `username` | `str` | svc@example.com | no | Integration user (pre-authorised on the app's Policies tab). Required for jwt_bearer (the JWT `sub` claim); not needed when auth_mode: client_credentials. |
| `private_key_file` | `Path` | *(secret)* | no | File path to the jwt_bearer private key; not needed when auth_mode: client_credentials. |
| `private_key` | `SecretStr` | *(secret)* | no | jwt_bearer private key, inline (alternative to private_key_file). |
| `api_version` | `str` | "60.0" | no | Salesforce REST/SOAP API version to target. |
| `org_id` | `str` | null | no | Set this to keep the app on the `api` scope alone; left null it is auto-resolved via /services/oauth2/userinfo (which then needs the `openid` scope). |
| `token_ttl` | `Duration` | 1h | no | Assumed access-token lifetime (Salesforce returns no expires_in for these flows; the real lifetime is the org's session timeout, which can be as short as 15m). Refresh is also reactive on 401, so this only tunes proactive re-mint cadence. |
| `limits` | `SalesforceLimitsConfig` |  | no | Org-limits metric poller settings. |

## SalesforceLimitsConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Poll /services/data/vXX.0/limits for org-limit gauges (API usage, storage, streaming events, ...). |
| `poll_interval` | `Duration` | 5m | no | How often to poll the limits endpoint. |

## SourcesConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `pubsub` | `PubSubConfig` |  | no | Pub/Sub API streaming source. |
| `eventlog_objects` | `EventLogObjectsConfig` |  | no | Event-object SOQL polling source. |
| `eventlogfile` | `EventLogFileConfig` |  | no | EventLogFile (CSV) ingestion source. |
| `allow_overlap` | `bool` | false | no | Bypass the fail-fast overlap guard that refuses to start when one event category is enabled on more than one source. |

## PubSubConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | true | no | Enable the Pub/Sub streaming source. |
| `endpoint` | `str` | api.pubsub.salesforce.com:7443 | no | Pub/Sub API gRPC endpoint. |
| `default_num_requested` | `int` | 100 | no | Flow-control batch size (1-100; Salesforce clamps at 100 and returns INVALID_ARGUMENT when over-asked). |
| `replay_preset` | `Literal['LATEST', 'EARLIEST', 'CUSTOM']` | CUSTOM | no | Replay position; falls back to LATEST when no stored replay_id. |
| `topics` | `list[str]` | [/event/LoginEventStream, /event/ApiAnomalyEvent] | no | Explicit topics, or "*" to DISCOVER and subscribe to every RTEM stream the org exposes (the *EventStream channels), re-filtered by include/exclude. |
| `include` | `list[str]` | ["*"] | no | Operator inclusion globs applied to discovered/explicit topics. |
| `exclude` | `list[str]` | [] | no | Operator exclusion globs applied to discovered/explicit topics. |

## EventLogObjectsConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Enable the event-object polling source. |
| `objects` | `list[EventLogObjectConfig]` |  | no | Event objects to poll. |

## EventLogObjectConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | "" | yes | The event object API name to poll (e.g. LoginEvent). |
| `timestamp_field` | `str` | EventDate | no | The field used as the per-row event time for polling/checkpointing. |
| `poll_interval` | `Duration` | 5m | no | How often to poll this object. |
| `lookback` | `Duration` | 1h | no | Initial window to fetch on first run (no checkpoint). |

## EventLogFileConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Enable the EventLogFile ingestion source. |
| `interval` | `Literal['Hourly', 'Daily']` | Hourly | no | Daily (settled, ~1d lag) \| Hourly (fresher, needs the Event Monitoring hourly opt-in in Setup) — pick ONE. Many orgs generate ONLY Daily files; check before Hourly. |
| `event_types` | `list[EventLogFileTypeConfig]` |  | no | ELF EventTypes to ingest (required when enabled — there is no sensible "all" default given ~70 types and the either/or-per-category model). Each item is a bare string (e.g. "Login") or a per-type object (name + optional structured_metadata_fields/labels). Use "*" to discover and ingest every EventType the org produces for this interval. |
| `exclude` | `list[str]` | [] | no | EventTypes to skip when the wildcard "*" is used (e.g. a category served by another source, or a high-volume type you don't want). Ignored otherwise. |
| `poll_interval` | `Duration` | 1h | no | How often to list new files (Daily files land ~once/day). |
| `lookback` | `Duration` | 1d | no | Initial window on first run (no checkpoint); reach back far enough to catch the last few settled Daily files. |
| `timestamp_column` | `str` | TIMESTAMP_DERIVED | no | Per-row event time column. |
| `page_size` | `int` | 1000 | no | SOQL LIMIT for the file-listing query. |
| `settle_window` | `Duration` | 0s | no | Skip files whose CreatedDate is newer than now-settle_window, so we don't pull a half-written hourly CSV. 0 disables (safe for Daily); use a few minutes for Hourly. |
| `download_max_age` | `Duration` | 1d | no | A file whose body keeps failing to download and is older than this is abandoned (checkpoint advances past it) so a permanently-missing file can't wedge the watermark forever. Files younger than this are retried. |

## EventLogFileTypeConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | "" | yes | The ELF EventType this override applies to (e.g. ReportExport), or "*" to discover all types. |
| `structured_metadata_fields` | `list[str]` | [REPORT_ID, OWNER_ID] | no | Per-type override of the global sink.loki.structured_metadata_fields; omit (None) to inherit the global list, or set to [] to suppress it. |
| `labels` | `list[str]` | [DELEGATED_USER] | no | Columns promoted to Loki stream labels for this event type. Keep these LOW cardinality — each distinct value is a new Loki stream. |

## SinkConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `type` | `Literal['loki']` | loki | no | Sink backend (only loki is supported). |
| `loki` | `LokiConfig` |  | yes | Loki sink settings. |

## LokiConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `url` | `str` | ${GC_LOKI} | yes | Loki push API URL. |
| `tenant_id` | `str` | ${GC_TENANT_ID} | no | Loki tenant (X-Scope-OrgID); required for Grafana Cloud and multi-tenant Loki. |
| `auth_token_file` | `Path` | *(secret)* | no | File path to the Loki auth token. |
| `auth_token` | `SecretStr` | *(secret)* | no | Loki auth token, inline (alternative to auth_token_file). |
| `encoding` | `Literal['protobuf', 'json']` | protobuf | no | Wire encoding for the push request. |
| `compression` | `Literal['snappy', 'gzip', 'none']` | snappy | no | Compression: snappy (protobuf) \| gzip (json) \| none. |
| `batch` | `LokiBatchConfig` |  | no | Push batching settings. |
| `labels` | `dict[str, str]` | {environment: prod} | no | Static stream labels merged onto every push (job + sf_org_id are added automatically). |
| `structured_metadata_fields` | `list[str]` | [replay_id, schema_id, event_uuid, user_id, username, source_ip, session_key] | no | Event fields promoted to Loki structured metadata (not stream labels). |

## LokiBatchConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `max_entries` | `int` | 1000 | no | Flush the batch after this many log entries. |
| `max_bytes` | `int` | 1048576 | no | Flush the batch after this many bytes. |
| `flush_interval` | `Duration` | 1s | no | Flush the batch after this much time, regardless of size. |
| `max_line_bytes` | `int` | 262144 | no | Per-line UTF-8 byte cap; lines longer than this are truncated (with a marker) before push so one oversized row can't 400 its whole batch. Mirrors Loki's server-side `max_line_size` default (256 KiB). 0 disables. |

## StateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `store` | `Literal['file']` | file | no | State backend (local JSON file is the only backend). |
| `file` | `FileStateConfig` |  | no | File-backed state store settings. |

## FileStateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `path` | `Path` | /var/lib/sf2loki/state.json | no | Checkpoint file path; persist on a mounted volume for durable resume. |

## ServiceConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `log_level` | `Literal['debug', 'info', 'warning', 'warn', 'error', 'critical']` | info | no | Application log level: debug \| info \| warning \| error \| critical (case-insensitive). |
| `log_format` | `Literal['json', 'logfmt']` | json | no | Application log output format. |
| `health_addr` | `str` | ":8080" | no | Address to bind the health-check HTTP server. |
| `shutdown_grace` | `Duration` | 25s | no | Grace period allowed for in-flight work to finish on shutdown. |
| `telemetry` | `TelemetryConfig` |  | no | OTLP metrics egress settings. |

## TelemetryConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Push all metrics via OTLP/HTTP. |
| `endpoint` | `str` | https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics | no | Full OTLP/HTTP metrics URL. For Grafana Cloud this is the stack OTLP gateway, e.g. https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics; for a local Alloy otelcol.receiver.otlp, e.g. http://alloy:4318/v1/metrics. |
| `auth` | `Literal['basic', 'none']` | basic | no | Auth for the OTLP endpoint. "basic" sends Authorization: Basic base64(user:token); "none" sends none (e.g. in-cluster Alloy). For basic, the credentials default to the Loki sink's tenant_id/auth_token when left blank — Grafana Cloud uses one stack credential for both Loki and OTLP. |
| `basic_auth_user` | `str` | "" | no | Basic-auth username; defaults to the Loki sink's tenant_id when blank. |
| `basic_auth_token` | `SecretStr` | *(secret)* | no | Basic-auth token, inline; defaults to the Loki sink's auth_token when blank. |
| `basic_auth_token_file` | `Path` | *(secret)* | no | File path to the basic-auth token (alternative to basic_auth_token). |
| `headers` | `dict[str, str]` | {} | no | Explicit headers, merged on top of any computed Authorization header. Values support ${ENV} interpolation at config load. |
| `export_interval` | `Duration` | 1m | no | How often to export accumulated metrics via OTLP. |
| `resource_attributes` | `dict[str, str]` | {} | no | Extra OTel resource attributes merged onto the defaults (service.name, etc.). |
