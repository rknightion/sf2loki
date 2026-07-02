# sf2loki configuration reference

## Config

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `salesforce` | `SalesforceConfig` |  | no | Single-org Salesforce connection and authentication. Set this OR `orgs` (exactly one). Omit when using the multi-org `orgs` list. |
| `orgs` | `list[OrgConfig]` |  | no | Multi-org list: ingest several Salesforce orgs from one process into one shared sink. Set this OR top-level `salesforce` (exactly one). Each entry carries its own salesforce + sources; the sink/state/service stay shared. |
| `sources` | `SourcesConfig` |  | no | Single-org event source selection and settings. Ignored (and rejected if customized) when `orgs` is used — put per-org sources under each org. |
| `sink` | `SinkConfig` |  | yes | Log sink settings. |
| `state` | `StateConfig` |  | no | Checkpoint/state store settings. |
| `coordinate` | `CoordinateConfig` |  | no | Leadership coordination for active-passive HA. |
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

## OrgConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | prod | yes | Org identifier: becomes the `org` stream label and the checkpoint key prefix. Letters, digits, underscore, hyphen only; must be unique. |
| `salesforce` | `SalesforceConfig` |  | yes | This org's Salesforce connection and authentication. |
| `sources` | `SourcesConfig` |  | no | This org's event source selection and settings. |

## SourcesConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `pubsub` | `PubSubConfig` |  | no | Pub/Sub API streaming source. |
| `eventlog_objects` | `EventLogObjectsConfig` |  | no | Event-object SOQL polling source. |
| `eventlogfile` | `EventLogFileConfig` |  | no | EventLogFile (CSV) ingestion source. |
| `apexlog` | `ApexLogConfig` |  | no | ApexLog (Tooling API debug log) polling source. |
| `allow_overlap` | `bool` | false | no | Bypass the fail-fast overlap guard that refuses to start when one event category is enabled on more than one source. |
| `transform_salt` | `SecretStr` | *(secret)* | no | Deployment-wide salt for `hash` transform rules (stable pseudonyms that still correlate within this deployment). Strongly recommended whenever a hash rule is configured — unsalted hashes of low-entropy values (IPs, usernames) are trivially reversible by table lookup. |
| `transform_salt_file` | `Path` | /etc/sf2loki/secrets/transform-salt | no | File path to the transform hash salt (alternative to transform_salt). |

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
| `rediscovery_interval` | `Duration` | 1h | no | How often to re-run wildcard ("*") topic discovery while running, so channels enabled after startup are picked up without a restart. 0 disables (discovery then runs only at startup). |
| `sample` | `dict[str, typing.Annotated[float, FieldInfo(annotation=NoneType, required=True, metadata=[Gt(gt=0.0), Le(le=1.0)])]]` | {/event/ApiEventStream: 0.25} | no | Opt-in lossy volume control: topic glob -> keep fraction (0-1], first matching glob wins. Sampling is deterministic by replay_id hash, so a replay keeps exactly the same rows (Loki dedup stays intact). Sampled-out events still advance checkpoints. |
| `transforms` | `list[TransformRule]` |  | no | Redaction/filter rules applied to each decoded event payload before shaping (see TransformRule). |
| `bridge_max_bytes` | `int` | 134217728 | no | Approximate byte budget for the Pub/Sub source's internal topic->events bridge queue, which sits UPSTREAM of the pipeline's sink.loki.batch.queue_max_bytes. Topic tasks block (structural backpressure, propagated to flow-control credits) once the bridged-but-undrained bytes exceed this, so a sink outage can't balloon per-org buffering past the count bound. Default 128 MiB; 0 disables byte accounting on the bridge. |

## TransformRule

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `action` | `Literal['hash', 'mask', 'drop_field', 'drop_row', 'regex_replace']` | null | yes | hash (salted SHA-256 -> stable pseudonym) \| mask (format-aware: emails keep the domain, IPv4 truncates to /24, else '***') \| drop_field \| drop_row (row filter via `match`) \| regex_replace (pattern -> replacement). |
| `fields` | `list[str]` | [SOURCE_IP, CLIENT_IP] | no | Payload field names the action applies to (required for hash/mask/drop_field/regex_replace; not used by drop_row). |
| `match` | `dict[str, str]` | {} | no | drop_row only: drop rows where EVERY field equals (or glob-matches) its value, e.g. {EVENT_TYPE: Sites}. |
| `pattern` | `str` | null | no | regex_replace only: the regular expression to replace (must compile). |
| `replacement` | `str` | "" | no | regex_replace only: the replacement text (backrefs allowed). |
| `name` | `str` | "" | no | Optional stable rule name used as the `rule` metric label for drop_row counts; defaults to "<action>-<index>". |

## EventLogObjectsConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Enable the event-object polling source. |
| `objects` | `list[EventLogObjectConfig]` |  | no | Event objects to poll. |
| `transforms` | `list[TransformRule]` |  | no | Redaction/filter rules applied to each polled record before shaping (see TransformRule). |

## EventLogObjectConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | "" | yes | The event object API name to poll (e.g. LoginEvent). |
| `timestamp_field` | `str` | EventDate | no | The field used as the per-row event time for polling/checkpointing. |
| `poll_interval` | `Duration` | 5m | no | How often to poll this object. |
| `lookback` | `Duration` | 1h | no | Initial window to fetch on first run (no checkpoint). |
| `sample` | `float` | 1.0 | no | Opt-in lossy volume control: keep fraction (0-1] of rows, deterministic by record Id hash (replay-stable). 1.0 keeps everything. |
| `big_object` | `bool` | false | no | Set true for Salesforce Big Objects (the stored RTEM event family: LoginEvent, ApiEvent, FileEventStore, *EventStore, ...). Big Objects reject ORDER BY ASC, so the source drains them newest-first (ORDER BY timestamp_field DESC) with a ratcheting upper bound and re-sorts each cycle's window ascending before emitting. Leave false for standard and custom objects (LoginHistory, MyAudit__c), which use the ASC path. |
| `max_catchup_records` | `int` | 200000 | no | Cap on records collected into memory per big_object DESC drain cycle. The drain buffers a cycle's window in memory to re-sort it ascending; a post-outage catch-up over a large gap would otherwise be unbounded and can OOM. When the cap is hit the drain emits that (internally sorted) segment and ratchets its upper bound down so catch-up proceeds in bounded chunks across cycles. 0 = unbounded (pre-cap behaviour). Ignored on the ASC path (streamed page-by-page, already bounded). |

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
| `settle_window` | `Duration` | 0s | no | Skip files whose CreatedDate is newer than now-settle_window, so we don't pull a half-written hourly CSV whose tail rows would then be skipped when the watermark passes it. Left unset it defaults to 5m for interval: Hourly (Hourly blobs can be listed while server-side incomplete) and 0 for Daily (files land long after the day closes). Set explicitly to override either. |
| `download_max_age` | `Duration` | 1d | no | A file whose body keeps failing to download and is older than this is abandoned (checkpoint advances past it) so a permanently-missing file can't wedge the watermark forever. Files younger than this are retried. |
| `transforms` | `list[TransformRule]` |  | no | Redaction/filter rules applied to each CSV row before shaping (see TransformRule). |
| `concurrency` | `int` | 4 | no | Event types processed concurrently per poll cycle (per-type ordering and checkpoints are unaffected — types are independent). Peak memory is roughly concurrency x 8 MiB of download spool. |

## EventLogFileTypeConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `name` | `str` | "" | yes | The ELF EventType this override applies to (e.g. ReportExport), or "*" to discover all types. |
| `structured_metadata_fields` | `list[str]` | [REPORT_ID, OWNER_ID] | no | Per-type override of the global sink.loki.structured_metadata_fields; omit (None) to inherit the global list, or set to [] to suppress it. |
| `labels` | `list[str]` | [DELEGATED_USER] | no | Columns promoted to Loki stream labels for this event type. Keep these LOW cardinality — each distinct value is a new Loki stream. |
| `sample` | `float` | 1.0 | no | Opt-in lossy volume control: keep fraction (0-1] of this type's rows, deterministic by row key hash (replay-stable, Loki-dedup-safe). 1.0 keeps everything. |

## ApexLogConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `enabled` | `bool` | false | no | Enable the ApexLog polling source. |
| `poll_interval` | `Duration` | 1m | no | How often to poll for new ApexLog rows. |
| `lookback` | `Duration` | 1h | no | Initial window to fetch on first run (no checkpoint). |
| `users` | `list[str]` | [integration@example.com] | no | Salesforce usernames whose logs to ingest (matched via LogUser.Username). Empty = every ApexLog visible to the integration user. |
| `max_body_bytes` | `int` | 5242880 | no | Skip the body download for logs whose LogLength exceeds this (the metadata line is still shipped, flagged body_skipped); the per-line cap (sink.loki.batch.max_line_bytes) truncates whatever is shipped. |
| `sample` | `float` | 1.0 | no | Opt-in lossy volume control: keep fraction (0-1] of logs, deterministic by log Id hash (replay-stable). 1.0 keeps everything. |
| `transforms` | `list[TransformRule]` |  | no | Redaction/filter rules applied to each log's metadata before shaping. |

## SinkConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
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
| `egress` | `EgressConfig` |  | no | Egress guardrails: rate caps + daily byte budget (all off by default). |
| `labels` | `dict[str, str]` | {environment: prod} | no | Static stream labels merged onto every push (job + sf_org_id are added automatically). |
| `structured_metadata_fields` | `list[str]` | [replay_id, schema_id, event_uuid, user_id, username, source_ip, session_key] | no | Event fields promoted to Loki structured metadata (not stream labels). |

## LokiBatchConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `max_entries` | `int` | 1000 | no | Flush the batch after this many log entries. |
| `max_bytes` | `int` | 1048576 | no | Flush the batch after this many bytes. |
| `flush_interval` | `Duration` | 1s | no | Flush the batch after this much time, regardless of size. |
| `max_line_bytes` | `int` | 262144 | no | Per-line UTF-8 byte cap; lines longer than this are truncated (with a marker) before push so one oversized row can't 400 its whole batch. Mirrors Loki's server-side `max_line_size` default (256 KiB). 0 disables. |
| `queue_maxsize` | `int` | 10000 | no | Entry-count bound of each internal source->sink lane queue. Producers block (structural backpressure) when full. Applied PER LANE (streaming vs bulk), so the worst-case entry count is queue_maxsize x number-of-lanes (<= 2). |
| `queue_max_bytes` | `int` | 268435456 | no | Approximate byte budget for queued entries, applied PER LANE (streaming vs bulk). Producers on a lane block when its budget is exceeded, even if the entry-count bound is not reached. Worst-case buffered memory during a sink outage is therefore queue_max_bytes x number-of-lanes (<= 2x). Default 256 MiB; 0 disables accounting. |

## EgressConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `max_lines_per_second` | `float` | 0 | no | Token-bucket cap on pushed lines/second; 0 disables. |
| `max_bytes_per_second` | `float` | 0 | no | Token-bucket cap on pushed (pre-compression) bytes/second; 0 disables. |
| `daily_byte_budget` | `int` | 0 | no | Maximum pre-compression bytes pushed per UTC day; 0 disables. The used counter persists in the state store, so restarts don't reset it. WARN at 80%, ERROR + budget_action at 100%. |
| `budget_action` | `Literal['pause', 'drop']` | pause | no | What to do when the daily budget is exhausted: pause (hold pushes + checkpoints until the next UTC day; data delayed, never lost, readiness reports degraded) \| drop (keep running, discard over-budget entries). |

## StateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `store` | `Literal['file', 's3', 'gcs']` | file | no | State backend: file (local JSON, needs a persistent volume) \| s3 (S3-compatible object storage, for stateless deployments; needs the sf2loki[s3] extra) \| gcs (Google Cloud Storage, for stateless deployments; needs the sf2loki[gcs] extra). |
| `file` | `FileStateConfig` |  | no | File-backed state store settings. |
| `s3` | `S3StateConfig` |  | no | S3-backed state store settings. |
| `gcs` | `GcsStateConfig` |  | no | GCS-backed state store settings. |

## FileStateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `path` | `Path` | /var/lib/sf2loki/state.json | no | Checkpoint file path; persist on a mounted volume for durable resume. |

## S3StateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `bucket` | `str` | "" | no | Bucket name (required when state.store is s3). |
| `key` | `str` | sf2loki/state.json | no | Object key holding the checkpoint document. |
| `region` | `str` | null | no | AWS region; omit to use the default-chain region. |
| `endpoint_url` | `str` | http://minio:9000 | no | Custom S3 endpoint for MinIO/R2/Ceph; omit for AWS S3. |

## GcsStateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `bucket` | `str` | "" | no | Bucket name (required when state.store is gcs). |
| `object_name` | `str` | sf2loki/state.json | no | Object name holding the checkpoint document. |
| `service_file` | `Path` | null | no | Path to a service-account JSON key; omit to use Application Default Credentials. |

## CoordinateConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `type` | `Literal['noop', 'file_lease', 'k8s_lease']` | noop | no | noop (single instance) \| file_lease (active-passive via a shared lease file) \| k8s_lease (active-passive via a Kubernetes Lease; needs the sf2loki[k8s] extra). |
| `file_lease` | `FileLeaseConfig` |  | no | File-lease coordinator settings. |
| `k8s_lease` | `K8sLeaseConfig` |  | no | Kubernetes-Lease coordinator settings. |

## FileLeaseConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `path` | `Path` | /var/lib/sf2loki/leader.lease | no | Lease file path on storage shared by all replicas. |
| `ttl` | `Duration` | 30s | no | Lease lifetime: a standby takes over once the lease is this stale. Failover time is bounded by ttl; must exceed inter-host clock skew. |
| `renew_interval` | `Duration` | 10s | no | How often the leader re-writes the lease (must be < ttl/2). |
| `holder_id` | `str` | "" | no | Stable identity written into the lease; defaults to hostname-pid at startup. Set explicitly when hostnames aren't unique. |

## K8sLeaseConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `namespace` | `str` | default | no | Namespace holding the Lease object. |
| `name` | `str` | sf2loki-leader | no | Lease object name (shared by all replicas). |
| `identity` | `str` | "" | no | holderIdentity written into the Lease; defaults to the pod name ($HOSTNAME) at startup. |
| `lease_duration` | `Duration` | 30s | no | Lease lifetime: a standby takes over once the lease is this stale. Failover time is bounded by this. |
| `renew_interval` | `Duration` | 10s | no | How often the leader renews the Lease (must be < lease_duration/2). |
| `kubeconfig` | `Path` | null | no | Path to a kubeconfig for out-of-cluster dev; omit to use in-cluster config. |

## ServiceConfig

| Field | Type | Default | Required | Description |
| --- | --- | --- | --- | --- |
| `log_level` | `Literal['debug', 'info', 'warning', 'warn', 'error', 'critical']` | info | no | Application log level: debug \| info \| warning \| error \| critical (case-insensitive). |
| `log_format` | `Literal['json', 'logfmt']` | json | no | Application log output format. |
| `health_addr` | `str` | ":8080" | no | Address to bind the health-check HTTP server. |
| `shutdown_grace` | `Duration` | 25s | no | Grace period allowed for in-flight work to finish on shutdown. |
| `unready_after_sink_failing` | `Duration` | 15m | no | /readyz reports 503 when Loki pushes have been failing continuously for this long (data is retried and safe, but the instance is degraded and an orchestrator should surface it). 0 disables the readiness degradation. |
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
