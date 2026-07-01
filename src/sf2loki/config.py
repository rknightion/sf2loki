"""Configuration: pydantic models loaded from YAML and/or env, with file-injected secrets.

Precedence (highest first): environment (``SF2LOKI_*`` with ``__`` nesting) >
YAML file > model defaults. Secrets come from ``*_file`` paths or inline; a
missing/unreadable secret file is fatal at load time (no silent blanks).
"""

from __future__ import annotations

import base64
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    model_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigError(Exception):
    """Raised for any invalid or unreadable configuration."""


class StrictModel(BaseModel):
    """Base for all config models: unknown keys are fatal, not ignored.

    A typo'd key (e.g. ``event_type:`` for ``event_types:``) must fail loudly
    at load time rather than silently disabling the feature it was meant to
    configure (``--check`` would otherwise pass).
    """

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# Duration shorthand: "5m", "1h30m", "25s", "500ms" (DESIGN.md §11), in
# addition to pydantic's own timedelta/ISO-8601/numeric-seconds parsing.

_DURATION_TOKEN_RE = re.compile(r"(\d+(?:\.\d+)?)(ms|s|m|h|d|w)")
_DURATION_UNIT_SECONDS: dict[str, float] = {
    "ms": 0.001,
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
}


def _parse_duration(value: object) -> object:
    """Accept Go-style shorthand strings; pass everything else through unchanged.

    Non-strings (timedelta, int, float) and strings that don't look like
    shorthand (ISO-8601 "PT5M", plain numeric "30") fall through to pydantic's
    own timedelta parsing.
    """
    if not isinstance(value, str):
        return value
    tokens = list(_DURATION_TOKEN_RE.finditer(value))
    if not tokens or sum(len(t.group(0)) for t in tokens) != len(value):
        return value
    total_seconds = sum(float(t.group(1)) * _DURATION_UNIT_SECONDS[t.group(2)] for t in tokens)
    return timedelta(seconds=total_seconds)


Duration = Annotated[timedelta, BeforeValidator(_parse_duration)]


# ---------------------------------------------------------------------------
# ${VAR} interpolation in YAML-sourced values (DESIGN.md §11): a referenced
# environment variable that is unset is fatal at load time (fail fast, no
# silent blanks — same policy as *_file secrets below).

_ENV_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _interpolate_env(value: Any) -> Any:
    if isinstance(value, str):
        if not _ENV_VAR_RE.search(value):
            return value

        def _sub(match: re.Match[str]) -> str:
            name = match.group(1)
            try:
                return os.environ[name]
            except KeyError:
                raise ConfigError(
                    f"config references undefined environment variable ${{{name}}}"
                ) from None

        return _ENV_VAR_RE.sub(_sub, value)
    if isinstance(value, dict):
        return {k: _interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_interpolate_env(v) for v in value]
    return value


class SalesforceLimitsConfig(StrictModel):
    """Org-limits metric poller.

    Polls ``/services/data/vXX.0/limits`` and emits a max/remaining gauge per
    limit (DailyApiRequests, DataStorageMB, DailyStreamingApiEvents, ...). Cheap
    (one REST call per interval) and useful product telemetry.
    """

    enabled: bool = Field(
        default=False,
        description=(
            "Poll /services/data/vXX.0/limits for org-limit gauges "
            "(API usage, storage, streaming events, ...)."
        ),
    )
    poll_interval: Duration = Field(
        default=timedelta(minutes=5), description="How often to poll the limits endpoint."
    )


class SalesforceConfig(StrictModel):
    """Salesforce org connection and authentication.

    Two OAuth flows are supported via ``auth_mode``: ``jwt_bearer`` (asymmetric
    key + cert, no shared secret) or ``client_credentials`` (consumer key +
    secret, no keypair/cert/pre-auth). Defaults to ``jwt_bearer`` for backward
    compatibility.
    """

    login_url: str = Field(
        default="",
        description=(
            "Optional: derived from `environment` when omitted; set to a custom "
            "My Domain URL to override. An explicit value always takes precedence "
            "over the environment-derived default."
        ),
    )
    environment: Literal["production", "sandbox"] = Field(
        default="production",
        description=(
            "production | sandbox — derives login_url when login_url is unset; "
            "an explicit login_url takes precedence."
        ),
    )
    auth_mode: Literal["jwt_bearer", "client_credentials"] = Field(
        default="jwt_bearer",
        description=(
            "jwt_bearer (private key + cert) | client_credentials (consumer key + secret)."
        ),
    )
    client_id: str = Field(
        min_length=1,
        description="External Client App consumer key.",
        examples=["${SF_CLIENT_ID}"],
    )
    client_secret: SecretStr | None = Field(
        default=None,
        description=(
            "client_credentials flow secret (injectable from file/env like the key). "
            "Required when auth_mode is client_credentials; unused for jwt_bearer."
        ),
    )
    client_secret_file: Path | None = Field(
        default=None,
        description=(
            "File path to the client_credentials secret; required when "
            "auth_mode: client_credentials."
        ),
        examples=["/etc/sf2loki/secrets/client-secret"],
    )
    username: str = Field(
        default="",
        description=(
            "Integration user (pre-authorised on the app's Policies tab). Required "
            "for jwt_bearer (the JWT `sub` claim); not needed when auth_mode: client_credentials."
        ),
        examples=["svc@example.com"],
    )
    private_key_file: Path | None = Field(
        default=None,
        description=(
            "File path to the jwt_bearer private key; not needed when "
            "auth_mode: client_credentials."
        ),
        examples=["/etc/sf2loki/secrets/server.key"],
    )
    private_key: SecretStr | None = Field(
        default=None,
        description="jwt_bearer private key, inline (alternative to private_key_file).",
    )
    api_version: str = Field(
        default="60.0", description="Salesforce REST/SOAP API version to target."
    )
    org_id: str | None = Field(
        default=None,
        description=(
            "Set this to keep the app on the `api` scope alone; left null it is "
            "auto-resolved via /services/oauth2/userinfo (which then needs the `openid` scope)."
        ),
    )
    token_ttl: Duration = Field(
        default=timedelta(hours=1),
        description=(
            "Assumed access-token lifetime (Salesforce returns no expires_in for "
            "these flows; the real lifetime is the org's session timeout, which "
            "can be as short as 15m). Refresh is also reactive on 401, so this "
            "only tunes proactive re-mint cadence."
        ),
    )
    limits: SalesforceLimitsConfig = Field(
        default_factory=SalesforceLimitsConfig, description="Org-limits metric poller settings."
    )

    @model_validator(mode="after")
    def _resolve_login_url_and_validate_mode(self) -> SalesforceConfig:
        # Derive login_url from environment when not set explicitly. An explicit
        # custom My Domain URL is left untouched (takes precedence).
        if not self.login_url:
            self.login_url = (
                "https://test.salesforce.com"
                if self.environment == "sandbox"
                else "https://login.salesforce.com"
            )
        # jwt_bearer needs a username for the JWT ``sub`` claim; client_credentials
        # runs as the External Client App's "Run As" user and needs none.
        if self.auth_mode == "jwt_bearer" and not self.username:
            raise ValueError("salesforce.username is required when auth_mode is 'jwt_bearer'")
        # Salesforce rejects the client_credentials grant at the generic
        # login/test hosts — only the org's My Domain token endpoint works.
        if self.auth_mode == "client_credentials" and self.login_url in (
            "https://login.salesforce.com",
            "https://test.salesforce.com",
        ):
            raise ValueError(
                "auth_mode 'client_credentials' requires the org's My Domain token "
                f"endpoint; Salesforce rejects this grant at {self.login_url}. Set "
                "salesforce.login_url to your My Domain URL, e.g. "
                "https://yourorg.my.salesforce.com"
            )
        return self


class PubSubConfig(StrictModel):
    """Salesforce Pub/Sub API (real-time event streaming) source."""

    enabled: bool = Field(default=True, description="Enable the Pub/Sub streaming source.")
    endpoint: str = Field(
        default="api.pubsub.salesforce.com:7443", description="Pub/Sub API gRPC endpoint."
    )
    default_num_requested: int = Field(
        default=100,
        ge=1,
        le=100,
        description=(
            "Flow-control batch size (1-100; Salesforce clamps at 100 and returns "
            "INVALID_ARGUMENT when over-asked)."
        ),
    )
    replay_preset: Literal["LATEST", "EARLIEST", "CUSTOM"] = Field(
        default="CUSTOM",
        description="Replay position; falls back to LATEST when no stored replay_id.",
    )
    topics: list[str] = Field(
        default_factory=list,
        description=(
            'Explicit topics, or "*" to DISCOVER and subscribe to every RTEM stream the '
            "org exposes (the *EventStream channels), re-filtered by include/exclude."
        ),
        examples=[["/event/LoginEventStream", "/event/ApiAnomalyEvent"]],
    )
    include: list[str] = Field(
        default_factory=lambda: ["*"],
        description="Operator inclusion globs applied to discovered/explicit topics.",
    )
    exclude: list[str] = Field(
        default_factory=list,
        description="Operator exclusion globs applied to discovered/explicit topics.",
    )


# SOQL identifier safety: object/field/EventType names are interpolated into
# SOQL query strings, so restrict them to bare identifiers — a stray quote
# would otherwise surface as a runtime MALFORMED_QUERY crash mid-poll.
_SOQL_IDENTIFIER_PATTERN = r"^[A-Za-z0-9_]+$"
_SOQL_IDENTIFIER_RE = re.compile(_SOQL_IDENTIFIER_PATTERN)


class EventLogObjectConfig(StrictModel):
    """A single Salesforce event object polled via SOQL."""

    name: str = Field(
        min_length=1,
        pattern=_SOQL_IDENTIFIER_PATTERN,
        description="The event object API name to poll (e.g. LoginEvent).",
    )
    timestamp_field: str = Field(
        default="EventDate",
        min_length=1,
        pattern=_SOQL_IDENTIFIER_PATTERN,
        description="The field used as the per-row event time for polling/checkpointing.",
    )
    poll_interval: Duration = Field(
        default=timedelta(minutes=5), description="How often to poll this object."
    )
    lookback: Duration = Field(
        default=timedelta(hours=1),
        description="Initial window to fetch on first run (no checkpoint).",
    )


class EventLogObjectsConfig(StrictModel):
    """Poll stored event objects via SOQL.

    Use for categories you'd rather poll than stream (or that have no stream).
    Do NOT also stream the same category.
    """

    enabled: bool = Field(default=False, description="Enable the event-object polling source.")
    objects: list[EventLogObjectConfig] = Field(
        default_factory=list, description="Event objects to poll."
    )


# Label keys injected/reserved by the pipeline + sink; a promoted ELF column
# must not reuse these (it would clobber source identity or be silently
# overridden by the injected static labels in app._produce).
_RESERVED_LABEL_KEYS: frozenset[str] = frozenset(
    {"source", "event_type", "job", "sf_org_id", "environment"}
)
_LABEL_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


# Wildcard event-type: expands (at poll time) to every EventType the org
# currently produces for the configured interval — see EventLogFileSource.
EVENT_TYPE_WILDCARD = "*"


class EventLogFileTypeConfig(StrictModel):
    """Per-event-type ELF ingestion config.

    A bare string in ``eventlogfile.event_types`` coerces to ``{name: <string>}``
    (backward compatible). ``structured_metadata_fields`` defaults to ``None``,
    meaning "fall back to the global ``sink.loki.structured_metadata_fields``";
    set it to a list (including ``[]``) to override per type. ``labels`` lists
    columns promoted to stream labels — keep these LOW cardinality.
    """

    name: str = Field(
        min_length=1,
        description=(
            'The ELF EventType this override applies to (e.g. ReportExport), or "*" '
            "to discover all types."
        ),
    )
    structured_metadata_fields: list[str] | None = Field(
        default=None,
        description=(
            "Per-type override of the global sink.loki.structured_metadata_fields; "
            "omit (None) to inherit the global list, or set to [] to suppress it."
        ),
        examples=[["REPORT_ID", "OWNER_ID"]],
    )
    labels: list[str] = Field(
        default_factory=list,
        description=(
            "Columns promoted to Loki stream labels for this event type. Keep these "
            "LOW cardinality — each distinct value is a new Loki stream."
        ),
        examples=[["DELEGATED_USER"]],
    )

    @model_validator(mode="after")
    def _validate_labels(self) -> EventLogFileTypeConfig:
        # EventType names are interpolated into the SOQL file-listing query; a
        # non-identifier (stray quote, semicolon, ...) would crash at poll time
        # with MALFORMED_QUERY. "*" is the discovery wildcard, allowed as-is.
        if self.name != EVENT_TYPE_WILDCARD and not _SOQL_IDENTIFIER_RE.match(self.name):
            raise ValueError(
                f"eventlogfile event type {self.name!r} is not a valid EventType name "
                '(must match [A-Za-z0-9_]+ or be the discovery wildcard "*")'
            )
        for label in self.labels:
            if label in _RESERVED_LABEL_KEYS:
                raise ValueError(
                    f"eventlogfile type {self.name!r}: cannot promote reserved label "
                    f"key {label!r} (reserved: {', '.join(sorted(_RESERVED_LABEL_KEYS))})"
                )
            if not _LABEL_NAME_RE.match(label):
                raise ValueError(
                    f"eventlogfile type {self.name!r}: promoted label {label!r} is not a "
                    "valid Loki label name (must match [A-Za-z_][A-Za-z0-9_]*)"
                )
        return self


def _coerce_event_type(value: object) -> object:
    """Coerce a bare string event-type into a per-type mapping."""
    if isinstance(value, str):
        return {"name": value}
    return value


def _coerce_event_types(value: object) -> object:
    if isinstance(value, list):
        return [_coerce_event_type(v) for v in value]
    return value


class EventLogFileConfig(StrictModel):
    """EventLogFile (CSV) ingestion.

    Salesforce exposes ~70 EventType values (query ``SELECT EventType FROM
    EventLogFile``, or the EventType picklist, to see which your org actually
    produces); schema is read per-file. This is the workhorse for breadth —
    most activity only surfaces here, not as a streaming event.

    Ingest exactly ONE interval (``Hourly`` or ``Daily``); hourly and daily
    files are redundant copies of the same events (Salesforce), so ingesting
    both double-counts. ``Daily`` is settled (~1 day lag) and works for every
    org; ``Hourly`` is fresher but needs the Event Monitoring hourly opt-in in
    Setup, and some orgs generate ONLY Daily files — check before choosing
    Hourly.

    Use ``event_types: ["*"]`` to DISCOVER and ingest every EventType the org
    produces for the configured interval (re-checked each poll, so newly
    enabled types appear with no restart), then use ``exclude`` to drop
    categories owned by another source or high-volume types you don't want.
    Or list types explicitly — each item is a bare string (uses the global
    structured_metadata_fields, promotes no labels) or a per-type object
    (name + optional structured_metadata_fields/labels overrides). Explicit
    entries always win over discovered ones.
    """

    enabled: bool = Field(default=False, description="Enable the EventLogFile ingestion source.")
    interval: Literal["Hourly", "Daily"] = Field(
        default="Hourly",
        description=(
            "Daily (settled, ~1d lag) | Hourly (fresher, needs the Event Monitoring "
            "hourly opt-in in Setup) — pick ONE. Many orgs generate ONLY Daily files; "
            "check before Hourly."
        ),
    )
    event_types: Annotated[list[EventLogFileTypeConfig], BeforeValidator(_coerce_event_types)] = (
        Field(
            default_factory=list,
            description=(
                "ELF EventTypes to ingest (required when enabled — there is no sensible "
                '"all" default given ~70 types and the either/or-per-category model). '
                'Each item is a bare string (e.g. "Login") or a per-type object (name + '
                'optional structured_metadata_fields/labels). Use "*" to discover and '
                "ingest every EventType the org produces for this interval."
            ),
            examples=[["API", "RestApi", "BulkApi", "Report", "Login"]],
        )
    )
    exclude: list[str] = Field(
        default_factory=list,
        description=(
            'EventTypes to skip when the wildcard "*" is used (e.g. a category served '
            "by another source, or a high-volume type you don't want). Ignored otherwise."
        ),
    )
    poll_interval: Duration = Field(
        default=timedelta(hours=1),
        description="How often to list new files (Daily files land ~once/day).",
    )
    lookback: Duration = Field(
        default=timedelta(hours=24),
        description=(
            "Initial window on first run (no checkpoint); reach back far enough to "
            "catch the last few settled Daily files."
        ),
    )
    timestamp_column: str = Field(
        default="TIMESTAMP_DERIVED", description="Per-row event time column."
    )
    page_size: int = Field(default=1000, description="SOQL LIMIT for the file-listing query.")
    settle_window: Duration = Field(
        default=timedelta(0),
        description=(
            "Skip files whose CreatedDate is newer than now-settle_window, so we don't "
            "pull a half-written hourly CSV. 0 disables (safe for Daily); use a few "
            "minutes for Hourly."
        ),
    )
    download_max_age: Duration = Field(
        default=timedelta(hours=24),
        description=(
            "A file whose body keeps failing to download and is older than this is "
            "abandoned (checkpoint advances past it) so a permanently-missing file "
            "can't wedge the watermark forever. Files younger than this are retried."
        ),
    )

    @property
    def discover(self) -> bool:
        """True when the wildcard "*" is present — ingest all discovered EventTypes."""
        return any(t.name == EVENT_TYPE_WILDCARD for t in self.event_types)

    @model_validator(mode="after")
    def _require_event_types_when_enabled(self) -> EventLogFileConfig:
        if self.enabled and not self.event_types:
            raise ValueError(
                "eventlogfile.enabled is true but event_types is empty; "
                'list the ELF EventType values to ingest (e.g. [Login, API]) or "*" to '
                "discover and ingest all types the org produces"
            )
        return self


class SourcesConfig(StrictModel):
    """Event source selection: Pub/Sub streaming, event-object polling, and EventLogFile.

    Either/or per event category: by default ingest each category (Login,
    API, Report, ...) from exactly ONE source. Streaming
    ``/event/LoginEventStream`` AND polling ``LoginEvent`` AND pulling the
    "Login" EventLogFile are the SAME activity via different channels. A
    fail-fast guard refuses to start on an EXPLICIT overlap, and the ELF "*"
    wildcard auto-excludes categories a stream/object source already owns.
    Set ``allow_overlap: true`` to ingest a category via MULTIPLE sources on
    purpose — e.g. the real-time-but-lean stream AND the slower-but-richer
    EventLogFile rows for the same category (they are NOT byte-identical, so
    both flow — build dashboards on whichever fits).
    """

    pubsub: PubSubConfig = Field(
        default_factory=PubSubConfig, description="Pub/Sub API streaming source."
    )
    eventlog_objects: EventLogObjectsConfig = Field(
        default_factory=EventLogObjectsConfig, description="Event-object SOQL polling source."
    )
    eventlogfile: EventLogFileConfig = Field(
        default_factory=EventLogFileConfig, description="EventLogFile (CSV) ingestion source."
    )
    allow_overlap: bool = Field(
        default=False,
        description=(
            "Bypass the fail-fast overlap guard that refuses to start when one "
            "event category is enabled on more than one source."
        ),
    )


class LokiBatchConfig(StrictModel):
    """Loki push batching."""

    max_entries: int = Field(
        default=1000, description="Flush the batch after this many log entries."
    )
    max_bytes: int = Field(default=1_048_576, description="Flush the batch after this many bytes.")
    flush_interval: Duration = Field(
        default=timedelta(seconds=1),
        description="Flush the batch after this much time, regardless of size.",
    )
    max_line_bytes: int = Field(
        default=262144,
        description=(
            "Per-line UTF-8 byte cap; lines longer than this are truncated (with a "
            "marker) before push so one oversized row can't 400 its whole batch. "
            "Mirrors Loki's server-side `max_line_size` default (256 KiB). 0 disables."
        ),
    )


class LokiConfig(StrictModel):
    """Loki push-API sink.

    Grafana Cloud: https://logs-prod-xx.grafana.net/loki/api/v1/push (+ tenant_id + auth_token).
    Self-hosted: http://loki:3100/loki/api/v1/push (+ tenant_id -> X-Scope-OrgID).
    Local Alloy: http://alloy:3100/loki/api/v1/push (no auth).
    """

    url: str = Field(min_length=1, description="Loki push API URL.", examples=["${GC_LOKI}"])
    tenant_id: str | None = Field(
        default=None,
        description=(
            "Loki tenant (X-Scope-OrgID); required for Grafana Cloud and multi-tenant Loki."
        ),
        examples=["${GC_TENANT_ID}"],
    )
    auth_token_file: Path | None = Field(
        default=None,
        description="File path to the Loki auth token.",
        examples=["/etc/sf2loki/secrets/loki-token"],
    )
    auth_token: SecretStr | None = Field(
        default=None, description="Loki auth token, inline (alternative to auth_token_file)."
    )
    encoding: Literal["protobuf", "json"] = Field(
        default="protobuf", description="Wire encoding for the push request."
    )
    compression: Literal["snappy", "gzip", "none"] = Field(
        default="snappy", description="Compression: snappy (protobuf) | gzip (json) | none."
    )
    batch: LokiBatchConfig = Field(
        default_factory=LokiBatchConfig, description="Push batching settings."
    )
    labels: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Static stream labels merged onto every push (job + sf_org_id are added automatically)."
        ),
        examples=[{"environment": "prod"}],
    )
    structured_metadata_fields: list[str] = Field(
        default_factory=list,
        description="Event fields promoted to Loki structured metadata (not stream labels).",
        examples=[
            [
                "replay_id",
                "schema_id",
                "event_uuid",
                "user_id",
                "username",
                "source_ip",
                "session_key",
            ]
        ],
    )


class SinkConfig(StrictModel):
    type: Literal["loki"] = Field(
        default="loki", description="Sink backend (only loki is supported)."
    )
    loki: LokiConfig = Field(description="Loki sink settings.")


class FileStateConfig(StrictModel):
    path: Path = Field(
        default=Path("/var/lib/sf2loki/state.json"),
        description="Checkpoint file path; persist on a mounted volume for durable resume.",
    )


class StateConfig(StrictModel):
    store: Literal["file"] = Field(
        default="file", description="State backend (local JSON file is the only backend)."
    )
    file: FileStateConfig = Field(
        default_factory=FileStateConfig, description="File-backed state store settings."
    )


class TelemetryConfig(StrictModel):
    """OTLP metrics egress (self-observability + Salesforce product metrics).

    When ``enabled`` is false, metrics are still recorded in-process (cheap, used
    by tests via the in-memory reader) but exported nowhere — there is no
    Prometheus scrape endpoint; sf2loki is OTLP-push-native.
    """

    enabled: bool = Field(default=False, description="Push all metrics via OTLP/HTTP.")
    endpoint: str = Field(
        default="",
        description=(
            "Full OTLP/HTTP metrics URL. For Grafana Cloud this is the stack OTLP "
            "gateway, e.g. https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics; "
            "for a local Alloy otelcol.receiver.otlp, e.g. http://alloy:4318/v1/metrics."
        ),
        examples=["https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics"],
    )
    auth: Literal["basic", "none"] = Field(
        default="basic",
        description=(
            'Auth for the OTLP endpoint. "basic" sends Authorization: Basic '
            'base64(user:token); "none" sends none (e.g. in-cluster Alloy). For '
            "basic, the credentials default to the Loki sink's tenant_id/auth_token "
            "when left blank — Grafana Cloud uses one stack credential for both "
            "Loki and OTLP."
        ),
    )
    basic_auth_user: str = Field(
        default="",
        description="Basic-auth username; defaults to the Loki sink's tenant_id when blank.",
    )
    basic_auth_token: SecretStr | None = Field(
        default=None,
        description="Basic-auth token, inline; defaults to the Loki sink's auth_token when blank.",
    )
    basic_auth_token_file: Path | None = Field(
        default=None,
        description="File path to the basic-auth token (alternative to basic_auth_token).",
        examples=["/etc/sf2loki/secrets/otlp-token"],
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Explicit headers, merged on top of any computed Authorization header. "
            "Values support ${ENV} interpolation at config load."
        ),
    )
    export_interval: Duration = Field(
        default=timedelta(seconds=60),
        description="How often to export accumulated metrics via OTLP.",
    )
    resource_attributes: dict[str, str] = Field(
        default_factory=dict,
        description="Extra OTel resource attributes merged onto the defaults (service.name, etc.).",
    )


def _lowercase_str(value: object) -> object:
    return value.lower() if isinstance(value, str) else value


LogLevel = Literal["debug", "info", "warning", "warn", "error", "critical"]


class ServiceConfig(StrictModel):
    log_level: Annotated[LogLevel, BeforeValidator(_lowercase_str)] = Field(
        default="info",
        description=(
            "Application log level: debug | info | warning | error | critical (case-insensitive)."
        ),
    )
    log_format: Literal["json", "logfmt"] = Field(
        default="json", description="Application log output format."
    )
    health_addr: str = Field(
        default=":8080", description="Address to bind the health-check HTTP server."
    )
    shutdown_grace: Duration = Field(
        default=timedelta(seconds=25),
        description="Grace period allowed for in-flight work to finish on shutdown.",
    )
    telemetry: TelemetryConfig = Field(
        default_factory=TelemetryConfig, description="OTLP metrics egress settings."
    )


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SF2LOKI_",
        env_nested_delimiter="__",
        extra="forbid",
    )

    salesforce: SalesforceConfig = Field(
        description="Salesforce org connection and authentication."
    )
    sources: SourcesConfig = Field(
        default_factory=SourcesConfig, description="Event source selection and settings."
    )
    sink: SinkConfig = Field(description="Log sink settings.")
    state: StateConfig = Field(
        default_factory=StateConfig, description="Checkpoint/state store settings."
    )
    service: ServiceConfig = Field(
        default_factory=ServiceConfig, description="Application-level service settings."
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # env overrides the YAML-derived init values (env first = highest priority).
        return (env_settings, init_settings, dotenv_settings, file_secret_settings)


def _resolve_secret_file(
    file: Path | None, existing: SecretStr | None, what: str
) -> SecretStr | None:
    if existing is not None:
        return existing
    if file is None:
        return None
    try:
        return SecretStr(file.read_text().strip())
    except PermissionError as exc:
        raise ConfigError(
            f"cannot read {what} from {file}: permission denied. The service runs "
            "as uid 10001 in the container, so host-side secret files must be "
            "readable by it — e.g. `chmod 640` + `chown` the file to a group the "
            "container user is in, or mount the secrets group-readable "
            "(chmod 0600 root-owned files crash-loop the container)."
        ) from exc
    except OSError as exc:
        raise ConfigError(f"cannot read {what} from {file}: {exc}") from exc


def resolve_secrets(cfg: Config) -> Config:
    """Load file-injected secrets in place; fail fast on missing required secrets.

    The required Salesforce secret depends on ``auth_mode``: ``client_credentials``
    needs ``client_secret``; ``jwt_bearer`` needs ``private_key``.
    """
    sf = cfg.salesforce
    if sf.auth_mode == "client_credentials":
        sf.client_secret = _resolve_secret_file(
            sf.client_secret_file, sf.client_secret, "salesforce client secret"
        )
        if sf.client_secret is None:
            raise ConfigError(
                "salesforce client secret required for auth_mode=client_credentials "
                "(set client_secret or client_secret_file)"
            )
    else:  # jwt_bearer
        sf.private_key = _resolve_secret_file(
            sf.private_key_file, sf.private_key, "salesforce private key"
        )
        if sf.private_key is None:
            raise ConfigError(
                "salesforce private key required for auth_mode=jwt_bearer "
                "(set private_key or private_key_file)"
            )
    cfg.sink.loki.auth_token = _resolve_secret_file(
        cfg.sink.loki.auth_token_file,
        cfg.sink.loki.auth_token,
        "loki auth token",
    )
    # Telemetry basic-auth token is optional (falls back to the Loki token when
    # left unset), so resolve it if a file is given but never require it here.
    cfg.service.telemetry.basic_auth_token = _resolve_secret_file(
        cfg.service.telemetry.basic_auth_token_file,
        cfg.service.telemetry.basic_auth_token,
        "telemetry basic auth token",
    )
    # Telemetry auth="basic" with no resolvable credentials would silently
    # export unauthenticated OTLP (the gateway 401s every batch) — fail fast.
    telemetry = cfg.service.telemetry
    if telemetry.enabled and telemetry.auth == "basic":
        user = telemetry.basic_auth_user or (cfg.sink.loki.tenant_id or "")
        token = telemetry.basic_auth_token or cfg.sink.loki.auth_token
        if not (user and token):
            raise ConfigError(
                "service.telemetry.auth is 'basic' but no credentials resolve: set "
                "basic_auth_user + basic_auth_token(_file), or set the Loki sink's "
                "tenant_id + auth_token(_file) (the telemetry defaults), or use "
                "service.telemetry.auth: none for an unauthenticated endpoint"
            )
    return cfg


def telemetry_headers(telemetry: TelemetryConfig, loki: LokiConfig) -> dict[str, str]:
    """Build the final OTLP export headers (Basic auth + any explicit headers).

    For ``auth="basic"`` the credentials default to the Loki sink's
    ``tenant_id``/``auth_token`` when not set explicitly (Grafana Cloud shares one
    stack credential across Loki and OTLP). ``auth="none"`` sends no Authorization
    header (e.g. an in-cluster Alloy receiver). Explicit ``headers`` always merge
    on top. The base64 value carries no trailing newline (required by the gateway).
    """
    headers: dict[str, str] = {}
    if telemetry.auth == "basic":
        user = telemetry.basic_auth_user or (loki.tenant_id or "")
        token_secret = telemetry.basic_auth_token or loki.auth_token
        token = token_secret.get_secret_value() if token_secret is not None else ""
        if user and token:
            encoded = base64.b64encode(f"{user}:{token}".encode()).decode("ascii")
            headers["Authorization"] = f"Basic {encoded}"
    headers.update(telemetry.headers)
    return headers


def load(path: Path | None = None) -> Config:
    """Load config from an optional YAML file, apply env overrides, resolve secrets."""
    data: dict[str, Any] = {}
    if path is not None:
        try:
            raw = path.read_text()
        except OSError as exc:
            raise ConfigError(f"cannot read config file {path}: {exc}") from exc
        loaded = yaml.safe_load(raw) or {}
        if not isinstance(loaded, dict):
            raise ConfigError(f"config file {path} must be a YAML mapping")
        data = _interpolate_env(loaded)
    try:
        cfg = Config(**data)
    except ValidationError as exc:
        raise ConfigError(f"invalid configuration:\n{_format_validation_error(exc)}") from exc
    return resolve_secrets(cfg)


def _format_validation_error(exc: ValidationError) -> str:
    """Render a pydantic ValidationError as one readable line per problem.

    Unknown keys (``extra_forbidden``) get a dedicated message — they are the
    most common operator mistake (a typo'd key silently disabling a feature)
    and pydantic's default wording doesn't say "unknown key".
    """
    lines: list[str] = []
    for err in exc.errors():
        path = ".".join(str(part) for part in err["loc"]) or "<root>"
        if err["type"] == "extra_forbidden":
            lines.append(
                f"  {path}: unknown configuration key (check for typos; "
                "see docs/config-reference.md for valid keys)"
            )
        else:
            lines.append(f"  {path}: {err['msg']}")
    return "\n".join(lines)
