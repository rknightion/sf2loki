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


class TransformRule(StrictModel):
    """One declarative redaction/filter rule, applied at a source's decode boundary.

    Rules run on the decoded payload BEFORE field routing, label promotion, and
    timestamp extraction — so redacting the timestamp column triggers the
    timestamp-fallback path, and a ``drop_field`` of a label-promoted column is
    rejected at load time (it would silently drop the label).
    """

    action: Literal["hash", "mask", "drop_field", "drop_row", "regex_replace"] = Field(
        description=(
            "hash (salted SHA-256 -> stable pseudonym) | mask (format-aware: emails "
            "keep the domain, IPv4 truncates to /24, else '***') | drop_field | "
            "drop_row (row filter via `match`) | regex_replace (pattern -> replacement)."
        ),
    )
    fields: list[str] = Field(
        default_factory=list,
        description=(
            "Payload field names the action applies to (required for hash/mask/"
            "drop_field/regex_replace; not used by drop_row)."
        ),
        examples=[["SOURCE_IP", "CLIENT_IP"]],
    )
    match: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "drop_row only: drop rows where EVERY field equals (or glob-matches) "
            "its value, e.g. {EVENT_TYPE: Sites}."
        ),
    )
    pattern: str | None = Field(
        default=None,
        description="regex_replace only: the regular expression to replace (must compile).",
    )
    replacement: str = Field(
        default="",
        description="regex_replace only: the replacement text (backrefs allowed).",
    )
    name: str = Field(
        default="",
        description=(
            "Optional stable rule name used as the `rule` metric label for drop_row "
            'counts; defaults to "<action>-<index>".'
        ),
    )

    @model_validator(mode="after")
    def _validate_action_requirements(self) -> TransformRule:
        if self.action == "drop_row":
            if not self.match:
                raise ValueError("transform action 'drop_row' requires a non-empty `match`")
            if self.fields:
                raise ValueError("transform action 'drop_row' takes `match`, not `fields`")
        elif not self.fields:
            raise ValueError(f"transform action {self.action!r} requires non-empty `fields`")
        if self.action == "regex_replace":
            if not self.pattern:
                raise ValueError("transform action 'regex_replace' requires `pattern`")
            try:
                re.compile(self.pattern)
            except re.error as exc:
                raise ValueError(
                    f"transform pattern {self.pattern!r} does not compile: {exc}"
                ) from exc
        elif self.pattern is not None:
            raise ValueError(f"transform action {self.action!r} does not take `pattern`")
        return self


# Sampling rates are a keep-fraction in (0, 1]; 1.0 = keep everything (default).
SampleRate = Annotated[float, Field(gt=0.0, le=1.0)]


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
    rediscovery_interval: Duration = Field(
        default=timedelta(hours=1),
        description=(
            'How often to re-run wildcard ("*") topic discovery while running, so channels '
            "enabled after startup are picked up without a restart. 0 disables (discovery "
            "then runs only at startup)."
        ),
    )
    sample: dict[str, SampleRate] = Field(
        default_factory=dict,
        description=(
            "Opt-in lossy volume control: topic glob -> keep fraction (0-1], first "
            "matching glob wins. Sampling is deterministic by replay_id hash, so a "
            "replay keeps exactly the same rows (Loki dedup stays intact). Sampled-out "
            "events still advance checkpoints."
        ),
        examples=[{"/event/ApiEventStream": 0.25}],
    )
    transforms: list[TransformRule] = Field(
        default_factory=list,
        description=(
            "Redaction/filter rules applied to each decoded event payload before "
            "shaping (see TransformRule)."
        ),
    )
    bridge_max_bytes: int = Field(
        default=134_217_728,
        ge=0,
        description=(
            "Approximate byte budget for the Pub/Sub source's internal topic->events "
            "bridge queue, which sits UPSTREAM of the pipeline's sink.loki.batch."
            "queue_max_bytes. Topic tasks block (structural backpressure, propagated to "
            "flow-control credits) once the bridged-but-undrained bytes exceed this, so "
            "a sink outage can't balloon per-org buffering past the count bound. Default "
            "128 MiB; 0 disables byte accounting on the bridge."
        ),
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
    sample: SampleRate = Field(
        default=1.0,
        description=(
            "Opt-in lossy volume control: keep fraction (0-1] of rows, deterministic "
            "by record Id hash (replay-stable). 1.0 keeps everything."
        ),
    )
    big_object: bool = Field(
        default=False,
        description=(
            "Set true for Salesforce Big Objects (the stored RTEM event family: "
            "LoginEvent, ApiEvent, FileEventStore, *EventStore, ...). Big Objects "
            "reject ORDER BY ASC, so the source drains them newest-first (ORDER BY "
            "timestamp_field DESC) with a ratcheting upper bound and re-sorts each "
            "cycle's window ascending before emitting. Leave false for standard and "
            "custom objects (LoginHistory, MyAudit__c), which use the ASC path."
        ),
    )
    max_catchup_records: int = Field(
        default=200_000,
        ge=0,
        description=(
            "Cap on records collected into memory per big_object DESC drain cycle. "
            "The drain buffers a cycle's window in memory to re-sort it ascending; a "
            "post-outage catch-up over a large gap would otherwise be unbounded and "
            "can OOM. When the cap is hit the drain emits that (internally sorted) "
            "segment and ratchets its upper bound down so catch-up proceeds in bounded "
            "chunks across cycles. 0 = unbounded (pre-cap behaviour). Ignored on the "
            "ASC path (streamed page-by-page, already bounded)."
        ),
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
    transforms: list[TransformRule] = Field(
        default_factory=list,
        description=(
            "Redaction/filter rules applied to each polled record before shaping "
            "(see TransformRule)."
        ),
    )


# Salesforce usernames are interpolated into a SOQL `LogUser.Username IN (...)`
# clause; anything with a quote/backslash/control char could break the query or
# inject SOQL, so restrict to a conservative username charset.
_SF_USERNAME_RE = re.compile(r"^[A-Za-z0-9._%+\-@]+$")


class ApexLogConfig(StrictModel):
    """Poll Apex debug logs (``ApexLog``) via the Tooling API.

    Opt-in developer-focused source. ``ApexLog`` rows only exist while a
    ``TraceFlag`` is active for a user (24h retention, TraceFlags expire by
    design) — sf2loki does NOT manage TraceFlags; enable them via ``sf debug``
    or Setup -> Debug Logs. Each log costs one extra API call to download its
    body, so this is disabled by default and excluded from the free-tier path.
    """

    enabled: bool = Field(default=False, description="Enable the ApexLog polling source.")
    poll_interval: Duration = Field(
        default=timedelta(minutes=1), description="How often to poll for new ApexLog rows."
    )
    lookback: Duration = Field(
        default=timedelta(hours=1),
        description="Initial window to fetch on first run (no checkpoint).",
    )
    users: list[str] = Field(
        default_factory=list,
        description=(
            "Salesforce usernames whose logs to ingest (matched via LogUser.Username). "
            "Empty = every ApexLog visible to the integration user."
        ),
        examples=[["integration@example.com"]],
    )
    max_body_bytes: int = Field(
        default=5_242_880,
        gt=0,
        description=(
            "Skip the body download for logs whose LogLength exceeds this (the metadata "
            "line is still shipped, flagged body_skipped); the per-line cap "
            "(sink.loki.batch.max_line_bytes) truncates whatever is shipped."
        ),
    )
    sample: SampleRate = Field(
        default=1.0,
        description=(
            "Opt-in lossy volume control: keep fraction (0-1] of logs, deterministic "
            "by log Id hash (replay-stable). 1.0 keeps everything."
        ),
    )
    transforms: list[TransformRule] = Field(
        default_factory=list,
        description="Redaction/filter rules applied to each log's metadata before shaping.",
    )

    @model_validator(mode="after")
    def _validate_users(self) -> ApexLogConfig:
        for user in self.users:
            if not _SF_USERNAME_RE.match(user):
                raise ValueError(
                    f"apexlog user username {user!r} contains characters not allowed in a "
                    "Salesforce username (must match [A-Za-z0-9._%+-@]+)"
                )
        return self


# Label keys injected/reserved by the pipeline + sink; a promoted ELF column
# must not reuse these (it would clobber source identity or be silently
# overridden by the injected static labels in app._produce).
_RESERVED_LABEL_KEYS: frozenset[str] = frozenset(
    {"source", "event_type", "job", "sf_org_id", "environment", "org"}
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
    sample: SampleRate = Field(
        default=1.0,
        description=(
            "Opt-in lossy volume control: keep fraction (0-1] of this type's rows, "
            "deterministic by row key hash (replay-stable, Loki-dedup-safe). "
            "1.0 keeps everything."
        ),
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
            "pull a half-written hourly CSV whose tail rows would then be skipped when "
            "the watermark passes it. Left unset it defaults to 5m for interval: Hourly "
            "(Hourly blobs can be listed while server-side incomplete) and 0 for Daily "
            "(files land long after the day closes). Set explicitly to override either."
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
    transforms: list[TransformRule] = Field(
        default_factory=list,
        description=(
            "Redaction/filter rules applied to each CSV row before shaping (see TransformRule)."
        ),
    )
    concurrency: int = Field(
        default=4,
        ge=1,
        description=(
            "Event types processed concurrently per poll cycle (per-type ordering "
            "and checkpoints are unaffected — types are independent). Peak memory "
            "is roughly concurrency x 8 MiB of download spool."
        ),
    )

    @property
    def discover(self) -> bool:
        """True when the wildcard "*" is present — ingest all discovered EventTypes."""
        return any(t.name == EVENT_TYPE_WILDCARD for t in self.event_types)

    @model_validator(mode="after")
    def _default_hourly_settle_window(self) -> EventLogFileConfig:
        # Hourly EventLogFile blobs can be listed while Salesforce is still writing
        # them; if ingested incomplete and the watermark then passes, the missing
        # tail rows are permanently skipped. Default a conservative settle window
        # for Hourly (only when the operator left it unset) as cheap insurance;
        # Daily files land long after the day closes and stay at 0.
        if "settle_window" not in self.model_fields_set and self.interval == "Hourly":
            self.settle_window = timedelta(minutes=5)
        return self

    @model_validator(mode="after")
    def _require_event_types_when_enabled(self) -> EventLogFileConfig:
        if self.enabled and not self.event_types:
            raise ValueError(
                "eventlogfile.enabled is true but event_types is empty; "
                'list the ELF EventType values to ingest (e.g. [Login, API]) or "*" to '
                "discover and ingest all types the org produces"
            )
        return self

    @model_validator(mode="after")
    def _forbid_dropping_promoted_labels(self) -> EventLogFileConfig:
        # drop_field of a label-promoted column would silently drop the label per
        # row; hash/mask are allowed (a pseudonymised label is a legitimate choice).
        dropped = {
            f for rule in self.transforms if rule.action == "drop_field" for f in rule.fields
        }
        if not dropped:
            return self
        for t in self.event_types:
            clash = dropped.intersection(t.labels)
            if clash:
                raise ValueError(
                    f"eventlogfile type {t.name!r}: transform drop_field removes "
                    f"column(s) {sorted(clash)} promoted to labels; remove the label "
                    "or use hash/mask instead"
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
    apexlog: ApexLogConfig = Field(
        default_factory=ApexLogConfig,
        description="ApexLog (Tooling API debug log) polling source.",
    )
    allow_overlap: bool = Field(
        default=False,
        description=(
            "Bypass the fail-fast overlap guard that refuses to start when one "
            "event category is enabled on more than one source."
        ),
    )
    transform_salt: SecretStr | None = Field(
        default=None,
        description=(
            "Deployment-wide salt for `hash` transform rules (stable pseudonyms that "
            "still correlate within this deployment). Strongly recommended whenever a "
            "hash rule is configured — unsalted hashes of low-entropy values (IPs, "
            "usernames) are trivially reversible by table lookup."
        ),
    )
    transform_salt_file: Path | None = Field(
        default=None,
        description="File path to the transform hash salt (alternative to transform_salt).",
        examples=["/etc/sf2loki/secrets/transform-salt"],
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
    queue_maxsize: int = Field(
        default=10_000,
        ge=100,
        description=(
            "Entry-count bound of the internal source->sink queue. Producers block "
            "(structural backpressure) when full."
        ),
    )
    queue_max_bytes: int = Field(
        default=268_435_456,
        ge=0,
        description=(
            "Approximate byte budget for queued entries (worst-case memory during a sink "
            "outage). Producers block when exceeded, even if the entry-count bound is not "
            "reached. Default 256 MiB; 0 disables byte accounting."
        ),
    )


class EgressConfig(StrictModel):
    """Egress guardrails: rate caps and a daily byte budget (all OFF by default).

    Bytes are counted pre-compression (sum of pushed line bytes) — the closest
    approximation of what Loki-based platforms meter and bill. The rate caps
    delay pushes (lossless backpressure, propagated upstream to polling/stream
    flow control); the budget either pauses pushes until the next UTC day
    (lossless, delayed — bounded by Salesforce-side retention) or drops
    (lossy, counted in loki_entries_dropped{reason="budget"}).
    """

    max_lines_per_second: float = Field(
        default=0,
        ge=0,
        description="Token-bucket cap on pushed lines/second; 0 disables.",
    )
    max_bytes_per_second: float = Field(
        default=0,
        ge=0,
        description="Token-bucket cap on pushed (pre-compression) bytes/second; 0 disables.",
    )
    daily_byte_budget: int = Field(
        default=0,
        ge=0,
        description=(
            "Maximum pre-compression bytes pushed per UTC day; 0 disables. The used "
            "counter persists in the state store, so restarts don't reset it. WARN at "
            "80%, ERROR + budget_action at 100%."
        ),
    )
    budget_action: Literal["pause", "drop"] = Field(
        default="pause",
        description=(
            "What to do when the daily budget is exhausted: pause (hold pushes + "
            "checkpoints until the next UTC day; data delayed, never lost, readiness "
            "reports degraded) | drop (keep running, discard over-budget entries)."
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
    egress: EgressConfig = Field(
        default_factory=EgressConfig,
        description="Egress guardrails: rate caps + daily byte budget (all off by default).",
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
    loki: LokiConfig = Field(description="Loki sink settings.")


class FileStateConfig(StrictModel):
    path: Path = Field(
        default=Path("/var/lib/sf2loki/state.json"),
        description="Checkpoint file path; persist on a mounted volume for durable resume.",
    )


class S3StateConfig(StrictModel):
    """S3-compatible object-storage checkpoint store (stateless deployments).

    The whole checkpoint document lives at one key; commits use conditional
    writes (ETag If-Match) so two instances against the same key fail fast
    instead of clobbering each other. Requires the ``s3`` extra
    (``pip install sf2loki[s3]``). Credentials come from the standard AWS
    default chain (env vars, instance/task role, shared config).
    """

    bucket: str = Field(
        default="",
        description="Bucket name (required when state.store is s3).",
    )
    key: str = Field(
        default="sf2loki/state.json",
        description="Object key holding the checkpoint document.",
    )
    region: str | None = Field(
        default=None,
        description="AWS region; omit to use the default-chain region.",
    )
    endpoint_url: str | None = Field(
        default=None,
        description="Custom S3 endpoint for MinIO/R2/Ceph; omit for AWS S3.",
        examples=["http://minio:9000"],
    )


class GcsStateConfig(StrictModel):
    """Google Cloud Storage checkpoint store (stateless deployments).

    The whole checkpoint document lives at one object; commits use GCS
    generation preconditions (ifGenerationMatch) so two instances against the
    same object fail fast instead of clobbering each other. Requires the ``gcs``
    extra (``pip install sf2loki[gcs]``). Auth via Application Default
    Credentials (ADC); set ``service_file`` only for an explicit key file.
    """

    bucket: str = Field(
        default="",
        description="Bucket name (required when state.store is gcs).",
    )
    object_name: str = Field(
        default="sf2loki/state.json",
        description="Object name holding the checkpoint document.",
    )
    service_file: Path | None = Field(
        default=None,
        description=(
            "Path to a service-account JSON key; omit to use Application Default Credentials."
        ),
    )


class StateConfig(StrictModel):
    store: Literal["file", "s3", "gcs"] = Field(
        default="file",
        description=(
            "State backend: file (local JSON, needs a persistent volume) | s3 "
            "(S3-compatible object storage, for stateless deployments; needs the "
            "sf2loki[s3] extra) | gcs (Google Cloud Storage, for stateless "
            "deployments; needs the sf2loki[gcs] extra)."
        ),
    )
    file: FileStateConfig = Field(
        default_factory=FileStateConfig, description="File-backed state store settings."
    )
    s3: S3StateConfig = Field(
        default_factory=S3StateConfig, description="S3-backed state store settings."
    )
    gcs: GcsStateConfig = Field(
        default_factory=GcsStateConfig, description="GCS-backed state store settings."
    )

    @model_validator(mode="after")
    def _require_bucket_for_remote(self) -> StateConfig:
        if self.store == "s3" and not self.s3.bucket:
            raise ValueError("state.store is 's3' but state.s3.bucket is empty")
        if self.store == "gcs" and not self.gcs.bucket:
            raise ValueError("state.store is 'gcs' but state.gcs.bucket is empty")
        return self


class FileLeaseConfig(StrictModel):
    """File lease on shared storage (NFS/EFS) for active-passive failover.

    Lease-expiry semantics rather than flock (advisory locks are unreliable
    over NFS): the leader renews a holder+expiry document via atomic
    tmp+rename; a standby takes over once the lease has expired. Expiry uses
    wall-clock time, so keep the hosts NTP-synced — the ttl must comfortably
    exceed worst-case clock skew between replicas.
    """

    path: Path = Field(
        default=Path("/var/lib/sf2loki/leader.lease"),
        description="Lease file path on storage shared by all replicas.",
    )
    ttl: Duration = Field(
        default=timedelta(seconds=30),
        description=(
            "Lease lifetime: a standby takes over once the lease is this stale. "
            "Failover time is bounded by ttl; must exceed inter-host clock skew."
        ),
    )
    renew_interval: Duration = Field(
        default=timedelta(seconds=10),
        description="How often the leader re-writes the lease (must be < ttl/2).",
    )
    holder_id: str = Field(
        default="",
        description=(
            "Stable identity written into the lease; defaults to hostname-pid at "
            "startup. Set explicitly when hostnames aren't unique."
        ),
    )

    @model_validator(mode="after")
    def _renew_must_beat_ttl(self) -> FileLeaseConfig:
        if self.renew_interval.total_seconds() * 2 >= self.ttl.total_seconds():
            raise ValueError(
                "coordinate.file_lease.renew_interval must be less than half the ttl "
                f"(renew {self.renew_interval.total_seconds():g}s vs "
                f"ttl {self.ttl.total_seconds():g}s) so a single missed renewal "
                "cannot cost leadership"
            )
        return self


class K8sLeaseConfig(StrictModel):
    """Kubernetes Lease for active-passive failover (coordination.k8s.io/v1).

    The leader renews a Lease object (holderIdentity + renewTime); a standby
    watches and takes over once the lease is stale (renewTime + duration in the
    past). Optimistic concurrency uses the Lease's resourceVersion. Requires the
    ``k8s`` extra (``pip install sf2loki[k8s]``). In-cluster config by default;
    set ``kubeconfig`` for out-of-cluster dev.
    """

    namespace: str = Field(default="default", description="Namespace holding the Lease object.")
    name: str = Field(
        default="sf2loki-leader",
        description="Lease object name (shared by all replicas).",
    )
    identity: str = Field(
        default="",
        description=(
            "holderIdentity written into the Lease; defaults to the pod name "
            "($HOSTNAME) at startup."
        ),
    )
    lease_duration: Duration = Field(
        default=timedelta(seconds=30),
        description=(
            "Lease lifetime: a standby takes over once the lease is this stale. "
            "Failover time is bounded by this."
        ),
    )
    renew_interval: Duration = Field(
        default=timedelta(seconds=10),
        description="How often the leader renews the Lease (must be < lease_duration/2).",
    )
    kubeconfig: Path | None = Field(
        default=None,
        description=("Path to a kubeconfig for out-of-cluster dev; omit to use in-cluster config."),
    )

    @model_validator(mode="after")
    def _renew_must_beat_duration(self) -> K8sLeaseConfig:
        if self.renew_interval.total_seconds() * 2 >= self.lease_duration.total_seconds():
            raise ValueError(
                "coordinate.k8s_lease.renew_interval must be less than half the "
                f"lease_duration (renew {self.renew_interval.total_seconds():g}s vs "
                f"lease_duration {self.lease_duration.total_seconds():g}s) so a single "
                "missed renewal cannot cost leadership"
            )
        return self


class CoordinateConfig(StrictModel):
    """Leadership coordination for active-passive HA.

    ``noop`` (default) = single instance, always leader. ``file_lease`` lets
    two replicas share a lease file on common storage; ``k8s_lease`` uses a
    Kubernetes Lease object instead. Either way the standby takes over within
    one ttl/lease_duration when the leader dies, resuming from committed
    checkpoints (at-least-once — brief double-ingest during a takeover window is
    possible, loss is not).
    """

    type: Literal["noop", "file_lease", "k8s_lease"] = Field(
        default="noop",
        description=(
            "noop (single instance) | file_lease (active-passive via a shared "
            "lease file) | k8s_lease (active-passive via a Kubernetes Lease; needs "
            "the sf2loki[k8s] extra)."
        ),
    )
    file_lease: FileLeaseConfig = Field(
        default_factory=FileLeaseConfig, description="File-lease coordinator settings."
    )
    k8s_lease: K8sLeaseConfig = Field(
        default_factory=K8sLeaseConfig, description="Kubernetes-Lease coordinator settings."
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
    unready_after_sink_failing: Duration = Field(
        default=timedelta(minutes=15),
        description=(
            "/readyz reports 503 when Loki pushes have been failing continuously for this "
            "long (data is retried and safe, but the instance is degraded and an "
            "orchestrator should surface it). 0 disables the readiness degradation."
        ),
    )
    telemetry: TelemetryConfig = Field(
        default_factory=TelemetryConfig, description="OTLP metrics egress settings."
    )


_ORG_NAME_PATTERN = r"^[A-Za-z0-9_-]+$"


class OrgConfig(StrictModel):
    """One Salesforce org in a multi-org deployment.

    ``name`` becomes the ``org`` stream label value and the checkpoint-key
    prefix (``org=<name>:``), so it must be a short slug (letters, digits,
    ``_``, ``-``). Each org carries its own ``salesforce`` connection and its
    own ``sources`` selection; the sink, state store, coordinator, and service
    settings stay deployment-wide (one shared pipeline).
    """

    name: str = Field(
        min_length=1,
        pattern=_ORG_NAME_PATTERN,
        description=(
            "Org identifier: becomes the `org` stream label and the checkpoint "
            "key prefix. Letters, digits, underscore, hyphen only; must be unique."
        ),
        examples=["prod"],
    )
    salesforce: SalesforceConfig = Field(
        description="This org's Salesforce connection and authentication."
    )
    sources: SourcesConfig = Field(
        default_factory=SourcesConfig, description="This org's event source selection and settings."
    )


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SF2LOKI_",
        env_nested_delimiter="__",
        extra="forbid",
    )

    salesforce: SalesforceConfig | None = Field(
        default=None,
        description=(
            "Single-org Salesforce connection and authentication. Set this OR `orgs` "
            "(exactly one). Omit when using the multi-org `orgs` list."
        ),
    )
    orgs: list[OrgConfig] = Field(
        default_factory=list,
        description=(
            "Multi-org list: ingest several Salesforce orgs from one process into one "
            "shared sink. Set this OR top-level `salesforce` (exactly one). Each entry "
            "carries its own salesforce + sources; the sink/state/service stay shared."
        ),
    )
    sources: SourcesConfig = Field(
        default_factory=SourcesConfig,
        description=(
            "Single-org event source selection and settings. Ignored (and rejected if "
            "customized) when `orgs` is used — put per-org sources under each org."
        ),
    )
    sink: SinkConfig = Field(description="Log sink settings.")
    state: StateConfig = Field(
        default_factory=StateConfig, description="Checkpoint/state store settings."
    )
    coordinate: CoordinateConfig = Field(
        default_factory=CoordinateConfig,
        description="Leadership coordination for active-passive HA.",
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

    @model_validator(mode="after")
    def _validate_org_topology(self) -> Config:
        """Enforce exactly-one-of (top-level salesforce | non-empty orgs) + unique names.

        The old single-org shape (top-level ``salesforce:``) stays valid; the new
        multi-org shape uses ``orgs:``. Setting both, or neither, is fatal — with a
        message as actionable as the old "missing salesforce" error. When ``orgs`` is
        used, top-level ``sources`` must be left at its default (per-org sources live
        under each org, so a top-level block would be silently ignored otherwise).
        """
        has_top = self.salesforce is not None
        has_orgs = bool(self.orgs)
        if has_top and has_orgs:
            raise ValueError(
                "set EITHER top-level 'salesforce' (single-org) OR 'orgs' (multi-org), not both"
            )
        if not has_top and not has_orgs:
            raise ValueError(
                "no Salesforce org configured: set top-level 'salesforce' (single-org) "
                "or a non-empty 'orgs' list (multi-org)"
            )
        if has_orgs:
            names = [o.name for o in self.orgs]
            dupes = sorted({n for n in names if names.count(n) > 1})
            if dupes:
                raise ValueError(
                    f"duplicate org name(s) {dupes}; each orgs[].name must be unique "
                    "(it is the 'org' label value and checkpoint key prefix)"
                )
            if self.sources != SourcesConfig():
                raise ValueError(
                    "top-level 'sources' cannot be combined with 'orgs'; move per-org "
                    "source config under each entry's 'sources' key"
                )
        return self

    def resolved_orgs(self) -> list[OrgConfig]:
        """Normalise single-org and multi-org configs into one org list.

        A single-org config (top-level ``salesforce``) yields exactly one entry with
        an EMPTY name — the legacy/no-prefix mode: no ``org`` label is added and
        checkpoint keys stay unprefixed, so existing state files and streams are
        bit-identical. A multi-org config returns its ``orgs`` list verbatim.
        """
        if self.orgs:
            return list(self.orgs)
        assert self.salesforce is not None  # guaranteed by _validate_org_topology
        # model_construct: bypass validation for the empty-name legacy sentinel
        # (name="" violates OrgConfig's min_length/pattern by design).
        return [
            OrgConfig.model_construct(name="", salesforce=self.salesforce, sources=self.sources)
        ]


def select_org(cfg: Config, name: str | None) -> tuple[OrgConfig, str | None]:
    """Pick one org for the single-org CLI commands (``doctor``/``backfill``).

    ``name`` None selects the first configured org (the legacy empty-name org for
    single-org configs). Returns ``(org, note)`` where ``note`` is a human message
    printed when more than one org is configured (so the operator knows the command
    scoped to one). Raises :class:`ConfigError` for an unknown ``--org`` name.
    """
    orgs = cfg.resolved_orgs()
    chosen: OrgConfig
    if not name:
        chosen = orgs[0]
    else:
        match = next((o for o in orgs if o.name == name), None)
        if match is None:
            available = [o.name for o in orgs if o.name] or ["<single-org>"]
            raise ConfigError(f"--org {name!r} is not configured; available orgs: {available}")
        chosen = match
    note = None
    if len([o for o in orgs if o.name]) > 1:
        names = [o.name for o in orgs]
        note = (
            f"multiple orgs configured {names}; this command operates on org "
            f"'{chosen.name}' only (use --org to choose another)"
        )
    return chosen, note


def as_single_org_view(cfg: Config, org: OrgConfig) -> Config:
    """Return a single-org ``Config`` view scoped to *org* for the CLI commands.

    ``doctor`` and ``backfill`` consume ``cfg.salesforce``/``cfg.sources`` directly;
    this hands them the selected org's connection + sources with a guaranteed
    non-None ``salesforce`` (shared sink/state/service/coordinate untouched). Uses
    ``model_copy`` so no re-validation runs on the already-resolved secrets.
    """
    return cfg.model_copy(update={"salesforce": org.salesforce, "sources": org.sources, "orgs": []})


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


def _resolve_salesforce_secrets(sf: SalesforceConfig, *, where: str = "") -> None:
    """Resolve one Salesforce config's required secret in place (auth_mode-dependent).

    ``where`` (e.g. ``"org 'prod': "``) is prefixed to the error so a multi-org
    misconfiguration names the offending org.
    """
    if sf.auth_mode == "client_credentials":
        sf.client_secret = _resolve_secret_file(
            sf.client_secret_file, sf.client_secret, "salesforce client secret"
        )
        if sf.client_secret is None:
            raise ConfigError(
                f"{where}salesforce client secret required for "
                "auth_mode=client_credentials (set client_secret or client_secret_file)"
            )
    else:  # jwt_bearer
        sf.private_key = _resolve_secret_file(
            sf.private_key_file, sf.private_key, "salesforce private key"
        )
        if sf.private_key is None:
            raise ConfigError(
                f"{where}salesforce private key required for auth_mode=jwt_bearer "
                "(set private_key or private_key_file)"
            )


def resolve_secrets(cfg: Config) -> Config:
    """Load file-injected secrets in place; fail fast on missing required secrets.

    The required Salesforce secret depends on ``auth_mode``: ``client_credentials``
    needs ``client_secret``; ``jwt_bearer`` needs ``private_key``. Per-org configs
    resolve each org's salesforce secret and each org's transform hash salt (both
    live under the org); the legacy single-org path resolves the same objects via
    the one-entry ``resolved_orgs()`` view.
    """
    for org in cfg.resolved_orgs():
        where = f"org {org.name!r}: " if org.name else ""
        _resolve_salesforce_secrets(org.salesforce, where=where)
        # Transform hash salt is optional (hash rules work unsalted, with a documented
        # rainbow-table caveat), so resolve a file if given but never require it.
        org.sources.transform_salt = _resolve_secret_file(
            org.sources.transform_salt_file,
            org.sources.transform_salt,
            "transform hash salt",
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
