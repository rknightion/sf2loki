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


class SalesforceLimitsConfig(BaseModel):
    """Org-limits metric poller.

    Polls ``/services/data/vXX.0/limits`` and emits a max/remaining gauge per
    limit (DailyApiRequests, DataStorageMB, DailyStreamingApiEvents, ...). Cheap
    (one REST call per interval) and useful product telemetry.
    """

    enabled: bool = False
    poll_interval: Duration = timedelta(minutes=5)


class SalesforceConfig(BaseModel):
    # login_url, when left blank, is derived from ``environment`` below. An
    # explicit value (a custom My Domain URL) always takes precedence.
    login_url: str = ""
    environment: Literal["production", "sandbox"] = "production"
    # OAuth flow: ``jwt_bearer`` (asymmetric key + cert, no shared secret) or
    # ``client_credentials`` (consumer key + secret, no keypair/cert/pre-auth).
    # Defaults to jwt_bearer for backward compatibility.
    auth_mode: Literal["jwt_bearer", "client_credentials"] = "jwt_bearer"
    client_id: str
    # client_credentials flow secret (injectable from file/env like the key).
    client_secret: SecretStr | None = None
    client_secret_file: Path | None = None
    # Required for jwt_bearer (the JWT ``sub`` claim); unused for client_credentials.
    username: str = ""
    private_key_file: Path | None = None
    private_key: SecretStr | None = None
    api_version: str = "60.0"
    org_id: str | None = None
    limits: SalesforceLimitsConfig = Field(default_factory=SalesforceLimitsConfig)

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
        return self


class PubSubConfig(BaseModel):
    enabled: bool = True
    endpoint: str = "api.pubsub.salesforce.com:7443"
    default_num_requested: int = 100
    replay_preset: Literal["LATEST", "EARLIEST", "CUSTOM"] = "CUSTOM"
    topics: list[str] = Field(default_factory=list)
    include: list[str] = Field(default_factory=lambda: ["*"])
    exclude: list[str] = Field(default_factory=list)


class EventLogObjectConfig(BaseModel):
    name: str
    timestamp_field: str = "EventDate"
    poll_interval: Duration = timedelta(minutes=5)
    lookback: Duration = timedelta(hours=1)


class EventLogObjectsConfig(BaseModel):
    enabled: bool = False
    objects: list[EventLogObjectConfig] = Field(default_factory=list)


# Label keys injected/reserved by the pipeline + sink; a promoted ELF column
# must not reuse these (it would clobber source identity or be silently
# overridden by the injected static labels in app._produce).
_RESERVED_LABEL_KEYS: frozenset[str] = frozenset(
    {"source", "event_type", "job", "sf_org_id", "environment"}
)
_LABEL_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class EventLogFileTypeConfig(BaseModel):
    """Per-event-type ELF ingestion config.

    A bare string in ``eventlogfile.event_types`` coerces to ``{name: <string>}``
    (backward compatible). ``structured_metadata_fields`` defaults to ``None``,
    meaning "fall back to the global ``sink.loki.structured_metadata_fields``";
    set it to a list (including ``[]``) to override per type. ``labels`` lists
    columns promoted to stream labels — keep these LOW cardinality.
    """

    name: str
    structured_metadata_fields: list[str] | None = None
    labels: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_labels(self) -> EventLogFileTypeConfig:
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


class EventLogFileConfig(BaseModel):
    enabled: bool = False
    # Ingest exactly ONE interval; hourly and daily files are redundant copies
    # of the same events (Salesforce), so ingesting both double-counts.
    interval: Literal["Hourly", "Daily"] = "Hourly"
    # ELF EventTypes to ingest. Each item is either a bare string (e.g. "Login")
    # or a per-type object (name + optional structured_metadata_fields/labels).
    # Required when enabled — there is no sensible "all" default given ~70 types
    # and the either/or-per-category model.
    event_types: Annotated[list[EventLogFileTypeConfig], BeforeValidator(_coerce_event_types)] = (
        Field(default_factory=list)
    )
    poll_interval: Duration = timedelta(hours=1)  # how often to list new files
    lookback: Duration = timedelta(hours=24)  # initial window when no checkpoint
    timestamp_column: str = "TIMESTAMP_DERIVED"  # per-row timestamp column
    page_size: int = 1000  # SOQL LIMIT for the file-listing query
    # Resiliency knobs for the (unstable) Hourly path — ko.md §7.4.
    # settle_window: skip files whose CreatedDate is newer than now-settle_window,
    # so we don't pull a half-written hourly CSV. 0 disables (safe for Daily).
    settle_window: Duration = timedelta(0)
    # download_max_age: a file whose body keeps failing to download and is older
    # than this is abandoned (checkpoint advances past it) so a permanently-missing
    # file can't wedge the watermark forever. Files younger than this are retried.
    download_max_age: Duration = timedelta(hours=24)

    @model_validator(mode="after")
    def _require_event_types_when_enabled(self) -> EventLogFileConfig:
        if self.enabled and not self.event_types:
            raise ValueError(
                "eventlogfile.enabled is true but event_types is empty; "
                "list the ELF EventType values to ingest (e.g. [Login, API])"
            )
        return self


class SourcesConfig(BaseModel):
    pubsub: PubSubConfig = Field(default_factory=PubSubConfig)
    eventlog_objects: EventLogObjectsConfig = Field(default_factory=EventLogObjectsConfig)
    eventlogfile: EventLogFileConfig = Field(default_factory=EventLogFileConfig)
    # Bypass the fail-fast overlap guard (sources/overlap.py) that refuses to
    # start when one event category is enabled on more than one source.
    allow_overlap: bool = False


class LokiBatchConfig(BaseModel):
    max_entries: int = 1000
    max_bytes: int = 1_048_576
    flush_interval: Duration = timedelta(seconds=1)
    # Per-line UTF-8 byte cap; lines longer than this are truncated (with a
    # marker) before push so one oversized row can't 400 its whole batch.
    # Mirrors Loki's server-side `max_line_size` default (256 KiB). 0 disables.
    max_line_bytes: int = 262144


class LokiConfig(BaseModel):
    url: str
    tenant_id: str | None = None
    auth_token_file: Path | None = None
    auth_token: SecretStr | None = None
    encoding: Literal["protobuf", "json"] = "protobuf"
    compression: Literal["snappy", "gzip", "none"] = "snappy"
    batch: LokiBatchConfig = Field(default_factory=LokiBatchConfig)
    labels: dict[str, str] = Field(default_factory=dict)
    structured_metadata_fields: list[str] = Field(default_factory=list)


class SinkConfig(BaseModel):
    type: Literal["loki"] = "loki"
    loki: LokiConfig


class FileStateConfig(BaseModel):
    path: Path = Path("/var/lib/sf2loki/state.json")


class StateConfig(BaseModel):
    store: Literal["file", "configmap"] = "file"
    file: FileStateConfig = Field(default_factory=FileStateConfig)
    configmap_name: str = "sf2loki-state"
    namespace: str | None = None


class TelemetryConfig(BaseModel):
    """OTLP metrics egress (self-observability + Salesforce product metrics).

    When ``enabled`` is false, metrics are still recorded in-process (cheap, used
    by tests via the in-memory reader) but exported nowhere — there is no
    Prometheus scrape endpoint; sf2loki is OTLP-push-native.
    """

    enabled: bool = False
    # Full OTLP/HTTP metrics URL. For Grafana Cloud this is the stack OTLP
    # gateway, e.g. https://otlp-gateway-<zone>.grafana.net/otlp/v1/metrics;
    # for a local Alloy otelcol.receiver.otlp, e.g. http://alloy:4318/v1/metrics.
    endpoint: str = ""
    # Auth for the OTLP endpoint. "basic" sends Authorization: Basic
    # base64(user:token); "none" sends none (e.g. in-cluster Alloy). For basic,
    # the credentials default to the Loki sink's tenant_id/auth_token when left
    # blank — Grafana Cloud uses one stack credential for both Loki and OTLP.
    auth: Literal["basic", "none"] = "basic"
    basic_auth_user: str = ""
    basic_auth_token: SecretStr | None = None
    basic_auth_token_file: Path | None = None
    # Explicit headers, merged on top of any computed Authorization header.
    # Values support ${ENV} interpolation at config load.
    headers: dict[str, str] = Field(default_factory=dict)
    export_interval: Duration = timedelta(seconds=60)
    # Extra OTel resource attributes merged onto the defaults (service.name, etc.).
    resource_attributes: dict[str, str] = Field(default_factory=dict)


class ServiceConfig(BaseModel):
    log_level: str = "info"
    log_format: Literal["json", "logfmt"] = "json"
    health_addr: str = ":8080"
    shutdown_grace: Duration = timedelta(seconds=25)
    telemetry: TelemetryConfig = Field(default_factory=TelemetryConfig)


class Config(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="SF2LOKI_",
        env_nested_delimiter="__",
        extra="ignore",
    )

    salesforce: SalesforceConfig
    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    sink: SinkConfig
    state: StateConfig = Field(default_factory=StateConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)

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
        raise ConfigError(f"invalid configuration: {exc}") from exc
    return resolve_secrets(cfg)
