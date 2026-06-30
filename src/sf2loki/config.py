"""Configuration: pydantic models loaded from YAML and/or env, with file-injected secrets.

Precedence (highest first): environment (``SF2LOKI_*`` with ``__`` nesting) >
YAML file > model defaults. Secrets come from ``*_file`` paths or inline; a
missing/unreadable secret file is fatal at load time (no silent blanks).
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, SecretStr, ValidationError
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


class ConfigError(Exception):
    """Raised for any invalid or unreadable configuration."""


class SalesforceConfig(BaseModel):
    login_url: str = "https://login.salesforce.com"
    client_id: str
    username: str
    private_key_file: Path | None = None
    private_key: SecretStr | None = None
    api_version: str = "60.0"
    org_id: str | None = None


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
    poll_interval: timedelta = timedelta(minutes=5)
    lookback: timedelta = timedelta(hours=1)


class EventLogObjectsConfig(BaseModel):
    enabled: bool = False
    objects: list[EventLogObjectConfig] = Field(default_factory=list)


class EventLogFileConfig(BaseModel):
    enabled: bool = False


class SourcesConfig(BaseModel):
    pubsub: PubSubConfig = Field(default_factory=PubSubConfig)
    eventlog_objects: EventLogObjectsConfig = Field(default_factory=EventLogObjectsConfig)
    eventlogfile: EventLogFileConfig = Field(default_factory=EventLogFileConfig)


class LokiBatchConfig(BaseModel):
    max_entries: int = 1000
    max_bytes: int = 1_048_576
    flush_interval: timedelta = timedelta(seconds=1)


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


class ServiceConfig(BaseModel):
    log_level: str = "info"
    log_format: Literal["json", "logfmt"] = "json"
    metrics_addr: str = ":9090"
    health_addr: str = ":8080"
    shutdown_grace: timedelta = timedelta(seconds=25)


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
    """Load file-injected secrets in place; fail fast on missing required secrets."""
    cfg.salesforce.private_key = _resolve_secret_file(
        cfg.salesforce.private_key_file,
        cfg.salesforce.private_key,
        "salesforce private key",
    )
    if cfg.salesforce.private_key is None:
        raise ConfigError("salesforce private key required (set private_key or private_key_file)")
    cfg.sink.loki.auth_token = _resolve_secret_file(
        cfg.sink.loki.auth_token_file,
        cfg.sink.loki.auth_token,
        "loki auth token",
    )
    return cfg


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
        data = loaded
    try:
        cfg = Config(**data)
    except ValidationError as exc:
        raise ConfigError(f"invalid configuration: {exc}") from exc
    return resolve_secrets(cfg)
