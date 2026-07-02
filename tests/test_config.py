from __future__ import annotations

import base64
from datetime import timedelta
from pathlib import Path

import pydantic
import pytest
from pydantic import SecretStr, ValidationError

from sf2loki import config as cfg_mod
from sf2loki.config import (
    ConfigError,
    EventLogFileConfig,
    EventLogFileTypeConfig,
    EventLogObjectConfig,
    LokiBatchConfig,
    LokiConfig,
    SalesforceConfig,
    ServiceConfig,
    SourcesConfig,
    TelemetryConfig,
    load,
    telemetry_headers,
)


def _all_config_models():
    seen = set()
    stack = [cfg_mod.Config]
    while stack:
        model = stack.pop()
        if model in seen or not (isinstance(model, type) and issubclass(model, pydantic.BaseModel)):
            continue
        seen.add(model)
        for f in model.model_fields.values():
            ann = f.annotation
            # descend into nested BaseModel annotations (incl. list[Model])
            for candidate in (ann, *getattr(ann, "__args__", ())):
                if isinstance(candidate, type) and issubclass(candidate, pydantic.BaseModel):
                    stack.append(candidate)
    return seen


def test_every_config_field_has_a_description():
    missing = []
    for model in _all_config_models():
        for name, f in model.model_fields.items():
            if not (f.description and f.description.strip()):
                missing.append(f"{model.__name__}.{name}")
    assert not missing, f"fields missing Field(description=...): {sorted(missing)}"


def _write_config(tmp_path: Path, key_file: Path) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key_file}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
    labels:
      environment: test
""".lstrip()
    )
    return p


def test_load_yaml_with_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    cfg_path = _write_config(tmp_path, key)
    monkeypatch.setenv("SF2LOKI_SERVICE__LOG_LEVEL", "debug")
    cfg = load(cfg_path)
    assert cfg.service.log_level == "debug"  # env wins over YAML default
    assert cfg.salesforce.client_id == "cid"  # YAML value preserved
    assert cfg.sink.loki.labels == {"environment": "test"}


def test_secret_file_resolution(tmp_path: Path) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PKDATA\n")
    cfg = load(_write_config(tmp_path, key))
    assert cfg.salesforce.private_key is not None
    assert cfg.salesforce.private_key.get_secret_value() == "PKDATA"


def test_missing_secret_file_is_fatal(tmp_path: Path) -> None:
    cfg_path = _write_config(tmp_path, tmp_path / "does-not-exist.pem")
    with pytest.raises(ConfigError):
        load(cfg_path)


def test_missing_required_field_is_config_error(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text("sink:\n  loki:\n    url: http://x\n")  # no salesforce block
    with pytest.raises(ConfigError):
        load(p)


# --- Duration shorthand (DESIGN.md §11: "5m", "1s", "25s") ------------------


@pytest.mark.parametrize(
    ("shorthand", "expected"),
    [
        ("5m", timedelta(minutes=5)),
        ("1h", timedelta(hours=1)),
        ("25s", timedelta(seconds=25)),
        ("1h30m", timedelta(hours=1, minutes=30)),
        ("500ms", timedelta(milliseconds=500)),
        ("1d", timedelta(days=1)),
    ],
)
def test_duration_shorthand_parses(shorthand: str, expected: timedelta) -> None:
    cfg = ServiceConfig(shutdown_grace=shorthand)  # type: ignore[arg-type]
    assert cfg.shutdown_grace == expected

    obj = EventLogObjectConfig(name="LoginEvent", poll_interval=shorthand)  # type: ignore[arg-type]
    assert obj.poll_interval == expected


def test_duration_passthrough_for_timedelta_and_iso8601() -> None:
    # Existing forms (timedelta object, ISO-8601, plain seconds) still work.
    assert ServiceConfig(shutdown_grace=timedelta(seconds=9)).shutdown_grace == timedelta(seconds=9)
    assert ServiceConfig(shutdown_grace="PT9S").shutdown_grace == timedelta(seconds=9)  # type: ignore[arg-type]
    assert ServiceConfig(shutdown_grace=9).shutdown_grace == timedelta(seconds=9)  # type: ignore[arg-type]


def test_duration_shorthand_rejects_garbage() -> None:
    with pytest.raises(ValidationError):
        ServiceConfig(shutdown_grace="5x")  # type: ignore[arg-type]


# --- ${ENV} interpolation (DESIGN.md §11: "client_id: ${SF_CLIENT_ID}") -----


def test_env_interpolation_in_yaml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: ${{SF_CLIENT_ID}}
  username: svc@example.com
  private_key_file: {key}
sink:
  loki:
    url: ${{LOKI_URL}}
""".lstrip()
    )
    monkeypatch.setenv("SF_CLIENT_ID", "real-client-id")
    monkeypatch.setenv("LOKI_URL", "http://loki:3100/loki/api/v1/push")

    cfg = load(p)

    assert cfg.salesforce.client_id == "real-client-id"
    assert cfg.sink.loki.url == "http://loki:3100/loki/api/v1/push"


def test_env_interpolation_embedded_in_string(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sink:
  loki:
    url: https://${{LOKI_HOST}}/loki/api/v1/push
""".lstrip()
    )
    monkeypatch.setenv("LOKI_HOST", "logs-prod-42.grafana.net")

    cfg = load(p)

    assert cfg.sink.loki.url == "https://logs-prod-42.grafana.net/loki/api/v1/push"


def test_env_interpolation_missing_var_is_config_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: ${{NOT_SET_ANYWHERE}}
  username: svc@example.com
  private_key_file: {key}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    monkeypatch.delenv("NOT_SET_ANYWHERE", raising=False)

    with pytest.raises(ConfigError, match="NOT_SET_ANYWHERE"):
        load(p)


# ---------------------------------------------------------------------------
# EventLogFile config (Phase 3)


def test_eventlogfile_config_defaults() -> None:
    cfg = EventLogFileConfig(enabled=True, event_types=["Login"])
    assert cfg.interval == "Hourly"
    # Bare strings coerce to per-type objects (backward compatible).
    assert [t.name for t in cfg.event_types] == ["Login"]
    assert cfg.event_types[0].structured_metadata_fields is None
    assert cfg.event_types[0].labels == []
    assert cfg.poll_interval == timedelta(hours=1)
    assert cfg.lookback == timedelta(hours=24)
    assert cfg.timestamp_column == "TIMESTAMP_DERIVED"
    assert cfg.page_size == 1000
    # Resiliency knobs (ko.md §7.4): settle disabled by default; abandon after 24h.
    assert cfg.settle_window == timedelta(0)
    assert cfg.download_max_age == timedelta(hours=24)


def test_eventlogfile_resiliency_durations_parse() -> None:
    cfg = EventLogFileConfig(
        enabled=True,
        event_types=["Login"],
        settle_window="5m",  # type: ignore[arg-type]
        download_max_age="48h",  # type: ignore[arg-type]
    )
    assert cfg.settle_window == timedelta(minutes=5)
    assert cfg.download_max_age == timedelta(hours=48)


def test_eventlogfile_rich_per_type_objects() -> None:
    cfg = EventLogFileConfig(
        enabled=True,
        event_types=[
            "Login",
            {
                "name": "ReportExport",
                "structured_metadata_fields": ["REPORT_ID", "OWNER_ID"],
                "labels": ["DELEGATED_USER"],
            },
        ],
    )
    assert [t.name for t in cfg.event_types] == ["Login", "ReportExport"]
    # Bare string -> no overrides (falls back to global at runtime).
    assert cfg.event_types[0].structured_metadata_fields is None
    assert cfg.event_types[0].labels == []
    # Rich object carries per-type sm + label promotion.
    assert cfg.event_types[1].structured_metadata_fields == ["REPORT_ID", "OWNER_ID"]
    assert cfg.event_types[1].labels == ["DELEGATED_USER"]


def test_eventlogfile_type_rejects_reserved_label() -> None:
    with pytest.raises(ValidationError):
        EventLogFileTypeConfig(name="Login", labels=["source"])


def test_eventlogfile_type_rejects_invalid_label_identifier() -> None:
    with pytest.raises(ValidationError):
        EventLogFileTypeConfig(name="Login", labels=["not-a-valid-label"])


def test_eventlogfile_type_accepts_valid_label() -> None:
    cfg = EventLogFileTypeConfig(name="API", labels=["API_TYPE", "METHOD_NAME"])
    assert cfg.labels == ["API_TYPE", "METHOD_NAME"]


def test_loki_batch_max_line_bytes_default() -> None:
    assert LokiBatchConfig().max_line_bytes == 262144


def test_loki_batch_max_line_bytes_override() -> None:
    assert LokiBatchConfig(max_line_bytes=0).max_line_bytes == 0


def test_eventlogfile_interval_rejects_junk() -> None:
    with pytest.raises(ValidationError):
        EventLogFileConfig(enabled=True, event_types=["Login"], interval="weekly")  # type: ignore[arg-type]


def test_eventlogfile_enabled_requires_event_types() -> None:
    with pytest.raises(ValidationError):
        EventLogFileConfig(enabled=True, event_types=[])


def test_eventlogfile_disabled_allows_empty_event_types() -> None:
    cfg = EventLogFileConfig(enabled=False)
    assert cfg.event_types == []


def test_eventlogfile_duration_shorthand() -> None:
    cfg = EventLogFileConfig(
        enabled=True,
        event_types=["Login"],
        poll_interval="15m",  # type: ignore[arg-type]
        lookback="2h",  # type: ignore[arg-type]
    )
    assert cfg.poll_interval == timedelta(minutes=15)
    assert cfg.lookback == timedelta(hours=2)


def test_sources_config_allow_overlap_default_false() -> None:
    assert SourcesConfig().allow_overlap is False


# ---------------------------------------------------------------------------
# Auth mode + environment toggle


def test_environment_sandbox_derives_login_url() -> None:
    cfg = SalesforceConfig(client_id="cid", username="svc@example.com", environment="sandbox")
    assert cfg.login_url == "https://test.salesforce.com"


def test_environment_production_is_default_login_url() -> None:
    cfg = SalesforceConfig(client_id="cid", username="svc@example.com")
    assert cfg.environment == "production"
    assert cfg.login_url == "https://login.salesforce.com"


def test_explicit_login_url_overrides_environment() -> None:
    cfg = SalesforceConfig(
        client_id="cid",
        username="svc@example.com",
        environment="sandbox",
        login_url="https://acme.my.salesforce.com",
    )
    assert cfg.login_url == "https://acme.my.salesforce.com"


def test_auth_mode_defaults_to_jwt_bearer() -> None:
    cfg = SalesforceConfig(client_id="cid", username="svc@example.com")
    assert cfg.auth_mode == "jwt_bearer"


def test_jwt_bearer_requires_username() -> None:
    with pytest.raises(ValidationError):
        SalesforceConfig(client_id="cid", auth_mode="jwt_bearer")


def test_client_credentials_does_not_require_username() -> None:
    cfg = SalesforceConfig(
        client_id="cid",
        auth_mode="client_credentials",
        login_url="https://acme.my.salesforce.com",
    )
    assert cfg.username == ""
    assert cfg.auth_mode == "client_credentials"


def _write_cc_config(tmp_path: Path, secret_file: Path) -> Path:
    p = tmp_path / "cc.yaml"
    p.write_text(
        f"""
salesforce:
  auth_mode: client_credentials
  client_id: cid
  login_url: https://acme.my.salesforce.com
  client_secret_file: {secret_file}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    return p


def test_client_credentials_resolves_secret_file(tmp_path: Path) -> None:
    sec = tmp_path / "secret.txt"
    sec.write_text("topsecret\n")
    cfg = load(_write_cc_config(tmp_path, sec))
    assert cfg.salesforce.client_secret is not None
    assert cfg.salesforce.client_secret.get_secret_value() == "topsecret"
    # private key is NOT required in client_credentials mode
    assert cfg.salesforce.private_key is None


def test_client_credentials_missing_secret_is_fatal(tmp_path: Path) -> None:
    cfg_path = _write_cc_config(tmp_path, tmp_path / "nope.txt")
    with pytest.raises(ConfigError):
        load(cfg_path)


# ---------------------------------------------------------------------------
# Telemetry (OTLP metrics egress) config + auth header resolution


def test_telemetry_disabled_by_default() -> None:
    t = TelemetryConfig()
    assert t.enabled is False
    assert t.auth == "basic"


def test_telemetry_headers_basic_defaults_to_loki_creds() -> None:
    t = TelemetryConfig(enabled=True, endpoint="https://otlp/otlp/v1/metrics")
    loki = LokiConfig(url="http://loki/push", tenant_id="12345", auth_token=SecretStr("glc_tok"))
    headers = telemetry_headers(t, loki)
    expected = "Basic " + base64.b64encode(b"12345:glc_tok").decode("ascii")
    assert headers["Authorization"] == expected
    assert "\n" not in headers["Authorization"]


def test_telemetry_headers_explicit_creds_override_loki() -> None:
    t = TelemetryConfig(enabled=True, basic_auth_user="999", basic_auth_token=SecretStr("tok2"))
    loki = LokiConfig(url="http://loki/push", tenant_id="12345", auth_token=SecretStr("glc_tok"))
    headers = telemetry_headers(t, loki)
    assert headers["Authorization"] == "Basic " + base64.b64encode(b"999:tok2").decode("ascii")


def test_telemetry_headers_none_auth_omits_authorization() -> None:
    t = TelemetryConfig(enabled=True, auth="none")
    loki = LokiConfig(url="http://loki/push", tenant_id="12345", auth_token=SecretStr("glc_tok"))
    assert "Authorization" not in telemetry_headers(t, loki)


def test_telemetry_headers_explicit_headers_merge() -> None:
    t = TelemetryConfig(enabled=True, auth="none", headers={"X-Scope-OrgID": "7"})
    headers = telemetry_headers(t, LokiConfig(url="http://loki/push"))
    assert headers["X-Scope-OrgID"] == "7"


def test_eventlogfile_wildcard_and_exclude() -> None:
    from sf2loki.config import EventLogFileConfig

    cfg = EventLogFileConfig(enabled=True, event_types=["*"], exclude=["ApexCallout", "Login"])
    assert cfg.discover is True
    assert cfg.exclude == ["ApexCallout", "Login"]
    assert any(t.name == "*" for t in cfg.event_types)


def test_eventlogfile_no_wildcard_discover_false() -> None:
    from sf2loki.config import EventLogFileConfig

    cfg = EventLogFileConfig(enabled=True, event_types=["Login", "API"])
    assert cfg.discover is False
    assert cfg.exclude == []


# ---------------------------------------------------------------------------
# extra="forbid": unknown/typo'd keys must fail loudly, not silently no-op


def test_unknown_top_level_key_is_config_error(tmp_path: Path) -> None:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
sourcess:
  pubsub:
    enabled: false
""".lstrip()
    )
    with pytest.raises(ConfigError, match="sourcess"):
        load(p)


def test_typoed_nested_key_fails_with_path_and_hint(tmp_path: Path) -> None:
    """The exact real-world trap: `event_type:` (singular) used to be silently
    ignored, disabling ELF ingestion while --check passed."""
    key = tmp_path / "k.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sources:
  eventlogfile:
    enabled: true
    event_type: ["Login"]
    event_types: ["Login"]
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    with pytest.raises(ConfigError) as excinfo:
        load(p)
    msg = str(excinfo.value)
    assert "sources.eventlogfile.event_type" in msg
    assert "unknown" in msg.lower()


def test_every_config_model_forbids_extras() -> None:
    for model in _all_config_models():
        assert model.model_config.get("extra") == "forbid", (
            f"{model.__name__} must set extra='forbid'"
        )


# ---------------------------------------------------------------------------
# client_credentials requires a My Domain token endpoint (Salesforce rejects
# the grant at login.salesforce.com / test.salesforce.com)


def test_client_credentials_rejects_generic_login_url() -> None:
    with pytest.raises(ValidationError, match=r"my\.salesforce\.com"):
        SalesforceConfig(
            client_id="cid",
            auth_mode="client_credentials",
            login_url="https://login.salesforce.com",
        )


def test_client_credentials_rejects_derived_generic_login_url() -> None:
    # No explicit login_url -> derived https://login.salesforce.com -> must fail.
    with pytest.raises(ValidationError, match="login_url"):
        SalesforceConfig(client_id="cid", auth_mode="client_credentials")


def test_client_credentials_rejects_sandbox_generic_login_url() -> None:
    with pytest.raises(ValidationError):
        SalesforceConfig(client_id="cid", auth_mode="client_credentials", environment="sandbox")


def test_client_credentials_accepts_my_domain_login_url() -> None:
    cfg = SalesforceConfig(
        client_id="cid",
        auth_mode="client_credentials",
        login_url="https://acme.my.salesforce.com",
    )
    assert cfg.login_url == "https://acme.my.salesforce.com"


# ---------------------------------------------------------------------------
# Validation hardening: identifiers, bounds, enums


def test_client_id_must_be_non_empty() -> None:
    with pytest.raises(ValidationError):
        SalesforceConfig(client_id="", username="svc@example.com")


def test_loki_url_must_be_non_empty() -> None:
    with pytest.raises(ValidationError):
        LokiConfig(url="")


def test_eventlog_object_name_must_be_soql_identifier() -> None:
    with pytest.raises(ValidationError):
        EventLogObjectConfig(name="Login'Event")
    with pytest.raises(ValidationError):
        EventLogObjectConfig(name="")


def test_eventlog_object_custom_object_name_is_valid() -> None:
    assert EventLogObjectConfig(name="MyAudit__c").name == "MyAudit__c"


def test_eventlog_object_timestamp_field_must_be_soql_identifier() -> None:
    with pytest.raises(ValidationError):
        EventLogObjectConfig(name="LoginEvent", timestamp_field="EventDate;DROP")


def test_eventlogfile_event_type_must_be_soql_identifier_or_wildcard() -> None:
    with pytest.raises(ValidationError):
        EventLogFileTypeConfig(name="Login'")
    with pytest.raises(ValidationError):
        EventLogFileTypeConfig(name="")
    assert EventLogFileTypeConfig(name="*").name == "*"
    assert EventLogFileTypeConfig(name="ReportExport").name == "ReportExport"


def test_pubsub_default_num_requested_bounds() -> None:
    from sf2loki.config import PubSubConfig

    with pytest.raises(ValidationError):
        PubSubConfig(default_num_requested=0)
    with pytest.raises(ValidationError):
        PubSubConfig(default_num_requested=101)  # Salesforce clamps at 100
    assert PubSubConfig(default_num_requested=100).default_num_requested == 100
    assert PubSubConfig(default_num_requested=1).default_num_requested == 1


def test_log_level_rejects_typo() -> None:
    with pytest.raises(ValidationError):
        ServiceConfig(log_level="inof")  # used to silently fall back to INFO


def test_log_level_is_case_insensitive() -> None:
    assert ServiceConfig(log_level="INFO").log_level == "info"
    assert ServiceConfig(log_level="Debug").log_level == "debug"


# ---------------------------------------------------------------------------
# Telemetry basic-auth credentials must resolve at load time (no silent
# unauthenticated OTLP)


def _write_telemetry_config(tmp_path: Path, *, with_loki_creds: bool) -> Path:
    key = tmp_path / "k.pem"
    key.write_text("PK")
    loki_creds = ""
    if with_loki_creds:
        tok = tmp_path / "loki-token"
        tok.write_text("glc_tok")
        loki_creds = f"    tenant_id: '12345'\n    auth_token_file: {tok}\n"
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
{loki_creds}service:
  telemetry:
    enabled: true
    endpoint: https://otlp-gateway-prod-eu-west-2.grafana.net/otlp/v1/metrics
    auth: basic
""".lstrip()
    )
    return p


def test_telemetry_basic_auth_unresolvable_is_fatal_at_load(tmp_path: Path) -> None:
    p = _write_telemetry_config(tmp_path, with_loki_creds=False)
    with pytest.raises(ConfigError, match="telemetry"):
        load(p)


def test_telemetry_basic_auth_falls_back_to_loki_creds_ok(tmp_path: Path) -> None:
    p = _write_telemetry_config(tmp_path, with_loki_creds=True)
    cfg = load(p)
    assert telemetry_headers(cfg.service.telemetry, cfg.sink.loki)["Authorization"].startswith(
        "Basic "
    )


# ---------------------------------------------------------------------------
# Secret file permission errors must be actionable (uid-10001 crash-loop trap)


def test_secret_file_permission_error_is_actionable(tmp_path: Path) -> None:
    import os

    if os.geteuid() == 0:
        pytest.skip("running as root; permission bits are not enforced")
    key = tmp_path / "k.pem"
    key.write_text("PK")
    key.chmod(0o000)
    try:
        cfg_path = _write_config(tmp_path, key)
        with pytest.raises(ConfigError) as excinfo:
            load(cfg_path)
        msg = str(excinfo.value)
        assert "chmod 640" in msg
        assert "10001" in msg
    finally:
        key.chmod(0o600)


# ---------------------------------------------------------------------------
# Configurable assumed token TTL (real expiry = org session timeout)


def test_salesforce_token_ttl_defaults_to_one_hour() -> None:
    cfg = SalesforceConfig(client_id="cid", username="svc@example.com")
    assert cfg.token_ttl == timedelta(hours=1)


def test_salesforce_token_ttl_accepts_shorthand() -> None:
    cfg = SalesforceConfig(client_id="cid", username="svc@example.com", token_ttl="15m")  # type: ignore[arg-type]
    assert cfg.token_ttl == timedelta(minutes=15)


def test_apexlog_config_defaults_and_user_validation() -> None:
    from sf2loki.config import ApexLogConfig

    c = ApexLogConfig()
    assert c.enabled is False
    assert c.poll_interval == timedelta(minutes=1)
    assert c.lookback == timedelta(hours=1)
    assert c.users == []
    assert c.max_body_bytes == 5_242_880
    assert c.sample == 1.0

    # a username with a single quote would break the SOQL IN(...) clause
    with pytest.raises(ValueError, match="username"):
        ApexLogConfig(users=["ok@example.com", "bad'; DROP--"])


def test_sources_config_has_apexlog() -> None:
    from sf2loki.config import ApexLogConfig, SourcesConfig

    assert isinstance(SourcesConfig().apexlog, ApexLogConfig)


def test_eventlog_object_big_object_flag_defaults_false() -> None:
    from sf2loki.config import EventLogObjectConfig

    assert EventLogObjectConfig(name="MyAudit__c").big_object is False
    assert EventLogObjectConfig(name="LoginEvent", big_object=True).big_object is True


# --- GCS state store + k8s_lease coordinator config seams (#37, #36) ----------


def test_state_store_gcs_requires_bucket() -> None:
    from sf2loki.config import StateConfig

    with pytest.raises(ValueError, match="bucket"):
        StateConfig(store="gcs")  # gcs.bucket empty
    StateConfig(store="gcs", gcs={"bucket": "b"})  # ok


def test_gcs_state_config_defaults() -> None:
    from sf2loki.config import GcsStateConfig

    c = GcsStateConfig(bucket="b")
    assert c.object_name == "sf2loki/state.json"
    assert c.service_file is None


def test_state_store_s3_still_requires_bucket() -> None:
    # The validator rename must not regress the existing s3 rule.
    from sf2loki.config import StateConfig

    with pytest.raises(ValueError, match="bucket"):
        StateConfig(store="s3")


def test_k8s_lease_renew_must_beat_duration() -> None:
    from sf2loki.config import K8sLeaseConfig

    with pytest.raises(ValueError, match="less than half"):
        K8sLeaseConfig(lease_duration=timedelta(seconds=10), renew_interval=timedelta(seconds=8))
    K8sLeaseConfig(lease_duration=timedelta(seconds=30), renew_interval=timedelta(seconds=10))  # ok


def test_k8s_lease_config_defaults() -> None:
    from sf2loki.config import K8sLeaseConfig

    c = K8sLeaseConfig()
    assert c.namespace == "default"
    assert c.name == "sf2loki-leader"
    assert c.identity == ""
    assert c.kubeconfig is None


def test_coordinate_type_accepts_k8s_lease() -> None:
    from sf2loki.config import CoordinateConfig

    c = CoordinateConfig(type="k8s_lease", k8s_lease={"namespace": "ns", "name": "l"})
    assert c.type == "k8s_lease" and c.k8s_lease.namespace == "ns"
