from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
from pydantic import ValidationError

from sf2loki.config import ConfigError, EventLogObjectConfig, ServiceConfig, load


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
