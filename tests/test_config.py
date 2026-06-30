from __future__ import annotations

from pathlib import Path

import pytest

from sf2loki.config import ConfigError, load


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
