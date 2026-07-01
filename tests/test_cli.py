"""Tests for the CLI entrypoint (cli.py), focused on --check (offline validation)."""

from __future__ import annotations

from pathlib import Path

import pytest

from sf2loki.cli import main


def _valid_config(tmp_path: Path) -> Path:
    key = tmp_path / "key.pem"
    key.write_text("PK")
    p = tmp_path / "config.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sources:
  pubsub:
    enabled: false
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    return p


def test_check_ok_returns_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rc = main(["--config", str(_valid_config(tmp_path)), "--check"])
    assert rc == 0
    assert "OK" in capsys.readouterr().out


def test_check_fails_on_missing_secret(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(
        """
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: /does/not/exist.pem
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    rc = main(["--config", str(p), "--check"])
    assert rc == 1
    assert "FAILED" in capsys.readouterr().err


def test_check_fails_on_source_overlap(tmp_path: Path) -> None:
    """Two sources feeding the same category must fail the offline check."""
    key = tmp_path / "key.pem"
    key.write_text("PK")
    p = tmp_path / "overlap.yaml"
    p.write_text(
        f"""
salesforce:
  client_id: cid
  username: svc@example.com
  private_key_file: {key}
sources:
  pubsub:
    enabled: true
    topics: ["/event/LoginEventStream"]
  eventlog_objects:
    enabled: true
    objects:
      - {{name: LoginEvent}}
sink:
  loki:
    url: http://loki:3100/loki/api/v1/push
""".lstrip()
    )
    rc = main(["--config", str(p), "--check"])
    assert rc == 1


def test_config_example_subcommand_prints_yaml(capsys):
    rc = main(["config", "example"])
    out = capsys.readouterr().out
    assert rc == 0 and "salesforce:" in out


def test_config_schema_subcommand_prints_json(capsys):
    rc = main(["config", "schema"])
    out = capsys.readouterr().out
    assert rc == 0 and '"title": "Config"' in out
