"""Drift gate: committed config artifacts must match the generator, and every
hand-authored config (docker + presets) must parse against the ``Config`` schema.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

import pytest
import yaml

from sf2loki import configdoc
from sf2loki.config import Config, _interpolate_env

ROOT = Path(__file__).resolve().parents[1]


def test_example_yaml_matches_generator():
    assert (ROOT / "config.example.yaml").read_text() == configdoc.example_yaml()


def test_reference_md_matches_generator():
    assert (ROOT / "docs/config-reference.md").read_text() == configdoc.reference_markdown()


def _known_keys(model: type[Any]) -> set[str]:
    return set(model.model_fields)


def _assert_known_keys(data: dict[str, Any], model: type[Any], where: str) -> None:
    """Assert every key in ``data`` is a known field of ``model`` (recursing one
    level into nested mapping/list-of-mapping fields so a misspelled nested key
    is also caught).
    """
    known = _known_keys(model)
    unknown = set(data) - known
    assert not unknown, f"{where}: unknown key(s) {sorted(unknown)} (known: {sorted(known)})"
    for name, value in data.items():
        field = model.model_fields[name]
        annotation = configdoc._unwrap_optional(field.annotation)
        if configdoc._is_model_type(field.annotation) and isinstance(value, dict):
            _assert_known_keys(value, annotation, f"{where}.{name}")
        item_model = configdoc._list_item_model_type(field.annotation)
        if item_model is not None and isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    _assert_known_keys(item, item_model, f"{where}.{name}[{i}]")


@pytest.mark.parametrize(
    "path",
    sorted(map(Path, glob.glob(str(ROOT / "examples/presets/*.yaml")))),
)
def test_preset_configs_have_known_keys(path: Path):
    # Presets are fragments (not a full Config), so validate structurally:
    # every top-level and nested key must be a known field on the schema.
    data = yaml.safe_load(path.read_text())
    assert isinstance(data, dict)
    _assert_known_keys(data, Config, path.name)


def test_docker_config_constructs_against_schema(monkeypatch: pytest.MonkeyPatch):
    # config.docker.yaml is a full config (references ${VAR}s + *_file secrets).
    # Stub the env vars it interpolates and construct a real Config, but skip
    # resolve_secrets() so the (non-existent, in CI) mounted secret files don't
    # need to exist.
    env = {
        "SF_LOGIN_URL": "https://example-dev-ed.my.salesforce.com",
        "SF_CLIENT_ID": "3MVG9test-client-id",
        "SF_USERNAME": "svc@example.com",
        "GC_LOKI": "https://logs-prod-999.grafana.net/loki/api/v1/push",
        "GC_LOKI_USER": "123456",
        "GC_OTLP_ENDPOINT": "https://otlp-gateway-prod-999.grafana.net/otlp",
        "GC_OTLP_USER": "654321",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    raw = yaml.safe_load((ROOT / "config.docker.yaml").read_text())
    assert isinstance(raw, dict)
    _assert_known_keys(raw, Config, "config.docker.yaml")

    data = _interpolate_env(raw)
    cfg = Config(**data)  # deliberately not resolve_secrets(cfg) — no secret files here.

    assert cfg.salesforce.login_url == env["SF_LOGIN_URL"]
    assert cfg.sink.loki.url == env["GC_LOKI"]
