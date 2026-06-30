"""Shared test helpers."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from sf2loki.config import Config


@pytest.fixture
def config_with() -> Callable[..., Config]:
    """Factory for a minimal valid Config, with shallow per-section overrides."""

    def _make(**overrides: object) -> Config:
        base: dict[str, object] = {
            "salesforce": {
                "client_id": "cid",
                "username": "svc@example.com",
                "private_key": "DUMMYKEY",
            },
            "sink": {"loki": {"url": "http://loki:3100/loki/api/v1/push"}},
        }
        base.update(overrides)
        return Config(**base)

    return _make
