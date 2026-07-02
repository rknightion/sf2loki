"""build_store factory: remote backends fail fast with an actionable ConfigError
when their optional extra is not installed (rather than a raw ImportError on the
first commit)."""

from __future__ import annotations

import importlib.util

import pytest

from sf2loki.config import ConfigError, StateConfig
from sf2loki.state import build_store
from sf2loki.state.file_store import FileCheckpointStore


def test_build_store_defaults_to_file() -> None:
    assert isinstance(build_store(StateConfig()), FileCheckpointStore)


@pytest.mark.skipif(
    importlib.util.find_spec("aiobotocore") is not None, reason="s3 extra installed"
)
def test_build_store_s3_without_extra_raises_actionable_error() -> None:
    with pytest.raises(ConfigError, match=r"sf2loki\[s3\]"):
        build_store(StateConfig(store="s3", s3={"bucket": "b"}))


@pytest.mark.skipif(importlib.util.find_spec("gcloud") is not None, reason="gcs extra installed")
def test_build_store_gcs_without_extra_raises_actionable_error() -> None:
    # Must surface the friendly ConfigError, NOT a raw ModuleNotFoundError — the
    # "gcloud" top-level name is checked (find_spec on the dotted path would raise).
    with pytest.raises(ConfigError, match=r"sf2loki\[gcs\]"):
        build_store(StateConfig(store="gcs", gcs={"bucket": "b"}))
