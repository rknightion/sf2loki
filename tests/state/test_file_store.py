"""Tests for FileCheckpointStore."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from sf2loki.state.file_store import FileCheckpointStore


@pytest.mark.asyncio
async def test_load_unknown_key_returns_none(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    result = await store.load("no-such-key")
    assert result is None


@pytest.mark.asyncio
async def test_commit_then_load_round_trips(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "value1")
    result = await store.load("k1")
    assert result == "value1"


@pytest.mark.asyncio
async def test_persistence_across_instances(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store1 = FileCheckpointStore(path)
    await store1.commit("stream-a", "offset-42")

    store2 = FileCheckpointStore(path)
    result = await store2.load("stream-a")
    assert result == "offset-42"


@pytest.mark.asyncio
async def test_multiple_keys_coexist(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "v1")
    await store.commit("k2", "v2")
    await store.commit("k3", "v3")

    assert await store.load("k1") == "v1"
    assert await store.load("k2") == "v2"
    assert await store.load("k3") == "v3"


@pytest.mark.asyncio
async def test_overwrite_updates_key(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k1", "old")
    await store.commit("k1", "new")
    assert await store.load("k1") == "new"


@pytest.mark.asyncio
async def test_creates_parent_directory(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deep" / "state.json"
    store = FileCheckpointStore(path)
    await store.commit("k", "v")
    assert path.exists()
    assert await store.load("k") == "v"


@pytest.mark.asyncio
async def test_atomic_write_produces_valid_json(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    await store.commit("k1", "v1")
    # File on disk should be valid JSON with the expected content
    data = json.loads(path.read_text())
    assert data == {"k1": "v1"}


@pytest.mark.asyncio
async def test_concurrent_commits_no_corruption(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)

    # Concurrent commits: all keys should survive
    keys = [f"key-{i}" for i in range(20)]
    await asyncio.gather(*[store.commit(k, f"val-{k}") for k in keys])

    # File must be valid JSON
    data = json.loads(path.read_text())
    assert isinstance(data, dict)
    # All keys must be present (may have serialised to any order, but all must be there)
    for k in keys:
        assert k in data, f"missing {k}"
        assert data[k] == f"val-{k}"
