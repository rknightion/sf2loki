"""Tests for FileCheckpointStore."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from sf2loki.state.file_store import (
    FileCheckpointStore,
    StateFileCorruptError,
    StateFileLockError,
)


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
    store1.close()  # release the exclusive lock so a successor instance can start

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
async def test_second_instance_on_same_state_file_fails_fast(tmp_path: Path) -> None:
    """Two instances sharing a state file would double-ingest and clobber each
    other's checkpoints — the second must fail fast with a clear error."""
    path = tmp_path / "state.json"
    store1 = FileCheckpointStore(path)
    await store1.commit("k", "v")  # first use acquires the exclusive lock

    store2 = FileCheckpointStore(path)
    with pytest.raises(StateFileLockError) as exc_info:
        await store2.load("k")

    msg = str(exc_info.value)
    assert str(path) in msg
    assert "another" in msg.lower()  # points at the concurrent-instance cause

    # Releasing the first instance's lock lets a successor start.
    store1.close()
    store3 = FileCheckpointStore(path)
    assert await store3.load("k") == "v"


@pytest.mark.asyncio
async def test_corrupt_state_file_raises_actionable_error(tmp_path: Path) -> None:
    """Corrupt/truncated JSON must raise a clear, actionable error — not a raw
    traceback crash-loop, and never a silent state discard."""
    path = tmp_path / "state.json"
    path.write_text('{"k1": "v1"')  # truncated

    store = FileCheckpointStore(path)
    with pytest.raises(StateFileCorruptError) as exc_info:
        await store.load("k1")

    msg = str(exc_info.value)
    assert str(path) in msg
    assert "aside" in msg  # tells the operator moving the file aside resets state
    # The corrupt file is preserved, not discarded.
    assert path.read_text() == '{"k1": "v1"'


@pytest.mark.asyncio
async def test_non_object_state_file_raises_actionable_error(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text('["not", "a", "dict"]')

    store = FileCheckpointStore(path)
    with pytest.raises(StateFileCorruptError) as exc_info:
        await store.load("k1")

    assert str(path) in str(exc_info.value)


@pytest.mark.asyncio
async def test_close_is_idempotent(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k", "v")
    store.close()
    store.close()  # second close must not raise


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
