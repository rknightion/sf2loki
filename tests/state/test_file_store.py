"""Tests for FileCheckpointStore."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from sf2loki.coordinate.base import StateFenceError
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


# ---------------------------------------------------------------------------
# commit_many (#54): merge N keys into one load + one flush.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_many_round_trips_all_keys(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit_many({"k1": "v1", "k2": "v2", "k3": "v3"})
    assert await store.load("k1") == "v1"
    assert await store.load("k2") == "v2"
    assert await store.load("k3") == "v3"


@pytest.mark.asyncio
async def test_commit_many_preserves_existing_keys(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    await store.commit("k0", "v0")
    await store.commit_many({"k1": "v1", "k2": "v2"})
    assert await store.load("k0") == "v0"
    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_commit_many_does_a_single_flush(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    calls: list[int] = []
    original_flush = store._flush

    def counting_flush() -> None:
        calls.append(1)
        original_flush()

    monkeypatch.setattr(store, "_flush", counting_flush)
    await store.commit_many({"k1": "v1", "k2": "v2", "k3": "v3"})
    assert len(calls) == 1
    data = json.loads(path.read_text())
    assert data == {"k1": "v1", "k2": "v2", "k3": "v3"}


@pytest.mark.asyncio
async def test_commit_many_fence_raises_before_any_write(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)

    def fence() -> None:
        raise StateFenceError("not leader")

    store.set_fence(fence)
    with pytest.raises(StateFenceError):
        await store.commit_many({"k1": "v1"})
    assert not path.exists()


# ---------------------------------------------------------------------------
# reset (#48): a demote -> external change -> promote cycle serves fresh
# values and commits cleanly instead of regressing/crashing.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_clears_cache_and_serves_fresh_value(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    await store.commit("k", "v1")
    assert await store.load("k") == "v1"

    # Simulate an external writer (e.g. the other instance in a file-lease
    # pair) changing the file directly while this instance still holds it.
    path.write_text(json.dumps({"k": "external"}))

    store.reset()
    assert await store.load("k") == "external"

    # A commit after reset must merge onto the fresh doc, not the stale cache.
    await store.commit("k2", "v2")
    data = json.loads(path.read_text())
    assert data == {"k": "external", "k2": "v2"}


@pytest.mark.asyncio
async def test_reset_before_any_load_is_a_noop(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    store.reset()  # must not raise even though nothing was ever loaded
    assert await store.load("k") is None


# ---------------------------------------------------------------------------
# exclusive_lock=False (#49): skip the sidecar flock for coordinator-backed
# HA deployments where the lease itself is the exclusivity mechanism.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exclusive_lock_false_allows_two_instances(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store1 = FileCheckpointStore(path, exclusive_lock=False)
    store2 = FileCheckpointStore(path, exclusive_lock=False)

    await store1.commit("k1", "v1")
    await store2.commit("k2", "v2")  # would raise StateFileLockError if flock taken

    assert await store1.load("k1") == "v1"


@pytest.mark.asyncio
async def test_exclusive_lock_default_true_still_fails_fast(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store1 = FileCheckpointStore(path)
    await store1.commit("k", "v")

    store2 = FileCheckpointStore(path)
    with pytest.raises(StateFileLockError):
        await store2.load("k")


# ---------------------------------------------------------------------------
# set_epoch (#47): a CAS-less protection against a stale leader that is still
# passing the (lagging) boolean set_fence check.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_records_epoch_when_set(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    store.set_epoch(lambda: 3)
    await store.commit("k", "v")
    data = json.loads(path.read_text())
    assert data["k"] == "v"
    assert data["__fence_epoch__"] == "3"


@pytest.mark.asyncio
async def test_commit_many_records_epoch_once(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    store.set_epoch(lambda: 2)
    await store.commit_many({"a": "1", "b": "2"})
    data = json.loads(path.read_text())
    assert data == {"a": "1", "b": "2", "__fence_epoch__": "2"}


@pytest.mark.asyncio
async def test_stale_leader_rejected_by_epoch_even_when_fence_boolean_says_leader(
    tmp_path: Path,
) -> None:
    """A stale leader whose local set_fence() boolean has not yet flipped (the
    scenario #47 describes) must still be rejected once a newer leader has
    persisted a higher epoch to the shared file."""
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path, exclusive_lock=False)
    store.set_epoch(lambda: 5)
    store.set_fence(lambda: None)  # local boolean still (wrongly) says "leader"
    await store.commit("k", "v1")  # persists epoch 5

    # A newer leader (epoch 10) advances the shared file directly.
    doc = json.loads(path.read_text())
    doc["__fence_epoch__"] = "10"
    doc["k"] = "from-newer-leader"
    path.write_text(json.dumps(doc))

    store.reset()
    with pytest.raises(StateFenceError):
        await store.commit("k2", "v2")

    # The stale commit must never have been written.
    data = json.loads(path.read_text())
    assert "k2" not in data
    assert data["k"] == "from-newer-leader"


@pytest.mark.asyncio
async def test_epoch_fence_allows_commit_when_epoch_is_not_older(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    store = FileCheckpointStore(path)
    store.set_epoch(lambda: 5)
    await store.commit("k", "v1")
    await store.commit("k2", "v2")  # same epoch every time — must not be rejected
    data = json.loads(path.read_text())
    assert data["k2"] == "v2"


@pytest.mark.asyncio
async def test_set_fence_is_checked_before_epoch(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "state.json")
    store.set_epoch(lambda: 1)

    def fence() -> None:
        raise StateFenceError("boolean fence says not-leader")

    store.set_fence(fence)
    with pytest.raises(StateFenceError, match="boolean fence"):
        await store.commit("k", "v")
