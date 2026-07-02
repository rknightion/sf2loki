"""Tests for GcsCheckpointStore.

Uses a tiny in-memory fake GCS client (no real network) that mimics the
gcloud-aio-storage shapes the store actually depends on: coroutine
``download``/``download_metadata``/``upload`` methods, and a ``.status``
attribute on raised errors. The real ``gcloud-aio-storage`` package is never
imported here — that is the point of the lazy-import design under test.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

import sf2loki.state.gcs_store as gcs_store_module
from sf2loki.config import GcsStateConfig
from sf2loki.state.gcs_store import GcsCheckpointStore
from sf2loki.state.s3_store import StateObjectCorruptError, StateStoreConflictError


class FakeGcsError(Exception):
    """Mimics the gcloud-aio-storage / aiohttp ClientResponseError shape."""

    def __init__(self, status: int, message: str = "error") -> None:
        self.status = status
        super().__init__(message)


class FakeGcsBackend:
    """Shared in-memory (bucket, object_name) -> (body, generation) store."""

    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], tuple[bytes, int]] = {}
        self._generation_counter = 0
        self.download_calls = 0
        self.download_metadata_calls = 0
        self.upload_calls = 0
        self.concurrent_uploads = 0
        self.max_concurrent_uploads = 0
        self._upload_delay = 0.0

    def set_upload_delay(self, seconds: float) -> None:
        self._upload_delay = seconds

    async def download(self, bucket: str, object_name: str) -> bytes:
        self.download_calls += 1
        obj = self._objects.get((bucket, object_name))
        if obj is None:
            raise FakeGcsError(404, "not found")
        body, _generation = obj
        return body

    async def download_metadata(self, bucket: str, object_name: str) -> dict[str, Any]:
        self.download_metadata_calls += 1
        obj = self._objects.get((bucket, object_name))
        if obj is None:
            raise FakeGcsError(404, "not found")
        _body, generation = obj
        return {"generation": generation}

    async def upload(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        *,
        parameters: dict[str, str],
    ) -> dict[str, Any]:
        self.upload_calls += 1
        self.concurrent_uploads += 1
        self.max_concurrent_uploads = max(self.max_concurrent_uploads, self.concurrent_uploads)
        try:
            if self._upload_delay:
                await asyncio.sleep(self._upload_delay)
            existing = self._objects.get((bucket, object_name))
            match = parameters.get("ifGenerationMatch")
            if match == "0":
                if existing is not None:
                    raise FakeGcsError(412, "precondition failed")
            elif match is not None:
                if existing is None or str(existing[1]) != match:
                    raise FakeGcsError(412, "precondition failed")
            else:  # pragma: no cover - defensive; store always sends a precondition
                raise AssertionError("upload called without ifGenerationMatch")
            self._generation_counter += 1
            generation = self._generation_counter
            self._objects[(bucket, object_name)] = (data, generation)
            return {"generation": generation}
        finally:
            self.concurrent_uploads -= 1


class FakeGcsClient:
    """Thin per-store client view over a shared FakeGcsBackend."""

    def __init__(self, backend: FakeGcsBackend) -> None:
        self._backend = backend

    async def download(self, bucket: str, object_name: str) -> bytes:
        return await self._backend.download(bucket, object_name)

    async def download_metadata(self, bucket: str, object_name: str) -> dict[str, Any]:
        return await self._backend.download_metadata(bucket, object_name)

    async def upload(
        self,
        bucket: str,
        object_name: str,
        data: bytes,
        *,
        parameters: dict[str, str],
    ) -> dict[str, Any]:
        return await self._backend.upload(bucket, object_name, data, parameters=parameters)


class TrackingClientFactory:
    """A client_factory that records whether its context manager was entered/exited."""

    def __init__(self, client: FakeGcsClient) -> None:
        self.client = client
        self.entered = False
        self.exited = False

    def __call__(self) -> AbstractAsyncContextManager[FakeGcsClient]:
        return self._cm()

    @asynccontextmanager
    async def _cm(self):  # type: ignore[no-untyped-def]
        self.entered = True
        try:
            yield self.client
        finally:
            self.exited = True


def make_store(
    backend: FakeGcsBackend | None = None,
    *,
    bucket: str = "test-bucket",
    object_name: str = "sf2loki/state.json",
) -> tuple[GcsCheckpointStore, FakeGcsBackend, TrackingClientFactory]:
    backend = backend or FakeGcsBackend()
    client = FakeGcsClient(backend)
    factory = TrackingClientFactory(client)
    cfg = GcsStateConfig(bucket=bucket, object_name=object_name)
    store = GcsCheckpointStore(cfg, client_factory=factory)
    return store, backend, factory


@pytest.mark.asyncio
async def test_load_missing_object_returns_none() -> None:
    store, backend, _ = make_store()
    result = await store.load("no-such-key")
    assert result is None
    assert backend.download_metadata_calls == 1


@pytest.mark.asyncio
async def test_load_caches_after_first_get() -> None:
    store, backend, _ = make_store()
    await store.load("k1")
    await store.load("k2")
    assert backend.download_metadata_calls == 1  # second load hits the in-memory cache


@pytest.mark.asyncio
async def test_commit_creates_with_if_generation_match_zero() -> None:
    store, backend, _ = make_store()
    await store.commit("stream-a", "offset-1")
    assert backend.upload_calls == 1
    result = await store.load("stream-a")
    assert result == "offset-1"


@pytest.mark.asyncio
async def test_commit_then_load_on_fresh_store_round_trips() -> None:
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    await store1.commit("k1", "v1")

    store2, _, _ = make_store(backend)
    assert await store2.load("k1") == "v1"


@pytest.mark.asyncio
async def test_commit_updates_with_if_generation_match() -> None:
    backend = FakeGcsBackend()
    store, _, _ = make_store(backend)
    await store.commit("k1", "v1")
    await store.commit("k1", "v2")
    assert await store.load("k1") == "v2"
    assert backend.upload_calls == 2


@pytest.mark.asyncio
async def test_commit_preserves_other_keys() -> None:
    store, _, _ = make_store()
    await store.commit("k1", "v1")
    await store.commit("k2", "v2")
    assert await store.load("k1") == "v1"
    assert await store.load("k2") == "v2"


@pytest.mark.asyncio
async def test_commit_after_load_round_trip_preserves_other_keys() -> None:
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    await store1.commit("k1", "v1")

    store2, _, _ = make_store(backend)
    await store2.load("k1")  # populate store2's cache from the shared backend
    await store2.commit("k2", "v2")

    assert await store2.load("k1") == "v1"
    assert await store2.load("k2") == "v2"


@pytest.mark.asyncio
async def test_create_conflict_raises_state_store_conflict_error() -> None:
    """Two stores race to create the object; the loser's ifGenerationMatch=0 fails."""
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    # Both stores observe the object as absent before either writes — mirrors
    # the real race where two fresh instances start up against an empty object.
    await store1.load("k1")
    await store2.load("k1")

    await store1.commit("k1", "from-1")
    with pytest.raises(StateStoreConflictError) as exc_info:
        await store2.commit("k1", "from-2")

    assert "another sf2loki instance" in str(exc_info.value)
    # The loser's local cache/generation must not have been updated by the failed upload.
    assert await store1.load("k1") == "from-1"


@pytest.mark.asyncio
async def test_update_conflict_raises_state_store_conflict_error() -> None:
    """Two stores both loaded the same version, then race to update it."""
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    await store1.commit("k1", "v1")  # creates the object
    await store2.load("k1")  # store2 now has the same generation cached

    await store1.commit("k1", "v2")  # store1 updates; generation moves on
    with pytest.raises(StateStoreConflictError):
        await store2.commit("k1", "v3")  # store2's stale generation is rejected


@pytest.mark.asyncio
async def test_split_brain_second_writer_fails_fast() -> None:
    """Split-brain scenario: two instances pointed at the same object must not both win."""
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    # Both instances start against the same empty object before either writes.
    await store1.load("shared-key")
    await store2.load("shared-key")

    results: list[str] = []
    for store, value in ((store1, "instance-1"), (store2, "instance-2")):
        try:
            await store.commit("shared-key", value)
            results.append("committed")
        except StateStoreConflictError:
            results.append("conflict")

    assert results == ["committed", "conflict"]


@pytest.mark.asyncio
async def test_non_object_state_document_raises_actionable_error() -> None:
    backend = FakeGcsBackend()
    backend._objects[("test-bucket", "sf2loki/state.json")] = (b'["not", "a", "dict"]', 1)
    store, _, _ = make_store(backend)

    with pytest.raises(StateObjectCorruptError) as exc_info:
        await store.load("k1")

    assert "test-bucket" in str(exc_info.value)


@pytest.mark.asyncio
async def test_close_exits_client_context() -> None:
    store, _, factory = make_store()
    await store.load("k1")  # forces the client to be created (entered)
    assert factory.entered is True
    assert factory.exited is False

    await store.close()
    assert factory.exited is True


@pytest.mark.asyncio
async def test_close_without_use_is_a_noop() -> None:
    store, _, factory = make_store()
    await store.close()
    assert factory.entered is False


@pytest.mark.asyncio
async def test_fence_raises_blocks_commit_before_any_upload() -> None:
    store, backend, _ = make_store()

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.commit("k1", "v1")

    assert backend.upload_calls == 0


@pytest.mark.asyncio
async def test_fence_allows_commit_when_it_does_not_raise() -> None:
    store, backend, _ = make_store()
    calls: list[str] = []
    store.set_fence(lambda: calls.append("checked"))

    await store.commit("k1", "v1")

    assert calls == ["checked"]
    assert backend.upload_calls == 1


# ---------------------------------------------------------------------------
# commit_many (#54): merge N keys into one conditional upload.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_many_does_a_single_upload() -> None:
    store, backend, _ = make_store()
    await store.commit_many({"k1": "v1", "k2": "v2", "k3": "v3"})
    assert backend.upload_calls == 1
    assert await store.load("k1") == "v1"
    assert await store.load("k2") == "v2"
    assert await store.load("k3") == "v3"


@pytest.mark.asyncio
async def test_commit_many_preserves_existing_keys() -> None:
    backend = FakeGcsBackend()
    store, _, _ = make_store(backend)
    await store.commit("k0", "v0")
    await store.commit_many({"k1": "v1", "k2": "v2"})
    assert await store.load("k0") == "v0"
    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_commit_many_fence_raises_before_any_upload() -> None:
    store, backend, _ = make_store()

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.commit_many({"k1": "v1"})
    assert backend.upload_calls == 0


# ---------------------------------------------------------------------------
# reset (#48): a demote -> external change -> promote cycle serves fresh
# values and commits cleanly, instead of a stale-generation 412 crash.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_clears_cache_and_generation_serves_fresh_value() -> None:
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    await store1.commit("k", "v1")

    # Another instance leads in between and advances the object.
    store2, _, _ = make_store(backend)
    await store2.commit("k", "external")

    store1.reset()
    assert await store1.load("k") == "external"
    # Without reset this would 412 on store1's stale generation.
    await store1.commit("k2", "v2")
    assert await store1.load("k2") == "v2"


@pytest.mark.asyncio
async def test_reset_before_any_load_is_a_noop() -> None:
    store, _, _ = make_store()
    store.reset()
    assert await store.load("k1") is None


# ---------------------------------------------------------------------------
# bounded retry on transient errors (#44); precondition conflicts still fail
# fast, unretried.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_commit_survives_two_transient_errors_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gcs_store_module, "_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MIN", 0.0)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MAX", 0.0)
    backend = FakeGcsBackend()
    store, _, _ = make_store(backend)

    calls = {"n": 0}
    original_upload = backend.upload

    async def flaky_upload(
        bucket: str, object_name: str, data: bytes, *, parameters: dict[str, str]
    ) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] <= 2:
            raise FakeGcsError(503, "service unavailable")
        result: dict[str, Any] = await original_upload(
            bucket, object_name, data, parameters=parameters
        )
        return result

    monkeypatch.setattr(backend, "upload", flaky_upload)

    await store.commit("k1", "v1")
    assert await store.load("k1") == "v1"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_load_survives_transient_error_then_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gcs_store_module, "_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MIN", 0.0)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MAX", 0.0)
    backend = FakeGcsBackend()
    seed, _, _ = make_store(backend)
    await seed.commit("k1", "v1")

    store, _, _ = make_store(backend)
    calls = {"n": 0}
    original_download_metadata = backend.download_metadata

    async def flaky_download_metadata(bucket: str, object_name: str) -> dict[str, Any]:
        calls["n"] += 1
        if calls["n"] <= 1:
            raise FakeGcsError(500, "internal error")
        result: dict[str, Any] = await original_download_metadata(bucket, object_name)
        return result

    monkeypatch.setattr(backend, "download_metadata", flaky_download_metadata)

    assert await store.load("k1") == "v1"


@pytest.mark.asyncio
async def test_commit_exhausts_retries_and_raises_underlying_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gcs_store_module, "_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MIN", 0.0)
    monkeypatch.setattr(gcs_store_module, "_WAIT_MAX", 0.0)
    backend = FakeGcsBackend()
    store, _, _ = make_store(backend)

    async def always_503(
        bucket: str, object_name: str, data: bytes, *, parameters: dict[str, str]
    ) -> dict[str, Any]:
        raise FakeGcsError(503, "service unavailable")

    monkeypatch.setattr(backend, "upload", always_503)

    with pytest.raises(FakeGcsError):
        await store.commit("k1", "v1")


@pytest.mark.asyncio
async def test_precondition_conflict_is_not_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(gcs_store_module, "_MAX_ATTEMPTS", 5)
    backend = FakeGcsBackend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    await store1.load("k1")
    await store2.load("k1")
    await store1.commit("k1", "from-1")

    with pytest.raises(StateStoreConflictError):
        await store2.commit("k1", "from-2")

    # store1's successful upload + store2's single failed upload — no
    # retries on the precondition conflict.
    assert backend.upload_calls == 2


@pytest.mark.asyncio
async def test_concurrent_commits_are_serialized() -> None:
    backend = FakeGcsBackend()
    backend.set_upload_delay(0.01)
    store, _, _ = make_store(backend)

    keys = [f"key-{i}" for i in range(10)]
    await asyncio.gather(*[store.commit(k, f"val-{k}") for k in keys])

    assert backend.max_concurrent_uploads == 1
    for k in keys:
        assert await store.load(k) == f"val-{k}"


@pytest.mark.asyncio
async def test_default_client_factory_used_when_none_injected() -> None:
    """Without gcloud-aio-storage installed, using the default factory fails at
    first use, not at import/construction time — the module itself must import
    cleanly."""
    cfg = GcsStateConfig(bucket="b", object_name="k")
    store = GcsCheckpointStore(cfg)
    with pytest.raises(ImportError):
        await store.load("k1")


def test_module_imports_without_gcloud_installed() -> None:
    with pytest.raises(ImportError):
        import gcloud.aio.storage  # noqa: F401

    # If we got this far the import genuinely failed in this environment, so the
    # fact that sf2loki.state.gcs_store imported cleanly above proves the lazy
    # boundary holds. Re-import to make sure it's still importable now.
    import importlib

    import sf2loki.state.gcs_store as gcs_store_module

    # Snapshot + restore so the reload doesn't leak new class identities into the
    # rest of the session (see the same guard in test_s3_store.py) — gcs_store
    # re-imports StateStoreConflictError from s3_store on reload, which would
    # otherwise mismatch a later pytest.raises in tests/test_statecmd.py.
    saved = dict(vars(gcs_store_module))
    try:
        importlib.reload(gcs_store_module)
    finally:
        vars(gcs_store_module).clear()
        vars(gcs_store_module).update(saved)


class _CallableFactory:
    def __init__(self, backend: FakeGcsBackend) -> None:
        self._backend = backend

    def make(self) -> Callable[[], AbstractAsyncContextManager[FakeGcsClient]]:
        client = FakeGcsClient(self._backend)

        @asynccontextmanager
        async def _factory():  # type: ignore[no-untyped-def]
            yield client

        return _factory


@pytest.mark.asyncio
async def test_client_factory_may_be_a_plain_async_context_manager_callable() -> None:
    backend = FakeGcsBackend()
    factory = _CallableFactory(backend).make()
    cfg = GcsStateConfig(bucket="test-bucket", object_name="sf2loki/state.json")
    store = GcsCheckpointStore(cfg, client_factory=factory)

    await store.commit("k1", "v1")
    assert await store.load("k1") == "v1"
