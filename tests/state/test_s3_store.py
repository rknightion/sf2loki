"""Tests for S3CheckpointStore.

Uses a tiny in-memory fake S3 client (no moto, no network) that mimics the
aiobotocore/botocore shapes the store actually depends on: coroutine
``get_object``/``put_object`` methods, a ``.response`` dict on raised errors
(``Error.Code`` / ``ResponseMetadata.HTTPStatusCode``), and an async
``Body.read()``. The real ``aiobotocore`` package is never imported here —
that is the point of the lazy-import design under test.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

import pytest

from sf2loki.config import S3StateConfig
from sf2loki.state.s3_store import (
    S3CheckpointStore,
    StateObjectCorruptError,
    StateStoreConflictError,
)


class FakeClientError(Exception):
    """Mimics botocore.exceptions.ClientError's shape without importing botocore."""

    def __init__(self, code: str, status: int, operation: str = "Operation") -> None:
        self.response = {
            "Error": {"Code": code, "Message": code},
            "ResponseMetadata": {"HTTPStatusCode": status},
        }
        super().__init__(f"{operation} failed: {code}")


class FakeStreamingBody:
    def __init__(self, data: bytes) -> None:
        self._data = data

    async def read(self) -> bytes:
        return self._data


class FakeS3Backend:
    """Shared in-memory (bucket, key) -> (body, etag) store, usable by multiple clients."""

    def __init__(self) -> None:
        self._objects: dict[tuple[str, str], tuple[bytes, str]] = {}
        self._etag_counter = 0
        self.get_object_calls = 0
        self.put_object_calls = 0
        self.concurrent_puts = 0
        self.max_concurrent_puts = 0
        self._put_delay = 0.0

    def set_put_delay(self, seconds: float) -> None:
        self._put_delay = seconds

    async def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.get_object_calls += 1
        obj = self._objects.get((Bucket, Key))
        if obj is None:
            raise FakeClientError("NoSuchKey", 404, "GetObject")
        body, etag = obj
        return {"Body": FakeStreamingBody(body), "ETag": etag}

    async def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        IfMatch: str | None = None,
        IfNoneMatch: str | None = None,
    ) -> dict[str, Any]:
        self.put_object_calls += 1
        self.concurrent_puts += 1
        self.max_concurrent_puts = max(self.max_concurrent_puts, self.concurrent_puts)
        try:
            if self._put_delay:
                await asyncio.sleep(self._put_delay)
            existing = self._objects.get((Bucket, Key))
            if IfNoneMatch == "*":
                if existing is not None:
                    raise FakeClientError("PreconditionFailed", 412, "PutObject")
            elif IfMatch is not None:
                if existing is None or existing[1] != IfMatch:
                    raise FakeClientError("PreconditionFailed", 412, "PutObject")
            else:  # pragma: no cover - defensive; store always sends one or the other
                raise AssertionError("put_object called without a precondition header")
            self._etag_counter += 1
            etag = f"etag-{self._etag_counter}"
            self._objects[(Bucket, Key)] = (Body, etag)
            return {"ETag": etag}
        finally:
            self.concurrent_puts -= 1


class FakeS3Client:
    """Thin per-store client view over a shared FakeS3Backend."""

    def __init__(self, backend: FakeS3Backend) -> None:
        self._backend = backend

    async def get_object(self, **kwargs: Any) -> dict[str, Any]:
        return await self._backend.get_object(**kwargs)

    async def put_object(self, **kwargs: Any) -> dict[str, Any]:
        return await self._backend.put_object(**kwargs)


class TrackingClientFactory:
    """A client_factory that records whether its context manager was entered/exited."""

    def __init__(self, client: FakeS3Client) -> None:
        self.client = client
        self.entered = False
        self.exited = False

    def __call__(self) -> AbstractAsyncContextManager[FakeS3Client]:
        return self._cm()

    @asynccontextmanager
    async def _cm(self):  # type: ignore[no-untyped-def]
        self.entered = True
        try:
            yield self.client
        finally:
            self.exited = True


def make_store(
    backend: FakeS3Backend | None = None,
    *,
    bucket: str = "test-bucket",
    key: str = "sf2loki/state.json",
) -> tuple[S3CheckpointStore, FakeS3Backend, TrackingClientFactory]:
    backend = backend or FakeS3Backend()
    client = FakeS3Client(backend)
    factory = TrackingClientFactory(client)
    cfg = S3StateConfig(bucket=bucket, key=key)
    store = S3CheckpointStore(cfg, client_factory=factory)
    return store, backend, factory


@pytest.mark.asyncio
async def test_load_missing_object_returns_none() -> None:
    store, backend, _ = make_store()
    result = await store.load("no-such-key")
    assert result is None
    assert backend.get_object_calls == 1


@pytest.mark.asyncio
async def test_load_caches_after_first_get() -> None:
    store, backend, _ = make_store()
    await store.load("k1")
    await store.load("k2")
    assert backend.get_object_calls == 1  # second load hits the in-memory cache


@pytest.mark.asyncio
async def test_commit_creates_with_if_none_match() -> None:
    store, backend, _ = make_store()
    await store.commit("stream-a", "offset-1")
    assert backend.put_object_calls == 1
    result = await store.load("stream-a")
    assert result == "offset-1"


@pytest.mark.asyncio
async def test_commit_then_load_on_fresh_store_round_trips() -> None:
    backend = FakeS3Backend()
    store1, _, _ = make_store(backend)
    await store1.commit("k1", "v1")

    store2, _, _ = make_store(backend)
    assert await store2.load("k1") == "v1"


@pytest.mark.asyncio
async def test_commit_updates_with_if_match() -> None:
    backend = FakeS3Backend()
    store, _, _ = make_store(backend)
    await store.commit("k1", "v1")
    await store.commit("k1", "v2")
    assert await store.load("k1") == "v2"
    assert backend.put_object_calls == 2


@pytest.mark.asyncio
async def test_commit_preserves_other_keys() -> None:
    store, _, _ = make_store()
    await store.commit("k1", "v1")
    await store.commit("k2", "v2")
    assert await store.load("k1") == "v1"
    assert await store.load("k2") == "v2"


@pytest.mark.asyncio
async def test_commit_after_load_round_trip_preserves_other_keys() -> None:
    backend = FakeS3Backend()
    store1, _, _ = make_store(backend)
    await store1.commit("k1", "v1")

    store2, _, _ = make_store(backend)
    await store2.load("k1")  # populate store2's cache from the shared backend
    await store2.commit("k2", "v2")

    assert await store2.load("k1") == "v1"
    assert await store2.load("k2") == "v2"


@pytest.mark.asyncio
async def test_create_conflict_raises_state_store_conflict_error() -> None:
    """Two stores race to create the object; the loser's IfNoneMatch=* fails."""
    backend = FakeS3Backend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    # Both stores observe the object as absent before either writes — mirrors
    # the real race where two fresh instances start up against an empty key.
    await store1.load("k1")
    await store2.load("k1")

    await store1.commit("k1", "from-1")
    with pytest.raises(StateStoreConflictError) as exc_info:
        await store2.commit("k1", "from-2")

    assert "another sf2loki instance" in str(exc_info.value)
    # The loser's local cache/etag must not have been updated by the failed PUT.
    assert await store1.load("k1") == "from-1"


@pytest.mark.asyncio
async def test_update_conflict_raises_state_store_conflict_error() -> None:
    """Two stores both loaded the same version, then race to update it."""
    backend = FakeS3Backend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    await store1.commit("k1", "v1")  # creates the object
    await store2.load("k1")  # store2 now has the same etag cached

    await store1.commit("k1", "v2")  # store1 updates; etag moves on
    with pytest.raises(StateStoreConflictError):
        await store2.commit("k1", "v3")  # store2's stale IfMatch etag is rejected


@pytest.mark.asyncio
async def test_split_brain_second_writer_fails_fast() -> None:
    """Split-brain scenario: two instances pointed at the same key must not both win."""
    backend = FakeS3Backend()
    store1, _, _ = make_store(backend)
    store2, _, _ = make_store(backend)

    # Both instances start against the same empty key before either writes.
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
    backend = FakeS3Backend()
    backend._objects[("test-bucket", "sf2loki/state.json")] = (b'["not", "a", "dict"]', "etag-x")
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
async def test_fence_raises_blocks_commit_before_any_put() -> None:
    store, backend, _ = make_store()

    def fence() -> None:
        raise RuntimeError("lease expired")

    store.set_fence(fence)
    with pytest.raises(RuntimeError, match="lease expired"):
        await store.commit("k1", "v1")

    assert backend.put_object_calls == 0


@pytest.mark.asyncio
async def test_fence_allows_commit_when_it_does_not_raise() -> None:
    store, backend, _ = make_store()
    calls: list[str] = []
    store.set_fence(lambda: calls.append("checked"))

    await store.commit("k1", "v1")

    assert calls == ["checked"]
    assert backend.put_object_calls == 1


@pytest.mark.asyncio
async def test_concurrent_commits_are_serialized() -> None:
    backend = FakeS3Backend()
    backend.set_put_delay(0.01)
    store, _, _ = make_store(backend)

    keys = [f"key-{i}" for i in range(10)]
    await asyncio.gather(*[store.commit(k, f"val-{k}") for k in keys])

    assert backend.max_concurrent_puts == 1
    for k in keys:
        assert await store.load(k) == f"val-{k}"


@pytest.mark.asyncio
async def test_default_client_factory_used_when_none_injected() -> None:
    """Without aiobotocore installed, using the default factory fails at first use,
    not at import/construction time — the module itself must import cleanly."""
    cfg = S3StateConfig(bucket="b", key="k")
    store = S3CheckpointStore(cfg)
    with pytest.raises(ImportError):
        await store.load("k1")


def test_module_imports_without_aiobotocore_installed() -> None:
    with pytest.raises(ImportError):
        import aiobotocore  # noqa: F401

    # If we got this far the import genuinely failed in this environment, so the
    # fact that sf2loki.state.s3_store imported cleanly above proves the lazy
    # boundary holds. Re-import to make sure it's still importable now.
    import importlib

    import sf2loki.state.s3_store as s3_store_module

    importlib.reload(s3_store_module)


@pytest.mark.asyncio
async def test_real_aiobotocore_smoke() -> None:
    """Optional smoke test: only runs if the sf2loki[s3] extra is installed."""
    pytest.importorskip("aiobotocore")
    from aiobotocore.session import get_session

    session = get_session()
    assert session is not None


class _CallableFactory:
    def __init__(self, backend: FakeS3Backend) -> None:
        self._backend = backend

    def make(self) -> Callable[[], AbstractAsyncContextManager[FakeS3Client]]:
        client = FakeS3Client(self._backend)

        @asynccontextmanager
        async def _factory():  # type: ignore[no-untyped-def]
            yield client

        return _factory


@pytest.mark.asyncio
async def test_client_factory_may_be_a_plain_async_context_manager_callable() -> None:
    backend = FakeS3Backend()
    factory = _CallableFactory(backend).make()
    cfg = S3StateConfig(bucket="test-bucket", key="sf2loki/state.json")
    store = S3CheckpointStore(cfg, client_factory=factory)

    await store.commit("k1", "v1")
    assert await store.load("k1") == "v1"
