"""Google Cloud Storage checkpoint store.

The whole checkpoint document (one small JSON object) lives at a single
bucket/object; commits are conditional writes (GCS ``ifGenerationMatch``
preconditions — ``"0"`` for the first write, the current generation for an
update) so a second writer against the same object fails fast instead of
clobbering — the object-store analogue of the file store's flock. Requires
the ``sf2loki[gcs]`` extra (gcloud-aio-storage).

The module itself never imports ``gcloud.aio.storage`` at top level: the
default GCS client is built lazily on first use, inside
:meth:`GcsCheckpointStore._default_client_factory`. That keeps this module
importable — and unit-testable with an injected fake client — even when the
optional dependency is not installed; only actually *using* the default
factory without the extra installed raises ``ImportError``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

from sf2loki.state.s3_store import StateObjectCorruptError, StateStoreConflictError

if TYPE_CHECKING:
    from sf2loki.config import GcsStateConfig

GcsClientFactory = Callable[[], AbstractAsyncContextManager[Any]]

__all__ = [
    "GcsCheckpointStore",
    "GcsClientFactory",
    "StateObjectCorruptError",
    "StateStoreConflictError",
]


def _status_code(exc: Exception) -> int | None:
    """Extract the HTTP status from a gcloud-aio / aiohttp ClientResponseError-shaped error.

    Works for real ``aiohttp.ClientResponseError``-shaped exceptions and for
    lightweight test doubles alike — both expose a ``.status`` attribute —
    and duck-typing it here means this module never needs to import
    gcloud-aio-storage or aiohttp just to catch their exception types.
    """
    status = getattr(exc, "status", None)
    return status if isinstance(status, int) else None


_NOT_FOUND = 404
_PRECONDITION_FAILED = 412


class GcsCheckpointStore:
    """CheckpointStore backed by one GCS object with compare-and-swap commits.

    Multi-instance protection: commits are conditional uploads keyed off the
    object's generation (``ifGenerationMatch: "0"`` for the first write, the
    current generation for an update). A second sf2loki instance racing to
    commit the same object loses the compare-and-swap and gets
    :class:`StateStoreConflictError` instead of silently clobbering the first
    instance's checkpoints — the same fail-fast contract as the file store's
    flock, enforced by GCS's generation preconditions rather than an OS lock.

    ``client_factory`` (keyword-only) overrides how the GCS client is built —
    it must return an async context manager yielding a client shaped like
    gcloud-aio-storage's (``download``/``download_metadata``/``upload``
    coroutines), matching ``gcloud.aio.storage.Storage``. Tests inject a
    fake; production leaves it ``None`` and gets a real gcloud-aio-storage
    client built lazily on first use, requiring the ``sf2loki[gcs]`` extra.
    """

    def __init__(
        self,
        cfg: GcsStateConfig,
        *,
        client_factory: GcsClientFactory | None = None,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory
        self._client: Any | None = None
        self._client_cm: AbstractAsyncContextManager[Any] | None = None
        self._lock = asyncio.Lock()
        self._cache: dict[str, str] | None = None  # None = not yet loaded
        self._generation: str | None = None  # None + _cache loaded = object absent upstream
        self._fence: Callable[[], None] | None = None

    def set_fence(self, fence: Callable[[], None]) -> None:
        """Install a fence callback invoked (and allowed to raise) before every commit.

        Mirrors the file store's lease fence: an independent, cheaper-to-check
        protection layer ahead of the GCS conditional-write CAS. If the fence
        raises, the upload never happens.
        """
        self._fence = fence

    def _default_client_factory(self) -> AbstractAsyncContextManager[Any]:
        """Build the default gcloud-aio-storage client (lazy import; needs ``sf2loki[gcs]``)."""
        from gcloud.aio.storage import Storage  # type: ignore[import-not-found]

        kwargs: dict[str, Any] = {}
        if self._cfg.service_file is not None:
            kwargs["service_file"] = str(self._cfg.service_file)
        result: AbstractAsyncContextManager[Any] = Storage(**kwargs)
        return result

    async def _get_client(self) -> Any:
        if self._client is None:
            factory = self._client_factory or self._default_client_factory
            self._client_cm = factory()
            self._client = await self._client_cm.__aenter__()
        return self._client

    async def _ensure_loaded(self) -> None:
        """Load the JSON document into the in-memory cache if not already loaded."""
        if self._cache is not None:
            return
        client = await self._get_client()
        try:
            meta = await client.download_metadata(self._cfg.bucket, self._cfg.object_name)
        except Exception as exc:
            if _status_code(exc) == _NOT_FOUND:
                self._cache = {}
                self._generation = None
                return
            raise
        self._generation = str(meta["generation"])
        body = await client.download(self._cfg.bucket, self._cfg.object_name)
        data = json.loads(body)
        if not isinstance(data, dict):
            raise StateObjectCorruptError(
                f"state object gs://{self._cfg.bucket}/{self._cfg.object_name} must contain a "
                f"JSON object, got {type(data).__name__}."
            )
        self._cache = data

    async def load(self, key: str) -> str | None:
        async with self._lock:
            await self._ensure_loaded()
            assert self._cache is not None
            return self._cache.get(key)

    async def commit(self, key: str, value: str) -> None:
        async with self._lock:
            if self._fence is not None:
                self._fence()
            await self._ensure_loaded()
            assert self._cache is not None
            new_cache = {**self._cache, key: value}
            client = await self._get_client()
            body = json.dumps(new_cache).encode("utf-8")
            precondition = (
                {"ifGenerationMatch": "0"}
                if self._generation is None
                else {"ifGenerationMatch": self._generation}
            )
            try:
                resp = await client.upload(
                    self._cfg.bucket,
                    self._cfg.object_name,
                    body,
                    parameters=precondition,
                )
            except Exception as exc:
                if _status_code(exc) == _PRECONDITION_FAILED:
                    raise StateStoreConflictError(
                        f"commit to gs://{self._cfg.bucket}/{self._cfg.object_name} lost a "
                        "compare-and-swap race — another sf2loki instance is writing "
                        "the same state object. Two instances sharing GCS checkpoint "
                        "state would double-ingest and clobber each other's "
                        "checkpoints; point each instance at its own object."
                    ) from exc
                raise
            self._cache = new_cache
            self._generation = str(resp["generation"])

    async def close(self) -> None:
        if self._client_cm is not None:
            cm, self._client_cm = self._client_cm, None
            self._client = None
            await cm.__aexit__(None, None, None)
