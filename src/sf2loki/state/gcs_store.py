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
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

import tenacity

from sf2loki.state.s3_store import StateObjectCorruptError, StateStoreConflictError

if TYPE_CHECKING:
    from sf2loki.config import GcsStateConfig

GcsClientFactory = Callable[[], AbstractAsyncContextManager[Any]]

# ---------------------------------------------------------------------------
# Module-level retry knobs — monkeypatched in tests to keep them fast.
# Mirrors sinks/loki/sink.py's discipline: bounded, jittered retry around
# transient object-store errors (5xx, TCP reset, ...). The precondition-
# conflict (412) CAS failure is classified separately below and is never
# retried — it means another writer won the race, not a transient blip.
# ---------------------------------------------------------------------------
_MAX_ATTEMPTS: int = 4
_WAIT_MIN: float = 0.1
_WAIT_MAX: float = 2.0

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


def _is_transient(exc: BaseException) -> bool:
    """True for a transient GCS error that is safe to retry.

    Never true for the precondition-conflict CAS failure (already converted
    to :class:`StateStoreConflictError` by the time this predicate sees it,
    which has no ``.status`` attribute) — that one must fail fast, not retry.
    """
    if not isinstance(exc, Exception):
        return False
    status = _status_code(exc)
    if status is not None and status >= 500:
        return True
    # TCP resets / connection drops surface as bare OSError-family exceptions
    # with no aiohttp response shape at all.
    if isinstance(exc, TimeoutError | ConnectionError | OSError):
        return True
    return False


async def _retry_transient(attempt: Callable[[], Any]) -> Any:
    """Run *attempt* under a bounded, jittered retry for transient errors only.

    Any non-transient exception (including StateStoreConflictError) is
    re-raised immediately on the first failure — reraise=True means tenacity
    surfaces the original exception, not a wrapped RetryError.
    """
    retrying: tenacity.AsyncRetrying = tenacity.AsyncRetrying(
        reraise=True,
        stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
        wait=tenacity.wait_random_exponential(min=_WAIT_MIN, max=_WAIT_MAX),
        retry=tenacity.retry_if_exception(_is_transient),
    )
    return await retrying(attempt)


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

    def reset(self) -> None:
        """Invalidate the in-memory cache and cached generation (issue #48).

        Called on leadership loss so a later re-acquisition re-fetches the
        object and its current generation instead of committing against a
        stale generation — which would otherwise 412
        (StateStoreConflictError) on the first commit after re-promotion.
        """
        self._cache = None
        self._generation = None

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

        async def _do_download_metadata() -> dict[str, Any] | None:
            try:
                result: dict[str, Any] = await client.download_metadata(
                    self._cfg.bucket, self._cfg.object_name
                )
                return result
            except Exception as exc:
                if _status_code(exc) == _NOT_FOUND:
                    return None
                raise

        meta = await _retry_transient(_do_download_metadata)
        if meta is None:
            self._cache = {}
            self._generation = None
            return
        self._generation = str(meta["generation"])

        async def _do_download() -> bytes:
            result: bytes = await client.download(self._cfg.bucket, self._cfg.object_name)
            return result

        body = await _retry_transient(_do_download)
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
        await self.commit_many({key: value})

    async def commit_many(self, items: Mapping[str, str]) -> None:
        """Merge *items* into the state document with exactly one conditional upload.

        Replaces N per-key commits (N downloads + N full-object uploads) with
        one download + one upload for the whole batch — see issue #54.
        """
        async with self._lock:
            if self._fence is not None:
                self._fence()
            await self._ensure_loaded()
            assert self._cache is not None
            new_cache = {**self._cache, **items}
            client = await self._get_client()
            body = json.dumps(new_cache).encode("utf-8")
            precondition = (
                {"ifGenerationMatch": "0"}
                if self._generation is None
                else {"ifGenerationMatch": self._generation}
            )

            async def _do_upload() -> dict[str, Any]:
                try:
                    result: dict[str, Any] = await client.upload(
                        self._cfg.bucket,
                        self._cfg.object_name,
                        body,
                        parameters=precondition,
                    )
                    return result
                except Exception as exc:
                    if _status_code(exc) == _PRECONDITION_FAILED:
                        raise StateStoreConflictError(
                            f"commit to gs://{self._cfg.bucket}/{self._cfg.object_name} lost "
                            "a compare-and-swap race — another sf2loki instance is writing "
                            "the same state object. Two instances sharing GCS checkpoint "
                            "state would double-ingest and clobber each other's "
                            "checkpoints; point each instance at its own object."
                        ) from exc
                    raise

            resp = await _retry_transient(_do_upload)
            self._cache = new_cache
            self._generation = str(resp["generation"])

    async def close(self) -> None:
        if self._client_cm is not None:
            cm, self._client_cm = self._client_cm, None
            self._client = None
            await cm.__aexit__(None, None, None)
