"""S3-compatible object-storage checkpoint store.

The whole checkpoint document (one small JSON object) lives at a single
bucket/key; commits are conditional writes (ETag ``If-Match``, or
``If-None-Match: *`` for the first write) so a second writer against the same
key fails fast instead of clobbering — the object-store analogue of the file
store's flock. Requires the ``sf2loki[s3]`` extra (aiobotocore).

The module itself never imports ``aiobotocore`` at top level: the default S3
client is built lazily on first use, inside
:meth:`S3CheckpointStore._default_client_factory`. That keeps this module
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

if TYPE_CHECKING:
    from sf2loki.config import S3StateConfig

ClientFactory = Callable[[], AbstractAsyncContextManager[Any]]


class StateStoreConflictError(RuntimeError):
    """A conditional S3 write lost a compare-and-swap race.

    Raised when a PUT's ``If-Match``/``If-None-Match`` precondition fails
    (HTTP 412 / ``PreconditionFailed``): another sf2loki instance is writing
    the same checkpoint object concurrently. This is the object-store
    analogue of the file store's flock — fail fast rather than silently
    clobbering the other writer's checkpoints.
    """


class StateObjectCorruptError(RuntimeError):
    """The checkpoint object exists but does not contain a valid JSON object."""


def _error_code(exc: Exception) -> str | None:
    """Extract the botocore-style error code from a ``ClientError``-shaped exception.

    Works for real ``botocore.exceptions.ClientError`` instances and for
    lightweight test doubles alike — both expose a ``.response`` dict with an
    ``Error.Code`` entry, and duck-typing it here means this module never
    needs to import botocore just to catch its exception type.
    """
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    error = response.get("Error")
    if isinstance(error, dict):
        code = error.get("Code")
        if isinstance(code, str):
            return code
    return None


def _status_code(exc: Exception) -> int | None:
    """Extract the HTTP status code from a ``ClientError``-shaped exception."""
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return None
    meta = response.get("ResponseMetadata")
    if isinstance(meta, dict):
        status = meta.get("HTTPStatusCode")
        if isinstance(status, int):
            return status
    return None


_NOT_FOUND_CODES = frozenset({"NoSuchKey", "404"})
_PRECONDITION_FAILED_CODES = frozenset({"PreconditionFailed", "412"})


class S3CheckpointStore:
    """CheckpointStore backed by one S3 object with compare-and-swap commits.

    Multi-instance protection: commits are conditional PUTs keyed off the
    object's ETag (``If-Match`` for an update, ``If-None-Match: *`` for the
    first write). A second sf2loki instance racing to commit the same object
    loses the compare-and-swap and gets :class:`StateStoreConflictError`
    instead of silently clobbering the first instance's checkpoints — the
    same fail-fast contract as the file store's flock, enforced by the
    object store's conditional-write support rather than an OS lock.

    ``client_factory`` (keyword-only) overrides how the S3 client is built —
    it must return an async context manager yielding a client shaped like
    aiobotocore's (``get_object``/``put_object`` coroutines), matching
    ``session.create_client("s3", ...)``. Tests inject a fake; production
    leaves it ``None`` and gets a real aiobotocore client built lazily on
    first use, requiring the ``sf2loki[s3]`` extra.
    """

    def __init__(
        self,
        cfg: S3StateConfig,
        *,
        client_factory: ClientFactory | None = None,
    ) -> None:
        self._cfg = cfg
        self._client_factory = client_factory
        self._client: Any | None = None
        self._client_cm: AbstractAsyncContextManager[Any] | None = None
        self._lock = asyncio.Lock()
        self._cache: dict[str, str] | None = None  # None = not yet loaded
        self._etag: str | None = None  # None + _cache loaded = object absent upstream
        self._fence: Callable[[], None] | None = None

    def set_fence(self, fence: Callable[[], None]) -> None:
        """Install a fence callback invoked (and allowed to raise) before every commit.

        Mirrors the file store's lease fence: an independent, cheaper-to-check
        protection layer ahead of the S3 conditional-write CAS. If the fence
        raises, the PUT never happens.
        """
        self._fence = fence

    def _default_client_factory(self) -> AbstractAsyncContextManager[Any]:
        """Build the default aiobotocore S3 client (lazy import; needs ``sf2loki[s3]``)."""
        from aiobotocore.session import get_session  # type: ignore[import-not-found]

        session = get_session()
        kwargs: dict[str, Any] = {}
        if self._cfg.region:
            kwargs["region_name"] = self._cfg.region
        if self._cfg.endpoint_url:
            kwargs["endpoint_url"] = self._cfg.endpoint_url
        result: AbstractAsyncContextManager[Any] = session.create_client("s3", **kwargs)
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
            resp = await client.get_object(Bucket=self._cfg.bucket, Key=self._cfg.key)
        except Exception as exc:
            if _error_code(exc) in _NOT_FOUND_CODES:
                self._cache = {}
                self._etag = None
                return
            raise
        body = await resp["Body"].read()
        data = json.loads(body)
        if not isinstance(data, dict):
            raise StateObjectCorruptError(
                f"state object s3://{self._cfg.bucket}/{self._cfg.key} must contain a "
                f"JSON object, got {type(data).__name__}."
            )
        self._cache = data
        self._etag = resp.get("ETag")

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
            put_kwargs: dict[str, Any] = {
                "Bucket": self._cfg.bucket,
                "Key": self._cfg.key,
                "Body": body,
            }
            if self._etag is None:
                put_kwargs["IfNoneMatch"] = "*"
            else:
                put_kwargs["IfMatch"] = self._etag
            try:
                resp = await client.put_object(**put_kwargs)
            except Exception as exc:
                code = _error_code(exc)
                status = _status_code(exc)
                if code in _PRECONDITION_FAILED_CODES or status == 412:
                    raise StateStoreConflictError(
                        f"commit to s3://{self._cfg.bucket}/{self._cfg.key} lost a "
                        "compare-and-swap race — another sf2loki instance is writing "
                        "the same state object. Two instances sharing S3 checkpoint "
                        "state would double-ingest and clobber each other's "
                        "checkpoints; point each instance at its own key."
                    ) from exc
                raise
            self._cache = new_cache
            self._etag = resp.get("ETag")

    async def close(self) -> None:
        if self._client_cm is not None:
            cm, self._client_cm = self._client_cm, None
            self._client = None
            await cm.__aexit__(None, None, None)
