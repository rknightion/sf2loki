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
from collections.abc import Callable, Mapping
from contextlib import AbstractAsyncContextManager
from typing import TYPE_CHECKING, Any

import tenacity

if TYPE_CHECKING:
    from sf2loki.config import S3StateConfig

ClientFactory = Callable[[], AbstractAsyncContextManager[Any]]

# ---------------------------------------------------------------------------
# Module-level retry knobs — monkeypatched in tests to keep them fast.
# Mirrors sinks/loki/sink.py's discipline: bounded, jittered retry around
# transient object-store errors (503 SlowDown, InternalError, TCP reset,
# ...). The precondition-conflict (412) CAS failure is classified separately
# below and is never retried — it means another writer won the race, not a
# transient blip, and must fail fast (see StateStoreConflictError).
# ---------------------------------------------------------------------------
_MAX_ATTEMPTS: int = 4
_WAIT_MIN: float = 0.1
_WAIT_MAX: float = 2.0


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

# Botocore error codes that mean "retry me" — throttling/overload/internal
# blips, never a data or auth problem.
_TRANSIENT_CODES = frozenset(
    {
        "SlowDown",
        "InternalError",
        "InternalFailure",
        "ServiceUnavailable",
        "RequestTimeout",
        "RequestTimeTooSkewed",
        "Throttling",
        "ThrottlingException",
        "RequestLimitExceeded",
    }
)


def _is_transient(exc: BaseException) -> bool:
    """True for a transient object-store error that is safe to retry.

    Never true for the precondition-conflict CAS failure (already converted
    to :class:`StateStoreConflictError` by the time this predicate sees it,
    which has neither a botocore error code nor an HTTP status) — that one
    must fail fast, not retry.
    """
    if not isinstance(exc, Exception):
        return False
    if _error_code(exc) in _TRANSIENT_CODES:
        return True
    status = _status_code(exc)
    if status is not None and status >= 500:
        return True
    # TCP resets / connection drops surface as bare OSError-family exceptions
    # with no botocore response shape at all.
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

    def reset(self) -> None:
        """Invalidate the in-memory cache and cached ETag (issue #48).

        Called on leadership loss so a later re-acquisition re-fetches the
        object and its current ETag instead of committing against a stale
        ETag — which would otherwise 412 (StateStoreConflictError) on the
        first commit after re-promotion.
        """
        self._cache = None
        self._etag = None

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

        async def _do_get() -> dict[str, Any] | None:
            try:
                result: dict[str, Any] = await client.get_object(
                    Bucket=self._cfg.bucket, Key=self._cfg.key
                )
                return result
            except Exception as exc:
                if _error_code(exc) in _NOT_FOUND_CODES:
                    return None
                raise

        resp = await _retry_transient(_do_get)
        if resp is None:
            self._cache = {}
            self._etag = None
            return
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
        await self.commit_many({key: value})

    async def commit_many(self, items: Mapping[str, str]) -> None:
        """Merge *items* into the state document with exactly one conditional PUT.

        Replaces N per-key commits (N loads + N full-object PUTs) with one
        load + one PUT for the whole batch — see issue #54.
        """
        async with self._lock:
            if self._fence is not None:
                self._fence()
            await self._ensure_loaded()
            assert self._cache is not None
            new_cache = {**self._cache, **items}
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

            async def _do_put() -> dict[str, Any]:
                try:
                    result: dict[str, Any] = await client.put_object(**put_kwargs)
                    return result
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

            resp = await _retry_transient(_do_put)
            self._cache = new_cache
            self._etag = resp.get("ETag")

    async def close(self) -> None:
        if self._client_cm is not None:
            cm, self._client_cm = self._client_cm, None
            self._client = None
            await cm.__aexit__(None, None, None)
