"""LokiSink: HTTP push sink for Grafana Loki.

Satisfies the Sink protocol (push / aclose). Handles auth, encoding/compression,
bounded retries with tenacity (honouring Retry-After), and 400/413 payload
splitting. Auth/config rejections (401/403/404) are retryable — the data is
fine, so they must never be treated as poison.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
from typing import Any

import httpx
import tenacity

from sf2loki.config import LokiConfig
from sf2loki.model import Batch
from sf2loki.obs.logging import get_logger
from sf2loki.obs.metrics import Metrics
from sf2loki.shaping import cap_line
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.labels import guard_static_labels
from sf2loki.sinks.loki.push import encode_json, encode_protobuf

log = get_logger(__name__)

# ---------------------------------------------------------------------------
# Module-level retry knobs — monkeypatched in tests to keep them fast.
# ---------------------------------------------------------------------------

_MAX_ATTEMPTS: int = 5
_WAIT_MIN: float = 0.5  # seconds
_WAIT_MAX: float = 30.0  # seconds

# Longest we will honour a server-sent Retry-After for (seconds).
_RETRY_AFTER_CAP: float = 60.0

# Statuses that mean broken auth/config (rotated token, wrong tenant/URL): the
# data is fine, so these are retryable — the pipeline holds the batch and its
# checkpoints and retries with capped backoff until an operator fixes it.
_AUTH_CONFIG_STATUSES: frozenset[int] = frozenset({401, 403, 404})

# Log the auth/config ERROR on the 1st consecutive failure and every Nth after
# (with the pipeline's 30s backoff cap that is roughly one ERROR per 5 minutes).
_AUTH_LOG_EVERY: int = 10

# Below this (approximate, pre-encode) batch size, encode inline on the event
# loop; at/above it, hop to a worker thread. protobuf SerializeToString +
# cramjam snappy and JSON dumps + gzip are ~10-15 ms of blocking CPU per 1 MiB
# (both cramjam and the protobuf C extension release the GIL, so
# asyncio.to_thread reclaims that cleanly) — but the thread handoff itself
# has a small fixed cost, so a threshold keeps tiny batches (the common case
# for low-volume orgs) on the cheap inline path. 64 KiB is comfortably above
# "a handful of log lines" and comfortably below where encode time becomes
# noticeable to producers/the gRPC receive loop/the health server sharing
# this event loop.
_ENCODE_OFFLOAD_THRESHOLD_BYTES: int = 65_536


def _parse_retry_after(value: str | None) -> float | None:
    """Parse a Retry-After header in delay-seconds form.

    Returns the (non-negative) delay, or ``None`` for a missing header or the
    HTTP-date form — callers then fall back to normal backoff.
    """
    if value is None:
        return None
    try:
        seconds = float(value.strip())
    except ValueError:
        return None
    return max(0.0, seconds)


def _compute_wait(retry_state: tenacity.RetryCallState) -> float:
    """Tenacity wait: exponential backoff, raised to any server Retry-After.

    Waits at least the Retry-After carried by the failed attempt's
    :class:`_TransientError` (capped at :data:`_RETRY_AFTER_CAP`), so a 429/503
    from Grafana Cloud is not hammered ahead of the server's own schedule.
    """
    base = float(tenacity.wait_random_exponential(min=_WAIT_MIN, max=_WAIT_MAX)(retry_state))
    exc = retry_state.outcome.exception() if retry_state.outcome is not None else None
    retry_after: float | None = getattr(exc, "retry_after", None)
    if retry_after is None:
        return base
    return min(max(base, retry_after), _RETRY_AFTER_CAP)


class LokiSink:
    """Push Batch objects to a Loki HTTP endpoint.

    Implements the Sink protocol: ``async push(batch) -> None`` and
    ``async aclose() -> None``.
    """

    def __init__(
        self,
        cfg: LokiConfig,
        client: httpx.AsyncClient,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        # Fail fast on disallowed/reserved static label keys (config error).
        guard_static_labels(cfg.labels)

        self._cfg = cfg
        self._client = client
        self._headers = self._build_headers()
        self._metrics = metrics if metrics is not None else Metrics()
        self._consecutive_auth_failures = 0

    # ------------------------------------------------------------------
    # Header construction
    # ------------------------------------------------------------------

    def _build_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}

        # Auth: Grafana Cloud (auth_token set) > self-hosted (tenant_id only) > none
        if self._cfg.auth_token is not None:
            user = self._cfg.tenant_id or ""
            pw = self._cfg.auth_token.get_secret_value()
            cred = base64.b64encode(f"{user}:{pw}".encode()).decode()
            headers["Authorization"] = f"Basic {cred}"
        elif self._cfg.tenant_id is not None:
            headers["X-Scope-OrgID"] = self._cfg.tenant_id

        # Content headers depend on encoding, set per-request in _encode()
        return headers

    # ------------------------------------------------------------------
    # Encoding
    # ------------------------------------------------------------------

    def _encode(self, batch: Batch) -> tuple[bytes, dict[str, str]]:
        """Return (body, content_headers) for the given batch."""
        content_headers: dict[str, str] = {}

        if self._cfg.encoding == "protobuf":
            # encode_protobuf already snappy-block-compresses the payload.
            body = encode_protobuf(batch)
            content_headers["Content-Type"] = "application/x-protobuf"
            content_headers["Content-Encoding"] = "snappy"
        else:
            # JSON: optionally gzip.
            raw = encode_json(batch)
            content_headers["Content-Type"] = "application/json"
            if self._cfg.compression == "gzip":
                body = gzip.compress(raw)
                content_headers["Content-Encoding"] = "gzip"
            else:
                body = raw

        return body, content_headers

    @staticmethod
    def _encode_size_estimate(batch: Batch) -> int:
        """Cheap pre-encode size signal (sum of UTF-8 line bytes).

        Not the final wire size (labels, structured metadata, and protobuf/gzip
        framing all add overhead) — just enough to decide, before paying the
        encode cost, whether this batch is worth offloading to a thread.

        Uses the memoized per-entry line length (issue #69) so this reuses the
        UTF-8 encode already paid during pipeline byte accounting.
        """
        return sum(entry.line_nbytes() for entry in batch.entries)

    async def _encode_async(self, batch: Batch) -> tuple[bytes, dict[str, str]]:
        """Encode *batch*, offloading to a worker thread above a size threshold.

        Small batches (the common case) encode inline — the thread hop itself
        would cost more than it saves. Large batches offload via
        ``asyncio.to_thread`` so the CPU-bound protobuf/snappy or JSON/gzip
        work doesn't block the event loop (producers, the gRPC receive loop,
        the health server). Output is byte-identical either way — only where
        the work runs changes.
        """
        if self._encode_size_estimate(batch) >= _ENCODE_OFFLOAD_THRESHOLD_BYTES:
            return await asyncio.to_thread(self._encode, batch)
        return self._encode(batch)

    # ------------------------------------------------------------------
    # Internal POST with tenacity retry (retryable status codes only)
    # ------------------------------------------------------------------

    async def _post(self, body: bytes, content_headers: dict[str, str]) -> int:
        """POST *body* to the Loki push URL.

        Returns the HTTP status code for the caller to inspect (non-retryable
        codes).  Raises :class:`RetryableSinkError` when the retry budget is
        exhausted on transient errors (429 / 5xx / transport failure).
        """

        # Build the tenacity retry decorator with current module-level knobs so
        # that tests can monkeypatch _MAX_ATTEMPTS / _WAIT_MIN / _WAIT_MAX.
        async def _attempt() -> int:
            try:
                resp = await self._client.post(
                    self._cfg.url,
                    content=body,
                    headers={**self._headers, **content_headers},
                )
            except httpx.TransportError as exc:
                raise _TransientError(str(exc)) from exc

            status = resp.status_code
            if status in (429,) or (500 <= status < 600):
                raise _TransientError(
                    f"HTTP {status}",
                    retry_after=_parse_retry_after(resp.headers.get("Retry-After")),
                )
            return status

        retry = tenacity.AsyncRetrying(
            reraise=False,
            stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
            wait=_compute_wait,
            retry=tenacity.retry_if_exception_type(_TransientError),
        )

        try:
            result: int = await retry(_attempt)
            return result
        except tenacity.RetryError as exc:
            # Extract the underlying cause for a meaningful message.
            last: BaseException = exc.last_attempt.exception() or exc
            raise RetryableSinkError(f"Loki push failed after retries: {last}") from last

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def push(self, batch: Batch) -> None:
        """Encode *batch* and POST it to Loki, with retries and 400/413 splitting."""
        if not batch.entries:
            return

        self._cap_lines(batch)

        body, content_headers = await self._encode_async(batch)
        status = await self._post(body, content_headers)

        if 200 <= status < 300:
            self._metrics.loki_bytes_pushed.inc(len(body))
            self._note_auth_ok()
            return

        if status in _AUTH_CONFIG_STATUSES:
            # Rotated token / wrong tenant or URL. The data is fine — surface
            # loudly and let the pipeline retry forever with capped backoff;
            # checkpoints are held, so nothing is lost while an operator fixes it.
            self._note_auth_failure(status)
            raise RetryableSinkError(
                f"Loki rejected push with HTTP {status} (auth/config error); "
                "retrying — no data dropped"
            )

        if status in (400, 413):
            reason = "oversized_413" if status == 413 else "bad_request"
            if len(batch.entries) == 1:
                raise PermanentSinkError(
                    f"unsplittable {status}: single-entry batch rejected", reason=reason
                )
            # Split and recurse — re-encode each half independently. A permanent
            # failure in one half (e.g. one entry that is rejected even when
            # alone) must NOT discard the other half: drop+count just the poison
            # and keep delivering the rest. Only a top-level single-entry batch
            # propagates PermanentSinkError (no parent to absorb it).
            mid = len(batch.entries) // 2
            for half in (Batch(entries=batch.entries[:mid]), Batch(entries=batch.entries[mid:])):
                try:
                    await self.push(half)
                except PermanentSinkError as exc:
                    self._metrics.loki_entries_dropped.labels(reason=exc.reason).inc(
                        len(half.entries)
                    )
                    log.warning(
                        "dropping undeliverable Loki entries (permanent error during split)",
                        entries=len(half.entries),
                        reason=exc.reason,
                        error=str(exc),
                    )
            return

        # Any other status is unexpected: retry rather than silently dropping
        # data — permanent drops are reserved for statuses known to mean a
        # poison payload (400 / single-entry 413).
        raise RetryableSinkError(f"Loki push failed with unexpected HTTP {status}")

    def _note_auth_failure(self, status: int) -> None:
        """Log auth/config rejections loudly, rate-limited across retries."""
        self._consecutive_auth_failures += 1
        n = self._consecutive_auth_failures
        if n == 1 or n % _AUTH_LOG_EVERY == 0:
            log.error(
                "Loki push rejected (auth/config) — check auth token / tenant_id / url; "
                "retrying with backoff, no data dropped",
                status=status,
                consecutive_failures=n,
            )

    def _note_auth_ok(self) -> None:
        if self._consecutive_auth_failures:
            log.info(
                "Loki push auth recovered",
                after_failures=self._consecutive_auth_failures,
            )
            self._consecutive_auth_failures = 0

    def _cap_lines(self, batch: Batch) -> None:
        """Truncate any over-cap lines in place before encoding.

        A single oversized line would otherwise be rejected by Loki (HTTP 400,
        ``max_line_size`` exceeded) and take its whole batch down. Truncating
        here keeps the rest of the batch deliverable. Idempotent — re-capping an
        already-capped line is a no-op, so this is safe under 413 split recursion.
        """
        cap = self._cfg.batch.max_line_bytes
        if cap <= 0:
            return
        for entry in batch.entries:
            # Fast path (the common case): the memoized line length (already
            # computed during pipeline byte accounting, issue #69) says the line
            # fits, so skip cap_line's re-encode entirely.
            if entry.line_nbytes() <= cap:
                continue
            capped, truncated = cap_line(entry.line, cap)
            if truncated:
                entry.line = capped
                entry._line_nbytes = -1  # line mutated: drop the stale memo
                self._metrics.lines_truncated.labels(
                    source=entry.labels.get("source", "unknown")
                ).inc()

    async def aclose(self) -> None:
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Internal sentinel for tenacity
# ---------------------------------------------------------------------------


class _TransientError(Exception):
    """Internal signal: this failure is retryable.

    ``retry_after`` carries a parsed Retry-After delay (seconds) when the
    server sent one; :func:`_compute_wait` honours it.
    """

    def __init__(self, message: str, *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


# ---------------------------------------------------------------------------
# Type stubs for mypy: tenacity.AsyncRetrying is used as an async context manager
# ---------------------------------------------------------------------------

# Make the module importable for type checking purposes; mypy sees tenacity stubs
# via the package itself (no overrides needed for standard usage).


def __getattr__(name: str) -> Any:
    raise AttributeError(name)
