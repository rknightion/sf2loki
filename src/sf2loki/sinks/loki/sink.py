"""LokiSink: HTTP push sink for Grafana Loki.

Satisfies the Sink protocol (push / aclose). Handles auth, encoding/compression,
bounded retries with tenacity, and 413 payload splitting.
"""

from __future__ import annotations

import base64
import gzip
import logging
from typing import Any

import httpx
import tenacity

from sf2loki.config import LokiConfig
from sf2loki.model import Batch
from sf2loki.obs.metrics import Metrics
from sf2loki.shaping import cap_line
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki.labels import guard_labels
from sf2loki.sinks.loki.push import encode_json, encode_protobuf

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level retry knobs — monkeypatched in tests to keep them fast.
# ---------------------------------------------------------------------------

_MAX_ATTEMPTS: int = 5
_WAIT_MIN: float = 0.5  # seconds
_WAIT_MAX: float = 30.0  # seconds


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
        guard_labels(cfg.labels)  # fail fast on disallowed static label keys

        self._cfg = cfg
        self._client = client
        self._headers = self._build_headers()
        self._metrics = metrics if metrics is not None else Metrics()

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
                raise _TransientError(f"HTTP {status}")
            return status

        retry = tenacity.AsyncRetrying(
            reraise=False,
            stop=tenacity.stop_after_attempt(_MAX_ATTEMPTS),
            wait=tenacity.wait_random_exponential(min=_WAIT_MIN, max=_WAIT_MAX),
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
        """Encode *batch* and POST it to Loki, with retries and 413 splitting."""
        if not batch.entries:
            return

        self._cap_lines(batch)

        body, content_headers = self._encode(batch)
        status = await self._post(body, content_headers)

        if 200 <= status < 300:
            self._metrics.loki_bytes_pushed.inc(len(body))
            return

        if status == 413:
            if len(batch.entries) == 1:
                raise PermanentSinkError("unsplittable 413: single-entry batch rejected")
            # Split and recurse — re-encode each half independently. A permanent
            # failure in one half (e.g. one oversized entry that 413s even when
            # alone) must NOT discard the other half: drop+count just the poison
            # and keep delivering the rest. Only a top-level single-entry batch
            # propagates PermanentSinkError (no parent to absorb it).
            mid = len(batch.entries) // 2
            for half in (Batch(entries=batch.entries[:mid]), Batch(entries=batch.entries[mid:])):
                try:
                    await self.push(half)
                except PermanentSinkError:
                    self._metrics.loki_push.labels(outcome="dropped").inc(len(half.entries))
                    logger.warning(
                        "dropping %d undeliverable Loki entries (permanent error during 413 split)",
                        len(half.entries),
                    )
            return

        if status == 400:
            raise PermanentSinkError("Loki rejected batch (400 Bad Request)")

        # Any other 4xx is also permanent.
        raise PermanentSinkError(f"Loki rejected batch (HTTP {status})")

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
            capped, truncated = cap_line(entry.line, cap)
            if truncated:
                entry.line = capped
                self._metrics.lines_truncated.labels(
                    source=entry.labels.get("source", "unknown")
                ).inc()

    async def aclose(self) -> None:
        await self._client.aclose()


# ---------------------------------------------------------------------------
# Internal sentinel for tenacity
# ---------------------------------------------------------------------------


class _TransientError(Exception):
    """Internal signal: this failure is retryable."""


# ---------------------------------------------------------------------------
# Type stubs for mypy: tenacity.AsyncRetrying is used as an async context manager
# ---------------------------------------------------------------------------

# Make the module importable for type checking purposes; mypy sees tenacity stubs
# via the package itself (no overrides needed for standard usage).


def __getattr__(name: str) -> Any:
    raise AttributeError(name)
