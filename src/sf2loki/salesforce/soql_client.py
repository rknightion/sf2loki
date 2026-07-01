"""SOQL REST API client with pagination and 401-retry logic.

Ref: DESIGN.md §7.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics

# Salesforce REST errorCode signalling the org's rolling 24h API request limit
# was exhausted (HTTP 403). There is no Retry-After header on SF responses.
_REQUEST_LIMIT_ERROR_CODE = "REQUEST_LIMIT_EXCEEDED"


class SoqlError(Exception):
    """Raised when the Salesforce SOQL query endpoint returns a non-2xx (non-401) response
    or the request fails at the transport layer (connect/read/timeout)."""


class SoqlThrottledError(SoqlError):
    """Raised on a 403 REQUEST_LIMIT_EXCEEDED — the org's API request limit is exhausted.

    Callers should back off (skip to the next poll interval) rather than retry
    immediately; Salesforce sends no Retry-After.
    """


def to_soql_datetime_literal(value: str) -> str:
    """Normalize a datetime string into a SOQL-legal dateTime literal (UTC, ms, ``Z``).

    Salesforce REST serializes datetimes as e.g. ``2026-06-30T01:00:00.000+0000``
    (no colon in the offset). That is **not** a legal SOQL dateTime literal — SOQL
    requires a colon offset (``+hh:mm``) or a bare ``Z``. Echoing the raw REST value
    straight back into a ``WHERE`` clause therefore yields ``MALFORMED_QUERY``, so any
    persisted watermark/CreatedDate must be reformatted before it is reused in SOQL.

    Unparseable input is returned unchanged (best effort — better a possibly-bad
    literal than a crash on an unexpected format).
    """
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


class SoqlClient:
    """Thin async wrapper around the Salesforce REST query endpoint.

    Handles:
    - Authorization header injection via :class:`~sf2loki.auth.jwt_auth.TokenProvider`.
    - Automatic pagination via ``nextRecordsUrl``.
    - One transparent retry on 401 (token invalidation + re-mint).
    - :class:`SoqlThrottledError` on 403 ``REQUEST_LIMIT_EXCEEDED`` (plus the
      ``salesforce_api_throttled`` metric).
    - :class:`SoqlError` on any other non-2xx response or transport failure —
      callers only ever see the SoqlError family.
    """

    def __init__(
        self,
        cfg: SalesforceConfig,
        tokens: TokenProvider,
        client: httpx.AsyncClient,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        self._cfg = cfg
        self._tokens = tokens
        self._client = client
        self._metrics = metrics if metrics is not None else Metrics()

    async def query(self, soql: str) -> AsyncIterator[dict[str, object]]:
        """Execute *soql* and yield each record, following pagination.

        Yields records as-is (Salesforce ``attributes`` key included if present).
        Raises :class:`SoqlError` (or :class:`SoqlThrottledError`) on failures
        other than 401 (which triggers a single retry with a fresh token).
        """
        tok = await self._tokens.token()
        base_url = tok.instance_url
        url: str | None = f"{base_url}/services/data/v{self._cfg.api_version}/query"
        params: dict[str, str] | None = {"q": soql}

        while url is not None:
            headers = {"Authorization": f"Bearer {tok.value}"}
            try:
                response = await self._client.get(url, params=params, headers=headers)

                if response.status_code == 401:
                    # Invalidate and retry exactly once with a fresh token.
                    self._tokens.invalidate()
                    tok = await self._tokens.token()
                    headers = {"Authorization": f"Bearer {tok.value}"}
                    response = await self._client.get(url, params=params, headers=headers)
            except httpx.HTTPError as exc:
                # Normalize transport errors (connect/read/timeout/...) into the
                # SoqlError family so callers have ONE exception type to handle.
                raise SoqlError(f"SOQL query failed: {type(exc).__name__}: {exc}") from exc

            if not response.is_success:
                if response.status_code == 403 and _REQUEST_LIMIT_ERROR_CODE in response.text:
                    self._metrics.salesforce_api_throttled.labels(api="soql").inc()
                    raise SoqlThrottledError(
                        f"Salesforce API request limit exceeded (HTTP 403 "
                        f"{_REQUEST_LIMIT_ERROR_CODE}) — {response.text}"
                    )
                raise SoqlError(f"SOQL query failed: HTTP {response.status_code} — {response.text}")

            body = response.json()
            for record in body.get("records", []):
                yield record

            # Pagination: nextRecordsUrl is an absolute path (e.g. /services/data/...)
            if body.get("done") is False and body.get("nextRecordsUrl"):
                url = f"{base_url}{body['nextRecordsUrl']}"
                params = None  # Query string is embedded in the URL for page 2+
            else:
                url = None
