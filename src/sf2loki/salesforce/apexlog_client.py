"""ApexLog Tooling-API client: lists debug-log metadata via SOQL, downloads bodies.

Ref: issue #33. ApexLog + TraceFlag are Tooling-API sObjects, so listing goes
through an internal tooling-mode :class:`~sf2loki.salesforce.soql_client.SoqlClient`
(reusing its pagination / 401-retry / throttle handling). Bodies are downloaded
one REST call at a time from ``/tooling/sobjects/ApexLog/<id>/Body``.

All failures surface as the :class:`ApexLogError` family; a 403
``REQUEST_LIMIT_EXCEEDED`` raises :class:`ApexLogThrottledError` so callers back
off until the next poll instead of hammering an exhausted API budget.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.soql_client import (
    SoqlClient,
    SoqlError,
    SoqlThrottledError,
    to_soql_datetime_literal,
)

_REQUEST_LIMIT_ERROR_CODE = "REQUEST_LIMIT_EXCEEDED"

# Fields selected for each ApexLog row (metadata only — the body is a separate
# blob download).
_APEXLOG_FIELDS = (
    "Id, LogUserId, LogLength, Operation, Request, Status, StartTime, "
    "Application, DurationMilliseconds, Location"
)


class ApexLogError(Exception):
    """Raised when an ApexLog listing or body download fails (HTTP/SOQL/transport)."""


class ApexLogThrottledError(ApexLogError):
    """Raised on a 403 REQUEST_LIMIT_EXCEEDED — back off until the next poll."""


def _as_int(value: object) -> int:
    """Coerce a Salesforce numeric field to int, tolerating floats / None."""
    if value is None:
        return 0
    try:
        return int(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True, slots=True)
class ApexLogMeta:
    """Metadata for one ApexLog row (body fetched separately by id)."""

    id: str
    log_user_id: str
    log_length: int
    operation: str
    request: str
    status: str
    start_time: str
    application: str
    duration_ms: int
    location: str


class ApexLogClient:
    """Lists ApexLog metadata via the Tooling API and downloads log bodies."""

    def __init__(
        self,
        sf_cfg: SalesforceConfig,
        tokens: TokenProvider,
        client: httpx.AsyncClient,
        *,
        metrics: Metrics | None = None,
    ) -> None:
        self._cfg = sf_cfg
        self._tokens = tokens
        self._client = client
        self._metrics = metrics if metrics is not None else Metrics()
        self._soql = SoqlClient(sf_cfg, tokens, client, metrics=self._metrics, tooling=True)

    async def list_logs(
        self, since: str, users: Sequence[str], page_size: int
    ) -> list[ApexLogMeta]:
        """List ApexLog rows with ``StartTime >= since``, oldest first.

        *since* is passed through :func:`to_soql_datetime_literal` (a raw
        Salesforce ``…+0000`` value echoed from a checkpoint is not a legal SOQL
        literal). When *users* is non-empty a ``LogUser.Username IN (...)`` filter
        is added (usernames are validated by config to a safe charset).
        """
        where = [f"StartTime >= {to_soql_datetime_literal(since)}"]
        if users:
            joined = ",".join(f"'{u}'" for u in users)
            where.append(f"LogUser.Username IN ({joined})")
        soql = (
            f"SELECT {_APEXLOG_FIELDS} FROM ApexLog "
            f"WHERE {' AND '.join(where)} "
            f"ORDER BY StartTime ASC LIMIT {page_size}"
        )
        logs: list[ApexLogMeta] = []
        try:
            async for r in self._soql.query(soql):
                logs.append(
                    ApexLogMeta(
                        id=str(r.get("Id", "")),
                        log_user_id=str(r.get("LogUserId", "")),
                        log_length=_as_int(r.get("LogLength")),
                        operation=str(r.get("Operation", "")),
                        request=str(r.get("Request", "")),
                        status=str(r.get("Status", "")),
                        start_time=str(r.get("StartTime", "")),
                        application=str(r.get("Application", "")),
                        duration_ms=_as_int(r.get("DurationMilliseconds")),
                        location=str(r.get("Location", "")),
                    )
                )
        except SoqlThrottledError as exc:
            raise ApexLogThrottledError(f"ApexLog listing throttled: {exc}") from exc
        except SoqlError as exc:
            raise ApexLogError(f"ApexLog listing failed: {exc}") from exc
        return logs

    async def count_active_traceflags(self) -> int:
        """Number of currently-active TraceFlags (ExpirationDate in the future).

        Used by ``doctor`` to warn when apexlog is enabled but nothing is
        generating logs. Counts up to one page (200) — enough for a boolean-ish
        health signal.
        """
        now_literal = to_soql_datetime_literal(datetime.now(UTC).isoformat())
        soql = f"SELECT Id FROM TraceFlag WHERE ExpirationDate > {now_literal} LIMIT 200"
        count = 0
        try:
            async for _ in self._soql.query(soql):
                count += 1
        except SoqlThrottledError as exc:
            raise ApexLogThrottledError(f"TraceFlag count throttled: {exc}") from exc
        except SoqlError as exc:
            raise ApexLogError(f"TraceFlag count failed: {exc}") from exc
        return count

    async def download_body(self, log_id: str) -> str:
        """Download the debug-log body for *log_id* (one API call, with one 401 retry).

        The caller (source) only invokes this when the row's LogLength is within
        ``max_body_bytes``, so the body fits comfortably in memory.
        """
        tok = await self._tokens.token()
        for attempt in (0, 1):
            url = (
                f"{tok.instance_url}/services/data/v{self._cfg.api_version}"
                f"/tooling/sobjects/ApexLog/{log_id}/Body"
            )
            headers = {"Authorization": f"Bearer {tok.value}"}
            try:
                resp = await self._client.get(url, headers=headers)
            except httpx.HTTPError as exc:
                self._metrics.apexlog_download_errors.labels(reason="transport").inc()
                raise ApexLogError(
                    f"ApexLog body download failed for {log_id}: {type(exc).__name__}: {exc}"
                ) from exc

            if resp.status_code == 401 and attempt == 0:
                self._tokens.invalidate()
                tok = await self._tokens.token()
                continue

            if not resp.is_success:
                self._metrics.apexlog_download_errors.labels(
                    reason=f"HTTP {resp.status_code}"
                ).inc()
                if resp.status_code == 403 and _REQUEST_LIMIT_ERROR_CODE in resp.text:
                    self._metrics.salesforce_api_throttled.labels(api="apexlog_body").inc()
                    raise ApexLogThrottledError(
                        f"ApexLog body download throttled for {log_id}: "
                        f"HTTP 403 {_REQUEST_LIMIT_ERROR_CODE}"
                    )
                raise ApexLogError(
                    f"ApexLog body download failed for {log_id}: "
                    f"HTTP {resp.status_code} — {resp.text}"
                )

            self._metrics.apexlog_download_bytes.inc(len(resp.content))
            return resp.text
        raise AssertionError("unreachable: 401 retry loop exhausted")
