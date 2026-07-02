"""EventLogFile REST client: lists ELF metadata via SOQL, downloads + parses LogFile CSVs.

Ref: docs/sources/eventlogfile.md.

All failures surface as the :class:`EventLogFileError` family (SOQL errors from
the internal listing client and httpx transport errors are re-wrapped), so the
source has exactly one exception type to handle per client. A 403
``REQUEST_LIMIT_EXCEEDED`` raises the :class:`EventLogFileThrottledError`
subclass so callers can back off distinctly.
"""

from __future__ import annotations

import csv
import email.utils
import io
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

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

# Downloaded CSV bodies up to this size stay in memory; larger ones spill to a
# temp file on disk (Salesforce documents ELF blobs exceeding 100MB — buffering
# those in RAM, let alone as decoded str + parsed rows, is not acceptable).
_SPOOL_MAX_MEMORY_BYTES = 8 * 1024 * 1024

# DictReader key for row cells beyond the header width. A stable string (never
# the default ``None`` restkey) so json.dumps on the row can't TypeError.
_OVERFLOW_KEY = "_extra"

_REQUEST_LIMIT_ERROR_CODE = "REQUEST_LIMIT_EXCEEDED"

# The stdlib csv module caps a single field at 128 KiB by default. ELF
# free-text columns (QUERY, URI, USER_AGENT, stack traces, ...) can exceed
# that, making csv.Error deterministic for the whole file (issue #41). Raise
# it well past the sink's default per-line cap (sink.loki.batch.max_line_bytes,
# 262144 bytes) — an oversized line is truncated downstream by the sink
# regardless, so there's no upside to the stdlib's conservative default here.
# csv.field_size_limit is process-global, so this is set once at import time.
_CSV_FIELD_SIZE_LIMIT = 4 * 262_144
csv.field_size_limit(_CSV_FIELD_SIZE_LIMIT)


def _as_int(value: object) -> int:
    """Coerce a Salesforce numeric field to int, tolerating floats / float-strings.

    Salesforce serialises ``Sequence``/``LogFileLength`` as JSON numbers that decode
    to floats (e.g. ``12899.0``), so a naive ``int(str(v))`` raises on the ``.0``.
    Missing/unparseable values map to 0.
    """
    if value is None:
        return 0
    try:
        return int(float(value))  # type: ignore[arg-type]
    except TypeError, ValueError:
        return 0


class EventLogFileError(Exception):
    """Raised when an EventLogFile listing or LogFile download fails (HTTP, SOQL,
    transport, or CSV parse)."""


class EventLogFileThrottledError(EventLogFileError):
    """Raised on a 403 REQUEST_LIMIT_EXCEEDED — back off until the next poll."""


@dataclass(frozen=True, slots=True)
class EventLogFileMeta:
    """Metadata for a single EventLogFile record (one ELF object per type/interval/period)."""

    id: str
    event_type: str
    interval: str
    log_date: str
    created_date: str
    sequence: int
    length: int


class EventLogFileClient:
    """Lists EventLogFile metadata via SOQL and downloads + parses the CSV LogFile body.

    Reuses :class:`~sf2loki.salesforce.soql_client.SoqlClient` for listing (constructed
    internally) so SOQL pagination/401-retry logic is not duplicated here.
    """

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
        self._soql = SoqlClient(sf_cfg, tokens, client, metrics=self._metrics)
        # Last observed (Salesforce server time - local now), from the Date
        # response header of this client's OWN requests. Captured PER-REQUEST
        # (never via a hook registered on the shared httpx client) — a hook on
        # the shared client fires for every org's responses in a multi-org
        # deployment, so N clients' hooks would stack and stomp on each
        # other's `_clock_skew` with whichever org's response landed last
        # (issue #68). SOQL listing/discovery go through SoqlClient, which
        # doesn't expose response headers back to this client, so skew is
        # refreshed only from this client's own LogFile download responses —
        # sufficient, since the settle/abandon gates only need an occasional
        # reading of server time, not a per-call one.
        self._clock_skew: timedelta | None = None

    def _record_server_date(self, response: httpx.Response) -> None:
        """Record clock skew from *response*'s Date header, if present.

        Called explicitly after every request this client makes itself
        (currently just LogFile downloads) — never registered as a shared
        client-wide hook, so this instance's skew can never be overwritten by
        another org's client sharing the same httpx.AsyncClient.
        """
        date_header = response.headers.get("Date")
        if not date_header:
            return
        try:
            server_time = email.utils.parsedate_to_datetime(date_header)
        except TypeError, ValueError:
            return
        if server_time.tzinfo is None:
            server_time = server_time.replace(tzinfo=UTC)
        self._clock_skew = server_time - datetime.now(UTC)

    def clock_skew(self) -> timedelta | None:
        """Most recent (Salesforce server time - local now), or None if unknown.

        1-second resolution plus network latency noise — callers should ignore
        small values (the ELF source applies it only beyond a 30s threshold).
        """
        return self._clock_skew

    async def list_files(
        self,
        event_type: str,
        interval: str,
        since: str,
        page_size: int,
    ) -> list[EventLogFileMeta]:
        """List EventLogFile records for *event_type*/*interval* created since *since*.

        *since* is a SOQL datetime literal and must NOT be quoted in the generated SOQL
        (CreatedDate is a datetime field, not a string). It is passed through
        :func:`to_soql_datetime_literal` so a raw Salesforce CreatedDate echoed back from
        a checkpoint (``…+0000`` offset) is reformatted into a SOQL-legal ``…Z`` literal.
        """
        since_literal = to_soql_datetime_literal(since)
        soql = (
            "SELECT Id,EventType,Interval,LogDate,CreatedDate,LogFileLength,Sequence "
            "FROM EventLogFile "
            f"WHERE EventType='{event_type}' AND Interval='{interval}' "
            f"AND CreatedDate >= {since_literal} "
            "ORDER BY CreatedDate, Id "
            f"LIMIT {page_size}"
        )
        files: list[EventLogFileMeta] = []
        try:
            async for record in self._soql.query(soql):
                files.append(
                    EventLogFileMeta(
                        id=str(record["Id"]),
                        event_type=str(record.get("EventType", event_type)),
                        interval=str(record.get("Interval", interval)),
                        log_date=str(record.get("LogDate", "")),
                        created_date=str(record.get("CreatedDate", "")),
                        # Salesforce returns Sequence/LogFileLength as JSON numbers that
                        # decode to floats (e.g. 12899.0); go via float() so int() doesn't
                        # choke on the ".0" (int("12899.0") raises).
                        sequence=_as_int(record.get("Sequence")),
                        length=_as_int(record.get("LogFileLength")),
                    )
                )
        except SoqlError as exc:
            raise self._wrap_soql_error(
                exc, f"EventLogFile listing failed for {event_type}"
            ) from exc
        return files

    async def list_event_types(self, interval: str) -> list[str]:
        """Discover the distinct ELF EventTypes the org currently produces for *interval*.

        Uses a *filtered* ``GROUP BY`` — an unfiltered ``COUNT()``/``GROUP BY`` on
        EventLogFile under-reports (a Salesforce aggregate quirk), but
        ``WHERE Interval=... GROUP BY EventType`` reliably returns the full set.
        """
        soql = f"SELECT EventType FROM EventLogFile WHERE Interval='{interval}' GROUP BY EventType"
        types: set[str] = set()
        try:
            async for record in self._soql.query(soql):
                value = record.get("EventType")
                if value:
                    types.add(str(value))
        except SoqlError as exc:
            raise self._wrap_soql_error(exc, "EventLogFile EventType discovery failed") from exc
        return sorted(types)

    def _wrap_soql_error(self, exc: SoqlError, context: str) -> EventLogFileError:
        """Re-wrap a SoqlError from the internal listing client into this client's family."""
        self._metrics.eventlogfile_download_errors.labels(reason="listing").inc()
        if isinstance(exc, SoqlThrottledError):
            return EventLogFileThrottledError(f"{context}: {exc}")
        return EventLogFileError(f"{context}: {exc}")

    async def download(self, file_meta: EventLogFileMeta) -> AsyncIterator[dict[str, str]]:
        """Download the CSV body for *file_meta*, yielding one dict per row.

        The body is **streamed** to a spooled temp file (in-memory up to
        ``_SPOOL_MAX_MEMORY_BYTES``, then disk) and parsed incrementally, so peak
        RAM is O(row) instead of O(file) — Salesforce documents ELF blobs
        exceeding 100MB. The full body is fetched *before* the first row is
        yielded, so all network failures surface at the first ``anext()`` and a
        partially-downloaded file never emits rows.

        Uses ``csv.DictReader`` (not naive line-splitting) because ELF CSV
        fields (e.g. ``QUERY``) may contain embedded newlines inside quoted
        values — splitting on ``\\n`` would corrupt those rows. Overflow cells
        beyond the header land under the string key ``"_extra"`` (joined) and
        short rows are padded with ``""`` — a malformed row must never produce
        a ``None`` key/value that breaks downstream JSON serialization.
        """
        try:
            spool, total_bytes = await self._fetch_to_spool(file_meta)
        except httpx.HTTPError as exc:
            self._metrics.eventlogfile_download_errors.labels(reason="transport").inc()
            raise EventLogFileError(
                f"EventLogFile download failed for {file_meta.id}: {type(exc).__name__}: {exc}"
            ) from exc

        self._metrics.eventlogfile_download_bytes.labels(event_type=file_meta.event_type).inc(
            total_bytes
        )
        self._metrics.eventlogfile_files_processed.labels(event_type=file_meta.event_type).inc()

        with spool:
            text = io.TextIOWrapper(spool, encoding="utf-8", errors="replace", newline="")
            reader: csv.DictReader[str] = csv.DictReader(text, restkey=_OVERFLOW_KEY, restval="")
            try:
                for row in reader:
                    extra = row.get(_OVERFLOW_KEY)
                    if isinstance(extra, list):
                        row[_OVERFLOW_KEY] = ",".join(extra)
                    yield row
            except csv.Error as exc:
                raise EventLogFileError(
                    f"EventLogFile CSV parse failed for {file_meta.id} "
                    f"at line {reader.line_num}: {exc}"
                ) from exc
            finally:
                # Detach so closing the TextIOWrapper doesn't double-close spool
                # (the `with spool` above owns it).
                text.detach()

    async def _fetch_to_spool(
        self, file_meta: EventLogFileMeta
    ) -> tuple[tempfile.SpooledTemporaryFile[bytes], int]:
        """Stream the LogFile body into a spooled temp file (with one 401 retry).

        Returns the spool (rewound to 0) and the total byte count.
        """
        tok = await self._tokens.token()
        for attempt in (0, 1):
            url = (
                f"{tok.instance_url}/services/data/v{self._cfg.api_version}"
                f"/sobjects/EventLogFile/{file_meta.id}/LogFile"
            )
            headers = {"Authorization": f"Bearer {tok.value}"}
            async with self._client.stream("GET", url, headers=headers) as response:
                self._record_server_date(response)
                if response.status_code == 401 and attempt == 0:
                    self._tokens.invalidate()
                    tok = await self._tokens.token()
                    continue

                if not response.is_success:
                    body = (await response.aread()).decode("utf-8", errors="replace")
                    self._metrics.eventlogfile_download_errors.labels(
                        reason=f"HTTP {response.status_code}"
                    ).inc()
                    if response.status_code == 403 and _REQUEST_LIMIT_ERROR_CODE in body:
                        self._metrics.salesforce_api_throttled.labels(api="eventlogfile").inc()
                        raise EventLogFileThrottledError(
                            f"EventLogFile download throttled for {file_meta.id}: "
                            f"HTTP 403 {_REQUEST_LIMIT_ERROR_CODE} — {body}"
                        )
                    raise EventLogFileError(
                        f"EventLogFile download failed for {file_meta.id}: "
                        f"HTTP {response.status_code} — {body}"
                    )

                spool: tempfile.SpooledTemporaryFile[bytes] = tempfile.SpooledTemporaryFile(
                    max_size=_SPOOL_MAX_MEMORY_BYTES
                )
                total = 0
                try:
                    async for chunk in response.aiter_bytes():
                        spool.write(chunk)
                        total += len(chunk)
                except BaseException:
                    spool.close()
                    raise
                spool.seek(0)
                return spool, total
        raise AssertionError("unreachable: 401 retry loop exhausted")
