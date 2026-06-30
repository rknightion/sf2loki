"""EventLogFile REST client: lists ELF metadata via SOQL, downloads + parses LogFile CSVs.

Ref: DESIGN.md §8.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass

import httpx

from sf2loki.auth.jwt_auth import TokenProvider
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.soql_client import SoqlClient


class EventLogFileError(Exception):
    """Raised when the EventLogFile LogFile download endpoint returns a non-2xx response."""


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
        self._soql = SoqlClient(sf_cfg, tokens, client)

    async def list_files(
        self,
        event_type: str,
        interval: str,
        since: str,
        page_size: int,
    ) -> list[EventLogFileMeta]:
        """List EventLogFile records for *event_type*/*interval* created since *since*.

        *since* is a SOQL datetime literal (e.g. ``"2026-06-30T10:00:00Z"``) and must
        NOT be quoted in the generated SOQL (CreatedDate is a datetime field, not a
        string).
        """
        soql = (
            "SELECT Id,EventType,Interval,LogDate,CreatedDate,LogFileLength,Sequence "
            "FROM EventLogFile "
            f"WHERE EventType='{event_type}' AND Interval='{interval}' "
            f"AND CreatedDate >= {since} "
            "ORDER BY CreatedDate, Id "
            f"LIMIT {page_size}"
        )
        files: list[EventLogFileMeta] = []
        async for record in self._soql.query(soql):
            sequence_raw = record.get("Sequence")
            length_raw = record.get("LogFileLength")
            files.append(
                EventLogFileMeta(
                    id=str(record["Id"]),
                    event_type=str(record.get("EventType", event_type)),
                    interval=str(record.get("Interval", interval)),
                    log_date=str(record.get("LogDate", "")),
                    created_date=str(record.get("CreatedDate", "")),
                    sequence=int(str(sequence_raw)) if sequence_raw is not None else 0,
                    length=int(str(length_raw)) if length_raw is not None else 0,
                )
            )
        return files

    async def download(self, file_meta: EventLogFileMeta) -> list[dict[str, str]]:
        """Download and parse the CSV body for *file_meta*.

        NOTE: the entire response body is buffered in memory before parsing.
        Acceptable for v1; EventLogFiles larger than ~100MB are a known
        limitation (DESIGN.md §8).

        Uses ``csv.DictReader`` (not naive line-splitting) because ELF CSV
        fields (e.g. ``QUERY``) may contain embedded newlines inside quoted
        values — splitting on ``\\n`` would corrupt those rows.
        """
        tok = await self._tokens.token()
        url = (
            f"{tok.instance_url}/services/data/v{self._cfg.api_version}"
            f"/sobjects/EventLogFile/{file_meta.id}/LogFile"
        )
        headers = {"Authorization": f"Bearer {tok.value}"}
        response = await self._client.get(url, headers=headers)

        if response.status_code == 401:
            self._tokens.invalidate()
            tok = await self._tokens.token()
            headers = {"Authorization": f"Bearer {tok.value}"}
            response = await self._client.get(url, headers=headers)

        if not response.is_success:
            self._metrics.eventlogfile_download_errors.labels(
                reason=f"HTTP {response.status_code}"
            ).inc()
            raise EventLogFileError(
                f"EventLogFile download failed for {file_meta.id}: "
                f"HTTP {response.status_code} — {response.text}"
            )

        reader: csv.DictReader[str] = csv.DictReader(io.StringIO(response.text))
        rows: list[dict[str, str]] = [dict(row) for row in reader]

        self._metrics.eventlogfile_download_bytes.labels(event_type=file_meta.event_type).inc(
            len(response.content)
        )
        self._metrics.eventlogfile_files_processed.labels(event_type=file_meta.event_type).inc()

        return rows
