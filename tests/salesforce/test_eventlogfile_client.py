"""Tests for EventLogFileClient — SOQL listing + LogFile CSV download."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import SalesforceConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.eventlogfile_client import (
    EventLogFileClient,
    EventLogFileError,
    EventLogFileMeta,
)

# ---------------------------------------------------------------------------
# Shared fakes (mirrors tests/salesforce/test_soql_client.py)


class FakeTokenProvider:
    """Minimal token provider for unit tests."""

    def __init__(
        self, token_value: str = "tok", instance_url: str = "https://x.my.salesforce.com"
    ) -> None:
        self._token_value = token_value
        self._invalidated = False

    async def token(self) -> AccessToken:
        value = f"{self._token_value}-refreshed" if self._invalidated else self._token_value
        return AccessToken(
            value=value,
            instance_url="https://x.my.salesforce.com",
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )

    async def org_id(self) -> str:
        return "00Dxx"

    def invalidate(self) -> None:
        self._invalidated = True


def make_sf_cfg(api_version: str = "60.0") -> SalesforceConfig:
    return SalesforceConfig(
        client_id="cid",
        username="svc@example.com",
        private_key="DUMMYKEY",
        api_version=api_version,
    )


def _query_url(instance: str = "https://x.my.salesforce.com", version: str = "60.0") -> str:
    return f"{instance}/services/data/v{version}/query"


def _logfile_url(
    file_id: str,
    instance: str = "https://x.my.salesforce.com",
    version: str = "60.0",
) -> str:
    return f"{instance}/services/data/v{version}/sobjects/EventLogFile/{file_id}/LogFile"


def make_file_meta(
    *,
    id: str = "0ATxx0000000001",
    event_type: str = "Login",
    interval: str = "Hourly",
) -> EventLogFileMeta:
    return EventLogFileMeta(
        id=id,
        event_type=event_type,
        interval=interval,
        log_date="2026-06-30T00:00:00.000+0000",
        created_date="2026-06-30T01:00:00.000+0000",
        sequence=1,
        length=1234,
    )


# ---------------------------------------------------------------------------
# list_files


@pytest.mark.asyncio
@respx.mock
async def test_list_files_builds_correct_soql_and_maps_records() -> None:
    """SOQL includes EventType/Interval filters + unquoted CreatedDate, maps records."""
    captured_q: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        qs = parse_qs(urlparse(str(request.url)).query)
        captured_q.append(qs["q"][0])
        return httpx.Response(
            200,
            json={
                "records": [
                    {
                        "Id": "0ATxx0000000001",
                        "EventType": "Login",
                        "Interval": "Hourly",
                        "LogDate": "2026-06-30T00:00:00.000+0000",
                        "CreatedDate": "2026-06-30T01:00:00.000+0000",
                        "LogFileLength": 9999,
                        "Sequence": 2,
                    },
                    # Missing Sequence/LogFileLength -> map to 0.
                    {
                        "Id": "0ATxx0000000002",
                        "EventType": "Login",
                        "Interval": "Hourly",
                        "LogDate": "2026-06-30T00:00:00.000+0000",
                        "CreatedDate": "2026-06-30T02:00:00.000+0000",
                    },
                ],
                "done": True,
            },
        )

    respx.get(_query_url()).mock(side_effect=capture)

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client)
        files = await elf_client.list_files(
            event_type="Login", interval="Hourly", since="2026-06-30T00:00:00Z", page_size=1000
        )

    assert len(captured_q) == 1
    q = captured_q[0]
    assert "FROM EventLogFile" in q
    assert "EventType='Login'" in q
    assert "Interval='Hourly'" in q
    # CreatedDate is a SOQL datetime literal -- NO surrounding quotes, normalized to ms+Z.
    assert "CreatedDate >= 2026-06-30T00:00:00.000Z" in q
    assert "'2026-06-30T00:00:00.000Z'" not in q
    assert "LIMIT 1000" in q

    assert len(files) == 2
    f1, f2 = files
    assert f1 == EventLogFileMeta(
        id="0ATxx0000000001",
        event_type="Login",
        interval="Hourly",
        log_date="2026-06-30T00:00:00.000+0000",
        created_date="2026-06-30T01:00:00.000+0000",
        sequence=2,
        length=9999,
    )
    assert f2.sequence == 0
    assert f2.length == 0


@pytest.mark.asyncio
@respx.mock
async def test_list_files_handles_float_formatted_length_and_sequence() -> None:
    """Real Salesforce returns LogFileLength (and sometimes Sequence) as JSON numbers
    that decode to floats (e.g. 12899.0); mapping must not choke on ``int('12899.0')``."""
    respx.get(_query_url()).mock(
        return_value=httpx.Response(
            200,
            json={
                "records": [
                    {
                        "Id": "0ATxx0000000009",
                        "EventType": "Login",
                        "Interval": "Daily",
                        "LogDate": "2026-07-01T00:00:00.000+0000",
                        "CreatedDate": "2026-07-01T00:47:20.000+0000",
                        "LogFileLength": 12899.0,
                        "Sequence": 0.0,
                    },
                ],
                "done": True,
            },
        )
    )

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client)
        files = await elf_client.list_files(
            event_type="Login", interval="Daily", since="2026-07-01T00:00:00Z", page_size=1000
        )

    assert len(files) == 1
    assert files[0].length == 12899
    assert files[0].sequence == 0


@pytest.mark.asyncio
@respx.mock
async def test_list_files_normalizes_raw_salesforce_since() -> None:
    """A checkpointed CreatedDate (+0000 offset) is reformatted into a legal SOQL literal."""
    captured_q: list[str] = []

    def capture(request: httpx.Request) -> httpx.Response:
        captured_q.append(parse_qs(urlparse(str(request.url)).query)["q"][0])
        return httpx.Response(200, json={"records": [], "done": True})

    respx.get(_query_url()).mock(side_effect=capture)

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client)
        await elf_client.list_files(
            event_type="Login",
            interval="Hourly",
            since="2026-06-30T01:00:00.000+0000",  # raw Salesforce REST form
            page_size=1000,
        )

    q = captured_q[0]
    assert "CreatedDate >= 2026-06-30T01:00:00.000Z" in q
    assert "+0000" not in q  # the malformed-for-SOQL form is gone


# ---------------------------------------------------------------------------
# download


@pytest.mark.asyncio
@respx.mock
async def test_download_parses_csv_with_embedded_newline() -> None:
    """csv.DictReader correctly parses a field with an embedded newline.

    Naively splitting the response body on '\\n' would break this row in two.
    """
    file_meta = make_file_meta()
    csv_body = (
        "TIMESTAMP_DERIVED,QUERY,ROW_COUNT\r\n"
        '20260630010000.000,"SELECT Id\nFROM Account",5\r\n'
        "20260630010100.000,SELECT Id FROM Contact,3\r\n"
    )
    respx.get(_logfile_url(file_meta.id)).mock(return_value=httpx.Response(200, text=csv_body))

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client)
        rows = await elf_client.download(file_meta)

    assert len(rows) == 2
    assert rows[0]["QUERY"] == "SELECT Id\nFROM Account"
    assert rows[0]["ROW_COUNT"] == "5"
    assert rows[1]["QUERY"] == "SELECT Id FROM Contact"


@pytest.mark.asyncio
@respx.mock
async def test_download_401_invalidates_and_retries_with_fresh_token() -> None:
    file_meta = make_file_meta()
    call_count = 0

    def side_effect(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        auth = request.headers.get("Authorization", "")
        if "refreshed" in auth:
            return httpx.Response(200, text="A,B\r\n1,2\r\n")
        return httpx.Response(401, text="Unauthorized")

    respx.get(_logfile_url(file_meta.id)).mock(side_effect=side_effect)

    tokens = FakeTokenProvider()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client)
        rows = await elf_client.download(file_meta)

    assert rows == [{"A": "1", "B": "2"}]
    assert tokens._invalidated is True
    assert call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_download_increments_metrics_on_success() -> None:
    file_meta = make_file_meta(event_type="Login")
    csv_body = "A,B\r\n1,2\r\n3,4\r\n"
    respx.get(_logfile_url(file_meta.id)).mock(return_value=httpx.Response(200, text=csv_body))

    tokens = FakeTokenProvider()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client, metrics=metrics)
        await elf_client.download(file_meta)

    bytes_val = metrics.registry.get_sample_value(
        "sf2loki_eventlogfile_download_bytes_total", {"event_type": "Login"}
    )
    assert bytes_val == float(len(csv_body.encode()))

    processed_val = metrics.registry.get_sample_value(
        "sf2loki_eventlogfile_files_processed_total", {"event_type": "Login"}
    )
    assert processed_val == 1.0


@pytest.mark.asyncio
@respx.mock
async def test_download_non_2xx_raises_and_increments_error_metric() -> None:
    file_meta = make_file_meta()
    respx.get(_logfile_url(file_meta.id)).mock(
        return_value=httpx.Response(500, text="Internal Server Error")
    )

    tokens = FakeTokenProvider()
    metrics = Metrics()
    async with httpx.AsyncClient() as client:
        elf_client = EventLogFileClient(make_sf_cfg(), tokens, client, metrics=metrics)
        with pytest.raises(EventLogFileError) as exc_info:
            await elf_client.download(file_meta)

    assert "500" in str(exc_info.value)

    err_val = metrics.registry.get_sample_value(
        "sf2loki_eventlogfile_download_errors_total", {"reason": "HTTP 500"}
    )
    assert err_val == 1.0
