"""Tests for `sf2loki backfill` (src/sf2loki/backfill.py).

Mocks both Salesforce (OAuth token mint, SOQL EventLogFile listing, LogFile
CSV download) and Loki (JSON push, for easy body introspection) via respx —
`run_backfill` takes only a `Config`, so it always builds its own real
TokenProvider/EventLogFileClient/LokiSink internally; there is no injection
seam to substitute fakes for.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from pydantic import SecretStr

from sf2loki import backfill as backfill_module
from sf2loki.backfill import parse_backfill_date, run_backfill
from sf2loki.config import Config
from sf2loki.salesforce.eventlogfile_client import EventLogFileMeta
from sf2loki.sinks.loki import sink as sink_module

# ---------------------------------------------------------------------------
# Fixtures & constants
# ---------------------------------------------------------------------------

LOGIN_URL = "https://login.salesforce.com"
INSTANCE_URL = "https://myorg.my.salesforce.com"
TOKEN_ENDPOINT = f"{LOGIN_URL}/services/oauth2/token"
QUERY_URL = f"{INSTANCE_URL}/services/data/v60.0/query"
LOKI_URL = "http://loki:3100/loki/api/v1/push"


@pytest.fixture()
def rsa_private_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


@pytest.fixture()
def private_key_pem(rsa_private_key: rsa.RSAPrivateKey) -> str:
    return rsa_private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()


def _deep_merge(base: dict[str, object], override: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in override.items():
        existing = merged.get(key)
        if isinstance(value, dict) and isinstance(existing, dict):
            merged[key] = _deep_merge(existing, value)
        else:
            merged[key] = value
    return merged


def _make_cfg(tmp_path, private_key_pem: str, **overrides: object) -> Config:
    base: dict[str, object] = {
        "salesforce": {
            "login_url": LOGIN_URL,
            "client_id": "cid",
            "username": "svc@example.com",
            "private_key": SecretStr(private_key_pem),
            "api_version": "60.0",
        },
        "sink": {
            "loki": {
                "url": LOKI_URL,
                "encoding": "json",
                "compression": "none",
            }
        },
        "sources": {
            "eventlogfile": {
                "enabled": True,
                "event_types": ["Login"],
            }
        },
        "state": {"file": {"path": tmp_path / "state.json"}},
    }
    merged = _deep_merge(base, overrides)
    return Config(**merged)


def _mock_token() -> None:
    respx.post(TOKEN_ENDPOINT).mock(
        return_value=httpx.Response(200, json={"access_token": "tok", "instance_url": INSTANCE_URL})
    )


def _created(days_ago: float, base: datetime | None = None) -> str:
    """A Salesforce-style CreatedDate literal `days_ago` before *base* (default: a
    fixed anchor so tests are deterministic)."""
    anchor = base or datetime(2026, 6, 15, tzinfo=UTC)
    dt = anchor - timedelta(days=days_ago)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def _file(id: str, event_type: str, created_date: str) -> EventLogFileMeta:
    return EventLogFileMeta(
        id=id,
        event_type=event_type,
        interval="Daily",
        log_date=created_date,
        created_date=created_date,
        sequence=1,
        length=100,
    )


def _parse_created_for_test(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")


def _mock_list_files(
    files_by_type: dict[str, list[EventLogFileMeta]],
) -> list[httpx.Request]:
    """Register the SOQL listing responder; returns the list of captured requests."""
    captured: list[httpx.Request] = []

    def responder(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        q = parse_qs(urlparse(str(request.url)).query)["q"][0]
        if "GROUP BY EventType" in q:
            types = sorted(files_by_type.keys())
            return httpx.Response(
                200, json={"records": [{"EventType": t} for t in types], "done": True}
            )
        et_match = re.search(r"EventType='([^']+)'", q)
        since_match = re.search(r"CreatedDate >= (\S+) ", q)
        limit_match = re.search(r"LIMIT (\d+)", q)
        event_type = et_match.group(1) if et_match else ""
        candidates = list(files_by_type.get(event_type, []))
        if since_match:
            since_dt = datetime.fromisoformat(since_match.group(1).replace("Z", "+00:00"))
            candidates = [
                f for f in candidates if _parse_created_for_test(f.created_date) >= since_dt
            ]
        candidates.sort(key=lambda f: (_parse_created_for_test(f.created_date), f.id))
        if limit_match:
            candidates = candidates[: int(limit_match.group(1))]
        records = [
            {
                "Id": f.id,
                "EventType": f.event_type,
                "Interval": f.interval,
                "LogDate": f.log_date,
                "CreatedDate": f.created_date,
                "LogFileLength": f.length,
                "Sequence": f.sequence,
            }
            for f in candidates
        ]
        return httpx.Response(200, json={"records": records, "done": True})

    respx.get(QUERY_URL).mock(side_effect=responder)
    return captured


def _logfile_url(file_id: str) -> str:
    return f"{INSTANCE_URL}/services/data/v60.0/sobjects/EventLogFile/{file_id}/LogFile"


def _mock_downloads(csv_by_id: dict[str, str]) -> dict[str, int]:
    """Register a download route per file id; returns a live id->call-count dict."""
    call_counts: dict[str, int] = dict.fromkeys(csv_by_id, 0)

    def make_responder(file_id: str) -> Callable[[httpx.Request], httpx.Response]:
        def responder(request: httpx.Request) -> httpx.Response:
            call_counts[file_id] += 1
            return httpx.Response(200, text=csv_by_id[file_id])

        return responder

    for file_id in csv_by_id:
        respx.get(_logfile_url(file_id)).mock(side_effect=make_responder(file_id))
    return call_counts


def _decode_pushes(route: respx.Route) -> list[dict[str, object]]:
    """Decode every JSON push body sent to the mocked Loki route, in call order."""
    return [json.loads(call.request.content) for call in route.calls]


def _all_values(bodies: list[dict[str, object]]) -> list[list[object]]:
    """Flatten every (stream, values) row across all push bodies, in order sent."""
    out: list[list[object]] = []
    for body in bodies:
        for stream in body["streams"]:  # type: ignore[index]
            out.extend(stream["values"])
    return out


def _row_label(line: str) -> str:
    """route_fields() renders the log line as canonical JSON of the whole row;
    pull out the ``ROW`` test column for label assertions."""
    label = json.loads(line)["ROW"]
    assert isinstance(label, str)
    return label


CSV_HEADER = "TIMESTAMP_DERIVED,ROW\r\n"


def _csv(rows: list[tuple[str, str]]) -> str:
    """rows: list of (TIMESTAMP_DERIVED value, ROW label)."""
    body = CSV_HEADER
    for ts, label in rows:
        body += f"{ts},{label}\r\n"
    return body


# TIMESTAMP_DERIVED uses the ELF compact form (see shaping._parse_ts_value).
def _ts(days_ago: float, base: datetime | None = None) -> str:
    anchor = base or datetime(2026, 6, 15, tzinfo=UTC)
    dt = anchor - timedelta(days=days_ago)
    return dt.strftime("%Y%m%d%H%M%S.000")


# ---------------------------------------------------------------------------
# parse_backfill_date (unchanged skeleton behavior — smoke test only)
# ---------------------------------------------------------------------------


def test_parse_backfill_date_returns_utc_midnight() -> None:
    dt = parse_backfill_date("2026-06-01")
    assert dt == datetime(2026, 6, 1, tzinfo=UTC)


def test_parse_backfill_date_rejects_bad_format() -> None:
    with pytest.raises(ValueError, match="invalid date"):
        parse_backfill_date("06/01/2026")


# ---------------------------------------------------------------------------
# Ordering: oldest -> newest, within a file and across files
# ---------------------------------------------------------------------------


@respx.mock
async def test_rows_pushed_oldest_to_newest_within_a_file(tmp_path, private_key_pem: str) -> None:
    """CSV rows arrive out of order; the label-mode strategy sorts by true event time."""
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    csv = _csv([(_ts(0.5), "third"), (_ts(0.9), "first"), (_ts(0.7), "second")])
    _mock_downloads({"f1": csv})
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=2,
    )

    assert code == 0
    values = _all_values(_decode_pushes(route))
    labels_in_order = [_row_label(v[1]) for v in values]  # type: ignore[arg-type]
    assert labels_in_order == ["first", "second", "third"]
    timestamps = [int(v[0]) for v in values]
    assert timestamps == sorted(timestamps)


@respx.mock
async def test_files_pushed_oldest_file_first(tmp_path, private_key_pem: str) -> None:
    """Files are listed oldest CreatedDate first, and pushed (as separate batches,
    one per file) in that order."""
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    older = _file("older", "Login", _created(2))
    newer = _file("newer", "Login", _created(1))
    _mock_list_files({"Login": [newer, older]})  # deliberately out of order in the fixture
    _mock_downloads(
        {
            "older": _csv([(_ts(2), "old-row")]),
            "newer": _csv([(_ts(1), "new-row")]),
        }
    )
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=2,
    )

    assert code == 0
    bodies = _decode_pushes(route)
    assert len(bodies) == 2
    first_labels = [_row_label(v[1]) for v in bodies[0]["streams"][0]["values"]]  # type: ignore[index]
    second_labels = [_row_label(v[1]) for v in bodies[1]["streams"][0]["values"]]  # type: ignore[index]
    assert first_labels == ["old-row"]
    assert second_labels == ["new-row"]


# ---------------------------------------------------------------------------
# Backfill label vs --ingest-timestamps
# ---------------------------------------------------------------------------


@respx.mock
async def test_default_strategy_adds_backfill_label_and_true_timestamps(
    tmp_path, private_key_pem: str
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    row_ts = _ts(1)
    _mock_downloads({"f1": _csv([(row_ts, "row")])})
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    body = _decode_pushes(route)[0]
    stream = body["streams"][0]  # type: ignore[index]
    assert stream["stream"]["backfill"] == "true"  # type: ignore[index]
    assert stream["stream"]["source"] == "eventlogfile"  # type: ignore[index]
    assert stream["stream"]["event_type"] == "Login"  # type: ignore[index]
    pushed_ts_ns = int(stream["values"][0][0])  # type: ignore[index]
    expected = datetime.strptime(row_ts, "%Y%m%d%H%M%S.%f").replace(tzinfo=UTC)
    assert abs(pushed_ts_ns - int(expected.timestamp() * 1_000_000_000)) < 1_000_000_000


@respx.mock
async def test_ingest_timestamps_mode_preserves_event_time_no_backfill_label_monotonic(
    tmp_path, private_key_pem: str
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    # Rows deliberately out of true-time order; ingest-timestamps mode ignores
    # that and just ticks forward in emission order.
    _mock_downloads({"f1": _csv([(_ts(0.9), "a"), (_ts(0.5), "b")])})
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    before = datetime.now(UTC)
    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=True,
        concurrency=1,
    )
    after = datetime.now(UTC)

    assert code == 0
    body = _decode_pushes(route)[0]
    stream = body["streams"][0]  # type: ignore[index]
    assert "backfill" not in stream["stream"]  # type: ignore[index]
    values = stream["values"]  # type: ignore[index]
    ts_values = [int(v[0]) for v in values]
    assert ts_values == sorted(ts_values)
    assert len(set(ts_values)) == len(ts_values)  # strictly increasing, no ties
    for ts_ns in ts_values:
        ts_dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=UTC)
        assert before - timedelta(seconds=5) <= ts_dt <= after + timedelta(seconds=5)
    event_times = [v[2]["event_time"] for v in values]
    assert event_times[0] != event_times[1]


# ---------------------------------------------------------------------------
# Static labels from cfg.sink.loki.labels
# ---------------------------------------------------------------------------


@respx.mock
async def test_static_sink_labels_are_merged_onto_every_stream(
    tmp_path, private_key_pem: str
) -> None:
    cfg = _make_cfg(
        tmp_path, private_key_pem, sink={"loki": {"labels": {"environment": "bf-test"}}}
    )
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    _mock_downloads({"f1": _csv([(_ts(1), "row")])})
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    body = _decode_pushes(route)[0]
    assert body["streams"][0]["stream"]["environment"] == "bf-test"  # type: ignore[index]


# ---------------------------------------------------------------------------
# Resume from checkpoint + separate state file
# ---------------------------------------------------------------------------


@respx.mock
async def test_resume_skips_already_done_files_and_uses_separate_state_file(
    tmp_path, private_key_pem: str
) -> None:
    state_path = tmp_path / "state.json"
    state_path.write_text('{"eventlogfile:Login": "sentinel-daemon-checkpoint"}')
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    older = _file("older", "Login", _created(2))
    newer = _file("newer", "Login", _created(1))
    _mock_list_files({"Login": [older, newer]})
    download_calls = _mock_downloads(
        {
            "older": _csv([(_ts(2), "old-row")]),
            "newer": _csv([(_ts(1), "new-row")]),
        }
    )
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    since = datetime(2026, 6, 1, tzinfo=UTC)
    code1 = await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=2,
    )
    assert code1 == 0
    assert download_calls == {"older": 1, "newer": 1}

    # A second run against the SAME config/state must not re-download either file.
    code2 = await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=2,
    )
    assert code2 == 0
    assert download_calls == {"older": 1, "newer": 1}

    # The daemon's own state file (separate, sibling path) is completely untouched.
    assert state_path.read_text() == '{"eventlogfile:Login": "sentinel-daemon-checkpoint"}'
    backfill_state_path = tmp_path / "state-backfill.json"
    assert backfill_state_path.exists()
    saved = json.loads(json.loads(backfill_state_path.read_text())["backfill:Daily:Login"])
    assert saved["last_created"] == newer.created_date
    assert saved["done_ids"] == ["newer"]


@respx.mock
async def test_resume_reprocesses_new_file_at_same_created_date_boundary(
    tmp_path, private_key_pem: str
) -> None:
    """A file id NOT in done_ids, even at the exact watermark CreatedDate, must
    still be processed (e.g. a late-arriving sibling file)."""
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    boundary_created = _created(1)
    f1 = _file("f1", "Login", boundary_created)
    _mock_list_files({"Login": [f1]})
    download_calls = _mock_downloads({"f1": _csv([(_ts(1), "row1")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))
    since = datetime(2026, 6, 1, tzinfo=UTC)
    await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )
    assert download_calls == {"f1": 1}

    # A sibling file lands later at the SAME CreatedDate boundary.
    f2 = _file("f2", "Login", boundary_created)
    _mock_list_files({"Login": [f1, f2]})
    download_calls2 = _mock_downloads(
        {"f1": _csv([(_ts(1), "row1")]), "f2": _csv([(_ts(1), "row2")])}
    )

    await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )
    assert download_calls2["f2"] == 1
    assert download_calls2["f1"] == 0  # f1 already in done_ids at this boundary


# ---------------------------------------------------------------------------
# Pagination: the backfill window can exceed one SOQL LIMIT page
# ---------------------------------------------------------------------------


@respx.mock
async def test_pages_through_multiple_listing_pages(tmp_path, private_key_pem: str) -> None:
    # page_size=1 is a genuinely unsolvable degenerate case for ANY inclusive
    # (>=) + client-side-dedup listing scheme: a single already-done file at
    # the boundary fills the whole page forever. page_size=2 (< the 3 total
    # files) still forces multiple listing pages without that pathology.
    cfg = _make_cfg(
        tmp_path,
        private_key_pem,
        sources={"eventlogfile": {"enabled": True, "event_types": ["Login"], "page_size": 2}},
    )
    _mock_token()
    f1 = _file("f1", "Login", _created(3))
    f2 = _file("f2", "Login", _created(2))
    f3 = _file("f3", "Login", _created(1))
    requests = _mock_list_files({"Login": [f1, f2, f3]})
    download_calls = _mock_downloads(
        {
            "f1": _csv([(_ts(3), "r1")]),
            "f2": _csv([(_ts(2), "r2")]),
            "f3": _csv([(_ts(1), "r3")]),
        }
    )
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    assert download_calls == {"f1": 1, "f2": 1, "f3": 1}
    # 3 files with page_size=1 needs at least 3 listing calls (one per page) plus
    # the final short/empty page that signals exhaustion.
    assert len(requests) >= 3


# ---------------------------------------------------------------------------
# --until cutoff
# ---------------------------------------------------------------------------


@respx.mock
async def test_until_excludes_files_created_on_or_after_cutoff(
    tmp_path, private_key_pem: str
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    in_window = _file("in", "Login", _created(5))
    out_of_window = _file("out", "Login", _created(1))
    _mock_list_files({"Login": [in_window, out_of_window]})
    download_calls = _mock_downloads({"in": _csv([(_ts(5), "r")]), "out": _csv([(_ts(1), "r")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=datetime(2026, 6, 15, tzinfo=UTC) - timedelta(days=3),
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    assert download_calls == {"in": 1, "out": 0}


# ---------------------------------------------------------------------------
# Retention / OOO guard warnings
# ---------------------------------------------------------------------------


@respx.mock
async def test_warns_when_since_beyond_elf_retention(
    tmp_path, private_key_pem: str, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    _mock_list_files({"Login": []})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime.now(UTC) - timedelta(days=45),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=True,
        concurrency=1,
    )

    assert code == 0
    err = capsys.readouterr().err
    assert "beyond ELF retention" in err


@respx.mock
async def test_warns_about_loki_ooo_window_in_label_mode_only(
    tmp_path, private_key_pem: str, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    _mock_list_files({"Login": []})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    since = datetime.now(UTC) - timedelta(days=10)

    code = await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "reject_old_samples_max_age" in err

    code = await run_backfill(
        cfg,
        since=since,
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=True,
        concurrency=1,
    )
    assert code == 0
    err = capsys.readouterr().err
    assert "reject_old_samples_max_age" not in err


# ---------------------------------------------------------------------------
# Permanent Loki errors: rows counted dropped, run continues
# ---------------------------------------------------------------------------


@respx.mock
async def test_permanent_sink_error_counts_rows_dropped_and_continues(
    tmp_path, private_key_pem: str, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    # Exactly one row -> one entry -> an unsplittable single-entry batch, so a
    # 400 propagates as PermanentSinkError straight to our layer.
    _mock_downloads({"f1": _csv([(_ts(1), "row")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(400, text="bad request"))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    out = capsys.readouterr().out
    assert "rows_dropped=1" in out
    assert "rows_pushed=0" in out


# ---------------------------------------------------------------------------
# Retryable Loki errors: our own backoff loop, then success or abort
# ---------------------------------------------------------------------------


@respx.mock
async def test_retryable_error_then_success(
    tmp_path, private_key_pem: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
    monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)
    monkeypatch.setattr(backfill_module, "_RETRY_BACKOFF_BASE", 0.0)
    monkeypatch.setattr(backfill_module, "_RETRY_BACKOFF_MAX", 0.0)

    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    _mock_downloads({"f1": _csv([(_ts(1), "row1"), (_ts(1), "row2")])})
    route = respx.post(LOKI_URL).mock(
        side_effect=[httpx.Response(500, text="boom"), httpx.Response(204)]
    )

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    assert route.call_count == 2


@respx.mock
async def test_exit_code_1_after_exhausting_consecutive_push_failures(
    tmp_path,
    private_key_pem: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 1)
    monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
    monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)
    monkeypatch.setattr(backfill_module, "_RETRY_BACKOFF_BASE", 0.0)
    monkeypatch.setattr(backfill_module, "_RETRY_BACKOFF_MAX", 0.0)
    monkeypatch.setattr(backfill_module, "_MAX_CONSECUTIVE_PUSH_FAILURES", 2)

    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    _mock_downloads({"f1": _csv([(_ts(1), "row1"), (_ts(1), "row2")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(500, text="boom"))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 1
    err = capsys.readouterr().err
    assert "aborting after 2 consecutive Loki push failures" in err


# ---------------------------------------------------------------------------
# Event type resolution
# ---------------------------------------------------------------------------


@respx.mock
async def test_explicit_event_types_override_config(tmp_path, private_key_pem: str) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Report", _created(1))
    requests = _mock_list_files({"Report": [f1]})
    _mock_downloads({"f1": _csv([(_ts(1), "row")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=["Report"],
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    assert any(
        "EventType='Report'" in parse_qs(urlparse(str(r.url)).query)["q"][0] for r in requests
    )


@respx.mock
async def test_wildcard_only_config_triggers_discovery(tmp_path, private_key_pem: str) -> None:
    cfg = _make_cfg(
        tmp_path, private_key_pem, sources={"eventlogfile": {"enabled": True, "event_types": ["*"]}}
    )
    _mock_token()
    f1 = _file("Login", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    _mock_downloads({"Login": _csv([(_ts(1), "row")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0


# ---------------------------------------------------------------------------
# Summary output
# ---------------------------------------------------------------------------


@respx.mock
async def test_summary_reports_files_rows_bytes_api_calls(
    tmp_path, private_key_pem: str, capsys: pytest.CaptureFixture[str]
) -> None:
    cfg = _make_cfg(tmp_path, private_key_pem)
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    _mock_downloads({"f1": _csv([(_ts(1), "row1"), (_ts(1), "row2")])})
    respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=1,
    )

    assert code == 0
    out = capsys.readouterr().out
    assert "files=1" in out
    assert "rows_pushed=2" in out
    assert "rows_dropped=0" in out
    assert "api_calls=1" in out
    assert re.search(r"bytes_pushed=\d+", out)
    assert re.search(r"elapsed=\d+\.\d+s", out)


# ---------------------------------------------------------------------------
# Transforms wiring: backfill applies sources.eventlogfile.transforms
# ---------------------------------------------------------------------------


@respx.mock
async def test_backfill_applies_configured_transforms(tmp_path, private_key_pem: str) -> None:
    """The same redaction/filter rules the daemon applies also govern backfill:
    a drop_row rule removes matching rows, and a hash rule pseudonymises the
    ROW column in what gets pushed."""
    cfg = _make_cfg(
        tmp_path,
        private_key_pem,
        sources={
            "transform_salt": "pepper",
            "eventlogfile": {
                "transforms": [
                    {"action": "drop_row", "match": {"ROW": "secret-*"}},
                    {"action": "hash", "fields": ["ROW"]},
                ],
            },
        },
    )
    _mock_token()
    f1 = _file("f1", "Login", _created(1))
    _mock_list_files({"Login": [f1]})
    csv = _csv([(_ts(0.9), "keep-me"), (_ts(0.7), "secret-row")])
    _mock_downloads({"f1": csv})
    route = respx.post(LOKI_URL).mock(return_value=httpx.Response(204))

    code = await run_backfill(
        cfg,
        since=datetime(2026, 6, 1, tzinfo=UTC),
        until=None,
        event_types=None,
        interval="Daily",
        ingest_timestamps=False,
        concurrency=2,
    )

    assert code == 0
    values = _all_values(_decode_pushes(route))
    rows = [_row_label(v[1]) for v in values]  # type: ignore[arg-type]
    assert len(rows) == 1  # secret-row dropped by drop_row
    hashed = hashlib.sha256(b"pepperkeep-me").hexdigest()[:16]
    assert rows == [hashed]
