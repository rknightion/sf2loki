from __future__ import annotations

from datetime import UTC, datetime

from sf2loki.shaping import extract_timestamp, route_fields


def test_route_fields_routes_only_listed_present_keys() -> None:
    line, sm = route_fields(
        {"UserId": "005", "SourceIp": "1.2.3.4", "n": 1},
        ["UserId", "Missing"],
    )
    assert sm == {"UserId": "005"}  # Missing absent; SourceIp not promoted
    # canonical sorted-key JSON: SourceIp sorts before UserId
    assert line.index('"SourceIp"') < line.index('"UserId"')


def test_route_fields_skips_null_sm_values() -> None:
    _, sm = route_fields({"UserId": None, "Ip": "x"}, ["UserId", "Ip"])
    assert sm == {"Ip": "x"}


def test_extract_timestamp_epoch_millis() -> None:
    assert extract_timestamp({"EventDate": 1_700_000_000_000}) == datetime.fromtimestamp(
        1_700_000_000, tz=UTC
    )


def test_extract_timestamp_iso8601() -> None:
    ts = extract_timestamp({"CreatedDate": "2026-06-30T12:00:00Z"})
    assert ts.year == 2026 and ts.tzinfo is not None


def test_extract_timestamp_prefers_eventdate() -> None:
    ts = extract_timestamp(
        {"EventDate": "2026-01-01T00:00:00Z", "CreatedDate": "2020-01-01T00:00:00Z"}
    )
    assert ts.year == 2026


def test_extract_timestamp_falls_back_to_now() -> None:
    assert extract_timestamp({"foo": "bar"}).tzinfo is not None
