from __future__ import annotations

from datetime import UTC, datetime

from sf2loki.shaping import cap_line, extract_timestamp, promote_labels, route_fields


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


def test_promote_labels_present_and_stringified() -> None:
    labels = promote_labels({"API_TYPE": "REST", "COUNT": 3, "X": "y"}, ["API_TYPE", "COUNT"])
    assert labels == {"API_TYPE": "REST", "COUNT": "3"}


def test_promote_labels_skips_absent_and_null() -> None:
    labels = promote_labels({"A": None, "B": "v"}, ["A", "B", "MISSING"])
    assert labels == {"B": "v"}


def test_promote_labels_empty_when_no_fields() -> None:
    assert promote_labels({"A": "v"}, []) == {}


def test_cap_line_under_limit_untouched() -> None:
    line, truncated = cap_line("hello", 100)
    assert line == "hello"
    assert truncated is False


def test_cap_line_disabled_when_zero() -> None:
    big = "x" * 1000
    line, truncated = cap_line(big, 0)
    assert line == big
    assert truncated is False


def test_cap_line_truncates_with_marker_under_limit() -> None:
    big = "x" * 1000
    line, truncated = cap_line(big, 100)
    assert truncated is True
    assert len(line.encode("utf-8")) <= 100
    assert "truncated" in line


def test_cap_line_utf8_multibyte_boundary_safe() -> None:
    # Each '€' is 3 UTF-8 bytes; cutting at a byte budget must not split a char.
    big = "€" * 200  # 600 bytes
    line, truncated = cap_line(big, 100)
    assert truncated is True
    assert len(line.encode("utf-8")) <= 100
    # No replacement char / no UnicodeDecodeError implies a clean boundary.
    line.encode("utf-8").decode("utf-8")


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


def test_extract_timestamp_naive_iso_coerced_to_utc() -> None:
    # No 'Z' and no offset -> fromisoformat yields a naive datetime; must be
    # coerced to UTC, never returned naive (would crash Pipeline lag calc).
    ts = extract_timestamp({"EventDate": "2026-06-30T12:00:00"})
    assert ts.tzinfo is not None
    assert ts == datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)


def test_extract_timestamp_custom_field_names() -> None:
    ts = extract_timestamp(
        {"TIMESTAMP_DERIVED": "2026-06-30T12:00:00.000Z"},
        field_names=("TIMESTAMP_DERIVED", "TIMESTAMP"),
    )
    assert ts.year == 2026 and ts.tzinfo is not None


def test_extract_timestamp_elf_compact_format() -> None:
    # ELF legacy TIMESTAMP column: yyyyMMddHHmmss.SSS, assumed UTC.
    ts = extract_timestamp(
        {"TIMESTAMP": "20231231130000.000"},
        field_names=("TIMESTAMP_DERIVED", "TIMESTAMP"),
    )
    assert ts == datetime(2023, 12, 31, 13, 0, 0, tzinfo=UTC)


def test_extract_timestamp_unparseable_string_falls_through() -> None:
    # An unparseable candidate must not crash; falls through to now() (aware).
    ts = extract_timestamp({"EventDate": "not-a-timestamp"})
    assert ts.tzinfo is not None
