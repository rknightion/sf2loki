from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from sf2loki.shaping import (
    cap_line,
    derive_level,
    extract_timestamp,
    extract_timestamp_checked,
    promote_labels,
    route_fields,
)


def test_route_fields_routes_only_listed_present_keys() -> None:
    line, sm = route_fields(
        {"UserId": "005", "SourceIp": "1.2.3.4", "n": 1},
        ["UserId", "Missing"],
    )
    # Missing absent; SourceIp not promoted; level always injected.
    assert sm == {"UserId": "005", "level": "info"}
    # canonical sorted-key JSON: SourceIp sorts before UserId
    assert line.index('"SourceIp"') < line.index('"UserId"')


def test_route_fields_skips_null_sm_values() -> None:
    _, sm = route_fields({"UserId": None, "Ip": "x"}, ["UserId", "Ip"])
    assert sm == {"Ip": "x", "level": "info"}


def test_route_fields_injects_derived_level() -> None:
    # A failed REST call: Salesforce's own REQUEST_STATUS drives the level.
    _, sm = route_fields({"REQUEST_STATUS": "F"}, [])
    assert sm == {"level": "warn"}
    # level is metadata only — never promoted into the log line body itself.
    line, _ = route_fields({"REQUEST_STATUS": "F"}, [])
    assert "level" not in line


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        # No signal at all -> info (the common case).
        ({}, "info"),
        ({"DATABASE_STATEMENTS": "3"}, "info"),
        # HTTP status code (ApiTotalUsage / RestApi STATUS_CODE).
        ({"STATUS_CODE": "200"}, "info"),
        ({"STATUS_CODE": "302"}, "info"),
        ({"STATUS_CODE": "404"}, "warn"),
        ({"STATUS_CODE": "429"}, "warn"),
        ({"STATUS_CODE": "500"}, "error"),
        ({"STATUS_CODE": "503"}, "error"),
        ({"STATUS_CODE": ""}, "info"),  # blank -> ignored
        ({"STATUS_CODE": "n/a"}, "info"),  # unparseable -> ignored
        # REQUEST_STATUS (S/F).
        ({"REQUEST_STATUS": "S"}, "info"),
        ({"REQUEST_STATUS": "F"}, "warn"),
        ({"REQUEST_STATUS": ""}, "info"),  # blank (seen on Login/OneCommerce)
        # Explicit exceptions/errors -> error, and they win over a 2xx code.
        ({"EXCEPTION_MESSAGE": "NullPointer", "STATUS_CODE": "200"}, "error"),
        ({"EXCEPTION_TYPE": "System.LimitException"}, "error"),
        ({"ERROR_MESSAGE": "boom"}, "error"),
        ({"ERROR_CODE": "APEX_ERROR"}, "error"),
        ({"ERROR_CODE": ""}, "info"),  # blank -> not an error
        ({"ERROR_CODE": "0"}, "info"),  # zero -> not an error
        # Login (ELF LOGIN_STATUS).
        ({"LOGIN_STATUS": "LOGIN_NO_ERROR"}, "info"),
        ({"LOGIN_STATUS": "LOGIN_ERROR_INVALID_PASSWORD"}, "warn"),
        # OneCommerce OPERATION_STATUS.
        ({"OPERATION_STATUS": "Success"}, "info"),
        ({"OPERATION_STATUS": "Failed"}, "warn"),
        # Streaming LoginEventStream.Status (free text).
        ({"Status": "Success"}, "info"),
        ({"Status": "Invalid password"}, "warn"),
        # Fields that look status-ish but are NOT severity must be ignored.
        ({"SessionLevel": "HIGH_ASSURANCE"}, "info"),
        ({"SESSION_LEVEL": "1"}, "info"),
        ({"LoginType": "Remote Access 2.0"}, "info"),
        ({"UserType": "Guest"}, "info"),
        ({"COUNTRY_CODE": "IN"}, "info"),
    ],
)
def test_derive_level(payload: dict[str, object], expected: str) -> None:
    assert derive_level(payload) == expected


def test_derive_level_priority_error_beats_status() -> None:
    # A 5xx with a success-ish REQUEST_STATUS is still an error.
    assert derive_level({"STATUS_CODE": "500", "REQUEST_STATUS": "S"}) == "error"


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


# ---------------------------------------------------------------------------
# Deterministic timestamp fallback (issue #20): a stable fallback beats
# now(UTC) so byte-identical replays dedup in Loki; callers can count usage.


def test_extract_timestamp_checked_parseable_is_not_fallback() -> None:
    ts, used_fallback = extract_timestamp_checked(
        {"EventDate": "2026-06-30T12:00:00Z"},
        fallback=datetime.now(UTC),
    )
    assert used_fallback is False
    assert ts == datetime(2026, 6, 30, 12, 0, 0, tzinfo=UTC)


def test_extract_timestamp_checked_uses_recent_fallback() -> None:
    fb = datetime.now(UTC) - timedelta(minutes=30)
    ts, used_fallback = extract_timestamp_checked({"EventDate": "garbage"}, fallback=fb)
    assert used_fallback is True
    assert ts == fb


def test_extract_timestamp_checked_without_fallback_uses_now() -> None:
    before = datetime.now(UTC)
    ts, used_fallback = extract_timestamp_checked({"foo": "bar"})
    assert used_fallback is True
    assert before <= ts <= datetime.now(UTC)


def test_extract_timestamp_checked_old_fallback_clamped_near_now() -> None:
    # A fallback >1h old would be rejected by Loki's out-of-order guard (and the
    # whole row dropped via the 400 path) — it must be clamped near now instead,
    # while still reporting used_fallback=True so the metric counts it.
    fb = datetime.now(UTC) - timedelta(hours=3)
    ts, used_fallback = extract_timestamp_checked({}, fallback=fb)
    assert used_fallback is True
    assert ts > fb  # clamped forward, not the stale fallback
    assert timedelta(0) <= datetime.now(UTC) - ts < timedelta(minutes=10)


def test_extract_timestamp_accepts_fallback_kwarg() -> None:
    # Back-compat wrapper: same fallback semantics, plain datetime return.
    fb = datetime.now(UTC) - timedelta(minutes=5)
    assert extract_timestamp({}, fallback=fb) == fb
