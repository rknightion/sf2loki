"""Shared event-shaping helpers used by every source.

Kept source-agnostic (and out of the Loki sink) so Pub/Sub and SOQL sources
produce identically-shaped entries.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime


def route_fields(
    payload: Mapping[str, object], sm_fields: Sequence[str]
) -> tuple[str, dict[str, str]]:
    """Split a decoded event into a log line and structured metadata.

    The full payload becomes a canonical (sorted-key) JSON line. Any
    ``sm_fields`` that are present and non-null are promoted to structured
    metadata (stringified). High-cardinality fields therefore never become
    stream labels.
    """
    sm = {k: str(payload[k]) for k in sm_fields if payload.get(k) is not None}
    line = json.dumps(payload, sort_keys=True, default=str)
    return line, sm


def _parse_ts_value(value: object) -> datetime | None:
    """Parse a single timestamp value, or return None if it can't be parsed.

    Accepts epoch-millis (int/float), ISO-8601 strings (with or without a 'Z'
    or offset), and the EventLogFile compact ``yyyyMMddHHmmss.SSS`` format.
    Any naive result is coerced to UTC so callers always get an aware datetime.
    """
    if isinstance(value, int | float):
        return datetime.fromtimestamp(value / 1000, tz=UTC)
    text = str(value)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            # ELF legacy TIMESTAMP column, e.g. "20231231130000.000" (UTC).
            dt = datetime.strptime(text, "%Y%m%d%H%M%S.%f")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def extract_timestamp(
    payload: Mapping[str, object],
    field_names: Sequence[str] = ("EventDate", "CreatedDate"),
) -> datetime:
    """Best-effort event occurrence time, always timezone-aware (UTC).

    Tries each name in *field_names* in order (default ``EventDate`` then
    ``CreatedDate``; EventLogFile passes ``TIMESTAMP_DERIVED``/``TIMESTAMP``),
    accepting epoch-millis, ISO-8601, or the ELF compact format. Falls back to
    ingest time when no field yields a parseable value.
    """
    for field_name in field_names:
        value = payload.get(field_name)
        if value is None:
            continue
        parsed = _parse_ts_value(value)
        if parsed is not None:
            return parsed
    return datetime.now(UTC)
