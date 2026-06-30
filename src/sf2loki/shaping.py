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


def promote_labels(payload: Mapping[str, object], label_fields: Sequence[str]) -> dict[str, str]:
    """Promote selected payload fields to (stringified) stream labels.

    Only fields present and non-null are promoted. Mirrors :func:`route_fields`'s
    structured-metadata selection, but the result is intended for stream labels
    — so callers MUST restrict *label_fields* to low-cardinality columns to avoid
    a Loki stream-cardinality explosion.
    """
    return {k: str(payload[k]) for k in label_fields if payload.get(k) is not None}


_TRUNCATION_MARKER = "…[truncated, original {orig} bytes]"


def cap_line(line: str, max_bytes: int) -> tuple[str, bool]:
    """Cap *line* to at most *max_bytes* UTF-8 bytes, returning (line, truncated).

    A non-positive *max_bytes* disables the cap. When the line fits it is
    returned unchanged. Otherwise it is truncated on a UTF-8 character boundary
    (never splitting a multibyte char) and a marker noting the original size is
    appended; the marker is included in the byte budget so the result never
    exceeds *max_bytes*. Idempotent: re-capping an already-capped line is a
    no-op. If the budget is too small to fit even the marker, the line is hard
    cut to *max_bytes* with no marker.
    """
    if max_bytes <= 0:
        return line, False
    encoded = line.encode("utf-8")
    if len(encoded) <= max_bytes:
        return line, False

    marker = _TRUNCATION_MARKER.format(orig=len(encoded))
    marker_bytes = len(marker.encode("utf-8"))
    budget = max_bytes - marker_bytes
    if budget <= 0:
        # Marker won't fit; hard-cut to the limit on a char boundary, no marker.
        return encoded[:max_bytes].decode("utf-8", errors="ignore"), True
    head = encoded[:budget].decode("utf-8", errors="ignore")
    return head + marker, True


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
