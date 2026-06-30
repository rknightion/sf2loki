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


def extract_timestamp(payload: Mapping[str, object]) -> datetime:
    """Best-effort event occurrence time.

    Tries ``EventDate`` then ``CreatedDate`` (epoch-millis or ISO-8601),
    falling back to ingest time. Always timezone-aware (UTC).
    """
    for field_name in ("EventDate", "CreatedDate"):
        value = payload.get(field_name)
        if value is None:
            continue
        if isinstance(value, int | float):
            return datetime.fromtimestamp(value / 1000, tz=UTC)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return datetime.now(UTC)
