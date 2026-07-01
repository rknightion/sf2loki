"""Label validation and rendering for Loki LogQL label sets."""

from __future__ import annotations

from collections.abc import Mapping

ALLOWED_LABELS: frozenset[str] = frozenset(
    {"job", "service_name", "source", "event_type", "sf_org_id", "environment"}
)

# Per-entry identity labels set by the sources; a static operator override
# (sink.loki.labels) would be merged over every entry and collapse all stream
# separation, so these may never appear in the static label set.
RESERVED_STATIC_LABELS: frozenset[str] = frozenset({"source", "event_type"})


class LabelGuardError(ValueError):
    """Raised when a label key is not in the allowed set."""


def guard_labels(
    labels: Mapping[str, str],
    allowed: frozenset[str] = ALLOWED_LABELS,
) -> None:
    """Fail fast if any key in *labels* is not in *allowed*.

    Raises :class:`LabelGuardError` listing all offending keys.
    """
    bad = sorted(k for k in labels if k not in allowed)
    if bad:
        raise LabelGuardError(f"Disallowed label keys: {', '.join(bad)}")


def guard_static_labels(
    labels: Mapping[str, str],
    allowed: frozenset[str] = ALLOWED_LABELS,
    reserved: frozenset[str] = RESERVED_STATIC_LABELS,
) -> None:
    """Validate operator-supplied static labels (``sink.loki.labels``).

    On top of :func:`guard_labels`, rejects per-entry identity keys
    (``source``/``event_type``): merged as static labels they would override
    every entry's own value and collapse all stream separation.
    """
    guard_labels(labels, allowed)
    bad = sorted(k for k in labels if k in reserved)
    if bad:
        raise LabelGuardError(
            f"Reserved per-entry label keys cannot be set as static sink.loki.labels "
            f"(they would clobber every entry's own value): {', '.join(bad)}"
        )


def _escape_value(value: str) -> str:
    """Escape special characters in a Loki label value."""
    # Order matters: escape backslashes first, then quotes, then newlines.
    value = value.replace("\\", "\\\\")
    value = value.replace('"', '\\"')
    value = value.replace("\n", "\\n")
    return value


def render_labels(labels: Mapping[str, str]) -> str:
    """Render *labels* as a LogQL label set string.

    Keys are sorted ascending; values are escaped (backslash, double-quote,
    newline). An empty mapping renders as ``"{}"``.
    """
    if not labels:
        return "{}"
    pairs = ",".join(f'{k}="{_escape_value(v)}"' for k, v in sorted(labels.items()))
    return "{" + pairs + "}"
