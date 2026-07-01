"""Label validation and rendering for Loki LogQL label sets."""

from __future__ import annotations

from collections.abc import Mapping

ALLOWED_LABELS: frozenset[str] = frozenset(
    {"job", "service_name", "source", "event_type", "sf_org_id", "environment"}
)


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
