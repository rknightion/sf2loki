"""Fail-fast overlap guard: one event category, one source.

Salesforce exposes the same monitoring activity through up to three channels:
Pub/Sub streaming (e.g. ``/event/LoginEventStream``), stored event objects
queried via SOQL (e.g. ``LoginEvent``), and EventLogFile (e.g. ``Login``).
Ingesting one category from more than one channel double-counts events
(``LoginEventStream`` and ``LoginEvent`` are the *same* records).

This guard normalises each enabled source's identifiers to a canonical
*category* key and refuses to start when one category is fed by more than one
source — mirroring the label-allowlist guard's fail-fast philosophy. The
operator can bypass it with ``sources.allow_overlap: true`` when they have
deliberately accepted the duplication (e.g. relying on Loki to collapse
byte-identical entries).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence

# ELF EventType values whose lowercased name does NOT already match the
# normalised RTEM stem. Most do (e.g. ELF "API" -> "api" matches the
# ApiEventStream/ApiEvent stem "api"), so this map only holds true exceptions.
# Keyed by lowercased ELF EventType -> canonical category.
_ELF_CATEGORY_ALIASES: dict[str, str] = {}

# Suffixes that distinguish a Salesforce channel name from its category stem.
# Ordered longest-first so "EventStream"/"EventStore" win over "Event".
_CHANNEL_SUFFIXES: tuple[str, ...] = ("EventStream", "EventStore", "Event")


class OverlapError(Exception):
    """Raised when one event category is enabled on more than one source."""


def _basename(topic: str) -> str:
    """Return the last path segment of a Pub/Sub topic name."""
    return topic.rstrip("/").rsplit("/", 1)[-1]


def _strip_channel_suffix(name: str) -> str:
    for suffix in _CHANNEL_SUFFIXES:
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def category_of_pubsub(topic: str) -> str:
    """Canonical category for a Pub/Sub topic (``/event/LoginEventStream`` -> ``login``)."""
    return _strip_channel_suffix(_basename(topic)).lower()


def category_of_stored_object(name: str) -> str:
    """Canonical category for a stored event object (``LoginEvent`` -> ``login``)."""
    return _strip_channel_suffix(name).lower()


def category_of_elf(event_type: str) -> str:
    """Canonical category for an ELF EventType (``Login`` -> ``login``, ``API`` -> ``api``)."""
    key = event_type.lower()
    return _ELF_CATEGORY_ALIASES.get(key, key)


def check_overlap(
    *,
    pubsub_topics: Sequence[str] = (),
    stored_objects: Sequence[str] = (),
    elf_event_types: Sequence[str] = (),
    allow_overlap: bool = False,
) -> None:
    """Raise :class:`OverlapError` if a category is fed by more than one source.

    Each argument lists the identifiers for an *enabled* source (empty when the
    source is disabled). When ``allow_overlap`` is true the guard only logs
    nothing and returns — the caller has opted into the duplication.
    """
    if allow_overlap:
        return

    buckets: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for topic in pubsub_topics:
        buckets[category_of_pubsub(topic)].add(("pubsub", topic))
    for obj in stored_objects:
        buckets[category_of_stored_object(obj)].add(("eventlog_objects", obj))
    for et in elf_event_types:
        buckets[category_of_elf(et)].add(("eventlogfile", et))

    collisions = {
        category: members
        for category, members in buckets.items()
        if len({source for source, _ in members}) > 1
    }
    if not collisions:
        return

    lines = [
        f"  category {category!r}: "
        + ", ".join(f"{source}={ident}" for source, ident in sorted(members))
        for category, members in sorted(collisions.items())
    ]
    raise OverlapError(
        "the same event category is enabled on more than one source, which would "
        "ingest duplicate events:\n"
        + "\n".join(lines)
        + "\n\nEnable each category on exactly one source, or set "
        "sources.allow_overlap: true to ingest anyway (relying on Loki to drop "
        "byte-identical duplicates)."
    )
