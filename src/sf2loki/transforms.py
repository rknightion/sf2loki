"""Declarative PII redaction / row-filter transforms (issue #27).

A :class:`TransformPipeline` is compiled once per source from that source's
``transforms: list[TransformRule]`` config and applied to every decoded payload
at the source's decode boundary — BEFORE field routing, label promotion, and
timestamp extraction. Redacting a column therefore also redacts anything derived
from it downstream (structured metadata, the JSON line, a fallback timestamp).

Everything expensive is precompiled at :func:`compile_rules` time (regexes, glob
matchers, resolved rule names, the bound ``rows_filtered`` counter). The hot path
(:meth:`TransformPipeline.apply`) does only the dict work each row needs; an
empty rule list compiles to a no-op pipeline whose ``apply`` is the identity.

``apply`` mutates the payload IN PLACE (the source owns the dict at that point)
and returns it, or returns ``None`` when a ``drop_row`` rule matched — the
source then drops the row/event but still advances its checkpoint.
"""

from __future__ import annotations

import fnmatch
import hashlib
import re
from typing import TYPE_CHECKING, Any

from sf2loki.config import TransformRule

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from sf2loki.obs.metrics import Metrics

# Payload values may be non-str (ints, nested dicts from Avro); actions coerce
# with str() where they need text, so the value type is intentionally Any.
Payload = dict[str, Any]

# Strict dotted-quad IPv4 (each octet 0-255) so a mask rule only /24-truncates
# real IPv4 literals and falls through to "***" for anything else.
_IPV4_RE = re.compile(
    r"^(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"
    r"(?:\.(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)){3}$"
)


def _mask_value(value: object) -> str:
    """Format-aware mask of a single value.

    - contains ``@`` → treat as an email: keep the domain, mask the local part
      (``alice@example.com`` → ``***@example.com``);
    - a dotted-quad IPv4 → truncate to /24 (``203.0.113.7`` → ``203.0.113.x``);
    - anything else → ``***``.
    """
    text = str(value)
    if "@" in text:
        _, _, domain = text.partition("@")
        return f"***@{domain}"
    if _IPV4_RE.match(text):
        first, second, third, _ = text.split(".")
        return f"{first}.{second}.{third}.x"
    return "***"


def _make_hash(fields: Sequence[str], salt: str) -> Callable[[Payload], bool]:
    keys = tuple(fields)

    def _apply(payload: Payload) -> bool:
        for key in keys:
            value = payload.get(key)
            if value is not None:  # leave absent/None fields untouched
                payload[key] = hashlib.sha256(f"{salt}{value}".encode()).hexdigest()[:16]
        return False

    return _apply


def _make_mask(fields: Sequence[str]) -> Callable[[Payload], bool]:
    keys = tuple(fields)

    def _apply(payload: Payload) -> bool:
        for key in keys:
            value = payload.get(key)
            if value is not None:
                payload[key] = _mask_value(value)
        return False

    return _apply


def _make_drop_field(fields: Sequence[str]) -> Callable[[Payload], bool]:
    keys = tuple(fields)

    def _apply(payload: Payload) -> bool:
        for key in keys:
            payload.pop(key, None)
        return False

    return _apply


def _make_regex(
    fields: Sequence[str], pattern: re.Pattern[str], replacement: str
) -> Callable[[Payload], bool]:
    keys = tuple(fields)

    def _apply(payload: Payload) -> bool:
        for key in keys:
            value = payload.get(key)
            if value is not None:
                payload[key] = pattern.sub(replacement, str(value))
        return False

    return _apply


def _make_drop_row(
    match: dict[str, str], rule_name: str, source: str, metrics: Metrics | None
) -> Callable[[Payload], bool]:
    matchers = tuple(match.items())
    # Bind the rows_filtered counter once at compile time (cheap per-row inc()).
    counter = (
        metrics.rows_filtered.labels(source=source, rule=rule_name) if metrics is not None else None
    )

    def _apply(payload: Payload) -> bool:
        # EVERY (field, glob) must match str(payload.get(field, "")); equality is
        # the degenerate glob. A non-match short-circuits (keep the row).
        for field, glob in matchers:
            if not fnmatch.fnmatchcase(str(payload.get(field, "")), glob):
                return False
        if counter is not None:
            counter.inc()
        return True

    return _apply


class TransformPipeline:
    """Precompiled sequence of transform rules applied to each payload."""

    __slots__ = ("_rules",)

    def __init__(self, rules: list[Callable[[Payload], bool]]) -> None:
        self._rules = rules

    def __bool__(self) -> bool:
        """True when there is at least one rule (sources may skip a no-op pipeline)."""
        return bool(self._rules)

    def apply(self, payload: Payload) -> Payload | None:
        """Apply every rule in order, mutating *payload* in place.

        Returns the (mutated) payload, or ``None`` when a ``drop_row`` rule
        matched — in which case ``rows_filtered{source, rule}`` was incremented.
        """
        for rule in self._rules:
            if rule(payload):
                return None
        return payload


def compile_rules(
    rules: Sequence[TransformRule],
    *,
    salt: str,
    source: str,
    metrics: Metrics | None = None,
) -> TransformPipeline:
    """Compile *rules* into a :class:`TransformPipeline` for *source*.

    Regexes, glob matchers, resolved rule names (``name`` or ``"<action>-<index>"``)
    and the bound ``rows_filtered`` counter are all resolved here so
    :meth:`TransformPipeline.apply` allocates nothing beyond the dict work.
    """
    compiled: list[Callable[[Payload], bool]] = []
    for index, rule in enumerate(rules):
        name = rule.name or f"{rule.action}-{index}"
        if rule.action == "hash":
            compiled.append(_make_hash(rule.fields, salt))
        elif rule.action == "mask":
            compiled.append(_make_mask(rule.fields))
        elif rule.action == "drop_field":
            compiled.append(_make_drop_field(rule.fields))
        elif rule.action == "regex_replace":
            # config validated the pattern compiles; pattern is non-None here.
            assert rule.pattern is not None
            compiled.append(_make_regex(rule.fields, re.compile(rule.pattern), rule.replacement))
        elif rule.action == "drop_row":
            compiled.append(_make_drop_row(rule.match, name, source, metrics))
    return TransformPipeline(compiled)
