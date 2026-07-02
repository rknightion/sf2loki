"""Per-org checkpoint namespacing with a transparent legacy-migration shim.

Multi-org deployments prefix every checkpoint key with ``org=<name>:`` so two
orgs sharing one state store never collide (a ``pubsub:/event/LoginEventStream``
key for org ``prod`` and org ``emea`` are distinct streams). The prefix is
deliberately distinct from every existing namespace (``pubsub:``,
``eventlogfile:``, ``eventlog_objects:``, ``backfill:``, ``egress:``) so it can
never shadow one.

:class:`OrgCheckpointView` wraps the shared store and is handed to a source's
``events()`` so the source's own ``state.load()`` calls transparently read the
prefixed key. Commits go through the pipeline (which commits the already-prefixed
``entry.checkpoint.key`` rewritten by :class:`~sf2loki.sources.org_adapter.OrgSource`),
but the view also prefixes any direct ``commit()`` for completeness. The FIRST
org additionally falls back to the unprefixed legacy key on load, so a deployment
upgraded from single-org to multi-org resumes from its existing state file and
then migrates forward (the next commit writes the prefixed key).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from sf2loki.state.base import CheckpointStore


class _CommitManyCapable(Protocol):
    """The duck-typed commit_many(...) surface (#54), not part of the frozen
    CheckpointStore Protocol — see state/base.py and the store implementations."""

    async def commit_many(self, items: Mapping[str, str]) -> None: ...


def org_prefix(name: str) -> str:
    """Return the checkpoint-key prefix for org *name* (``"org=<name>:"``)."""
    return f"org={name}:"


class OrgCheckpointView:
    """A :class:`~sf2loki.state.base.CheckpointStore` view scoped to one org.

    ``load`` reads the prefixed key; when ``legacy_fallback`` is set (the first
    org only) a miss falls back to the unprefixed key so pre-multi-org state
    migrates transparently. ``commit`` always writes the prefixed key. Any other
    attribute (``set_fence``, ``close``, ...) passes through to the wrapped store.
    """

    def __init__(
        self, inner: CheckpointStore, *, prefix: str, legacy_fallback: bool = False
    ) -> None:
        self._inner = inner
        self._prefix = prefix
        self._legacy_fallback = legacy_fallback

    async def load(self, key: str) -> str | None:
        value = await self._inner.load(self._prefix + key)
        if value is None and self._legacy_fallback:
            # Transparent migration: resume from the pre-multi-org (unprefixed)
            # key. The next commit writes the prefixed key, completing the move.
            return await self._inner.load(key)
        return value

    async def commit(self, key: str, value: str) -> None:
        await self._inner.commit(self._prefix + key, value)

    async def commit_many(self, items: Mapping[str, str]) -> None:
        """Prefix every key and forward to the inner store's commit_many (#54).

        Explicit (not left to __getattr__) because the keys need prefixing
        before forwarding — unlike reset()/set_epoch(), which pass through
        unprefixed via __getattr__ below.
        """
        prefixed = {self._prefix + key: value for key, value in items.items()}
        inner = cast(_CommitManyCapable, self._inner)
        await inner.commit_many(prefixed)

    def __getattr__(self, name: str) -> Any:
        # Passthrough for set_fence/close/etc. Only reached for attributes not
        # defined above (self._inner is set in __init__ before any such access).
        return getattr(self._inner, name)
