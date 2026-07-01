"""Org-scoping adapter: wraps a Source so its entries and checkpoints carry an org.

Multi-org deployments run one :class:`~sf2loki.sources.base.Source` per org per
category. :class:`OrgSource` wraps each inner source to:

1. merge the ``org`` stream label (plus the org's own ``sf_org_id`` and
   ``environment``) into every real entry — ``sf_org_id``/``environment`` move
   from deployment-wide static labels to per-entry here, since they differ per
   org (see :func:`sf2loki.app.build_static_labels`, single-org path, unchanged);
2. rewrite each entry's checkpoint key to the ``org=<name>:`` prefix so the
   shared state store namespaces cleanly; and
3. hand the inner source an :class:`~sf2loki.state.org_view.OrgCheckpointView`
   so its own ``state.load()`` reads the prefixed key (with a first-org legacy
   fallback for transparent migration).

Single-org (legacy) deployments never construct an ``OrgSource`` — the App uses
sources raw, keeping keys unprefixed and no ``org`` label (bit-identical to
pre-multi-org behaviour).

The inner source's ``name`` is preserved verbatim (e.g. ``"pubsub"``), NOT
prefixed with the org: ``org`` is its own label dimension, so keeping ``source``
clean lets dashboards aggregate a category across orgs and slice by ``org`` when
wanted.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING

from sf2loki.auth.jwt_auth import AuthError
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.logging import get_logger
from sf2loki.state.org_view import OrgCheckpointView, org_prefix

if TYPE_CHECKING:
    from sf2loki.sources.base import Source
    from sf2loki.state.base import CheckpointStore

log = get_logger(__name__)

# Resolves this org's Salesforce org id (usually ``TokenProvider.org_id``); may
# raise if auth is currently failing, in which case sf_org_id is simply omitted
# until it resolves (the entry still carries org + environment).
OrgIdProvider = Callable[[], Awaitable[str]]

# Backoff bounds for the per-org auth-failure supervisor (seconds).
_RETRY_BACKOFF_BASE = 1.0
_RETRY_BACKOFF_MAX = 30.0


class OrgSource:
    """Wrap *inner* so its entries/checkpoints are scoped to one org."""

    def __init__(
        self,
        inner: Source,
        *,
        org: str,
        environment: str,
        org_id_provider: OrgIdProvider | None = None,
        legacy_fallback: bool = False,
    ) -> None:
        self._inner = inner
        # Preserve the inner source's identity (see module docstring).
        self.name = inner.name
        self._org = org
        self._environment = environment
        self._org_id_provider = org_id_provider
        self._prefix = org_prefix(org)
        self._legacy_fallback = legacy_fallback
        self._org_id: str = ""

    async def _resolve_org_id(self) -> str:
        """Best-effort, cached org-id resolution; empty string while unresolved.

        An org whose auth is failing yields no entries, so this is only awaited
        once auth works; a transient failure just leaves it unresolved (no
        sf_org_id label) and retries on the next entry.
        """
        if self._org_id:
            return self._org_id
        if self._org_id_provider is None:
            return ""
        try:
            self._org_id = await self._org_id_provider()
        except Exception:
            return ""
        return self._org_id

    async def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        """Yield the inner source's entries, org-scoped, with per-org auth isolation.

        The polling clients (soql/eventlogfile) let an ``AuthError`` from token
        minting ESCAPE their generators — in single-org that crashes the pipeline
        and the process restarts (fine). In multi-org that would take healthy orgs
        down with the failing one, so here it is contained: log, back off (honouring
        ``stop``), and restart the inner generator. The source resumes from its last
        committed checkpoint (at-least-once, same as a process restart), so the
        failing org retries auth reactively while the others keep streaming.
        """
        view = OrgCheckpointView(state, prefix=self._prefix, legacy_fallback=self._legacy_fallback)
        backoff = _RETRY_BACKOFF_BASE
        while not stop.is_set():
            try:
                async for entry in self._inner.events(view, stop):
                    if not entry.checkpoint_only:
                        extra = {"org": self._org, "environment": self._environment}
                        org_id = await self._resolve_org_id()
                        if org_id:
                            extra["sf_org_id"] = org_id
                        entry.labels = {**entry.labels, **extra}
                    entry.checkpoint = CheckpointToken(
                        key=self._prefix + entry.checkpoint.key, value=entry.checkpoint.value
                    )
                    yield entry
                return  # inner source exhausted normally (finite run)
            except AuthError as exc:
                log.error(
                    "org source auth failing; retrying (other orgs unaffected)",
                    org=self._org,
                    source=self.name,
                    error=str(exc),
                )
                # Wait out the backoff, but wake immediately on shutdown.
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=backoff)
                    return
                backoff = min(backoff * 2, _RETRY_BACKOFF_MAX)
