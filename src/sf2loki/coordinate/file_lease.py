"""File-lease :class:`Coordinator`: active-passive failover on shared storage.

The leader owns a small JSON lease document — ``{"holder", "expires_at"}`` —
on storage shared by every replica (NFS/EFS/a shared volume). It renews the
lease (rewrites ``expires_at``) faster than the ttl; a standby takes over once
the lease has gone stale. Expiry is **wall-clock** because it must be compared
across hosts, so the deployment must keep the replicas NTP-synced and set the
ttl comfortably above worst-case clock skew (see :class:`FileLeaseConfig`).

Advisory ``flock`` is deliberately not used: it is unreliable over NFS and does
not survive the holder disappearing without a clean release. Lease-expiry with
an atomic tmp+rename write (the same durability pattern the file checkpoint
store uses) is robust to a leader that simply dies.

Races are resolved by "last writer wins on rename" plus a verification re-read:
two standbys can both rename their tmp file over an expired lease; whichever
landed last owns it, so after writing we pause briefly and re-read — if the
holder is no longer us, we lost the race and back off.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from sf2loki.coordinate.base import StateFenceError
from sf2loki.obs.logging import get_logger

if TYPE_CHECKING:
    from pathlib import Path

    from sf2loki.config import FileLeaseConfig

log = get_logger(__name__)

# Fraction of the renew interval to wait between writing a contested lease and
# re-reading it to confirm we won the rename race. Short — just long enough for
# a competing rename to land — and only paid at takeover, never while holding.
_VERIFY_FRACTION = 0.1
_VERIFY_MIN = 0.05
_VERIFY_MAX = 1.0


def _default_utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class _Lease:
    holder: str
    expires_at: datetime

    def expired(self, now: datetime) -> bool:
        return now >= self.expires_at


class FileLeaseCoordinator:
    """Lease-based leader election over a shared file.

    ``utcnow`` supplies the wall clock written into / compared against the lease
    (injected in tests); ``sleep`` performs the interval waits (injected so
    tests never sleep for real). ``holder`` is the identity written into the
    lease — the app derives ``hostname-pid`` when config leaves it blank.
    """

    def __init__(
        self,
        cfg: FileLeaseConfig,
        *,
        holder: str | None = None,
        utcnow: Callable[[], datetime] = _default_utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._path: Path = cfg.path
        self._ttl: float = cfg.ttl.total_seconds()
        self._renew: float = cfg.renew_interval.total_seconds()
        self._holder: str = holder or cfg.holder_id or f"{os.uname().nodename}-{os.getpid()}"
        self._utcnow = utcnow
        self._sleep = sleep
        self._verify_delay: float = min(
            _VERIFY_MAX, max(_VERIFY_MIN, self._renew * _VERIFY_FRACTION)
        )
        self._is_leader: bool = False

    # ------------------------------------------------------------------
    # Fencing contract (consumed by the state store via set_fence)

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    def check_fence(self) -> None:
        """Raise :class:`StateFenceError` unless this instance currently leads.

        Wired into the checkpoint store as a pre-commit fence so a stale leader
        cannot advance checkpoints after losing the lease.
        """
        if not self._is_leader:
            raise StateFenceError(
                f"refusing checkpoint commit: {self._holder} does not hold the "
                f"lease {self._path} (leadership lost) — the new leader owns the "
                "checkpoints now"
            )

    @property
    def holder(self) -> str:
        return self._holder

    # ------------------------------------------------------------------
    # Coordinator protocol

    async def run(
        self,
        *,
        on_acquire: Callable[[], Awaitable[None]],
        on_lose: Callable[[], Awaitable[None]],
        stop: asyncio.Event,
    ) -> None:
        """Standby → acquire → on_acquire → hold → on_lose → standby, until stop.

        ``on_acquire`` and ``on_lose`` are awaited in strict alternation: every
        acquisition is paired with exactly one loss via ``try/finally``.
        """
        try:
            while not stop.is_set():
                won = await self._acquire(stop)
                if not won:
                    return  # stop fired while standing by
                self._is_leader = True
                log.info("acquired file lease", holder=self._holder, lease=str(self._path))
                await on_acquire()
                try:
                    await self._hold(stop)
                finally:
                    self._is_leader = False
                    log.info("released file lease", holder=self._holder, lease=str(self._path))
                    await on_lose()
        finally:
            self._is_leader = False

    # ------------------------------------------------------------------
    # Standby / acquire

    async def _acquire(self, stop: asyncio.Event) -> bool:
        """Block until this instance owns the lease; return False if stop fires."""
        while not stop.is_set():
            lease = self._read()
            now = self._utcnow()
            if lease is None or lease.expired(now):
                # Contest the (absent/expired) lease, then verify we won the
                # rename race before declaring leadership.
                try:
                    self._write(now)
                except OSError as exc:
                    log.warning("cannot write file lease; retrying", error=str(exc))
                    if await self._pause(self._renew, stop):
                        return False
                    continue
                if await self._pause(self._verify_delay, stop):
                    return False
                confirm = self._read()
                if (
                    confirm is not None
                    and confirm.holder == self._holder
                    and not confirm.expired(self._utcnow())
                ):
                    return True
                # Lost the race to another standby: back off and retry.
                log.info("lost file-lease race; backing off", holder=self._holder)
                if await self._pause(self._verify_delay, stop):
                    return False
            else:
                # A live foreign holder: poll at the renew interval until expiry.
                if await self._pause(self._renew, stop):
                    return False
        return False

    # ------------------------------------------------------------------
    # Hold / renew

    async def _hold(self, stop: asyncio.Event) -> None:
        """Renew the lease until leadership is lost or stop fires."""
        last_ok = self._utcnow()
        while not stop.is_set():
            if await self._pause(self._renew, stop):
                return
            now = self._utcnow()
            # Re-read before renewing: a foreign holder means we were fenced out
            # during a pause/GC gap — surrender immediately.
            lease = self._read()
            if lease is not None and lease.holder != self._holder:
                log.warning(
                    "file lease taken over by another holder; surrendering",
                    holder=self._holder,
                    new_holder=lease.holder,
                )
                return
            try:
                self._write(now)
                last_ok = now
            except OSError as exc:
                # Can't reach shared storage. Tolerate transient failures, but
                # if we've been unable to renew for a full ttl the lease has
                # (or will have) expired for everyone — assume we've lost it.
                if (now - last_ok).total_seconds() >= self._ttl:
                    log.warning(
                        "file lease renewal failing past ttl; surrendering",
                        holder=self._holder,
                        error=str(exc),
                    )
                    return
                log.warning("file lease renewal failed; will retry", error=str(exc))

    # ------------------------------------------------------------------
    # Timing helper

    async def _pause(self, seconds: float, stop: asyncio.Event) -> bool:
        """Sleep up to *seconds*, returning early once stop is set.

        Returns True if stop fired (caller should give up), else False.
        """
        if stop.is_set():
            return True
        sleeper = asyncio.ensure_future(self._sleep(seconds))
        waiter = asyncio.ensure_future(stop.wait())
        try:
            await asyncio.wait({sleeper, waiter}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, waiter):
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        return stop.is_set()

    # ------------------------------------------------------------------
    # Lease file I/O

    def _read(self) -> _Lease | None:
        """Read + parse the lease; None if absent or unparseable (→ takeover)."""
        try:
            raw = self._path.read_text()
        except FileNotFoundError:
            return None
        except OSError as exc:
            log.warning("cannot read file lease; treating as absent", error=str(exc))
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError, UnicodeDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        holder = data.get("holder")
        expires_raw = data.get("expires_at")
        if not isinstance(holder, str) or not isinstance(expires_raw, str):
            return None
        try:
            expires_at = datetime.fromisoformat(expires_raw)
        except ValueError:
            return None
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        return _Lease(holder=holder, expires_at=expires_at)

    def _write(self, now: datetime) -> None:
        """Atomically (tmp + rename) write our holder + ``now + ttl`` expiry."""
        expires_at = now + timedelta(seconds=self._ttl)
        payload = json.dumps({"holder": self._holder, "expires_at": expires_at.isoformat()})
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
        except Exception:
            with contextlib.suppress(OSError):
                os.unlink(tmp_path)
            raise
        os.replace(tmp_path, self._path)
        # Durable rename: fsync the parent directory so a crash right after the
        # rename can't lose the new lease.
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
