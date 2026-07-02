"""FileCheckpointStore: durable state backed by a local JSON file."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path

from sf2loki.coordinate.base import StateFenceError

# Reserved doc key: the epoch of the leader that last wrote this file, used by
# the (file-store-only) set_epoch() fencing below. No source ever uses this
# key, so it never collides with a real checkpoint key.
_EPOCH_KEY = "__fence_epoch__"


class StateFileLockError(RuntimeError):
    """Another process holds the exclusive lock on the state file."""


class StateFileCorruptError(RuntimeError):
    """The state file exists but does not contain a valid JSON object."""


class FileCheckpointStore:
    """Checkpoint store that persists a flat {str: str} map to a local JSON file.

    File I/O is performed synchronously inside async methods — this is intentional;
    the state file is small and sync I/O avoids the complexity of a thread pool for
    what is, in practice, a handful of bytes written at infrequent intervals.

    Multi-instance protection: an exclusive advisory lock (``flock``) is taken on
    a ``<state file>.lock`` sidecar at first use and held for the store's (i.e.
    the process's) lifetime. A second sf2loki instance pointed at the same state
    file would double-ingest every source and clobber the first instance's
    checkpoints — it fails fast with :class:`StateFileLockError` instead.

    Corruption safety: a corrupt/truncated state file raises
    :class:`StateFileCorruptError` with the path and the recovery step (move the
    file aside to reset every checkpoint to its lookback default). The corrupt
    file is never silently discarded — it may still be hand-recoverable.
    """

    def __init__(self, path: Path, *, exclusive_lock: bool = True) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._cache: dict[str, str] | None = None  # None = not yet loaded
        self._lock_fd: int | None = None  # exclusive-instance flock, held once acquired
        # When False, skip the sidecar .lock flock entirely: a real coordinator
        # (e.g. the Kubernetes Lease / file-lease HA topology) is already the
        # exclusivity mechanism, and the flock doesn't fit that topology (see
        # StateFileLockError's docstring and issue #49) — cross-host flock
        # propagation is unreliable (NFS local_lock) and, worse, a demoted-but-
        # alive old leader holding the flock crash-loops the newly promoted one.
        self._exclusive_lock = exclusive_lock
        # Optional pre-commit fence (installed by the app when a leader-election
        # coordinator is configured). Called before every commit; it raises
        # StateFenceError when this instance is not the leader, so a stale
        # leader cannot advance checkpoints. None = unfenced (single instance).
        self._fence: Callable[[], None] | None = None
        # Optional epoch source (file-store-only CAS substitute, see
        # set_epoch()). None = unfenced by epoch (the set_fence() boolean is
        # the only protection, or none at all for standalone deployments).
        self._epoch_fn: Callable[[], int | None] | None = None

    def set_fence(self, fence: Callable[[], None]) -> None:
        """Install a pre-commit fence, evaluated before every :meth:`commit`.

        *fence* must raise (:class:`~sf2loki.coordinate.base.StateFenceError`)
        to veto a commit. A fenced commit raises **before** any state is
        mutated or written.
        """
        self._fence = fence

    def set_epoch(self, epoch_fn: Callable[[], int | None]) -> None:
        """Install an epoch source giving the file store a real CAS-like fence.

        ``set_fence``'s boolean is a *lagging local* signal (up to a
        ``renew_interval`` stale — see issue #47); the file store also has no
        object-store-style ETag/generation to catch a stale leader at write
        time. ``epoch_fn`` closes that gap: on every commit the file is
        re-read **fresh** (bypassing the cache) so a concurrently-advanced
        epoch written by a newer leader is always seen, and the commit is
        rejected with :class:`~sf2loki.coordinate.base.StateFenceError` if the
        file's persisted epoch is newer than ``epoch_fn()``'s. The merged
        document is written back with its epoch advanced to ``epoch_fn()``.

        ``epoch_fn`` returning ``None`` (no epoch assigned yet, e.g. a
        ``NoopCoordinator``) disables this check for that commit.
        """
        self._epoch_fn = epoch_fn

    def close(self) -> None:
        """Release the exclusive instance lock (idempotent).

        Normally the lock lives for the process lifetime and the OS releases it
        at exit; ``close()`` exists for orderly handover (and tests).
        """
        if self._lock_fd is not None:
            fd, self._lock_fd = self._lock_fd, None
            os.close(fd)  # closing the fd releases the flock

    def __del__(self) -> None:
        # Raw os.open fds have no GC-managed wrapper: release the lock fd if a
        # store is garbage-collected without an explicit close() (tests, mainly).
        with contextlib.suppress(Exception):
            self.close()

    # ------------------------------------------------------------------
    # Internal helpers (called under the lock)

    def _acquire_instance_lock(self) -> None:
        """Take the exclusive per-state-file flock (no-op once held, or disabled)."""
        if not self._exclusive_lock:
            return
        if self._lock_fd is not None:
            return
        self._path.parent.mkdir(parents=True, exist_ok=True)
        lock_path = self._path.with_name(self._path.name + ".lock")
        fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            raise StateFileLockError(
                f"state file {self._path} is locked ({lock_path} is held) — is "
                "another sf2loki instance already running against the same state "
                "directory? Two instances sharing state would double-ingest and "
                "clobber each other's checkpoints."
            ) from exc
        self._lock_fd = fd

    def _read_file_fresh(self) -> dict[str, str]:
        """Read the state file from disk unconditionally, bypassing the cache."""
        if not self._path.exists():
            return {}
        try:
            data = json.loads(self._path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise StateFileCorruptError(
                f"state file {self._path} is corrupt ({exc}); refusing to "
                "start rather than silently discarding checkpoints. Move the "
                "file aside to reset all sources to their lookback defaults "
                "(events within each lookback window will be re-ingested)."
            ) from exc
        if not isinstance(data, dict):
            raise StateFileCorruptError(
                f"state file {self._path} must contain a JSON object, got "
                f"{type(data).__name__}. Move the file aside to reset all "
                "sources to their lookback defaults."
            )
        return data

    def _ensure_loaded(self) -> None:
        """Load the JSON file into the in-memory cache if not already loaded."""
        self._acquire_instance_lock()
        if self._cache is not None:
            return
        self._cache = self._read_file_fresh()

    def _flush(self) -> None:
        """Write the cache to disk atomically using a temp file + os.replace."""
        assert self._cache is not None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(dir=self._path.parent, suffix=".tmp")
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(self._cache, fh)
                fh.flush()
                os.fsync(fh.fileno())
        except Exception:
            os.unlink(tmp_path)
            raise
        os.replace(tmp_path, self._path)
        # fsync the parent directory so the rename itself is durable — without
        # it a crash shortly after commit can leave the OLD file (or none).
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    # ------------------------------------------------------------------
    # CheckpointStore protocol

    def reset(self) -> None:
        """Invalidate the in-memory cache so the next load/commit re-reads the file.

        Called on leadership loss (demote) so a later re-acquisition (promote)
        never serves the stale pre-demotion cache — see issue #48. The
        exclusive-instance lock (if held) is left alone; only the cached
        document is dropped.
        """
        self._cache = None

    async def load(self, key: str) -> str | None:
        async with self._lock:
            self._ensure_loaded()
            assert self._cache is not None
            return self._cache.get(key)

    async def commit(self, key: str, value: str) -> None:
        await self.commit_many({key: value})

    async def commit_many(self, items: Mapping[str, str]) -> None:
        """Merge *items* into the state document with exactly one flush.

        Replaces N per-key commits (N loads + N flushes, each fsyncing twice)
        with one load + one flush for the whole batch — see issue #54.
        """
        async with self._lock:
            # Fence first: a non-leader must raise before touching any state.
            if self._fence is not None:
                self._fence()
            if self._epoch_fn is not None:
                self._commit_many_epoch_fenced(items)
            else:
                self._ensure_loaded()
                assert self._cache is not None
                self._cache.update(items)
                self._flush()

    def _commit_many_epoch_fenced(self, items: Mapping[str, str]) -> None:
        """Commit path used when set_epoch() is installed (issue #47).

        Always re-reads the file fresh (never the in-memory cache) so a
        concurrently-advanced epoch written by a newer leader is seen even if
        this instance's own cache — or its lagging set_fence() boolean — is
        stale.
        """
        self._acquire_instance_lock()
        doc = self._read_file_fresh()
        stored_raw = doc.get(_EPOCH_KEY)
        stored = int(stored_raw) if stored_raw is not None else None
        assert self._epoch_fn is not None
        mine = self._epoch_fn()
        if stored is not None and mine is not None and stored > mine:
            raise StateFenceError(
                f"stale leader (epoch {mine}) rejected: state file {self._path} "
                f"was already advanced to epoch {stored} by a newer leader"
            )
        doc.update(items)
        if mine is not None:
            doc[_EPOCH_KEY] = str(mine)
        self._cache = doc
        self._flush()
