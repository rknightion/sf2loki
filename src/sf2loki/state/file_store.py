"""FileCheckpointStore: durable state backed by a local JSON file."""

from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import tempfile
from pathlib import Path


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

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._cache: dict[str, str] | None = None  # None = not yet loaded
        self._lock_fd: int | None = None  # exclusive-instance flock, held once acquired

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
        """Take the exclusive per-state-file flock (no-op once held)."""
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

    def _ensure_loaded(self) -> None:
        """Load the JSON file into the in-memory cache if not already loaded."""
        self._acquire_instance_lock()
        if self._cache is not None:
            return
        if self._path.exists():
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
            self._cache = data
        else:
            self._cache = {}

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

    async def load(self, key: str) -> str | None:
        async with self._lock:
            self._ensure_loaded()
            assert self._cache is not None
            return self._cache.get(key)

    async def commit(self, key: str, value: str) -> None:
        async with self._lock:
            self._ensure_loaded()
            assert self._cache is not None
            self._cache[key] = value
            self._flush()
