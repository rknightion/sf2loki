"""FileCheckpointStore: durable state backed by a local JSON file."""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path


class FileCheckpointStore:
    """Checkpoint store that persists a flat {str: str} map to a local JSON file.

    File I/O is performed synchronously inside async methods — this is intentional;
    the state file is small and sync I/O avoids the complexity of a thread pool for
    what is, in practice, a handful of bytes written at infrequent intervals.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = asyncio.Lock()
        self._cache: dict[str, str] | None = None  # None = not yet loaded

    # ------------------------------------------------------------------
    # Internal helpers (called under the lock)

    def _ensure_loaded(self) -> None:
        """Load the JSON file into the in-memory cache if not already loaded."""
        if self._cache is not None:
            return
        if self._path.exists():
            data = json.loads(self._path.read_text())
            if not isinstance(data, dict):
                raise ValueError(f"state file {self._path} must contain a JSON object")
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
