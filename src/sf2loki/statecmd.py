"""`sf2loki state`: inspect/repair checkpoints in the CONFIGURED store (issue #63).

There was previously no supported way to inspect or repair a checkpoint: on
the file store an operator could hand-edit the JSON (undocumented, risky, and
racing the flock); on s3/gcs there was nothing at all. This module backs the
three ``state show`` / ``state set`` / ``state delete`` subcommands wired up
in ``cli.py``, operating through the same :func:`sf2loki.state.build_store`
factory the daemon uses so all three backends (file/s3/gcs) work identically.

Checkpoint values are not secret — ``state show`` redacts nothing.

Safety model:
- The file backend's exclusive flock is acquired lazily, on first
  ``load``/``commit``/``delete`` (see ``FileCheckpointStore._ensure_loaded``),
  so *every* subcommand here — including a read-only ``show`` — attempts it.
  If the daemon is running against the same state file, that raises
  :class:`~sf2loki.state.file_store.StateFileLockError`; this module reports
  a clear "the daemon is running" message and exits non-zero, unless
  ``--force`` was passed (which builds the store with ``exclusive_lock=False``,
  bypassing the flock — unsafe if the daemon is genuinely still running,
  since writes can then race and clobber each other).
- The s3/gcs backends need no lock check (there is no sidecar lock), but a
  concurrent writer against the same object can still lose a compare-and-swap
  race; that surfaces as
  :class:`~sf2loki.state.s3_store.StateStoreConflictError`, reported here as
  a clear "another writer raced; retry" message.

``state show`` needs to enumerate every key, which the frozen
``CheckpointStore`` Protocol's per-key ``load(key)`` cannot do. Rather than
adding a new "list all keys" method to the stores (out of scope for #63,
which only asks for a minimal ``delete``), this reads the store's private
in-memory document cache directly once ``load`` has populated it — every
concrete store keeps the whole checkpoint document in a ``_cache`` dict (see
``file_store.py``/``s3_store.py``/``gcs_store.py``'s ``_ensure_loaded``). The
file store's internal fencing bookkeeping key (``__fence_epoch__``) is
filtered out of the listing — it is plumbing, not a checkpoint.
"""

from __future__ import annotations

import fnmatch
import inspect
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol, cast

from sf2loki.config import ConfigError, load
from sf2loki.state import build_store
from sf2loki.state.file_store import _EPOCH_KEY as _FILE_STORE_EPOCH_KEY
from sf2loki.state.file_store import StateFileLockError
from sf2loki.state.s3_store import StateStoreConflictError

# Mirrors cli.py's _CONFIG_ERROR_EXIT_CODE: a bad/unloadable config, or a
# state backend selected without its extra installed.
_CONFIG_ERROR_EXIT_CODE = 2
# A runtime operational failure once the config itself is fine: the file
# store's flock is held by another process, or an object-store CAS race.
_OPERATION_ERROR_EXIT_CODE = 1

# Reserved keys no operator should ever show/set/delete directly — they are
# internal bookkeeping, not a source's checkpoint.
_RESERVED_KEYS = frozenset({_FILE_STORE_EPOCH_KEY})


class _CacheCapable(Protocol):
    """Duck-typed view onto a store's whole-document in-memory cache.

    Not part of the frozen ``CheckpointStore`` Protocol (state/base.py) —
    see the module docstring for why ``state show`` reads it directly.
    """

    _cache: dict[str, str] | None


class _DeleteCapable(Protocol):
    """Duck-typed ``delete`` hook added to all three stores for issue #63."""

    async def delete(self, key: str) -> None: ...


async def _close_store(store: Any) -> None:
    """Best-effort close: FileCheckpointStore.close() is sync; s3/gcs's is async."""
    close = getattr(store, "close", None)
    if close is None:
        return
    try:
        result = close()
        if inspect.isawaitable(result):
            await result
    except Exception:  # pragma: no cover - best-effort cleanup only
        pass


async def _run(
    config_path: Path | None,
    *,
    force: bool,
    op: Callable[[Any], Awaitable[None]],
) -> int:
    """Load config, build the configured store, run *op*, and map failures to
    exit codes + operator-facing messages. Shared by show/set/delete."""
    try:
        cfg = load(config_path)
    except ConfigError as exc:
        print(f"sf2loki: {exc}", file=sys.stderr)
        return _CONFIG_ERROR_EXIT_CODE

    try:
        store = build_store(cfg.state, exclusive_lock=not force)
    except ConfigError as exc:
        print(f"sf2loki: {exc}", file=sys.stderr)
        return _CONFIG_ERROR_EXIT_CODE

    try:
        await op(store)
    except StateFileLockError as exc:
        print(
            f"sf2loki: {exc}\n"
            "Refusing to touch the state file while another sf2loki instance holds "
            "its lock. Stop the running daemon first, or pass --force to bypass the "
            "lock (unsafe if the daemon is actually still running — a write from "
            "here can race the daemon's own and clobber checkpoints).",
            file=sys.stderr,
        )
        return _OPERATION_ERROR_EXIT_CODE
    except StateStoreConflictError as exc:
        print(
            f"sf2loki: {exc}\nAnother writer raced this state object; retry the command.",
            file=sys.stderr,
        )
        return _OPERATION_ERROR_EXIT_CODE
    finally:
        await _close_store(store)
    return 0


async def _whole_document(store: Any) -> dict[str, str]:
    """Force *store* to load its whole document, then return a copy of it."""
    await store.load("")  # any key: populates the store's cache as a side effect
    cache = cast(_CacheCapable, store)._cache
    return dict(cache) if cache is not None else {}


async def run_state_show(
    config_path: Path | None,
    *,
    key_glob: str = "*",
    force: bool = False,
) -> int:
    """``sf2loki state show``: pretty-print checkpoints from the configured store.

    Values are never redacted — checkpoint state is not secret. *key_glob* is
    an ``fnmatch`` pattern (default ``"*"``, i.e. every key).
    """

    async def _op(store: Any) -> None:
        doc = await _whole_document(store)
        matched = {
            k: v for k, v in doc.items() if k not in _RESERVED_KEYS and fnmatch.fnmatch(k, key_glob)
        }
        if not matched:
            print("(no checkpoints match)")
            return
        for key in sorted(matched):
            print(f"{key}\t{matched[key]}")
        suffix = "" if key_glob == "*" else f" matching {key_glob!r}"
        print(f"\n{len(matched)} checkpoint(s){suffix}")

    return await _run(config_path, force=force, op=_op)


async def run_state_set(
    config_path: Path | None,
    key: str,
    value: str,
    *,
    force: bool = False,
) -> int:
    """``sf2loki state set KEY VALUE``: CAS-safe write of a single checkpoint."""
    if key in _RESERVED_KEYS:
        print(f"sf2loki: {key!r} is a reserved internal key; refusing to set it", file=sys.stderr)
        return _OPERATION_ERROR_EXIT_CODE

    async def _op(store: Any) -> None:
        await store.commit(key, value)
        print(f"sf2loki: set {key} = {value}")

    return await _run(config_path, force=force, op=_op)


async def run_state_delete(
    config_path: Path | None,
    key: str,
    *,
    force: bool = False,
) -> int:
    """``sf2loki state delete KEY``: remove a checkpoint so its source restarts
    from its preset/lookback default on the next run.

    Deleting a key that ingestion has already advanced past means the
    source's next run re-lists its lookback/preset window from scratch —
    records already pushed to Loki within that window are pushed again
    (a duplicate window), not lost. See the runbook linked from
    ``docs/state-runbook.md`` for the exact recovery procedure.
    """
    if key in _RESERVED_KEYS:
        print(
            f"sf2loki: {key!r} is a reserved internal key; refusing to delete it",
            file=sys.stderr,
        )
        return _OPERATION_ERROR_EXIT_CODE

    async def _op(store: Any) -> None:
        deleter = cast(_DeleteCapable, store)
        await deleter.delete(key)
        print(f"sf2loki: deleted {key} (its source restarts from preset/lookback on next run)")

    return await _run(config_path, force=force, op=_op)
