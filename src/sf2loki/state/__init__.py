"""Checkpoint state stores: the CheckpointStore seam and its backends."""

from __future__ import annotations

import importlib.util

from sf2loki.config import ConfigError, StateConfig
from sf2loki.state.base import CheckpointStore
from sf2loki.state.file_store import FileCheckpointStore


def build_store(cfg: StateConfig) -> CheckpointStore:
    """Construct the configured checkpoint store.

    The S3 backend keeps aiobotocore out of its module imports (so its unit
    tests run without the extra), which means selecting it can't fail on
    import — check dependency availability explicitly here instead, so a
    missing extra fails fast at build/--check time with an actionable message
    rather than as a raw ImportError on the first commit.
    """
    if cfg.store == "s3":
        if importlib.util.find_spec("aiobotocore") is None:
            raise ConfigError(
                "state.store is 's3' but the S3 dependencies are not installed; "
                "install the extra: pip install 'sf2loki[s3]'"
            )
        from sf2loki.state.s3_store import S3CheckpointStore

        return S3CheckpointStore(cfg.s3)
    return FileCheckpointStore(cfg.file.path)
