"""Checkpoint state stores: the CheckpointStore seam and its backends."""

from __future__ import annotations

from sf2loki.config import ConfigError, StateConfig
from sf2loki.state.base import CheckpointStore
from sf2loki.state.file_store import FileCheckpointStore


def build_store(cfg: StateConfig) -> CheckpointStore:
    """Construct the configured checkpoint store.

    The S3 backend is imported lazily so its aiobotocore dependency (the
    ``sf2loki[s3]`` extra) is only required when actually selected.
    """
    if cfg.store == "s3":
        try:
            from sf2loki.state.s3_store import S3CheckpointStore
        except ImportError as exc:
            raise ConfigError(
                "state.store is 's3' but the S3 dependencies are not installed; "
                "install the extra: pip install 'sf2loki[s3]'"
            ) from exc
        return S3CheckpointStore(cfg.s3)
    return FileCheckpointStore(cfg.file.path)
