"""Checkpoint state stores: the CheckpointStore seam and its backends."""

from __future__ import annotations

import importlib.util

from sf2loki.config import ConfigError, StateConfig
from sf2loki.state.base import CheckpointStore
from sf2loki.state.file_store import FileCheckpointStore


def build_store(cfg: StateConfig, *, exclusive_lock: bool = True) -> CheckpointStore:
    """Construct the configured checkpoint store.

    The S3 backend keeps aiobotocore out of its module imports (so its unit
    tests run without the extra), which means selecting it can't fail on
    import — check dependency availability explicitly here instead, so a
    missing extra fails fast at build/--check time with an actionable message
    rather than as a raw ImportError on the first commit.

    ``exclusive_lock`` applies only to the file backend: the app passes ``False``
    when a real coordinator is configured (the lease, not a sidecar flock, is
    the exclusivity mechanism for HA). The object stores ignore it.
    """
    if cfg.store == "s3":
        if importlib.util.find_spec("aiobotocore") is None:
            raise ConfigError(
                "state.store is 's3' but the S3 dependencies are not installed; "
                "install the extra: pip install 'sf2loki[s3]'"
            )
        from sf2loki.state.s3_store import S3CheckpointStore

        return S3CheckpointStore(cfg.s3)
    if cfg.store == "gcs":
        # Check the TOP-LEVEL package "gcloud": find_spec on the dotted
        # "gcloud.aio.storage" would RAISE ModuleNotFoundError (it imports the
        # parent to look inside) when the extra is absent, bypassing this
        # friendly error. "gcloud" is a bare name → find_spec returns None cleanly.
        if importlib.util.find_spec("gcloud") is None:
            raise ConfigError(
                "state.store is 'gcs' but the GCS dependencies are not installed; "
                "install the extra: pip install 'sf2loki[gcs]'"
            )
        from sf2loki.state.gcs_store import GcsCheckpointStore

        return GcsCheckpointStore(cfg.gcs)
    return FileCheckpointStore(cfg.file.path, exclusive_lock=exclusive_lock)
