"""S3-compatible object-storage checkpoint store.

The whole checkpoint document (one small JSON object) lives at a single
bucket/key; commits are conditional writes (ETag ``If-Match``, or
``If-None-Match: *`` for the first write) so a second writer against the same
key fails fast instead of clobbering — the object-store analogue of the file
store's flock. Requires the ``sf2loki[s3]`` extra (aiobotocore).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sf2loki.config import S3StateConfig


class S3CheckpointStore:
    """CheckpointStore backed by one S3 object with compare-and-swap commits."""

    def __init__(self, cfg: S3StateConfig) -> None:
        self._cfg = cfg
        raise NotImplementedError("implemented in the S3 state lane")

    async def load(self, key: str) -> str | None:
        raise NotImplementedError("implemented in the S3 state lane")

    async def commit(self, key: str, value: str) -> None:
        raise NotImplementedError("implemented in the S3 state lane")

    async def close(self) -> None:
        raise NotImplementedError("implemented in the S3 state lane")
