"""Leadership coordination for active-passive HA."""

from __future__ import annotations

from sf2loki.coordinate.base import Coordinator, NoopCoordinator, StateFenceError
from sf2loki.coordinate.file_lease import FileLeaseCoordinator

__all__ = [
    "Coordinator",
    "FileLeaseCoordinator",
    "NoopCoordinator",
    "StateFenceError",
]
