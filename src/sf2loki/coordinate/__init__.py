"""Leadership coordination for active-passive HA."""

from __future__ import annotations

from sf2loki.coordinate.base import Coordinator, NoopCoordinator, StateFenceError
from sf2loki.coordinate.file_lease import FileLeaseCoordinator
from sf2loki.coordinate.k8s_lease import K8sLeaseCoordinator

__all__ = [
    "Coordinator",
    "FileLeaseCoordinator",
    "K8sLeaseCoordinator",
    "NoopCoordinator",
    "StateFenceError",
]
