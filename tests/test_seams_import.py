"""The frozen seams must import cleanly and instantiate."""

from __future__ import annotations


def test_seams_import() -> None:
    from sf2loki.coordinate.base import Coordinator, NoopCoordinator
    from sf2loki.model import Batch, CheckpointToken, LogEntry
    from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError, Sink
    from sf2loki.sources.base import Source
    from sf2loki.state.base import CheckpointStore

    assert CheckpointToken(key="k", value="v").key == "k"
    # protocols + error types are importable
    assert all(
        x is not None
        for x in (
            Coordinator,
            NoopCoordinator,
            Batch,
            LogEntry,
            Sink,
            Source,
            CheckpointStore,
            RetryableSinkError,
            PermanentSinkError,
        )
    )
