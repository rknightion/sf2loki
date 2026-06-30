from __future__ import annotations

from datetime import UTC, datetime

import pytest

from sf2loki.model import Batch, CheckpointToken, LogEntry


def test_checkpoint_token_is_frozen() -> None:
    t = CheckpointToken(key="k", value="v")
    assert (t.key, t.value) == ("k", "v")
    with pytest.raises(AttributeError):
        t.value = "other"  # type: ignore[misc]


def test_logentry_and_batch() -> None:
    entry = LogEntry(
        timestamp=datetime.now(UTC),
        labels={"source": "pubsub", "event_type": "LoginEventStream"},
        line="{}",
        structured_metadata={"user_id": "005"},
        checkpoint=CheckpointToken("pubsub:/event/LoginEventStream", "abc"),
    )
    batch = Batch(entries=[entry])
    assert batch.entries[0].labels["source"] == "pubsub"
    assert batch.entries[0].structured_metadata["user_id"] == "005"
    assert Batch().entries == []
