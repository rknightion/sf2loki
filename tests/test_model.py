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


def _entry(line: str) -> LogEntry:
    return LogEntry(
        timestamp=datetime.now(UTC),
        labels={},
        line=line,
        structured_metadata={},
        checkpoint=CheckpointToken("k", "v"),
    )


def test_line_nbytes_memoizes_utf8_length() -> None:
    entry = _entry("héllo")  # 'é' is 2 UTF-8 bytes -> 6 total
    assert entry.line_nbytes() == 6
    # Cached: the private memo is populated and reused.
    assert entry._line_nbytes == 6
    assert entry.line_nbytes() == 6


def test_line_nbytes_excluded_from_equality() -> None:
    """Two otherwise-equal entries compare equal regardless of memo state
    (the cached length must not leak into __eq__)."""
    a = _entry("same line")
    b = _entry("same line")
    a.timestamp = b.timestamp  # equalize the one field that would differ
    a.line_nbytes()  # populate a's memo, leave b's uncomputed
    assert a._line_nbytes == 9
    assert b._line_nbytes == -1
    assert a == b
