"""Tests for sf2loki.sinks.loki.push."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import cramjam

from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.sinks.loki._generated import loki_push_pb2 as loki_pb
from sf2loki.sinks.loki.push import encode_json, encode_protobuf

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _entry(
    ts: datetime,
    labels: dict[str, str],
    line: str,
    metadata: dict[str, str] | None = None,
) -> LogEntry:
    return LogEntry(
        timestamp=ts,
        labels=labels,
        line=line,
        structured_metadata=metadata or {},
        checkpoint=CheckpointToken(key="test", value="0"),
    )


TS1 = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
TS2 = datetime(2024, 1, 15, 12, 0, 1, tzinfo=UTC)
TS3 = datetime(2024, 1, 15, 12, 0, 2, tzinfo=UTC)

LABELS_A = {"job": "sf2loki", "environment": "prod"}
LABELS_B = {"job": "sf2loki", "environment": "staging"}


# ---------------------------------------------------------------------------
# encode_protobuf tests
# ---------------------------------------------------------------------------


class TestEncodeProtobuf:
    def _decode(self, data: bytes) -> loki_pb.PushRequest:
        raw = bytes(cramjam.snappy.decompress_raw(data))
        req = loki_pb.PushRequest()
        req.ParseFromString(raw)
        return req

    def test_two_label_groups_produce_two_streams(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "line1"),
                _entry(TS2, LABELS_A, "line2"),
                _entry(TS3, LABELS_B, "line3"),
            ]
        )
        result = encode_protobuf(batch)
        assert isinstance(result, bytes)
        req = self._decode(result)
        assert len(req.streams) == 2

    def test_streams_have_correct_entry_counts(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "line1"),
                _entry(TS2, LABELS_A, "line2"),
                _entry(TS3, LABELS_B, "line3"),
            ]
        )
        req = self._decode(encode_protobuf(batch))
        # Find streams by entry count
        counts = sorted(len(s.entries) for s in req.streams)
        assert counts == [1, 2]

    def test_stream_labels_strings(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "line1"),
                _entry(TS3, LABELS_B, "line3"),
            ]
        )
        req = self._decode(encode_protobuf(batch))
        label_strings = {s.labels for s in req.streams}
        assert '{environment="prod",job="sf2loki"}' in label_strings
        assert '{environment="staging",job="sf2loki"}' in label_strings

    def test_entry_lines_preserved(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "hello world"),
                _entry(TS2, LABELS_A, "second entry"),
            ]
        )
        req = self._decode(encode_protobuf(batch))
        assert len(req.streams) == 1
        lines = {e.line for e in req.streams[0].entries}
        assert lines == {"hello world", "second entry"}

    def test_timestamps_round_trip(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "ts-test")])
        req = self._decode(encode_protobuf(batch))
        entry = req.streams[0].entries[0]
        recovered = entry.timestamp.ToDatetime(tzinfo=UTC)
        assert recovered == TS1

    def test_structured_metadata_round_trips(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "meta-test", metadata={"trace_id": "abc", "span_id": "def"}),
            ]
        )
        req = self._decode(encode_protobuf(batch))
        pairs = {p.name: p.value for p in req.streams[0].entries[0].structuredMetadata}
        assert pairs == {"trace_id": "abc", "span_id": "def"}

    def test_structured_metadata_sorted(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "sort-test", metadata={"z_key": "1", "a_key": "2"}),
            ]
        )
        req = self._decode(encode_protobuf(batch))
        names = [p.name for p in req.streams[0].entries[0].structuredMetadata]
        assert names == sorted(names)

    def test_snappy_block_format_required(self) -> None:
        """Decompress with block-format (decompress_raw) must succeed; framed would differ."""
        batch = Batch(entries=[_entry(TS1, LABELS_A, "snappy-test")])
        result = encode_protobuf(batch)
        # block-format decompression succeeds
        raw = bytes(cramjam.snappy.decompress_raw(result))
        req = loki_pb.PushRequest()
        req.ParseFromString(raw)
        assert len(req.streams) == 1

    def test_empty_batch_produces_empty_streams(self) -> None:
        batch = Batch(entries=[])
        req = self._decode(encode_protobuf(batch))
        assert len(req.streams) == 0

    def test_single_entry_batch(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "only")])
        req = self._decode(encode_protobuf(batch))
        assert len(req.streams) == 1
        assert req.streams[0].entries[0].line == "only"


# ---------------------------------------------------------------------------
# encode_json tests
# ---------------------------------------------------------------------------


class TestEncodeJson:
    def _decode(self, data: bytes) -> dict:  # type: ignore[type-arg]
        return json.loads(data.decode("utf-8"))

    def test_returns_bytes(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "json-test")])
        result = encode_json(batch)
        assert isinstance(result, bytes)

    def test_two_label_groups_produce_two_streams(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "line1"),
                _entry(TS2, LABELS_A, "line2"),
                _entry(TS3, LABELS_B, "line3"),
            ]
        )
        body = self._decode(encode_json(batch))
        assert len(body["streams"]) == 2

    def test_stream_labels_dict(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "line1")])
        body = self._decode(encode_json(batch))
        assert body["streams"][0]["stream"] == LABELS_A

    def test_nanosecond_string_timestamp(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "ts-test")])
        body = self._decode(encode_json(batch))
        ts_str = body["streams"][0]["values"][0][0]
        assert isinstance(ts_str, str)
        expected_ns = str(int(TS1.timestamp() * 1_000_000_000))
        assert ts_str == expected_ns

    def test_line_is_second_element(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "my log line")])
        body = self._decode(encode_json(batch))
        assert body["streams"][0]["values"][0][1] == "my log line"

    def test_structured_metadata_is_third_element(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "meta", metadata={"trace_id": "xyz"}),
            ]
        )
        body = self._decode(encode_json(batch))
        meta = body["streams"][0]["values"][0][2]
        assert isinstance(meta, dict)
        assert meta == {"trace_id": "xyz"}

    def test_empty_structured_metadata_is_empty_dict(self) -> None:
        batch = Batch(entries=[_entry(TS1, LABELS_A, "no-meta")])
        body = self._decode(encode_json(batch))
        meta = body["streams"][0]["values"][0][2]
        assert meta == {}

    def test_empty_batch(self) -> None:
        batch = Batch(entries=[])
        body = self._decode(encode_json(batch))
        assert body == {"streams": []}

    def test_multiple_entries_same_labels_one_stream(self) -> None:
        batch = Batch(
            entries=[
                _entry(TS1, LABELS_A, "first"),
                _entry(TS2, LABELS_A, "second"),
            ]
        )
        body = self._decode(encode_json(batch))
        assert len(body["streams"]) == 1
        assert len(body["streams"][0]["values"]) == 2

    def test_output_is_uncompressed(self) -> None:
        """encode_json must return plain UTF-8 JSON, not compressed."""
        batch = Batch(entries=[_entry(TS1, LABELS_A, "plain")])
        result = encode_json(batch)
        # Must parse as JSON directly, no decompression step
        parsed = json.loads(result)
        assert "streams" in parsed
