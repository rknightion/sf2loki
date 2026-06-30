"""Protobuf and JSON encoders for Loki push payloads."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Mapping

import cramjam
from google.protobuf.timestamp_pb2 import Timestamp

from sf2loki.model import Batch, LogEntry
from sf2loki.sinks.loki._generated import loki_push_pb2 as loki_pb
from sf2loki.sinks.loki.labels import render_labels

# A stable key for grouping entries by their label set.
_LabelKey = tuple[tuple[str, str], ...]


def _label_key(labels: Mapping[str, str]) -> _LabelKey:
    return tuple(sorted(labels.items()))


def _group_by_labels(entries: list[LogEntry]) -> dict[_LabelKey, list[LogEntry]]:
    groups: dict[_LabelKey, list[LogEntry]] = defaultdict(list)
    for entry in entries:
        groups[_label_key(entry.labels)].append(entry)
    return groups


def _make_timestamp(entry: LogEntry) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(entry.timestamp)
    return ts


def encode_protobuf(batch: Batch) -> bytes:
    """Encode *batch* as a snappy-compressed (block format) protobuf PushRequest.

    Entries are grouped by label set; each group becomes one StreamAdapter.
    Structured metadata keys within each entry are sorted ascending.
    """
    groups = _group_by_labels(batch.entries)
    streams = []
    for key, entries in groups.items():
        label_str = render_labels(dict(key))
        pb_entries = [
            loki_pb.EntryAdapter(  # type: ignore[attr-defined]
                timestamp=_make_timestamp(e),
                line=e.line,
                structuredMetadata=[
                    loki_pb.LabelPairAdapter(name=k, value=v)  # type: ignore[attr-defined]
                    for k, v in sorted(e.structured_metadata.items())
                ],
            )
            for e in entries
        ]
        streams.append(loki_pb.StreamAdapter(labels=label_str, entries=pb_entries))  # type: ignore[attr-defined]

    request = loki_pb.PushRequest(streams=streams)  # type: ignore[attr-defined]
    serialized = request.SerializeToString()
    return bytes(cramjam.snappy.compress_raw(serialized))


def encode_json(batch: Batch) -> bytes:
    """Encode *batch* as uncompressed UTF-8 JSON in the Loki HTTP push format.

    Returns the raw JSON bytes — callers apply any desired compression (e.g. gzip)
    before sending to Loki.
    """
    groups = _group_by_labels(batch.entries)
    streams = []
    for key, entries in groups.items():
        label_dict = dict(key)
        values = [
            [
                str(int(e.timestamp.timestamp() * 1_000_000_000)),
                e.line,
                dict(e.structured_metadata),
            ]
            for e in entries
        ]
        streams.append({"stream": label_dict, "values": values})

    body = {"streams": streams}
    return json.dumps(body).encode("utf-8")
