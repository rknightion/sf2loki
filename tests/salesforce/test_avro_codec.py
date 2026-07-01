"""Tests for AvroCodec (salesforce/avro_codec.py).

Covers: decode round-trip, schema caching (no double-fetch), and concurrent
decode of a new schema_id fetches only once (lock ensures single fetch).
"""

from __future__ import annotations

import asyncio
import io
import json

import fastavro
import pytest

from sf2loki.salesforce.avro_codec import AvroCodec, SchemaFetchError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_SCHEMA: dict[str, object] = {
    "type": "record",
    "name": "TestEvent",
    "fields": [
        {"name": "Id", "type": "string"},
        {"name": "EventDate", "type": "string"},
        {"name": "Count", "type": "int"},
    ],
}

_TEST_RECORD: dict[str, object] = {
    "Id": "abc123",
    "EventDate": "2024-01-15T10:00:00Z",
    "Count": 42,
}


def _encode_record(schema: dict[str, object], record: dict[str, object]) -> bytes:
    """Encode *record* to schemaless Avro bytes using *schema*."""
    parsed = fastavro.parse_schema(schema)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, record)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_decode_round_trips_record() -> None:
    """AvroCodec.decode returns the original record dict."""
    schema_json = json.dumps(_TEST_SCHEMA)
    payload = _encode_record(_TEST_SCHEMA, _TEST_RECORD)

    async def fetcher(schema_id: str) -> str:
        return schema_json

    codec = AvroCodec(fetcher)
    result = await codec.decode("schema-1", payload)

    assert result == _TEST_RECORD


@pytest.mark.asyncio
async def test_cache_hit_avoids_second_fetch() -> None:
    """A second decode for the same schema_id does NOT call the fetcher again."""
    schema_json = json.dumps(_TEST_SCHEMA)
    payload = _encode_record(_TEST_SCHEMA, _TEST_RECORD)
    call_count = 0

    async def fetcher(schema_id: str) -> str:
        nonlocal call_count
        call_count += 1
        return schema_json

    codec = AvroCodec(fetcher)
    await codec.decode("schema-1", payload)
    await codec.decode("schema-1", payload)

    assert call_count == 1, f"fetcher called {call_count} times, expected 1"


@pytest.mark.asyncio
async def test_cache_size_reflects_distinct_schemas() -> None:
    """cache_size() returns the number of distinct schema_ids cached."""
    schemas = {
        "s1": {
            "type": "record",
            "name": "E1",
            "fields": [{"name": "x", "type": "string"}],
        },
        "s2": {
            "type": "record",
            "name": "E2",
            "fields": [{"name": "y", "type": "int"}],
        },
    }

    async def fetcher(schema_id: str) -> str:
        return json.dumps(schemas[schema_id])

    codec = AvroCodec(fetcher)

    buf1 = io.BytesIO()
    fastavro.schemaless_writer(buf1, fastavro.parse_schema(schemas["s1"]), {"x": "hello"})
    await codec.decode("s1", buf1.getvalue())

    assert codec.cache_size() == 1

    buf2 = io.BytesIO()
    fastavro.schemaless_writer(buf2, fastavro.parse_schema(schemas["s2"]), {"y": 7})
    await codec.decode("s2", buf2.getvalue())

    assert codec.cache_size() == 2


@pytest.mark.asyncio
async def test_concurrent_decode_fetches_schema_once() -> None:
    """Concurrent first-time decodes for the same schema_id fetch exactly once."""
    schema_json = json.dumps(_TEST_SCHEMA)
    payload = _encode_record(_TEST_SCHEMA, _TEST_RECORD)
    fetch_count = 0

    async def slow_fetcher(schema_id: str) -> str:
        nonlocal fetch_count
        fetch_count += 1
        await asyncio.sleep(0.01)  # allow other tasks to reach the lock
        return schema_json

    codec = AvroCodec(slow_fetcher)
    tasks = [asyncio.create_task(codec.decode("schema-x", payload)) for _ in range(5)]
    results = await asyncio.gather(*tasks)

    # Every task should succeed and return the same record.
    for r in results:
        assert r == _TEST_RECORD

    assert fetch_count == 1, f"schema fetched {fetch_count} times, expected 1"


# ---------------------------------------------------------------------------
# Schema-fetch failures must be distinguishable from poison payloads (B3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_fetch_failure_raises_schema_fetch_error() -> None:
    """A failing fetch_schema callback surfaces as SchemaFetchError (not a generic error).

    The subscribe loop poison-skips generic decode errors; a schema-fetch RPC
    failure must be a distinct type so it propagates (kills the stream) instead
    of silently skipping events and advancing the checkpoint past them.
    """

    async def failing_fetcher(schema_id: str) -> str:
        raise RuntimeError("schema registry down")

    codec = AvroCodec(failing_fetcher)
    with pytest.raises(SchemaFetchError) as excinfo:
        await codec.decode("schema-1", b"\x00")

    assert isinstance(excinfo.value.__cause__, RuntimeError)


@pytest.mark.asyncio
async def test_schema_parse_failure_raises_schema_fetch_error() -> None:
    """Unparseable schema JSON is a schema problem, not a payload problem."""

    async def garbage_fetcher(schema_id: str) -> str:
        return "not valid json {"

    codec = AvroCodec(garbage_fetcher)
    with pytest.raises(SchemaFetchError):
        await codec.decode("schema-1", b"\x00")


@pytest.mark.asyncio
async def test_failed_fetch_is_not_cached_and_retry_succeeds() -> None:
    """A failed fetch leaves the cache empty; the next decode refetches and succeeds."""
    schema_json = json.dumps(_TEST_SCHEMA)
    payload = _encode_record(_TEST_SCHEMA, _TEST_RECORD)
    calls = 0

    async def flaky_fetcher(schema_id: str) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("transient")
        return schema_json

    codec = AvroCodec(flaky_fetcher)
    with pytest.raises(SchemaFetchError):
        await codec.decode("schema-1", payload)
    assert codec.cache_size() == 0

    result = await codec.decode("schema-1", payload)
    assert result == _TEST_RECORD
    assert calls == 2


@pytest.mark.asyncio
async def test_malformed_payload_with_valid_schema_is_not_schema_fetch_error() -> None:
    """A payload decode failure with a valid (cached) schema is NOT SchemaFetchError.

    This is the poison-payload case the subscribe loop skips-and-counts.
    """
    schema_json = json.dumps(_TEST_SCHEMA)

    async def fetcher(schema_id: str) -> str:
        return schema_json

    codec = AvroCodec(fetcher)
    # Warm the cache with a good decode first.
    await codec.decode("schema-1", _encode_record(_TEST_SCHEMA, _TEST_RECORD))

    with pytest.raises(Exception) as excinfo:
        await codec.decode("schema-1", b"\xff\xff\xff not valid avro")
    assert not isinstance(excinfo.value, SchemaFetchError)
