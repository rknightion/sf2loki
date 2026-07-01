"""Avro schema-registry cache and payload decoder for Salesforce Pub/Sub events.

Each Salesforce schema is identified by a stable schema_id string; the schema
itself (JSON) is fetched once via *fetch_schema* and then cached forever —
schemas are immutable per ID so caching is unconditionally safe.
"""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import Awaitable, Callable
from typing import Any, cast

import fastavro


class SchemaFetchError(Exception):
    """The Avro schema for a schema_id could not be fetched or parsed.

    Raised when the *fetch_schema* callback fails (e.g. a GetSchema gRPC/transport
    error) or the returned schema JSON is unparseable. Deliberately distinct from
    a payload decode error: the subscribe loop poison-skips payload errors, but a
    schema-fetch failure must propagate — skipping events because the registry was
    briefly unreachable would advance the checkpoint past them (silent data loss).
    """


class AvroCodec:
    """Decode schemaless Avro payloads, caching parsed schemas by schema_id.

    Parameters
    ----------
    fetch_schema:
        Async callable ``(schema_id: str) -> str`` that retrieves the Avro
        schema JSON for a given *schema_id*.  Called at most once per unique
        *schema_id* — subsequent calls use the cached parsed schema.
    """

    def __init__(self, fetch_schema: Callable[[str], Awaitable[str]]) -> None:
        self._fetch_schema = fetch_schema
        # Parsed schemas (fastavro.parse_schema output) keyed by schema_id.
        self._cache: dict[str, Any] = {}
        # Per-schema-id locks: single-flight per id (concurrent first-time
        # decodes of one schema_id fetch exactly once), while DIFFERENT ids
        # fetch concurrently — a slow fetch of schema A must not block the
        # first decode of schema B. Bounded by the number of distinct schema
        # ids, i.e. the same cardinality as the cache itself.
        self._locks: dict[str, asyncio.Lock] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def decode(self, schema_id: str, payload: bytes) -> dict[str, object]:
        """Decode a schemaless Avro *payload* using the schema for *schema_id*.

        Returns the deserialized record as a plain Python dict.
        """
        parsed = await self._get_parsed_schema(schema_id)
        # Pub/Sub payloads are always Avro records; cast the union return type.
        return cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(payload), parsed))

    def cache_size(self) -> int:
        """Return the number of schema_ids currently in the schema cache.

        Useful for exposing as a ``schema_cache_size`` metric.
        """
        return len(self._cache)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_parsed_schema(self, schema_id: str) -> Any:
        """Return the cached parsed schema for *schema_id*, fetching on miss."""
        # Fast path: check outside the lock to avoid contention on every decode.
        if schema_id in self._cache:
            return self._cache[schema_id]

        # setdefault has no await point, so it is atomic under asyncio's
        # cooperative scheduling — no guard lock needed around lock creation.
        lock = self._locks.setdefault(schema_id, asyncio.Lock())
        async with lock:
            # Double-check: another coroutine may have populated the cache
            # while we waited for the lock.
            if schema_id in self._cache:
                return self._cache[schema_id]

            try:
                schema_json = await self._fetch_schema(schema_id)
                parsed = fastavro.parse_schema(json.loads(schema_json))
            except Exception as exc:
                raise SchemaFetchError(
                    f"failed to fetch/parse Avro schema {schema_id!r}: {exc!r}"
                ) from exc
            self._cache[schema_id] = parsed
            return parsed
