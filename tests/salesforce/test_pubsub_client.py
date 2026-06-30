"""Tests for PubSubClient (salesforce/pubsub_client.py).

Uses an in-process fake gRPC servicer to exercise:
- subscribe() yields DecodedEvents with correct fields
- flow-control: top-up FetchRequest sent when pending_num_requested drains
- get_schema() returns schema_json and passes correct auth metadata
- preset_for() maps string names to pb enum values
"""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import fastavro
import grpc
import grpc.aio
import pytest

from sf2loki.auth.jwt_auth import AccessToken
from sf2loki.config import PubSubConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce._generated import pubsub_api_pb2 as pb
from sf2loki.salesforce._generated import pubsub_api_pb2_grpc as pb_grpc
from sf2loki.salesforce.pubsub_client import DecodedEvent, PubSubClient, preset_for

# ---------------------------------------------------------------------------
# Test schema + encoding helpers
# ---------------------------------------------------------------------------

_SCHEMA: dict[str, object] = {
    "type": "record",
    "name": "ApexLog",
    "fields": [
        {"name": "Id", "type": "string"},
        {"name": "EventDate", "type": "string"},
    ],
}
_SCHEMA_ID = "schema-abc"
_SCHEMA_JSON = json.dumps(_SCHEMA)
_PARSED_SCHEMA = fastavro.parse_schema(_SCHEMA)


def _encode(record: dict[str, object]) -> bytes:
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, _PARSED_SCHEMA, record)
    return buf.getvalue()


def _make_consumer_event(
    record: dict[str, object], replay_id: bytes, schema_id: str = _SCHEMA_ID
) -> pb.ConsumerEvent:
    return pb.ConsumerEvent(
        event=pb.ProducerEvent(
            id="evt-1",
            schema_id=schema_id,
            payload=_encode(record),
        ),
        replay_id=replay_id,
    )


# ---------------------------------------------------------------------------
# Fake TokenProvider
# ---------------------------------------------------------------------------


class FakeTokenProvider:
    """Minimal stub that satisfies TokenProvider's async interface."""

    def __init__(
        self,
        token_value: str = "tok-test",
        instance_url: str = "https://test.salesforce.com",
        org: str = "00Dtest000org",
    ) -> None:
        self._token = AccessToken(
            value=token_value,
            instance_url=instance_url,
            expires_at=datetime(2099, 1, 1, tzinfo=UTC),
        )
        self._org = org

    async def token(self) -> AccessToken:
        return self._token

    async def org_id(self) -> str:
        return self._org


# ---------------------------------------------------------------------------
# Fake gRPC servicers
# ---------------------------------------------------------------------------


class BasicFakeServicer(pb_grpc.PubSubServicer):
    """Servicer: returns one event batch then closes the stream.

    Records the metadata supplied by the client for later inspection.
    """

    def __init__(self, events: list[pb.ConsumerEvent], pending_after: int = 0) -> None:
        self._events = events
        self._pending_after = pending_after
        self.received_metadata: list[tuple[str, str]] = []
        self.received_fetch_requests: list[pb.FetchRequest] = []

    async def GetSchema(
        self, request: pb.SchemaRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.SchemaInfo:
        self.received_metadata = list(context.invocation_metadata())
        return pb.SchemaInfo(schema_json=_SCHEMA_JSON, schema_id=request.schema_id)

    async def Subscribe(  # type: ignore[override]
        self,
        request_iterator: AsyncIterator[pb.FetchRequest],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.FetchResponse]:
        self.received_metadata = list(context.invocation_metadata())
        async for req in request_iterator:
            self.received_fetch_requests.append(req)
            yield pb.FetchResponse(
                events=list(self._events),
                latest_replay_id=b"\x00\x01",
                rpc_id="rpc-1",
                pending_num_requested=self._pending_after,
            )
            # Close after one response so the client's async-for terminates.
            return


class MalformedPayloadFakeServicer(pb_grpc.PubSubServicer):
    """Servicer that returns one bad-payload event followed by one good event.

    Used to verify a single decode error doesn't kill the stream.
    """

    async def GetSchema(
        self, request: pb.SchemaRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.SchemaInfo:
        return pb.SchemaInfo(schema_json=_SCHEMA_JSON, schema_id=request.schema_id)

    async def Subscribe(  # type: ignore[override]
        self,
        request_iterator: AsyncIterator[pb.FetchRequest],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.FetchResponse]:
        good_record = {"Id": "ok", "EventDate": "2024-01-01T00:00:00Z"}
        async for _req in request_iterator:
            yield pb.FetchResponse(
                events=[
                    pb.ConsumerEvent(
                        event=pb.ProducerEvent(
                            id="evt-bad",
                            schema_id=_SCHEMA_ID,
                            payload=b"\xff\xff\xff not valid avro",
                        ),
                        replay_id=b"\x01",
                    ),
                    _make_consumer_event(good_record, b"\x02"),
                ],
                latest_replay_id=b"\x02",
                rpc_id="rpc-1",
                pending_num_requested=5,
            )
            return


class FlowControlFakeServicer(pb_grpc.PubSubServicer):
    """Servicer that forces a flow-control top-up by setting pending=0.

    Yields two responses:
    1. One event, pending_num_requested=0 (forces top-up).
    2. After receiving the top-up FetchRequest: one event, pending=n//2+1.
    """

    def __init__(self, n: int) -> None:
        self._n = n
        self.fetch_request_count = 0

    async def GetSchema(
        self, request: pb.SchemaRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.SchemaInfo:
        return pb.SchemaInfo(schema_json=_SCHEMA_JSON, schema_id=request.schema_id)

    async def Subscribe(  # type: ignore[override]
        self,
        request_iterator: AsyncIterator[pb.FetchRequest],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.FetchResponse]:
        record = {"Id": "r1", "EventDate": "2024-01-01T00:00:00Z"}
        async for _req in request_iterator:
            self.fetch_request_count += 1
            if self.fetch_request_count == 1:
                # First request: return one event with pending=0 to trigger top-up.
                yield pb.FetchResponse(
                    events=[_make_consumer_event(record, b"\x01")],
                    latest_replay_id=b"\x01",
                    rpc_id="rpc-1",
                    pending_num_requested=0,
                )
            elif self.fetch_request_count == 2:
                # Top-up received: return one more event and close.
                yield pb.FetchResponse(
                    events=[_make_consumer_event(record, b"\x02")],
                    latest_replay_id=b"\x02",
                    rpc_id="rpc-2",
                    pending_num_requested=self._n // 2 + 1,
                )
                return


# ---------------------------------------------------------------------------
# Pytest fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def pubsub_cfg() -> PubSubConfig:
    return PubSubConfig(
        enabled=True,
        endpoint="ignored-in-tests",
        default_num_requested=10,
        replay_preset="LATEST",
        topics=["/event/ApexCalloutEventStream"],
    )


@pytest.fixture()
def token_provider() -> FakeTokenProvider:
    return FakeTokenProvider()


async def _make_server_and_client(
    servicer: pb_grpc.PubSubServicer,
    cfg: PubSubConfig,
    tokens: FakeTokenProvider,
    metrics: Metrics | None = None,
) -> tuple[grpc.aio.Server, PubSubClient]:
    server: grpc.aio.Server = grpc.aio.server()
    pb_grpc.add_PubSubServicer_to_server(servicer, server)
    port: int = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel: grpc.aio.Channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    client = PubSubClient(cfg, tokens, channel=channel, metrics=metrics)
    return server, client


# ---------------------------------------------------------------------------
# Tests: preset_for
# ---------------------------------------------------------------------------


def test_preset_for_latest() -> None:
    assert preset_for("LATEST") == pb.LATEST


def test_preset_for_earliest() -> None:
    assert preset_for("EARLIEST") == pb.EARLIEST


def test_preset_for_custom() -> None:
    assert preset_for("CUSTOM") == pb.CUSTOM


def test_preset_for_invalid_raises() -> None:
    with pytest.raises(ValueError, match="unknown replay_preset"):
        preset_for("BOGUS")


# ---------------------------------------------------------------------------
# Tests: subscribe yields DecodedEvents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_yields_decoded_events(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """subscribe() yields DecodedEvent with correct topic/replay_id/schema_id/payload."""
    record = {"Id": "EV001", "EventDate": "2024-06-01T12:00:00Z"}
    replay_id = b"\xde\xad\xbe\xef"
    servicer = BasicFakeServicer([_make_consumer_event(record, replay_id)])
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        topic = "/event/ApexCalloutEventStream"
        events: list[DecodedEvent] = []
        async for ev in client.subscribe(topic, replay_preset=pb.LATEST):
            events.append(ev)

        assert len(events) == 1
        ev = events[0]
        assert ev.topic == topic
        assert ev.replay_id == replay_id
        assert ev.schema_id == _SCHEMA_ID
        assert ev.payload == record
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_subscribe_passes_auth_metadata(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """subscribe() sends accesstoken/instanceurl/tenantid metadata headers."""
    servicer = BasicFakeServicer([])
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        async for _ in client.subscribe("/event/X", replay_preset=pb.LATEST):
            pass

        meta = dict(servicer.received_metadata)
        assert meta.get("accesstoken") == token_provider._token.value
        assert meta.get("instanceurl") == token_provider._token.instance_url
        assert meta.get("tenantid") == token_provider._org
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: flow control top-up
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flow_control_sends_topup(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """Client sends a second FetchRequest when pending_num_requested drains to 0."""
    n = pubsub_cfg.default_num_requested
    servicer = FlowControlFakeServicer(n)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        events: list[DecodedEvent] = []
        async for ev in client.subscribe("/event/ApexCalloutEventStream", replay_preset=pb.LATEST):
            events.append(ev)

        # Should have received 2 events (one per response)
        assert len(events) == 2
        # Servicer received initial request + top-up = 2 total
        assert servicer.fetch_request_count == 2, (
            f"expected 2 FetchRequests (initial + top-up), got {servicer.fetch_request_count}"
        )
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: get_schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_schema_returns_json_and_sends_metadata(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """get_schema() returns schema_json and sends auth metadata to GetSchema RPC."""
    servicer = BasicFakeServicer([])
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        schema_json = await client.get_schema(_SCHEMA_ID)

        assert schema_json == _SCHEMA_JSON

        meta = dict(servicer.received_metadata)
        assert meta.get("accesstoken") == token_provider._token.value
        assert meta.get("instanceurl") == token_provider._token.instance_url
        assert meta.get("tenantid") == token_provider._org
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: metrics wiring
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_updates_schema_cache_size(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """Decoding an event updates the schema_cache_size gauge."""
    record = {"Id": "EV001", "EventDate": "2024-06-01T12:00:00Z"}
    servicer = BasicFakeServicer([_make_consumer_event(record, b"\x01")])
    metrics = Metrics()
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider, metrics)

    try:
        async for _ in client.subscribe("/event/X", replay_preset=pb.LATEST):
            pass
        assert metrics.registry.get_sample_value("sf2loki_schema_cache_size") == 1.0
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_subscribe_updates_pending_credits(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """Each FetchResponse updates the pubsub_pending_credits gauge for its topic."""
    record = {"Id": "EV001", "EventDate": "2024-06-01T12:00:00Z"}
    servicer = BasicFakeServicer([_make_consumer_event(record, b"\x01")], pending_after=42)
    metrics = Metrics()
    topic = "/event/ApexCalloutEventStream"
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider, metrics)

    try:
        async for _ in client.subscribe(topic, replay_preset=pb.LATEST):
            pass
        val = metrics.registry.get_sample_value("sf2loki_pubsub_pending_credits", {"topic": topic})
        assert val == 42.0
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_subscribe_skips_malformed_event_and_counts_decode_error(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """A malformed payload is skipped (counted), not fatal to the rest of the stream."""
    servicer = MalformedPayloadFakeServicer()
    metrics = Metrics()
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider, metrics)

    try:
        events: list[DecodedEvent] = []
        async for ev in client.subscribe("/event/X", replay_preset=pb.LATEST):
            events.append(ev)

        # The good event still comes through despite the bad one preceding it.
        assert len(events) == 1
        assert events[0].payload == {"Id": "ok", "EventDate": "2024-01-01T00:00:00Z"}
        decode_errors = metrics.registry.get_sample_value(
            "sf2loki_decode_errors_total", {"reason": "EOFError"}
        )
        assert decode_errors == 1.0
    finally:
        await server.stop(None)
        await client.aclose()
