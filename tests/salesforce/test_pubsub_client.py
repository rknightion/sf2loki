"""Tests for PubSubClient (salesforce/pubsub_client.py).

Uses an in-process fake gRPC servicer to exercise:
- subscribe() yields DecodedEvents with correct fields
- flow-control: top-up FetchRequest sent when pending_num_requested drains
- get_schema() returns schema_json and passes correct auth metadata
- preset_for() maps string names to pb enum values
"""

from __future__ import annotations

import asyncio
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
from sf2loki.salesforce.avro_codec import SchemaFetchError
from sf2loki.salesforce.pubsub_client import (
    DecodedEvent,
    KeepaliveEvent,
    PubSubClient,
    StreamStalledError,
    preset_for,
)

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
        self.invalidate_calls = 0

    async def token(self) -> AccessToken:
        return self._token

    async def org_id(self) -> str:
        return self._org

    def invalidate(self) -> None:
        self.invalidate_calls += 1


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


class AbortingFakeServicer(pb_grpc.PubSubServicer):
    """Servicer that aborts every RPC with a configurable gRPC status code.

    Used to verify auth-failure handling: an UNAUTHENTICATED abort mid-stream
    is what Salesforce returns once the access token backing the subscription
    expires server-side.
    """

    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    async def GetSchema(
        self, request: pb.SchemaRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.SchemaInfo:
        await context.abort(self._code, "rejected")
        return pb.SchemaInfo()  # pragma: no cover - abort() never returns

    async def Subscribe(  # type: ignore[override]
        self,
        request_iterator: AsyncIterator[pb.FetchRequest],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.FetchResponse]:
        # Consume the initial FetchRequest, then reject — mirrors a token that
        # was accepted at subscribe time but rejected once expired.
        async for _req in request_iterator:
            break
        await context.abort(self._code, "rejected")
        yield pb.FetchResponse()  # pragma: no cover - abort() never returns


class SchemaFetchFailingServicer(pb_grpc.PubSubServicer):
    """GetSchema fails with UNAVAILABLE while Subscribe delivers events.

    Models a transient schema-registry outage during an otherwise healthy
    stream: the events must NOT be poison-skipped (that would advance the
    checkpoint past them = loss); the stream must die so the source replays.
    """

    async def GetSchema(
        self, request: pb.SchemaRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.SchemaInfo:
        await context.abort(grpc.StatusCode.UNAVAILABLE, "schema registry down")
        return pb.SchemaInfo()  # pragma: no cover - abort() never returns

    async def Subscribe(  # type: ignore[override]
        self,
        request_iterator: AsyncIterator[pb.FetchRequest],
        context: grpc.aio.ServicerContext[Any, Any],
    ) -> AsyncIterator[pb.FetchResponse]:
        record = {"Id": "ok", "EventDate": "2024-01-01T00:00:00Z"}
        async for _req in request_iterator:
            yield pb.FetchResponse(
                events=[_make_consumer_event(record, b"\x01")],
                latest_replay_id=b"\x01",
                rpc_id="rpc-1",
                pending_num_requested=5,
            )
            return


class HangingFakeServicer(pb_grpc.PubSubServicer):
    """Servicer that accepts the subscription but never sends any response.

    Models a half-open connection: the client sees no error and no data.
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
        async for _req in request_iterator:
            await asyncio.sleep(3600)
        yield pb.FetchResponse()  # pragma: no cover - never reached


class _RecordingFakeCall:
    """Fake StreamStreamCall: yields queued responses, then hangs; records cancel()."""

    def __init__(self, responses: list[pb.FetchResponse]) -> None:
        self._responses = responses
        self.cancelled = False
        self._hang = asyncio.Event()

    async def write(self, request: pb.FetchRequest) -> None:
        return None

    def cancel(self) -> bool:
        self.cancelled = True
        self._hang.set()
        return True

    def __aiter__(self) -> AsyncIterator[pb.FetchResponse]:
        return self._gen()

    async def _gen(self) -> AsyncIterator[pb.FetchResponse]:
        for r in self._responses:
            yield r
        await self._hang.wait()


class _FakeStub:
    """Stub double returning a pre-built fake call from Subscribe."""

    def __init__(self, call: _RecordingFakeCall) -> None:
        self._call = call

    def Subscribe(self, metadata: object = None) -> _RecordingFakeCall:
        return self._call


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
    stall_timeout: float | None = None,
) -> tuple[grpc.aio.Server, PubSubClient]:
    server: grpc.aio.Server = grpc.aio.server()
    pb_grpc.add_PubSubServicer_to_server(servicer, server)
    port: int = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel: grpc.aio.Channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    kwargs: dict[str, Any] = {}
    if stall_timeout is not None:
        kwargs["stall_timeout"] = stall_timeout
    client = PubSubClient(cfg, tokens, channel=channel, metrics=metrics, **kwargs)
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


def test_channel_created_lazily_not_in_init(
    pubsub_cfg: PubSubConfig,
    token_provider: FakeTokenProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The gRPC channel must NOT be created in __init__ (channel=None case).

    App.build() constructs PubSubClient synchronously, before the event loop
    starts. grpc.aio binds a channel to the loop current at creation time, so an
    eagerly-created channel binds to the wrong loop and Subscribe silently never
    delivers. The channel must be created lazily, inside the running loop.
    """
    import grpc.aio as grpc_aio

    created: list[object] = []
    monkeypatch.setattr(
        grpc_aio, "secure_channel", lambda *a, **k: created.append((a, k)) or object()
    )
    client = PubSubClient(pubsub_cfg, token_provider)  # channel=None -> production path
    assert created == [], "grpc channel was created eagerly in __init__"
    assert client._channel is None


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
# Tests: auth-failure handling (token invalidation on UNAUTHENTICATED)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_invalidates_token_on_unauthenticated(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """An UNAUTHENTICATED stream error invalidates the cached token.

    Salesforce rejects the subscription once its backing access token expires
    server-side. The token cache must be cleared so the source's reconnect
    re-mints a fresh token — otherwise every reconnect re-presents the dead
    token and the stream never recovers.
    """
    servicer = AbortingFakeServicer(grpc.StatusCode.UNAUTHENTICATED)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            async for _ in client.subscribe("/event/X", replay_preset=pb.LATEST):
                pass
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert token_provider.invalidate_calls == 1
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_subscribe_does_not_invalidate_on_non_auth_error(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """A non-auth stream error (e.g. UNAVAILABLE) must NOT invalidate the token.

    Re-minting on every transient transport hiccup would hammer the token
    endpoint pointlessly; only genuine auth rejections clear the cache.
    """
    servicer = AbortingFakeServicer(grpc.StatusCode.UNAVAILABLE)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        with pytest.raises(grpc.aio.AioRpcError):
            async for _ in client.subscribe("/event/X", replay_preset=pb.LATEST):
                pass
        assert token_provider.invalidate_calls == 0
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_get_schema_invalidates_token_on_unauthenticated(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """GetSchema also invalidates the token on an UNAUTHENTICATED rejection."""
    servicer = AbortingFakeServicer(grpc.StatusCode.UNAUTHENTICATED)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        with pytest.raises(grpc.aio.AioRpcError):
            await client.get_schema(_SCHEMA_ID)
        assert token_provider.invalidate_calls == 1
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
        # decode_errors now carries a topic label (#43) so a losing topic is
        # identifiable; the sample is keyed by (reason, topic).
        decode_errors = metrics.registry.get_sample_value(
            "sf2loki_decode_errors_total", {"reason": "EOFError", "topic": "/event/X"}
        )
        assert decode_errors == 1.0
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: keepalive latest_replay_id (B2)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_yields_keepalive_on_empty_batch(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """An empty-batch FetchResponse with latest_replay_id yields a KeepaliveEvent.

    Salesforce sends one at least every 270s on a quiet topic and recommends
    saving the id; without it the checkpoint ages out of the 72h replay window.
    """
    servicer = BasicFakeServicer([])  # empty events + latest_replay_id=b"\x00\x01"
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        items = [ev async for ev in client.subscribe("/event/X", replay_preset=pb.LATEST)]
        keepalives = [i for i in items if isinstance(i, KeepaliveEvent)]
        assert keepalives == [KeepaliveEvent(topic="/event/X", latest_replay_id=b"\x00\x01")]
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_subscribe_no_keepalive_when_events_present(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """A batch WITH events yields only DecodedEvents (no keepalive marker)."""
    record = {"Id": "EV001", "EventDate": "2024-06-01T12:00:00Z"}
    servicer = BasicFakeServicer([_make_consumer_event(record, b"\x01")])
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        items = [ev async for ev in client.subscribe("/event/X", replay_preset=pb.LATEST)]
        assert items and all(isinstance(i, DecodedEvent) for i in items)
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: schema-fetch RPC failures propagate (B3)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_fetch_rpc_failure_kills_stream_not_skipped(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """A GetSchema transport failure propagates instead of poison-skipping the event.

    Skipping would advance the checkpoint past the event = silent loss; killing
    the stream makes the source reconnect and replay from the checkpoint.
    """
    servicer = SchemaFetchFailingServicer()
    metrics = Metrics()
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider, metrics)

    try:
        events: list[object] = []
        with pytest.raises(SchemaFetchError):
            async for ev in client.subscribe("/event/X", replay_preset=pb.LATEST):
                events.append(ev)

        assert events == []
        decode_errors = metrics.registry.get_sample_value(
            "sf2loki_decode_errors_total", {"reason": "SchemaFetchError"}
        )
        assert decode_errors is None, "schema-fetch failure was wrongly counted as poison"
    finally:
        await server.stop(None)
        await client.aclose()


# ---------------------------------------------------------------------------
# Tests: dead-stream watchdog + channel keepalive options (B4)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stalled_stream_raises_and_increments_metric(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """No FetchResponse within stall_timeout raises StreamStalledError + counts a stall."""
    servicer = HangingFakeServicer()
    metrics = Metrics()
    topic = "/event/X"
    server, client = await _make_server_and_client(
        servicer, pubsub_cfg, token_provider, metrics, stall_timeout=0.1
    )

    try:
        with pytest.raises(StreamStalledError):
            async for _ in client.subscribe(topic, replay_preset=pb.LATEST):
                pass

        val = metrics.registry.get_sample_value(
            "sf2loki_pubsub_stream_stalls_total", {"topic": topic}
        )
        assert val == 1.0
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_owned_channel_sets_keepalive_options(
    pubsub_cfg: PubSubConfig,
    token_provider: FakeTokenProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The lazily-created production channel enables HTTP/2 keepalive pings.

    Without them a half-open TCP connection (NAT/LB idle timeout) hangs a topic
    forever with zero errors.
    """
    import grpc.aio as grpc_aio

    captured: dict[str, Any] = {}
    real_insecure = grpc_aio.insecure_channel

    def fake_secure_channel(target: str, creds: object, options: object = None) -> grpc.aio.Channel:
        captured["options"] = options
        return real_insecure(target)

    monkeypatch.setattr(grpc_aio, "secure_channel", fake_secure_channel)
    client = PubSubClient(pubsub_cfg, token_provider)  # channel=None -> production path
    client._stub()

    opts = dict(captured["options"])
    assert opts["grpc.keepalive_time_ms"] == 30_000
    assert opts["grpc.keepalive_timeout_ms"] == 10_000
    assert opts["grpc.keepalive_permit_without_calls"] == 1
    assert opts["grpc.http2.max_pings_without_data"] == 0
    await client.aclose()


# ---------------------------------------------------------------------------
# Tests: generator close cancels the RPC (B8)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_generator_close_cancels_rpc(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """Closing the subscribe generator mid-iteration cancels the underlying RPC.

    The stop path closes the generator via GeneratorExit; without an explicit
    cancel the RPC lingers on the channel.
    """
    keepalive_resp = pb.FetchResponse(
        events=[], latest_replay_id=b"\x07", pending_num_requested=100
    )
    fake_call = _RecordingFakeCall([keepalive_resp])
    channel: grpc.aio.Channel = grpc.aio.insecure_channel("127.0.0.1:1")
    client = PubSubClient(pubsub_cfg, token_provider, channel=channel)
    client._stub_cached = _FakeStub(fake_call)

    try:
        agen = client.subscribe("/event/X", replay_preset=pb.LATEST)
        async for _ev in agen:
            break  # generator now suspended at the yield
        assert not fake_call.cancelled
        await agen.aclose()
        assert fake_call.cancelled, "RPC was not cancelled on generator close"
    finally:
        await channel.close()


# ---------------------------------------------------------------------------
# Tests: get_topic (used by `sf2loki doctor` to probe per-topic reachability)
# ---------------------------------------------------------------------------


class GetTopicFakeServicer(pb_grpc.PubSubServicer):
    """Servicer exercising only GetTopic: returns TopicInfo or aborts.

    Records the metadata and topic_name supplied by the client for inspection.
    """

    def __init__(
        self,
        topic_info: pb.TopicInfo | None = None,
        abort_code: grpc.StatusCode | None = None,
    ) -> None:
        self._topic_info = topic_info
        self._abort_code = abort_code
        self.received_metadata: list[tuple[str, str]] = []
        self.received_topic_name: str | None = None

    async def GetTopic(  # type: ignore[override]
        self, request: pb.TopicRequest, context: grpc.aio.ServicerContext[Any, Any]
    ) -> pb.TopicInfo:
        self.received_metadata = list(context.invocation_metadata())
        self.received_topic_name = request.topic_name
        if self._abort_code is not None:
            await context.abort(self._abort_code, "rejected")
            return pb.TopicInfo()  # pragma: no cover - abort() never returns
        assert self._topic_info is not None
        return self._topic_info


@pytest.mark.asyncio
async def test_get_topic_returns_none_and_sends_metadata(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """get_topic() succeeds (returns None) and sends the topic name + auth metadata."""
    topic = "/event/ApexCalloutEventStream"
    servicer = GetTopicFakeServicer(
        topic_info=pb.TopicInfo(topic_name=topic, can_subscribe=True, schema_id=_SCHEMA_ID)
    )
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        result = await client.get_topic(topic)
        assert result is None
        assert servicer.received_topic_name == topic
        meta = dict(servicer.received_metadata)
        assert meta.get("accesstoken") == token_provider._token.value
        assert meta.get("instanceurl") == token_provider._token.instance_url
        assert meta.get("tenantid") == token_provider._org
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_get_topic_raises_and_invalidates_on_unauthenticated(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """An UNAUTHENTICATED GetTopic rejection invalidates the cached token."""
    servicer = GetTopicFakeServicer(abort_code=grpc.StatusCode.UNAUTHENTICATED)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.get_topic("/event/DoesNotExist")
        assert exc_info.value.code() == grpc.StatusCode.UNAUTHENTICATED
        assert token_provider.invalidate_calls == 1
    finally:
        await server.stop(None)
        await client.aclose()


@pytest.mark.asyncio
async def test_get_topic_propagates_non_auth_error_without_invalidating(
    pubsub_cfg: PubSubConfig, token_provider: FakeTokenProvider
) -> None:
    """A non-auth GetTopic error (e.g. NOT_FOUND: missing RTEM entitlement/channel)
    propagates without invalidating the token."""
    servicer = GetTopicFakeServicer(abort_code=grpc.StatusCode.NOT_FOUND)
    server, client = await _make_server_and_client(servicer, pubsub_cfg, token_provider)

    try:
        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await client.get_topic("/event/DoesNotExist")
        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
        assert token_provider.invalidate_calls == 0
    finally:
        await server.stop(None)
        await client.aclose()
