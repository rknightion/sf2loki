"""Salesforce Pub/Sub API gRPC client.

Wraps the generated PubSubStub to provide:
- Authenticated bidi-streaming subscriptions with automatic flow-control top-ups.
- Schema fetching via GetSchema (delegated to AvroCodec for caching/decoding).
- A single :class:`DecodedEvent` dataclass as the public output type.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import grpc
import grpc.aio

from sf2loki.config import PubSubConfig
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce._generated import pubsub_api_pb2 as pb
from sf2loki.salesforce._generated import pubsub_api_pb2_grpc as pb_grpc
from sf2loki.salesforce.avro_codec import AvroCodec, SchemaFetchError

if TYPE_CHECKING:
    from sf2loki.auth.jwt_auth import AccessToken, TokenProvider

# Low-watermark fraction: send a top-up when outstanding credits drop below
# this fraction of the originally requested batch size.
_TOPUP_THRESHOLD_FRACTION = 0.5

# Application-level watchdog: Salesforce guarantees SOME FetchResponse (events
# or an empty keepalive batch with latest_replay_id) at least every 270s while
# flow-control credits are pending. Silence beyond this means a dead (half-open)
# stream, so it is torn down and the source reconnects.
STREAM_STALL_TIMEOUT_SECONDS = 300.0

# HTTP/2 keepalive pings on the owned channel: detect dead transports (NAT/LB
# idle drops) below the application watchdog, without waiting the full 300s.
_CHANNEL_OPTIONS: list[tuple[str, int]] = [
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", 1),
    ("grpc.http2.max_pings_without_data", 0),
]


class StreamStalledError(TimeoutError):
    """No FetchResponse arrived within the stall timeout; stream presumed dead."""


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def preset_for(name: str) -> int:
    """Map a replay_preset config string to the corresponding pb enum value.

    Parameters
    ----------
    name:
        One of ``"LATEST"``, ``"EARLIEST"``, or ``"CUSTOM"`` (case-sensitive).

    Raises
    ------
    ValueError
        If *name* is not a recognised preset name.
    """
    _MAP: dict[str, int] = {
        "LATEST": pb.LATEST,  # type: ignore[attr-defined]
        "EARLIEST": pb.EARLIEST,  # type: ignore[attr-defined]
        "CUSTOM": pb.CUSTOM,  # type: ignore[attr-defined]
    }
    if name not in _MAP:
        raise ValueError(f"unknown replay_preset {name!r}; must be LATEST, EARLIEST, or CUSTOM")
    return _MAP[name]


# ---------------------------------------------------------------------------
# DecodedEvent
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecodedEvent:
    """A single Salesforce Pub/Sub event after Avro decoding.

    Attributes
    ----------
    topic:
        The Pub/Sub topic name this event was received from.
    replay_id:
        Opaque bytes that can be passed to a subsequent Subscribe call to
        resume from this position (replay_preset=CUSTOM).
    schema_id:
        The Avro schema identifier for this event's payload.
    payload:
        The decoded event record as a plain Python dict.
    """

    topic: str
    replay_id: bytes
    schema_id: str
    payload: dict[str, object]


@dataclass(frozen=True, slots=True)
class KeepaliveEvent:
    """An empty-batch FetchResponse's ``latest_replay_id`` (Salesforce keepalive).

    Salesforce sends one at least every 270s while credits are pending and
    explicitly recommends persisting the id: it is >= every replay_id delivered
    before it, so it is a valid resume position even on a quiet topic — without
    it the stored checkpoint ages out of the 72h replay retention window.
    """

    topic: str
    latest_replay_id: bytes


# ---------------------------------------------------------------------------
# PubSubClient
# ---------------------------------------------------------------------------


class PubSubClient:
    """Async client for the Salesforce eventbus.v1.PubSub gRPC service.

    Parameters
    ----------
    cfg:
        Pub/Sub configuration (endpoint, default batch size, replay preset, etc.).
    tokens:
        Token provider supplying access tokens and org ID for gRPC metadata.
    channel:
        Optional injected :class:`grpc.aio.Channel`.  When *None* (production),
        a TLS channel to ``cfg.endpoint`` is created and owned by this client.
        Pass an insecure channel in tests to avoid TLS setup.
    stall_timeout:
        Seconds of stream silence tolerated before the watchdog declares the
        stream dead (default :data:`STREAM_STALL_TIMEOUT_SECONDS`).  Injectable
        for tests.
    """

    def __init__(
        self,
        cfg: PubSubConfig,
        tokens: TokenProvider,
        *,
        channel: grpc.aio.Channel | None = None,
        metrics: Metrics | None = None,
        stall_timeout: float = STREAM_STALL_TIMEOUT_SECONDS,
    ) -> None:
        self._cfg = cfg
        self._tokens = tokens
        self._metrics = metrics if metrics is not None else Metrics()
        self._stall_timeout = stall_timeout

        if channel is None:
            # Production path: the TLS channel is created LAZILY on first use
            # (see _stub()). grpc.aio binds a channel to the event loop that is
            # current when it is created; App.build() constructs this client
            # synchronously *before* the asyncio/uvloop loop starts, so an
            # eagerly-created channel would bind to the wrong loop and Subscribe
            # would silently never deliver. Deferring creation to first use
            # guarantees it binds to the running loop.
            self._channel: grpc.aio.Channel | None = None
            self._owns_channel = True
        else:
            # Injected channel (tests): do not close on aclose().
            self._channel = channel
            self._owns_channel = False

        self._stub_cached: Any = (
            pb_grpc.PubSubStub(channel)  # type: ignore[no-untyped-call]
            if channel is not None
            else None
        )
        self._codec = AvroCodec(self.get_schema)

    def _stub(self) -> Any:
        """Return the gRPC stub, creating the owned channel lazily on first use.

        Must be called from within the running event loop so the channel binds
        to it (see the note in __init__).
        """
        if self._stub_cached is None:
            creds = grpc.ssl_channel_credentials()
            self._channel = grpc.aio.secure_channel(
                self._cfg.endpoint, creds, options=_CHANNEL_OPTIONS
            )
            self._stub_cached = pb_grpc.PubSubStub(self._channel)  # type: ignore[no-untyped-call]
        return self._stub_cached

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_schema(self, schema_id: str) -> str:
        """Fetch the Avro schema JSON for *schema_id* via GetSchema RPC.

        This is also the fetch_schema callback wired into :class:`AvroCodec`,
        so the codec calls it on cache miss — callers rarely need it directly.
        """
        try:
            resp = await self._stub().GetSchema(
                pb.SchemaRequest(schema_id=schema_id),  # type: ignore[attr-defined]
                metadata=await self._metadata(),
            )
        except grpc.aio.AioRpcError as exc:
            self._handle_rpc_error(exc)
            raise
        return resp.schema_json  # type: ignore[no-any-return]

    async def get_topic(self, topic: str) -> None:
        """Probe *topic* via the GetTopic RPC without subscribing.

        Used by ``sf2loki doctor`` to verify per-topic Pub/Sub reachability
        (a missing RTEM entitlement or a mistyped channel name surfaces as a
        gRPC error here, e.g. ``NOT_FOUND``, instead of only being discovered
        once a real subscription is attempted). Raises the underlying
        :class:`grpc.aio.AioRpcError` on failure; callers handle it.
        """
        try:
            await self._stub().GetTopic(
                pb.TopicRequest(topic_name=topic),  # type: ignore[attr-defined]
                metadata=await self._metadata(),
            )
        except grpc.aio.AioRpcError as exc:
            self._handle_rpc_error(exc)
            raise

    async def subscribe(
        self,
        topic: str,
        *,
        replay_preset: int,
        replay_id: bytes = b"",
        num_requested: int | None = None,
    ) -> AsyncIterator[DecodedEvent | KeepaliveEvent]:
        """Subscribe to *topic* and yield decoded events and keepalive markers.

        Parameters
        ----------
        topic:
            Fully-qualified Pub/Sub topic name (e.g. ``/event/ApexCalloutEventStream``).
        replay_preset:
            One of the ``pb.LATEST``, ``pb.EARLIEST``, or ``pb.CUSTOM`` enum values.
        replay_id:
            When *replay_preset* is ``pb.CUSTOM``, the replay position to start from.
        num_requested:
            Initial batch size.  Defaults to ``cfg.default_num_requested``.

        Yields
        ------
        DecodedEvent
            One per Salesforce event received on the stream.
        KeepaliveEvent
            One per empty-batch FetchResponse carrying a ``latest_replay_id``
            (Salesforce's keepalive) so callers can advance their checkpoint
            on quiet topics.

        Raises
        ------
        StreamStalledError
            When no FetchResponse of any kind arrives within *stall_timeout*
            seconds — the stream is presumed dead (half-open connection).
        """
        n = num_requested if num_requested is not None else self._cfg.default_num_requested
        low_watermark = n // 2

        call: grpc.aio.StreamStreamCall[Any, Any] | None = None
        try:
            call = self._stub().Subscribe(metadata=await self._metadata())
            assert call is not None  # narrow for the type checker

            # Initial FetchRequest identifies the topic and replay position.
            await call.write(
                pb.FetchRequest(  # type: ignore[attr-defined]
                    topic_name=topic,
                    replay_preset=replay_preset,
                    replay_id=replay_id,
                    num_requested=n,
                )
            )

            responses = call.__aiter__()
            while True:
                # Watchdog: Salesforce guarantees SOME FetchResponse (events or
                # keepalive) at least every 270s while credits are pending, so
                # silence beyond the stall timeout means a dead stream.
                try:
                    response = await asyncio.wait_for(
                        responses.__anext__(), timeout=self._stall_timeout
                    )
                except StopAsyncIteration:
                    break
                except TimeoutError:
                    self._metrics.pubsub_stream_stalls.labels(topic=topic).inc()
                    raise StreamStalledError(
                        f"no FetchResponse on {topic} for {self._stall_timeout}s; "
                        "stream presumed dead"
                    ) from None

                for ce in response.events:
                    schema_id: str = ce.event.schema_id
                    try:
                        decoded_payload = await self._codec.decode(schema_id, ce.event.payload)
                    except SchemaFetchError:
                        # Schema fetch/parse failed (e.g. GetSchema RPC error).
                        # NOT a poison payload: skipping would advance the
                        # checkpoint past the event (silent loss). Propagate so
                        # the source reconnects and replays from the checkpoint.
                        raise
                    except Exception as exc:
                        # One malformed event must not kill the whole topic stream.
                        self._metrics.decode_errors.labels(reason=type(exc).__name__).inc()
                        continue
                    self._metrics.schema_cache_size.set(self._codec.cache_size())
                    yield DecodedEvent(
                        topic=topic,
                        replay_id=ce.replay_id,
                        schema_id=schema_id,
                        payload=decoded_payload,
                    )

                if not response.events and response.latest_replay_id:
                    # Keepalive: empty batch whose latest_replay_id is a valid
                    # resume position — surface it so quiet topics can still
                    # advance their checkpoint.
                    yield KeepaliveEvent(topic=topic, latest_replay_id=response.latest_replay_id)

                self._metrics.pubsub_pending_credits.labels(topic=topic).set(
                    response.pending_num_requested
                )

                # Flow-control: top up when outstanding credits drop to or below
                # the low watermark so the server never runs dry.  This fires on
                # every response (including keepalives), which also satisfies
                # Salesforce's rule that a FetchRequest must arrive within 60s
                # of pending credits hitting 0.  Guard against writing to a
                # stream the server has already half-closed.
                if response.pending_num_requested <= low_watermark:
                    try:
                        await call.write(
                            pb.FetchRequest(  # type: ignore[attr-defined]
                                topic_name=topic,
                                num_requested=n,
                            )
                        )
                    except asyncio.InvalidStateError:
                        # Stream already half-closed by the server — no more
                        # events coming; stop cleanly.
                        break
                    except grpc.aio.AioRpcError as exc:
                        # The server closed the stream between responses (a
                        # benign end-of-stream on the write side). Still
                        # invalidate on an auth rejection so the reconnect
                        # re-mints, then stop cleanly.
                        self._handle_rpc_error(exc)
                        break
        except grpc.aio.AioRpcError as exc:
            # The RPC terminated — most commonly Salesforce returning
            # UNAUTHENTICATED once the access token backing the subscription
            # expires server-side. Invalidate the cached token so the caller's
            # reconnect mints a fresh one, then re-raise to trigger that
            # reconnect/backoff. Without the invalidation the reconnect would
            # re-present the dead token and the stream would never recover.
            self._handle_rpc_error(exc)
            raise
        finally:
            # Cancel the RPC so it doesn't linger when the generator is closed
            # mid-iteration (GeneratorExit via the stop path) or torn down by
            # the watchdog. A no-op on an already-finished call.
            if call is not None:
                call.cancel()

    async def aclose(self) -> None:
        """Close the underlying gRPC channel (only if owned + actually created)."""
        if self._owns_channel and self._channel is not None:
            await self._channel.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _handle_rpc_error(self, exc: grpc.aio.AioRpcError) -> None:
        """Invalidate the cached token on an auth rejection.

        Mirrors the 401 handling in the REST-based Salesforce clients: an
        ``UNAUTHENTICATED`` gRPC status means the access token is no longer
        accepted, so the cache is cleared and the next :meth:`_metadata` call
        (on reconnect) mints a fresh token.
        """
        if exc.code() == grpc.StatusCode.UNAUTHENTICATED:
            self._tokens.invalidate()

    async def _metadata(self) -> list[tuple[str, str]]:
        """Build the gRPC metadata list required for every Salesforce RPC call."""
        tok: AccessToken = await self._tokens.token()
        org: str = await self._tokens.org_id()
        return [
            ("accesstoken", tok.value),
            ("instanceurl", tok.instance_url),
            ("tenantid", org),
        ]
