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
from sf2loki.salesforce._generated import pubsub_api_pb2 as pb
from sf2loki.salesforce._generated import pubsub_api_pb2_grpc as pb_grpc
from sf2loki.salesforce.avro_codec import AvroCodec

if TYPE_CHECKING:
    from sf2loki.auth.jwt_auth import AccessToken, TokenProvider

# Low-watermark fraction: send a top-up when outstanding credits drop below
# this fraction of the originally requested batch size.
_TOPUP_THRESHOLD_FRACTION = 0.5


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
    """

    def __init__(
        self,
        cfg: PubSubConfig,
        tokens: TokenProvider,
        *,
        channel: grpc.aio.Channel | None = None,
    ) -> None:
        self._cfg = cfg
        self._tokens = tokens

        if channel is None:
            # Production path: TLS channel owned by this client.
            creds = grpc.ssl_channel_credentials()
            self._channel: grpc.aio.Channel = grpc.aio.secure_channel(cfg.endpoint, creds)
            self._owns_channel = True
        else:
            # Injected channel (tests): do not close on aclose().
            self._channel = channel
            self._owns_channel = False

        self._stub = pb_grpc.PubSubStub(self._channel)  # type: ignore[no-untyped-call]
        self._codec = AvroCodec(self.get_schema)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get_schema(self, schema_id: str) -> str:
        """Fetch the Avro schema JSON for *schema_id* via GetSchema RPC.

        This is also the fetch_schema callback wired into :class:`AvroCodec`,
        so the codec calls it on cache miss — callers rarely need it directly.
        """
        resp = await self._stub.GetSchema(
            pb.SchemaRequest(schema_id=schema_id),  # type: ignore[attr-defined]
            metadata=await self._metadata(),
        )
        return resp.schema_json  # type: ignore[no-any-return]

    async def subscribe(
        self,
        topic: str,
        *,
        replay_preset: int,
        replay_id: bytes = b"",
        num_requested: int | None = None,
    ) -> AsyncIterator[DecodedEvent]:
        """Subscribe to *topic* and yield decoded events.

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
        """
        n = num_requested if num_requested is not None else self._cfg.default_num_requested
        low_watermark = n // 2

        call: grpc.aio.StreamStreamCall[Any, Any] = self._stub.Subscribe(
            metadata=await self._metadata()
        )

        # Initial FetchRequest identifies the topic and replay position.
        await call.write(
            pb.FetchRequest(  # type: ignore[attr-defined]
                topic_name=topic,
                replay_preset=replay_preset,
                replay_id=replay_id,
                num_requested=n,
            )
        )

        async for response in call:
            for ce in response.events:
                schema_id: str = ce.event.schema_id
                decoded_payload = await self._codec.decode(schema_id, ce.event.payload)
                yield DecodedEvent(
                    topic=topic,
                    replay_id=ce.replay_id,
                    schema_id=schema_id,
                    payload=decoded_payload,
                )

            # Flow-control: top up when outstanding credits drop to or below
            # the low watermark so the server never runs dry.  Guard against
            # writing to a stream the server has already half-closed.
            if response.pending_num_requested <= low_watermark:
                try:
                    await call.write(
                        pb.FetchRequest(  # type: ignore[attr-defined]
                            topic_name=topic,
                            num_requested=n,
                        )
                    )
                except asyncio.InvalidStateError, grpc.aio.AioRpcError:
                    # Stream already finished — no more events coming; stop.
                    break

    async def aclose(self) -> None:
        """Close the underlying gRPC channel (only if owned by this client)."""
        if self._owns_channel:
            await self._channel.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _metadata(self) -> list[tuple[str, str]]:
        """Build the gRPC metadata list required for every Salesforce RPC call."""
        tok: AccessToken = await self._tokens.token()
        org: str = await self._tokens.org_id()
        return [
            ("accesstoken", tok.value),
            ("instanceurl", tok.instance_url),
            ("tenantid", org),
        ]
