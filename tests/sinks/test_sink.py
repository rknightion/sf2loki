"""Tests for sf2loki.sinks.loki.sink.LokiSink."""

from __future__ import annotations

import base64
import gzip
from datetime import UTC, datetime

import httpx
import pytest
import respx
from pydantic import SecretStr

from sf2loki.config import LokiBatchConfig, LokiConfig
from sf2loki.model import Batch, CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.sinks.base import PermanentSinkError, RetryableSinkError
from sf2loki.sinks.loki import sink as sink_module
from sf2loki.sinks.loki.labels import LabelGuardError
from sf2loki.sinks.loki.sink import LokiSink

# ---------------------------------------------------------------------------
# Constants & Helpers
# ---------------------------------------------------------------------------

PUSH_URL = "http://loki:3100/loki/api/v1/push"
TS = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)


def _cfg(
    *,
    encoding: str = "protobuf",
    compression: str = "snappy",
    tenant_id: str | None = None,
    auth_token: str | None = None,
) -> LokiConfig:
    return LokiConfig(
        url=PUSH_URL,
        encoding=encoding,  # type: ignore[arg-type]
        compression=compression,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        auth_token=SecretStr(auth_token) if auth_token else None,
        batch=LokiBatchConfig(max_entries=1000, max_bytes=1_048_576),
        labels={"source": "pubsub", "event_type": "LoginEventStream"},
    )


def _entry(n: int = 1) -> LogEntry:
    return LogEntry(
        timestamp=TS,
        labels={"source": "pubsub", "event_type": "LoginEventStream"},
        line=f"log line {n}",
        structured_metadata={"trace_id": f"trace-{n}"},
        checkpoint=CheckpointToken(key="pubsub:/event/Login", value=str(n)),
    )


def _batch(n: int = 1) -> Batch:
    return Batch(entries=[_entry(i) for i in range(n)])


def _make_sink(cfg: LokiConfig) -> LokiSink:
    client = httpx.AsyncClient()
    return LokiSink(cfg, client)


# ---------------------------------------------------------------------------
# guard_labels called at init
# ---------------------------------------------------------------------------


class TestInit:
    def test_bad_label_key_raises_at_init(self) -> None:
        bad_cfg = LokiConfig(
            url=PUSH_URL,
            labels={"disallowed_key": "value"},
        )
        with pytest.raises(LabelGuardError):
            LokiSink(bad_cfg, httpx.AsyncClient())

    def test_good_labels_ok(self) -> None:
        cfg = _cfg()
        sink = _make_sink(cfg)
        assert sink is not None


# ---------------------------------------------------------------------------
# Content-Type / Content-Encoding headers
# ---------------------------------------------------------------------------


class TestContentHeaders:
    @respx.mock
    async def test_protobuf_encoding_headers(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(encoding="protobuf")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        assert req.headers["Content-Type"] == "application/x-protobuf"
        assert req.headers["Content-Encoding"] == "snappy"

    @respx.mock
    async def test_json_no_compression_headers(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(encoding="json", compression="none")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        assert req.headers["Content-Type"] == "application/json"
        assert "Content-Encoding" not in req.headers

    @respx.mock
    async def test_json_gzip_compression_headers(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(encoding="json", compression="gzip")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        assert req.headers["Content-Type"] == "application/json"
        assert req.headers["Content-Encoding"] == "gzip"
        # Body should be valid gzip
        gzip.decompress(req.content)

    @respx.mock
    async def test_json_gzip_body_is_valid_json_after_decompress(self) -> None:
        import json

        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(encoding="json", compression="gzip")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        body = json.loads(gzip.decompress(req.content))
        assert "streams" in body


# ---------------------------------------------------------------------------
# Auth header modes
# ---------------------------------------------------------------------------


class TestAuthHeaders:
    @respx.mock
    async def test_auth_token_produces_basic_auth(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(tenant_id="my-tenant", auth_token="secret-token")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        auth_header = req.headers["Authorization"]
        assert auth_header.startswith("Basic ")
        decoded = base64.b64decode(auth_header[6:]).decode()
        user, pw = decoded.split(":", 1)
        assert user == "my-tenant"
        assert pw == "secret-token"

    @respx.mock
    async def test_auth_token_no_tenant_uses_empty_username(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(tenant_id=None, auth_token="secret-token")
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        auth_header = req.headers["Authorization"]
        decoded = base64.b64decode(auth_header[6:]).decode()
        user, _ = decoded.split(":", 1)
        assert user == ""

    @respx.mock
    async def test_only_tenant_id_produces_org_id_header(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(tenant_id="my-org", auth_token=None)
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        assert req.headers["X-Scope-OrgID"] == "my-org"
        assert "Authorization" not in req.headers

    @respx.mock
    async def test_no_auth_no_header(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg(tenant_id=None, auth_token=None)
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))

        req = route.calls.last.request
        assert "Authorization" not in req.headers
        assert "X-Scope-OrgID" not in req.headers


# ---------------------------------------------------------------------------
# HTTP response handling
# ---------------------------------------------------------------------------


class TestResponseHandling:
    @respx.mock
    async def test_2xx_push_returns_without_error(self) -> None:
        respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))  # no exception

    @respx.mock
    async def test_successful_push_increments_bytes_pushed(self) -> None:
        respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg()
        metrics = Metrics()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client, metrics=metrics)
            await s.push(_batch(1))

        val = metrics.registry.get_sample_value("sf2loki_loki_bytes_pushed_total")
        assert val is not None and val > 0.0

    @respx.mock
    async def test_500_repeated_raises_retryable_sink_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 2)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(return_value=httpx.Response(500))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(RetryableSinkError):
                await s.push(_batch(1))

    @respx.mock
    async def test_500_then_200_succeeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(side_effect=[httpx.Response(500), httpx.Response(204)])
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))  # succeeds on second attempt

    @respx.mock
    async def test_400_raises_permanent_sink_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(return_value=httpx.Response(400))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError):
                await s.push(_batch(1))

    @respx.mock
    async def test_400_is_not_retried(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(400))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError):
                await s.push(_batch(1))

        assert route.call_count == 1  # exactly one attempt, no retry

    @respx.mock
    async def test_429_repeated_raises_retryable_sink_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 2)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(return_value=httpx.Response(429))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(RetryableSinkError):
                await s.push(_batch(1))

    @respx.mock
    async def test_other_4xx_raises_permanent_sink_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(return_value=httpx.Response(403))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError):
                await s.push(_batch(1))


# ---------------------------------------------------------------------------
# 413 splitting
# ---------------------------------------------------------------------------


class TestSplitting:
    @respx.mock
    async def test_413_multi_entry_splits_and_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        # First POST gets 413, subsequent POSTs get 204
        route = respx.post(PUSH_URL).mock(
            side_effect=[httpx.Response(413), httpx.Response(204), httpx.Response(204)]
        )
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(4))  # 4 entries, splits into 2+2

        assert route.call_count > 1  # split triggered multiple requests

    @respx.mock
    async def test_413_single_entry_raises_permanent(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(return_value=httpx.Response(413))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError, match="unsplittable 413"):
                await s.push(_batch(1))


# ---------------------------------------------------------------------------
# Empty batch
# ---------------------------------------------------------------------------


class TestEmptyBatch:
    @respx.mock
    async def test_empty_batch_makes_no_http_call(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(Batch())

        assert route.call_count == 0


# ---------------------------------------------------------------------------
# aclose
# ---------------------------------------------------------------------------


class TestAclose:
    async def test_aclose_closes_client(self) -> None:
        cfg = _cfg()
        client = httpx.AsyncClient()
        s = LokiSink(cfg, client)
        await s.aclose()
        assert client.is_closed
