"""Tests for sf2loki.sinks.loki.sink.LokiSink."""

from __future__ import annotations

import base64
import gzip
from datetime import UTC, datetime

import httpx
import pytest
import respx
import structlog.testing
import tenacity
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
        labels={"job": "sf2loki", "environment": "test"},
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

    @pytest.mark.parametrize("key", ["source", "event_type"])
    def test_reserved_static_label_rejected_at_init(self, key: str) -> None:
        """`source`/`event_type` are per-entry identity labels; a static override
        would collapse all stream separation — reject at startup."""
        bad_cfg = LokiConfig(url=PUSH_URL, labels={key: "clobber"})
        with pytest.raises(LabelGuardError, match=key):
            LokiSink(bad_cfg, httpx.AsyncClient())


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
    async def test_unexpected_4xx_raises_retryable_sink_error(self) -> None:
        """Unknown 4xx must not silently drop data — retry, don't poison."""
        respx.post(PUSH_URL).mock(return_value=httpx.Response(418))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(RetryableSinkError):
                await s.push(_batch(1))


# ---------------------------------------------------------------------------
# Auth/config errors (401/403/404): retryable, loud, never poison
# ---------------------------------------------------------------------------


class TestAuthConfigErrors:
    @pytest.mark.parametrize("status", [401, 403, 404])
    @respx.mock
    async def test_auth_config_status_raises_retryable(self, status: int) -> None:
        """A rotated token / wrong URL must hold the batch (and checkpoints), not drop it."""
        respx.post(PUSH_URL).mock(return_value=httpx.Response(status))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(RetryableSinkError):
                await s.push(_batch(1))

    @respx.mock
    async def test_401_logs_error_with_status_on_first_failure(self) -> None:
        respx.post(PUSH_URL).mock(return_value=httpx.Response(401))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with structlog.testing.capture_logs() as captured:
                with pytest.raises(RetryableSinkError):
                    await s.push(_batch(1))

        errors = [e for e in captured if e["log_level"] == "error"]
        assert errors, f"no ERROR log on 401; got {captured}"
        assert errors[0]["status"] == 401

    @respx.mock
    async def test_repeated_auth_failures_log_error_rate_limited(self) -> None:
        """ERROR on the 1st failure and every Nth after — loud but not one per retry."""
        respx.post(PUSH_URL).mock(return_value=httpx.Response(403))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with structlog.testing.capture_logs() as captured:
                for _ in range(12):
                    with pytest.raises(RetryableSinkError):
                        await s.push(_batch(1))

        errors = [e for e in captured if e["log_level"] == "error"]
        assert len(errors) == 2  # 1st and 10th consecutive failure

    @respx.mock
    async def test_auth_recovery_logs_and_resets_counter(self) -> None:
        respx.post(PUSH_URL).mock(
            side_effect=[
                httpx.Response(401),
                httpx.Response(204),
                httpx.Response(401),
            ]
        )
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with structlog.testing.capture_logs() as captured:
                with pytest.raises(RetryableSinkError):
                    await s.push(_batch(1))
                await s.push(_batch(1))  # recovers
                with pytest.raises(RetryableSinkError):
                    await s.push(_batch(1))  # fresh failure streak → loud again

        errors = [e for e in captured if e["log_level"] == "error"]
        assert len(errors) == 2  # first failure of each streak
        infos = [e for e in captured if e["log_level"] == "info"]
        assert any("recovered" in e["event"] for e in infos)


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

    @respx.mock
    async def test_413_poison_entry_does_not_discard_siblings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only the middle entry is unsplittably-413; the other two must still be delivered."""
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        delivered: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = request.content
            # The poison entry (line 1) makes any batch containing it 413.
            if b"log line 1" in body:
                return httpx.Response(413)
            # Record which non-poison lines were successfully delivered.
            for n in (0, 2):
                if f"log line {n}".encode() in body:
                    delivered.append(f"line-{n}")
            return httpx.Response(204)

        respx.post(PUSH_URL).mock(side_effect=handler)
        cfg = _cfg(encoding="json", compression="none")  # inspectable bodies
        metrics = Metrics()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client, metrics=metrics)
            await s.push(_batch(3))  # entries 0,1,2 — only 1 is poison

        assert sorted(delivered) == ["line-0", "line-2"]  # siblings delivered, not discarded
        dropped = metrics.registry.get_sample_value(
            "sf2loki_loki_entries_dropped_total", {"reason": "oversized_413"}
        )
        assert dropped == 1.0  # exactly the one poison entry counted

    @respx.mock
    async def test_400_multi_entry_splits_and_isolates_poison(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A flat 400 must not discard the whole mixed batch — split like 413."""
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        delivered: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = request.content
            if b"log line 1" in body:  # the poison entry 400s any batch containing it
                return httpx.Response(400)
            for n in (0, 2):
                if f"log line {n}".encode() in body:
                    delivered.append(f"line-{n}")
            return httpx.Response(204)

        respx.post(PUSH_URL).mock(side_effect=handler)
        cfg = _cfg(encoding="json", compression="none")
        metrics = Metrics()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client, metrics=metrics)
            await s.push(_batch(3))  # entries 0,1,2 — only 1 is poison

        assert sorted(delivered) == ["line-0", "line-2"]
        dropped = metrics.registry.get_sample_value(
            "sf2loki_loki_entries_dropped_total", {"reason": "bad_request"}
        )
        assert dropped == 1.0

    @respx.mock
    async def test_400_single_entry_raises_permanent_with_reason(self) -> None:
        respx.post(PUSH_URL).mock(return_value=httpx.Response(400))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError) as exc_info:
                await s.push(_batch(1))
        assert exc_info.value.reason == "bad_request"

    @respx.mock
    async def test_413_single_entry_permanent_reason(self) -> None:
        respx.post(PUSH_URL).mock(return_value=httpx.Response(413))
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            with pytest.raises(PermanentSinkError) as exc_info:
                await s.push(_batch(1))
        assert exc_info.value.reason == "oversized_413"


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
# Retry-After honoured on 429/503
# ---------------------------------------------------------------------------


def _retry_state_with_exception(exc: BaseException) -> tenacity.RetryCallState:
    state = tenacity.RetryCallState(retry_object=None, fn=None, args=(), kwargs={})
    try:
        raise exc
    except BaseException:
        import sys

        state.set_exception(sys.exc_info())  # type: ignore[arg-type]
    return state


class TestRetryAfter:
    def test_parse_seconds(self) -> None:
        assert sink_module._parse_retry_after("7") == 7.0

    def test_parse_fractional_seconds(self) -> None:
        assert sink_module._parse_retry_after("0.5") == 0.5

    def test_parse_http_date_falls_back_to_none(self) -> None:
        assert sink_module._parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") is None

    def test_parse_none_and_negative(self) -> None:
        assert sink_module._parse_retry_after(None) is None
        assert sink_module._parse_retry_after("-3") == 0.0

    def test_wait_uses_retry_after_when_longer_than_backoff(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)
        state = _retry_state_with_exception(
            sink_module._TransientError("HTTP 429", retry_after=7.0)
        )
        assert sink_module._compute_wait(state) == 7.0

    def test_wait_caps_retry_after(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)
        state = _retry_state_with_exception(
            sink_module._TransientError("HTTP 429", retry_after=3600.0)
        )
        assert sink_module._compute_wait(state) == sink_module._RETRY_AFTER_CAP

    def test_wait_falls_back_to_backoff_without_retry_after(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)
        state = _retry_state_with_exception(sink_module._TransientError("HTTP 500"))
        assert sink_module._compute_wait(state) == 0.0

    @respx.mock
    async def test_429_with_retry_after_zero_still_retries_then_succeeds(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """End-to-end: the header is read off the response and doesn't break the retry loop."""
        monkeypatch.setattr(sink_module, "_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(sink_module, "_WAIT_MIN", 0.0)
        monkeypatch.setattr(sink_module, "_WAIT_MAX", 0.0)

        respx.post(PUSH_URL).mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0"}),
                httpx.Response(204),
            ]
        )
        cfg = _cfg()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(_batch(1))  # succeeds on the second attempt


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


# ---------------------------------------------------------------------------
# Per-line byte cap (max_line_bytes)
# ---------------------------------------------------------------------------


def _cfg_line_cap(max_line_bytes: int) -> LokiConfig:
    return LokiConfig(
        url=PUSH_URL,
        encoding="json",
        compression="none",
        batch=LokiBatchConfig(max_line_bytes=max_line_bytes),
        labels={"job": "sf2loki", "environment": "test"},
    )


class TestLineCap:
    @respx.mock
    async def test_oversized_line_truncated_before_push(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg_line_cap(200)
        big = "x" * 5000
        entry = LogEntry(
            timestamp=TS,
            labels={"source": "eventlogfile", "event_type": "API"},
            line=big,
            structured_metadata={},
            checkpoint=CheckpointToken(key="eventlogfile:API", value="1"),
        )
        metrics = Metrics()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client, metrics=metrics)
            await s.push(Batch(entries=[entry]))

        body = route.calls.last.request.content.decode("utf-8")
        # The 5000-char line must not appear in full; a truncation marker should.
        assert "x" * 5000 not in body
        assert "truncated" in body
        # The entry was mutated in place to the capped line.
        assert len(entry.line.encode("utf-8")) <= 200
        assert (
            metrics.registry.get_sample_value(
                "sf2loki_lines_truncated_total", {"source": "eventlogfile"}
            )
            == 1.0
        )

    @respx.mock
    async def test_under_cap_line_untouched(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg_line_cap(262144)
        entry = LogEntry(
            timestamp=TS,
            labels={"source": "eventlogfile", "event_type": "API"},
            line="small line",
            structured_metadata={},
            checkpoint=CheckpointToken(key="eventlogfile:API", value="1"),
        )
        metrics = Metrics()
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client, metrics=metrics)
            await s.push(Batch(entries=[entry]))

        assert entry.line == "small line"
        assert route.call_count == 1
        assert (
            metrics.registry.get_sample_value(
                "sf2loki_lines_truncated_total", {"source": "eventlogfile"}
            )
            is None
        )

    @respx.mock
    async def test_cap_disabled_when_zero(self) -> None:
        route = respx.post(PUSH_URL).mock(return_value=httpx.Response(204))
        cfg = _cfg_line_cap(0)
        big = "y" * 5000
        entry = LogEntry(
            timestamp=TS,
            labels={"source": "eventlogfile", "event_type": "API"},
            line=big,
            structured_metadata={},
            checkpoint=CheckpointToken(key="eventlogfile:API", value="1"),
        )
        async with httpx.AsyncClient() as client:
            s = LokiSink(cfg, client)
            await s.push(Batch(entries=[entry]))

        assert entry.line == big  # untouched
        body = route.calls.last.request.content.decode("utf-8")
        assert "y" * 5000 in body
