"""Tests for obs/health.py."""

from __future__ import annotations

import httpx
import pytest

from sf2loki.obs.health import Health, decide

# --- unit tests for decide() ---


def test_decide_healthz_always_ok() -> None:
    assert decide("/healthz", ready=True) == (200, "ok")
    assert decide("/healthz", ready=False) == (200, "ok")


def test_decide_readyz_when_ready() -> None:
    assert decide("/readyz", ready=True) == (200, "ready")


def test_decide_readyz_when_not_ready() -> None:
    assert decide("/readyz", ready=False) == (503, "not ready")


def test_decide_unknown_path() -> None:
    status, body = decide("/unknown", ready=True)
    assert status == 404
    assert "not found" in body.lower()


def test_decide_root_path() -> None:
    status, _ = decide("/", ready=True)
    assert status == 404


# --- integration tests ---


@pytest.mark.asyncio
async def test_health_server_liveness() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/healthz")
        assert r.status_code == 200
        assert r.text == "ok"
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_server_readyz_not_ready_by_default() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/readyz")
        assert r.status_code == 503
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_server_readyz_after_set_ready() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        h.set_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/readyz")
        assert r.status_code == 200
        assert r.text == "ready"
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_server_set_not_ready() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        h.set_ready()
        h.set_not_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/readyz")
        assert r.status_code == 503
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_server_404() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/nope")
        assert r.status_code == 404
    finally:
        await h.stop()


def test_ready_property() -> None:
    h = Health()
    assert h.ready is False
    h.set_ready()
    assert h.ready is True
    h.set_not_ready()
    assert h.ready is False


# --- readiness degradation (issue #17) ---


def test_decide_readyz_degraded_returns_503_with_reason() -> None:
    reason = "degraded: loki pushes failing for 17m"
    assert decide("/readyz", ready=True, degraded_reason=reason) == (503, reason)


def test_decide_readyz_not_ready_wins_over_degraded() -> None:
    assert decide("/readyz", ready=False, degraded_reason="x") == (503, "not ready")


def test_decide_healthz_ignores_degraded() -> None:
    # Liveness must never degrade for sink failures — data is safe and retrying;
    # a restart would not help.
    assert decide("/healthz", ready=True, degraded_reason="x") == (200, "ok")
    assert decide("/healthz", ready=False, degraded_reason="x") == (200, "ok")


@pytest.mark.asyncio
async def test_readyz_degrades_and_recovers_via_installed_check() -> None:
    h = Health()
    reason: list[str | None] = [None]
    h.set_degraded_check(lambda: reason[0])
    await h.start(":0")
    try:
        h.set_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{h.port}/readyz")
            assert r.status_code == 200

            reason[0] = "degraded: loki pushes failing for 17m"
            r = await client.get(f"http://127.0.0.1:{h.port}/readyz")
            assert r.status_code == 503
            assert r.text == "degraded: loki pushes failing for 17m"
            # Liveness stays green throughout.
            r = await client.get(f"http://127.0.0.1:{h.port}/healthz")
            assert r.status_code == 200

            reason[0] = None  # e.g. next push succeeded
            r = await client.get(f"http://127.0.0.1:{h.port}/readyz")
            assert r.status_code == 200
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_readyz_no_degraded_check_installed_behaves_as_before() -> None:
    h = Health()
    await h.start(":0")
    try:
        h.set_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{h.port}/readyz")
        assert r.status_code == 200
        assert r.text == "ready"
    finally:
        await h.stop()


# --- hardening (D9): status reason phrase + read timeout ---


@pytest.mark.asyncio
async def test_health_response_has_reason_phrase() -> None:
    h = Health()
    await h.start(":0")
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{h.port}/healthz")
        assert r.reason_phrase == "OK"
        h.set_not_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{h.port}/readyz")
        assert r.reason_phrase == "Service Unavailable"
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_read_timeout_closes_idle_connection() -> None:
    """A client that connects and never sends must not hold the fd forever."""
    import asyncio

    h = Health(read_timeout=0.2)
    await h.start(":0")
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", h.port)
        # Send nothing; the server must close the connection after read_timeout.
        data = await asyncio.wait_for(reader.read(), timeout=2.0)
        assert data == b""  # EOF, no response bytes
        writer.close()
        await writer.wait_closed()
    finally:
        await h.stop()


# --- standby readiness reason (HA active-passive) ---


def test_decide_readyz_standby_reason() -> None:
    assert decide("/readyz", ready=False, not_ready_reason="standby") == (503, "standby")


def test_decide_healthz_ok_when_standby() -> None:
    # A passive replica is not ready, but it IS alive.
    assert decide("/healthz", ready=False, not_ready_reason="standby") == (200, "ok")


@pytest.mark.asyncio
async def test_health_server_standby_readyz_503_healthz_200() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        h.set_not_ready("standby")
        async with httpx.AsyncClient() as client:
            base = f"http://127.0.0.1:{port}"
            r = await client.get(f"{base}/readyz")
            assert r.status_code == 503
            assert r.text == "standby"
            r = await client.get(f"{base}/healthz")
            assert r.status_code == 200
            assert r.text == "ok"
    finally:
        await h.stop()


@pytest.mark.asyncio
async def test_health_set_ready_clears_standby() -> None:
    h = Health()
    await h.start(":0")
    port = h.port
    try:
        h.set_not_ready("standby")
        h.set_ready()
        async with httpx.AsyncClient() as client:
            r = await client.get(f"http://127.0.0.1:{port}/readyz")
        assert r.status_code == 200
        assert r.text == "ready"
    finally:
        await h.stop()
