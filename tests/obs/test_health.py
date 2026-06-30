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
