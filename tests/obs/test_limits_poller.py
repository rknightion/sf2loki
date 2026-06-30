"""Tests for the org-limits poller (obs/limits_poller.py)."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from sf2loki.obs.limits_poller import LimitsPoller
from sf2loki.obs.metrics import Metrics


class _FakeClient:
    def __init__(
        self, result: dict[str, dict[str, int]] | None = None, *, err: bool = False
    ) -> None:
        self._result = result or {}
        self._err = err
        self.calls = 0

    async def fetch(self) -> dict[str, dict[str, int]]:
        self.calls += 1
        if self._err:
            raise RuntimeError("boom")
        return self._result


@pytest.mark.asyncio
async def test_poller_sets_limit_gauges() -> None:
    metrics = Metrics()
    client = _FakeClient({"DailyApiRequests": {"Max": 15000, "Remaining": 14998}})
    poller = LimitsPoller(client, metrics, timedelta(seconds=60), poll_once=True)

    await poller.run(asyncio.Event())

    assert (
        metrics.registry.get_sample_value(
            "sf2loki_salesforce_limit_max", {"limit_name": "DailyApiRequests"}
        )
        == 15000.0
    )
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_salesforce_limit_remaining", {"limit_name": "DailyApiRequests"}
        )
        == 14998.0
    )


@pytest.mark.asyncio
async def test_poller_counts_errors_and_survives() -> None:
    metrics = Metrics()
    poller = LimitsPoller(_FakeClient(err=True), metrics, timedelta(seconds=60), poll_once=True)

    await poller.run(asyncio.Event())  # must not raise

    assert metrics.registry.get_sample_value("sf2loki_salesforce_limits_poll_errors_total") == 1.0


@pytest.mark.asyncio
async def test_poller_returns_immediately_when_stopped() -> None:
    client = _FakeClient({"DailyApiRequests": {"Max": 1, "Remaining": 1}})
    poller = LimitsPoller(client, Metrics(), timedelta(seconds=60))
    stop = asyncio.Event()
    stop.set()

    await poller.run(stop)

    assert client.calls == 0
