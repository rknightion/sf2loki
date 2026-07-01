"""Metrics.for_org: per-org label injection + zero-overhead single-org passthrough."""

from __future__ import annotations

from sf2loki.obs.metrics import Metrics


def test_empty_org_returns_same_instance() -> None:
    m = Metrics()
    assert m.for_org("") is m  # single-org / legacy: no proxy, no org label


def test_counter_inc_injects_org() -> None:
    m = Metrics()
    m.for_org("prod").auth_refreshes.inc()
    assert m.registry.get_sample_value("sf2loki_auth_refreshes_total", {"org": "prod"}) == 1.0


def test_counter_labels_injects_org_alongside_other_labels() -> None:
    m = Metrics()
    m.for_org("emea").events_ingested.labels(source="pubsub", event_type="Login").inc()
    got = m.registry.get_sample_value(
        "sf2loki_events_ingested_total",
        {"source": "pubsub", "event_type": "Login", "org": "emea"},
    )
    assert got == 1.0


def test_gauge_set_injects_org() -> None:
    m = Metrics()
    m.for_org("prod").queue_depth.set(7)
    assert m.registry.get_sample_value("sf2loki_queue_depth", {"org": "prod"}) == 7.0


def test_gauge_labels_injects_org() -> None:
    m = Metrics()
    m.for_org("prod").salesforce_limit_max.labels(limit_name="DailyApiRequests").set(1000)
    got = m.registry.get_sample_value(
        "sf2loki_salesforce_limit_max", {"limit_name": "DailyApiRequests", "org": "prod"}
    )
    assert got == 1000.0


def test_histogram_observe_injects_org() -> None:
    m = Metrics()
    m.for_org("prod").loki_push_duration.observe(0.5)
    got = m.registry.get_sample_value("sf2loki_loki_push_duration_seconds_count", {"org": "prod"})
    assert got == 1.0


def test_two_orgs_are_distinct_series() -> None:
    m = Metrics()
    m.for_org("prod").auth_refreshes.inc()
    m.for_org("emea").auth_refreshes.inc(2)
    assert m.registry.get_sample_value("sf2loki_auth_refreshes_total", {"org": "prod"}) == 1.0
    assert m.registry.get_sample_value("sf2loki_auth_refreshes_total", {"org": "emea"}) == 2.0


def test_proxy_delegates_non_instrument_attributes() -> None:
    m = Metrics()
    proxy = m.for_org("prod")
    # registry / force_flush / shutdown are delegated to the wrapped instance.
    assert proxy.registry is m.registry
    proxy.force_flush()  # must not raise
