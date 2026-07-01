"""Tests for obs/metrics.py."""

from __future__ import annotations

from sf2loki.obs.metrics import Metrics


def make_metrics(**kwargs: object) -> Metrics:
    return Metrics(**kwargs)  # type: ignore[arg-type]


def test_events_ingested_counter() -> None:
    m = make_metrics()
    m.events_ingested.labels(source="pubsub", event_type="LoginEventStream").inc()
    val = m.registry.get_sample_value(
        "sf2loki_events_ingested_total",
        {"source": "pubsub", "event_type": "LoginEventStream"},
    )
    assert val == 1.0


def test_decode_errors_counter() -> None:
    m = make_metrics()
    m.decode_errors.labels(reason="avro").inc(3)
    val = m.registry.get_sample_value(
        "sf2loki_decode_errors_total",
        {"reason": "avro"},
    )
    assert val == 3.0


def test_loki_push_counter() -> None:
    m = make_metrics()
    m.loki_push.labels(outcome="success").inc()
    val = m.registry.get_sample_value(
        "sf2loki_loki_push_total",
        {"outcome": "success"},
    )
    assert val == 1.0


def test_loki_push_duration_histogram() -> None:
    m = make_metrics()
    m.loki_push_duration.observe(0.5)
    count = m.registry.get_sample_value("sf2loki_loki_push_duration_seconds_count")
    assert count == 1.0


def test_loki_bytes_pushed_counter() -> None:
    m = make_metrics()
    m.loki_bytes_pushed.inc(1024)
    val = m.registry.get_sample_value("sf2loki_loki_bytes_pushed_total")
    assert val == 1024.0


def test_ingest_lag_histogram() -> None:
    m = make_metrics()
    m.ingest_lag.labels(event_type="LoginEventStream").observe(42.0)
    count = m.registry.get_sample_value(
        "sf2loki_ingest_lag_seconds_count",
        {"event_type": "LoginEventStream"},
    )
    total = m.registry.get_sample_value(
        "sf2loki_ingest_lag_seconds_sum",
        {"event_type": "LoginEventStream"},
    )
    assert count == 1.0
    assert total == 42.0


def test_last_replay_commit_ts_gauge() -> None:
    m = make_metrics()
    m.last_replay_commit_ts.labels(topic="/event/LoginEventStream").set(1_700_000_000.0)
    val = m.registry.get_sample_value(
        "sf2loki_last_replay_commit_timestamp_seconds",
        {"topic": "/event/LoginEventStream"},
    )
    assert val == 1_700_000_000.0


def test_pubsub_pending_credits_gauge() -> None:
    m = make_metrics()
    m.pubsub_pending_credits.labels(topic="/event/ApiAnomalyEvent").set(50)
    val = m.registry.get_sample_value(
        "sf2loki_pubsub_pending_credits",
        {"topic": "/event/ApiAnomalyEvent"},
    )
    assert val == 50.0


def test_pubsub_reconnects_counter() -> None:
    m = make_metrics()
    m.pubsub_reconnects.labels(topic="/event/LoginEventStream").inc()
    val = m.registry.get_sample_value(
        "sf2loki_pubsub_reconnects_total",
        {"topic": "/event/LoginEventStream"},
    )
    assert val == 1.0


def test_watermark_ts_gauge() -> None:
    m = make_metrics()
    m.watermark_ts.labels(source="eventlog_objects", object="LoginEvent").set(1_700_000_000.0)
    val = m.registry.get_sample_value(
        "sf2loki_watermark_timestamp_seconds",
        {"source": "eventlog_objects", "object": "LoginEvent"},
    )
    assert val == 1_700_000_000.0


def test_auth_refreshes_counter() -> None:
    m = make_metrics()
    m.auth_refreshes.inc(2)
    val = m.registry.get_sample_value("sf2loki_auth_refreshes_total")
    assert val == 2.0


def test_auth_errors_counter() -> None:
    m = make_metrics()
    m.auth_errors.inc()
    val = m.registry.get_sample_value("sf2loki_auth_errors_total")
    assert val == 1.0


def test_schema_cache_size_gauge() -> None:
    m = make_metrics()
    m.schema_cache_size.set(7)
    val = m.registry.get_sample_value("sf2loki_schema_cache_size")
    assert val == 7.0


def test_queue_depth_gauge() -> None:
    m = make_metrics()
    m.queue_depth.set(99)
    val = m.registry.get_sample_value("sf2loki_queue_depth")
    assert val == 99.0


def test_build_info_gauge_is_1() -> None:
    m = make_metrics(version="1.2.3")
    val = m.registry.get_sample_value("sf2loki_build_info", {"version": "1.2.3"})
    assert val == 1.0


def test_separate_registries_no_collision() -> None:
    """Two Metrics instances with separate registries must not raise."""
    m1 = make_metrics()
    m2 = make_metrics()
    m1.events_ingested.labels(source="a", event_type="X").inc()
    m2.events_ingested.labels(source="b", event_type="Y").inc()
    # No exception == pass


def test_default_version_in_build_info() -> None:
    m = make_metrics()
    val = m.registry.get_sample_value("sf2loki_build_info", {"version": "0.1.0"})
    assert val == 1.0


def test_eventlogfile_files_processed_counter() -> None:
    m = make_metrics()
    m.eventlogfile_files_processed.labels(event_type="Login").inc()
    val = m.registry.get_sample_value(
        "sf2loki_eventlogfile_files_processed_total", {"event_type": "Login"}
    )
    assert val == 1.0


def test_eventlogfile_rows_ingested_counter() -> None:
    m = make_metrics()
    m.eventlogfile_rows_ingested.labels(event_type="API").inc(42)
    val = m.registry.get_sample_value(
        "sf2loki_eventlogfile_rows_ingested_total", {"event_type": "API"}
    )
    assert val == 42.0


def test_eventlogfile_download_bytes_counter() -> None:
    m = make_metrics()
    m.eventlogfile_download_bytes.labels(event_type="Login").inc(2048)
    val = m.registry.get_sample_value(
        "sf2loki_eventlogfile_download_bytes_total", {"event_type": "Login"}
    )
    assert val == 2048.0


def test_eventlogfile_download_errors_counter() -> None:
    m = make_metrics()
    m.eventlogfile_download_errors.labels(reason="HTTPStatusError").inc()
    val = m.registry.get_sample_value(
        "sf2loki_eventlogfile_download_errors_total", {"reason": "HTTPStatusError"}
    )
    assert val == 1.0
