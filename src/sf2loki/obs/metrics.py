"""Prometheus metrics for sf2loki.

Each Metrics instance owns its own CollectorRegistry so tests can construct
fresh instances without triggering duplicate-timeseries errors on the
default global registry.
"""

from __future__ import annotations

import threading
import wsgiref.simple_server

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)


class Metrics:
    """Container for all sf2loki Prometheus metrics."""

    def __init__(
        self,
        *,
        version: str = "0.1.0",
        registry: CollectorRegistry | None = None,
    ) -> None:
        self.registry: CollectorRegistry = registry if registry is not None else CollectorRegistry()

        self.events_ingested = Counter(
            "sf2loki_events_ingested",
            "Total events ingested from Salesforce sources",
            ["source", "event_type"],
            registry=self.registry,
        )

        self.decode_errors = Counter(
            "sf2loki_decode_errors",
            "Total Avro/payload decode errors",
            ["reason"],
            registry=self.registry,
        )

        self.loki_push = Counter(
            "sf2loki_loki_push",
            "Total Loki push attempts",
            ["outcome"],
            registry=self.registry,
        )

        self.loki_push_duration = Histogram(
            "sf2loki_loki_push_duration_seconds",
            "Duration of Loki push requests in seconds",
            registry=self.registry,
        )

        self.loki_bytes_pushed = Counter(
            "sf2loki_loki_bytes_pushed",
            "Total bytes pushed to Loki",
            registry=self.registry,
        )

        self.ingest_lag = Gauge(
            "sf2loki_ingest_lag_seconds",
            "Difference between ingest time and event EventDate, per event type (SLI)",
            ["event_type"],
            registry=self.registry,
        )

        self.last_replay_commit_ts = Gauge(
            "sf2loki_last_replay_commit_timestamp_seconds",
            "Unix timestamp of the last committed replay_id, per topic",
            ["topic"],
            registry=self.registry,
        )

        self.pubsub_pending_credits = Gauge(
            "sf2loki_pubsub_pending_credits",
            "Pending flow-control credits outstanding to Salesforce Pub/Sub, per topic",
            ["topic"],
            registry=self.registry,
        )

        self.pubsub_reconnects = Counter(
            "sf2loki_pubsub_reconnects",
            "Total Pub/Sub stream reconnects, per topic",
            ["topic"],
            registry=self.registry,
        )

        self.watermark_ts = Gauge(
            "sf2loki_watermark_timestamp_seconds",
            "Unix timestamp of the current polling watermark, per source and object",
            ["source", "object"],
            registry=self.registry,
        )

        self.auth_refreshes = Counter(
            "sf2loki_auth_refreshes",
            "Total Salesforce OAuth token refreshes",
            registry=self.registry,
        )

        self.auth_errors = Counter(
            "sf2loki_auth_errors",
            "Total Salesforce auth errors",
            registry=self.registry,
        )

        self.schema_cache_size = Gauge(
            "sf2loki_schema_cache_size",
            "Current number of Avro schemas in the codec cache",
            registry=self.registry,
        )

        self.queue_depth = Gauge(
            "sf2loki_queue_depth",
            "Current depth of the internal event queue",
            registry=self.registry,
        )

        # --- EventLogFile source (Phase 3) ---
        self.eventlogfile_files_processed = Counter(
            "sf2loki_eventlogfile_files_processed",
            "Total EventLogFile records downloaded and parsed, per event type",
            ["event_type"],
            registry=self.registry,
        )

        self.eventlogfile_rows_ingested = Counter(
            "sf2loki_eventlogfile_rows_ingested",
            "Total CSV rows ingested from EventLogFiles, per event type",
            ["event_type"],
            registry=self.registry,
        )

        self.eventlogfile_download_bytes = Counter(
            "sf2loki_eventlogfile_download_bytes",
            "Total bytes downloaded from EventLogFile LogFile endpoints, per event type",
            ["event_type"],
            registry=self.registry,
        )

        self.eventlogfile_download_errors = Counter(
            "sf2loki_eventlogfile_download_errors",
            "Total EventLogFile listing/download errors, per reason",
            ["reason"],
            registry=self.registry,
        )

        self.build_info = Gauge(
            "sf2loki_build_info",
            "Build metadata; value is always 1",
            ["version"],
            registry=self.registry,
        )
        self.build_info.labels(version=version).set(1)


def start_metrics_server(
    addr: str, registry: CollectorRegistry
) -> tuple[wsgiref.simple_server.WSGIServer, threading.Thread]:
    """Start a Prometheus HTTP server on *addr* (e.g. ':9090' or '0.0.0.0:9090').

    Returns the (HTTPServer, Thread) tuple from prometheus_client.start_http_server so
    the caller can shut it down.
    """
    host, _, port_str = addr.rpartition(":")
    host = host or "0.0.0.0"
    port = int(port_str)
    return start_http_server(port, addr=host, registry=registry)
