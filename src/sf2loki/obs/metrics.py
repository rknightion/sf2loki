"""OpenTelemetry-native metrics for sf2loki.

All metrics are emitted via OTLP/HTTP (push). There is no Prometheus scrape
endpoint — sf2loki is OTLP-native. An always-attached in-memory reader backs a
small Prometheus-compatible read shim (:attr:`Metrics.registry`) used by tests.

Call-site ergonomics (``.labels(**kw).inc()/.set()/.observe()``) are preserved
by thin wrappers over OTel instruments, so sources/sink/pipeline/auth call sites
are unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource

if TYPE_CHECKING:
    from sf2loki.config import TelemetryConfig

# Empty attribute mapping reused for unlabelled instruments.
_NO_ATTRS: dict[str, str] = {}


# Structural protocols for the OTel instruments we use (the synchronous Gauge
# type is not publicly exported by opentelemetry.metrics, so we match by shape).
class _AddInstrument(Protocol):
    def add(self, amount: float, attributes: Mapping[str, str] | None = ...) -> None: ...


class _SetInstrument(Protocol):
    def set(self, amount: float, attributes: Mapping[str, str] | None = ...) -> None: ...


class _RecordInstrument(Protocol):
    def record(self, amount: float, attributes: Mapping[str, str] | None = ...) -> None: ...


# ---------------------------------------------------------------------------
# Thin wrappers giving OTel instruments the prometheus_client call surface.


class _BoundCounter:
    __slots__ = ("_attrs", "_instr")

    def __init__(self, instr: _AddInstrument, attrs: Mapping[str, str]) -> None:
        self._instr = instr
        self._attrs = attrs

    def inc(self, amount: float = 1) -> None:
        self._instr.add(amount, self._attrs)


class _Counter:
    __slots__ = ("_instr",)

    def __init__(self, instr: _AddInstrument) -> None:
        self._instr = instr

    def labels(self, **kwargs: str) -> _BoundCounter:
        return _BoundCounter(self._instr, kwargs)

    def inc(self, amount: float = 1) -> None:
        self._instr.add(amount, _NO_ATTRS)


class _BoundGauge:
    __slots__ = ("_attrs", "_instr")

    def __init__(self, instr: _SetInstrument, attrs: Mapping[str, str]) -> None:
        self._instr = instr
        self._attrs = attrs

    def set(self, value: float) -> None:
        self._instr.set(value, self._attrs)


class _Gauge:
    __slots__ = ("_instr",)

    def __init__(self, instr: _SetInstrument) -> None:
        self._instr = instr

    def labels(self, **kwargs: str) -> _BoundGauge:
        return _BoundGauge(self._instr, kwargs)

    def set(self, value: float) -> None:
        self._instr.set(value, _NO_ATTRS)


class _Histogram:
    __slots__ = ("_instr",)

    def __init__(self, instr: _RecordInstrument) -> None:
        self._instr = instr

    def observe(self, value: float) -> None:
        self._instr.record(value, _NO_ATTRS)


# ---------------------------------------------------------------------------
# Prometheus-compatible read shim over the in-memory reader (test introspection).


class _MetricsView:
    """Resolves prometheus-style sample names against an OTel in-memory reader.

    Mirrors prometheus_client's ``CollectorRegistry.get_sample_value`` so tests
    keep working: counters are queried with a ``_total`` suffix, histograms with
    ``_count`` / ``_sum``, gauges by their bare name.
    """

    def __init__(self, reader: InMemoryMetricReader) -> None:
        self._reader = reader
        # (instrument_name, sorted-attrs) -> {field: value}. Merged across reads
        # because get_metrics_data() is destructive for gauges (a second collect
        # with no new measurement returns nothing), so we retain last-seen values.
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, float]] = {}

    @staticmethod
    def _candidate(name: str) -> tuple[str, str]:
        """Map a requested prometheus name → (otel instrument name, field)."""
        if name.endswith("_total"):
            return name[: -len("_total")], "value"
        if name.endswith("_count"):
            return name[: -len("_count")], "count"
        if name.endswith("_sum"):
            return name[: -len("_sum")], "sum"
        return name, "value"

    def _refresh(self) -> None:
        data = self._reader.get_metrics_data()
        if data is None:
            return
        for rm in data.resource_metrics:
            for sm in rm.scope_metrics:
                for metric in sm.metrics:
                    for dp in metric.data.data_points:
                        attrs = tuple(sorted((k, str(v)) for k, v in (dp.attributes or {}).items()))
                        rec: dict[str, float] = {}
                        for f in ("value", "count", "sum"):
                            v = getattr(dp, f, None)
                            if v is not None:
                                rec[f] = float(v)
                        self._cache[(metric.name, attrs)] = rec

    def get_sample_value(self, name: str, labels: Mapping[str, str] | None = None) -> float | None:
        self._refresh()
        base, field = self._candidate(name)
        want = tuple(sorted((k, str(v)) for k, v in (labels or {}).items()))
        rec = self._cache.get((base, want))
        return None if rec is None else rec.get(field)


class Metrics:
    """Container for all sf2loki metrics, backed by an OTel ``MeterProvider``.

    Each instance owns its own provider + in-memory reader, so tests can build
    isolated instances freely. When ``telemetry.enabled`` is set, a periodic
    OTLP/HTTP exporter is attached and metrics are pushed to ``telemetry.endpoint``
    using ``otlp_headers`` (resolved by the caller — e.g. Basic auth).
    """

    def __init__(
        self,
        *,
        version: str = "0.1.0",
        telemetry: TelemetryConfig | None = None,
        otlp_headers: Mapping[str, str] | None = None,
    ) -> None:
        self._in_memory = InMemoryMetricReader()
        readers: list[MetricReader] = [self._in_memory]

        if telemetry is not None and telemetry.enabled:
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import (
                OTLPMetricExporter,
            )

            headers = dict(otlp_headers) if otlp_headers is not None else dict(telemetry.headers)
            exporter_kwargs: dict[str, object] = {}
            if telemetry.endpoint:
                exporter_kwargs["endpoint"] = telemetry.endpoint
            if headers:
                exporter_kwargs["headers"] = headers
            exporter = OTLPMetricExporter(**exporter_kwargs)  # type: ignore[arg-type]
            readers.append(
                PeriodicExportingMetricReader(
                    exporter,
                    export_interval_millis=int(telemetry.export_interval.total_seconds() * 1000),
                )
            )

        resource_attrs: dict[str, str] = {
            "service.name": "sf2loki",
            "service.version": version,
        }
        if telemetry is not None:
            resource_attrs.update(telemetry.resource_attributes)

        self._provider = MeterProvider(
            metric_readers=readers, resource=Resource.create(resource_attrs)
        )
        meter = self._provider.get_meter("sf2loki")
        self.registry = _MetricsView(self._in_memory)

        self.events_ingested = _Counter(
            meter.create_counter(
                "sf2loki_events_ingested",
                description="Total events ingested from Salesforce sources",
            )
        )
        self.decode_errors = _Counter(
            meter.create_counter(
                "sf2loki_decode_errors", description="Total Avro/payload decode errors"
            )
        )
        self.loki_push = _Counter(
            meter.create_counter("sf2loki_loki_push", description="Total Loki push attempts")
        )
        self.loki_entries_dropped = _Counter(
            meter.create_counter(
                "sf2loki_loki_entries_dropped",
                description="Loki entries dropped as undeliverable (permanent errors), per reason",
            )
        )
        self.loki_push_duration = _Histogram(
            meter.create_histogram(
                "sf2loki_loki_push_duration_seconds",
                description="Duration of Loki push requests in seconds",
            )
        )
        self.loki_bytes_pushed = _Counter(
            meter.create_counter(
                "sf2loki_loki_bytes_pushed", description="Total bytes pushed to Loki"
            )
        )
        self.ingest_lag = _Gauge(
            meter.create_gauge(
                "sf2loki_ingest_lag_seconds",
                description="now - event EventDate, per event type (SLI)",
            )
        )
        self.last_replay_commit_ts = _Gauge(
            meter.create_gauge(
                "sf2loki_last_replay_commit_timestamp_seconds",
                description="Unix ts of the last committed replay_id, per topic",
            )
        )
        self.pubsub_pending_credits = _Gauge(
            meter.create_gauge(
                "sf2loki_pubsub_pending_credits",
                description="Pending Pub/Sub flow-control credits, per topic",
            )
        )
        self.pubsub_reconnects = _Counter(
            meter.create_counter(
                "sf2loki_pubsub_reconnects",
                description="Total Pub/Sub stream reconnects, per topic",
            )
        )
        self.pubsub_replay_fallbacks = _Counter(
            meter.create_counter(
                "sf2loki_pubsub_replay_fallbacks",
                description=(
                    "Subscriptions restarted with a fallback replay preset after an "
                    "invalid/expired replay id, per topic"
                ),
            )
        )
        self.pubsub_stream_stalls = _Counter(
            meter.create_counter(
                "sf2loki_pubsub_stream_stalls",
                description=(
                    "Pub/Sub streams force-reconnected by the keepalive watchdog, per topic"
                ),
            )
        )
        self.salesforce_api_throttled = _Counter(
            meter.create_counter(
                "sf2loki_salesforce_api_throttled",
                description=("Salesforce REST calls rejected with REQUEST_LIMIT_EXCEEDED, per api"),
            )
        )
        self.watermark_ts = _Gauge(
            meter.create_gauge(
                "sf2loki_watermark_timestamp_seconds",
                description="Unix ts of the current polling watermark, per source/object",
            )
        )
        self.auth_refreshes = _Counter(
            meter.create_counter(
                "sf2loki_auth_refreshes", description="Total Salesforce OAuth token refreshes"
            )
        )
        self.auth_errors = _Counter(
            meter.create_counter("sf2loki_auth_errors", description="Total Salesforce auth errors")
        )
        self.schema_cache_size = _Gauge(
            meter.create_gauge(
                "sf2loki_schema_cache_size", description="Avro schemas in the codec cache"
            )
        )
        self.queue_depth = _Gauge(
            meter.create_gauge(
                "sf2loki_queue_depth", description="Depth of the internal event queue"
            )
        )

        # --- EventLogFile source (Phase 3) ---
        self.eventlogfile_files_processed = _Counter(
            meter.create_counter(
                "sf2loki_eventlogfile_files_processed",
                description="EventLogFile records downloaded+parsed, per event type",
            )
        )
        self.eventlogfile_rows_ingested = _Counter(
            meter.create_counter(
                "sf2loki_eventlogfile_rows_ingested",
                description="CSV rows ingested from EventLogFiles, per event type",
            )
        )
        self.eventlogfile_download_bytes = _Counter(
            meter.create_counter(
                "sf2loki_eventlogfile_download_bytes",
                description="Bytes downloaded from EventLogFile LogFile endpoints",
            )
        )
        self.eventlogfile_download_errors = _Counter(
            meter.create_counter(
                "sf2loki_eventlogfile_download_errors",
                description="EventLogFile listing/download errors, per reason",
            )
        )
        self.lines_truncated = _Counter(
            meter.create_counter(
                "sf2loki_lines_truncated",
                description="Log lines truncated to max_line_bytes, per source",
            )
        )

        # --- Salesforce org limits (API usage, storage, streaming events, ...) ---
        self.salesforce_limit_max = _Gauge(
            meter.create_gauge(
                "sf2loki_salesforce_limit_max",
                description="Salesforce org limit maximum, per limit_name",
            )
        )
        self.salesforce_limit_remaining = _Gauge(
            meter.create_gauge(
                "sf2loki_salesforce_limit_remaining",
                description="Salesforce org limit remaining, per limit_name",
            )
        )
        self.salesforce_limits_poll_errors = _Counter(
            meter.create_counter(
                "sf2loki_salesforce_limits_poll_errors",
                description="Total Salesforce /limits poll errors",
            )
        )

        self.build_info = _Gauge(
            meter.create_gauge(
                "sf2loki_build_info", description="Build metadata; value is always 1"
            )
        )
        self.build_info.labels(version=version).set(1)

    def force_flush(self, timeout_millis: int = 5000) -> None:
        """Flush any pending exports (best-effort; safe to call when disabled)."""
        self._provider.force_flush(timeout_millis=timeout_millis)

    def shutdown(self, timeout_millis: int = 5000) -> None:
        """Flush and shut down the meter provider (call during graceful shutdown)."""
        self._provider.shutdown(timeout_millis=timeout_millis)
