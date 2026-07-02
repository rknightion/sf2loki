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
from typing import TYPE_CHECKING, Protocol, cast

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


class _BoundHistogram:
    __slots__ = ("_attrs", "_instr")

    def __init__(self, instr: _RecordInstrument, attrs: Mapping[str, str]) -> None:
        self._instr = instr
        self._attrs = attrs

    def observe(self, value: float) -> None:
        self._instr.record(value, self._attrs)


class _Histogram:
    __slots__ = ("_instr",)

    def __init__(self, instr: _RecordInstrument) -> None:
        self._instr = instr

    def labels(self, **kwargs: str) -> _BoundHistogram:
        return _BoundHistogram(self._instr, kwargs)

    def observe(self, value: float) -> None:
        self._instr.record(value, _NO_ATTRS)


# ---------------------------------------------------------------------------
# Per-org proxies: inject an ``org=<name>`` attribute into every measurement of a
# per-org component (token provider, clients, sources, limits poller) without
# touching a single call-site. Built once per org in _OrgMetrics.__init__.


class _OrgCounter:
    __slots__ = ("_inner", "_org")

    def __init__(self, inner: _Counter, org: str) -> None:
        self._inner = inner
        self._org = org

    def labels(self, **kwargs: str) -> _BoundCounter:
        return self._inner.labels(org=self._org, **kwargs)

    def inc(self, amount: float = 1) -> None:
        self._inner.labels(org=self._org).inc(amount)


class _OrgGauge:
    __slots__ = ("_inner", "_org")

    def __init__(self, inner: _Gauge, org: str) -> None:
        self._inner = inner
        self._org = org

    def labels(self, **kwargs: str) -> _BoundGauge:
        return self._inner.labels(org=self._org, **kwargs)

    def set(self, value: float) -> None:
        self._inner.labels(org=self._org).set(value)


class _OrgHistogram:
    __slots__ = ("_inner", "_org")

    def __init__(self, inner: _Histogram, org: str) -> None:
        self._inner = inner
        self._org = org

    def labels(self, **kwargs: str) -> _BoundHistogram:
        return self._inner.labels(org=self._org, **kwargs)

    def observe(self, value: float) -> None:
        self._inner.labels(org=self._org).observe(value)


class _OrgMetrics:
    """A ``Metrics``-shaped facade that stamps ``org=<name>`` onto every instrument.

    Wraps each ``_Counter``/``_Gauge``/``_Histogram`` attribute of the shared
    ``Metrics`` with an org-injecting proxy (built once, here — not per call).
    Any non-instrument attribute or method (``registry``, ``force_flush``,
    ``shutdown``, ...) delegates to the wrapped instance via ``__getattr__``.
    """

    def __init__(self, inner: Metrics, org: str) -> None:
        self._inner = inner
        self._org = org
        for attr_name, attr in vars(inner).items():
            if isinstance(attr, _Counter):
                setattr(self, attr_name, _OrgCounter(attr, org))
            elif isinstance(attr, _Gauge):
                setattr(self, attr_name, _OrgGauge(attr, org))
            elif isinstance(attr, _Histogram):
                setattr(self, attr_name, _OrgHistogram(attr, org))

    def __getattr__(self, name: str) -> object:
        # Only reached for attributes NOT set in __init__ (the instrument
        # proxies above shadow the originals); delegates registry/force_flush/etc.
        return getattr(self._inner, name)


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
        self.last_push_success_ts = _Gauge(
            meter.create_gauge(
                "sf2loki_last_push_success_timestamp_seconds",
                description="Unix ts of the last successful Loki push",
            )
        )
        # Histogram (not a gauge): enables p95/p99 lag alerting. Buckets reach 24h
        # because EventLogFile lag is legitimately 3-6h — the OTel defaults top out
        # at 10000s and would collapse the entire ELF range into +Inf.
        self.ingest_lag = _Histogram(
            meter.create_histogram(
                "sf2loki_ingest_lag_seconds",
                description="now - event EventDate, per event type (SLI)",
                explicit_bucket_boundaries_advisory=[
                    1,
                    5,
                    15,
                    60,
                    300,
                    900,
                    1800,
                    3600,
                    7200,
                    14400,
                    28800,
                    86400,
                ],
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
        self.pubsub_stream_up = _Gauge(
            meter.create_gauge(
                "sf2loki_pubsub_stream_up",
                description=(
                    "1 while a topic's subscribe stream is connected and healthy, "
                    "0 while erroring/reconnecting, per topic"
                ),
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
        self.soql_poll_errors = _Counter(
            meter.create_counter(
                "sf2loki_soql_poll_errors",
                description="Failed SOQL poll cycles, per source and object/event type",
            )
        )
        self.apexlog_logs_ingested = _Counter(
            meter.create_counter(
                "sf2loki_apexlog_logs_ingested",
                description="ApexLog debug logs ingested",
            )
        )
        self.apexlog_download_bytes = _Counter(
            meter.create_counter(
                "sf2loki_apexlog_download_bytes",
                description="Bytes downloaded from ApexLog Body endpoints",
            )
        )
        self.apexlog_bodies_skipped = _Counter(
            meter.create_counter(
                "sf2loki_apexlog_bodies_skipped",
                description=(
                    "ApexLog bodies not shipped (over max_body_bytes or download error), "
                    "per reason"
                ),
            )
        )
        self.apexlog_download_errors = _Counter(
            meter.create_counter(
                "sf2loki_apexlog_download_errors",
                description="ApexLog body download errors, per reason",
            )
        )
        self.timestamp_fallbacks = _Counter(
            meter.create_counter(
                "sf2loki_timestamp_fallbacks",
                description=(
                    "Entries whose event timestamp was missing/unparseable and a "
                    "fallback was used, per source"
                ),
            )
        )
        self.lines_truncated = _Counter(
            meter.create_counter(
                "sf2loki_lines_truncated",
                description="Log lines truncated to max_line_bytes, per source",
            )
        )

        # --- Egress guardrails (sampling / rate cap / daily byte budget) ---
        self.entries_sampled_out = _Counter(
            meter.create_counter(
                "sf2loki_entries_sampled_out",
                description=(
                    "Rows/events dropped by deterministic per-type sampling, "
                    "per source and event type"
                ),
            )
        )
        self.rows_filtered = _Counter(
            meter.create_counter(
                "sf2loki_rows_filtered",
                description="Rows dropped by transform drop_row rules, per source and rule",
            )
        )
        self.egress_budget_used_bytes = _Gauge(
            meter.create_gauge(
                "sf2loki_egress_budget_used_bytes",
                description=(
                    "Pre-compression bytes counted against the daily egress budget "
                    "(current UTC day)"
                ),
            )
        )
        self.egress_paused = _Gauge(
            meter.create_gauge(
                "sf2loki_egress_paused",
                description="1 while pushes are paused by the daily byte budget, else 0",
            )
        )

        # --- EventLogFile cycle timing (concurrent per-type processing) ---
        self.eventlogfile_cycle_seconds = _Gauge(
            meter.create_gauge(
                "sf2loki_eventlogfile_cycle_seconds",
                description="Wall-clock duration of the last EventLogFile poll cycle",
            )
        )

        # --- HA / leadership ---
        self.leader = _Gauge(
            meter.create_gauge(
                "sf2loki_leader",
                description="1 while this instance holds leadership (or runs standalone), else 0",
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

    def for_org(self, org: str) -> Metrics:
        """Return a view that stamps ``org=<name>`` onto every measurement.

        ``org == ""`` (single-org / legacy mode) returns ``self`` — zero overhead
        and no ``org`` label, so single-org metrics are bit-identical. A non-empty
        name returns an :class:`_OrgMetrics` facade (cast to ``Metrics`` for the
        call-sites, which are unchanged). Give it to per-org components (token
        provider, Salesforce clients, sources, limits poller); keep the raw
        ``Metrics`` for the deployment-wide pipeline/sink.
        """
        if not org:
            return self
        return cast("Metrics", _OrgMetrics(self, org))

    def force_flush(self, timeout_millis: int = 5000) -> None:
        """Flush any pending exports (best-effort; safe to call when disabled)."""
        self._provider.force_flush(timeout_millis=timeout_millis)

    def shutdown(self, timeout_millis: int = 5000) -> None:
        """Flush and shut down the meter provider (call during graceful shutdown)."""
        self._provider.shutdown(timeout_millis=timeout_millis)
