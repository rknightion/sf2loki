"""ApexLog source: polls Salesforce debug logs via the Tooling API.

Ref: issue #33. Mirrors the eventlog_objects watermark/dedup design (StartTime
``>=`` cursor + rolling Id window + drain-until-short-page) since ApexLog is a
timestamped, id-keyed sObject. One Loki entry per log: the raw debug-log body is
the line (capped downstream by sink.loki.batch.max_line_bytes); the metadata
(LogUserId, Operation, Status, ...) goes to structured metadata. Logs whose
LogLength exceeds ``max_body_bytes`` skip the body download entirely (saving the
API call) and ship a metadata-only line flagged ``body_skipped``.

TraceFlags are NOT managed here — ApexLog only exists while a TraceFlag is active
(24h retention); operators enable them out-of-band (``sf debug`` / Setup).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

from sf2loki.config import ApexLogConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.obs.metrics import Metrics
from sf2loki.salesforce.apexlog_client import (
    ApexLogClient,
    ApexLogError,
    ApexLogMeta,
    ApexLogThrottledError,
)
from sf2loki.shaping import extract_timestamp_checked, route_fields, should_keep
from sf2loki.state.base import CheckpointStore
from sf2loki.transforms import compile_rules

_PAGE_LIMIT = 200
_MAX_CARRIED_IDS = 500
_ERROR_LOG_THRESHOLD = 3
_CHECKPOINT_KEY = "apexlog"

_log = logging.getLogger(__name__)


def _watermark_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)


def _is_valid_watermark(value: str) -> bool:
    return _watermark_datetime(value) is not None


def _parse_checkpoint(raw: str) -> tuple[str, list[str]]:
    try:
        parsed = json.loads(raw)
    except ValueError:
        return raw, []
    if isinstance(parsed, dict):
        wm = str(parsed.get("last_ts") or "")
        raw_ids = parsed.get("ids", [])
        ids = [str(i) for i in raw_ids] if isinstance(raw_ids, list) else []
        return wm, ids
    return raw, []


def _meta_payload(m: ApexLogMeta) -> dict[str, object]:
    """The metadata fields, always promoted to structured metadata + the fallback line."""
    return {
        "Id": m.id,
        "LogUserId": m.log_user_id,
        "LogLength": m.log_length,
        "Operation": m.operation,
        "Request": m.request,
        "Status": m.status,
        "StartTime": m.start_time,
        "Application": m.application,
        "DurationMilliseconds": m.duration_ms,
        "Location": m.location,
    }


class ApexLogSource:
    """Polls ApexLog via the Tooling API and yields one :class:`LogEntry` per log."""

    name = "apexlog"

    def __init__(
        self,
        cfg: ApexLogConfig,
        client: ApexLogClient,
        *,
        sm_fields: Sequence[str],
        metrics: Metrics | None = None,
        poll_once: bool = False,
        transform_salt: str = "",
    ) -> None:
        self._cfg = cfg
        self._client = client
        self._sm_fields = sm_fields
        self._metrics = metrics if metrics is not None else Metrics()
        self._poll_once = poll_once
        self._consecutive_failures = 0
        self._transforms = compile_rules(
            cfg.transforms, salt=transform_salt, source=self.name, metrics=self._metrics
        )

    async def events(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        if not self._cfg.enabled:
            return

        while True:
            if stop.is_set():
                return
            async for entry in self._poll(state, stop):
                yield entry
            if self._poll_once:
                return
            interval = self._cfg.poll_interval.total_seconds()
            if interval > 0:
                interval *= random.uniform(0.9, 1.1)  # jitter, not cryptographic
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=max(0.0, interval))

    async def _poll(self, state: CheckpointStore, stop: asyncio.Event) -> AsyncIterator[LogEntry]:
        raw = await state.load(_CHECKPOINT_KEY)
        watermark: str = ""
        window: list[str] = []
        if raw is not None:
            watermark, window = _parse_checkpoint(raw)

        if not _is_valid_watermark(watermark):
            if raw is not None:
                _log.warning(
                    "apexlog: stored watermark %r invalid; falling back to now-lookback (%s)",
                    watermark,
                    self._cfg.lookback,
                )
            watermark = (datetime.now(UTC) - self._cfg.lookback).strftime("%Y-%m-%dT%H:%M:%SZ")

        while True:
            try:
                page = await self._client.list_logs(
                    since=watermark, users=self._cfg.users, page_size=_PAGE_LIMIT
                )
            except ApexLogThrottledError as exc:
                self._metrics.soql_poll_errors.labels(source="apexlog", object="apexlog").inc()
                _log.error("apexlog: API request limit exceeded; backing off: %s", exc)
                return
            except ApexLogError as exc:
                self._consecutive_failures += 1
                self._metrics.soql_poll_errors.labels(source="apexlog", object="apexlog").inc()
                level = (
                    logging.ERROR
                    if self._consecutive_failures >= _ERROR_LOG_THRESHOLD
                    else logging.WARNING
                )
                _log.log(
                    level,
                    "apexlog: listing failed (%d consecutive): %s",
                    self._consecutive_failures,
                    exc,
                )
                return

            seen = set(window)
            new_logs = [m for m in page if m.id and m.id not in seen]
            if not new_logs:
                if len(page) >= _PAGE_LIMIT:
                    _log.warning(
                        "apexlog: full page at watermark %s all already-seen; "
                        "stopping cycle to avoid a hot loop",
                        watermark,
                    )
                break

            for m in new_logs:
                if stop.is_set():
                    return
                if _is_valid_watermark(m.start_time):
                    watermark = m.start_time
                window = [*window, m.id][-_MAX_CARRIED_IDS:]

                payload = _meta_payload(m)
                if self._transforms.apply(payload) is None:
                    continue
                if self._cfg.sample < 1.0 and not should_keep(m.id, self._cfg.sample):
                    self._metrics.entries_sampled_out.labels(
                        source="apexlog", event_type="apexlog"
                    ).inc()
                    continue

                # Build the checkpoint HERE — `window` lives in this scope; the
                # committed value is the advanced watermark + rolling id window.
                ckpt = json.dumps({"ids": window, "last_ts": watermark}, sort_keys=True)
                try:
                    entry = await self._build_entry(m, payload, watermark, ckpt)
                except ApexLogThrottledError as exc:
                    # A 403 on the body download exhausts the same API budget as
                    # the listing — abort the cycle and back off (the checkpoint
                    # from the last yielded entry is already safe).
                    self._metrics.soql_poll_errors.labels(source="apexlog", object="apexlog").inc()
                    _log.error("apexlog: body download throttled; backing off: %s", exc)
                    return
                yield entry
                self._metrics.apexlog_logs_ingested.inc()
                self._consecutive_failures = 0

            if len(page) < _PAGE_LIMIT:
                break

    async def _build_entry(
        self,
        m: ApexLogMeta,
        payload: dict[str, object],
        watermark: str,
        checkpoint_value: str,
    ) -> LogEntry:
        # route_fields on the metadata gives us the configured sm_fields + level;
        # we then always promote the core metadata and use the body as the line.
        _discarded_line, sm = route_fields(payload, self._sm_fields)
        for k, v in payload.items():
            if v is not None and str(v) != "":
                sm.setdefault(k, str(v))

        line = await self._resolve_line(m, payload, sm)

        ts, used_fallback = extract_timestamp_checked(
            payload, field_names=("StartTime",), fallback=_watermark_datetime(watermark)
        )
        if used_fallback:
            self._metrics.timestamp_fallbacks.labels(source="apexlog").inc()

        return LogEntry(
            timestamp=ts,
            labels={"source": "apexlog", "event_type": "apexlog"},
            line=line,
            structured_metadata=sm,
            checkpoint=CheckpointToken(key=_CHECKPOINT_KEY, value=checkpoint_value),
        )

    async def _resolve_line(
        self, m: ApexLogMeta, payload: dict[str, object], sm: dict[str, str]
    ) -> str:
        """Return the log line: the body when downloaded, else a metadata JSON line.

        Skips the download (and its API call) for oversize logs; on a download
        error, ships metadata-only. Re-raises :class:`ApexLogThrottledError` so the
        caller aborts the whole cycle (never swallowed as a per-log skip).
        """
        if m.log_length > self._cfg.max_body_bytes:
            self._metrics.apexlog_bodies_skipped.labels(reason="size").inc()
            sm["body_skipped"] = "true"
            sm["body_skip_reason"] = "size"
            return json.dumps(payload, sort_keys=True, default=str)
        try:
            return await self._client.download_body(m.id)
        except ApexLogThrottledError:
            raise  # propagate: caller aborts the cycle
        except ApexLogError as exc:
            self._metrics.apexlog_bodies_skipped.labels(reason="download_error").inc()
            _log.warning(
                "apexlog: body download failed for %s; shipping metadata only: %s", m.id, exc
            )
            sm["body_skipped"] = "true"
            sm["body_skip_reason"] = "download_error"
            return json.dumps(payload, sort_keys=True, default=str)
