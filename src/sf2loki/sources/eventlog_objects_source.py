"""EventLog Objects source: polls Salesforce SOQL for EventLog object records.

Ref: DESIGN.md §7.

Supports any sObject that Salesforce surfaces as a queryable EventLog object
(e.g. LoginEvent, ApiEvent). Uses FIELDS(ALL) which requires LIMIT <=200
(Salesforce documented constraint).

BigObject caveat (from DESIGN §7): objects in the EventStore family (e.g.
ApiEventStream) have restrictive SOQL support — FIELDS(ALL) and ORDER BY may
not work. Standard EventLog objects (LoginEvent, etc.) are fine.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime

from sf2loki.config import EventLogObjectsConfig
from sf2loki.model import CheckpointToken, LogEntry
from sf2loki.salesforce.soql_client import SoqlClient, to_soql_datetime_literal
from sf2loki.shaping import extract_timestamp, route_fields
from sf2loki.state.base import CheckpointStore


class EventLogObjectsSource:
    """Polls Salesforce EventLog sObjects via SOQL and yields :class:`~sf2loki.model.LogEntry`.

    Satisfies the :class:`~sf2loki.sources.base.Source` protocol.
    """

    name = "eventlog_objects"

    def __init__(
        self,
        cfg: EventLogObjectsConfig,
        soql: SoqlClient,
        *,
        sm_fields: Sequence[str],
        poll_once: bool = False,
    ) -> None:
        self._cfg = cfg
        self._soql = soql
        self._sm_fields = sm_fields
        # poll_once=True runs a single cycle and returns (useful in tests to avoid
        # an infinite polling loop).
        self._poll_once = poll_once

    async def events(
        self,
        state: CheckpointStore,
        stop: asyncio.Event,
    ) -> AsyncIterator[LogEntry]:
        """Yield log entries for all enabled EventLog objects.

        Each poll cycle:
        1. Load the stored watermark per object (or default to now-lookback).
        2. Issue SOQL with WHERE timestamp_field > <watermark> ORDER BY ASC LIMIT 200.
        3. Yield a :class:`~sf2loki.model.LogEntry` per record, with a
           :class:`~sf2loki.model.CheckpointToken` carrying the record's timestamp
           field value as the new watermark (so the pipeline can commit it after push).
        4. Sleep for poll_interval before the next cycle.

        Stop semantics: checks ``stop`` before each cycle and between records;
        returns promptly when set.

        The pipeline (not this source) is responsible for committing checkpoints
        to ``state``; we only read the watermark here.
        """
        while True:
            if stop.is_set():
                return

            for obj in self._cfg.objects:
                if stop.is_set():
                    return

                # --- 1. Resolve watermark ---
                watermark = await state.load(f"eventlog_objects:{obj.name}")
                if watermark is None:
                    # No stored watermark: look back from now.
                    default_wm = datetime.now(UTC) - obj.lookback
                    # Format as SOQL datetime literal: ISO-8601 with Z suffix.
                    watermark = default_wm.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    # A stored watermark is the raw EventDate Salesforce returned
                    # (e.g. "…+0000"), which is NOT a legal SOQL literal — reformat
                    # before interpolating it into the WHERE clause.
                    watermark = to_soql_datetime_literal(watermark)

                # --- 2. Build SOQL ---
                # FIELDS(ALL) is a Salesforce convenience that selects every field;
                # it requires LIMIT <=200 (platform constraint).
                # Note: EventStore BigObjects (e.g. ApiEventStream) may not support
                # FIELDS(ALL) or ORDER BY — use standard EventLog objects with this source.
                soql = (
                    f"SELECT FIELDS(ALL) FROM {obj.name} "
                    f"WHERE {obj.timestamp_field} > {watermark} "
                    f"ORDER BY {obj.timestamp_field} ASC "
                    f"LIMIT 200"
                )

                # --- 3. Yield entries in ascending timestamp order ---
                async for record in self._soql.query(soql):
                    if stop.is_set():
                        return

                    ts = extract_timestamp(record)
                    line, sm = route_fields(record, self._sm_fields)

                    # labels: source and event_type only — job/sf_org_id/environment
                    # are injected downstream by the pipeline.
                    labels: dict[str, str] = {
                        "source": "eventlog_objects",
                        "event_type": obj.name,
                    }

                    # Checkpoint value: the record's timestamp field as ISO-8601
                    # string (the new watermark for the next poll cycle).
                    ts_field_val = str(record.get(obj.timestamp_field, ""))
                    checkpoint = CheckpointToken(
                        key=f"eventlog_objects:{obj.name}",
                        value=ts_field_val,
                    )

                    yield LogEntry(
                        timestamp=ts,
                        labels=labels,
                        line=line,
                        structured_metadata=sm,
                        checkpoint=checkpoint,
                    )

            if self._poll_once:
                return

            # --- 4. Sleep between cycles (per poll_interval of first object for simplicity;
            # a production implementation could track per-object timers independently) ---
            if self._cfg.objects:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        stop.wait(),
                        timeout=self._cfg.objects[0].poll_interval.total_seconds(),
                    )
