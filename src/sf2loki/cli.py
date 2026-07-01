"""Command-line entrypoint for the sf2loki service."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

import uvloop

from sf2loki import configdoc
from sf2loki.app import App
from sf2loki.config import ConfigError, load

_CONFIGDOC_RENDERERS = {
    "example": configdoc.example_yaml,
    "reference": configdoc.reference_markdown,
    "schema": configdoc.json_schema,
}


def main(argv: Sequence[str] | None = None) -> int:
    """Load config, build the app, and run it on the uvloop event loop."""
    parser = argparse.ArgumentParser(prog="sf2loki")
    parser.add_argument(
        "--config",
        type=Path,
        default=None,
        help="Path to a YAML config file (env vars override; defaults apply if omitted).",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate the config and wiring (secrets, labels, source overlap) without "
        "making any network calls, then exit 0 (ok) or 1 (invalid).",
    )

    subparsers = parser.add_subparsers(dest="command")
    config_parser = subparsers.add_parser(
        "config", help="Print generated configuration documentation."
    )
    config_parser.add_argument(
        "kind",
        choices=["example", "reference", "schema"],
        help="Which artifact to print: an annotated example YAML, a Markdown "
        "reference, or the JSON schema.",
    )

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Live end-to-end preflight: auth, permissions, entitlements, Pub/Sub "
        "reachability, a Loki test write, and state-dir checks; prints a "
        "PASS/WARN/FAIL table and exits 1 on any FAIL.",
    )
    doctor_parser.add_argument(
        "--json",
        action="store_true",
        dest="json_output",
        help="Emit machine-readable JSON instead of the table (for CI).",
    )
    doctor_parser.add_argument(
        "--org",
        default=None,
        help="For a multi-org config, which org to check (default: the first "
        "configured org). Ignored for single-org configs.",
    )

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="One-shot historical EventLogFile backfill into Loki (resumable; uses "
        "a separate state file so it is safe alongside the running service).",
    )
    backfill_parser.add_argument(
        "--since",
        required=True,
        help="Start of the backfill window, YYYY-MM-DD (UTC, inclusive).",
    )
    backfill_parser.add_argument(
        "--until",
        default=None,
        help="End of the backfill window, YYYY-MM-DD (UTC, exclusive; default: now).",
    )
    backfill_parser.add_argument(
        "--event-types",
        default=None,
        help="Comma-separated ELF EventTypes to backfill (default: the configured types).",
    )
    backfill_parser.add_argument(
        "--interval",
        choices=["Daily", "Hourly"],
        default="Daily",
        help="Which ELF interval to backfill (Daily is complete per Salesforce docs).",
    )
    backfill_parser.add_argument(
        "--ingest-timestamps",
        action="store_true",
        help="Use ingest-time timestamps (event time preserved in structured metadata "
        'key event_time) instead of the default backfill="true" label strategy.',
    )
    backfill_parser.add_argument(
        "--concurrency",
        type=int,
        default=2,
        help="Concurrent file downloads (each spools up to 8 MiB).",
    )
    backfill_parser.add_argument(
        "--org",
        default=None,
        help="For a multi-org config, which org to backfill (default: the first "
        "configured org). Ignored for single-org configs.",
    )

    args = parser.parse_args(argv)

    if args.command == "config":
        sys.stdout.write(_CONFIGDOC_RENDERERS[args.kind]())
        return 0

    if args.command == "doctor":
        from sf2loki.doctor import run_doctor

        return uvloop.run(run_doctor(args.config, json_output=args.json_output, org_name=args.org))

    if args.command == "backfill":
        from sf2loki.backfill import parse_backfill_date, run_backfill
        from sf2loki.config import as_single_org_view, select_org

        try:
            since = parse_backfill_date(args.since)
            until = parse_backfill_date(args.until) if args.until else None
            cfg = load(args.config)
            org, note = select_org(cfg, args.org)
            cfg = as_single_org_view(cfg, org)
        except (ConfigError, ValueError) as exc:
            print(f"sf2loki: {exc}", file=sys.stderr)
            return 2
        if note:
            print(f"sf2loki: {note}", file=sys.stderr)
        event_types = (
            [t.strip() for t in args.event_types.split(",") if t.strip()]
            if args.event_types
            else None
        )
        return uvloop.run(
            run_backfill(
                cfg,
                since=since,
                until=until,
                event_types=event_types,
                interval=args.interval,
                ingest_timestamps=args.ingest_timestamps,
                concurrency=args.concurrency,
            )
        )

    if args.check:
        try:
            App.build(load(args.config))
        except Exception as exc:
            print(f"config check FAILED: {exc}", file=sys.stderr)
            return 1
        print("config check OK")
        return 0

    try:
        cfg = load(args.config)
        app = App.build(cfg)
    except ConfigError as exc:
        # Operator-facing config problems get a clean message, not a traceback
        # (same failure surface --check reports; exit 2 = bad configuration).
        print(f"sf2loki: {exc}", file=sys.stderr)
        return 2
    uvloop.run(app.run())
    return 0
