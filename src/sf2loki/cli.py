"""Command-line entrypoint for the sf2loki service."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

import uvloop

from sf2loki import __version__, configdoc
from sf2loki.app import App
from sf2loki.config import ConfigError, load


def _version() -> str:
    """The installed distribution version, falling back to the package constant
    when running from an uninstalled source tree."""
    try:
        return _pkg_version("sf2loki")
    except PackageNotFoundError:
        return __version__


_CONFIGDOC_RENDERERS = {
    "example": configdoc.example_yaml,
    "reference": configdoc.reference_markdown,
    "schema": configdoc.json_schema,
}

# Unified exit code for "the config is invalid" across every subcommand that
# validates config before doing anything else: --check, run, and backfill all
# return this on a config/wiring error (bad secrets, source overlap, org
# selection, etc.), so scripts can check for one code regardless of which
# entrypoint they invoke. `doctor` also returns this code when the config
# itself can't be loaded/selected (same "bad config" case), but keeps its own
# exit 1 for a check FAIL on an otherwise-valid config — a live-preflight
# health verdict, distinct from a config error (see doctor._CONFIG_ERROR_EXIT_CODE).
_CONFIG_ERROR_EXIT_CODE = 2


def main(argv: Sequence[str] | None = None) -> int:
    """Load config, build the app, and run it on the uvloop event loop."""
    parser = argparse.ArgumentParser(prog="sf2loki")
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {_version()}",
        help="Print the sf2loki version and exit.",
    )
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
        "making any network calls, then exit 0 (ok) or 2 (invalid config; same code "
        "as `run`/`backfill` on a config error).",
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

    state_parser = subparsers.add_parser(
        "state",
        help="Inspect/repair checkpoints in the configured state store (see "
        "docs/state-runbook.md for stuck-watermark recovery).",
    )
    state_subparsers = state_parser.add_subparsers(dest="state_command", required=True)

    state_show_parser = state_subparsers.add_parser(
        "show", help="Pretty-print checkpoints from the configured store (nothing is redacted)."
    )
    state_show_parser.add_argument(
        "--key",
        dest="key_glob",
        default="*",
        help="fnmatch glob to filter checkpoint keys (default: show all).",
    )
    state_show_parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the file store's exclusive lock (unsafe if the daemon is "
        "actually still running against the same state file).",
    )

    state_set_parser = state_subparsers.add_parser(
        "set", help="CAS-safe write of a single checkpoint key."
    )
    state_set_parser.add_argument("key")
    state_set_parser.add_argument("value")
    state_set_parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the file store's exclusive lock (unsafe if the daemon is "
        "actually still running against the same state file).",
    )

    state_delete_parser = state_subparsers.add_parser(
        "delete",
        help="Remove a checkpoint key so its source restarts from its preset/lookback.",
    )
    state_delete_parser.add_argument("key")
    state_delete_parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass the file store's exclusive lock (unsafe if the daemon is "
        "actually still running against the same state file).",
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
            # Capture the org identity BEFORE as_single_org_view drops org.name,
            # so the backfill checkpoint key can be namespaced per org (#40).
            # The first configured org additionally falls back to the legacy
            # unprefixed key, mirroring the live daemon's OrgCheckpointView.
            resolved = cfg.resolved_orgs()
            org_name = org.name
            legacy_fallback = bool(resolved) and org.name == resolved[0].name
            cfg = as_single_org_view(cfg, org)
        except (ConfigError, ValueError) as exc:
            print(f"sf2loki: {exc}", file=sys.stderr)
            return _CONFIG_ERROR_EXIT_CODE
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
                org_name=org_name,
                legacy_fallback=legacy_fallback,
            )
        )

    if args.command == "state":
        from sf2loki.statecmd import run_state_delete, run_state_set, run_state_show

        if args.state_command == "show":
            return uvloop.run(run_state_show(args.config, key_glob=args.key_glob, force=args.force))
        if args.state_command == "set":
            return uvloop.run(run_state_set(args.config, args.key, args.value, force=args.force))
        return uvloop.run(run_state_delete(args.config, args.key, force=args.force))

    if args.check:
        try:
            App.build(load(args.config))
        except Exception as exc:
            print(f"config check FAILED: {exc}", file=sys.stderr)
            return _CONFIG_ERROR_EXIT_CODE
        print("config check OK")
        return 0

    try:
        cfg = load(args.config)
        app = App.build(cfg)
    except ConfigError as exc:
        # Operator-facing config problems get a clean message, not a traceback
        # (same failure surface --check reports; unified config-error exit code).
        print(f"sf2loki: {exc}", file=sys.stderr)
        return _CONFIG_ERROR_EXIT_CODE
    uvloop.run(app.run())
    return 0
