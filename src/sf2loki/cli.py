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

    args = parser.parse_args(argv)

    if args.command == "config":
        sys.stdout.write(_CONFIGDOC_RENDERERS[args.kind]())
        return 0

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
