"""Command-line entrypoint for the sf2loki service."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

import uvloop

from sf2loki.app import App
from sf2loki.config import load


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
    args = parser.parse_args(argv)

    if args.check:
        try:
            App.build(load(args.config))
        except Exception as exc:
            print(f"config check FAILED: {exc}", file=sys.stderr)
            return 1
        print("config check OK")
        return 0

    cfg = load(args.config)
    app = App.build(cfg)
    uvloop.run(app.run())
    return 0
