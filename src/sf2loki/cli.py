"""Command-line entrypoint for the sf2loki service."""

from __future__ import annotations

import argparse
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
    args = parser.parse_args(argv)

    cfg = load(args.config)
    app = App.build(cfg)
    uvloop.run(app.run())
    return 0
