#!/usr/bin/env python
"""Fail if the built sdist/wheel have the wrong contents.

A packaging regression (dropping the generated proto stubs, or shipping tests /
deploy / docs / dev scratch) would produce a broken or bloated PyPI release that
only surfaces after publish. This check runs in CI against freshly built
artifacts so such a regression fails the gate instead.

Usage: python scripts/check_dist.py <dist-dir>   (default: dist)
"""

from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path

# The generated gRPC/protobuf stubs are import-critical and live inside the
# package; a wheel without them is broken. Both must be present.
REQUIRED_IN_WHEEL = (
    "sf2loki/salesforce/_generated/__init__.py",
    "sf2loki/salesforce/_generated/pubsub_api_pb2.py",
    "sf2loki/salesforce/_generated/pubsub_api_pb2_grpc.py",
    "sf2loki/sinks/loki/_generated/__init__.py",
    "sf2loki/sinks/loki/_generated/loki_push_pb2.py",
)

# Substrings that must never appear in a published artifact's member paths:
# tests, deploy assets, prose docs, internal working-notes, and dev scratch.
FORBIDDEN_SUBSTRINGS = (
    "/tests/",
    "/deploy/",
    "/docs/",
    "CLAUDE.md",
    "AGENTS.md",
    ".superpowers",
    ".claude",
    ".github",
)


def _wheel_members(path: Path) -> list[str]:
    with zipfile.ZipFile(path) as zf:
        return zf.namelist()


def _sdist_members(path: Path) -> list[str]:
    with tarfile.open(path, "r:gz") as tf:
        # Strip the leading "sf2loki-<ver>/" component so checks match the wheel.
        return [m.name.split("/", 1)[1] if "/" in m.name else m.name for m in tf.getmembers()]


def _forbidden_hits(members: list[str]) -> list[str]:
    return sorted({m for m in members for bad in FORBIDDEN_SUBSTRINGS if bad in f"/{m}"})


def main(argv: list[str]) -> int:
    dist = Path(argv[1]) if len(argv) > 1 else Path("dist")
    wheels = sorted(dist.glob("*.whl"))
    sdists = sorted(dist.glob("*.tar.gz"))
    if not wheels or not sdists:
        print(f"error: expected a wheel and an sdist in {dist}/ (found {wheels}, {sdists})")
        return 1

    errors: list[str] = []

    wheel_members = _wheel_members(wheels[0])
    for required in REQUIRED_IN_WHEEL:
        if required not in wheel_members:
            errors.append(f"wheel {wheels[0].name}: MISSING required member {required}")
    for hit in _forbidden_hits(wheel_members):
        errors.append(f"wheel {wheels[0].name}: forbidden member {hit}")

    sdist_members = _sdist_members(sdists[0])
    # The generated stubs must survive into the sdist too (it builds the wheel).
    for required in REQUIRED_IN_WHEEL:
        if f"src/{required}" not in sdist_members:
            errors.append(f"sdist {sdists[0].name}: MISSING required member src/{required}")
    for hit in _forbidden_hits(sdist_members):
        errors.append(f"sdist {sdists[0].name}: forbidden member {hit}")

    if errors:
        print("dist content check FAILED:")
        for e in errors:
            print(f"  - {e}")
        return 1

    print(f"dist content check OK ({wheels[0].name}, {sdists[0].name})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
