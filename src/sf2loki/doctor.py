"""`sf2loki doctor`: live end-to-end preflight diagnostics.

Runs a sequenced, read-only-except-one-test-write check of the whole path —
config, auth, Salesforce permissions/entitlements, Pub/Sub reachability, the
Loki write path, and the state directory — and prints a PASS/WARN/FAIL table
so first-run problems surface in one command instead of one at a time at
runtime.
"""

from __future__ import annotations

from pathlib import Path


async def run_doctor(config_path: Path | None, *, json_output: bool = False) -> int:
    """Run all preflight checks; return 0 (no FAIL) or 1 (any FAIL).

    Loads the config itself (a config problem is check #1's FAIL row, not a
    crash before the doctor starts).
    """
    raise NotImplementedError("implemented in the doctor lane")
