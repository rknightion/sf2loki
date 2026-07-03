#!/usr/bin/env python
"""Splice the generated ``config:`` block into ``deploy/helm/values.yaml``.

The block is produced by :func:`sf2loki.configdoc.helm_values_config` and bounded
by the ``# >>> BEGIN generated config ... <<<`` / ``# >>> END generated config <<<``
sentinel lines (see ``configdoc._HELM_VALUES_BEGIN/_END``). This replaces that
inclusive region in place, leaving the hand-authored knobs above/below untouched.

Run via ``just gen-helm-values`` after any ``config.py`` change; the drift gate
(``tests/test_config_artifacts_drift.py``) fails if the committed region is stale.
"""

from __future__ import annotations

import sys
from pathlib import Path

from sf2loki import configdoc

ROOT = Path(__file__).resolve().parents[1]
VALUES = ROOT / "deploy/helm/values.yaml"


def splice(text: str, block: str) -> str:
    """Return ``text`` with the inclusive [BEGIN..END] region replaced by ``block``."""
    lines = text.splitlines(keepends=True)
    begin = end = None
    for i, line in enumerate(lines):
        if line.rstrip("\n") == configdoc._HELM_VALUES_BEGIN:
            begin = i
        elif line.rstrip("\n") == configdoc._HELM_VALUES_END:
            end = i
            break
    if begin is None or end is None or end < begin:
        raise SystemExit(
            f"{VALUES}: could not find the generated-config markers "
            f"({configdoc._HELM_VALUES_BEGIN!r} .. {configdoc._HELM_VALUES_END!r})"
        )
    return "".join(lines[:begin]) + block + "".join(lines[end + 1 :])


def main() -> int:
    block = configdoc.helm_values_config()
    original = VALUES.read_text()
    updated = splice(original, block)
    if updated != original:
        VALUES.write_text(updated)
        print(f"updated {VALUES.relative_to(ROOT)}")
    else:
        print(f"{VALUES.relative_to(ROOT)} already up to date")
    return 0


if __name__ == "__main__":
    sys.exit(main())
