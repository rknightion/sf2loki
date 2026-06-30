"""``python -m sf2loki`` entrypoint."""

from __future__ import annotations

import sys

from sf2loki.cli import main

if __name__ == "__main__":
    sys.exit(main())
