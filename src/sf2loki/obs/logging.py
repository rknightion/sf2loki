"""Structured logging configuration for sf2loki.

Configures structlog with either JSON or logfmt rendering, a timestamp, and
level-based filtering, AND wires the stdlib ``logging`` root logger through
the same renderer (several modules — the Loki sink, the EventLogFile source —
log via stdlib ``logging``; without a handler their warnings would fall out
unformatted via ``lastResort`` and DEBUG lines would vanish).  Call
configure_logging() once at startup; subsequent calls re-configure in place
(idempotent-safe for tests).
"""

from __future__ import annotations

import logging
from typing import Literal, cast

import structlog
import structlog.stdlib

_LEVEL_MAP: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def configure_logging(
    level: str = "info",
    fmt: Literal["json", "logfmt"] = "json",
) -> None:
    """Configure structlog globally.

    Parameters
    ----------
    level:
        Log level string, case-insensitive (debug/info/warning/error/critical).
    fmt:
        Output format: "json" (default) or "logfmt".
    """
    level_int = _LEVEL_MAP.get(level.lower(), logging.INFO)

    if fmt == "logfmt":
        renderer: structlog.types.Processor = structlog.processors.LogfmtRenderer()
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level_int),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )

    # Route stdlib logging through the SAME renderer/level so every logger in
    # the process (structlog or stdlib) emits one consistent format. Replacing
    # the handler list wholesale keeps repeat calls idempotent.
    handler = logging.StreamHandler()
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                renderer,
            ],
            foreign_pre_chain=[
                structlog.stdlib.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.format_exc_info,
            ],
        )
    )
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level_int)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger.

    Parameters
    ----------
    name:
        Optional logger name bound into context.  When None the root logger is returned.
    """
    if name is not None:
        return cast(structlog.stdlib.BoundLogger, structlog.get_logger(name))
    return cast(structlog.stdlib.BoundLogger, structlog.get_logger())
