"""Tests for obs/logging.py."""

from __future__ import annotations

import io
import json

import structlog
import structlog.testing

from sf2loki.obs.logging import configure_logging, get_logger


def test_configure_logging_json_does_not_raise() -> None:
    configure_logging("info", "json")


def test_configure_logging_logfmt_does_not_raise() -> None:
    configure_logging("info", "logfmt")


def test_configure_logging_debug_level() -> None:
    configure_logging("debug", "json")


def test_configure_logging_warning_level() -> None:
    configure_logging("warning", "json")


def test_get_logger_returns_bound_logger() -> None:
    configure_logging("info", "json")
    logger = get_logger("test")
    assert logger is not None


def test_get_logger_no_name() -> None:
    configure_logging("info", "json")
    logger = get_logger()
    assert logger is not None


def test_capture_logs_event() -> None:
    """structlog.testing.capture_logs captures events regardless of renderer."""
    configure_logging("info", "json")
    logger = get_logger("test")
    with structlog.testing.capture_logs() as captured:
        logger.info("hello world", key="value")
    assert len(captured) == 1
    assert captured[0]["event"] == "hello world"
    assert captured[0]["key"] == "value"


def test_capture_logs_logfmt() -> None:
    configure_logging("info", "logfmt")
    logger = get_logger("test")
    with structlog.testing.capture_logs() as captured:
        logger.warning("logfmt event", foo="bar")
    assert any(c["event"] == "logfmt event" for c in captured)


def test_json_output_is_valid_json() -> None:
    """Configure JSON, capture output via a StringIO processor, verify parseable JSON."""
    buf = io.StringIO()

    def capture(logger: object, method: str, event_dict: dict[str, object]) -> str:  # type: ignore[type-arg]
        line = json.dumps(event_dict)
        buf.write(line + "\n")
        return line

    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            capture,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=io.StringIO()),
    )
    logger = structlog.get_logger()
    logger.info("json test", answer=42)

    buf.seek(0)
    lines = [ln for ln in buf.read().splitlines() if ln]
    assert len(lines) >= 1
    parsed = json.loads(lines[0])
    assert parsed["event"] == "json test"
    assert parsed["answer"] == 42
