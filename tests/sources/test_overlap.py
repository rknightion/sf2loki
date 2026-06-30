"""Tests for the fail-fast overlap guard (sources/overlap.py)."""

from __future__ import annotations

import pytest

from sf2loki.sources.overlap import (
    OverlapError,
    category_of_elf,
    category_of_pubsub,
    category_of_stored_object,
    check_overlap,
)

# --- normalization ----------------------------------------------------------


def test_category_of_pubsub_strips_event_stream() -> None:
    assert category_of_pubsub("/event/LoginEventStream") == "login"
    assert category_of_pubsub("/event/ReportEventStream") == "report"
    assert category_of_pubsub("/event/ApiAnomalyEvent") == "apianomaly"


def test_category_of_stored_object_strips_event() -> None:
    assert category_of_stored_object("LoginEvent") == "login"
    assert category_of_stored_object("ApiAnomalyEventStore") == "apianomaly"


def test_category_of_elf_lowercases() -> None:
    assert category_of_elf("Login") == "login"
    assert category_of_elf("API") == "api"
    assert category_of_elf("Report") == "report"


def test_category_of_elf_strips_spaces() -> None:
    # A display-style "Login As" must match the RTEM stem loginas.
    assert category_of_elf("Login As") == "loginas"
    assert category_of_elf("Login As") == category_of_stored_object("LoginAsEvent")


def test_pubsub_and_stored_object_agree_on_category() -> None:
    # The whole point: streaming a category and polling its stored object are
    # the same data -> they must normalise to the same category.
    assert category_of_pubsub("/event/LoginEventStream") == category_of_stored_object("LoginEvent")


# --- check_overlap ----------------------------------------------------------


def test_stream_plus_stored_object_same_category_raises() -> None:
    with pytest.raises(OverlapError, match="login"):
        check_overlap(
            pubsub_topics=["/event/LoginEventStream"],
            stored_objects=["LoginEvent"],
        )


def test_stream_plus_elf_same_category_raises() -> None:
    with pytest.raises(OverlapError, match="login"):
        check_overlap(
            pubsub_topics=["/event/LoginEventStream"],
            elf_event_types=["Login"],
        )


def test_allow_overlap_bypasses() -> None:
    # Must not raise.
    check_overlap(
        pubsub_topics=["/event/LoginEventStream"],
        stored_objects=["LoginEvent"],
        allow_overlap=True,
    )


def test_disjoint_categories_pass() -> None:
    check_overlap(
        pubsub_topics=["/event/LoginEventStream"],
        elf_event_types=["Report", "API"],
    )


def test_single_source_many_topics_passes() -> None:
    check_overlap(
        pubsub_topics=["/event/LoginEventStream", "/event/ApiEventStream"],
    )


def test_three_way_overlap_raises_and_lists_all() -> None:
    with pytest.raises(OverlapError) as exc:
        check_overlap(
            pubsub_topics=["/event/ApiEventStream"],
            stored_objects=["ApiEvent"],
            elf_event_types=["API"],
        )
    msg = str(exc.value)
    assert "pubsub=" in msg and "eventlog_objects=" in msg and "eventlogfile=" in msg


def test_no_sources_passes() -> None:
    check_overlap()
