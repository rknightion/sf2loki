"""Tests for sf2loki.sinks.loki.labels."""

from __future__ import annotations

import pytest

from sf2loki.sinks.loki.labels import (
    ALLOWED_LABELS,
    RESERVED_STATIC_LABELS,
    LabelGuardError,
    guard_labels,
    guard_static_labels,
    render_labels,
)


class TestGuardLabels:
    def test_passes_for_all_allowed_keys(self) -> None:
        labels = {k: "v" for k in ALLOWED_LABELS}
        guard_labels(labels)  # must not raise

    def test_passes_for_subset_of_allowed_keys(self) -> None:
        guard_labels({"job": "sf2loki", "environment": "prod"})

    def test_service_name_is_an_allowed_label(self) -> None:
        assert "service_name" in ALLOWED_LABELS
        guard_labels({"service_name": "sf2loki"})  # must not raise

    def test_passes_for_empty_mapping(self) -> None:
        guard_labels({})

    def test_raises_for_stray_key(self) -> None:
        with pytest.raises(LabelGuardError, match="user_id"):
            guard_labels({"job": "sf2loki", "user_id": "abc"})

    def test_raises_listing_all_bad_keys(self) -> None:
        with pytest.raises(LabelGuardError) as exc_info:
            guard_labels({"bad_one": "x", "bad_two": "y"})
        msg = str(exc_info.value)
        assert "bad_one" in msg
        assert "bad_two" in msg

    def test_custom_allowed_set(self) -> None:
        guard_labels({"custom_key": "v"}, allowed=frozenset({"custom_key"}))

    def test_raises_with_custom_allowed_set(self) -> None:
        with pytest.raises(LabelGuardError, match="job"):
            guard_labels({"job": "sf2loki"}, allowed=frozenset({"custom_key"}))


class TestGuardStaticLabels:
    """Operator static labels must not clobber per-entry identity labels."""

    def test_reserved_set_contains_identity_labels(self) -> None:
        assert RESERVED_STATIC_LABELS == frozenset({"source", "event_type"})

    @pytest.mark.parametrize("key", ["source", "event_type"])
    def test_rejects_reserved_key(self, key: str) -> None:
        with pytest.raises(LabelGuardError, match=key):
            guard_static_labels({key: "x"})

    def test_error_message_explains_static_label_clobbering(self) -> None:
        with pytest.raises(LabelGuardError, match="static"):
            guard_static_labels({"event_type": "x"})

    def test_allows_non_reserved_allowed_keys(self) -> None:
        guard_static_labels(
            {"job": "sf2loki", "service_name": "sf", "environment": "prod", "sf_org_id": "00D"}
        )  # must not raise

    def test_still_rejects_disallowed_keys(self) -> None:
        with pytest.raises(LabelGuardError, match="user_id"):
            guard_static_labels({"user_id": "abc"})

    def test_empty_ok(self) -> None:
        guard_static_labels({})


class TestRenderLabels:
    def test_empty_mapping_returns_empty_braces(self) -> None:
        assert render_labels({}) == "{}"

    def test_single_key_value(self) -> None:
        assert render_labels({"job": "sf2loki"}) == '{job="sf2loki"}'

    def test_multiple_keys_sorted_ascending(self) -> None:
        result = render_labels({"job": "sf2loki", "environment": "prod"})
        assert result == '{environment="prod",job="sf2loki"}'

    def test_all_allowed_keys_sorted(self) -> None:
        labels = {
            "sf_org_id": "00D",
            "job": "sf2loki",
            "environment": "prod",
            "source": "pubsub",
            "event_type": "LoginEvent",
        }
        result = render_labels(labels)
        # keys must be in ascending order
        assert result.startswith("{")
        assert result.endswith("}")
        keys_in_order = [part.split("=")[0] for part in result[1:-1].split(",")]
        assert keys_in_order == sorted(keys_in_order)

    def test_escapes_double_quote_in_value(self) -> None:
        result = render_labels({"job": 'say "hi"'})
        assert result == '{job="say \\"hi\\""}'

    def test_escapes_backslash_in_value(self) -> None:
        result = render_labels({"job": "a\\b"})
        assert result == '{job="a\\\\b"}'

    def test_escapes_newline_in_value(self) -> None:
        result = render_labels({"job": "line1\nline2"})
        assert result == '{job="line1\\nline2"}'

    def test_escapes_combined_special_chars(self) -> None:
        result = render_labels({"job": 'back\\slash and "quote"\nnewline'})
        assert result == '{job="back\\\\slash and \\"quote\\"\\nnewline"}'
