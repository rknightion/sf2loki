"""Unit tests for the declarative transform engine (issue #27)."""

from __future__ import annotations

import hashlib

from sf2loki.config import TransformRule
from sf2loki.obs.metrics import Metrics
from sf2loki.transforms import compile_rules, unsalted_hash_warnings


def _pipeline(rules: list[TransformRule], *, salt: str = "", metrics: Metrics | None = None):
    return compile_rules(rules, salt=salt, source="pubsub", metrics=metrics)


# ---------------------------------------------------------------------------
# hash


def test_hash_replaces_value_with_16_char_pseudonym() -> None:
    pipe = _pipeline([TransformRule(action="hash", fields=["USER_ID"])], salt="s3cret")
    out = pipe.apply({"USER_ID": "005abc", "OTHER": "keep"})
    assert out is not None
    expected = hashlib.sha256(b"s3cret005abc").hexdigest()[:16]
    assert out["USER_ID"] == expected
    assert len(out["USER_ID"]) == 16
    assert out["OTHER"] == "keep"  # untouched


def test_hash_is_stable_across_calls() -> None:
    pipe = _pipeline([TransformRule(action="hash", fields=["IP"])], salt="salt")
    a = pipe.apply({"IP": "203.0.113.7"})
    b = pipe.apply({"IP": "203.0.113.7"})
    assert a is not None and b is not None
    assert a["IP"] == b["IP"]


def test_hash_is_salt_sensitive() -> None:
    unsalted = _pipeline([TransformRule(action="hash", fields=["IP"])], salt="")
    salted = _pipeline([TransformRule(action="hash", fields=["IP"])], salt="pepper")
    a = unsalted.apply({"IP": "203.0.113.7"})
    b = salted.apply({"IP": "203.0.113.7"})
    assert a is not None and b is not None
    assert a["IP"] != b["IP"]


def test_hash_coerces_non_str_and_leaves_none_untouched() -> None:
    pipe = _pipeline([TransformRule(action="hash", fields=["N", "MISSING", "NULL"])], salt="")
    out = pipe.apply({"N": 12345, "NULL": None})
    assert out is not None
    assert out["N"] == hashlib.sha256(b"12345").hexdigest()[:16]
    assert out["NULL"] is None  # None left untouched
    assert "MISSING" not in out  # absent left untouched


# ---------------------------------------------------------------------------
# mask


def test_mask_email_keeps_domain() -> None:
    pipe = _pipeline([TransformRule(action="mask", fields=["EMAIL"])])
    out = pipe.apply({"EMAIL": "alice@example.com"})
    assert out is not None
    assert out["EMAIL"] == "***@example.com"


def test_mask_ipv4_truncates_to_24() -> None:
    pipe = _pipeline([TransformRule(action="mask", fields=["IP"])])
    out = pipe.apply({"IP": "203.0.113.7"})
    assert out is not None
    assert out["IP"] == "203.0.113.x"


def test_mask_other_becomes_stars() -> None:
    pipe = _pipeline([TransformRule(action="mask", fields=["NAME", "NUM"])])
    out = pipe.apply({"NAME": "Alice Smith", "NUM": 42})
    assert out is not None
    assert out["NAME"] == "***"
    assert out["NUM"] == "***"  # non-str coerced, not an IPv4/email


def test_mask_non_ipv4_dotted_is_not_truncated() -> None:
    # Out-of-range octet: not a valid IPv4, so falls through to "***".
    pipe = _pipeline([TransformRule(action="mask", fields=["X"])])
    out = pipe.apply({"X": "999.1.1.1"})
    assert out is not None
    assert out["X"] == "***"


# ---------------------------------------------------------------------------
# drop_field


def test_drop_field_removes_key() -> None:
    pipe = _pipeline([TransformRule(action="drop_field", fields=["SECRET", "GONE"])])
    out = pipe.apply({"SECRET": "x", "KEEP": "y"})
    assert out is not None
    assert "SECRET" not in out
    assert "GONE" not in out  # absent key: no error
    assert out["KEEP"] == "y"


# ---------------------------------------------------------------------------
# regex_replace


def test_regex_replace_with_backreference() -> None:
    pipe = _pipeline(
        [
            TransformRule(
                action="regex_replace",
                fields=["MSG"],
                pattern=r"(\d{4})\d{8}(\d{4})",
                replacement=r"\1********\2",
            )
        ]
    )
    out = pipe.apply({"MSG": "card 1234567812345678 used"})
    assert out is not None
    assert out["MSG"] == "card 1234********5678 used"


def test_regex_replace_coerces_non_str() -> None:
    pipe = _pipeline(
        [TransformRule(action="regex_replace", fields=["N"], pattern=r"\d", replacement="#")]
    )
    out = pipe.apply({"N": 42})
    assert out is not None
    assert out["N"] == "##"


# ---------------------------------------------------------------------------
# drop_row


def test_drop_row_equality_match_drops_and_counts() -> None:
    metrics = Metrics()
    pipe = compile_rules(
        [TransformRule(action="drop_row", match={"EVENT_TYPE": "Sites"})],
        salt="",
        source="eventlogfile",
        metrics=metrics,
    )
    assert pipe.apply({"EVENT_TYPE": "Sites", "X": "1"}) is None
    # default rule name is "<action>-<index>"
    val = metrics.registry.get_sample_value(
        "sf2loki_rows_filtered_total", {"source": "eventlogfile", "rule": "drop_row-0"}
    )
    assert val == 1.0


def test_drop_row_non_match_keeps_row() -> None:
    metrics = Metrics()
    pipe = compile_rules(
        [TransformRule(action="drop_row", match={"EVENT_TYPE": "Sites"})],
        salt="",
        source="eventlogfile",
        metrics=metrics,
    )
    out = pipe.apply({"EVENT_TYPE": "Login"})
    assert out is not None
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_rows_filtered_total", {"source": "eventlogfile", "rule": "drop_row-0"}
        )
        is None
    )


def test_drop_row_glob_match() -> None:
    pipe = _pipeline([TransformRule(action="drop_row", match={"URI": "/internal/*"})])
    assert pipe.apply({"URI": "/internal/health"}) is None
    assert pipe.apply({"URI": "/public/home"}) is not None


def test_drop_row_requires_all_fields_to_match() -> None:
    pipe = _pipeline([TransformRule(action="drop_row", match={"A": "x", "B": "y"})])
    # both match -> drop
    assert pipe.apply({"A": "x", "B": "y"}) is None
    # only one matches -> keep
    assert pipe.apply({"A": "x", "B": "z"}) is not None


def test_drop_row_missing_field_matches_empty_glob() -> None:
    # str(payload.get(field, "")) == "" so a "*" glob still matches an absent field.
    pipe = _pipeline([TransformRule(action="drop_row", match={"MISSING": "*"})])
    assert pipe.apply({"OTHER": "1"}) is None


def test_drop_row_custom_name_is_metric_label() -> None:
    metrics = Metrics()
    pipe = compile_rules(
        [TransformRule(action="drop_row", match={"K": "v"}, name="soql-noise")],
        salt="",
        source="eventlog_objects",
        metrics=metrics,
    )
    assert pipe.apply({"K": "v"}) is None
    assert (
        metrics.registry.get_sample_value(
            "sf2loki_rows_filtered_total", {"source": "eventlog_objects", "rule": "soql-noise"}
        )
        == 1.0
    )


# ---------------------------------------------------------------------------
# pipeline composition + no-op


def test_empty_pipeline_is_identity_and_falsy() -> None:
    pipe = _pipeline([])
    assert not pipe
    payload = {"A": "1"}
    out = pipe.apply(payload)
    assert out is payload  # identity, in place


def test_rules_apply_in_order() -> None:
    # mask the IP, then drop the row only if a DIFFERENT field matches — proves
    # both rules run and order is preserved (mask does not affect the drop match).
    pipe = _pipeline(
        [
            TransformRule(action="mask", fields=["IP"]),
            TransformRule(action="drop_row", match={"ENV": "test"}),
        ]
    )
    kept = pipe.apply({"IP": "203.0.113.7", "ENV": "prod"})
    assert kept is not None
    assert kept["IP"] == "203.0.113.x"
    assert pipe.apply({"IP": "203.0.113.7", "ENV": "test"}) is None


def test_pipeline_mutates_in_place() -> None:
    pipe = _pipeline([TransformRule(action="drop_field", fields=["X"])])
    payload = {"X": "1", "Y": "2"}
    out = pipe.apply(payload)
    assert out is payload
    assert payload == {"Y": "2"}


def test_drop_row_without_metrics_does_not_crash() -> None:
    pipe = compile_rules(
        [TransformRule(action="drop_row", match={"K": "v"})],
        salt="",
        source="pubsub",
        metrics=None,
    )
    assert pipe.apply({"K": "v"}) is None


# ---------------------------------------------------------------------------
# unsalted_hash_warnings (issue #67)


def test_unsalted_hash_rule_warns_when_salt_empty() -> None:
    rules = [TransformRule(action="hash", fields=["USER_ID"])]
    warnings = unsalted_hash_warnings(rules, "")
    assert len(warnings) == 1
    assert "USER_ID" in warnings[0]
    assert "rainbow-table" in warnings[0] or "reversible" in warnings[0]


def test_hash_rule_with_salt_produces_no_warning() -> None:
    rules = [TransformRule(action="hash", fields=["USER_ID"])]
    assert unsalted_hash_warnings(rules, "s3cret") == []


def test_non_hash_rules_never_warn_regardless_of_salt() -> None:
    rules = [
        TransformRule(action="mask", fields=["EMAIL"]),
        TransformRule(action="drop_field", fields=["SECRET"]),
    ]
    assert unsalted_hash_warnings(rules, "") == []


def test_no_rules_produces_no_warnings() -> None:
    assert unsalted_hash_warnings([], "") == []


def test_multiple_unsalted_hash_rules_each_get_a_warning() -> None:
    rules = [
        TransformRule(action="hash", fields=["A"]),
        TransformRule(action="mask", fields=["B"]),
        TransformRule(action="hash", fields=["C"], name="hash-c"),
    ]
    warnings = unsalted_hash_warnings(rules, "")
    assert len(warnings) == 2
    assert any("hash-0" in w or "['A']" in w for w in warnings)
    assert any("hash-c" in w for w in warnings)


def test_unsalted_hash_warning_names_default_rule_like_compile_rules() -> None:
    # Mirrors compile_rules' "<action>-<index>" default naming so the warning
    # names the same rule an operator would see in other diagnostics.
    rules = [
        TransformRule(action="mask", fields=["B"]),
        TransformRule(action="hash", fields=["A"]),
    ]
    warnings = unsalted_hash_warnings(rules, "")
    assert len(warnings) == 1
    assert "hash-1" in warnings[0]
