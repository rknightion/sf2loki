"""Tests for the deployment-wide static label set (sf2loki.app.build_static_labels)."""

from __future__ import annotations

from sf2loki.app import build_static_labels


def test_always_sets_job_service_name_environment_and_org() -> None:
    labels = build_static_labels(environment="sandbox", org_id="00DdM0", operator_labels={})
    assert labels["job"] == "sf2loki"
    assert labels["service_name"] == "sf2loki"
    assert labels["environment"] == "sandbox"  # derived from salesforce.environment
    assert labels["sf_org_id"] == "00DdM0"


def test_operator_labels_override_defaults() -> None:
    labels = build_static_labels(
        environment="production",
        org_id="00DdM0",
        operator_labels={"service_name": "salesforce", "environment": "prod-eu"},
    )
    # Operator-supplied sink.loki.labels win over the derived defaults...
    assert labels["service_name"] == "salesforce"
    assert labels["environment"] == "prod-eu"
    # ...but job + sf_org_id are still present.
    assert labels["job"] == "sf2loki"
    assert labels["sf_org_id"] == "00DdM0"
