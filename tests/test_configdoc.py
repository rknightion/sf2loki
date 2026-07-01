import yaml

from sf2loki import configdoc


def test_example_yaml_parses_and_covers_top_sections():
    text = configdoc.example_yaml()
    doc = yaml.safe_load(text)
    assert set(doc) >= {"salesforce", "sources", "sink", "state", "service"}


def test_example_yaml_masks_secrets_and_uses_duration_shorthand():
    text = configdoc.example_yaml()
    assert "BEGIN PRIVATE KEY" not in text and "-----" not in text
    # duration fields render as go-style shorthand, not timedelta repr
    assert "0:05:00" not in text and "PT" not in text
    assert "5m" in text  # e.g. limits.poll_interval / eventlog poll_interval


def test_example_yaml_marks_required_fields():
    text = configdoc.example_yaml()
    # client_id and loki.url are required leaves
    assert "client_id" in text and "url" in text
