import json

import yaml
from pydantic import BaseModel, Field, SecretStr

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


def test_example_yaml_renders_list_of_models_as_yaml_list():
    # Regression: list[Model] fields (e.g. EventLogObjectConfig, EventLogFileTypeConfig)
    # must render as a YAML list of mappings, not a single collapsed mapping.
    doc = yaml.safe_load(configdoc.example_yaml())
    objects = doc["sources"]["eventlog_objects"]["objects"]
    assert isinstance(objects, list)
    assert isinstance(objects[0], dict)
    assert "name" in objects[0]

    event_types = doc["sources"]["eventlogfile"]["event_types"]
    assert isinstance(event_types, list)
    assert isinstance(event_types[0], dict)
    assert "name" in event_types[0]


def test_example_yaml_renders_wildcard_list_as_literal_list():
    # Regression: a ["*"] default must round-trip as the literal list ["*"],
    # not as an unquoted "*" which YAML would parse as an alias reference.
    doc = yaml.safe_load(configdoc.example_yaml())
    assert doc["sources"]["pubsub"]["include"] == ["*"]


def test_example_yaml_never_leaks_pydantic_undefined_sentinel():
    # Regression: a required leaf with no example/default must fall back to a
    # type-appropriate stub, never leak the PydanticUndefined sentinel.
    text = configdoc.example_yaml()
    assert "PydanticUndefined" not in text


def test_secret_leaf_marks_required_when_no_default():
    # Finding 2: the secret-field render branch must append the same
    # required_tag the generic leaf branch appends. No required SecretStr
    # field exists in the real Config model (all have Optional[..] = None
    # with a *_file fallback), so exercise the private render helper directly
    # against a minimal local model with a required secret leaf.
    class _RequiredSecretModel(BaseModel):
        api_token: SecretStr = Field(description="A required secret token.")

    lines: list[str] = []
    field = _RequiredSecretModel.model_fields["api_token"]
    configdoc._render_field("api_token", field, "", lines)

    assert len(lines) == 1
    assert "(required)" in lines[0]


def test_reference_markdown_has_a_section_per_model_and_lists_limits():
    md = configdoc.reference_markdown()
    assert md.startswith("# ")
    assert "salesforce" in md and "limits" in md and "telemetry" in md
    assert "| Field |" in md  # a table header


def test_reference_markdown_masks_secrets():
    # Regression (deferred from Task 3): reference_markdown() must never leak
    # real secret material or private-key-shaped text — every secret field
    # (SecretStr leaves and *_file secret paths alike) renders the masked
    # placeholder in its Default column instead.
    md = configdoc.reference_markdown()
    assert "private_key" in md
    assert "-----" not in md
    assert "BEGIN PRIVATE KEY" not in md
    for line in md.splitlines():
        if line.startswith("| `private_key`") or line.startswith("| `client_secret`"):
            assert "*(secret)*" in line


def test_render_narrative_comment_wraps_docstring_body_above_model_field():
    # The generator recovers a nested model's class-docstring *body* (the
    # paragraphs after the one-line summary) as a wrapped "#"-comment block
    # rendered immediately above that model's key — this is how the rich
    # pedagogical narrative Task 1 moved into class docstrings (e.g.
    # SourcesConfig's either/or-per-category essay) survives into the
    # generated example.yaml instead of only living in --help/source.
    class _Inner(BaseModel):
        """One-line summary.

        This is the narrative body that should appear as a wrapped comment
        block above the field, not just the one-line summary.
        """

        x: int = Field(default=1, description="An int.")

    class _Outer(BaseModel):
        inner: _Inner = Field(default_factory=_Inner, description="desc")

    lines: list[str] = []
    configdoc._render_field("inner", _Outer.model_fields["inner"], "", lines)

    comment_lines = [line for line in lines if line.startswith("#")]
    assert comment_lines, "expected at least one narrative comment line"
    joined = " ".join(line.lstrip("# ") for line in comment_lines)
    assert "narrative body" in joined
    # The one-line summary itself must not be duplicated into the body block.
    assert "One-line summary" not in joined
    # The narrative comment must precede the field's own "inner:" line.
    inner_index = next(i for i, line in enumerate(lines) if line.startswith("inner:"))
    assert lines.index(comment_lines[0]) < inner_index


def test_json_schema_is_valid_json_for_config():
    obj = json.loads(configdoc.json_schema())
    assert obj["title"] == "Config"
    assert "properties" in obj
