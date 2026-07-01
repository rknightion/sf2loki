"""Generate an annotated example YAML document, a Markdown config reference,
and a JSON schema from the ``Config`` model.

Pure generator: no file I/O, no argparse. Walks ``model.model_fields``
recursively (in model-declaration order) and renders each field as
``key: <value>  # <description>`` with nested models rendered as an indented
block. Deliberately does NOT round-trip pydantic's ``model_dump()`` — that
would drop comments and render durations/paths in the wrong shape.

``reference_markdown()`` walks the same recursive set of nested models (via
``_iter_models``) to emit one Markdown table per model.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from types import UnionType
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel, SecretStr
from pydantic.fields import FieldInfo
from pydantic_core import PydanticUndefined
from pydantic_settings import BaseSettings

from sf2loki.config import _DURATION_UNIT_SECONDS, Config

_HEADER = """\
# sf2loki example configuration.
#
# Precedence (highest first): env (SF2LOKI_* with __ nesting) > this file > defaults.
# ${VAR} placeholders below are interpolated from the environment at load time
# (missing var = fatal). Secrets are injected from *_file paths (mounted secret
# files); a missing/unreadable secret file is fatal at startup too (no silent
# blanks, in either case).
"""

_SECRET_PLACEHOLDER_ROOT = "/etc/sf2loki/secrets"

# Largest-unit-first so shorthand formatting picks the coarsest exact unit.
_DURATION_UNITS_LARGEST_FIRST: list[str] = sorted(
    _DURATION_UNIT_SECONDS, key=lambda u: _DURATION_UNIT_SECONDS[u], reverse=True
)


def _fmt_duration(td: timedelta) -> str:
    """Render a timedelta as Go-style shorthand: 5m, 1h, 24h, 1s, 500ms, 0s."""
    total_seconds = td.total_seconds()
    if total_seconds == 0:
        return "0s"
    for unit in _DURATION_UNITS_LARGEST_FIRST:
        unit_seconds = _DURATION_UNIT_SECONDS[unit]
        quotient = total_seconds / unit_seconds
        if abs(quotient - round(quotient)) < 1e-9:
            return f"{round(quotient)}{unit}"
    # Fallback: shouldn't normally happen given the unit table covers ms..w.
    return f"{total_seconds}s"


def _is_secret_field(name: str, annotation: Any) -> bool:
    if _unwrap_optional(annotation) is SecretStr:
        return True
    return name.endswith("_file") and ("secret" in name or "key" in name or "token" in name)


def _unwrap_optional(annotation: Any) -> Any:
    """Strip an Optional[...] / X | None wrapper down to the inner type.

    Only unwraps actual unions (``X | None``); leaves ``list[X]``, ``dict[K, V]``,
    etc. untouched (their origin is ``list``/``dict``, not a union).
    """
    origin = get_origin(annotation)
    if origin is Union or origin is UnionType:
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _is_model_type(annotation: Any) -> bool:
    inner = _unwrap_optional(annotation)
    return isinstance(inner, type) and issubclass(inner, BaseModel)


def _list_item_model_type(annotation: Any) -> type[BaseModel] | None:
    """If annotation is list[SomeBaseModel] (optionally Optional), return SomeBaseModel."""
    inner = _unwrap_optional(annotation)
    if get_origin(inner) is list:
        args = get_args(inner)
        if args:
            item = args[0]
            if isinstance(item, type) and issubclass(item, BaseModel):
                return item
    return None


def _resolve_value(field: FieldInfo) -> Any:
    if field.examples:
        return field.examples[0]
    default = field.get_default(call_default_factory=True)
    if default is not PydanticUndefined:
        return default
    # No example, no default (required leaf): type-appropriate stub.
    annotation = _unwrap_optional(field.annotation)
    if annotation is str:
        return ""
    return None


def _fmt_scalar(value: Any) -> str:
    if isinstance(value, timedelta):
        return _fmt_duration(value)
    if isinstance(value, Path):
        return str(value)
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        if value == "":
            return '""'
        # Quote values that YAML would otherwise misparse: leading digit,
        # reserved scalars, or a leading indicator character (*, &, !, |, >,
        # %, @, `, ', ", -, ?, :, [, ], {, }, ,, #) that changes YAML's parse.
        if (
            value[0].isdigit()
            or value in {"null", "true", "false", "~"}
            or value[0] in "*&!|>%@`'\"-?:[]{},#"
        ):
            return f'"{value}"'
        return value
    if isinstance(value, list):
        if not value:
            return "[]"
        return "[" + ", ".join(_fmt_scalar(v) for v in value) + "]"
    if isinstance(value, dict):
        if not value:
            return "{}"
        inner = ", ".join(f"{k}: {_fmt_scalar(v)}" for k, v in value.items())
        return "{" + inner + "}"
    return str(value)


def _first_line(text: str | None) -> str:
    if not text:
        return ""
    return text.strip().splitlines()[0].strip()


def _render_field(name: str, field: FieldInfo, indent: str, lines: list[str]) -> None:
    annotation = field.annotation
    comment = _first_line(field.description)
    is_required = field.is_required()

    if _is_model_type(annotation):
        if comment:
            lines.append(f"{indent}{name}:  # {comment}")
        else:
            lines.append(f"{indent}{name}:")
        inner_type = _unwrap_optional(annotation)
        assert isinstance(inner_type, type) and issubclass(inner_type, BaseModel)
        _render_model(inner_type, indent + "  ", lines)
        return

    item_model = _list_item_model_type(annotation)
    if item_model is not None:
        if comment:
            lines.append(f"{indent}{name}:  # {comment}")
        else:
            lines.append(f"{indent}{name}:")
        item_lines: list[str] = []
        _render_model(item_model, "", item_lines)
        if item_lines:
            lines.append(f"{indent}  - {item_lines[0]}")
            for extra in item_lines[1:]:
                lines.append(f"{indent}    {extra}")
        return

    if _is_secret_field(name, annotation):
        if field.examples:
            value_str = _fmt_scalar(field.examples[0])
        else:
            value_str = f"{_SECRET_PLACEHOLDER_ROOT}/{name.removesuffix('_file').replace('_', '-')}"
            value_str = _fmt_scalar(value_str)
        required_tag = " (required)" if is_required else ""
        if comment:
            lines.append(f"{indent}{name}: {value_str}  # {comment}{required_tag}")
        elif required_tag:
            lines.append(f"{indent}{name}: {value_str}  #{required_tag}")
        else:
            lines.append(f"{indent}{name}: {value_str}")
        return

    value = _resolve_value(field)
    value_str = _fmt_scalar(value)
    required_tag = " (required)" if is_required else ""
    if comment:
        lines.append(f"{indent}{name}: {value_str}  # {comment}{required_tag}")
    elif required_tag:
        lines.append(f"{indent}{name}: {value_str}  #{required_tag}")
    else:
        lines.append(f"{indent}{name}: {value_str}")


def _render_model(model: type[BaseModel], indent: str, lines: list[str]) -> None:
    for name, field in model.model_fields.items():
        _render_field(name, field, indent, lines)


def _iter_models(model: type[BaseModel]) -> list[type[BaseModel]]:
    """Recursively collect every nested ``BaseModel`` reachable from ``model``.

    Traversal order matches ``_render_model``'s (model-declaration order,
    depth-first), including ``model`` itself first. Each model type appears
    once, at its first-seen position, so a model reused in multiple places
    (unlikely here) still gets a single section.
    """
    seen: dict[type[BaseModel], None] = {}

    def visit(m: type[BaseModel]) -> None:
        if m in seen:
            return
        seen[m] = None
        for field in m.model_fields.values():
            annotation = field.annotation
            if _is_model_type(annotation):
                inner = _unwrap_optional(annotation)
                assert isinstance(inner, type) and issubclass(inner, BaseModel)
                visit(inner)
                continue
            item_model = _list_item_model_type(annotation)
            if item_model is not None:
                visit(item_model)

    visit(model)
    return list(seen)


def _fmt_type(annotation: Any) -> str:
    """Render a field annotation as a short, human-readable type string."""
    inner = _unwrap_optional(annotation)
    origin = get_origin(inner)

    if origin is not None:
        args = get_args(inner)
        if origin is list:
            return f"list[{_fmt_type(args[0])}]" if args else "list"
        if origin is dict:
            if len(args) == 2:
                return f"dict[{_fmt_type(args[0])}, {_fmt_type(args[1])}]"
            return "dict"
        # Literal[...] and other typing constructs: fall back to their repr,
        # trimmed of the module-qualified prefix typing sometimes adds.
        name = getattr(origin, "__name__", str(origin))
        if name == "Literal":
            return f"Literal[{', '.join(repr(a) for a in args)}]"
        return str(inner)

    if isinstance(inner, type):
        if inner is timedelta:
            return "Duration"
        return inner.__name__

    return str(inner)


def _md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


def _render_field_row(name: str, field: FieldInfo) -> str:
    annotation = field.annotation
    type_str = _fmt_type(annotation)
    is_required = field.is_required()

    if _is_secret_field(name, annotation):
        # Never render real secret material or a filesystem placeholder here.
        default_str = "*(secret)*"
    elif _is_model_type(annotation) or _list_item_model_type(annotation) is not None:
        default_str = ""
    else:
        value = _resolve_value(field)
        default_str = _fmt_scalar(value) if value is not None else "null"

    description = _md_escape(_first_line(field.description))
    required_str = "yes" if is_required else "no"
    return f"| `{name}` | `{type_str}` | {default_str} | {required_str} | {description} |"


def _render_model_section(model: type[BaseModel], lines: list[str]) -> None:
    lines.append(f"## {model.__name__}")
    lines.append("")
    lines.append("| Field | Type | Default | Required | Description |")
    lines.append("| --- | --- | --- | --- | --- |")
    for name, field in model.model_fields.items():
        lines.append(_render_field_row(name, field))
    lines.append("")


def example_yaml() -> str:
    """Render the full annotated example YAML body for ``Config``.

    Value precedence per field: ``field.examples[0]`` if present, else the
    resolved default, else a type-appropriate stub. Durations render as
    Go-style shorthand (5m, 1h, ...). Secret fields never render a real value.
    """
    lines: list[str] = []
    assert issubclass(Config, BaseSettings)
    _render_model(Config, "", lines)
    body = "\n".join(lines)
    return _HEADER + "\n" + body + "\n"


def reference_markdown() -> str:
    """Render a Markdown configuration reference for ``Config``.

    One ``##`` section per model in the same recursive set ``example_yaml()``
    walks (see ``_iter_models``), each with a ``Field | Type | Default |
    Required | Description`` table. Durations render via ``_fmt_duration``
    (through ``_resolve_value``/``_fmt_scalar``); secret fields show a
    placeholder, never real values.
    """
    lines: list[str] = ["# sf2loki configuration reference", ""]
    for model in _iter_models(Config):
        _render_model_section(model, lines)
    # Single trailing newline: strip any trailing blank lines from the last
    # section, then join with newlines and end with exactly one "\n".
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


def json_schema() -> str:
    """Render the ``Config`` model's JSON schema as pretty-printed JSON."""
    return json.dumps(Config.model_json_schema(), indent=2) + "\n"
