"""Multi-org config: topology validation, secret resolution, and org selection."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from sf2loki.config import (
    Config,
    ConfigError,
    OrgConfig,
    SourcesConfig,
    as_single_org_view,
    resolve_secrets,
    select_org,
)


def _sf(**over: Any) -> dict[str, Any]:
    base: dict[str, Any] = {"client_id": "cid", "username": "svc@example.com", "private_key": "PK"}
    base.update(over)
    return base


def _sink() -> dict[str, Any]:
    return {"loki": {"url": "http://loki:3100/loki/api/v1/push"}}


# --- exactly-one-of topology ------------------------------------------------


def test_single_org_top_level_salesforce_is_accepted() -> None:
    cfg = Config(salesforce=_sf(), sink=_sink())
    orgs = cfg.resolved_orgs()
    assert len(orgs) == 1
    assert orgs[0].name == ""  # legacy empty-name sentinel (no prefix, no org label)


def test_multi_org_list_is_accepted() -> None:
    cfg = Config(
        orgs=[
            {"name": "prod", "salesforce": _sf()},
            {"name": "emea", "salesforce": _sf(client_id="other")},
        ],
        sink=_sink(),
    )
    assert [o.name for o in cfg.resolved_orgs()] == ["prod", "emea"]


def test_both_salesforce_and_orgs_is_rejected() -> None:
    with pytest.raises(ValidationError, match="not both"):
        Config(salesforce=_sf(), orgs=[{"name": "p", "salesforce": _sf()}], sink=_sink())


def test_neither_salesforce_nor_orgs_is_rejected() -> None:
    with pytest.raises(ValidationError, match="no Salesforce org configured"):
        Config(sink=_sink())


def test_duplicate_org_names_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate org name"):
        Config(
            orgs=[
                {"name": "prod", "salesforce": _sf()},
                {"name": "prod", "salesforce": _sf()},
            ],
            sink=_sink(),
        )


def test_org_name_pattern_enforced() -> None:
    with pytest.raises(ValidationError):
        Config(orgs=[{"name": "bad name!", "salesforce": _sf()}], sink=_sink())


def test_top_level_sources_rejected_with_orgs() -> None:
    with pytest.raises(ValidationError, match="top-level 'sources' cannot be combined"):
        Config(
            orgs=[{"name": "prod", "salesforce": _sf()}],
            sources={"eventlogfile": {"enabled": True, "event_types": ["Login"]}},
            sink=_sink(),
        )


def test_default_top_level_sources_allowed_with_orgs() -> None:
    # An untouched (default) top-level sources block must not trip the guard.
    cfg = Config(orgs=[{"name": "prod", "salesforce": _sf()}], sink=_sink())
    assert cfg.sources == SourcesConfig()


def test_load_surfaces_topology_error_as_config_error(tmp_path: Path) -> None:
    from sf2loki.config import load

    p = tmp_path / "bad.yaml"
    p.write_text("sink:\n  loki:\n    url: http://x\n")  # neither salesforce nor orgs
    with pytest.raises(ConfigError, match="no Salesforce org configured"):
        load(p)


# --- per-org secret resolution ----------------------------------------------


def test_per_org_secret_files_resolved(tmp_path: Path) -> None:
    key_a = tmp_path / "a.pem"
    key_a.write_text("KEYA\n")
    key_b = tmp_path / "b.pem"
    key_b.write_text("KEYB\n")
    cfg = Config(
        orgs=[
            {"name": "prod", "salesforce": _sf(private_key=None, private_key_file=str(key_a))},
            {"name": "emea", "salesforce": _sf(private_key=None, private_key_file=str(key_b))},
        ],
        sink=_sink(),
    )
    resolve_secrets(cfg)
    assert cfg.orgs[0].salesforce.private_key is not None
    assert cfg.orgs[0].salesforce.private_key.get_secret_value() == "KEYA"
    assert cfg.orgs[1].salesforce.private_key.get_secret_value() == "KEYB"


def test_per_org_missing_secret_names_the_org(tmp_path: Path) -> None:
    cfg = Config(
        orgs=[
            {"name": "prod", "salesforce": _sf()},
            {"name": "emea", "salesforce": _sf(private_key=None)},
        ],
        sink=_sink(),
    )
    with pytest.raises(ConfigError, match="org 'emea'"):
        resolve_secrets(cfg)


def test_per_org_transform_salt_resolved(tmp_path: Path) -> None:
    salt = tmp_path / "salt"
    salt.write_text("PEPPER\n")
    cfg = Config(
        orgs=[
            {
                "name": "prod",
                "salesforce": _sf(),
                "sources": {"transform_salt_file": str(salt)},
            }
        ],
        sink=_sink(),
    )
    resolve_secrets(cfg)
    assert cfg.orgs[0].sources.transform_salt is not None
    assert cfg.orgs[0].sources.transform_salt.get_secret_value() == "PEPPER"


# --- org selection for the single-org CLI commands --------------------------


def test_select_org_defaults_to_first() -> None:
    cfg = Config(
        orgs=[{"name": "prod", "salesforce": _sf()}, {"name": "emea", "salesforce": _sf()}],
        sink=_sink(),
    )
    org, note = select_org(cfg, None)
    assert org.name == "prod"
    assert note is not None and "prod" in note  # multi-org note names the chosen org


def test_select_org_by_name() -> None:
    cfg = Config(
        orgs=[{"name": "prod", "salesforce": _sf()}, {"name": "emea", "salesforce": _sf()}],
        sink=_sink(),
    )
    org, _ = select_org(cfg, "emea")
    assert org.name == "emea"


def test_select_org_unknown_name_errors() -> None:
    cfg = Config(orgs=[{"name": "prod", "salesforce": _sf()}], sink=_sink())
    with pytest.raises(ConfigError, match="not configured"):
        select_org(cfg, "nope")


def test_select_org_single_org_no_note() -> None:
    cfg = Config(salesforce=_sf(), sink=_sink())
    org, note = select_org(cfg, None)
    assert org.name == ""
    assert note is None  # single-org: no scoping note


def test_as_single_org_view_sets_salesforce_and_sources() -> None:
    cfg = Config(
        orgs=[
            {
                "name": "prod",
                "salesforce": _sf(client_id="prod-cid"),
                "sources": {"eventlogfile": {"enabled": True, "event_types": ["Login"]}},
            }
        ],
        sink=_sink(),
    )
    org, _ = select_org(cfg, "prod")
    view = as_single_org_view(cfg, org)
    assert view.salesforce is not None
    assert view.salesforce.client_id == "prod-cid"
    assert view.sources.eventlogfile.enabled is True
    assert view.orgs == []
    # Shared blocks are untouched.
    assert view.sink.loki.url == cfg.sink.loki.url


def test_resolved_orgs_returns_orgconfig_type() -> None:
    cfg = Config(salesforce=_sf(), sink=_sink())
    assert isinstance(cfg.resolved_orgs()[0], OrgConfig)
