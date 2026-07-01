"""Tests for the synthetic activity generator (scripts/generate_activity.py).

The generator is a self-contained dev utility living under scripts/, so it is
imported by path rather than as a package.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import generate_activity as g

# --- marker filtering (cleanup) --------------------------------------------


def test_marker_ids_selects_only_marked_records() -> None:
    records = [
        {"Id": "a", "Description": f"{g.MARKER} synthetic account"},
        {"Id": "b", "Description": "a real customer record"},
        {"Id": "c", "Description": f"prefix {g.MARKER} suffix"},
    ]
    assert g.marker_ids(records) == ["a", "c"]


def test_marker_ids_tolerates_missing_or_null_description() -> None:
    records = [
        {"Id": "a"},  # no Description key at all
        {"Id": "b", "Description": None},
        {"Id": "c", "Description": f"{g.MARKER} keep me"},
    ]
    assert g.marker_ids(records) == ["c"]


# --- CSV pool loading -------------------------------------------------------


def test_load_companies_parses_rows_and_skips_malformed(tmp_path: Path) -> None:
    csv = tmp_path / "companies.csv"
    csv.write_text(
        "Northwind Labs,Technology,https://northwind.example,Great data platform\n"
        "\n"  # blank line
        "TooFewColumns,Retail\n"  # short row -> skipped
        "Acme Freight,Transportation,https://acme.example,Moves things fast\n"
    )
    companies = g.load_companies(tmp_path)
    assert [c.name for c in companies] == ["Northwind Labs", "Acme Freight"]
    assert companies[0].industry == "Technology"
    assert companies[0].website == "https://northwind.example"
    assert companies[0].description == "Great data platform"


def test_load_people_parses_rows_and_skips_malformed(tmp_path: Path) -> None:
    csv = tmp_path / "people.csv"
    csv.write_text(
        "Olivia,Smith,VP Engineering\n"
        "onlyone\n"  # skipped
        "Wei,Zhang,CTO\n"
    )
    people = g.load_people(tmp_path)
    assert [(p.first, p.last, p.title) for p in people] == [
        ("Olivia", "Smith", "VP Engineering"),
        ("Wei", "Zhang", "CTO"),
    ]


def test_load_companies_falls_back_to_builtins_when_file_absent(tmp_path: Path) -> None:
    companies = g.load_companies(tmp_path / "does-not-exist")
    assert len(companies) > 0  # built-in fallback pool


def test_load_people_falls_back_to_builtins_when_file_absent(tmp_path: Path) -> None:
    people = g.load_people(tmp_path / "does-not-exist")
    assert len(people) > 0


# --- cleanup behaviour ------------------------------------------------------


class _FakeSF:
    """Minimal stand-in exercising cleanup()'s query/delete contract."""

    instance_url = "https://example.my.salesforce.com"

    def __init__(self, records_by_object: dict[str, list[dict]]) -> None:
        self._records = records_by_object
        self.queries: list[str] = []
        self.deleted: list[str] = []

    async def query(self, soql: str) -> dict:
        self.queries.append(soql)
        obj = soql.split(" FROM ")[1].split()[0]
        return {"records": self._records.get(obj, []), "done": True}

    async def delete_many(self, ids: list[str]) -> int:
        self.deleted.extend(ids)
        return len(ids)


async def test_cleanup_deletes_only_marked_records_and_never_filters_description() -> None:
    marked = {"Id": "keep", "Description": f"{g.MARKER} synthetic account"}
    real = {"Id": "real", "Description": "genuine customer data"}
    sf = _FakeSF({obj: [marked, real] for obj in g.CLEANUP_OBJECTS})

    await g.cleanup(sf)

    # every marked record deleted, no real records touched
    assert set(sf.deleted) == {"keep"}
    assert "real" not in sf.deleted
    # queries select Description for client-side filtering, never LIKE-filter it
    assert all("Description" in q for q in sf.queries)
    assert all("LIKE" not in q for q in sf.queries)


@pytest.mark.parametrize("obj", g.CLEANUP_OBJECTS)
def test_cleanup_objects_are_child_before_parent(obj: str) -> None:
    # Account must be deleted last so its children are gone first.
    assert g.CLEANUP_OBJECTS[-1] == "Account"


def test_marker_ids_can_project_an_alternate_id_field() -> None:
    records = [
        {"ContentDocumentId": "doc1", "Description": f"{g.MARKER} file"},
        {"ContentDocumentId": "doc2", "Description": "real file"},
    ]
    assert g.marker_ids(records, "ContentDocumentId") == ["doc1"]


# --- capability-gated action menu ------------------------------------------


def _engine() -> g.ActivityEngine:
    return g.ActivityEngine(sf=object())  # type: ignore[arg-type]


def _menu_names(engine: g.ActivityEngine) -> set[str]:
    return {action.__name__ for action, _weight in engine._menu()}


def test_menu_always_offers_universal_api_actions() -> None:
    engine = _engine()
    engine.caps = set()
    names = _menu_names(engine)
    for always_on in ("act_file", "act_describe", "act_composite", "act_bulk_query"):
        assert always_on in names


def test_menu_hides_capability_gated_actions_without_caps() -> None:
    engine = _engine()
    engine.caps = set()
    names = _menu_names(engine)
    for gated in (
        "act_report",
        "act_dashboard",
        "act_create_campaign",
        "act_create_contract",
    ):
        assert gated not in names


def test_menu_reveals_actions_for_present_caps() -> None:
    engine = _engine()
    engine.caps = {"report", "dashboard", "campaign", "contract"}
    names = _menu_names(engine)
    for gated in (
        "act_report",
        "act_dashboard",
        "act_create_campaign",
        "act_create_contract",
    ):
        assert gated in names
