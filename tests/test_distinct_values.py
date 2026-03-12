"""Tests for distinct_values()."""

from __future__ import annotations

import pytest

from tests.conftest import ImageRecord, make_image


def _setup(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    store.write("images", [
        make_image("img_a", instrument="IFCB107", year=2024, month=1),
        make_image("img_b", instrument="IFCB107", year=2024, month=2),
        make_image("img_c", instrument="IFCB200", year=2024, month=1),
        make_image("img_d", instrument="IFCB200", year=2025, month=1),
    ])
    return store


# ---------------------------------------------------------------------------
# Path A: Hive directory introspection (fields == partition_by, no filters)
# ---------------------------------------------------------------------------


def test_distinct_partition_keys_hive_path(store):
    """Fields matching partition_by exactly with no filters uses directory listing."""
    _setup(store)
    results = store.distinct_values("images", ["instrument", "year", "month"])
    assert len(results) == 4
    # Each result is a dict with the partition key fields
    for r in results:
        assert set(r.keys()) == {"instrument", "year", "month"}


def test_distinct_partition_keys_values(store):
    _setup(store)
    results = store.distinct_values("images", ["instrument", "year", "month"])
    combos = {(r["instrument"], r["year"], r["month"]) for r in results}
    assert ("IFCB107", 2024, 1) in combos
    assert ("IFCB107", 2024, 2) in combos
    assert ("IFCB200", 2024, 1) in combos
    assert ("IFCB200", 2025, 1) in combos


def test_distinct_partition_keys_empty_table(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    results = store.distinct_values("images", ["instrument", "year", "month"])
    assert results == []


# ---------------------------------------------------------------------------
# Path B: SQL SELECT DISTINCT (non-partition fields or filters present)
# ---------------------------------------------------------------------------


def test_distinct_non_partition_field(store):
    """Single non-partition field uses SQL DISTINCT."""
    _setup(store)
    results = store.distinct_values("images", ["instrument"])
    instruments = {r["instrument"] for r in results}
    assert instruments == {"IFCB107", "IFCB200"}


def test_distinct_with_filter(store):
    """Filters trigger SQL path even if fields match partition_by."""
    _setup(store)
    results = store.distinct_values(
        "images",
        ["instrument", "year", "month"],
        filters={"instrument": "IFCB107"},
    )
    assert len(results) == 2
    assert all(r["instrument"] == "IFCB107" for r in results)


def test_distinct_subset_of_partition_keys(store):
    """Subset of partition keys uses SQL path."""
    _setup(store)
    results = store.distinct_values("images", ["instrument"])
    instruments = {r["instrument"] for r in results}
    assert "IFCB107" in instruments
    assert "IFCB200" in instruments


def test_distinct_empty_table_sql_path(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    results = store.distinct_values("images", ["instrument"])
    assert results == []
