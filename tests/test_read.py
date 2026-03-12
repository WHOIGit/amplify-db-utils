"""Tests for read() and bulk_read()."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from tests.conftest import ImageRecord, make_image


def _setup_images(store, records=None):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    if records is None:
        records = [
            make_image("img_jan_1", instrument="IFCB107", year=2024, month=1),
            make_image("img_jan_2", instrument="IFCB107", year=2024, month=1),
            make_image("img_feb_1", instrument="IFCB107", year=2024, month=2),
            make_image("img_other", instrument="IFCB200", year=2024, month=1),
        ]
    store.write("images", records)
    return store


# ---------------------------------------------------------------------------
# read() — iterator
# ---------------------------------------------------------------------------


def test_read_all(store):
    _setup_images(store)
    rows = list(store.read("images"))
    assert len(rows) == 4


def test_read_returns_dicts(store):
    _setup_images(store, [make_image()])
    rows = list(store.read("images"))
    assert isinstance(rows[0], dict)
    assert "image_id" in rows[0]


def test_read_equality_filter(store):
    _setup_images(store)
    rows = list(store.read("images", filters={"instrument": "IFCB200"}))
    assert len(rows) == 1
    assert rows[0]["instrument"] == "IFCB200"


def test_read_multi_filter(store):
    _setup_images(store)
    rows = list(store.read("images", filters={"instrument": "IFCB107", "month": 1}))
    assert len(rows) == 2


def test_read_range_filter(store):
    _setup_images(store)
    rows = list(store.read("images", filters={"month": {"gte": 2}}))
    assert all(r["month"] >= 2 for r in rows)
    assert len(rows) == 1


def test_read_in_filter(store):
    _setup_images(store)
    rows = list(store.read("images", filters={"month": {"in": [1, 2]}}))
    assert len(rows) == 4


def test_read_empty_result(store):
    _setup_images(store)
    rows = list(store.read("images", filters={"instrument": "NONEXISTENT"}))
    assert rows == []


def test_read_empty_table(store):
    """Reading a table with no data should yield no rows (not error)."""
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    rows = list(store.read("images"))
    assert rows == []


# ---------------------------------------------------------------------------
# bulk_read() — Arrow Table
# ---------------------------------------------------------------------------


def test_bulk_read_returns_arrow_table(store):
    _setup_images(store, [make_image()])
    result = store.bulk_read("images")
    assert isinstance(result, pa.Table)


def test_bulk_read_content(store):
    _setup_images(store, [make_image("my_img")])
    result = store.bulk_read("images")
    assert len(result) == 1
    ids = result.column("image_id").to_pylist()
    assert "my_img" in ids


def test_bulk_read_filtered(store):
    _setup_images(store)
    result = store.bulk_read("images", filters={"instrument": "IFCB107", "month": 1})
    assert len(result) == 2
    assert all(r == "IFCB107" for r in result.column("instrument").to_pylist())


def test_bulk_read_empty_table(store):
    """bulk_read on an empty table returns an empty Arrow Table."""
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    result = store.bulk_read("images")
    assert isinstance(result, pa.Table)
    assert len(result) == 0


# ---------------------------------------------------------------------------
# count()
# ---------------------------------------------------------------------------


def test_count_all(store):
    _setup_images(store)
    assert store.count("images") == 4


def test_count_filtered(store):
    _setup_images(store)
    assert store.count("images", filters={"instrument": "IFCB107"}) == 3


def test_count_empty(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    assert store.count("images") == 0
