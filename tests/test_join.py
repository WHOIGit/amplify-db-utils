"""Tests for join()."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from tests.conftest import ImageRecord, make_image


def _setup_join(store):
    """Set up images and a geo_index table for join tests."""
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])

    import pyarrow as pa
    geo_schema = pa.schema([
        pa.field("image_id", pa.utf8()),
        pa.field("lat", pa.float64()),
        pa.field("lon", pa.float64()),
        pa.field("instrument", pa.utf8()),
        pa.field("year", pa.int64()),
        pa.field("month", pa.int64()),
    ])
    store.create_table("geo_index", geo_schema, partition_by=["instrument", "year", "month"])

    images = [
        make_image("img_001", instrument="IFCB107", year=2024, month=1),
        make_image("img_002", instrument="IFCB107", year=2024, month=1),
        make_image("img_003", instrument="IFCB107", year=2024, month=1),
    ]
    store.write("images", images)

    geo_records = [
        {"image_id": "img_001", "lat": 41.5, "lon": -70.5, "instrument": "IFCB107", "year": 2024, "month": 1},
        {"image_id": "img_002", "lat": 42.0, "lon": -71.0, "instrument": "IFCB107", "year": 2024, "month": 1},
        # img_003 has no geolocation
    ]
    store.write("geo_index", geo_records)

    return store


def test_join_basic(store):
    _setup_join(store)
    rows = list(store.join("geo_index", "images", on="image_id"))
    assert len(rows) == 2  # Only images with geolocation


def test_join_select_right(store):
    """select='right' returns only right table columns — the canonical spatial-filter pattern."""
    _setup_join(store)
    rows = list(store.join("geo_index", "images", on="image_id", select="right"))
    assert len(rows) == 2
    # Should have image columns, not geo columns
    for row in rows:
        assert "timestamp" in row
        assert "image_id" in row


def test_join_select_left(store):
    _setup_join(store)
    rows = list(store.join("geo_index", "images", on="image_id", select="left"))
    assert len(rows) == 2
    for row in rows:
        assert "lat" in row
        assert "lon" in row


def test_join_left_filter(store):
    """Filter on the left table (geo_index) to get images in a bounding box."""
    _setup_join(store)
    rows = list(store.join(
        "geo_index", "images", on="image_id",
        left_filters={"lat": {"gte": 42.0}},
        select="right",
    ))
    assert len(rows) == 1
    assert rows[0]["image_id"] == "img_002"


def test_join_right_filter(store):
    _setup_join(store)
    rows = list(store.join(
        "geo_index", "images", on="image_id",
        right_filters={"image_id": "img_001"},
        select="left",
    ))
    assert len(rows) == 1
    assert rows[0]["image_id"] == "img_001"
    assert rows[0]["lat"] == pytest.approx(41.5)


def test_join_empty_result(store):
    _setup_join(store)
    rows = list(store.join(
        "geo_index", "images", on="image_id",
        left_filters={"lat": {"gte": 90.0}},  # no lat this high
    ))
    assert rows == []


def test_join_no_matches(store):
    """If no rows match the join condition, returns empty."""
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    import pyarrow as pa
    geo_schema = pa.schema([
        pa.field("image_id", pa.utf8()),
        pa.field("instrument", pa.utf8()),
        pa.field("year", pa.int64()),
        pa.field("month", pa.int64()),
    ])
    store.create_table("geo_index", geo_schema, partition_by=["instrument", "year", "month"])
    store.write("images", [make_image("img_X")])
    store.write("geo_index", [{"image_id": "img_Y", "instrument": "IFCB107", "year": 2024, "month": 1}])
    rows = list(store.join("geo_index", "images", on="image_id"))
    assert rows == []
