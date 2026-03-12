"""Tests for write() — append path."""

from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from tests.conftest import (
    ClassificationRecord,
    ImageRecord,
    RecordWithOptional,
    make_classification,
    make_image,
)


def test_write_single_record(image_store):
    image_store.write("images", [make_image()])
    assert image_store.count("images") == 1


def test_write_multiple_records(image_store):
    records = [
        make_image(image_id=f"D20240101T120000_IFCB107_{i:05d}")
        for i in range(10)
    ]
    image_store.write("images", records)
    assert image_store.count("images") == 10


def test_write_appends_on_second_call(image_store):
    image_store.write("images", [make_image(image_id="img_001")])
    image_store.write("images", [make_image(image_id="img_002")])
    assert image_store.count("images") == 2


def test_write_multi_partition_batch(image_store):
    """A single write call spanning multiple months should route correctly."""
    records = [
        make_image(image_id="img_jan", month=1),
        make_image(image_id="img_feb", month=2),
        make_image(image_id="img_mar", month=3),
    ]
    image_store.write("images", records)
    assert image_store.count("images") == 3
    assert image_store.count("images", filters={"month": 1}) == 1
    assert image_store.count("images", filters={"month": 2}) == 1


def test_write_arrow_table_input(image_store):
    record = make_image()
    schema = pa.schema([
        pa.field("image_id", pa.utf8()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("instrument", pa.utf8()),
        pa.field("year", pa.int64()),
        pa.field("month", pa.int64()),
    ])
    table = pa.Table.from_pylist([record], schema=schema)
    image_store.write("images", table)
    assert image_store.count("images") == 1


def test_write_raises_without_create_table(store):
    with pytest.raises(RuntimeError, match="not registered"):
        store.write("images", [make_image()])


def test_write_raises_on_missing_required_column(image_store):
    bad_record = {"image_id": "x", "instrument": "IFCB107", "year": 2024, "month": 1}
    # Missing 'timestamp'
    with pytest.raises(ValueError):
        image_store.write("images", [bad_record])


def test_write_raises_on_missing_partition_key(image_store):
    bad_record = {
        "image_id": "x",
        "timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "instrument": "IFCB107",
        "year": 2024,
        # missing 'month'
    }
    with pytest.raises(ValueError):
        image_store.write("images", [bad_record])


def test_write_unpartitioned_table(store):
    """Write to a table with no partition_by."""
    store.create_table("notes", RecordWithOptional, partition_by=None)
    store.write("notes", [
        {"image_id": "a", "instrument": "X", "year": 2024, "month": 1},
        {"image_id": "b", "instrument": "X", "year": 2024, "month": 2},
    ])
    assert store.count("notes") == 2


def test_write_unpartitioned_appends(store):
    store.create_table("notes", RecordWithOptional, partition_by=None)
    store.write("notes", [{"image_id": "a", "instrument": "X", "year": 2024, "month": 1}])
    store.write("notes", [{"image_id": "b", "instrument": "X", "year": 2024, "month": 2}])
    assert store.count("notes") == 2


def test_write_with_optional_column(store):
    store.create_table("notes", RecordWithOptional, partition_by=["instrument", "year", "month"])
    store.write("notes", [
        {"image_id": "a", "instrument": "X", "year": 2024, "month": 1, "notes": "hi"},
        {"image_id": "b", "instrument": "X", "year": 2024, "month": 1},
    ])
    assert store.count("notes") == 2
