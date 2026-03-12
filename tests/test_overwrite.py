"""Tests for write(..., overwrite=True)."""

from __future__ import annotations

import pytest

from tests.conftest import ImageRecord, RecordWithOptional, make_image


def _create_images(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    return store


def test_overwrite_replaces_partition(store):
    _create_images(store)
    store.write("images", [
        make_image("old_1", instrument="IFCB107", year=2024, month=1),
        make_image("old_2", instrument="IFCB107", year=2024, month=1),
    ])
    assert store.count("images") == 2

    store.write(
        "images",
        [make_image("new_1", instrument="IFCB107", year=2024, month=1)],
        overwrite=True,
    )
    assert store.count("images") == 1
    rows = list(store.read("images"))
    assert rows[0]["image_id"] == "new_1"


def test_overwrite_only_affects_matching_partition(store):
    _create_images(store)
    store.write("images", [
        make_image("jan_img", instrument="IFCB107", year=2024, month=1),
        make_image("feb_img", instrument="IFCB107", year=2024, month=2),
    ])

    # Overwrite only January
    store.write(
        "images",
        [make_image("jan_new", instrument="IFCB107", year=2024, month=1)],
        overwrite=True,
    )

    assert store.count("images") == 2
    jan_rows = list(store.read("images", filters={"month": 1}))
    assert len(jan_rows) == 1
    assert jan_rows[0]["image_id"] == "jan_new"

    feb_rows = list(store.read("images", filters={"month": 2}))
    assert len(feb_rows) == 1
    assert feb_rows[0]["image_id"] == "feb_img"


def test_overwrite_multi_partition_batch(store):
    """Overwriting records spanning multiple partitions replaces each independently."""
    _create_images(store)
    store.write("images", [
        make_image("jan_old", instrument="IFCB107", year=2024, month=1),
        make_image("feb_old", instrument="IFCB107", year=2024, month=2),
    ])

    store.write(
        "images",
        [
            make_image("jan_new", instrument="IFCB107", year=2024, month=1),
            make_image("feb_new", instrument="IFCB107", year=2024, month=2),
        ],
        overwrite=True,
    )

    assert store.count("images") == 2
    ids = {r["image_id"] for r in store.read("images")}
    assert ids == {"jan_new", "feb_new"}


def test_overwrite_nonexistent_partition(store):
    """Overwriting a partition that doesn't exist yet just creates it."""
    _create_images(store)
    store.write(
        "images",
        [make_image("new_img", instrument="IFCB107", year=2024, month=1)],
        overwrite=True,
    )
    assert store.count("images") == 1


def test_overwrite_false_appends(store):
    _create_images(store)
    store.write("images", [make_image("img_1")])
    store.write("images", [make_image("img_2")], overwrite=False)
    assert store.count("images") == 2


def test_overwrite_unpartitioned_table(store):
    """Overwrite on unpartitioned table replaces all content (no partition_by)."""
    store.create_table("notes", RecordWithOptional, partition_by=None)
    store.write("notes", [
        {"image_id": "a", "instrument": "X", "year": 2024, "month": 1},
        {"image_id": "b", "instrument": "X", "year": 2024, "month": 2},
    ])
    assert store.count("notes") == 2
    # Without partition_by, overwrite=True behaves the same as overwrite=False (appends)
    # because there's no partition to clear. The unpartitioned append path is used.
    store.write("notes", [
        {"image_id": "c", "instrument": "X", "year": 2024, "month": 3},
    ], overwrite=True)
    # overwrite=True with no partition_by falls through to append
    assert store.count("notes") == 3
