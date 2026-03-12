"""Tests for schema evolution rules enforced by create_table()."""

from __future__ import annotations

from typing import Optional

import pytest
from pydantic import BaseModel

from tests.conftest import ImageRecord


class ImageRecordV1(BaseModel):
    image_id: str
    instrument: str
    year: int
    month: int


class ImageRecordV2AddNullable(BaseModel):
    """V1 + a new nullable column. This is allowed."""
    image_id: str
    instrument: str
    year: int
    month: int
    notes: Optional[str] = None


class ImageRecordV2AddRequired(BaseModel):
    """V1 + a new non-nullable column. This is NOT allowed."""
    image_id: str
    instrument: str
    year: int
    month: int
    notes: str  # non-nullable — forbidden


class ImageRecordRemoveColumn(BaseModel):
    """V1 minus a column. NOT allowed."""
    image_id: str
    instrument: str
    year: int
    # month removed


class ImageRecordChangeType(BaseModel):
    """V1 with year changed to str. NOT allowed."""
    image_id: str
    instrument: str
    year: str  # was int — forbidden
    month: int


# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------


def test_create_table_idempotent(store):
    """Calling create_table with the same schema twice is a no-op."""
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    # Should not raise


# ---------------------------------------------------------------------------
# Allowed evolution
# ---------------------------------------------------------------------------


def test_add_nullable_column_allowed(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    store.create_table("t", ImageRecordV2AddNullable, partition_by=["instrument", "year", "month"])
    # Should not raise


def test_add_nullable_column_write_old_data(store):
    """After adding a nullable column, old records (without the column) still write."""
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    store.write("t", [{"image_id": "a", "instrument": "IFCB107", "year": 2024, "month": 1}])

    store.create_table("t", ImageRecordV2AddNullable, partition_by=["instrument", "year", "month"])
    # Old-style records (no 'notes') should still write
    store.write("t", [{"image_id": "b", "instrument": "IFCB107", "year": 2024, "month": 1}])
    assert store.count("t") == 2


# ---------------------------------------------------------------------------
# Forbidden evolution
# ---------------------------------------------------------------------------


def test_add_non_nullable_column_raises(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    with pytest.raises(ValueError, match="nullable"):
        store.create_table("t", ImageRecordV2AddRequired, partition_by=["instrument", "year", "month"])


def test_remove_column_raises(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    with pytest.raises(ValueError):
        # Either "Cannot remove" (from registry) or "not present in schema" (partition key check)
        store.create_table("t", ImageRecordRemoveColumn, partition_by=["instrument", "year", "month"])


def test_change_type_raises(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    with pytest.raises(ValueError, match="[Cc]annot change type"):
        store.create_table("t", ImageRecordChangeType, partition_by=["instrument", "year", "month"])


def test_change_partition_by_raises(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    with pytest.raises(ValueError, match="partition_by"):
        store.create_table("t", ImageRecordV1, partition_by=["instrument", "year"])


def test_change_partition_by_to_none_raises(store):
    store.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    with pytest.raises(ValueError, match="partition_by"):
        store.create_table("t", ImageRecordV1, partition_by=None)


# ---------------------------------------------------------------------------
# Registry persistence across instances
# ---------------------------------------------------------------------------


def test_registry_persists_across_instances(tmp_path):
    """Schema registered in one store instance is loaded by a new instance."""
    from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

    config = DuckDBParquetConfig(root=str(tmp_path))

    store1 = DuckDBParquetStore(config)
    store1.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])
    store1.write("t", [{"image_id": "a", "instrument": "IFCB107", "year": 2024, "month": 1}])

    # New store instance pointing at same root
    store2 = DuckDBParquetStore(config)
    assert store2.count("t") == 1
    # Idempotent re-registration should work
    store2.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])


def test_registry_prevents_breaking_change_after_reload(tmp_path):
    """Breaking changes are caught even after reloading from disk."""
    from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

    config = DuckDBParquetConfig(root=str(tmp_path))

    store1 = DuckDBParquetStore(config)
    store1.create_table("t", ImageRecordV1, partition_by=["instrument", "year", "month"])

    store2 = DuckDBParquetStore(config)
    with pytest.raises(ValueError):
        store2.create_table("t", ImageRecordRemoveColumn, partition_by=["instrument", "year", "month"])
