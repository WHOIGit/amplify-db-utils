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


class ImageRecordWithEmbedding(BaseModel):
    """V1 + a nullable list[float] column (e.g. an embedding vector)."""
    image_id: str
    instrument: str
    year: int
    month: int
    embedding: Optional[list[float]] = None


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


# ---------------------------------------------------------------------------
# list[T] columns across the full db lifecycle
# ---------------------------------------------------------------------------


def test_list_column_persists_across_instances(tmp_path):
    """A list[T] column survives create → write → reopen → read.

    Regression: before the registry round-trip fix, opening a fresh store on a
    directory whose registry held a ``list<…>`` type crashed in
    ``SchemaRegistry.load()`` with "Cannot deserialize PyArrow type".
    """
    from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

    config = DuckDBParquetConfig(root=str(tmp_path))

    store1 = DuckDBParquetStore(config)
    store1.create_table("t", ImageRecordWithEmbedding, partition_by=["instrument", "year", "month"])
    store1.write("t", [{
        "image_id": "a",
        "instrument": "IFCB107",
        "year": 2024,
        "month": 1,
        "embedding": [0.1, 0.2, 0.3],
    }])

    # Fresh instance on the same root must load the list type without crashing.
    store2 = DuckDBParquetStore(config)
    assert store2.count("t") == 1
    rows = list(store2.read("t", filters={"instrument": "IFCB107", "year": 2024, "month": 1}))
    assert len(rows) == 1
    assert rows[0]["embedding"] == [0.1, 0.2, 0.3]


def test_list_column_idempotent_after_reload(tmp_path):
    """Re-registering a list[T] schema after reload from disk is a no-op."""
    from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

    config = DuckDBParquetConfig(root=str(tmp_path))

    store1 = DuckDBParquetStore(config)
    store1.create_table("t", ImageRecordWithEmbedding, partition_by=["instrument", "year", "month"])

    store2 = DuckDBParquetStore(config)
    # Round-tripped schema must compare equal, so this should not raise.
    store2.create_table("t", ImageRecordWithEmbedding, partition_by=["instrument", "year", "month"])


# ---------------------------------------------------------------------------
# SchemaRegistry serialization round-trip (unit level)
# ---------------------------------------------------------------------------


def test_registry_roundtrips_list_type(tmp_path):
    import pyarrow as pa
    import pyarrow.fs as pa_fs

    from amplify_db_utils.registry import SchemaRegistry

    schema = pa.schema([
        pa.field("id", pa.string(), nullable=False),
        pa.field("vec", pa.list_(pa.float32()), nullable=True),
    ])

    registry = SchemaRegistry()
    registry.register("t", schema, partition_by=None)

    fs = pa_fs.LocalFileSystem()
    registry.save(fs, str(tmp_path))
    loaded = SchemaRegistry.load(fs, str(tmp_path))

    got, _ = loaded.get("t")
    assert got == schema
    assert got.field("vec").type == pa.list_(pa.float32())


def test_registry_roundtrips_nested_types(tmp_path):
    import pyarrow as pa
    import pyarrow.fs as pa_fs

    from amplify_db_utils.registry import SchemaRegistry

    schema = pa.schema([
        pa.field("tags", pa.large_list(pa.utf8()), nullable=True),
        pa.field("meta", pa.struct([
            pa.field("x", pa.int32()),
            pa.field("y", pa.utf8()),
        ]), nullable=True),
    ])

    registry = SchemaRegistry()
    registry.register("t", schema, partition_by=None)

    fs = pa_fs.LocalFileSystem()
    registry.save(fs, str(tmp_path))
    loaded = SchemaRegistry.load(fs, str(tmp_path))

    got, _ = loaded.get("t")
    assert got == schema


def test_registry_loads_legacy_schema_fields_only(tmp_path):
    """Registries written before schema_ipc (schema_fields only) still load.

    Back-compat: stores created by older versions wrote a scalar-only
    schema_fields form with no IPC blob; those live sidecars must keep loading.
    """
    import json

    import pyarrow as pa
    import pyarrow.fs as pa_fs

    from amplify_db_utils.registry import SchemaRegistry

    registry_dir = tmp_path / "_registry"
    registry_dir.mkdir()
    (registry_dir / "tables.json").write_text(json.dumps({
        "t": {
            "schema_fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "year", "type": "int64", "nullable": False},
                {"name": "ts", "type": "timestamp[us, tz=UTC]", "nullable": True},
            ],
            "partition_by": ["year"],
        }
    }))

    fs = pa_fs.LocalFileSystem()
    loaded = SchemaRegistry.load(fs, str(tmp_path))

    got, partition_by = loaded.get("t")
    assert got.field("id").type == pa.utf8()
    assert got.field("year").type == pa.int64()
    assert got.field("ts").type == pa.timestamp("us", tz="UTC")
    assert partition_by == ["year"]
