"""Tests for column projection on read() and bulk_read()."""

from __future__ import annotations

from typing import Optional

import pyarrow as pa
import pytest
from pydantic import BaseModel

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
# Projected columns and their order
# ---------------------------------------------------------------------------


def test_bulk_read_projects_requested_columns(store):
    _setup_images(store)
    result = store.bulk_read("images", columns=["image_id", "timestamp"])
    assert result.schema.names == ["image_id", "timestamp"]
    assert len(result) == 4


def test_bulk_read_preserves_caller_column_order(store):
    _setup_images(store)
    result = store.bulk_read("images", columns=["month", "image_id", "instrument"])
    assert result.schema.names == ["month", "image_id", "instrument"]


def test_bulk_read_single_column(store):
    _setup_images(store, [make_image("only_one")])
    result = store.bulk_read("images", columns=["image_id"])
    assert result.schema.names == ["image_id"]
    assert result.column("image_id").to_pylist() == ["only_one"]


def test_bulk_read_projected_types_match_schema(store):
    _setup_images(store)
    schema = store.get_schema("images")
    result = store.bulk_read("images", columns=["year", "image_id"])
    assert result.schema.field("year").type == schema.field("year").type
    assert result.schema.field("image_id").type == schema.field("image_id").type


# ---------------------------------------------------------------------------
# Backward compatibility: columns=None
# ---------------------------------------------------------------------------


def test_bulk_read_columns_none_returns_all_columns(store):
    _setup_images(store)
    default = store.bulk_read("images")
    explicit = store.bulk_read("images", columns=None)
    assert default.schema.names == explicit.schema.names
    assert set(default.schema.names) == set(ImageRecord.model_fields)
    assert len(default) == 4


def test_read_columns_none_returns_all_columns(store):
    _setup_images(store, [make_image()])
    rows = list(store.read("images", columns=None))
    assert set(rows[0]) == set(ImageRecord.model_fields)


# ---------------------------------------------------------------------------
# Filtering on unprojected columns
# ---------------------------------------------------------------------------


def test_filter_column_need_not_be_projected(store):
    _setup_images(store)
    result = store.bulk_read(
        "images",
        filters={"instrument": "IFCB107", "month": 1},
        columns=["image_id"],
    )
    assert result.schema.names == ["image_id"]
    assert sorted(result.column("image_id").to_pylist()) == ["img_jan_1", "img_jan_2"]


def test_range_filter_on_unprojected_column(store):
    _setup_images(store)
    result = store.bulk_read("images", filters={"month": {"gte": 2}}, columns=["image_id"])
    assert result.column("image_id").to_pylist() == ["img_feb_1"]


# ---------------------------------------------------------------------------
# Partition columns
# ---------------------------------------------------------------------------


def test_project_partition_column_alongside_data_column(store):
    _setup_images(store)
    result = store.bulk_read("images", columns=["image_id", "year"])
    assert result.schema.names == ["image_id", "year"]
    assert set(result.column("year").to_pylist()) == {2024}


def test_project_only_partition_columns(store):
    """Hive partition values are synthesized from the directory path, not stored
    in the Parquet files — projecting only these must still work."""
    _setup_images(store)
    result = store.bulk_read("images", columns=["instrument", "year", "month"])
    assert result.schema.names == ["instrument", "year", "month"]
    assert len(result) == 4
    combos = sorted(
        zip(
            result.column("instrument").to_pylist(),
            result.column("year").to_pylist(),
            result.column("month").to_pylist(),
        )
    )
    assert combos == [
        ("IFCB107", 2024, 1),
        ("IFCB107", 2024, 1),
        ("IFCB107", 2024, 2),
        ("IFCB200", 2024, 1),
    ]


def test_project_single_partition_column_with_pruning_filter(store):
    _setup_images(store)
    result = store.bulk_read(
        "images",
        filters={"instrument": "IFCB107", "year": 2024, "month": 1},
        columns=["month"],
    )
    assert result.schema.names == ["month"]
    assert result.column("month").to_pylist() == [1, 1]


def test_read_only_partition_columns(store):
    _setup_images(store)
    rows = list(store.read("images", columns=["instrument"]))
    assert all(set(r) == {"instrument"} for r in rows)
    assert sorted(r["instrument"] for r in rows) == [
        "IFCB107",
        "IFCB107",
        "IFCB107",
        "IFCB200",
    ]


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_unknown_column_raises_value_error(store):
    _setup_images(store)
    with pytest.raises(ValueError, match="no_such_column"):
        store.bulk_read("images", columns=["image_id", "no_such_column"])


def test_unknown_column_message_lists_valid_columns(store):
    _setup_images(store)
    with pytest.raises(ValueError) as exc:
        store.bulk_read("images", columns=["nope"])
    message = str(exc.value)
    assert "nope" in message
    for name in ImageRecord.model_fields:
        assert name in message


def test_unknown_column_raises_before_any_query(store, monkeypatch):
    """Validation must happen before IO — no cursor is ever opened."""
    _setup_images(store)

    def fail(*args, **kwargs):
        raise AssertionError("a query was executed despite an invalid projection")

    monkeypatch.setattr(store, "_cursor", fail)
    with pytest.raises(ValueError, match="typo_id"):
        store.bulk_read("images", columns=["typo_id"])


def test_read_unknown_column_raises_at_call_time(store, monkeypatch):
    """read() is a generator factory; the error must not wait for iteration."""
    _setup_images(store)

    def fail(*args, **kwargs):
        raise AssertionError("a query was executed despite an invalid projection")

    monkeypatch.setattr(store, "_cursor", fail)
    with pytest.raises(ValueError, match="typo_id"):
        store.read("images", columns=["typo_id"])


def test_empty_columns_list_raises(store):
    _setup_images(store)
    with pytest.raises(ValueError, match="ambiguous"):
        store.bulk_read("images", columns=[])


def test_read_empty_columns_list_raises(store):
    _setup_images(store)
    with pytest.raises(ValueError, match="ambiguous"):
        store.read("images", columns=[])


def test_duplicate_columns_raise(store):
    _setup_images(store)
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        store.bulk_read("images", columns=["image_id", "year", "image_id"])


# ---------------------------------------------------------------------------
# Empty table (no Parquet files written yet)
# ---------------------------------------------------------------------------


def test_empty_table_projection_returns_projected_schema(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    result = store.bulk_read("images", columns=["image_id", "timestamp"])
    assert isinstance(result, pa.Table)
    assert len(result) == 0
    assert result.schema.names == ["image_id", "timestamp"]


def test_empty_table_projection_preserves_field_types(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    registered = store.get_schema("images")
    result = store.bulk_read("images", columns=["year", "timestamp"])
    assert result.schema.field("year").type == registered.field("year").type
    assert result.schema.field("timestamp").type == registered.field("timestamp").type


def test_empty_table_projection_matches_populated_projection(store):
    """The motivating case: an empty projected read lines up with a populated one.

    Field names, order, and types match. Nullability differs — the empty path
    carries the registered schema's flags while DuckDB reports every column as
    nullable — which is pre-existing behavior on the ``columns=None`` path too,
    so callers concatenating results must promote nullability either way.
    """
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    empty = store.bulk_read("images", columns=["image_id", "year"])
    store.write("images", [make_image("img_1")])
    populated = store.bulk_read("images", columns=["image_id", "year"])

    assert empty.schema.names == populated.schema.names
    assert empty.schema.types == populated.schema.types

    combined = pa.concat_tables([empty, populated], promote_options="permissive")
    assert combined.schema.names == ["image_id", "year"]
    assert len(combined) == 1


def test_empty_table_columns_none_unchanged(store):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    result = store.bulk_read("images")
    assert result.schema.names == list(store.get_schema("images").names)


# ---------------------------------------------------------------------------
# read() dict keys
# ---------------------------------------------------------------------------


def test_read_yields_exactly_projected_keys(store):
    _setup_images(store)
    rows = list(store.read("images", columns=["image_id", "month"]))
    assert len(rows) == 4
    for row in rows:
        assert list(row) == ["image_id", "month"]


def test_read_projection_with_filter(store):
    _setup_images(store)
    rows = list(
        store.read("images", filters={"instrument": "IFCB200"}, columns=["image_id"])
    )
    assert rows == [{"image_id": "img_other"}]


# ---------------------------------------------------------------------------
# list[T] columns
# ---------------------------------------------------------------------------


class RecordWithEmbedding(BaseModel):
    image_id: str
    instrument: str
    embeddings: Optional[list[float]] = None


def test_project_list_column_not_json_encoded(store):
    store.create_table("embeds", RecordWithEmbedding, partition_by=["instrument"])
    store.write(
        "embeds",
        [
            {"image_id": "a", "instrument": "IFCB107", "embeddings": [0.1, 0.2, 0.3]},
            {"image_id": "b", "instrument": "IFCB107", "embeddings": [0.5]},
        ],
    )
    result = store.bulk_read("embeds", columns=["embeddings", "image_id"])
    assert result.schema.names == ["embeddings", "image_id"]
    assert result.schema.field("embeddings").type == pa.list_(pa.float64())
    values = dict(
        zip(result.column("image_id").to_pylist(), result.column("embeddings").to_pylist())
    )
    assert values["a"] == [0.1, 0.2, 0.3]
    assert values["b"] == [0.5]


def test_project_around_wide_list_column(store):
    """The motivating use case: read narrow columns, skip the embedding."""
    store.create_table("embeds", RecordWithEmbedding, partition_by=["instrument"])
    store.write(
        "embeds",
        [{"image_id": "a", "instrument": "IFCB107", "embeddings": [0.1] * 128}],
    )
    result = store.bulk_read("embeds", columns=["image_id"])
    assert result.schema.names == ["image_id"]


def test_read_list_column_projection(store):
    store.create_table("embeds", RecordWithEmbedding, partition_by=["instrument"])
    store.write(
        "embeds",
        [{"image_id": "a", "instrument": "IFCB107", "embeddings": [0.1, 0.2]}],
    )
    rows = list(store.read("embeds", columns=["embeddings"]))
    assert rows == [{"embeddings": [0.1, 0.2]}]


# ---------------------------------------------------------------------------
# Column names that require quoting
# ---------------------------------------------------------------------------


def _quoted_name_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field("image_id", pa.string(), nullable=False),
            pa.field("select", pa.int64(), nullable=True),
            pa.field("my column", pa.string(), nullable=True),
            pa.field("instrument", pa.string(), nullable=False),
        ]
    )


def test_project_reserved_word_and_spaced_column_names(store):
    store.create_table("weird", _quoted_name_schema(), partition_by=["instrument"])
    store.write(
        "weird",
        [{"image_id": "a", "select": 7, "my column": "hello", "instrument": "IFCB107"}],
    )
    result = store.bulk_read("weird", columns=["select", "my column"])
    assert result.schema.names == ["select", "my column"]
    assert result.column("select").to_pylist() == [7]
    assert result.column("my column").to_pylist() == ["hello"]


def test_read_reserved_word_column_name(store):
    store.create_table("weird", _quoted_name_schema(), partition_by=["instrument"])
    store.write(
        "weird",
        [{"image_id": "a", "select": 7, "my column": "hello", "instrument": "IFCB107"}],
    )
    rows = list(store.read("weird", columns=["select"]))
    assert rows == [{"select": 7}]
