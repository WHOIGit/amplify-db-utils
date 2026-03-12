"""Tests for schema conversion and validation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import pytest
from pydantic import BaseModel

from amplify_db_utils.schema import (
    check_partition_fields,
    to_arrow_schema,
    validate_records,
)


# ---------------------------------------------------------------------------
# to_arrow_schema
# ---------------------------------------------------------------------------


class AllTypes(BaseModel):
    s: str
    i: int
    f: float
    b: bool
    dt: datetime
    d: dict


class WithOptional(BaseModel):
    required_field: str
    optional_field: Optional[str] = None
    optional_int: Optional[int] = None


def test_to_arrow_schema_passthrough():
    schema = pa.schema([pa.field("x", pa.int64())])
    assert to_arrow_schema(schema) is schema


def test_to_arrow_schema_str():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("s").type == pa.utf8()
    assert not schema.field("s").nullable


def test_to_arrow_schema_int():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("i").type == pa.int64()


def test_to_arrow_schema_float():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("f").type == pa.float64()


def test_to_arrow_schema_bool():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("b").type == pa.bool_()


def test_to_arrow_schema_datetime():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("dt").type == pa.timestamp("us", tz="UTC")


def test_to_arrow_schema_dict_is_json_blob():
    schema = to_arrow_schema(AllTypes)
    assert schema.field("d").type == pa.large_utf8()


def test_to_arrow_schema_optional_is_nullable():
    schema = to_arrow_schema(WithOptional)
    assert not schema.field("required_field").nullable
    assert schema.field("optional_field").nullable
    assert schema.field("optional_int").nullable


def test_to_arrow_schema_unsupported_type():
    class Bad(BaseModel):
        x: list  # not supported

    with pytest.raises(TypeError, match="Unsupported"):
        to_arrow_schema(Bad)


def test_to_arrow_schema_not_a_model():
    with pytest.raises(TypeError):
        to_arrow_schema("not a model")


# ---------------------------------------------------------------------------
# validate_records — list[dict] input
# ---------------------------------------------------------------------------


def test_validate_list_dict_basic():
    class Simple(BaseModel):
        name: str
        value: int

    schema = to_arrow_schema(Simple)
    records = [{"name": "foo", "value": 1}, {"name": "bar", "value": 2}]
    table = validate_records(records, schema)
    assert isinstance(table, pa.Table)
    assert len(table) == 2
    assert table.schema.field("name").type == pa.utf8()


def test_validate_drops_extra_columns():
    class Simple(BaseModel):
        name: str

    schema = to_arrow_schema(Simple)
    records = [{"name": "foo", "extra": "ignored"}]
    table = validate_records(records, schema)
    assert "extra" not in table.schema.names


def test_validate_fills_nullable_missing_column():
    schema = to_arrow_schema(WithOptional)
    records = [{"required_field": "hello"}]
    table = validate_records(records, schema)
    assert table.column("optional_field")[0].as_py() is None


def test_validate_raises_on_missing_required():
    class Simple(BaseModel):
        name: str
        value: int

    schema = to_arrow_schema(Simple)
    records = [{"name": "foo"}]  # missing required 'value'
    with pytest.raises(ValueError, match="Missing required"):
        validate_records(records, schema)


def test_validate_pyarrow_table_input():
    schema = pa.schema([pa.field("x", pa.int64()), pa.field("y", pa.utf8())])
    table = pa.table({"x": [1, 2], "y": ["a", "b"]})
    result = validate_records(table, schema)
    assert len(result) == 2


def test_validate_dict_serialized_to_json():
    class WithData(BaseModel):
        image_id: str
        data: dict

    schema = to_arrow_schema(WithData)
    records = [{"image_id": "abc", "data": {"key": "value"}}]
    table = validate_records(records, schema)
    # data column should be a JSON string
    import json
    val = table.column("data")[0].as_py()
    assert json.loads(val) == {"key": "value"}


# ---------------------------------------------------------------------------
# check_partition_fields
# ---------------------------------------------------------------------------


def test_check_partition_fields_ok():
    table = pa.table({"instrument": ["IFCB107"], "year": [2024], "month": [1]})
    check_partition_fields(table, ["instrument", "year", "month"])  # no error


def test_check_partition_fields_missing():
    table = pa.table({"instrument": ["IFCB107"], "year": [2024]})
    with pytest.raises(ValueError, match="month"):
        check_partition_fields(table, ["instrument", "year", "month"])


def test_check_partition_fields_null():
    table = pa.table({"instrument": [None], "year": [2024], "month": [1]})
    with pytest.raises(ValueError, match="null"):
        check_partition_fields(table, ["instrument", "year", "month"])
