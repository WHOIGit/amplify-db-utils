"""Schema utilities: Pydantic→PyArrow conversion and record validation."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, get_args, get_origin

import pyarrow as pa
from pydantic import BaseModel

# Mapping from Python scalar types to PyArrow types.
# dict fields are stored as JSON blobs (large_utf8).
_PYTHON_TO_ARROW: dict[type, pa.DataType] = {
    str: pa.utf8(),
    int: pa.int64(),
    float: pa.float64(),
    bool: pa.bool_(),
    datetime: pa.timestamp("us", tz="UTC"),
    dict: pa.large_utf8(),
}


def _annotation_to_arrow(annotation: Any) -> tuple[pa.DataType, bool]:
    """Convert a Python type annotation to ``(arrow_type, nullable)``.

    Handles ``Optional[T]`` (``Union[T, None]``) and the ``T | None`` pipe
    syntax (Python 3.10+). Returns ``nullable=True`` for Optional types.
    """
    args = get_args(annotation)

    if args:
        # Generic/Union type — only Optional[T] (Union[T, None]) is supported
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            arrow_type, _ = _annotation_to_arrow(non_none[0])
            return arrow_type, True
        raise TypeError(
            f"Unsupported generic type annotation: {annotation!r}. "
            f"Only Optional[T] (Union[T, None] / T | None) is supported."
        )

    if annotation in _PYTHON_TO_ARROW:
        return _PYTHON_TO_ARROW[annotation], False

    raise TypeError(
        f"Unsupported field type {annotation!r}. "
        f"Supported types: {list(_PYTHON_TO_ARROW.keys())}"
    )


def to_arrow_schema(schema: type[BaseModel] | pa.Schema) -> pa.Schema:
    """Convert a Pydantic model class or PyArrow schema to a ``pa.Schema``.

    For Pydantic models, fields with ``Optional[T]`` annotations or fields
    that have a default value map to nullable Arrow fields.

    Args:
        schema: A Pydantic model class or an existing ``pa.Schema``.

    Returns:
        ``pa.Schema`` with field names, types, and nullability.

    Raises:
        TypeError: For unsupported field type annotations.
    """
    if isinstance(schema, pa.Schema):
        return schema

    if not (isinstance(schema, type) and issubclass(schema, BaseModel)):
        raise TypeError(f"Expected a Pydantic model class or pa.Schema, got {type(schema)!r}")

    fields: list[pa.Field] = []
    for name, field_info in schema.model_fields.items():
        annotation = field_info.annotation
        arrow_type, nullable = _annotation_to_arrow(annotation)
        # Fields with a default (including None) are nullable
        if not field_info.is_required():
            nullable = True
        fields.append(pa.field(name, arrow_type, nullable=nullable))

    return pa.schema(fields)


def _preprocess_list_records(records: list[dict], schema: pa.Schema) -> list[dict]:
    """Serialize dict/list values to JSON strings for utf8/large_utf8 columns."""
    json_cols = {
        f.name for f in schema if f.type in (pa.large_utf8(), pa.utf8())
    }
    if not json_cols:
        return records

    result = []
    for record in records:
        row = {}
        for k, v in record.items():
            if k in json_cols and isinstance(v, (dict, list)):
                row[k] = json.dumps(v)
            else:
                row[k] = v
        result.append(row)
    return result


def validate_records(
    records: list[dict] | pa.Table,
    schema: pa.Schema,
) -> pa.Table:
    """Convert and validate records against a registered schema.

    Unknown columns (not in schema) are silently dropped. Missing nullable
    columns are filled with nulls. Missing non-nullable columns raise
    ``ValueError``.

    Args:
        records: Input records as ``list[dict]``, ``pa.Table``, or
            ``pd.DataFrame`` (pandas must be installed).
        schema: Target schema to validate against.

    Returns:
        ``pa.Table`` conforming to the schema.

    Raises:
        ValueError: If required columns are missing or types are incompatible.
        ImportError: If pandas is not installed and a DataFrame is passed.
    """
    if isinstance(records, pa.Table):
        table = records
    elif isinstance(records, list):
        processed = _preprocess_list_records(records, schema)
        try:
            table = pa.Table.from_pylist(processed)
        except Exception as e:
            raise ValueError(f"Failed to convert records to Arrow table: {e}") from e
    else:
        # Try pandas DataFrame
        try:
            import pandas as pd  # noqa: F401
        except ImportError:
            raise ImportError(
                "pandas is required for DataFrame input. "
                "Install with: pip install 'amplify-db-utils[pandas]'"
            )
        try:
            import pandas as pd
            if isinstance(records, pd.DataFrame):
                table = pa.Table.from_pandas(records, preserve_index=False)
            else:
                raise TypeError(f"Expected list[dict], pa.Table, or pd.DataFrame; got {type(records)!r}")
        except Exception as e:
            raise ValueError(f"Failed to convert DataFrame to Arrow table: {e}") from e

    # Check for missing required columns
    present = set(table.schema.names)
    missing_required = [
        f.name for f in schema if f.name not in present and not f.nullable
    ]
    if missing_required:
        raise ValueError(f"Missing required columns: {missing_required}")

    # Add missing nullable columns as null arrays
    for field in schema:
        if field.name not in present:
            table = table.append_column(
                field,
                pa.array([None] * len(table), type=field.type),
            )

    # Select only columns present in schema (drop extras) and reorder
    schema_names = [f.name for f in schema if f.name in set(table.schema.names)]
    # Some schema columns may have been added above — select all of them
    table = table.select(schema.names)

    # Cast to target schema types
    try:
        table = table.cast(schema)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as e:
        raise ValueError(f"Schema validation failed: {e}") from e

    return table


def check_partition_fields(table: pa.Table, partition_by: list[str]) -> None:
    """Verify partition key fields are present and contain no null values.

    Args:
        table: Arrow table to check.
        partition_by: Required partition key column names.

    Raises:
        ValueError: If any partition key field is missing or contains nulls.
    """
    for field in partition_by:
        if field not in table.schema.names:
            raise ValueError(
                f"Partition key field '{field}' is missing from records. "
                f"All partition key fields must be populated before writing: {partition_by}"
            )
        col = table.column(field)
        null_count = col.null_count
        if null_count > 0:
            raise ValueError(
                f"Partition key field '{field}' contains {null_count} null value(s). "
                f"Null values in partition keys produce malformed Hive paths."
            )
