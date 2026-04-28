"""Public types and ColumnarStore ABC."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Literal

import pyarrow as pa
from pydantic import BaseModel

if TYPE_CHECKING:
    import pandas as pd

# Filter value types:
#   Equality:      {"field": "value"}  or  {"field": 42}
#   Range:         {"field": {"gte": x, "lt": y}}  (any subset of gte/gt/lte/lt)
#   Set member:    {"field": {"in": [a, b, c]}}
FilterValue = str | int | float | bool | dict
Filters = dict[str, FilterValue]


class ColumnarStore(ABC):
    """Abstract base class for a columnar database store.

    Provides a minimal, purpose-built API for append-only writes and
    analytical reads against tabular data. Implementations include
    DuckDB+Parquet (portable, serverless) and VAST DB (production scale).

    Tables must be registered via ``create_table()`` before use. Schemas
    are defined as Pydantic models or PyArrow schemas. Partition key fields
    are stored as ordinary data columns — no separate partition routing needed.
    """

    @abstractmethod
    def create_table(
        self,
        table: str,
        schema: type[BaseModel] | pa.Schema,
        partition_by: list[str] | None = None,
    ) -> None:
        """Register a schema and partition key structure for a table.

        Idempotent — safe to call at service startup. On subsequent calls,
        performs a compatibility check:
        - Adding a nullable column is allowed.
        - Removing or renaming a column, changing a type, or changing
          ``partition_by`` raises ``ValueError``.

        Args:
            table: Table name.
            schema: Row schema as a Pydantic model class or PyArrow schema.
                Partition key fields must be included as ordinary columns.
            partition_by: Ordered list of column names that define the partition
                structure, e.g. ``["instrument", "year", "month"]``.
                For DuckDB+Parquet, determines Hive directory path structure.
                Immutable once set.
        """

    @abstractmethod
    def get_schema(
        self,
        table: str,
    ) -> pa.Schema:
        """Return the registered schema for a table.

        Args:
            table: Table name.

        Returns:
            Registered schema for the table.

        Raises:
            KeyError: If the table has not been registered.
        """

    @abstractmethod
    def write(
        self,
        table: str,
        records: list[dict] | pa.Table | "pd.DataFrame",
        overwrite: bool = False,
    ) -> None:
        """Bulk write records to a table.

        Validates that records conform to the registered row schema.
        Partition key fields must be present as data columns in each record.

        Args:
            table: Table name (must have been registered via ``create_table``).
            records: Records to write. Accepts ``list[dict]``, PyArrow ``Table``,
                or pandas ``DataFrame``.
            overwrite: If False (default), append records to existing data.
                If True, for each distinct partition key combination present in
                the records, replace all existing rows in that partition.

        Raises:
            RuntimeError: If ``create_table`` was never called for this table.
            ValueError: On schema mismatch or missing partition key fields.
        """

    @abstractmethod
    def read(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> Iterator[dict]:
        """Filtered row iteration.

        Args:
            table: Table name.
            filters: Optional filter dict. See ``Filters`` type for syntax.
                Supports equality, range (gte/gt/lte/lt), and set membership (in).
                Include partition key fields for efficient partition pruning.

        Yields:
            Row dicts.
        """

    @abstractmethod
    def bulk_read(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> pa.Table:
        """Read rows matching filters as a PyArrow Table.

        Preferred for bulk retrieval (e.g., all features for a sample/run).
        Zero-copy; call ``.to_pandas()`` on the result if pandas is needed.

        Args:
            table: Table name.
            filters: Optional filter dict. Provide partition key fields
                to ensure efficient execution.

        Returns:
            PyArrow ``Table`` containing matching rows.
        """

    @abstractmethod
    def distinct_values(
        self,
        table: str,
        fields: list[str],
        filters: Filters | None = None,
    ) -> list[dict]:
        """Return distinct combinations of the specified field values.

        Use to discover what partitions or groupings exist.

        Args:
            table: Table name.
            fields: Field names to return distinct combinations for.
            filters: Optional filter dict to narrow results.

        Returns:
            List of dicts, each representing one distinct combination.
        """

    @abstractmethod
    def count(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> int:
        """Row count without materializing results.

        Args:
            table: Table name.
            filters: Optional filter dict.

        Returns:
            Number of matching rows.
        """

    @abstractmethod
    def join(
        self,
        left: str,
        right: str,
        on: str,
        left_filters: Filters | None = None,
        right_filters: Filters | None = None,
        select: Literal["left", "right", "both"] = "both",
    ) -> Iterator[dict]:
        """Join two tables on a shared key column and return filtered rows.

        Both tables must exist in the same ``ColumnarStore`` instance.
        Use ``select="right"`` when the left table is a filter index and the
        right table is the payload (e.g., ``geolocation_index JOIN images``).

        Note: ``select="both"`` may produce unexpected results when the tables
        share column names other than the join key. Use ``select="left"`` or
        ``select="right"`` to avoid ambiguity.

        Args:
            left: Left table name.
            right: Right table name.
            on: Column name to join on (must exist in both tables).
            left_filters: Optional filters applied to the left table.
            right_filters: Optional filters applied to the right table.
            select: Which table's columns to return. Default ``"both"``.

        Yields:
            Row dicts.
        """
