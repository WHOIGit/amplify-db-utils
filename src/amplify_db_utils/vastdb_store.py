"""VAST DB implementation of ColumnarStore.

Requires: vastdb (pip install vastdb)

This backend targets the VastDB columnar store. Key differences from
DuckDB+Parquet:
  - Append-only (WORM): no in-place overwrite or delete.
  - No Hive-style partitioning; partition_by columns are regular data columns
    with predicate pushdown on reads.
  - VastDB manages its own schema; no sidecar registry file needed.
  - Writes accept PyArrow Tables directly.
  - Joins are performed client-side via DuckDB on Arrow tables.

Addressing hierarchy: VastDB tables live at bucket.schema.table. A single
VastDBStore is bound to one (bucket, schema) pair; one table per call.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator, Literal

import duckdb
import pyarrow as pa
import vastdb
from pydantic import BaseModel
from ibis import _ as ibis_

from amplify_db_utils.base import ColumnarStore, Filters
from amplify_db_utils.schema import (
    check_partition_fields,
    to_arrow_schema,
    validate_records,
)


# ---------------------------------------------------------------------------
# Filter translation: Filters dict -> VastDB ibis-style predicate
# ---------------------------------------------------------------------------

def _filters_to_predicate(filters: Filters | None):
    """Convert a Filters dict into a VastDB ibis-style predicate."""
    if not filters:
        return None

    predicates = []
    for field, value in filters.items():
        col = getattr(ibis_, field)

        if isinstance(value, dict):
            if "in" in value:
                items = value["in"]
                if not items:
                    # Empty IN -> nothing matches.
                    return col.isin([])  # or col != col, but isin([]) is clearer
                predicates.append(col.isin(items))
            else:
                if "gte" in value:
                    predicates.append(col >= value["gte"])
                if "gt" in value:
                    predicates.append(col > value["gt"])
                if "lte" in value:
                    predicates.append(col <= value["lte"])
                if "lt" in value:
                    predicates.append(col < value["lt"])
        else:
            predicates.append(col == value)

    if not predicates:
        return None

    combined = predicates[0]
    for p in predicates[1:]:
        combined = combined & p
    return combined

def _normalize_for_vastdb(schema: pa.Schema) -> pa.Schema:
        """VastDB rejects nullable=False and strips timestamp tz.
        Normalize the requested schema to match what VastDB will actually store."""
        fields = []
        for f in schema:
            t = f.type
            if pa.types.is_timestamp(t) and t.tz is not None:
                t = pa.timestamp(t.unit)
            fields.append(pa.field(f.name, t, nullable=True))
        return pa.schema(fields)
# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class VastDBConfig:
    """Configuration for a VastDB columnar store.

    VastDB tables live at bucket.schema.table. A single VastDBStore is
    bound to one (bucket, schema) pair.

    Args:
        endpoint: VastDB endpoint URL, e.g. "https://vast.whoi.edu".
        access_key: S3-compatible access key for VastDB auth.
        secret_key: S3-compatible secret key for VastDB auth.
        bucket: VastDB bucket name, e.g. "scieng-db1".
        schema: VastDB schema name, e.g. "schema1".
        add_written_at: If True, automatically add a `written_at` timestamp
            column to every table and stamp it on every write. Opt-in because
            it mutates user-declared schemas. Use with dedup_by_written_at()
            at read time for WORM deduplication. Default False.
    """

    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    schema: str
    add_written_at: bool = False


# ---------------------------------------------------------------------------
# VastDBStore
# ---------------------------------------------------------------------------

class VastDBStore(ColumnarStore):
    """Columnar store backed by VAST DB.

    Append-only. The ``overwrite`` flag on ``write()`` is not supported
    (raises NotImplementedError). For idempotent loads, set
    ``add_written_at=True`` and use ``dedup_by_written_at()`` on read.

    Partition key columns (``partition_by``) are stored as ordinary data
    columns. On writes they are no-ops. On reads they are pushed down as
    WHERE predicates for efficient scanning.
    """

    def __init__(self, config: VastDBConfig) -> None:
        self._config = config
        self._session = vastdb.connect(
            endpoint=config.endpoint,
            access=config.access_key,
            secret=config.secret_key,
        )
        # Cache: table_name -> (pa.Schema, partition_by)
        self._table_meta: dict[str, tuple[pa.Schema, list[str] | None]] = {}

    # ------------------------------------------------------------------
    # Internal: resolve bucket/schema handles inside an open transaction
    # ------------------------------------------------------------------

    def _schema_handle(self, tx):
        """Return the VastDB schema handle (bucket.schema) inside a tx.

        Assumes the schema already exists. Use _ensure_schema for creation.
        """
        return tx.bucket(self._config.bucket).schema(self._config.schema)

    def _ensure_schema(self, tx):
        """Return the VastDB schema handle, creating it if absent."""
        bucket = tx.bucket(self._config.bucket)
        existing = {s.name for s in bucket.schemas()}
        if self._config.schema not in existing:
            return bucket.create_schema(self._config.schema)
        return bucket.schema(self._config.schema)
    

    # ------------------------------------------------------------------
    # create_table
    # ------------------------------------------------------------------

    def create_table(
        self,
        table: str,
        schema: type[BaseModel] | pa.Schema,
        partition_by: list[str] | None = None,
    ) -> None:
        arrow_schema = to_arrow_schema(schema)
        arrow_schema = _normalize_for_vastdb(arrow_schema)

        # Opt-in: inject written_at column for WORM dedup support.
        if self._config.add_written_at:
            if "written_at" not in arrow_schema.names:
                arrow_schema = arrow_schema.append(
                    pa.field("written_at", pa.timestamp("us"), nullable=True)
                )

        if partition_by:
            schema_names = set(arrow_schema.names)
            missing = [f for f in partition_by if f not in schema_names]
            if missing:
                raise ValueError(
                    f"Partition key field(s) {missing!r} not in schema."
                )

        # Idempotent: check existence first, don't rely on exception type
        # (broad except Exception was masking auth/connection errors).
        with self._session.transaction() as tx:
            vast_schema = self._ensure_schema(tx)
            existing_tables = {t.name for t in vast_schema.tables()}

            if table in existing_tables:
                vast_table = vast_schema.table(table)
                # SDK exposes schema via .columns() returning a pa.Schema.
                existing_arrow = vast_table.columns()
                self._check_schema_compat(
                    table, existing_arrow, arrow_schema, partition_by
                )
                # TODO: ALTER TABLE ADD COLUMN for new nullable columns
            else:
                vast_schema.create_table(table, arrow_schema)

        self._table_meta[table] = (arrow_schema, partition_by)

    def _check_schema_compat(
        self,
        table_name: str,
        existing: pa.Schema,
        requested: pa.Schema,
        partition_by: list[str] | None,
    ) -> None:
        """Validate schema evolution rules (mirrors SchemaRegistry.register)."""
        existing_fields = {f.name: f for f in existing}
        new_fields = {f.name: f for f in requested}

        removed = set(existing_fields) - set(new_fields)
        if removed:
            raise ValueError(
                f"Cannot remove columns from '{table_name}': {sorted(removed)}"
            )

        for name in new_fields:
            if name in existing_fields:
                # Compare types only; VastDB injects metadata (VAST:column_id) we don't control.
                if existing_fields[name].type != new_fields[name].type:
                    raise ValueError(
                        f"Cannot change type of '{name}' in '{table_name}': "
                        f"{existing_fields[name].type} -> {new_fields[name].type}"
                    )

        added = set(new_fields) - set(existing_fields)
        non_nullable = [n for n in added if not new_fields[n].nullable]
        if non_nullable:
            raise ValueError(
                f"New columns {non_nullable!r} must be nullable."
            )

    # ------------------------------------------------------------------
    # write
    # ------------------------------------------------------------------

    def write(
        self,
        table: str,
        records: list[dict] | pa.Table,
        overwrite: bool = False,
    ) -> None:
        if overwrite:
            raise NotImplementedError(
                "VastDB is append-only (WORM). overwrite=True is not supported. "
                "Use add_written_at=True and deduplicate at read time via "
                "dedup_by_written_at()."
            )

        if table not in self._table_meta:
            raise RuntimeError(
                f"Table '{table}' not registered. Call create_table() first."
            )

        schema, partition_by = self._table_meta[table]
        arrow_table = validate_records(records, schema)

        if partition_by:
            check_partition_fields(arrow_table, partition_by)

        if self._config.add_written_at and "written_at" in schema.names:
            now = datetime.now(timezone.utc)
            new_arr = pa.array([now] * len(arrow_table), type=pa.timestamp("us"))
            if "written_at" in arrow_table.schema.names:
                idx = arrow_table.schema.get_field_index("written_at")
                arrow_table = arrow_table.set_column(
                    idx, pa.field("written_at", pa.timestamp("us")), new_arr
                )
            else:
                arrow_table = arrow_table.append_column(
                    pa.field("written_at", pa.timestamp("us")), new_arr
                )

        with self._session.transaction() as tx:
            vast_table = self._schema_handle(tx).table(table)
            vast_table.insert(arrow_table)

    # ------------------------------------------------------------------
    # read
    # ------------------------------------------------------------------

    def read(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> Iterator[dict]:
        arrow_table = self.bulk_read(table, filters)
        for batch in arrow_table.to_batches():
            rows = batch.to_pydict()
            n = batch.num_rows
            col_names = batch.schema.names
            for i in range(n):
                yield {col: rows[col][i] for col in col_names}

    # ------------------------------------------------------------------
    # bulk_read
    # ------------------------------------------------------------------

    def bulk_read(self, table, filters=None) -> pa.Table:
        with self._session.transaction() as tx:
            vast_table = self._schema_handle(tx).table(table)
            predicate = _filters_to_predicate(filters)  # no table arg
            reader = vast_table.select(predicate=predicate)
            result = reader.read_all()
        return result

    # ------------------------------------------------------------------
    # distinct_values
    # ------------------------------------------------------------------

    def distinct_values(
        self,
        table: str,
        fields: list[str],
        filters: Filters | None = None,
    ) -> list[dict]:
        """Distinct values via client-side DuckDB on an Arrow stream.

        Note: this is the expensive path — VastDB has no server-side
        SELECT DISTINCT. For partition discovery on large tables, consider
        maintaining a separate lightweight index table.
        """
        arrow_table = self.bulk_read(table, filters)

        conn = duckdb.connect(":memory:")
        field_list = ", ".join(f'"{f}"' for f in fields)
        rows = conn.execute(
            f"SELECT DISTINCT {field_list} FROM arrow_table"
        ).fetchall()
        conn.close()

        return [dict(zip(fields, row)) for row in rows]

    # ------------------------------------------------------------------
    # count
    # ------------------------------------------------------------------

    def count(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> int:
        arrow_table = self.bulk_read(table, filters)
        return len(arrow_table)

    # ------------------------------------------------------------------
    # join
    # ------------------------------------------------------------------

    def join(
        self,
        left: str,
        right: str,
        on: str,
        left_filters: Filters | None = None,
        right_filters: Filters | None = None,
        select: Literal["left", "right", "both"] = "both",
    ) -> Iterator[dict]:
        """Client-side join via DuckDB on Arrow tables.

        Both tables are pulled from VastDB as Arrow, then joined in an
        in-process DuckDB instance. Efficient for moderate result sizes;
        will not scale to billion-row joins.
        """
        left_arrow = self.bulk_read(left, left_filters)
        right_arrow = self.bulk_read(right, right_filters)

        if select == "left":
            select_clause = "l.*"
        elif select == "right":
            select_clause = "r.*"
        else:
            select_clause = "l.*, r.*"

        conn = duckdb.connect(":memory:")
        sql = f"""
            SELECT {select_clause}
            FROM left_arrow l
            JOIN right_arrow r ON l."{on}" = r."{on}"
        """
        cursor = conn.execute(sql)
        cols = [d[0] for d in cursor.description]
        while True:
            batch = cursor.fetchmany(1000)
            if not batch:
                break
            for row in batch:
                yield dict(zip(cols, row))
        conn.close()

    # ------------------------------------------------------------------
    # drop_table / drop_schema  (primarily for test teardown)
    # ------------------------------------------------------------------

    def drop_table(self, table: str) -> None:
        """Drop a single table. Used primarily for test teardown.

        Irreversible — do not call against production tables.
        """
        with self._session.transaction() as tx:
            vast_schema = self._schema_handle(tx)
            existing = {t.name for t in vast_schema.tables()}
            if table in existing:
                vast_schema.table(table).drop()
        self._table_meta.pop(table, None)

    def drop_schema(self) -> None:
        """Drop all tables in this store's schema, then the schema itself.

        Intended for per-test-unique-schema teardown in parametrized test
        suites. Irreversible — do not call against production schemas.
        """
        with self._session.transaction() as tx:
            bucket = tx.bucket(self._config.bucket)
            existing_schemas = {s.name for s in bucket.schemas()}
            if self._config.schema not in existing_schemas:
                self._table_meta.clear()
                return
            vast_schema = bucket.schema(self._config.schema)
            for t in list(vast_schema.tables()):
                t.drop()
            vast_schema.drop()
        self._table_meta.clear()


# ---------------------------------------------------------------------------
# Helper: WORM deduplication at read time
# ---------------------------------------------------------------------------

def dedup_by_written_at(
    arrow_table: pa.Table,
    key_columns: list[str],
) -> pa.Table:
    """Deduplicate a WORM Arrow table, keeping the latest written_at per key.

    Use this after bulk_read() when the table may contain duplicate rows
    from re-ingestion. Applies a ROW_NUMBER() window function via DuckDB.

    Args:
        arrow_table: Input table (must contain a ``written_at`` column).
        key_columns: Columns that define row identity
            (e.g., ["bin_lid", "roi_number"]).

    Returns:
        Deduplicated pa.Table.
    """
    if len(arrow_table) == 0:
        return arrow_table

    partition_expr = ", ".join(f'"{c}"' for c in key_columns)

    conn = duckdb.connect(":memory:")
    result = conn.execute(f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_expr}
                    ORDER BY written_at DESC
                ) AS _rn
            FROM arrow_table
        )
        SELECT * EXCLUDE (_rn)
        FROM ranked
        WHERE _rn = 1
    """).arrow()
    conn.close()

    if not isinstance(result, pa.Table):
        result = result.read_all()
    return result