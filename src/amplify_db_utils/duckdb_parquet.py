"""DuckDB + Parquet implementation of ColumnarStore."""

from __future__ import annotations

import uuid
from typing import Iterator, Literal

import duckdb
import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
from pydantic import BaseModel

from amplify_db_utils.base import ColumnarStore, Filters
from amplify_db_utils.config import DuckDBParquetConfig
from amplify_db_utils.filters import filters_to_sql
from amplify_db_utils.registry import SchemaRegistry
from amplify_db_utils.schema import check_partition_fields, to_arrow_schema, validate_records


def _init_filesystem(config: DuckDBParquetConfig) -> tuple[pa_fs.FileSystem, str]:
    """Return ``(filesystem, fs_root)`` for PyArrow filesystem operations.

    For S3 URLs, ``fs_root`` strips the ``s3://`` prefix since PyArrow's
    ``S3FileSystem`` uses bare ``bucket/key`` paths.
    """
    if config.root.startswith("s3://"):
        rest = config.root[5:]  # strip "s3://"
        kwargs: dict = {}
        if config.s3_endpoint:
            kwargs["endpoint_override"] = config.s3_endpoint
        if config.s3_access_key:
            kwargs["access_key"] = config.s3_access_key
        if config.s3_secret_key:
            kwargs["secret_key"] = config.s3_secret_key
        kwargs["scheme"] = "https" if config.s3_use_ssl else "http"
        return pa_fs.S3FileSystem(**kwargs), rest
    else:
        return pa_fs.LocalFileSystem(), config.root


def _parse_partition_value(val: str, arrow_type: pa.DataType) -> int | float | bool | str:
    """Parse a Hive directory segment value string to the appropriate Python type."""
    if pa.types.is_integer(arrow_type):
        return int(val)
    if pa.types.is_floating(arrow_type):
        return float(val)
    if pa.types.is_boolean(arrow_type):
        return val.lower() == "true"
    return val


class DuckDBParquetStore(ColumnarStore):
    """Columnar store backed by DuckDB + Parquet files.

    Data is written as Hive-partitioned Parquet files at ``config.root``.
    Works against a local filesystem or any S3-compatible store (VAST S3,
    MinIO, etc.) via DuckDB's built-in ``httpfs`` extension.

    DuckDB runs in-process with an in-memory catalog — the data lives in
    Parquet files, not in a DuckDB file.

    Not suitable for concurrent multi-process writes (e.g., SLURM parallel
    jobs). Use VAST DB for production-scale concurrent access.

    Args:
        config: Store configuration.
    """

    def __init__(self, config: DuckDBParquetConfig) -> None:
        self._config = config
        self._fs, self._fs_root = _init_filesystem(config)

        # In-process DuckDB — data is in Parquet, not in this connection
        self._conn = duckdb.connect(":memory:")

        if config.threads is not None:
            self._conn.execute(f"SET threads = {int(config.threads)}")

        if config.root.startswith("s3://"):
            self._conn.execute("INSTALL httpfs")
            self._conn.execute("LOAD httpfs")
            if config.s3_endpoint:
                self._conn.execute(f"SET s3_endpoint = '{config.s3_endpoint}'")
            if config.s3_access_key:
                self._conn.execute(f"SET s3_access_key = '{config.s3_access_key}'")
            if config.s3_secret_key:
                self._conn.execute(f"SET s3_secret_key = '{config.s3_secret_key}'")
            if not config.s3_use_ssl:
                self._conn.execute("SET s3_use_ssl = false")

        self._registry = SchemaRegistry.load(self._fs, self._fs_root)

    # ------------------------------------------------------------------
    # Path helpers
    # ------------------------------------------------------------------

    def _duckdb_table_root(self, table: str) -> str:
        """DuckDB-facing root for a table (used in SQL)."""
        return f"{self._config.root}/{table}"

    def _fs_table_root(self, table: str) -> str:
        """PyArrow filesystem path for a table (used for directory ops)."""
        return f"{self._fs_root}/{table}"

    def _duckdb_partition_dir(self, table: str, partition_values: dict) -> str:
        """DuckDB-facing path for a specific Hive partition directory."""
        _, partition_by = self._registry.get(table)
        if not partition_by:
            return self._duckdb_table_root(table)
        parts = "/".join(f"{k}={partition_values[k]}" for k in partition_by)
        return f"{self._config.root}/{table}/{parts}"

    def _fs_partition_dir(self, table: str, partition_values: dict) -> str:
        """PyArrow filesystem path for a specific Hive partition directory."""
        _, partition_by = self._registry.get(table)
        if not partition_by:
            return self._fs_table_root(table)
        parts = "/".join(f"{k}={partition_values[k]}" for k in partition_by)
        return f"{self._fs_root}/{table}/{parts}"

    def _parquet_glob(self, table: str, filters: Filters | None) -> str:
        """Return a glob expression for enumerating Parquet files.

        When all partition key fields are present as equality filters, generates
        a specific path for partition pruning. Otherwise uses a wide glob and
        relies on DuckDB's Hive partition pruning from the WHERE clause.
        """
        try:
            _, partition_by = self._registry.get(table)
        except KeyError:
            return f"{self._config.root}/{table}/**/*.parquet"

        if partition_by and filters:
            path = self._duckdb_table_root(table)
            for key in partition_by:
                val = filters.get(key)
                if isinstance(val, (str, int, float)) and not isinstance(val, bool):
                    path += f"/{key}={val}"
                elif isinstance(val, bool):
                    path += f"/{key}={val}"
                else:
                    # Not a simple equality filter — use wide glob
                    return f"{self._config.root}/{table}/**/*.parquet"
            return f"{path}/**/*.parquet"

        return f"{self._config.root}/{table}/**/*.parquet"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_table(
        self,
        table: str,
        schema: type[BaseModel] | pa.Schema,
        partition_by: list[str] | None = None,
    ) -> None:
        arrow_schema = to_arrow_schema(schema)

        # Validate that partition key fields exist in the schema
        if partition_by:
            schema_names = {f.name for f in arrow_schema}
            missing = [f for f in partition_by if f not in schema_names]
            if missing:
                raise ValueError(
                    f"Partition key field(s) {missing!r} are not present in the schema. "
                    f"Partition key fields must be included as ordinary columns."
                )

        changed = self._registry.register(table, arrow_schema, partition_by)
        if changed:
            self._registry.save(self._fs, self._fs_root)

    def get_table_info(
        self,
        table: str,
    ) -> tuple[pa.Schema, list[str] | None]:
        """Return the registered schema and partition_by for a table."""
        return self._registry.get(table)

    def write(
        self,
        table: str,
        records: list[dict] | pa.Table,
        overwrite: bool = False,
    ) -> None:
        if not self._registry.has_table(table):
            raise RuntimeError(
                f"Table '{table}' is not registered. Call create_table() first."
            )

        schema, partition_by = self._registry.get(table)

        # Normalize and validate input
        arrow_table = validate_records(records, schema)

        # Validate partition key fields
        if partition_by:
            check_partition_fields(arrow_table, partition_by)

        if overwrite and partition_by:
            self._overwrite_partitions(table, arrow_table, schema, partition_by)
        else:
            self._append_records(table, arrow_table, partition_by)

    def _append_records(
        self,
        table: str,
        arrow_table: pa.Table,
        partition_by: list[str] | None,
    ) -> None:
        """Append records using PyArrow write_to_dataset with UUID-based filenames.

        Uses PyArrow rather than DuckDB COPY TO so that each write call gets a
        globally unique filename — DuckDB's COPY with OVERWRITE_OR_IGNORE silently
        drops writes that collide with existing filenames in a partition directory.
        DuckDB reads these files back correctly via hive_partitioning=True.
        """
        dest = self._fs_table_root(table)
        basename = f"{uuid.uuid4().hex}-{{i}}.parquet"

        if partition_by:
            pq.write_to_dataset(
                arrow_table,
                root_path=dest,
                partition_cols=partition_by,
                filesystem=self._fs,
                basename_template=basename,
            )
        else:
            # Unpartitioned: write a single uniquely named Parquet file
            import os
            os.makedirs(dest, exist_ok=True)
            file_path = f"{dest}/{uuid.uuid4().hex}.parquet"
            pq.write_table(arrow_table, file_path)

    def _overwrite_partitions(
        self,
        table: str,
        arrow_table: pa.Table,
        schema: pa.Schema,
        partition_by: list[str],
    ) -> None:
        """Replace existing partitions then write new records."""
        # Find distinct partition key combinations in the incoming records
        partition_combos = (
            arrow_table.select(partition_by)
            .to_pydict()
        )
        n_rows = len(arrow_table)
        seen: set[tuple] = set()
        combos: list[dict] = []
        for i in range(n_rows):
            key = tuple(partition_combos[k][i] for k in partition_by)
            if key not in seen:
                seen.add(key)
                combos.append({k: partition_combos[k][i] for k in partition_by})

        # Delete each affected partition directory
        for combo in combos:
            fs_dir = self._fs_partition_dir(table, combo)
            try:
                self._fs.delete_dir(fs_dir)
            except (FileNotFoundError, pa.ArrowIOError):
                pass  # Partition didn't exist yet

        # Append the new records (directories were cleared above)
        self._append_records(table, arrow_table, partition_by)

    def read(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> Iterator[dict]:
        glob = self._parquet_glob(table, filters)
        params: list = []
        where = filters_to_sql(filters, params)
        sql = f"SELECT * FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
        try:
            cursor = self._conn.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            while True:
                batch = cursor.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    yield dict(zip(cols, row))
        except duckdb.IOException:
            return  # No Parquet files found — table is empty

    def bulk_read(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> pa.Table:
        glob = self._parquet_glob(table, filters)
        params: list = []
        where = filters_to_sql(filters, params)
        sql = f"SELECT * FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
        try:
            result = self._conn.execute(sql, params).arrow()
            # In DuckDB >= 1.1, .arrow() may return a RecordBatchReader
            if not isinstance(result, pa.Table):
                result = result.read_all()
            return result
        except duckdb.IOException:
            # No Parquet files — return empty table with registered schema
            schema, _ = self._registry.get(table)
            return schema.empty_table()

    def distinct_values(
        self,
        table: str,
        fields: list[str],
        filters: Filters | None = None,
    ) -> list[dict]:
        schema, partition_by = self._registry.get(table)

        # Path A: Hive directory introspection when fields == partition_by and no filters
        if (
            partition_by
            and filters is None
            and set(fields) == set(partition_by)
        ):
            return self._distinct_values_from_hive(table, schema, partition_by)

        # Path B: SQL SELECT DISTINCT
        field_list = ", ".join(f'"{f}"' for f in fields)
        glob = self._parquet_glob(table, filters)
        params: list = []
        where = filters_to_sql(filters, params)
        sql = (
            f"SELECT DISTINCT {field_list} "
            f"FROM read_parquet('{glob}', hive_partitioning=True) "
            f"WHERE {where}"
        )
        try:
            rows = self._conn.execute(sql, params).fetchall()
            return [dict(zip(fields, row)) for row in rows]
        except duckdb.IOException:
            return []

    def _distinct_values_from_hive(
        self,
        table: str,
        schema: pa.Schema,
        partition_by: list[str],
    ) -> list[dict]:
        """List distinct partition combinations by introspecting the Hive directory tree."""
        table_fs_root = self._fs_table_root(table)

        def walk(
            path: str,
            remaining_keys: list[str],
            current: dict,
        ) -> list[dict]:
            if not remaining_keys:
                return [current]

            key = remaining_keys[0]
            try:
                selector = pa_fs.FileSelector(path, recursive=False)
                infos = self._fs.get_file_info(selector)
            except (FileNotFoundError, pa.ArrowIOError):
                return []

            results: list[dict] = []
            for info in infos:
                if info.type != pa_fs.FileType.Directory:
                    continue
                dir_name = info.path.rstrip("/").split("/")[-1]
                if "=" not in dir_name:
                    continue
                dir_key, dir_val = dir_name.split("=", 1)
                if dir_key != key:
                    continue
                arrow_type = schema.field(key).type
                parsed_val = _parse_partition_value(dir_val, arrow_type)
                results.extend(
                    walk(info.path, remaining_keys[1:], {**current, key: parsed_val})
                )
            return results

        return walk(table_fs_root, list(partition_by), {})

    def count(
        self,
        table: str,
        filters: Filters | None = None,
    ) -> int:
        glob = self._parquet_glob(table, filters)
        params: list = []
        where = filters_to_sql(filters, params)
        sql = f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
        try:
            result = self._conn.execute(sql, params).fetchone()
            return result[0] if result else 0
        except duckdb.IOException:
            return 0

    def join(
        self,
        left: str,
        right: str,
        on: str,
        left_filters: Filters | None = None,
        right_filters: Filters | None = None,
        select: Literal["left", "right", "both"] = "both",
    ) -> Iterator[dict]:
        left_glob = self._parquet_glob(left, left_filters)
        right_glob = self._parquet_glob(right, right_filters)
        params: list = []
        left_where = filters_to_sql(left_filters, params)
        right_where = filters_to_sql(right_filters, params)

        if select == "left":
            select_clause = "l.*"
        elif select == "right":
            select_clause = "r.*"
        else:
            select_clause = "l.*, r.*"

        # Use CTEs so each side's WHERE clause is resolved within its own scope,
        # avoiding ambiguous column references when both tables share column names
        # (e.g., partition key columns like instrument, year, month).
        sql = f"""
            WITH l AS (
                SELECT * FROM read_parquet('{left_glob}', hive_partitioning=True)
                WHERE {left_where}
            ),
            r AS (
                SELECT * FROM read_parquet('{right_glob}', hive_partitioning=True)
                WHERE {right_where}
            )
            SELECT {select_clause}
            FROM l
            JOIN r ON l."{on}" = r."{on}"
        """
        try:
            cursor = self._conn.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            while True:
                batch = cursor.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    yield dict(zip(cols, row))
        except duckdb.IOException:
            return  # One or both tables are empty
