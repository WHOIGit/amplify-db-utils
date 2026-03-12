# Implementation Plan: `amplify-db-utils` (DuckDB + Parquet Backend)

*Scope: Python package `amplify-db-utils` with `ColumnarStore` ABC and `DuckDBParquetStore` implementation. VAST DB backend is explicitly out of scope.*

---

## Decisions Made for This Plan

These lower-priority items from the design doc are resolved here:

| Item | Decision |
|------|----------|
| `bulk_read` return type | **PyArrow `Table`** — zero-copy, pandas-convertible via `.to_pandas()`, Arrow-native pipelines work without extra conversion |
| `write()` input types | Accepts `list[dict]`, `pa.Table`, or `pd.DataFrame` — all converted to Arrow internally before writing |
| `distinct_values` Hive optimization | **Implement it** — when queried fields exactly match the registered `partition_by` keys, introspect directory listing instead of scanning Parquet. Falls back to `SELECT DISTINCT` for non-partition fields |
| Schema registry persistence | Stored as `_registry/tables.json` at the root path — covers both filesystem and S3 roots transparently via DuckDB's own I/O |

---

## Package Layout

```
amplify-db-utils/
├── pyproject.toml
├── src/
│   └── amplify_db_utils/
│       ├── __init__.py          # public exports
│       ├── base.py              # ColumnarStore ABC + public types
│       ├── filters.py           # filter dict → SQL/Arrow predicate translation
│       ├── schema.py            # schema validation, Pydantic→PyArrow conversion
│       ├── registry.py          # schema registry persistence (DuckDB-specific)
│       ├── config.py            # DuckDBParquetConfig dataclass
│       └── duckdb_parquet.py    # DuckDBParquetStore implementation
└── tests/
    ├── conftest.py              # shared fixtures (tmp_path stores)
    ├── test_filters.py
    ├── test_schema.py
    ├── test_write.py
    ├── test_read.py
    ├── test_overwrite.py
    ├── test_distinct_values.py
    ├── test_join.py
    └── test_schema_evolution.py
```

---

## Dependencies

```toml
[project]
dependencies = [
    "duckdb>=1.0",
    "pyarrow>=14.0",
    "pydantic>=2.0",
]

[project.optional-dependencies]
pandas = ["pandas>=2.0"]
s3 = []  # DuckDB httpfs is bundled — no extra dep; document that VAST S3 endpoint config is in DuckDBParquetConfig
```

No dependency on `amplify-storage-utils` — they are parallel abstractions over the same backend, not a stack.

---

## Step 1: Public Types (`base.py`)

Define the types that callers import directly. Keep this file free of implementation details.

**`FilterValue` union type** representing the filter syntax:
```python
# Equality: "abc123"
# Range:    {"gte": ..., "lt": ...}  (any subset of gte/gt/lte/lt)
# Set:      {"in": [...]}
FilterValue = str | int | float | bool | dict
Filters = dict[str, FilterValue]
```

**`ColumnarStore` ABC** — the full method signatures from the design doc, with return types filled in:

```python
class ColumnarStore(ABC):
    @abstractmethod
    def create_table(self, table: str, schema: type[BaseModel] | pa.Schema, partition_by: list[str] | None = None) -> None: ...
    @abstractmethod
    def write(self, table: str, records: list[dict] | pa.Table | pd.DataFrame, overwrite: bool = False) -> None: ...
    @abstractmethod
    def read(self, table: str, filters: Filters | None = None) -> Iterator[dict]: ...
    @abstractmethod
    def bulk_read(self, table: str, filters: Filters | None = None) -> pa.Table: ...
    @abstractmethod
    def distinct_values(self, table: str, fields: list[str], filters: Filters | None = None) -> list[dict]: ...
    @abstractmethod
    def count(self, table: str, filters: Filters | None = None) -> int: ...
    @abstractmethod
    def join(self, left: str, right: str, on: str, left_filters: Filters | None = None, right_filters: Filters | None = None, select: Literal["left", "right", "both"] = "both") -> Iterator[dict]: ...
```

No other logic in `base.py`.

---

## Step 2: Filter Translation (`filters.py`)

This is the most broadly used piece — every read method goes through it.

**Implement `filters_to_sql(filters: Filters, params: list) -> str`** — returns a SQL `WHERE` clause fragment (without the `WHERE` keyword), appending positional parameters to `params`. Use DuckDB's `$1`-style positional params to prevent injection.

Supported operators:

| Input | SQL output |
|-------|-----------|
| `{"field": "val"}` | `field = $N` |
| `{"field": {"gte": x, "lt": y}}` | `field >= $N AND field < $M` |
| `{"field": {"in": [a, b]}}` | `field IN ($N, $M, ...)` |

Edge cases to handle:
- Empty `filters` or `None` → return `"1=1"` (always true; keeps query structure uniform)
- Range dict with only some keys (`gt` only, `lte` only, etc.) → emit only the applicable conditions
- List values in `in` filter → expand to individual parameters
- `bool` values → DuckDB accepts Python `bool` directly; no special handling needed

No SQL string interpolation of user values anywhere in this file — all values go through the params list.

---

## Step 3: Schema Utilities (`schema.py`)

**`to_arrow_schema(schema: type[BaseModel] | pa.Schema) -> pa.Schema`** — converts a Pydantic model class to a PyArrow schema by inspecting its fields. Handle the common field types: `str`, `int`, `float`, `bool`, `datetime`, `dict` (→ `pa.large_utf8()` for JSON blob columns). Raise `TypeError` for unsupported field types with a descriptive message. If passed a `pa.Schema` directly, return it unchanged.

**`validate_records(records: list[dict] | pa.Table | pd.DataFrame, schema: pa.Schema) -> pa.Table`** — converts input to a PyArrow Table and validates column presence and types against the registered schema. Raises `ValueError` with a descriptive message if columns are missing or types are incompatible. This is the single validation entry point called by `write()`.

**`check_partition_fields(records: pa.Table, partition_by: list[str]) -> None`** — verifies that all partition key fields are present in the table and contain no null values (nulls in partition keys would produce malformed Hive paths). Raises `ValueError` with field-level detail.

---

## Step 4: Schema Registry (`registry.py`)

The registry persists per-table metadata across sessions — required for `create_table`'s compatibility check.

**Storage location:** `{root}/_registry/tables.json` — a single JSON file containing a dict of table name → `{"schema_fields": [...], "partition_by": [...]}`. DuckDB reads/writes this file via its filesystem abstraction (works for both local paths and S3 roots via httpfs).

**`SchemaRegistry` class:**

- `load(conn: duckdb.DuckDBPyConnection, root: str) -> SchemaRegistry` — reads `_registry/tables.json` if it exists; returns empty registry otherwise
- `save(conn: duckdb.DuckDBPyConnection, root: str) -> None` — writes the current state back to `_registry/tables.json`
- `register(table: str, schema: pa.Schema, partition_by: list[str] | None) -> None` — adds or updates a table entry after compatibility check
- `get(table: str) -> tuple[pa.Schema, list[str] | None]` — retrieves registered schema + partition_by; raises `KeyError` if table unknown

**Compatibility check logic in `register()`:**

| Change detected | Action |
|----------------|--------|
| Table not in registry | Register and save — first `create_table` call |
| `partition_by` changed | Raise `ValueError` — partition keys are immutable |
| Column removed | Raise `ValueError` — requires explicit migration |
| Column type changed | Raise `ValueError` — breaking change |
| Column renamed | Detected as remove+add → `ValueError` |
| New nullable column added | Accept — update registry, existing Parquet files return NULL for the new column |
| No changes | No-op silently |

Implementation note: "nullable" is determined by whether the PyArrow field has `nullable=True`. Pydantic `Optional[T]` fields map to nullable Arrow fields.

---

## Step 5: Configuration (`config.py`)

```python
@dataclass
class DuckDBParquetConfig:
    root: str                    # local path or s3://bucket/prefix
    s3_endpoint: str | None = None      # e.g., "vast-s3.whoi.edu:9000"
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_use_ssl: bool = True
    threads: int | None = None          # DuckDB thread count; None = DuckDB default
```

`root` is the only required field. When `root` starts with `s3://`, S3 settings are required.

---

## Step 6: `DuckDBParquetStore` — Initialization and DuckDB Setup (`duckdb_parquet.py`)

**`__init__(self, config: DuckDBParquetConfig)`:**
1. Open an in-process DuckDB connection (`duckdb.connect(":memory:")` — not a persistent DuckDB file; the data is in Parquet, not the DuckDB file)
2. Install and load `httpfs` extension if `config.root` is an S3 path
3. Apply S3 settings via `SET s3_*` DuckDB SQL commands from config
4. If `threads` is set, apply `SET threads = N`
5. Load `SchemaRegistry` from `config.root`

**Hive path helper `_partition_path(table: str, partition_values: dict) -> str`** — given a table name and a dict of partition key → value, returns the Hive-style directory path: `{root}/{table}/instrument=IFCB107/year=2024/month=1/`. Values are URL-encoded for filesystem safety (spaces, slashes). Used by `write(..., overwrite=True)` to identify directories to clear before writing.

**`_parquet_glob(table: str, filters: Filters | None) -> str`** — returns a glob expression that DuckDB uses to enumerate Parquet files. When filters contain all partition key fields as equality constraints, generate a specific path (partition pruning). Otherwise generate `{root}/{table}/**/*.parquet`. DuckDB's Hive partition pruning handles the rest automatically when reading with `hive_partitioning=True`.

---

## Step 7: `create_table()` Implementation

1. Convert `schema` to `pa.Schema` via `to_arrow_schema()`
2. Validate that all `partition_by` fields exist in the schema
3. Delegate to `registry.register()` which performs the compatibility check and persists

This is the full implementation — no DDL or file system operations are needed at table-creation time for DuckDB+Parquet. Tables implicitly exist when their first Parquet file is written.

---

## Step 8: `write()` Implementation

**Steps:**

1. **Validate table is registered** — raise `RuntimeError` if `create_table` was never called
2. **Normalize input** → `pa.Table` via `validate_records()`
3. **Check partition fields** via `check_partition_fields()`
4. **Register the records as a DuckDB relation** — use `duckdb.from_arrow(table)` to register the Arrow table as an in-memory relation
5. **For `overwrite=False`:** use DuckDB's `COPY ... TO` with `PARTITION_BY` to write directly to the Hive directory structure:
   ```sql
   COPY (SELECT * FROM records) TO '{root}/{table}'
   (FORMAT PARQUET, PARTITION_BY (instrument, year, month), OVERWRITE_OR_IGNORE TRUE)
   ```
   `OVERWRITE_OR_IGNORE` avoids filename conflicts on repeated appends; DuckDB generates unique filenames per partition.
6. **For `overwrite=True`:** determine the distinct partition key combinations present in the records, delete all Parquet files under each affected Hive directory, then run the same `COPY` statement. Since DuckDB+Parquet is single-process, there is no concurrent writer to race against — no staging/rename needed.

The `_partition_path()` helper (from Step 6) is used to construct the directory path for deletion in the overwrite case. Deletion uses PyArrow's filesystem abstraction (`LocalFileSystem` or `S3FileSystem`) to list and remove files under each affected partition directory.

---

## Step 9: `read()` Implementation

```python
def read(self, table: str, filters: Filters | None = None) -> Iterator[dict]:
    glob = self._parquet_glob(table, filters)
    params = []
    where = filters_to_sql(filters, params)
    sql = f"SELECT * FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
    cursor = self._conn.execute(sql, params)
    for row in cursor.fetchall():
        yield dict(zip([d[0] for d in cursor.description], row))
```

Yields rows lazily — DuckDB streams results, so memory stays bounded for large result sets. Partition pruning happens automatically via the Hive path glob when partition key filters are present.

---

## Step 10: `bulk_read()` Implementation

```python
def bulk_read(self, table: str, filters: Filters | None = None) -> pa.Table:
    glob = self._parquet_glob(table, filters)
    params = []
    where = filters_to_sql(filters, params)
    sql = f"SELECT * FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
    return self._conn.execute(sql, params).arrow()
```

`.arrow()` returns a `pa.Table` directly from DuckDB with zero Python-level row iteration. This is the preferred method for bulk retrieval in downstream pipelines (features export, classification index reads, etc.).

---

## Step 11: `distinct_values()` Implementation

Two code paths:

**Path A — Hive directory introspection** (when `fields` exactly matches the registered `partition_by` keys, no filters):
- List the directory tree under `{root}/{table}/` up to depth `len(partition_by)`
- Parse Hive path segments (`key=value/`) into dicts
- Return the list of dicts
- Use PyArrow's filesystem abstraction for directory listing (works for both local and S3)

**Path B — SQL SELECT DISTINCT** (all other cases):
```python
field_list = ", ".join(fields)
glob = self._parquet_glob(table, filters)
params = []
where = filters_to_sql(filters, params)
sql = f"SELECT DISTINCT {field_list} FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
return [dict(row) for row in self._conn.execute(sql, params).fetchall()]
```

Path A is triggered when `set(fields) == set(self._registry.get(table)[1] or [])` and `filters` is None.

---

## Step 12: `count()` Implementation

```python
def count(self, table: str, filters: Filters | None = None) -> int:
    glob = self._parquet_glob(table, filters)
    params = []
    where = filters_to_sql(filters, params)
    sql = f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=True) WHERE {where}"
    return self._conn.execute(sql, params).fetchone()[0]
```

---

## Step 13: `join()` Implementation

Both tables must be registered in the same `DuckDBParquetStore` instance. DuckDB executes the join natively as a SQL query over two Parquet datasets.

```python
def join(self, left, right, on, left_filters=None, right_filters=None, select="both") -> Iterator[dict]:
    left_glob = self._parquet_glob(left, left_filters)
    right_glob = self._parquet_glob(right, right_filters)
    params = []
    left_where = filters_to_sql(left_filters, params)
    right_where = filters_to_sql(right_filters, params)

    if select == "left":
        select_clause = "l.*"
    elif select == "right":
        select_clause = "r.*"
    else:
        select_clause = "l.*, r.*"

    sql = f"""
        SELECT {select_clause}
        FROM read_parquet('{left_glob}', hive_partitioning=True) l
        JOIN read_parquet('{right_glob}', hive_partitioning=True) r
        ON l.{on} = r.{on}
        WHERE ({left_where}) AND ({right_where})
    """
    cursor = self._conn.execute(sql, params)
    for row in cursor.fetchall():
        yield dict(zip([d[0] for d in cursor.description], row))
```

**Column name collision** when `select="both"`: if left and right tables share column names other than the join key, DuckDB will include both with ambiguous names. Document that callers should use `select="left"` or `select="right"` when tables share column names. The primary use case (`geolocation_index JOIN images`) uses `select="right"` precisely to avoid this.

---

## Step 14: Public Exports (`__init__.py`)

Export exactly:
```python
from amplify_db_utils.base import ColumnarStore, Filters
from amplify_db_utils.config import DuckDBParquetConfig
from amplify_db_utils.duckdb_parquet import DuckDBParquetStore
```

Nothing else. Internal modules (`filters`, `schema`, `registry`) are not part of the public API.

---

## Testing Strategy

All tests use `tmp_path` fixtures (pytest) for local filesystem stores. No mocking of DuckDB or the filesystem — tests run against real DuckDB with real Parquet files.

| Test file | What it covers |
|-----------|---------------|
| `test_filters.py` | `filters_to_sql` unit tests: equality, range, in, empty, None, edge cases |
| `test_schema.py` | Pydantic→Arrow conversion, validation errors, nullable handling |
| `test_write.py` | Append writes, multi-partition batches, schema mismatch errors, missing partition key errors |
| `test_overwrite.py` | `overwrite=True` per-partition replacement, multi-partition overwrite, staging directory cleanup |
| `test_read.py` | Filtered reads, range queries, empty result sets, `bulk_read` returns Arrow Table |
| `test_distinct_values.py` | Hive directory path for partition keys, SQL DISTINCT for non-partition fields, with filters |
| `test_join.py` | Two-table join, `select="right"`, filter application on both sides |
| `test_schema_evolution.py` | Idempotent `create_table`, add nullable column (allowed), remove column (error), change type (error), change `partition_by` (error) |

One integration test that writes data, reads it back via `bulk_read`, and verifies the Arrow Table content matches. This covers the full write→Parquet→read round-trip.

**No S3 tests in CI** — S3 configuration tests are manual/integration-only. Document the S3 path in `tests/README.md` with instructions for running against a local MinIO instance.

---

## Implementation Order

1. `config.py` (no dependencies)
2. `filters.py` + `test_filters.py`
3. `schema.py` + `test_schema.py`
4. `registry.py`
5. `base.py` (ABC only)
6. `duckdb_parquet.py` — in method order: init → `create_table` → `write` (append only) → `read` → `bulk_read` → `count` → `distinct_values` → `write` (overwrite) → `join`
7. All remaining tests
8. `__init__.py` and `pyproject.toml`

Writing `write()` in two passes (append first, then overwrite) keeps the initial implementation simple and lets tests cover the core path before adding the staging/rename complexity.
