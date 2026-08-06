# amplify-db-utils

Columnar database abstraction layer for AMPLIfy media (e.g., image) workflows.

Provides a minimal, append-only API for writing and querying columnar data at scale behind a single
`ColumnarStore` interface. Two backends ship with the package:

| Backend | Class | Use for |
|---|---|---|
| DuckDB + Parquet | `DuckDBParquetStore` | Local filesystem or any S3-compatible store (VAST S3, MinIO). No server required. Development, laptops, single-process workflows. |
| VAST DB | `VastDBStore` | Production-scale concurrent access. Optional dependency — install the `vastdb` extra. |

Designed as the database counterpart to `amplify-storage-utils` — a parallel abstraction over the same storage infrastructure, not a dependency on it.

---

## Install

```bash
pip install amplify-db-utils              # core (DuckDB + Parquet backend)
pip install 'amplify-db-utils[pandas]'    # + pandas DataFrame support
pip install 'amplify-db-utils[vastdb]'    # + VAST DB backend
```

Requires Python 3.10+.

`VastDBConfig` / `VastDBStore` are only exported from `amplify_db_utils` when the `vastdb` extra is
installed; importing them otherwise raises `ImportError`. Everything else works without it.

---

## Getting started

### DuckDB + Parquet

```python
from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

# Local filesystem store
config = DuckDBParquetConfig(root="/data/ifcb")
store = DuckDBParquetStore(config)

# S3-compatible store (e.g., VAST S3)
config = DuckDBParquetConfig(
    root="s3://ifcb-data/columnar",
    s3_endpoint="vast-s3.whatever.edu:9000",
    s3_access_key="...",
    s3_secret_key="...",
    s3_use_ssl=True,   # default
    threads=None,      # None = DuckDB default (all cores)
)
store = DuckDBParquetStore(config)
```

### VAST DB

VAST DB tables are addressed as `bucket.schema.table`. One `VastDBStore` is bound to one
`(bucket, schema)` pair; the schema is created on first `create_table()` if it does not exist.

```python
from amplify_db_utils import VastDBConfig, VastDBStore

config = VastDBConfig(
    endpoint="https://vast.example.org",
    access_key="...",
    secret_key="...",
    bucket="avastdbbucket",
    schema="ifcb",
    add_written_at=False,   # see "WORM deduplication" below
)
store = VastDBStore(config)
```

Both classes implement `ColumnarStore`, so consuming services should annotate against the ABC and
take the concrete store by injection:

```python
from amplify_db_utils import ColumnarStore

def ingest(store: ColumnarStore, records: list[dict]) -> None:
    store.write("images", records)
```

---

## Examples

The examples below use `DuckDBParquetStore`, but every call except `write(..., overwrite=True)`
behaves the same on `VastDBStore`. See [VAST DB backend](#vast-db-backend) for the differences.

### Define and register a table

Tables are defined as Pydantic models (or a `pa.Schema` directly). Partition key fields are ordinary
columns — no separate routing needed.

```python
from datetime import datetime
from typing import Optional
from pydantic import BaseModel

class ImageRecord(BaseModel):
    image_id: str
    timestamp: datetime
    instrument: str   # partition key
    year: int         # partition key
    month: int        # partition key

# Idempotent — safe to call at service startup
store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])

# Inspect what was registered
schema = store.get_schema("images")   # -> pa.Schema
```

Supported field annotations:

| Python annotation | Arrow type |
|---|---|
| `str` | `utf8` |
| `int` | `int64` |
| `float` | `float64` |
| `bool` | `bool` |
| `datetime` | `timestamp("us", tz="UTC")` |
| `dict` | `large_utf8` — value is JSON-serialized on write |
| `list[T]` for `T` in str/int/float/bool/datetime | `list_(T)` |
| `Optional[T]` / `T \| None`, or any field with a default | as above, `nullable=True` |

`list[dict]` and bare `list` are not supported and raise `TypeError`.

### Write records

```python
store.write("images", [
    {
        "image_id":   "D20240101T120000_IFCB107_00001",
        "timestamp":  datetime(2024, 1, 1, 12, 0, 0),
        "instrument": "IFCB107",
        "year":       2024,
        "month":      1,
    },
    # ... records may span multiple partitions in a single call
])
```

`records` may be `list[dict]`, a PyArrow `Table`, or a pandas `DataFrame` (with the `pandas` extra).
Columns not in the registered schema are dropped; missing nullable columns are filled with nulls;
missing non-nullable columns raise `ValueError`. Partition key columns must be present and non-null.

### Read and query

```python
# Iterate rows — partition pruning applied automatically
for row in store.read("images", filters={"instrument": "IFCB107", "year": 2024}):
    print(row["image_id"])

# Bulk read as PyArrow Table (zero-copy; call .to_pandas() if needed)
table = store.bulk_read("images", filters={
    "instrument": "IFCB107",
    "timestamp":  {"gte": "2024-01-01", "lt": "2024-02-01"},
})

# Row count without materializing results
n = store.count("images", filters={"instrument": "IFCB107"})

# Discover what partitions exist (fast directory listing, no data scan)
partitions = store.distinct_values("images", ["instrument", "year", "month"])
```

### Overwrite a partition

Useful for idempotent batch re-runs. Replaces all rows for each distinct partition key combination
present in the records. **DuckDB+Parquet only** — see [VAST DB backend](#vast-db-backend).

```python
store.write("images", new_records, overwrite=True)
```

### Cross-table join

```python
# Spatial filter: find images within a bounding box via geolocation_index
rows = store.join(
    left="geolocation_index",
    right="images",
    on="image_id",
    left_filters={"lat": {"gte": 40.0, "lte": 42.0}, "lon": {"gte": -71.0, "lte": -70.0}},
    select="right",  # return image columns, not geo columns
)
```

`select="both"` may produce surprising results when the two tables share column names other than the
join key; prefer `"left"` or `"right"`.

### Filter syntax

```python
filters = {
    "instrument": "IFCB107",                              # equality
    "timestamp":  {"gte": "2024-01-01", "lt": "2024-02-01"},  # range (gte/gt/lte/lt)
    "class_name": {"in": ["Ceratium", "Dinoflagellate"]}, # set membership
}
```

The same dict syntax works on both backends: DuckDB+Parquet compiles it to a parameterized SQL
`WHERE` clause, VAST DB to an ibis-style predicate pushed down to the server.

---

## DuckDB + Parquet backend

Data is written as Hive-partitioned Parquet under `config.root`; DuckDB runs in-process with an
in-memory catalog, so the data lives in the Parquet files, not in a DuckDB file. S3 access goes
through DuckDB's `httpfs` extension for queries and PyArrow's `S3FileSystem` for directory
operations.

**Concurrency.** Reads are safe from multiple threads — every query gets its own DuckDB cursor (a
single connection holds only one pending result, which would silently truncate interleaved lazy
generators). Concurrent *writes* are not supported, from either threads or processes (e.g., SLURM
array jobs): the schema registry is read-modify-written without locking, and `overwrite=True`
deletes partition directories out from under in-flight readers. Use VAST DB for concurrent write
workloads.

**Schema registry.** Per-table schema and `partition_by` metadata are persisted as
`_registry/tables.json` at the store root, read and written through PyArrow's filesystem abstraction
(local or S3).

**Schema evolution.** Adding a nullable column is allowed; `create_table` on an existing table
performs a compatibility check and updates the registry. Removing columns, changing types, or
changing `partition_by` raise `ValueError`. The partition key structure is permanent at design time.

### Migrating a legacy registry

The registry stores each table's schema as a base64 Arrow-IPC blob under a `schema_ipc` key.
Registries written by older versions instead used a `schema_fields` list of `{name, type, nullable}`
dicts, which is no longer read on load — such a store fails to open with a `ValueError` pointing you
here.

Upgrade the sidecar in place with the bundled console script:

```bash
amplify-db-migrate path/to/store/_registry/tables.json
```

It rewrites each legacy entry to carry `schema_ipc` plus a human-readable `columns` list, leaving
`partition_by` untouched. The command is idempotent — re-running it on an already-migrated file is a
no-op — so it is safe to run defensively before opening a store of unknown age.

---

## VAST DB backend

`VastDBStore` implements the same `ColumnarStore` API against VAST DB. Differences from
DuckDB+Parquet:

**Append-only (WORM).** `write(..., overwrite=True)` raises `NotImplementedError`. There is no
update or delete of rows. For idempotent re-ingestion, use the `written_at` pattern below.

**No Hive partitioning.** `partition_by` columns are ordinary data columns. They are a no-op on
write; on read they are pushed down as server-side predicates. Passing `partition_by` still
validates that the named columns exist in the schema, so the same `create_table()` call works
against either backend.

**No sidecar registry.** VAST DB manages schemas itself. `create_table()` creates the VAST DB schema
if absent, creates the table if absent, and otherwise checks compatibility (no removed columns, no
type changes, new columns must be nullable). Adding a new nullable column to an existing table is
accepted by the check but not yet applied — `ALTER TABLE ADD COLUMN` is a TODO. `get_schema()` reads
from a per-instance cache populated by `create_table()`, so call `create_table()` before using a
table from a fresh process.

**Schema normalization.** VAST DB rejects `nullable=False` and does not carry timestamp time zones,
so requested schemas are normalized on `create_table()`: every field becomes nullable, tz-aware
timestamps lose their tz, `large_string` → `string`, `large_binary` → `binary`. Read back a
`datetime` column and you get a naive UTC timestamp.

**Client-side join and distinct.** `join()` pulls both sides into memory as Arrow and joins them in
an in-process DuckDB — fine for moderate result sizes, not for billion-row joins. `distinct_values()`
likewise scans and de-duplicates client-side, since VAST DB has no server-side `SELECT DISTINCT`;
for partition discovery on large tables, maintain a separate lightweight index table.
`count()` with no filters is free (server-side row-count metadata); a filtered count streams batches
of one projected column.

### WORM deduplication

Set `add_written_at=True` on the config and every table gains a nullable `written_at` timestamp
column, stamped on each write. At read time, collapse re-ingested rows to the most recent version
per key:

```python
from amplify_db_utils.vastdb_store import dedup_by_written_at

table = store.bulk_read("rois", filters={"bin_lid": "D20240101T120000_IFCB107"})
table = dedup_by_written_at(table, key_columns=["bin_lid", "roi_number"])
```

The flag is opt-in because it mutates user-declared schemas.

### Teardown helpers

`drop_table(name)` and `drop_schema()` exist primarily for test teardown (per-run throwaway
schemas). Both are irreversible; `drop_schema()` drops every table in the store's schema and then
the schema itself. Do not point them at production.

---

## Testing

```bash
pip install 'amplify-db-utils[dev]'
pytest
```

The unit suite under `tests/` runs entirely against temp-directory DuckDB+Parquet stores — no
credentials, no network. `tests/smoke/test_vastdbstore.py` exercises a live VAST DB and is skipped
unless `vastdb` is installed *and* `VASTDB_ENDPOINT` is set:

```bash
export VASTDB_ENDPOINT=https://vast.example.org
export VASTDB_ACCESS_KEY=...
export VASTDB_SECRET_KEY=...
export VASTDB_BUCKET=somevastdbbucket
pytest tests/smoke
```

Each smoke run creates a uniquely named throwaway schema and drops it on teardown.

---

## Design notes

**Purpose-built, not general SQL.** The API surface is shaped around the access patterns of a
scalable data / provenance store for observational data: point lookups, temporal/spatial range
scans, bulk partition reads, and append-only writes. It is not a SQL abstraction.

**Append-only writes.** There are no update or delete operations. On DuckDB+Parquet,
`write(..., overwrite=True)` replaces an entire partition, which is the supported pattern for
re-running batch jobs. VAST DB has no equivalent — deduplicate at read time instead.

**Backend independence.** `ColumnarStore` is the contract. Code written against it moves from a
laptop-scale Parquet store to production VAST DB without changes, with two caveats to design
around: `overwrite=True` and tz-aware timestamps do not survive the move.
