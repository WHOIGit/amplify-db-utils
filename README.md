# amplify-db-utils

Columnar database abstraction layer for AMPLIfy media (e.g., image) workflows.

Provides a minimal, append-only API for writing and querying columnar data at scale. The primary backend is currently **DuckDB + Parquet**, which works against a local filesystem or any S3-compatible store (VAST S3, MinIO) with no server required. Future versions will support VAST DB.

Designed as the database counterpart to `amplify-storage-utils` — a parallel abstraction over the same storage infrastructure, not a dependency on it.

---

## Install

```bash
pip install amplify-db-utils          # core
pip install 'amplify-db-utils[pandas]'  # + pandas DataFrame support
```

Requires Python 3.10+.

---

## Getting started

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
)
store = DuckDBParquetStore(config)
```

---

## Examples

### Define and register a table

Tables are defined as Pydantic models. Partition key fields are ordinary columns — no separate routing needed.

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
```

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

Useful for idempotent batch re-runs. Replaces all rows for each distinct partition key combination present in the records.

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

### Filter syntax

```python
filters = {
    "instrument": "IFCB107",                              # equality
    "timestamp":  {"gte": "2024-01-01", "lt": "2024-02-01"},  # range (gte/gt/lte/lt)
    "class_name": {"in": ["Ceratium", "Dinoflagellate"]}, # set membership
}
```

---

## Design notes

**Purpose-built, not general SQL.** The API surface is shaped around the access patterns of the generalized image service: point lookups, temporal/spatial range scans, bulk partition reads, and append-only writes. It is not a SQL abstraction.

**Append-only writes.** There are no update or delete operations. `write(..., overwrite=True)` replaces an entire partition atomically, which is the supported pattern for re-running batch jobs.

**Schema evolution.** Adding a nullable column is allowed; `create_table` on an existing table performs a compatibility check and updates the registry. Removing columns, changing types, or changing `partition_by` raise `ValueError`. The partition key structure is permanent at design time.

**Schema registry.** Per-table schema and `partition_by` metadata are persisted as `_registry/tables.json` at the store root, readable and writable via PyArrow's filesystem abstraction (local or S3).

**Backend independence.** `ColumnarStore` is an abstract base class. The DuckDB+Parquet backend is intended for local development, laptops, and single-process workflows. A VAST DB backend can be added as a drop-in for production-scale concurrent access without changes to the consuming service.

