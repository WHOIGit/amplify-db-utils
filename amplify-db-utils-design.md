# amplify-db-utils — Design Notes

*Originated from AMPLIfy planning / generalized image service discussion, 2026-03-05*

---

## What This Is

A columnar database abstraction layer, directly analogous to `amplify-storage-utils` (which abstracts the object store). The generalized image service should have no direct dependency on a specific columnar DB implementation. `amplify-db-utils` provides a minimal, purpose-built API surface that can be backed by:

- **DuckDB + Parquet** — portable, no server, works against local filesystem or any S3-compatible store; `httpfs` extension handles S3/VAST S3, plain paths handle filesystem — same implementation either way
- **VAST DB** — native VAST storage integration, better for concurrent access and server-side filtering

New implementations can be added without changing the image service.

---

## Design Principles

**Purpose-built, not general SQL.** The abstraction does not need to expose a general SQL interface. The generalized image service (the primary consumer) has well-defined, bounded access patterns. `amplify-db-utils` is built for those patterns specifically. This keeps the API minimal and implementations straightforward, avoiding the lowest-common-denominator trap of abstracting all of SQL.

**Instrument-agnostic schemas.** Schemas are defined by the consuming service, not by instrument type. The generalized image service defines a single `ImageRecord` schema used for images from any instrument — instrument identity is a field value, not a schema variant. `amplify-db-utils` itself has no knowledge of instruments or domains.

**No explicit partition routing.** VAST DB manages partitioning internally and does not support user-specified partition placement. Exposing a `partition` parameter in the API would mean the DuckDB implementation uses it and the VAST DB implementation silently ignores it — a leaky abstraction. Instead, partition key fields are stored as ordinary data columns, reads target them via filters, and DuckDB handles Hive path routing from the column values at write time.

---

## Required Access Patterns (from Generalized Image Service)

These are the patterns the API surface must support:

| Pattern | Description |
|---------|-------------|
| Point lookup | Retrieve rows by image_id |
| Range scan (temporal) | Filter by timestamp |
| Range scan (spatial) | Filter by lat/lon/depth via `geolocation_index` table — not the images table |
| Filtered scan | Filter by kind, source, run_id, sample_id |
| Bulk partition read | Read all rows for a sample/run — returns columnar format (DataFrame/Arrow) |
| Bulk append | Write a batch of records (from a batch workflow output) |
| Partition discovery | List distinct values of partition key fields |

All writes are **append-only**. No updates, no deletes.

---

## Proposed API Surface

```python
class ColumnarStore:

    def create_table(
        self,
        table: str,
        schema: type[BaseModel] | pa.Schema,
        partition_by: list[str] | None = None
    ) -> None:
        """Register a schema and partition key structure for a table.
        Subsequent writes are validated against both.
        - schema: row schema (Pydantic model or PyArrow schema); partition key
          fields must be included as ordinary columns in this schema
        - partition_by: ordered list of column names that define the partition
          structure, e.g. ["instrument", "year", "month"]
          For DuckDB+Parquet: determines the Hive directory path structure
          For VAST DB: informational/documentation only — VAST manages
          partitioning internally
        Maps to CREATE TABLE in VAST DB; stored as schema metadata for DuckDB+Parquet."""

    def write(
        self,
        table: str,
        records: list[dict] | DataFrame,
        overwrite: bool = False,
    ) -> None:
        """Bulk write records to a table.
        Validates that records conform to the registered row schema.
        Partition key fields must be present as data columns in each record.

        overwrite=False (default): append records to existing data.
        overwrite=True: for each distinct partition key combination present in
          the records, replace all existing rows in that partition with the new
          ones. Records may span multiple partitions; each is replaced
          independently. Scope is determined entirely by the partition key
          column values in the records — no separate partition argument needed.
          For DuckDB+Parquet: atomically swaps the Hive directory for each
          affected partition (write to staging path, then rename).
          For VAST DB: deletes rows matching each partition key value set,
          then inserts the new rows.

        Raises ValueError on schema mismatch or missing partition key fields."""

    def read(
        self,
        table: str,
        filters: dict | None = None,
    ) -> Iterator[dict]:
        """Filtered row iteration. Filters support equality, range, and IN operators.
        To target a specific partition, include the partition key fields in filters —
        DuckDB will apply Hive partition pruning; VAST DB will apply predicate pushdown."""

    def bulk_read(
        self,
        table: str,
        filters: dict | None = None,
    ) -> DataFrame:
        """Read rows matching filters as a DataFrame or Arrow table.
        Use for bulk retrieval (e.g., all features for a sample/run).
        Callers should provide filters covering the partition key fields to
        ensure efficient execution (partition pruning for DuckDB, predicate
        pushdown for VAST DB)."""

    def distinct_values(
        self,
        table: str,
        fields: list[str],
        filters: dict | None = None,
    ) -> list[dict]:
        """Return distinct combinations of the specified field values, optionally
        filtered. Use to discover what partitions or groupings exist.
        For DuckDB+Parquet: when fields match the registered partition_by keys,
        introspects the Hive directory structure directly rather than scanning data.
        For VAST DB: executes SELECT DISTINCT on the specified fields."""

    def count(
        self,
        table: str,
        filters: dict | None = None,
    ) -> int:
        """Row count without materializing results."""

    def join(
        self,
        left: str,
        right: str,
        on: str,
        left_filters: dict | None = None,
        right_filters: dict | None = None,
        select: Literal["left", "right", "both"] = "both",
    ) -> Iterator[dict]:
        """Join two tables on a shared key column and return filtered rows.
        Intended for cross-index queries (e.g., geolocation_index JOIN images).
        Both tables must exist in the same ColumnarStore instance.
        For DuckDB: executes as a native SQL JOIN across two Parquet datasets.
        For VAST DB: executes as a server-side SQL JOIN.
        Cross-backend joins (e.g., left in DuckDB, right in VAST DB) are not
        supported — use ETL (read from one store, write to another) for that case.
        select="right" returns only columns from the right table (useful when
        geolocation_index is the filter and images is the payload)."""
```

Schema enforcement is client-side for DuckDB+Parquet (validated before writing Parquet) and server-side for VAST DB (enforced by the database engine). The `create_table` call is idempotent — safe to call at service startup.

**Partition key consistency is mandatory.** For DuckDB+Parquet, partition key column values are encoded in the Hive directory path (`instrument=IFCB107/year=2024/day=20240101/`). If different `write()` calls produce inconsistent partition key shapes for the same table — e.g., some records have `{"year", "day"}` and others have `{"year", "month"}` — DuckDB cannot reconcile the mixed directory structure into a coherent table. The `partition_by` argument to `create_table` documents the required field set, and `write()` validates that all records include exactly those fields. Treat `partition_by` as permanent at design time.

### Filter Syntax (Sketch)

```python
filters = {
    "image_id": "abc123",                          # equality
    "timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"},  # range
    "kind": {"in": ["blob", "features"]},          # set membership
    "lat": {"gte": 40.0, "lte": 45.0},
}
```

---

## Partitioning Strategy

Partition by **instrument + year + month**. This is the right granularity because:

- Sample-level partitions are too small — IFCB samples have at most ~10K ROIs, Stingray samples a handful to ~100. Millions of tiny Parquet files create excessive overhead and slow DuckDB query planning.
- Day-level partitions are finer than needed and create unnecessary file proliferation at IFCB scale (~tens of thousands of ROIs/day is manageable per-month).
- Month-level partitions hit the sweet spot for this data: manageable file sizes, good temporal locality, and point lookups stay efficient because image IDs encode timestamps — the month is always recoverable from the image_id to target the right partition directly via filters. For expedition-based instruments (e.g., Stingray, deployed for hours during cruises), months without a cruise simply have no partition files — sparse deployment patterns are handled naturally with no overhead.

For derived images (e.g., Stingray YOLO ROIs) where monthly volume is very high, adding `run_id` to the partition key may be warranted.

**Partition key fields are real columns in the schema.** `instrument`, `year`, `month` (and `run_id` where applicable) are stored as ordinary data columns alongside `image_id`, `timestamp`, and other fields — not only encoded in file paths or managed implicitly. This is what makes the API backend-agnostic: DuckDB constructs Hive paths from column values at write time, and both DuckDB and VAST DB can filter on these fields efficiently at read time.

`year` and `month` are derivable from `timestamp` and are denormalized into the record to enable efficient partition pruning without timestamp parsing in the storage layer. Clients are responsible for populating these fields before calling `write()` — see [Client-Side Partition Key Enrichment](#client-side-partition-key-enrichment) below.

For DuckDB+Parquet, partitions map to Hive-style directory structure:
- `instrument=IFCB107/year=2024/month=1/`
- `instrument=Stingray/year=2024/month=1/run_id=yolov8-r3/`

For VAST DB, these fields are stored as ordinary columns and VAST handles physical organization internally.

---

## Client-Side Partition Key Enrichment

Clients (e.g., the image service) are responsible for injecting partition key fields into records before calling `write()`. `amplify-db-utils` has no knowledge of instrument ID formats or domain conventions — that logic belongs in the service layer that defines those formats.

In practice this is a thin wrapper around `write()`:

```python
# In the image service — not in amplify-db-utils
def _enrich_records(records: list[dict]) -> list[dict]:
    for r in records:
        ts = datetime.fromisoformat(r["timestamp"])
        r["instrument"] = extract_instrument(r["image_id"])  # image-service logic
        r["year"] = ts.year
        r["month"] = ts.month
    return records

def write_images(store: ColumnarStore, records: list[dict]) -> None:
    store.write("images", _enrich_records(records))
```

`year` and `month` are derived from `timestamp` rather than by parsing `image_id`, which is more robust and keeps the enrichment logic from depending on instrument-specific ID formats. Only `instrument` extraction requires ID parsing, and the image service owns that convention.

This approach also handles cross-partition batches naturally: a batch of records spanning multiple months is written in a single `write()` call. DuckDB groups records by their partition key column values and routes each group to the correct Hive path without the caller pre-splitting anything.

---

## Schema Evolution

**Pre-promote rather than evolve.** The cost of retroactive schema changes at scale (billions of rows, years of Parquet partitions) is high. The right approach is to define index schemas fully before data flows and never need to evolve them. See the pre-promotion pattern in the image service design doc.

When evolution is unavoidable, `create_table()` on an existing table performs a compatibility check:

| Change | Policy |
|--------|--------|
| Add nullable column | ✅ Allowed — DuckDB auto-merges across old and new Parquet files; old partitions return NULL for the new column |
| Remove column | ❌ Rejected — requires explicit migration |
| Rename column | ❌ Rejected — breaking change; DuckDB sees two separate columns |
| Change column type | ❌ Rejected — breaking change |
| Change `partition_by` | ❌ Rejected — partition keys are immutable once set; changing them requires rewriting all historical partitions |

Only additive changes (new nullable columns) are handled automatically. Everything else raises a descriptive error and requires explicit migration tooling. Partition key changes are the most expensive case — treat `partition_by` as permanent at design time.

For backfill jobs (adding a new column to historical data): write new Parquet files into existing partitions with the full updated schema. DuckDB merges on read. Backfills should be partition-parallel and resumable — the `write()` API should support idempotent overwrite of a partition for this purpose.

---

## Implementations

### DuckDB + Parquet (Baseline)
- Intended for small-scale deployments: local development, laptops, embedded use. Not for concurrent multi-process ingest (e.g., SLURM parallel jobs). VAST DB is the right backend for production-scale concurrent access.
- **Onshore sync pattern:** A cruise-deployed laptop writes to DuckDB+Parquet during a deployment. On return, a batch ETL job reads from the laptop store via `bulk_read()` and writes to the production VAST DB instance via `write()`. This is a straightforward read-from-one, write-to-the-other workflow — no cross-backend join is required.
- DuckDB reads and writes Parquet directly to S3/VAST S3 via its native `httpfs` extension — not via `amplify-storage-utils`
- `ColumnarStore` initialization configures DuckDB's S3 settings (endpoint, credentials) from the shared storage config; `amplify-storage-utils` and `amplify-db-utils` are parallel abstractions over the same backend, not a stack
- DuckDB executes queries locally with predicate pushdown and Hive partition pruning over remote Parquet files
- At write time, groups records by partition key column values and writes each group to the corresponding Hive path
- No server required; works with any S3-compatible store
- Excellent for bulk analytical reads; adequate for point lookups with proper file organization

### VAST DB
- Drop-in performance upgrade when VAST infrastructure is available
- Server-side filtering and aggregation; better for concurrent access
- Accessed via VAST DB's SQL or Arrow interface
- Partitioning is managed internally by VAST; `partition_by` from `create_table` is recorded as metadata but has no effect on physical storage layout
- Not a hard dependency — the image service never references VAST DB directly

---

## Relationship to Other Projects

| Project | Relationship |
|---------|-------------|
| `amplify-storage-utils` | Analogous abstraction for the object store layer; parallel sibling to `amplify-db-utils`, not a dependency — they share storage config but operate independently |
| Generalized image service | Primary consumer; should have no direct DB implementation dependency |
| IFCB REST API | Near-term consumer; may use directly before generalized service exists |

---

## Concrete Examples

### Example 1: IFCB Image-Level Metadata

IFCB produces billions of ROIs. Each ROI needs image_id, instrument, and timestamp. These cannot live as rows in PostgreSQL at this scale. Geolocation (lat/lon/depth) is not in the image record — it flows from sample metadata via a `geolocation` provenance record written separately.

**Table:** `images`, partitioned by instrument + year + month. Schema is `ImageRecord` — a single schema defined by the generalized image service, used for all instruments. `instrument`, `year`, and `month` are columns in `ImageRecord`.

```python
# At service startup — idempotent
store.create_table("images", schema=ImageRecord, partition_by=["instrument", "year", "month"])

# Written by an ingest workflow after processing a sample bin.
# The image service enriches records with partition key fields before writing.
store.write("images", [
    {
        "image_id":   "D20240101T120000_IFCB107_00001",
        "timestamp":  "2024-01-01T12:00:00Z",
        "instrument": "IFCB107",   # partition key — derived from image_id
        "year":       2024,         # partition key — derived from timestamp
        "month":      1,            # partition key — derived from timestamp
        # no sample_id or roi_number — these are recoverable from image_id
        # no lat/lon/depth — geolocation is a separate provenance kind
    },
    # ... all ROIs from this sample bin; may span multiple months without issue
])

# Bulk read: all images from a sample — expressed as a time-range query using
# sample bounds from the OLTP store (sample_id is not a columnar filter)
sample = postgres.get_sample("D20240101T120000_IFCB107")  # → time_start, time_end
df = store.bulk_read("images", filters={
    "instrument": "IFCB107",
    "timestamp":  {"gte": sample.time_start, "lte": sample.time_end},
})

# Temporal range query — directly on images table
results = store.read("images", filters={
    "timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"},
})

# Spatiotemporal bounding box — spatial filter goes through geolocation_index
geo_ids = store.read("geolocation_index", filters={
    "timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"},
    "lat":       {"gte": 40.0, "lte": 42.0},
    "lon":       {"gte": -71.0, "lte": -70.0},
})

# Partition discovery: what instrument/year/month combinations exist?
partitions = store.distinct_values("images", fields=["instrument", "year", "month"])
```

IFCB's implicit segmentation run is baked into the image_id — no run_id needed in the images table because there is always exactly one run per sample.

---

### Example 2: Stingray Image-Level Metadata

Stingray acquires 14 full frames/second. YOLO segmentation runs produce derived ROIs. These are two distinct tables because full frames and derived images have different identities and write patterns.

**Table 1:** `frames` — full acquisition images, written at ingest time.

```python
store.write("frames", [
    {
        "image_id":   "STR_20240101T120000_00001",
        "timestamp":  "2024-01-01T12:00:00.071Z",
        "instrument": "Stingray",  # partition key
        "year":       2024,         # partition key
        "month":      1,            # partition key
        # no lat/lon/depth — geolocation computed from nav track, written as provenance
    },
    # ... all frames from this sample
])
```

**Table 2:** `derived_images` — YOLO segmentation outputs, written per run per sample. Partitioned by run_id (so each run's output is a discrete, immutable partition).

```python
store.write("derived_images", [
    {
        "image_id":        "STR_20240101T120000_00001_yolov8-r3_0",
        "parent_image_id": "STR_20240101T120000_00001",
        "run_id":          "yolov8-r3",
        "roi_index":       0,
        "bbox":            {"x": 120, "y": 45, "w": 80, "h": 60},
        "timestamp":       "2024-01-01T12:00:00.071Z",  # inherited from parent
        "instrument":      "Stingray",  # partition key
        "year":            2024,         # partition key
        "month":           1,            # partition key
        # no lat/lon/depth — geolocation inherited from parent frame via geolocation_index
    },
    # ... all derived ROIs from this frame/run
])

# Query: all derived images from a time window using run Y
# (time window obtained from OLTP sample record: time_start, time_end)
df = store.bulk_read("derived_images", filters={
    "instrument": "Stingray",
    "year":       2024,
    "month":      1,
    "run_id":     "yolov8-r3",
    "timestamp":  {"gte": "2024-01-01T12:00:00", "lte": "2024-01-01T14:00:00"},
})
```

The run_id column in the partition key means new YOLO runs add new partitions without touching existing ones — immutable per run, as required.

---

### Example 3: Classification Index ("ROIs classified as X by model Y")

The provenance store doesn't support queries on payload content. For classification queries, a batch job produces a purpose-built index by reading `machine_annotation` provenance records and flattening class scores into queryable rows.

**Table:** `classification_index`, partitioned by instrument + model_version + year + month. Note `month` rather than `day` — classifier runs operate on larger time windows, so month-level partitions are the right granularity here.

```python
# Written by a batch indexer after a classifier run.
# instrument, year, month are derived from image_id/timestamp and injected by the indexer.
store.write("classification_index", [
    {
        "image_id":      "D20240101T120000_IFCB107_00001",
        "run_id":        "ecotaxa-cnn-v4_20240115",
        "model_version": "ecotaxa-cnn-v4",
        "class":         "Ceratium",
        "score":         0.92,
        "is_winner":     True,
        "instrument":    "IFCB107",         # partition key
        "year":          2024,               # partition key
        "month":         1,                  # partition key
    },
    {
        "image_id":      "D20240101T120000_IFCB107_00001",
        "run_id":        "ecotaxa-cnn-v4_20240115",
        "model_version": "ecotaxa-cnn-v4",
        "class":         "Dinoflagellate",
        "score":         0.05,
        "is_winner":     False,
        "instrument":    "IFCB107",
        "year":          2024,
        "month":         1,
    },
    # ... one row per class per image
])

# Query: all ROIs classified as Ceratium by model ecotaxa-cnn-v4
results = store.read("classification_index", filters={
    "model_version": "ecotaxa-cnn-v4",
    "class":         "Ceratium",
    "is_winner":     True,
})
```

This index is not part of the core image service — it is produced by a batch workflow and consumed by higher-order clients (e.g., the IFCB products dashboard, abundance time series pipelines). Multiple indexes can be built from the same provenance records for different query shapes.

---

## Open Questions

### Decided

- **VAST DB predicate pushdown.** VAST DB supports predicate pushdown on reads, applying filters at the storage layer. No special handling needed in the API — the existing `read()` and `bulk_read()` filter mechanism works as designed on both backends.

- **Cross-table join.** Both DuckDB and VAST DB support native SQL JOINs within the same instance. `join()` is a first-class API operation (see API surface above) that translates to a native SQL JOIN on each backend. All tables in a deployment live in the same store instance — there is no cross-instance join case. Cross-backend access (DuckDB ↔ VAST DB) is an ETL-only pattern: a batch workflow reads from one store and writes to the other as discrete operations. The canonical example is a cruise-deployed laptop running DuckDB+Parquet onshore sync: records are bulk-read from the laptop store and appended to VAST DB. No join across backends is needed.

- **Provenance `data` column representation.** *(Decided: JSON blob.)* The `data` column is never queried directly — any field that needs to be queryable is pre-promoted to an index table. The provenance table is accessed in two patterns only: "fetch all provenance for one image" (annotation tool, point lookup) and "scan all provenance records for a data product" (batch analysis). Neither requires column-level query into `data`. Store as a JSON blob. This is simple, human-readable in debugging, and doesn't require the columnar store to understand payload schemas.

- **Write idempotency.** `write(..., overwrite=True)` replaces all existing rows for each distinct partition key combination present in the records. Scope is inferred from the records — no separate partition argument. Records may span multiple partitions; each is replaced independently. For DuckDB+Parquet: atomic directory swap per affected partition. For VAST DB: delete-then-insert per partition key value set. See `write()` docstring in the API surface above.

- **Geolocation index ownership.** *(Decided: service-owned.)* `geolocation_index` is registered by the image service at startup alongside `images` and `provenance`. See Index Table Ownership in the image service design doc.

- **`ImageRecord` schema.** *(Decided.)* `sample_id` is not a column in `ImageRecord`. Bulk multi-sample queries go through a service-owned `sample_index` table pre-promoted from `sample_context` provenance — producers dual-write a provenance record and a `sample_index` row at the same time (or later, once sample IDs are assigned). An image may have multiple `sample_index` rows (different naming schemes, different sources). Queries across a list of sample IDs use `store.join(left="sample_index", right="images", on="image_id", left_filters={"sample_id": {"in": [...]}})`. `ImageRecord` is therefore `{image_id, instrument, timestamp, year, month}` — genuinely instrument-agnostic. `roi_number` is encoded in the `image_id` and recoverable without a separate column.

### Lower Priority

- What DataFrame/Arrow interchange format should `bulk_read` return? (pandas DataFrame, PyArrow Table, or both via a protocol?)
- Should `write` accept Arrow tables directly for zero-copy bulk ingest?
- Partition granularity: year/month works for both continuous and expedition-based instruments. Expedition instruments (e.g., Stingray) are deployed for hours during cruises; months without a cruise simply have no partition files. No cruise-based alternative needed.
- For `distinct_values`, when fields match the registered `partition_by` keys, DuckDB can introspect the Hive directory structure instead of scanning data — is this optimization worth implementing, or is a `SELECT DISTINCT` fast enough at expected scale?
