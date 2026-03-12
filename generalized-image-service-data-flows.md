# Generalized Image Service — Data Flows

*See also: `generalized-image-service-design.md`, `amplify-db-utils-design.md`*

---

## Overview

Three primary flows govern all data movement through the system:

1. **Image ingest** — acquisition metadata enters the system
2. **Provenance ingest** — value-added records (features, annotations, classifications) are attached to images
3. **Retrieval** — images and their provenance are accessed by consumers

Each flow is described from both the client perspective (producers/consumers) and the backend perspective (service + storage layers).

---

## Flow 1: Image Ingest

Image ingest has two distinct phases that happen at different times.

### Phase 1: Storage (at or near acquisition)

The first job is to get image files into storage-utils. This happens as soon as images are available:

1. Construct deterministic image IDs (`{sample_id}_{roi_number}` for IFCB; frame-addressed IDs for Stingray full frames)
2. Write image files (ROI crops, raw frames) → object store via `amplify-storage-utils`

No db-utils write happens here. The image exists in storage; it does not yet have a record in the columnar store.

For **IFCB**, segmentation happens at acquisition time, so all ROIs from a sample are stored in one shot. For **Stingray**, only full frames are stored here — derived ROIs enter the system later via the provenance flow (see Flow 2).

### Phase 2: ETL (post-run, after timestamp validation)

A separate ETL process runs after Phase 1 and after any ancillary data needed for timestamp validation is available:

1. **Timestamp validation gate** — plausibility checks (not in the future, not before instrument was deployed); corrections for known clock issues are applied here. ETL does not proceed until a validated timestamp is in hand.
2. Construct image metadata records: `image_id`, `instrument`, validated `timestamp`
3. Write metadata records → image service (or direct db-utils write for internal pipelines)

Spatial coordinates are **not** written here — geolocation is a separate provenance record produced by a geolocation pipeline (see Flow 2). For IFCB, the ETL phase is typically coupled tightly to acquisition; for Stingray and other towed instruments, it may run hours later once nav track data is processed.

### Backend (Phase 2)

The image service validates envelope fields and enriches records with partition key fields (`instrument`, `year`, `month`) derived from the `image_id` and `timestamp` before writing to the db-utils `images` table. These fields are stored as ordinary data columns — DuckDB uses them to route writes to the appropriate Hive-partitioned path, and both DuckDB and VAST DB use them for efficient filtering at read time. Because image IDs encode instrument identity and date, downstream lookups can always reconstruct the partition key values from the `image_id` and target the right data without a full table scan.

---

## Flow 2: Provenance Ingest

### Client

A batch pipeline (ifcb-features, a CNN classifier, an annotation tool) runs against a set of images and produces output. The producer:

1. Calls `create_table()` at startup for its index table(s) — idempotent, safe to call unconditionally (see Index Table Ownership in `generalized-image-service-design.md`)
2. Constructs provenance records: envelope (image_id, kind, source, timestamp) + payload validated against the shared Pydantic model for that kind
3. Dual-writes per the pre-promotion pattern:
   - Provenance record → image service (or direct db-utils write)
   - Pre-promoted index fields → purpose-built index table in db-utils

The dual-write is the producer's responsibility — the image service does not trigger or coordinate index writes. The shared Pydantic library defines both the provenance payload schema and the index schema; the same model validates both writes.

**Sample identity assignment** is a special case of this pattern. A pipeline that associates images with sample identifiers (e.g., linking IFCB images to a CTD cast, or assigning a cruise sample code) dual-writes:
- A `sample_context` provenance record (kind: `sample_context`) with the sample identifier in the payload
- A row to the service-owned `sample_index` table (pre-promoted; `sample_id`, `image_id`, `source`)

This write can happen at any time after image ingest — sample IDs are often assigned after the fact. An image may accumulate multiple `sample_index` rows from different sources or naming schemes. The `images` table is never modified.

For **Stingray derived images**, the segmentation run is itself a provenance write — this is when derived ROI records (IDs: `{sample_id}_{run_id}_{roi_index}`) enter the system, written to the `derived_images` table in db-utils.

### Backend

The image service validates provenance envelopes and writes records to the db-utils `provenance` table. This table is also partitioned by instrument + year + month, keeping provenance co-partitioned with images. At the scale of 1.5B IFCB images with multiple provenance records each, provenance has the same scale constraints as image records — PostgreSQL rows are not viable here.

The image service has no knowledge of index tables. Index writes go directly from producer to db-utils, bypassing the service entirely.

---

## Flow 3: Retrieval

Two distinct client types access the system differently, with different performance requirements.

### External / Interactive Consumers

Annotation tools, interactive dashboards, and external API clients go through the REST API:

```
GET  /images/{image_id}                    → core metadata (image_id, instrument, timestamp, resolved geolocation if available)
GET  /images/{image_id}/provenance         → full provenance log for this image
GET  /images/{image_id}/provenance/{kind}  → records of a specific kind
GET  /images?sample={sample_id}            → image discovery by sample
GET  /samples/{sample_id}                  → sample metadata
```

The image_id encodes instrument identity and date, so the service can derive the partition key field values directly from the image_id and target the right Parquet files via filters — no full table scan required.

### Internal / Bulk Consumers

IFCB products dashboards, ML training data pipelines, and abundance time series pipelines bypass the REST API and go to db-utils directly:

```python
# All images from a sample — resolve sample bounds from OLTP, then time-range query
sample = postgres.get_sample("D20240101T120000_IFCB107")  # → time_start, time_end
df = store.bulk_read("images", filters={
    "instrument": "IFCB107",
    "timestamp":  {"gte": sample.time_start, "lte": sample.time_end},
})

# Spatiotemporal bounding box — spatial filter goes through geolocation index,
# not the images table (lat/lon are not image envelope fields)
geo_ids = store.read("geolocation_index", filters={
    "timestamp": {"gte": "2024-01-01", "lt": "2024-02-01"},
    "lat": {"gte": 40.0, "lte": 42.0},
    "lon": {"gte": -71.0, "lte": -70.0},
})
results = store.read("images", filters={"image_id": {"in": [r["image_id"] for r in geo_ids]}})

# All images classified as Ceratium — goes to classification_index directly,
# not to the provenance table
results = store.read("classification_index", filters={
    "model_version": "ecotaxa-cnn-v4",
    "class": "Ceratium",
    "is_winner": True,
})
```

Classification queries go directly to the purpose-built index — no provenance payload parsing required. This is the payoff of pre-promotion.

### Binary Product Retrieval (e.g., IFCB Blobs/Masks)

All binary products are stored and retrieved via `amplify-storage-utils`, which abstracts the backend (filesystem, VAST S3, S3) behind a configured object key. Fetching a binary product is a two-hop operation:

1. Look up the `blob` provenance record for the image_id → get the object key from the payload
2. Call `storage.get(key)` — storage-utils resolves the key to the configured backend

The REST API surfaces this as a single endpoint (`GET /images/{image_id}/blob`) — the two hops are internal to the service.

**Shortcut with deterministic keys.** If the object key for blobs is derivable from the image_id, hop 1 is skipped — the service constructs the key directly and calls `storage.get(key)`. The provenance record still exists as an audit trail. The key is a storage-utils object key, not a raw S3 URL; backend configuration lives in storage-utils, not in the key structure. This is the recommended approach (see image service design doc).

`amplify-storage-utils` must be configured in any service or pipeline that reads or writes binary products. This is the only component that needs to know about the physical storage backend.

Scalar products (`features`, class scores) do not require storage-utils — their data lives in db-utils index tables and is retrieved in a single read.

### Backend

| Query type | Mechanism |
|------------|-----------|
| Point lookup by image_id | Decode image_id → derive partition key field values → filter to targeted Parquet read |
| Range scan (temporal) | DuckDB partition pruning on year/month + predicate pushdown on timestamp |
| Range scan (spatial) | Filter on `geolocation_index` table (lat/lon/depth are not in the images table); join to image_ids |
| Provenance lookup for an image | Filtered scan on `provenance` table by image_id within the known partition |
| Classification query | Direct read on `classification_index` — no provenance table involved |
| Bulk partition read | Single `bulk_read()` call returning full partition as DataFrame/Arrow table |
| Binary product fetch | Construct or resolve object key → `storage.get(key)` via `amplify-storage-utils` (backend is configuration, not convention) |

---

## Open Question: Direct db-utils Write vs. REST API for Internal Producers

Internal batch pipelines (ifcb-features, classifier runs) could either:

- **Write directly to db-utils** — bypasses the HTTP layer; appropriate for high-throughput batch jobs that are internal and trusted
- **Write through the image service REST API** — adds envelope validation at the service layer; appropriate for external or less-trusted producers

Both are valid. The likely pattern: internal pipelines write directly to db-utils (they are the producers and own the schema); external or third-party producers go through the REST API. This distinction should be decided per producer when the service is built out.
