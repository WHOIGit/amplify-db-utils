# Generalized Image Service — Design Notes

*Originated from AMPLIfy planning discussion, 2026-03-05*

---

## What This Is

A substrate for tying together the outputs of image acquisition, batch processing workflows, and on-demand data access. Designed to be instrument-agnostic. The IFCB REST API is a near-term precursor; this is the longer-term generalization.

A different codebase than `ifcb-rest-api`, which has IFCB-specific assumptions baked in.

---

## Core Data Model

Every image has:

```
image_id       → globally unique; assigned at storage time for all image types
instrument     → instrument identity (first-class field; partitioning key)
timestamp      → acquisition time (first-class field; see below)
provenance[]   → append-only log of value-add records
```

Only `image_id`, `instrument`, and `timestamp` are in the envelope. Spatial coordinates (lat/lon/depth) are **not** first-class fields — they are a provenance record of kind `geolocation` (see Well-Known Kinds). This is the right model because coordinate assignment is a processing step, not an inherent property of the image: for Stingray, geolocation is computed from a nav track interpolation run that can be versioned and re-run; for IFCB, coordinates come from sample-level metadata. In both cases the result is supersedable, which makes it structurally identical to a segmentation run rather than a correctable field. Spatial queries are served by pre-promoting geolocation records into an index table.

### Timestamp Discipline

`timestamp` stays in the envelope as a practical necessity: it is the primary partitioning axis for columnar storage and the organizing dimension for dataset membership. Without a correct timestamp in the envelope, time-range queries and dataset span resolution are meaningless — a misconfigured clock can place images in the future or at epoch, making them invisible to or disruptive of any span-based query.

Because of this load-bearing role, timestamps must be **validated and bolted down before ETL writes the db-utils record**. The ingest pipeline must include an explicit timestamp validation gate:

- Plausibility checks (not in the future, not before instrument was deployed, etc.)
- For instruments with known clock issues, correction is applied at this stage — not post-hoc
- The bolted timestamp is then treated as immutable in the columnar store; it is the partition key and cannot be changed without rewriting the row

For instruments where timestamp itself must be computed from ancillary data, that computation is part of pre-ETL processing. ETL does not run until a validated timestamp is in hand.

This is a stronger constraint than applies to geolocation — spatial coordinates can be absent or revised post-ETL (via a new geolocation provenance record); timestamps cannot.

---

## Provenance Records

The provenance log is append-only. Records are never edited or deleted. Each record has a mandatory envelope plus an arbitrary JSON payload:

```json
{
  "image_id": "...",
  "kind": "...",
  "source": "...",
  "timestamp": "...",
  "data": { "...arbitrary payload..." }
}
```

The service stores, retrieves, and indexes on envelope fields. It does not interpret payloads. Higher-order clients (feature pipelines, classifiers, annotation tools, sensor integrations) own their schemas for `data`.

### Well-Known Kinds

| Kind | What | Notes |
|------|------|-------|
| `geolocation` | lat, lon, depth | Computed from ancillary data (nav track, sample metadata); versioned and re-runnable; pre-promoted to index table for spatial queries |
| `blob` | Segmentation mask | Derived from image |
| `features` | Morphometric scalars | Derived from blob |
| `machine_annotation` | Classifier score distribution across all classes | Probabilistic; produced by a classifier run |
| `human_annotation` | Discrete label choice with annotator identity and region | Produced by a person via an annotation tool |
| `sample_context` | External sample identifiers | CTD cast, Niskin bottle, cruise sample code, etc. Multiple records possible per image (different naming schemes from different sources). Not to be confused with the OLTP sample entity, which holds system-internal time bounds. |
| `oceanographic_context` | External sensor/model data | See below |

`kind` values should be governed — not a free-for-all. The above are the initial well-known set; extension should be deliberate.

`machine_annotation` and `human_annotation` are kept as distinct kinds because their payloads are genuinely different data types (score distribution vs. discrete label), their query patterns differ, and their provenance semantics differ. They share the region descriptor schema.

### Binary vs. Scalar Products

Well-known kinds divide into two fundamentally different payload types, which determines where the data lives and how it is retrieved:

**Binary/image products** (`blob`, thumbnails) are files. They are stored and retrieved via `amplify-storage-utils`, which abstracts the backend (filesystem, VAST S3, S3) behind a single object key. The provenance record payload is a **pointer** — it contains the object key (and optionally checksum, format, dimensions) rather than the data itself. Fetching the product requires two hops: look up the provenance record to get the object key, then call `storage.get(key)`.

**Scalar/tabular products** (`features`, `machine_annotation` scores) are structured records. They live in db-utils index tables via pre-promotion. The provenance record or index record **is** the data, not a pointer.

This distinction is load-bearing: `amplify-storage-utils` is in the retrieval path for all binary products; scalar products do not require it.

**Deterministic object keys (recommended for binary products).** If the object key for a binary product is derivable from the image_id, the provenance lookup can be skipped — the service constructs the key directly and calls `storage.get(key)`. The provenance record still exists as an audit trail but is not in the retrieval path. The key is a storage-utils object key, not a raw S3 URL; where the data physically lives is storage-utils configuration, not a naming convention baked into the key itself. This is consistent with the system's overall reliance on deterministic identifiers and keeps backend configuration centralized in storage-utils rather than scattered across code.

### Schema Management

The service itself is schema-agnostic — it stores and retrieves envelopes without validating payloads. Payload schemas for well-known kinds are managed in a **shared Python library** (Pydantic models), maintained alongside or as part of the image service. Producers import and validate against these models before writing. This keeps enforcement in the client layer while providing a canonical, versioned schema reference.

Extension kinds (novel or experimental) are unstructured by convention — documented but not formally enforced until patterns stabilize and they graduate to well-known status.

### Oceanographic Context Records

Environmental metadata (salinity, temperature, SST, etc.) comes from sources external to image acquisition — different sensors, different instruments, model-derived data. Each source has its own provenance. The image service does not manage that provenance; it provides slots for multiple named context records per image (or per sample, for instruments with discrete samples).

There may be multiple overlapping context records per image (e.g., CTD-derived hydrography and MODIS SST interpolation). Consumers decide which to use.

---

## Time as the Fundamental Organizing Dimension

The service's primary organizing dimensions are **instrument** and **time**. All images have a timestamp and an instrument identity. Dataset definitions, collection membership, and query interfaces are built around `(instrument, time_start, time_end)` spans as the universal primitive.

### Samples

Some instruments have a natural concept of a **sample** — a discrete acquisition event with a clear start and end. IFCB is the canonical example: a sample ("bin") is a bounded physical event where the instrument pumps a discrete volume of water and images the particles. All images from a sample share sample-level metadata (lat/lon, depth, time, instrument state). Samples are scientifically meaningful units for these instruments, and the service exposes them as first-class entities where they exist.

Other instruments have no natural sample boundary. Stingray, for example, acquires continuous shadowgraph video at ~14 frames/second while towed through the water. Each frame images a roughly 2D focal volume, and an organism only rarely appears in consecutive frames. There is no scientifically meaningful boundary where one "sample" ends and another begins — time and position are the only organizing dimensions.

**"Sample" is therefore an instrument-specific concept, not a universal one.** The service supports samples for instruments that define them but does not require them. The general query interface is time-range + instrument; sample-addressed queries are a convenience layer for instruments where samples are real.

Sample-level metadata belongs to the sample entity, not to individual images. Image-level acquisition coordinates are populated from sample-level data at ingest where a sample exists. For continuous instruments, acquisition coordinates are per-image (interpolated from instrument telemetry).

---

## The ROI / Region Question

Most ROIs across imaging instruments are derived from full frames. IFCB is the exception — it performs ROI extraction at acquisition time and discards the full frame.

**Key insight:** If the service is generalized to "images" rather than "ROIs," the distinction collapses into the annotation data model:

- **IFCB ROI:** the image *is* the organism crop; annotated region = full frame
- **Other imagers:** full frame is the image; annotated region = bounding box or mask within the frame

The image service does not need to know about this distinction. Annotation records carry a region descriptor:

```json
{
  "kind": "human_annotation",
  "data": {
    "region": { "type": "full_frame" },
    "label": "...",
    "annotator": "...",
    ...
  }
}
```

or

```json
{
  "data": {
    "region": { "type": "bbox", "x": 120, "y": 45, "w": 80, "h": 60 },
    ...
  }
}
```

This unifies the LabelStudio (full-frame bounding boxes/masks) and Photic (IFCB bulk ROI annotation) use cases — both are annotation writers to the same service, differing only in region descriptor.

### Segmentation Runs and Image Identity

Every image in the system is the output of a segmentation run. The distinction between "acquisition images" and "derived images" is not a fundamental data model distinction — it's a spectrum:

- **IFCB:** segmentation happens at acquisition time, is hardware-defined, and there is exactly one implicit run per sample. The run_id is trivial (effectively "acquisition"). IFCB ROI IDs are deterministic from `{sample_id}_{roi_number}` with no ambiguity.
- **Stingray / YOLO:** segmentation is an explicit post-processing step with a model version and run_id. Multiple runs can produce different images from the same full frame. Image IDs are deterministic *within a run*: `{frame_timestamp}_{run_id}_{roi_index}`. There is no sample_id — the parent frame's timestamp is the anchor.

The data model does not special-case IFCB. All images have a segmentation provenance record; for IFCB it is implicit and trivial.

**Querying images by run.** For discrete instruments, "images from sample X" scopes to a segmentation run:

```
GET /samples/{sample_id}/images?segmentation_run={run_id}
```

For continuous instruments, the equivalent is a time-range query scoped to a run:

```
GET /images?instrument={instrument}&time_start={ts}&time_end={te}&segmentation_run={run_id}
```

**Caching implication.** Image lists per segmentation run are immutable — once a run completes, its output never changes. IFCB samples have exactly one implicit run; Stingray frames may accumulate multiple runs over time. Cache keys should include run_id.

---

## API Shape (Sketch)

Flat, image-addressed:

```
GET  /images/{image_id}                    → image record (image_id, timestamp, resolved geolocation if available)
GET  /images/{image_id}/provenance         → full provenance log
GET  /images/{image_id}/provenance/{kind}  → records of a specific kind
POST /images/{image_id}/provenance         → append a provenance record
```

Query/discovery layer (returns image IDs matching criteria). Time-range + instrument is the general-purpose interface:

```
GET  /images?instrument={instrument}&time_start={ts}&time_end={te}
GET  /images?instrument={instrument}&time_start={ts}&time_end={te}&lat_min=...&lat_max=...
GET  /images?collection={collection_name}
GET  /images?kind={kind}&source={source}   → "what images have features from pipeline X?"
```

Sample-level (for instruments with discrete samples):

```
GET  /samples/{sample_id}                  → sample metadata (timestamp, geolocation, oceanographic context)
GET  /samples/{sample_id}/images           → images in this sample
```

These are convenience endpoints backed by time-range queries to the columnar store. For IFCB, `sample_id` resolves to a narrow time range covering the bin's acquisition window. Instruments without discrete samples do not use these endpoints.

---

## OLTP Backing Store

The image service requires a relational store (PostgreSQL) for organizing metadata that is mutable, relational, and small relative to the bulk image data.

### What lives in the OLTP store

| Entity | Description |
|--------|-------------|
| Datasets / collections | User-defined groupings of images (e.g., "MVCO 2020–2024", "NES-LTER cruise EN688") |
| Dataset spans | `(instrument, time_start, time_end)` ranges that define dataset membership |
| Sample metadata | For instruments with discrete samples: system-internal sample_id, time_start, time_end, instrument state. time_start/time_end are required — sample-scoped columnar queries use these as time-range filters. External sample identifiers (CTD cast, Niskin bottle, cruise codes, etc.) are provenance records of kind `sample_context`, not columns here. |
| Instrument registry | Instrument identity, type, configuration, deployment history |

### Dataset definitions as time spans

Datasets are defined as sets of `(instrument, time_start, time_end)` spans. This representation works for both discrete and continuous instruments:

- An IFCB dataset covering a deployment is a span: `(IFCB107, 2020-01-01, 2025-12-31)`
- A cruise dataset is a span per instrument: `(Stingray, 2024-06-15T08:00, 2024-06-15T18:30)`
- A multi-instrument dataset is multiple spans

Resolving "all images in dataset X" translates dataset spans into partition-efficient time-range filters against the columnar store. No sample-level membership table is required — membership is derived from the spans.

For instruments with discrete samples, sample-level navigation within a dataset is available: resolve the dataset's spans, then query the sample metadata table for samples falling within those spans.

### Boundary between OLTP and OLAP

The OLTP store holds organizing metadata; the columnar store (`amplify-db-utils`) holds bulk image records, provenance, and indexes. The boundary is clean:

- **OLTP**: anything mutable or relational — dataset definitions, collection membership, sample metadata, instrument registry
- **OLAP**: anything append-only and high-volume — image records, features, classification indexes, provenance logs

Queries that cross the boundary (e.g., "all images in dataset X classified as Ceratium") resolve the OLTP side first (dataset → time spans), then use the spans as filters for the columnar read.

---

## Indexing Strategy

Indexing is a batch workflow — not a built-in service concern. The service itself stays simple.

### Pre-Promotion (Preferred Pattern)

Schema evolution in columnar stores at scale is expensive — adding a new queryable field to historical data requires a full backfill across potentially billions of rows. The way to avoid this is **pre-promotion**: decide upfront which fields from a well-known kind's payload will be queryable, define the index table schema before any data flows, and have producers dual-write from day one.

The pattern for a new well-known kind:
1. Define the Pydantic model for the kind, including all fields expected to be queryable
2. Define the index table schema and call `create_table()` before any data flows
3. Producers always write both: a provenance record to the image service, and an index record to db-utils, in the same operation

The image service remains schema-agnostic throughout — pre-promotion is a **producer-side discipline**, not a service-side mechanism. The shared Pydantic library is where the decision is encoded; the same model validates both writes.

The batch indexer pattern (reading historical provenance records to build or rebuild an index) is reserved for retroactive work: indexing data that predates an index, or recovering from an index that was not set up from the start. It is not the primary path.

When in doubt, pre-promote. The cost of deciding a field is queryable before it has data is zero. The cost of promoting it retroactively at 1.5B rows is not.

### Index Table Ownership

There are two categories of db-utils table, with different ownership:

**Service-owned tables** (`images`, `provenance`, `geolocation_index`, `sample_index`) are registered by the image service at startup via `create_table()`. These are the service's own schema — clients do not create or modify them. `geolocation_index` is service-owned because geolocation is exposed as a first-class query parameter on the service's own API (`?lat_min=...&lat_max=...`). `sample_index` is service-owned for the same reason: sample-scoped queries (`GET /images?sample=...`) are a first-class access pattern, requiring the service to know the table exists and its schema. `sample_index` is a pre-promoted index from `sample_context` provenance — producers dual-write a `sample_context` provenance record and a corresponding `sample_index` row. An image may have multiple rows in `sample_index` (one per naming scheme or source), enabling both the multi-ID case and late assignment of sample identifiers without modifying image records.

**Producer-owned index tables** (`classification_index`, `features_index`, etc.) are registered and managed by the producer that writes them. The image service is schema-agnostic and has no knowledge of index tables. A classifier pipeline calls `create_table("classification_index", ...)` at its own startup; ifcb-features calls `create_table("features_index", ...)` at its startup. Since `create_table()` is idempotent, producers call it unconditionally at startup without checking whether the table already exists.

Index schema definitions live in the **shared Pydantic library**, co-located with the well-known kind models they derive from — `ClassificationIndexRecord` lives alongside `MachineAnnotationRecord` because the index fields are promoted from the payload. There is no central registry of index tables; ownership is federated, with each producer responsible for its own. The db-utils layer enforces consistency once a table exists but has no opinion about what tables should exist.

### What to cache / index

| Data | Mutability | Strategy |
|------|------------|----------|
| Image list per segmentation run | Immutable per run | Cache per run_id; never invalidated |
| Provenance records | Append-only | Cache aggressively; invalidate on append |
| Instrument | Immutable | Cache indefinitely; partition key |
| Timestamp | Immutable once bolted (pre-ETL) | Cache indefinitely; partition key |
| Geolocation | Append-only (new record supersedes) | Cache per source+version; invalidate on new record; latest record is authoritative |
| Oceanographic context records | Append-only (multiple sources) | Cache per source; invalidate on new record |
| Dataset/collection membership | Mutable (user-managed) | OLTP store (PostgreSQL); short TTL or no cache |
| Sample metadata | Immutable once written | OLTP store; cache indefinitely; only applicable for discrete instruments |

### Bulk access
Avoid N individual lookups. For bulk retrieval (e.g., all features for a sample), bulk reads go through `amplify-db-utils` (see `amplify-db-utils-design.md`), which handles columnar storage and partitioned access. Bulk access patterns should be explicit endpoints, not N individual calls.

---

## Relationship to Existing Projects

| Project | Relationship |
|---------|-------------|
| `ifcb-rest-api` | Near-term precursor; IFCB-specific; may evolve toward or be superseded by this |
| `amplify-db-utils` | Storage layer for bulk/columnar access (images, features, classification index); see `amplify-db-utils-design.md` |
| `amplify-storage-utils` | Object store abstraction (S3/VAST S3/filesystem) for blob retrieval |
| ifcb-features | Batch writer of `blob` and `features` provenance records |
| Classifier pipelines | Batch writers of `machine_annotation` records |
| Photic | Writer of `human_annotation` records (IFCB full-frame region) |
| LabelStudio | Writer of `human_annotation` records (bbox/mask region) |
| IFCB products dashboard | Consumer of the access layer |
| S3 migration | S3 key structure should align with image_id addressing |
