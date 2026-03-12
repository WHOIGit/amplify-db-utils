# AMPLIfy Design Docs

Design documentation for the AMPLIfy platform components.

---

## Generalized Image Service — Executive Summary

The generalized image service is a shared data platform for scientific imaging instruments. It provides a single place to store, organize, and query images and their associated scientific products across instruments, deployments, and processing pipelines — regardless of instrument type or scale.

### What problem does it solve?

Imaging instruments accumulate vast numbers of images — millions to billions of ROIs over the lifetime of a deployment. Those images are only scientifically useful when paired with their context: where and when they were acquired, how they were segmented, what a classifier or an expert said about them, what the surrounding oceanographic conditions were. That context accumulates from multiple sources at different times, often years after acquisition.

Without a common substrate, each instrument or pipeline ends up with its own bespoke storage conventions, query tools, and integration patterns. Combining data across instruments, re-running classifiers on historical images, or building dashboards that span multiple deployments requires custom one-off engineering for every new combination.

The generalized image service provides that substrate: a single access layer where images from any instrument live alongside their scientific products, queryable in consistent ways regardless of origin.

### What it stores

Every image in the system has an identifier, an instrument, and an acquisition timestamp. Beyond that, the service stores an open-ended **provenance log** attached to each image — an append-only record of every value-adding operation ever performed on it:

- Geolocation (computed from nav tracks, sample metadata, or other ancillary data)
- Segmentation blobs and morphometric features
- Machine annotation scores from classifier runs
- Human annotations from expert reviewers
- Sample context (CTD cast IDs, Niskin bottle numbers, cruise codes)
- Oceanographic context (temperature, salinity, SST from sensors or models)

Provenance records are never deleted or edited. New records accumulate over time. A classifier can be re-run years later and its outputs attach to the same images alongside the original run.

### What you can query

- **By time and instrument** — retrieve all images from an instrument within a time window; the general-purpose access pattern for both continuous and sample-based instruments
- **By location** — filter by bounding box (lat/lon/depth); backed by a pre-built spatial index
- **By collection or dataset** — named groupings defined by time spans (e.g., "MVCO 2020–2024", "cruise EN688"); membership is derived automatically from the spans, no per-image tagging required
- **By sample** — for instruments with discrete sampling events (e.g., IFCB), navigate images by sample ID; instruments without discrete samples use time-range queries instead
- **By provenance kind or source** — "what images have features from pipeline X?" or "what images have human annotations from annotator Y?"
- **By classification** — batch-built classification indexes enable fast lookup of all images a given model called as class Z above a threshold

### What it is not

The service is not a general-purpose image database or a replacement for domain-specific tools. It is a data substrate — a layer that producers (ingest pipelines, classifiers, annotation tools) write into and consumers (dashboards, analysis pipelines, REST APIs) read from. It does not perform segmentation, run classifiers, or manage annotation workflows; those tools write their outputs into the provenance log.

### Who it is for

Any group operating scientific imaging instruments that wants to:
- Query across instruments and deployments without custom integration for each combination
- Attach classification and annotation results to images from multiple independent pipelines
- Build dashboards and data products on top of a consistent, queryable record
- Onboard new instruments without modifying existing pipelines

### Scale

The service scales from a single laptop collecting data during a field deployment to a multi-instrument production system ingesting billions of images. The same API and data model work at both ends — a cruise-deployed device writes locally during the deployment and syncs to the central store on return. There is no cloud dependency; the service runs on-premises, on embedded devices, or in a cloud environment.

---

## Documents

| Document | What |
|----------|------|
| [generalized-image-service-design.md](generalized-image-service-design.md) | Full design: data model, provenance, API shape, OLTP/OLAP boundary, indexing strategy |
| [generalized-image-service-data-flows.md](generalized-image-service-data-flows.md) | Data flow descriptions: ingest, provenance, retrieval |
| [amplify-db-utils-design.md](amplify-db-utils-design.md) | Columnar storage abstraction (DuckDB+Parquet / VAST DB) |
| [db-utils-implementation-plan.md](db-utils-implementation-plan.md) | Implementation plan for the DuckDB+Parquet backend |
