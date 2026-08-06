"""Configuration for DuckDBParquetStore."""

from dataclasses import dataclass, field


@dataclass
class DuckDBParquetConfig:
    """Configuration for a DuckDB+Parquet columnar store.

    Args:
        root: Root path for the store. Either a local filesystem path
            (e.g., "/data/ifcb") or an S3 URL (e.g., "s3://bucket/prefix").
        s3_endpoint: S3-compatible endpoint override, e.g., "vast-s3.example.org:9000".
            Required when root is an s3:// URL pointing to a non-AWS endpoint.
        s3_access_key: S3 access key ID. Required when using S3.
        s3_secret_key: S3 secret access key. Required when using S3.
        s3_use_ssl: Whether to use HTTPS for S3 connections. Default True.
        threads: DuckDB thread count. None uses DuckDB's default (all available cores).
    """

    root: str
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None
    s3_use_ssl: bool = True
    threads: int | None = None

    def __post_init__(self) -> None:
        if not self.root:
            raise ValueError("root must be a non-empty path or s3:// URL")
        self.root = self.root.rstrip("/")
