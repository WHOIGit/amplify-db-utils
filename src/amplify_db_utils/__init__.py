"""amplify-db-utils: columnar database abstraction for AMPLIfy image service workflows.

Public API:
    ColumnarStore     — Abstract base class; use as type annotation for injection.
    Filters           — Type alias for filter dicts passed to read/count/join.
    DuckDBParquetConfig — Configuration dataclass for DuckDB+Parquet stores.
    DuckDBParquetStore  — DuckDB+Parquet implementation of ColumnarStore.
"""

from amplify_db_utils.base import ColumnarStore, Filters
from amplify_db_utils.config import DuckDBParquetConfig
from amplify_db_utils.duckdb_parquet import DuckDBParquetStore

__all__ = [
    "ColumnarStore",
    "Filters",
    "DuckDBParquetConfig",
    "DuckDBParquetStore",
]

# Optional VAST DB backend; only available when the `vastdb` extra is installed.
try:
    from amplify_db_utils.vastdb_store import VastDBConfig, VastDBStore
except ImportError:
    pass
else:
    __all__ += ["VastDBConfig", "VastDBStore"]
