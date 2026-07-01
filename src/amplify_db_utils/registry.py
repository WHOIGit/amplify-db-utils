"""Schema registry: persistence of per-table schema metadata."""

from __future__ import annotations

import base64
import json

import pyarrow as pa
import pyarrow.fs as pa_fs


def _schema_to_ipc_b64(schema: pa.Schema) -> str:
    """Serialize a full schema to a base64 Arrow-IPC string.

    Round-trips losslessly for every Arrow type — including nested types like
    ``list``, ``large_list``, ``struct``, and ``map`` — plus field nullability
    and metadata, with no hand-maintained type table to drift behind PyArrow.
    """
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def _schema_from_ipc_b64(s: str) -> pa.Schema:
    """Deserialize a schema from its base64 Arrow-IPC representation."""
    return pa.ipc.read_schema(pa.py_buffer(base64.b64decode(s)))


class SchemaRegistry:
    """Persists per-table schema and partition_by metadata.

    Stored as JSON at ``{root}/_registry/tables.json``, readable and writable
    via PyArrow's filesystem abstraction (local or S3).
    """

    def __init__(self) -> None:
        # table_name → {"schema": pa.Schema, "partition_by": list[str] | None}
        self._tables: dict[str, dict] = {}

    @classmethod
    def load(cls, fs: pa_fs.FileSystem, fs_root: str) -> "SchemaRegistry":
        """Load registry from ``{fs_root}/_registry/tables.json``.

        Returns an empty registry if the file does not exist.
        """
        registry = cls()
        registry_path = f"{fs_root}/_registry/tables.json"
        try:
            with fs.open_input_stream(registry_path) as f:
                data = json.loads(f.read().decode("utf-8"))
            for table_name, entry in data.items():
                if "schema_ipc" not in entry:
                    if "schema_fields" in entry:
                        raise ValueError(
                            f"Registry entry for table '{table_name}' has no "
                            f"'schema_ipc' key but carries a legacy "
                            f"'schema_fields' entry — run "
                            f"'amplify-db-migrate <path>' to upgrade it."
                        )
                    raise ValueError(
                        f"Malformed registry: entry for table '{table_name}' "
                        f"has no 'schema_ipc' key."
                    )
                registry._tables[table_name] = {
                    "schema": _schema_from_ipc_b64(entry["schema_ipc"]),
                    "partition_by": entry.get("partition_by"),
                }
        except (FileNotFoundError, pa.ArrowIOError):
            pass  # Empty registry — first use
        return registry

    def save(self, fs: pa_fs.FileSystem, fs_root: str) -> None:
        """Persist registry to ``{fs_root}/_registry/tables.json``."""
        data = {}
        for table_name, entry in self._tables.items():
            schema: pa.Schema = entry["schema"]
            data[table_name] = {
                # schema_ipc is the only authoritative representation on load.
                "schema_ipc": _schema_to_ipc_b64(schema),
                # columns is a human-readable convenience listing the column
                # names only; it is never read back on load.
                "columns": schema.names,
                "partition_by": entry["partition_by"],
            }

        registry_dir = f"{fs_root}/_registry"
        try:
            fs.create_dir(registry_dir, recursive=True)
        except Exception:
            pass  # May already exist

        registry_path = f"{fs_root}/_registry/tables.json"
        with fs.open_output_stream(registry_path) as f:
            f.write(json.dumps(data, indent=2).encode("utf-8"))

    def register(
        self,
        table: str,
        schema: pa.Schema,
        partition_by: list[str] | None,
    ) -> bool:
        """Register or update a table entry after compatibility check.

        Args:
            table: Table name.
            schema: Target schema (already converted to pa.Schema).
            partition_by: Partition key field names, or None.

        Returns:
            True if the registry was modified (new table or new column added).

        Raises:
            ValueError: For any breaking schema or partition_by change.
        """
        if table not in self._tables:
            self._tables[table] = {"schema": schema, "partition_by": partition_by}
            return True

        existing = self._tables[table]
        existing_schema: pa.Schema = existing["schema"]
        existing_partition_by: list[str] | None = existing["partition_by"]

        # partition_by is immutable once set
        norm_new = list(partition_by or [])
        norm_existing = list(existing_partition_by or [])
        if norm_new != norm_existing:
            raise ValueError(
                f"Cannot change partition_by for table '{table}': "
                f"existing={norm_existing!r}, requested={norm_new!r}. "
                f"Partition keys are immutable once set."
            )

        existing_fields = {f.name: f for f in existing_schema}
        new_fields = {f.name: f for f in schema}

        # Removed columns are not allowed
        removed = set(existing_fields) - set(new_fields)
        if removed:
            raise ValueError(
                f"Cannot remove columns from table '{table}': {sorted(removed)}. "
                f"Column removal requires explicit migration."
            )

        # Changed types or narrowed nullability are not allowed
        for name in new_fields:
            if name in existing_fields:
                ex_field = existing_fields[name]
                new_field = new_fields[name]
                if ex_field.type != new_field.type:
                    raise ValueError(
                        f"Cannot change type of column '{name}' in table '{table}': "
                        f"{ex_field.type} → {new_field.type}. Type changes require explicit migration."
                    )
                if ex_field.nullable and not new_field.nullable:
                    raise ValueError(
                        f"Cannot make nullable column '{name}' non-nullable in table '{table}'."
                    )

        # New columns must be nullable (existing Parquet files return NULL for them)
        added = set(new_fields) - set(existing_fields)
        non_nullable_added = [n for n in added if not new_fields[n].nullable]
        if non_nullable_added:
            raise ValueError(
                f"New column(s) {non_nullable_added!r} in table '{table}' must be nullable. "
                f"Existing Parquet partitions will return NULL for new columns — "
                f"non-nullable new columns would violate schema on read."
            )

        if added:
            self._tables[table] = {"schema": schema, "partition_by": partition_by}
            return True

        return False  # No changes

    def get(self, table: str) -> tuple[pa.Schema, list[str] | None]:
        """Retrieve registered schema and partition_by for a table.

        Raises:
            KeyError: If the table has not been registered.
        """
        if table not in self._tables:
            raise KeyError(
                f"Table '{table}' is not registered. Call create_table() first."
            )
        entry = self._tables[table]
        return entry["schema"], entry["partition_by"]

    def has_table(self, table: str) -> bool:
        """Return True if the table has been registered."""
        return table in self._tables
