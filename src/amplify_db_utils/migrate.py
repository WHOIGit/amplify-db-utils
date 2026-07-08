"""In-place migration of legacy registry sidecar files to the IPC format.

Legacy registries (written before the schema was stored as an Arrow-IPC blob)
recorded each table's schema as a ``schema_fields`` list of
``{name, type, nullable}`` dicts, where ``type`` was the stringified PyArrow
type. That per-field JSON form is no longer read by
:mod:`amplify_db_utils.registry`; the only authoritative representation is now
the base64 ``schema_ipc`` blob.

This module reads such a legacy ``tables.json``, reconstructs each schema, and
rewrites the file in place so every entry carries:

* ``schema_ipc`` — base64 Arrow-IPC schema (authoritative on load)
* ``columns`` — human-readable list of column names
* ``partition_by`` — carried through unchanged

The legacy string→PyArrow parser lives here (not in ``registry.py``) so the
registry stays free of the format we are retiring.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from pathlib import Path

import pyarrow as pa


def _arrow_type_from_str(s: str) -> pa.DataType:
    """Deserialize a PyArrow type from its legacy string representation.

    Covers the scalar + timestamp types that legacy registries could emit.
    """
    simple: dict[str, pa.DataType] = {
        "string": pa.utf8(),
        "utf8": pa.utf8(),
        "large_string": pa.large_utf8(),
        "large_utf8": pa.large_utf8(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "uint8": pa.uint8(),
        "uint16": pa.uint16(),
        "uint32": pa.uint32(),
        "uint64": pa.uint64(),
        "float": pa.float32(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "double": pa.float64(),
        "halffloat": pa.float16(),
        "bool": pa.bool_(),
        "date32[day]": pa.date32(),
        "date64[ms]": pa.date64(),
    }
    if s in simple:
        return simple[s]

    # Timestamp: "timestamp[us, tz=UTC]" or "timestamp[us]"
    m = re.match(r"timestamp\[(\w+)(?:,\s*tz=(.+))?\]", s)
    if m:
        unit, tz = m.group(1), m.group(2)
        return pa.timestamp(unit, tz=tz)

    raise ValueError(f"Cannot deserialize legacy PyArrow type: {s!r}")


def _schema_from_legacy_fields(fields: list[dict]) -> pa.Schema:
    """Reconstruct a schema from the legacy ``schema_fields`` form."""
    return pa.schema([
        pa.field(f["name"], _arrow_type_from_str(f["type"]), nullable=f["nullable"])
        for f in fields
    ])


def _schema_to_ipc_b64(schema: pa.Schema) -> str:
    """Serialize a schema to a base64 Arrow-IPC string."""
    return base64.b64encode(schema.serialize().to_pybytes()).decode("ascii")


def migrate_file(path: str | Path) -> int:
    """Migrate a legacy registry ``tables.json`` in place.

    Each entry with a ``schema_fields`` list (and no ``schema_ipc``) is rewritten
    to carry ``schema_ipc`` + ``columns``; ``schema_fields`` is dropped. Entries
    that already have ``schema_ipc`` are left untouched (idempotent).

    Args:
        path: Path to the ``tables.json`` sidecar file.

    Returns:
        Number of table entries that were migrated.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
        ValueError: If an entry has neither ``schema_ipc`` nor ``schema_fields``.
    """
    path = Path(path)
    data = json.loads(path.read_text(encoding="utf-8"))

    migrated = 0
    for table_name, entry in data.items():
        if "schema_ipc" in entry:
            # Already IPC-backed; just drop any stale legacy field.
            entry.pop("schema_fields", None)
            entry.setdefault(
                "columns", _schema_from_ipc_columns(entry["schema_ipc"])
            )
            continue

        if "schema_fields" not in entry:
            raise ValueError(
                f"Registry entry for table '{table_name}' has neither "
                f"'schema_ipc' nor 'schema_fields'; cannot migrate."
            )

        schema = _schema_from_legacy_fields(entry.pop("schema_fields"))
        entry["schema_ipc"] = _schema_to_ipc_b64(schema)
        entry["columns"] = schema.names
        migrated += 1

    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return migrated


def _schema_from_ipc_columns(ipc_b64: str) -> list[str]:
    """Column names from an existing base64 IPC blob (for idempotent runs)."""
    schema = pa.ipc.read_schema(pa.py_buffer(base64.b64decode(ipc_b64)))
    return schema.names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="amplify-db-migrate",
        description=(
            "Migrate a legacy registry tables.json to the Arrow-IPC schema "
            "format in place."
        ),
    )
    parser.add_argument(
        "path",
        help="Path to the registry tables.json file to migrate.",
    )
    args = parser.parse_args(argv)

    try:
        count = migrate_file(args.path)
    except FileNotFoundError:
        print(f"error: no such file: {args.path}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    print(f"Migrated {count} table entr{'y' if count == 1 else 'ies'} in {args.path}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
