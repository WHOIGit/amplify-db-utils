"""Tests for the legacy registry migration script."""

from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.fs as pa_fs
import pytest

from amplify_db_utils.migrate import main, migrate_file
from amplify_db_utils.registry import SchemaRegistry


def _write_legacy(tmp_path):
    """Write a true-legacy tables.json (schema_fields only) and return its path."""
    registry_dir = tmp_path / "_registry"
    registry_dir.mkdir()
    path = registry_dir / "tables.json"
    path.write_text(json.dumps({
        "t": {
            "schema_fields": [
                {"name": "id", "type": "string", "nullable": False},
                {"name": "year", "type": "int64", "nullable": False},
                {"name": "ts", "type": "timestamp[us, tz=UTC]", "nullable": True},
            ],
            "partition_by": ["year"],
        }
    }))
    return path


def test_migrate_file_upgrades_legacy_in_place(tmp_path):
    path = _write_legacy(tmp_path)

    count = migrate_file(path)
    assert count == 1

    data = json.loads(path.read_text())
    entry = data["t"]
    assert "schema_ipc" in entry
    assert "schema_fields" not in entry  # legacy form dropped
    assert entry["columns"] == ["id", "year", "ts"]
    assert entry["partition_by"] == ["year"]


def test_migrated_file_loads_with_correct_schema(tmp_path):
    path = _write_legacy(tmp_path)
    migrate_file(path)

    loaded = SchemaRegistry.load(pa_fs.LocalFileSystem(), str(tmp_path))
    schema, partition_by = loaded.get("t")

    assert schema.field("id").type == pa.utf8()
    assert schema.field("year").type == pa.int64()
    assert schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert partition_by == ["year"]


def test_migrate_is_idempotent(tmp_path):
    path = _write_legacy(tmp_path)

    assert migrate_file(path) == 1
    first = path.read_text()

    # Second run finds nothing to migrate and leaves the file unchanged.
    assert migrate_file(path) == 0
    assert path.read_text() == first


def test_migrate_raises_on_unmigratable_entry(tmp_path):
    registry_dir = tmp_path / "_registry"
    registry_dir.mkdir()
    path = registry_dir / "tables.json"
    path.write_text(json.dumps({"t": {"partition_by": None}}))

    with pytest.raises(ValueError, match="neither"):
        migrate_file(path)


def test_main_cli_returns_zero_and_migrates(tmp_path, capsys):
    path = _write_legacy(tmp_path)

    rc = main([str(path)])
    assert rc == 0
    assert "Migrated 1 table entry" in capsys.readouterr().out
    assert "schema_ipc" in json.loads(path.read_text())["t"]


def test_main_cli_missing_file_returns_one(tmp_path, capsys):
    rc = main([str(tmp_path / "nope.json")])
    assert rc == 1
    assert "no such file" in capsys.readouterr().err
