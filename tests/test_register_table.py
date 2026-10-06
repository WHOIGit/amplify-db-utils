"""register_table declares a table without touching shared storage.

The distinction this module pins is the whole reason register_table exists.
create_table checks whether a table exists and then creates it, in two steps,
with no atomic create-if-absent underneath — so a multi-worker service that
creates tables at startup races itself, once per worker. Splitting the
metadata half out gives those workers something safe to call.

The tests below are cheap and dull, but the invariant is invisible at runtime:
a register_table that quietly wrote the registry would pass every functional
test in this suite and reintroduce the race in production.
"""

from __future__ import annotations

import pytest
from amplify_db_utils import DuckDBParquetConfig, DuckDBParquetStore

from tests.conftest import ImageRecord, make_image

PARTITION_BY = ["instrument", "year", "month"]
REGISTRY = "_registry/tables.json"


def test_register_table_writes_nothing_to_the_store_root(tmp_path):
    """The registry sidecar must not appear. This is the anti-race invariant."""
    root = tmp_path / "store"
    root.mkdir()
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))

    store.register_table("images", ImageRecord, partition_by=PARTITION_BY)

    assert not (root / REGISTRY).exists(), (
        "register_table wrote the schema registry — that file is "
        "read-modify-written without locking, so writing it from several "
        "processes at once loses entries. Only create_table may write it."
    )
    assert list(root.iterdir()) == [], f"register_table created {list(root.iterdir())}"


def test_create_table_does_write_the_registry(tmp_path):
    """Converse of the above, so the first test cannot pass vacuously."""
    root = tmp_path / "store"
    root.mkdir()
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))

    store.create_table("images", ImageRecord, partition_by=PARTITION_BY)

    assert (root / REGISTRY).exists()


def test_register_table_makes_the_table_usable(tmp_path):
    """A fresh process that only declares can still read and write."""
    root = str(tmp_path / "store")

    creator = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    creator.create_table("images", ImageRecord, partition_by=PARTITION_BY)

    # A second store instance, as a second uvicorn worker would have.
    worker = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    worker.register_table("images", ImageRecord, partition_by=PARTITION_BY)

    worker.write("images", [make_image()])
    assert worker.count("images") == 1
    assert set(worker.get_schema("images").names) == set(ImageRecord.model_fields)


def test_write_without_declaring_raises(tmp_path):
    """The failure mode when `improv schema sync` was run but startup was not."""
    root = str(tmp_path / "store")

    creator = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    creator.create_table("images", ImageRecord, partition_by=PARTITION_BY)

    # Fresh instance loads the on-disk registry in __init__, so this one
    # happens to know the table already — the error path is for a table that
    # was never created at all.
    fresh = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    with pytest.raises(RuntimeError, match="not registered"):
        fresh.write("never_created", [make_image()])


def test_register_table_rejects_a_partition_key_absent_from_the_schema(tmp_path):
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(tmp_path)))
    with pytest.raises(ValueError, match="not present in the schema"):
        store.register_table("images", ImageRecord, partition_by=["nonexistent"])


def test_register_table_applies_evolution_rules_against_the_on_disk_registry(tmp_path):
    """DuckDB has local metadata to compare against, so it validates.

    VastDBStore deliberately does not — there is nothing local to compare
    against — which is documented on ColumnarStore.register_table.
    """
    root = str(tmp_path / "store")

    creator = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    creator.create_table("images", ImageRecord, partition_by=PARTITION_BY)

    worker = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    with pytest.raises(ValueError, match="partition_by"):
        worker.register_table("images", ImageRecord, partition_by=["instrument"])
