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


def test_create_after_register_on_the_same_instance_still_persists(tmp_path):
    """Regression: declaring then creating must still write the registry.

    register_table adds to the in-memory registry, so a later create_table on
    the same instance finds nothing to add. create_table used to treat that
    "nothing changed" as "nothing to save" and skipped the write — leaving a
    table that this process could use and no other process, or restart, could
    see.
    """
    root = tmp_path / "store"
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))

    store.register_table("images", ImageRecord, partition_by=PARTITION_BY)
    store.create_table("images", ImageRecord, partition_by=PARTITION_BY)

    assert (root / REGISTRY).exists(), (
        "create_table after register_table skipped the registry write"
    )

    # The real test: a fresh process must see it.
    reopened = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))
    assert reopened.list_tables() == ["images"]
    assert set(reopened.get_schema("images").names) == set(ImageRecord.model_fields)

    reopened.write("images", [make_image()])
    assert reopened.count("images") == 1


def test_creating_one_table_does_not_persist_another_that_was_only_declared(tmp_path):
    """

    register_table and create_table share one in-memory registry, so a naive
    save() dumps both — recording a table as provisioned when it was only
    declared. A startup existence check built on list_tables would then
    believe it, which is exactly the failure the check exists to catch.
    """
    root = tmp_path / "store"
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))

    store.register_table("declared_only", ImageRecord, partition_by=PARTITION_BY)
    store.create_table("created", ImageRecord, partition_by=PARTITION_BY)

    reopened = DuckDBParquetStore(DuckDBParquetConfig(root=str(root)))
    assert reopened.list_tables() == ["created"], (
        "a table that was only declared got persisted as if it existed"
    )


def test_creating_does_not_drop_tables_created_earlier(tmp_path):
    """The converse: filtering the save must not lose existing entries."""
    root = str(tmp_path / "store")

    first = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    first.create_table("a", ImageRecord, partition_by=PARTITION_BY)

    second = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    second.register_table("declared_only", ImageRecord, partition_by=PARTITION_BY)
    second.create_table("b", ImageRecord, partition_by=PARTITION_BY)

    reopened = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    assert sorted(reopened.list_tables()) == ["a", "b"]


def test_list_tables_is_empty_before_anything_is_created(tmp_path):
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(tmp_path / "store")))
    assert store.list_tables() == []


def test_list_tables_reports_created_tables(tmp_path):
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(tmp_path / "store")))
    store.create_table("images", ImageRecord, partition_by=PARTITION_BY)
    assert store.list_tables() == ["images"]


def test_list_tables_ignores_merely_declared_tables(tmp_path):
    """The whole point: it answers "what exists", not "what did I declare".

    register_table adds to the in-memory registry without persisting it, so a
    naive implementation that read self._registry would hand back whatever the
    caller had just declared — and a startup check built on it would pass
    even against a completely unprovisioned store.
    """
    store = DuckDBParquetStore(DuckDBParquetConfig(root=str(tmp_path / "store")))
    store.register_table("images", ImageRecord, partition_by=PARTITION_BY)

    assert store.list_tables() == [], (
        "list_tables reported a table that was only declared, never created"
    )


def test_list_tables_sees_a_table_created_by_another_process(tmp_path):
    """A worker must see what the provisioning step created."""
    root = str(tmp_path / "store")

    DuckDBParquetStore(DuckDBParquetConfig(root=root)).create_table(
        "images", ImageRecord, partition_by=PARTITION_BY
    )

    worker = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    assert worker.list_tables() == ["images"]


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


def _registry_columns(root: str, table: str) -> list[str]:
    """Column names recorded on disk, via a fresh instance."""
    return [
        f.name
        for f in DuckDBParquetStore(DuckDBParquetConfig(root=root)).get_schema(table)
    ]


def test_declared_evolution_does_not_leak_into_the_registry_via_create_table(tmp_path):
    """A schema evolved by register_table stays in memory, even across a later save.

    Regression: register_table used to mutate the registry that create_table
    later saved, so declaring an extra nullable column on one table wrote it to
    disk when any other table was created on the same instance.
    """
    import pyarrow as pa

    root = str(tmp_path / "store")
    DuckDBParquetStore(DuckDBParquetConfig(root=root)).create_table(
        "a", pa.schema([("x", pa.int64())])
    )

    worker = DuckDBParquetStore(DuckDBParquetConfig(root=root))
    worker.register_table("a", pa.schema([("x", pa.int64()), ("y", pa.string())]))
    assert [f.name for f in worker.get_schema("a")] == ["x", "y"]  # declared locally

    worker.create_table("b", pa.schema([("z", pa.int64())]))

    assert _registry_columns(root, "a") == ["x"]


def test_create_table_keeps_tables_created_by_another_instance_since_construction(
    tmp_path,
):
    """create_table re-reads the registry, so a stale snapshot cannot drop entries."""
    import pyarrow as pa

    root = str(tmp_path / "store")
    cfg = DuckDBParquetConfig(root=root)
    stale = DuckDBParquetStore(cfg)  # snapshot taken before anything exists
    other = DuckDBParquetStore(cfg)

    other.create_table("c", pa.schema([("q", pa.int64())]))
    stale.create_table("e", pa.schema([("q", pa.int64())]))

    assert sorted(DuckDBParquetStore(cfg).list_tables()) == ["c", "e"]
