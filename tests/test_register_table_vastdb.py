"""VastDBStore.register_table checks re-declarations made on the same instance.

register_table makes no network call, so these tests stub ``vastdb.connect``
and run offline. Conflicts with other processes or with the server are not
detectable and are out of scope here (see ColumnarStore.register_table).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

pytest.importorskip("vastdb", reason="vastdb optional dependency not installed")

import vastdb  # noqa: E402
from amplify_db_utils.vastdb_store import VastDBConfig, VastDBStore  # noqa: E402


@pytest.fixture
def store(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("register_table must not touch the network")

    monkeypatch.setattr(vastdb, "connect", lambda **kwargs: _Session(fail))
    return VastDBStore(
        VastDBConfig(
            endpoint="http://unused",
            access_key="a",
            secret_key="s",
            bucket="b",
            schema="s",
        )
    )


class _Session:
    """Any use of the session fails the test."""

    def __init__(self, fail):
        self.transaction = fail


SCHEMA = pa.schema([("instrument", pa.string()), ("year", pa.int64())])


def test_identical_redeclaration_is_accepted(store):
    store.register_table("t", SCHEMA, partition_by=["instrument"])
    store.register_table("t", SCHEMA, partition_by=["instrument"])
    assert store.get_schema("t").names == ["instrument", "year"]


def test_changed_partition_by_is_rejected_and_keeps_the_original(store):
    store.register_table("t", SCHEMA, partition_by=["instrument"])

    with pytest.raises(ValueError, match="partition_by"):
        store.register_table("t", SCHEMA, partition_by=["year"])

    assert store._table_meta["t"][1] == ["instrument"]


def test_none_and_empty_partition_by_are_equivalent(store):
    store.register_table("t", SCHEMA)
    store.register_table("t", SCHEMA, partition_by=[])


def test_removed_column_is_rejected(store):
    store.register_table("t", SCHEMA)
    with pytest.raises(ValueError, match="remove"):
        store.register_table("t", pa.schema([("instrument", pa.string())]))


def test_changed_type_is_rejected(store):
    store.register_table("t", SCHEMA)
    with pytest.raises(ValueError, match="type"):
        store.register_table(
            "t", pa.schema([("instrument", pa.string()), ("year", pa.string())])
        )


def test_added_nullable_column_is_adopted(store):
    store.register_table("t", SCHEMA)
    store.register_table("t", SCHEMA.append(pa.field("note", pa.string())))
    assert store.get_schema("t").names == ["instrument", "year", "note"]


def test_first_declaration_is_unchecked(store):
    store.register_table("t", SCHEMA, partition_by=["instrument"])
    store.register_table("other", SCHEMA, partition_by=["year"])
