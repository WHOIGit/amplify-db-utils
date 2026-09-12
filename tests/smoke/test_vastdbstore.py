import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest

pytest.importorskip("vastdb", reason="vastdb optional dependency not installed")

from amplify_db_utils.vastdb_store import (
    VastDBConfig,
    VastDBStore,
    dedup_by_written_at,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("VASTDB_ENDPOINT"),
    reason="VastDB credentials not set; skipping live smoke test",
)


@pytest.fixture
def store():
    cfg = VastDBConfig(
        endpoint=os.environ["VASTDB_ENDPOINT"],
        access_key=os.environ["VASTDB_ACCESS_KEY"],
        secret_key=os.environ["VASTDB_SECRET_KEY"],
        bucket=os.environ.get("VASTDB_BUCKET", "scieng-db1"),
        schema=f"smoke_{uuid.uuid4().hex[:8]}",  # throwaway, unique per test run
        add_written_at=True,
    )
    s = VastDBStore(cfg)
    try:
        yield s
    finally:
        s.drop_schema()


def test_vastdb_smoke(store):
    schema = pa.schema([
        pa.field("image_id", pa.string(), nullable=True),
        pa.field("instrument", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("score", pa.float64(), nullable=True),
    ])

    # 1. idempotency: create twice, second call must be a no-op
    store.create_table("smoke", schema, partition_by=["instrument"])
    store.create_table("smoke", schema, partition_by=["instrument"])

    # 2. write a small batch
    now = datetime.now(timezone.utc)
    store.write("smoke", [
        {"image_id": "a", "instrument": "IFCB1", "timestamp": now, "score": 0.1},
        {"image_id": "b", "instrument": "IFCB1", "timestamp": now, "score": 0.2},
        {"image_id": "c", "instrument": "IFCB2", "timestamp": now, "score": 0.3},
    ])

    # 3. equality filter
    eq = list(store.read("smoke", filters={"instrument": "IFCB1"}))
    assert len(eq) == 2, eq

    # 4. range filter
    rng = list(store.read("smoke", filters={"score": {"gte": 0.15, "lte": 0.25}}))
    assert len(rng) == 1 and rng[0]["image_id"] == "b", rng

    # 5. bulk_read returns pa.Table with written_at stamped
    tbl = store.bulk_read("smoke")
    assert isinstance(tbl, pa.Table)
    assert "written_at" in tbl.schema.names
    assert len(tbl) == 3

    # 6. WORM dedup: rewrite same image_id, newer wins
    time.sleep(0.001)  # ensure written_at strictly increases between writes
    store.write("smoke", [
        {"image_id": "a", "instrument": "IFCB1", "timestamp": now, "score": 0.99},
    ])
    full = store.bulk_read("smoke")
    assert len(full) == 4  # two physical rows for image_id "a"

    deduped = dedup_by_written_at(full, key_columns=["image_id"])
    assert len(deduped) == 3
    a_row = deduped.filter(pc.equal(deduped["image_id"], "a")).to_pylist()[0]
    assert a_row["score"] == 0.99


def test_vastdb_projection(store):
    schema = pa.schema([
        pa.field("image_id", pa.string(), nullable=True),
        pa.field("instrument", pa.string(), nullable=True),
        pa.field("timestamp", pa.timestamp("us"), nullable=True),
        pa.field("score", pa.float64(), nullable=True),
    ])
    store.create_table("proj", schema, partition_by=["instrument"])

    now = datetime.now(timezone.utc)
    store.write("proj", [
        {"image_id": "a", "instrument": "IFCB1", "timestamp": now, "score": 0.1},
        {"image_id": "b", "instrument": "IFCB1", "timestamp": now, "score": 0.2},
        {"image_id": "c", "instrument": "IFCB2", "timestamp": now, "score": 0.3},
    ])

    # 1. projection returns exactly the requested columns, in caller order
    tbl = store.bulk_read("proj", columns=["score", "image_id"])
    assert tbl.schema.names == ["score", "image_id"]
    assert len(tbl) == 3

    # 2. columns=None is unchanged
    full = store.bulk_read("proj")
    assert set(full.schema.names) == set(schema.names) | {"written_at"}

    # 3. filter on a column that is not projected
    filtered = store.bulk_read(
        "proj", filters={"instrument": "IFCB1"}, columns=["image_id"]
    )
    assert filtered.schema.names == ["image_id"]
    assert sorted(filtered.column("image_id").to_pylist()) == ["a", "b"]

    # 4. partition key columns are projectable
    parts = store.bulk_read("proj", columns=["instrument"])
    assert parts.schema.names == ["instrument"]
    assert sorted(parts.column("instrument").to_pylist()) == ["IFCB1", "IFCB1", "IFCB2"]

    # 5. read() yields dicts with exactly the projected keys
    rows = list(store.read("proj", filters={"instrument": "IFCB2"}, columns=["image_id"]))
    assert rows == [{"image_id": "c"}]

    # 6. validation errors, raised before any IO
    with pytest.raises(ValueError, match="no_such_column"):
        store.bulk_read("proj", columns=["no_such_column"])
    with pytest.raises(ValueError, match="ambiguous"):
        store.bulk_read("proj", columns=[])
    with pytest.raises(ValueError, match="[Dd]uplicate"):
        store.bulk_read("proj", columns=["image_id", "image_id"])
    with pytest.raises(ValueError, match="no_such_column"):
        store.read("proj", columns=["no_such_column"])

    # 7. columns injected by the store (written_at) are projectable even though
    #    they are not in the caller's declared schema
    stamped = store.bulk_read("proj", columns=["written_at", "image_id"])
    assert stamped.schema.names == ["written_at", "image_id"]


def test_vastdb_projection_read_only_consumer(store):
    """A consumer that never calls create_table() can still read and project."""
    schema = pa.schema([
        pa.field("image_id", pa.string(), nullable=True),
        pa.field("score", pa.float64(), nullable=True),
    ])
    store.create_table("ro", schema)
    store.write("ro", [{"image_id": "a", "score": 0.1}])

    # Fresh store over the same bucket/schema, with an empty metadata cache —
    # this is what a read-only consumer looks like.
    reader = VastDBStore(store._config)

    full = reader.bulk_read("ro")
    assert len(full) == 1

    projected = reader.bulk_read("ro", columns=["score"])
    assert projected.schema.names == ["score"]
    assert list(reader.read("ro", columns=["image_id"])) == [{"image_id": "a"}]

    with pytest.raises(ValueError, match="no_such_column"):
        reader.bulk_read("ro", columns=["no_such_column"])