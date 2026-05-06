import os
import time
import uuid
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from amplify_db_utils import VastDBConfig, VastDBStore
from amplify_db_utils.vastdb_store import dedup_by_written_at

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