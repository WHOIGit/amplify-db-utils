"""Tests that concurrent reads do not interfere with each other.

A DuckDB connection holds exactly one pending result set, so running a second
query on it discards the first query's remaining rows. Because read() and
join() are lazy generators, that showed up as silently truncated/skipped rows
rather than an error. Every read path now runs on its own cursor.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from tests.conftest import (
    ClassificationRecord,
    ImageRecord,
    make_classification,
    make_image,
)

# read() fetches in batches of 1000, so the interleaving tests need enough rows
# to span several batches — a single-batch result would never expose the bug.
N_ROWS = 2500


def _setup_images(store, n=N_ROWS):
    store.create_table("images", ImageRecord, partition_by=["instrument", "year", "month"])
    store.write(
        "images",
        [
            make_image(f"img_{i:05d}", instrument="IFCB107", year=2024, month=1)
            for i in range(n)
        ],
    )
    return store


def _setup_classifications(store, n=N_ROWS):
    store.create_table(
        "classifications",
        ClassificationRecord,
        partition_by=["instrument", "year", "month"],
    )
    store.write(
        "classifications",
        [
            make_classification(f"img_{i:05d}", instrument="IFCB107", year=2024, month=1)
            for i in range(n)
        ],
    )
    return store


# ---------------------------------------------------------------------------
# Interleaved generators (single-threaded)
# ---------------------------------------------------------------------------


def test_interleaved_reads_are_independent(store):
    """Two read() generators consumed in lockstep must each see every row."""
    _setup_images(store)

    a = store.read("images")
    b = store.read("images")

    ids_a, ids_b = [], []
    for row_a, row_b in zip(a, b):
        ids_a.append(row_a["image_id"])
        ids_b.append(row_b["image_id"])

    expected = {f"img_{i:05d}" for i in range(N_ROWS)}
    assert set(ids_a) == expected
    assert set(ids_b) == expected


def test_read_not_disturbed_by_other_queries(store):
    """A partly-consumed read() must survive other queries on the same store."""
    _setup_images(store)

    gen = store.read("images")
    first = [next(gen)["image_id"] for _ in range(1500)]  # spans >1 fetch batch

    # These previously clobbered the generator's pending result set.
    assert store.count("images") == N_ROWS
    assert len(store.bulk_read("images")) == N_ROWS
    assert store.distinct_values("images", ["instrument"]) == [{"instrument": "IFCB107"}]

    rest = [row["image_id"] for row in gen]
    assert set(first) | set(rest) == {f"img_{i:05d}" for i in range(N_ROWS)}
    assert len(first) + len(rest) == N_ROWS


def test_interleaved_joins_are_independent(store):
    """Two join() generators consumed in lockstep must each see every row."""
    _setup_images(store)
    _setup_classifications(store)

    a = store.join("images", "classifications", on="image_id", select="left")
    b = store.join("images", "classifications", on="image_id", select="left")

    rows_a, rows_b = [], []
    for row_a, row_b in zip(a, b):
        rows_a.append(row_a["image_id"])
        rows_b.append(row_b["image_id"])

    expected = {f"img_{i:05d}" for i in range(N_ROWS)}
    assert set(rows_a) == expected
    assert set(rows_b) == expected


def test_read_and_join_interleaved(store):
    """Mixing a read() and a join() generator must not cross-contaminate."""
    _setup_images(store)
    _setup_classifications(store)

    reader = store.read("images")
    joiner = store.join("images", "classifications", on="image_id", select="right")

    read_ids, join_scores = [], []
    for r, j in zip(reader, joiner):
        read_ids.append(r["image_id"])
        join_scores.append(j["score"])

    assert len(read_ids) == N_ROWS
    assert all(s == pytest.approx(0.92) for s in join_scores)


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------


def test_concurrent_reads_from_threads(store):
    """Many threads reading the same store concurrently all get full results."""
    _setup_images(store)

    def work(_):
        return len(list(store.read("images", filters={"instrument": "IFCB107"})))

    with ThreadPoolExecutor(max_workers=8) as pool:
        counts = list(pool.map(work, range(24)))

    assert counts == [N_ROWS] * 24


def test_concurrent_mixed_queries_from_threads(store):
    """count/bulk_read/distinct_values/read run concurrently without interference."""
    _setup_images(store)
    _setup_classifications(store)

    def work(i):
        kind = i % 5
        if kind == 0:
            return store.count("images")
        if kind == 1:
            return len(store.bulk_read("images"))
        if kind == 2:
            return len(list(store.read("images")))
        if kind == 3:
            return len(store.distinct_values("images", ["image_id"]))
        return len(list(store.join("images", "classifications", on="image_id")))

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(work, range(40)))

    assert results == [N_ROWS] * 40


# ---------------------------------------------------------------------------
# Cursor settings
# ---------------------------------------------------------------------------


def test_cursor_replays_local_settings(store):
    """LOCAL-scope settings are per-connection, so _cursor() must reapply them.

    s3_endpoint and s3_use_ssl are LOCAL in DuckDB: a bare conn.cursor() would
    silently lose the configured S3 endpoint and start using SSL again.
    """
    store._local_settings = [
        "SET s3_endpoint = 'vast.example.org'",
        "SET s3_use_ssl = false",
    ]
    cursor = store._cursor()
    try:
        endpoint, use_ssl = cursor.execute(
            "SELECT current_setting('s3_endpoint'), current_setting('s3_use_ssl')"
        ).fetchone()
    finally:
        cursor.close()

    assert endpoint == "vast.example.org"
    assert use_ssl is False


def test_cursor_inherits_global_settings(store):
    """GLOBAL-scope settings set on the parent connection reach every cursor."""
    store._conn.execute("SET threads = 3")
    cursor = store._cursor()
    try:
        assert cursor.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    finally:
        cursor.close()
