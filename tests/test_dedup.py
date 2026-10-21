"""Tests for dedup_by_written_at (DuckDB-backed WORM deduplication)."""

from __future__ import annotations

import pyarrow as pa
import pytest

# vastdb_store imports vastdb at module level; skip where the extra is absent.
pytest.importorskip("vastdb")

from amplify_db_utils.vastdb_store import dedup_by_written_at  # noqa: E402


def _rows(table: pa.Table) -> list[dict]:
    """Rows sorted by key, since dedup makes no ordering guarantee."""
    return sorted(table.to_pylist(), key=lambda r: (r["bin"], r["roi"]))


def test_keeps_latest_written_at_per_key():
    table = pa.table({
        "bin": ["a", "a", "b"],
        "roi": [1, 1, 1],
        "written_at": [1, 2, 1],
        "label": ["old", "new", "only"],
    })

    result = dedup_by_written_at(table, ["bin", "roi"])

    assert isinstance(result, pa.Table)
    assert _rows(result) == [
        {"bin": "a", "roi": 1, "written_at": 2, "label": "new"},
        {"bin": "b", "roi": 1, "written_at": 1, "label": "only"},
    ]


def test_distinct_keys_all_kept():
    table = pa.table({
        "bin": ["a", "a"],
        "roi": [1, 2],
        "written_at": [1, 1],
    })

    result = dedup_by_written_at(table, ["bin", "roi"])

    assert result.num_rows == 2


def test_drops_helper_column():
    table = pa.table({"k": [1], "written_at": [1]})

    result = dedup_by_written_at(table, ["k"])

    assert result.column_names == ["k", "written_at"]


def test_empty_table_returned_unchanged():
    table = pa.table({"k": pa.array([], pa.int64()), "written_at": pa.array([], pa.int64())})

    result = dedup_by_written_at(table, ["k"])

    assert result.num_rows == 0
    assert result.schema == table.schema
